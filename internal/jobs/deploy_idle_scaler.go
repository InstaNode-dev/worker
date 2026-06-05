package jobs

// deploy_idle_scaler.go — scale-to-zero idle descheduler (Task #54).
//
// SIBLING TO deployment_expirer.go, NOT A REPLACEMENT
//
//   - deployment_expirer soft-deletes (status='expired') a deploy whose TTL
//     elapsed. That is PERMANENT and tears the app down.
//   - this idle-scaler patches an idle (but live) app's Deployment to
//     replicas=0 — ~$0 compute, fully REVERSIBLE. The row stays 'healthy';
//     only scaled_to_zero flips true. The api wake endpoint (or a redeploy)
//     brings it back. Idle ≠ expired.
//
// THE IDLE SIGNAL (v1 — stated honestly)
//
// instanode.dev serves a deployed app via a k8s Ingress that routes straight
// to the per-app Service; the api/worker processes are NOT in the request
// path, and no nginx-ingress request-total scrape is wired into the worker
// today. So the only reliable "activity" signal v1 has is the
// deployments.last_activity_at column, which is stamped at create-time and
// bumped on every deploy / redeploy / explicit wake (api migration 068).
//
// THEREFORE v1 idle = "no deploy / redeploy / wake for N minutes", NOT
// "no HTTP traffic for N minutes". This is a deliberately conservative signal:
// it will never deschedule an app the user is actively redeploying, and the
// explicit-wake path makes a wrongly-slept app one POST away from awake. The
// FOLLOW-UP to make this traffic-based is to scrape an nginx-ingress per-host
// request counter (or have the ingress bump last_activity_at) — that lifts the
// signal to true traffic-idle without changing this job's structure.
//
// FLAG-GATED, DEFAULT OFF
//
// The whole job is inert unless DEPLOY_SCALE_TO_ZERO_ENABLED is set. When off,
// Work() logs at DEBUG and returns immediately — no k8s patch, no DB write.
// Proven by TestDeployIdleScaler_FlagOffNoOp.
//
// FAIL-OPEN POSTURE
//
// Constructed with a deployScaleK8sProvider that may be nil (no cluster in CI /
// docker-compose). Work() short-circuits with a WARN when k8s is nil — the
// rest of the worker keeps running, identical to deploy_status_reconcile.
//
// PER-APP OPT-OUT
//
// always_on=true pins an app: the candidate SELECT excludes it, so a pinned
// (Pro+/operator) app never sleeps. The scale-down UPDATE is double-guarded on
// the same predicate so a concurrent pin/redeploy/wake between SELECT and
// UPDATE makes the row report 0 rows (skipped, not wrongly slept).

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"instant.dev/worker/internal/metrics"
)

const (
	// deployIdleScalerInterval is how often the sweep runs. 2 min is frequent
	// enough that an app sleeps promptly after crossing the idle threshold,
	// while the threshold itself (default 30 min) is what actually governs
	// when descheduling happens — the tick just polls.
	deployIdleScalerInterval = 2 * time.Minute

	// idleScalerBatchLimit caps how many apps one tick descheduals so a backlog
	// (flag flipped on for the first time with a large idle fleet) spreads the
	// k8s API load across ticks instead of one thundering burst.
	idleScalerBatchLimit = 50

	// idleScalerK8sTimeout caps a single ScaleDeployment call so one stuck
	// namespace can't stall the batch.
	idleScalerK8sTimeout = 5 * time.Second

	// Status / naming constants — verbatim copies of the api's canonical set
	// (the worker module does not import the api module; same convention as
	// deploy_status_reconcile.go). If the api strings change, update both.
	idleScalerStatusHealthy = "healthy"
	idleScalerProviderPfx   = "app-"            // provider_id = "app-<appID>"
	idleScalerNSPfx         = "instant-deploy-" // namespace = "instant-deploy-<appID>"
)

// DeployIdleScalerArgs is the periodic-job payload. Empty — every run is a
// full candidate sweep.
type DeployIdleScalerArgs struct{}

// Kind implements river.JobArgs.
func (DeployIdleScalerArgs) Kind() string { return "deploy_idle_scaler" }

// deployScaleK8sProvider is the slice of k8s the idle-scaler needs: patch a
// Deployment's replica count. Defined as an interface so the worker can pass
// nil when no cluster is reachable and so tests inject a recording fake.
type deployScaleK8sProvider interface {
	// ScaleDeployment patches spec.replicas on the named Deployment. A NotFound
	// Deployment MUST be returned as apierrors.IsNotFound so the caller can
	// treat a torn-down app as "skip, not fail" instead of wedging the row.
	ScaleDeployment(ctx context.Context, namespace, name string, replicas int32) error
}

// k8sDeployScaleClient is the concrete deployScaleK8sProvider, backed by a
// kubernetes.Clientset. Mirrors k8sDeployStatusClient in
// deploy_status_reconcile.go.
type k8sDeployScaleClient struct {
	cs kubernetes.Interface
}

// ScaleDeployment implements deployScaleK8sProvider via a read-modify-write
// Update (the fake clientset used in tests does not support strategic-merge
// Patch on subresources, and Update is the same idempotent shape the api's
// compute.Scale uses).
func (c *k8sDeployScaleClient) ScaleDeployment(ctx context.Context, namespace, name string, replicas int32) error {
	d, err := c.cs.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err // includes apierrors.IsNotFound — caller inspects it
	}
	if d.Spec.Replicas != nil && *d.Spec.Replicas == replicas {
		return nil // already at target — idempotent no-op
	}
	r := replicas
	d.Spec.Replicas = &r
	_, err = c.cs.AppsV1().Deployments(namespace).Update(ctx, d, metav1.UpdateOptions{})
	return err
}

// NewK8sDeployScaleClient builds a deployScaleK8sProvider sharing the supplied
// clientset (callers pass the same kubernetes.Interface used for the status
// reconciler so they share a connection pool).
func NewK8sDeployScaleClient(cs kubernetes.Interface) deployScaleK8sProvider {
	return &k8sDeployScaleClient{cs: cs}
}

// NewK8sDeployScaleClientFromCluster builds a scale client from in-cluster
// config (kubeconfig fallback for local dev). Returns (nil, err) when no
// cluster is reachable — StartWorkers logs and passes nil, so the idle-scaler
// short-circuits with a WARN each tick (fail-open, identical to the status
// reconciler). Reuses newDeployK8sClientset from deploy_status_reconcile.go.
func NewK8sDeployScaleClientFromCluster() (deployScaleK8sProvider, error) {
	cs, err := newDeployK8sClientset()
	if err != nil {
		return nil, err
	}
	return &k8sDeployScaleClient{cs: cs}, nil
}

// compile-time assertion that the production client satisfies the interface.
var _ deployScaleK8sProvider = (*k8sDeployScaleClient)(nil)

// compile-time assertion appsv1 is used (Get returns *appsv1.Deployment).
var _ = appsv1.Deployment{}

// DeployIdleScaler is the River worker that descheduals idle deployments.
type DeployIdleScaler struct {
	river.WorkerDefaults[DeployIdleScalerArgs]
	db          *sql.DB
	k8s         deployScaleK8sProvider // may be nil → Work warn-logs each tick
	enabled     bool                   // DEPLOY_SCALE_TO_ZERO_ENABLED
	idleMinutes int                    // descheduling threshold
}

// NewDeployIdleScaler constructs the worker. Pass nil for k8sProvider when the
// cluster is unreachable. enabled gates the entire job (default-off flag);
// idleMinutes is the no-activity threshold (the constructor floors it at 5).
func NewDeployIdleScaler(db *sql.DB, k8sProvider deployScaleK8sProvider, enabled bool, idleMinutes int) *DeployIdleScaler {
	if idleMinutes < 5 {
		idleMinutes = 30
	}
	return &DeployIdleScaler{
		db:          db,
		k8s:         k8sProvider,
		enabled:     enabled,
		idleMinutes: idleMinutes,
	}
}

// idleCandidate is the projection the scaler reads.
type idleCandidate struct {
	id         uuid.UUID
	providerID string
}

// Work runs one idle sweep.
func (w *DeployIdleScaler) Work(ctx context.Context, job *river.Job[DeployIdleScalerArgs]) error {
	start := time.Now()

	if !w.enabled {
		// Flag OFF → fully inert. Idle-tick at DEBUG per worker convention 1.
		slog.Debug("jobs.deploy_idle_scaler.disabled",
			"note", "DEPLOY_SCALE_TO_ZERO_ENABLED unset; scale-to-zero is off",
			"job_id", job.ID)
		return nil
	}

	if w.k8s == nil {
		slog.Warn("jobs.deploy_idle_scaler.skipped_no_k8s_client",
			"reason", "k8s client init failed at startup; idle apps will not be descheduled until the worker restarts with a reachable cluster",
			"job_id", job.ID)
		return nil
	}

	candidates, err := w.listIdleCandidates(ctx)
	if err != nil {
		return fmt.Errorf("deploy_idle_scaler: list candidates: %w", err)
	}

	var scaledDown, skipped, failed int
	for _, c := range candidates {
		ns, name := namespaceAndNameFromProviderID(c.providerID)
		if ns == "" {
			// provider_id not in app-<appID> shape (e.g. a stack row) — not ours.
			skipped++
			continue
		}

		scaleCtx, cancel := context.WithTimeout(ctx, idleScalerK8sTimeout)
		scaleErr := w.k8s.ScaleDeployment(scaleCtx, ns, name, 0)
		cancel()
		if scaleErr != nil {
			if apierrors.IsNotFound(scaleErr) {
				// Deployment torn down out from under us — skip, don't flip the
				// row (status reconciler / orphan sweep will reconcile it).
				skipped++
				continue
			}
			slog.Warn("jobs.deploy_idle_scaler.scale_failed",
				"id", c.id, "namespace", ns, "error", scaleErr)
			metrics.DeployScaledToZeroTotal.WithLabelValues("scale_failed").Inc()
			failed++
			continue
		}

		// DB half: CAS-flip scaled_to_zero=true. Double-guarded on the same
		// predicate as the SELECT so a row that raced into a non-eligible state
		// (woken, pinned, redeployed, expired) between SELECT and UPDATE is left
		// alone — k8s was already patched to 0, but a concurrent wake re-scales
		// to 1 and the next tick re-evaluates, so we never strand it.
		n, dbErr := w.markScaledToZero(ctx, c.id)
		if dbErr != nil {
			slog.Error("jobs.deploy_idle_scaler.db_flip_failed",
				"id", c.id, "error", dbErr)
			metrics.DeployScaledToZeroTotal.WithLabelValues("scale_failed").Inc()
			failed++
			continue
		}
		if n == 0 {
			skipped++
			continue
		}
		metrics.DeployScaledToZeroTotal.WithLabelValues("scaled_down").Inc()
		slog.Info("jobs.deploy_idle_scaler.scaled_down",
			"id", c.id, "namespace", ns,
			"idle_threshold_min", w.idleMinutes)
		scaledDown++
	}

	// Sample the asleep-fleet gauge regardless of whether we scaled anything
	// this tick (so the tile stays accurate even on quiet ticks).
	if asleep, gErr := w.countAsleep(ctx); gErr == nil {
		metrics.DeployIdleApps.Set(float64(asleep))
	} else {
		slog.Warn("jobs.deploy_idle_scaler.gauge_sample_failed", "error", gErr)
	}

	if scaledDown == 0 && failed == 0 {
		// Idle tick (nothing descheduled, nothing failed) → DEBUG per convention.
		slog.Debug("jobs.deploy_idle_scaler.completed",
			"candidates", len(candidates), "scaled_down", 0, "skipped", skipped,
			"duration_ms", time.Since(start).Milliseconds(), "job_id", job.ID)
		return nil
	}
	slog.Info("jobs.deploy_idle_scaler.completed",
		"candidates", len(candidates), "scaled_down", scaledDown,
		"skipped", skipped, "failed", failed,
		"duration_ms", time.Since(start).Milliseconds(), "job_id", job.ID)
	return nil
}

// listIdleCandidates returns healthy, not-already-zeroed, not-pinned
// deployments whose last_activity_at is older than the idle threshold. NULL
// last_activity_at rows (legacy, pre-068-backfill edge) are NOT selected —
// migration 068 backfills them, but a defensive NULL-excluding predicate means
// a row with no activity stamp is never descheduled blind.
func (w *DeployIdleScaler) listIdleCandidates(ctx context.Context) ([]idleCandidate, error) {
	cutoff := time.Now().UTC().Add(-time.Duration(w.idleMinutes) * time.Minute)
	rows, err := w.db.QueryContext(ctx, `
		SELECT id, COALESCE(provider_id, '')
		FROM deployments
		WHERE status = $1
		  AND scaled_to_zero = false
		  AND always_on = false
		  AND last_activity_at IS NOT NULL
		  AND last_activity_at < $2
		  AND provider_id IS NOT NULL
		  AND provider_id <> ''
		ORDER BY last_activity_at ASC
		LIMIT $3
	`, idleScalerStatusHealthy, cutoff, idleScalerBatchLimit)
	if err != nil {
		return nil, fmt.Errorf("listIdleCandidates: query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []idleCandidate
	for rows.Next() {
		var c idleCandidate
		if err := rows.Scan(&c.id, &c.providerID); err != nil {
			return nil, fmt.Errorf("listIdleCandidates: scan: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// markScaledToZero flips scaled_to_zero=true with the same eligibility CAS as
// the candidate SELECT. Returns rows affected (0 = raced into non-eligible
// state; skip).
func (w *DeployIdleScaler) markScaledToZero(ctx context.Context, id uuid.UUID) (int64, error) {
	res, err := w.db.ExecContext(ctx, `
		UPDATE deployments
		SET scaled_to_zero = true, updated_at = now()
		WHERE id = $1
		  AND status = $2
		  AND scaled_to_zero = false
		  AND always_on = false
	`, id, idleScalerStatusHealthy)
	if err != nil {
		return 0, fmt.Errorf("markScaledToZero: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// countAsleep returns how many deployments are currently scaled_to_zero — the
// value published to the instant_deploy_idle_apps gauge.
func (w *DeployIdleScaler) countAsleep(ctx context.Context) (int, error) {
	var n int
	err := w.db.QueryRowContext(ctx, `
		SELECT count(*) FROM deployments WHERE scaled_to_zero = true
	`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("countAsleep: %w", err)
	}
	return n, nil
}

// namespaceAndNameFromProviderID derives the per-deployment namespace +
// Deployment name from provider_id = "app-<appID>". Returns ("","") for a
// provider_id not in that shape (e.g. a stack row) so the caller skips it.
func namespaceAndNameFromProviderID(providerID string) (namespace, name string) {
	if !strings.HasPrefix(providerID, idleScalerProviderPfx) {
		return "", ""
	}
	appID := strings.TrimPrefix(providerID, idleScalerProviderPfx)
	if appID == "" {
		return "", ""
	}
	return idleScalerNSPfx + appID, providerID
}
