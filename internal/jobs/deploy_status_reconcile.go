package jobs

// deploy_status_reconcile.go — periodic reconciler for the deployments table.
//
// PROBLEM SHAPE
//
// The API's /deploy/new handler persists a deployments row with status="building"
// and kicks off runDeploy() in a goroutine. runDeploy() calls compute.Deploy()
// which, on a k8s backend, returns immediately after kicking off the kaniko
// build + creating the Deployment object. The handler then writes whatever
// status compute.Deploy() returned (typically still "building" if AvailableReplicas
// has not yet reached 1).
//
// After that single write, the deployments.status column is never touched again
// by the system. The k8s Deployment transitions to AvailableReplicas>=1 within
// ~30-90s on local clusters, but the DB row stays "building" forever — there
// is no background process that watches live k8s state and rolls it forward.
//
// Customers calling GET /deploy/:id to discover when their app is ready get
// the stale value. This worker fixes that by sweeping all non-terminal
// deployments every 30s and reconciling the column from the live k8s
// Deployment object's status.AvailableReplicas + conditions.
//
// LIFECYCLE (mirrors api/internal/providers/compute/k8s/client.go:deploymentStatus)
//
//   building ─→ deploying ─→ healthy
//                              │
//                              └→ failed (DeploymentReplicaFailure=True)
//
//   building ─→ stopped (k8s Deployment NotFound — namespace was deleted)
//
//   failed and stopped are TERMINAL — we never reconcile out of them. The
//   listActiveDeployments SQL filter already excludes them.
//
// SCOPE / FAIL-OPEN POSTURE
//
// The worker is constructed with a deployStatusK8sProvider interface. The
// concrete implementation lives in this file (k8sDeployStatusClient) and is
// built from rest.InClusterConfig() with a kubeconfig fallback for local
// dev. If client init fails (e.g. no kubeconfig in CI, no cluster reachable),
// the constructor returns a reconciler with k8s=nil and Work() short-circuits
// at the top with a warn log. This keeps the worker process alive — other
// periodic jobs (expire_anonymous, expire_stacks, custom_domain_reconcile)
// keep running. Same fail-open posture as ExpireAnonymousWorker on Redis errors.
//
// SQL SCHEMA
//
// The deployments table is owned by the api module. Columns referenced here:
//   - id           (uuid, PK)
//   - provider_id  (text, nullable until runDeploy completes — we skip those)
//   - status       (text)
//   - error_message (text, nullable)
//   - updated_at   (timestamptz)
// Status string set (kept in sync with api/internal/models/deployment.go):
//   building | deploying | healthy | failed | stopped
//
// NAMESPACE LAYOUT
//
// The api's k8s.K8sProvider derives namespace = "instant-deploy-<appID>" from
// the provider_id, where provider_id = "app-<appID>". This worker duplicates
// that derivation (deployNamespaceFromProviderID) rather than depending on
// the api package — same pattern custom_domain_reconcile.go uses for status
// strings.
//
// RBAC
//
// The worker ServiceAccount (infra/k8s/worker-rbac.yaml) gains a new ClusterRole
// "instant-worker-deploy-reader" with get on deployments.apps. We can't restrict
// it to namespace-by-namespace because instant-deploy-<appID> namespaces are
// created on demand by the api process; the deploy-reader ClusterRole is
// read-only and scoped to the deployments resource so the blast radius is tight.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Reconciler tunables. The interval matches the periodic-job registration in
// workers.go.
const (
	deployStatusReconcileInterval = 30 * time.Second

	// k8sGetTimeout caps a single Get-Deployment call so one stuck namespace
	// doesn't stall the whole batch sweep. The control plane is normally
	// sub-second; 5s is generous.
	k8sGetTimeout = 5 * time.Second

	// Status strings — verbatim copies of the canonical set in
	// api/internal/models/deployment.go and the k8s compute provider's
	// deploymentStatus() helper. Duplicated here because the worker module
	// does not import the api module. If the api strings ever change,
	// update both places.
	deployStatusBuilding  = "building"
	deployStatusDeploying = "deploying"
	deployStatusHealthy   = "healthy"
	deployStatusFailed    = "failed"
	deployStatusStopped   = "stopped"

	// providerIDPrefix mirrors api/internal/providers/compute/k8s/client.go's
	// deploymentName(appID) = "app-" + appID.
	providerIDPrefix = "app-"

	// deployNamespacePrefix mirrors deployNamespace(appID) in the api's k8s
	// provider. The worker derives the namespace from provider_id rather than
	// storing it on the deployments row.
	deployNamespacePrefix = "instant-deploy-"
)

// DeployStatusReconcileArgs is the periodic-job payload. Empty — every run is
// a full table sweep.
type DeployStatusReconcileArgs struct{}

// Kind implements river.JobArgs.
func (DeployStatusReconcileArgs) Kind() string { return "deploy_status_reconcile" }

// deployStatusK8sProvider is the slice of k8s API the reconciler uses. Defined
// as an interface so callers can pass nil when k8s isn't reachable (the
// reconciler then warn-logs and returns nil — same pattern as
// k8sCustomDomainProvider in custom_domain_reconcile.go).
type deployStatusK8sProvider interface {
	// GetDeployment returns the live k8s Deployment object, or apierrors.IsNotFound
	// when the namespace or Deployment has been deleted. The caller maps NotFound
	// to status="stopped".
	GetDeployment(ctx context.Context, namespace, name string) (*appsv1.Deployment, error)
}

// k8sDeployStatusClient is the concrete deployStatusK8sProvider implementation.
// Wraps a kubernetes.Clientset so the reconciler doesn't import the full client
// surface at every callsite.
type k8sDeployStatusClient struct {
	cs *kubernetes.Clientset
}

// GetDeployment implements deployStatusK8sProvider.
func (c *k8sDeployStatusClient) GetDeployment(ctx context.Context, namespace, name string) (*appsv1.Deployment, error) {
	return c.cs.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
}

// NewK8sDeployStatusClient builds a deployStatusK8sProvider from in-cluster
// config, falling back to the default kubeconfig for local dev. Returns nil
// (and a non-nil error) when neither is reachable — the caller logs and
// passes nil to NewDeployStatusReconciler.
//
// This mirrors api/internal/providers/compute/k8s/client.go:newClientset but
// duplicated here so the worker module does not depend on the api module.
func NewK8sDeployStatusClient() (deployStatusK8sProvider, error) {
	cs, err := newDeployK8sClientset()
	if err != nil {
		return nil, err
	}
	return &k8sDeployStatusClient{cs: cs}, nil
}

// NewK8sDeployStatusClientWithAutopsy is the same as NewK8sDeployStatusClient
// but also returns a deployAutopsyK8sProvider backed by the same underlying
// kubernetes.Clientset. Call this in StartWorkers so both the status reconciler
// and the autopsy capturer share a single TCP connection pool to the k8s API.
func NewK8sDeployStatusClientWithAutopsy() (deployStatusK8sProvider, deployAutopsyK8sProvider, error) {
	cs, err := newDeployK8sClientset()
	if err != nil {
		return nil, nil, err
	}
	return &k8sDeployStatusClient{cs: cs}, NewK8sAutopsyClient(cs), nil
}

// newDeployK8sClientset builds a kubernetes.Clientset from in-cluster config,
// falling back to the default kubeconfig for local dev.
func newDeployK8sClientset() (*kubernetes.Clientset, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		cfg, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
		if err != nil {
			return nil, fmt.Errorf("k8s config (in-cluster + kubeconfig both failed): %w", err)
		}
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("k8s NewForConfig: %w", err)
	}
	return cs, nil
}

// DeployStatusReconciler is the River worker that sweeps non-terminal
// deployments and rolls status forward from live k8s state.
type DeployStatusReconciler struct {
	river.WorkerDefaults[DeployStatusReconcileArgs]
	db         *sql.DB
	k8s        deployStatusK8sProvider      // may be nil — the worker then warn-logs each run
	autopsyK8s deployAutopsyK8sProvider     // may be nil — autopsy rows will use Unknown reason
}

// NewDeployStatusReconciler constructs the worker.
//
// Pass nil for k8sProvider in environments where the worker can't talk to
// the cluster API. Work() will short-circuit each run with a warn log so
// the rest of the periodic job lineup keeps functioning.
//
// Pass nil for autopsyK8s when the extended autopsy interface is not available;
// the reconciler will still write Unknown-reason autopsy rows on failure transitions
// so the api always surfaces a "failure" object on failed deployments.
func NewDeployStatusReconciler(db *sql.DB, k8sProvider deployStatusK8sProvider) *DeployStatusReconciler {
	return &DeployStatusReconciler{
		db:  db,
		k8s: k8sProvider,
	}
}

// WithAutopsyK8s wires the extended k8s client used for failure autopsy capture.
// Called by StartWorkers after constructing the reconciler when the cluster is
// reachable. Not called in CI / docker-compose where k8s is absent.
func (r *DeployStatusReconciler) WithAutopsyK8s(autopsyK8s deployAutopsyK8sProvider) *DeployStatusReconciler {
	r.autopsyK8s = autopsyK8s
	return r
}

// activeDeployment is the projection the reconciler reads — only the columns
// needed to drive transitions.
type activeDeployment struct {
	id         uuid.UUID
	providerID string
	status     string
}

// Work runs the full sweep. Errors on individual rows are logged and swallowed
// so one bad row never stops the rest — same fail-open posture as
// CustomDomainReconciler.
func (w *DeployStatusReconciler) Work(ctx context.Context, job *river.Job[DeployStatusReconcileArgs]) error {
	start := time.Now()

	if w.k8s == nil {
		// k8s wasn't reachable at startup — log once per tick at WARN so
		// operators notice, but don't fail the job (River would retry
		// forever). The rest of the worker process keeps running.
		slog.Warn("jobs.deploy_status_reconcile.skipped_no_k8s_client",
			"reason", "k8s client init failed at startup; deployments will stay stale until worker is restarted with reachable cluster",
			"job_id", job.ID,
		)
		return nil
	}

	deployments, err := w.listActiveDeployments(ctx)
	if err != nil {
		return fmt.Errorf("deploy_status_reconcile: list active: %w", err)
	}

	if len(deployments) == 0 {
		slog.Info("jobs.deploy_status_reconcile.completed",
			"total", 0,
			"duration_ms", time.Since(start).Milliseconds(),
			"job_id", job.ID,
		)
		return nil
	}

	var (
		transitions int
		errors      int
		skipped     int
	)

	for _, d := range deployments {
		if d.providerID == "" {
			// runDeploy() hasn't reached UpdateDeploymentProviderID yet
			// (kaniko build still in flight on the api side). Nothing to
			// poll — leave the row alone.
			skipped++
			continue
		}

		newStatus, err := w.computeNewStatus(ctx, d.providerID)
		if err == errSkipForeignProviderID {
			// Row was provisioned by a different backend (e.g. the stack
			// pipeline uses provider_id="instant-stack-*"). Not a fault.
			slog.Debug("jobs.deploy_status_reconcile.skip_foreign_provider_id",
				"id", d.id, "provider_id", d.providerID)
			skipped++
			continue
		}
		if err != nil {
			slog.Warn("jobs.deploy_status_reconcile.k8s_get_failed",
				"id", d.id, "provider_id", d.providerID, "error", err)
			errors++
			continue
		}

		if newStatus == d.status {
			continue
		}

		if err := w.updateStatus(ctx, d.id, newStatus); err != nil {
			slog.Error("jobs.deploy_status_reconcile.update_failed",
				"id", d.id, "provider_id", d.providerID,
				"from", d.status, "to", newStatus, "error", err)
			errors++
			continue
		}

		slog.Info("jobs.deploy_status_reconcile.transition",
			"id", d.id, "provider_id", d.providerID,
			"from", d.status, "to", newStatus)
		transitions++

		// Phase 0 — Failure Autopsy: capture a deployment_events row whenever
		// a deployment transitions INTO the "failed" state. The api's
		// GET /deploy/:id handler reads this row and surfaces it as the
		// optional "failure" object in the response.
		//
		// We capture on EVERY transition into "failed" (not just the first)
		// because the reconciler may see building→failed if k8s fires a
		// ReplicaFailure condition before the pod emits its first log.
		// The upsert is idempotent, so re-capturing overwrites the row with
		// the latest pod state rather than accumulating duplicates.
		//
		// This runs synchronously in the sweep loop because the log-tail call
		// has its own 10s timeout (k8sGetTimeout is reused per sub-call).
		// The worst case is one 10s stall per failed pod, which is acceptable
		// given the 30s reconcile interval and the small number of failed pods
		// in a healthy cluster.
		if newStatus == deployStatusFailed {
			captureDeploymentAutopsy(ctx, w.db, d.id, d.providerID, w.autopsyK8s)
		}
	}

	slog.Info("jobs.deploy_status_reconcile.completed",
		"total", len(deployments),
		"transitions", transitions,
		"errors", errors,
		"skipped", skipped,
		"duration_ms", time.Since(start).Milliseconds(),
		"job_id", job.ID,
	)
	return nil
}

// errSkipForeignProviderID is returned by computeNewStatus when a deployments
// row's provider_id does not match the "app-<appID>" shape (e.g. it was
// provisioned by the stack pipeline whose provider_id is "instant-stack-<id>").
// Sentinel error — Work() treats this as a skip, not an error.
var errSkipForeignProviderID = errors.New("provider_id not in app-<appID> shape; reconciler is single-app only")

// computeNewStatus performs a single Get against the per-deployment namespace
// and maps the result into the canonical status string set. NotFound (the
// namespace or Deployment has been deleted out from under us) maps to
// "stopped" — same as the api's k8s.Status() helper.
func (w *DeployStatusReconciler) computeNewStatus(ctx context.Context, providerID string) (string, error) {
	ns := deployNamespaceFromProviderID(providerID)
	if ns == "" {
		// provider_id doesn't have the "app-" prefix — almost certainly a row
		// from a future or alternate compute backend. Skip cleanly.
		return "", errSkipForeignProviderID
	}

	getCtx, cancel := context.WithTimeout(ctx, k8sGetTimeout)
	defer cancel()

	deploy, err := w.k8s.GetDeployment(getCtx, ns, providerID)
	if apierrors.IsNotFound(err) {
		// Namespace or Deployment is gone (manual cleanup, expiry sweep,
		// teardown). Mark stopped so the row leaves the active set on
		// the next sweep.
		return deployStatusStopped, nil
	}
	if err != nil {
		return "", err
	}

	return deploymentStatusFromK8s(deploy), nil
}

// deploymentStatusFromK8s mirrors api/internal/providers/compute/k8s/client.go's
// deploymentStatus() helper. Kept verbatim here to guarantee the worker's
// state machine matches what runDeploy() would write if it polled longer.
//
// Order matters: replica failure is checked first so a Deployment that has
// transient AvailableReplicas>=1 but a sticky failure condition (e.g. quota
// exceeded on a rolling update) does not flap into "healthy".
func deploymentStatusFromK8s(deploy *appsv1.Deployment) string {
	for _, cond := range deploy.Status.Conditions {
		if cond.Type == appsv1.DeploymentReplicaFailure && cond.Status == corev1.ConditionTrue {
			return deployStatusFailed
		}
	}
	if deploy.Status.AvailableReplicas >= 1 {
		return deployStatusHealthy
	}
	if deploy.Status.UpdatedReplicas > 0 || deploy.Status.UnavailableReplicas > 0 {
		return deployStatusDeploying
	}
	return deployStatusBuilding
}

// deployNamespaceFromProviderID derives the per-deployment namespace from the
// provider_id stored on a deployments row. provider_id = "app-<appID>";
// namespace = "instant-deploy-<appID>". Returns "" for rows whose provider_id
// doesn't match the expected shape (e.g. future Fly.io backend).
func deployNamespaceFromProviderID(providerID string) string {
	if !strings.HasPrefix(providerID, providerIDPrefix) {
		return ""
	}
	appID := strings.TrimPrefix(providerID, providerIDPrefix)
	if appID == "" {
		return ""
	}
	return deployNamespacePrefix + appID
}

// ── SQL helpers ───────────────────────────────────────────────────────────────
//
// These mirror the api's models.Deployment helpers but use only the columns
// the reconciler needs. Duplicated here intentionally — the worker module does
// not import the api module. The schema lives in the deployments migration in
// api/internal/db/migrations; if columns or status strings change, keep these
// in sync.

// listActiveDeployments returns every deployments row not in a terminal status.
// Terminal = failed | stopped. The api's runDeploy/Redeploy paths also write
// "deploying" and "building" transitively — both are picked up here.
func (w *DeployStatusReconciler) listActiveDeployments(ctx context.Context) ([]activeDeployment, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT id, COALESCE(provider_id, ''), status
		FROM deployments
		WHERE status IN ($1, $2, $3)
		ORDER BY updated_at ASC
	`, deployStatusBuilding, deployStatusDeploying, deployStatusHealthy)
	if err != nil {
		return nil, fmt.Errorf("listActiveDeployments: query: %w", err)
	}
	defer rows.Close()

	var out []activeDeployment
	for rows.Next() {
		var d activeDeployment
		if err := rows.Scan(&d.id, &d.providerID, &d.status); err != nil {
			return nil, fmt.Errorf("listActiveDeployments: scan: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// updateStatus advances the status column and bumps updated_at. We deliberately
// do NOT clear error_message — the api owns that field and we don't want to
// race with a concurrent runDeploy error write.
//
// We also do not transition out of "failed" or "stopped" — the SELECT above
// excludes those, so the UPDATE doesn't need a defensive WHERE clause for
// them. We do, however, add a WHERE status IN (...) guard so a row that
// concurrently transitioned to a terminal state between SELECT and UPDATE
// is left alone.
func (w *DeployStatusReconciler) updateStatus(ctx context.Context, id uuid.UUID, status string) error {
	_, err := w.db.ExecContext(ctx, `
		UPDATE deployments
		SET status = $1, updated_at = now()
		WHERE id = $2
		  AND status IN ($3, $4, $5)
	`, status, id, deployStatusBuilding, deployStatusDeploying, deployStatusHealthy)
	if err != nil {
		return fmt.Errorf("updateStatus: %w", err)
	}
	return nil
}
