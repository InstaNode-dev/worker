package jobs

// orphan_sweep_reconciler.go — the eventually-consistent safety net for
// team/account deletion.
//
// PROBLEM SHAPE
//
// Deleting a team removes state from MANY systems: the platform DB, the
// customer databases the provisioner created, k8s deploy namespaces, DO
// Spaces object-storage prefixes, and the Razorpay subscription. The
// happy-path executor (team_deletion_executor.go) runs all of those in
// order. But a process crash, a transient provisioner outage, or a k8s API
// blip can leave the teardown half-done:
//
//   - a team stuck in 'deletion_pending' whose destruction never finished;
//   - a customer DB / k8s namespace / storage prefix still live even though
//     its owning team is 'tombstoned' or gone;
//   - a Razorpay subscription still active (still CHARGING THE CARD) for a
//     team that is already tombstoned.
//
// The last one is the worst: a deleted customer who keeps getting billed.
//
// WHAT THIS RECONCILER DOES
//
// Every orphanSweepInterval it runs three detection passes:
//
//   PASS 1 — stuck-pending teams. Any team in 'deletion_pending' is a
//            destruction that began and did not finish. The reconciler
//            re-runs the executor's per-team teardown (which is fully
//            idempotent) to completion. On success the team reaches
//            'tombstoned'; on failure it stays 'deletion_pending' and is
//            retried next sweep.
//
//   PASS 2 — orphaned Razorpay subscriptions. Any team that is 'tombstoned'
//            or 'deletion_pending' but still carries a non-empty
//            stripe_customer_id (the legacy column name for the Razorpay
//            subscription id) has a live subscription that must be
//            cancelled NOW. The reconciler cancels it and NULLs the column.
//            This is the "stop the money" backstop.
//
//   PASS 3 — orphaned k8s deploy namespaces. Any instant-deploy-<appID>
//            namespace whose backing deployment row is owned by a
//            'tombstoned' / 'deletion_pending' team — or has no backing row
//            at all — is paid compute with no owner. The reconciler deletes
//            it.
//
//   PASS 4 — orphaned k8s customer namespaces (MR-P0-1b, BugBash 2026-05-20).
//            Every provisioned db/redis/mongo/queue resource gets a dedicated
//            instant-customer-<token> namespace. When the reaper marks a
//            resources row 'deleted' but the backend teardown failed (the
//            MR-P0-1a bug — now fixed, but 188 such namespaces had already
//            leaked in prod, 121 reclaimed by hand), the namespace is left
//            running a live Postgres/Redis pod with no active DB record.
//            PASS 4 lists every instant-customer-* namespace and deletes any
//            whose <token> has NO non-terminal (active/paused/suspended)
//            resources row — i.e. nothing the platform still considers a
//            live resource. This is the durable fix that stops the leak from
//            recurring.
//
// Each reclaimed orphan emits a team.orphan_reclaimed audit row; each orphan
// the reconciler cannot reclaim emits team.orphan_sweep_failed so an
// operator is alerted. Customer-DB / storage-prefix orphans for a cleanly
// deleted team are reclaimed transitively by PASS 1 (the executor's per-team
// path deprovisions every resource and deletes every S3 backup prefix); PASS
// 4 is the backstop for the case PASS 1 cannot see — a namespace whose row is
// already 'deleted' so no per-team teardown will ever revisit it.
//
// IDEMPOTENCY
//
// Every action is idempotent: re-running PASS 1 over a team whose teardown
// already completed is a no-op, cancelling an already-cancelled
// subscription is treated as success, deleting an already-gone namespace is
// a no-op. The reconciler can run as often as we like and on as many pods
// as we like (River's periodic-unique dedupe keeps it single-flight).
//
// FAIL-OPEN POSTURE
//
// A nil executor / razorpay canceler / k8s provider disables the
// corresponding pass with a WARN log — same posture as every other worker.
// A top-level query failure returns an error so River retries the dispatch;
// per-team / per-orphan failures are isolated so one bad orphan never
// stops the sweep.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	"instant.dev/worker/internal/metrics"
)

// orphanSweepInterval is how often the reconciler runs. 15 minutes is a
// deliberate middle ground: fast enough that a deleted customer is not
// charged for a meaningful billing window if the request-time cancel AND
// the nightly executor both somehow missed, slow enough that the cluster-
// wide namespace List + the per-team teardown re-runs are not a load
// concern. The nightly executor remains the primary path; this is the net.
const orphanSweepInterval = 15 * time.Minute

// orphanSweepBatchLimit caps how many stuck-pending teams the reconciler
// processes per run. A healthy steady-state has zero; a backlog (a
// provisioner outage that failed many teardowns) is drained a batch per
// tick rather than all at once.
const orphanSweepBatchLimit = 25

// orphanSweepActor is the audit_log.actor for reconciler-written rows.
const orphanSweepActor = "system"

// Audit kinds — verbatim copies of the api's audit_kinds.go constants.
// The worker and api are separate Go modules (CLAUDE.md), so the shared
// contract is the literal string. team_deletion_audit_kinds.go owns the
// other team.* deletion kinds; these two are the orphan-sweep pair.
const (
	auditKindOrphanReclaimed   = "team.orphan_reclaimed"
	auditKindOrphanSweepFailed = "team.orphan_sweep_failed"
)

// Orphan-kind labels for the audit metadata's orphan_kind field. Named
// constants so the dashboard / NR queries can filter on a stable value.
const (
	orphanKindStuckPendingTeam = "stuck_pending_team"
	orphanKindRazorpaySub      = "razorpay_subscription"
	orphanKindK8sNamespace     = "k8s_namespace"
	orphanKindK8sCustomerNS    = "k8s_customer_namespace"
	orphanKindK8sStackNS       = "k8s_stack_namespace"
	// orphanKindStuckBuild — PASS 6 flips a deployments row that has been
	// 'building' / 'deploying' for >30min with a stuck pod
	// (ImagePullBackOff / ErrImagePull / CrashLoopBackOff) to 'failed'.
	// The autopsy row is captured in deployment_events by the existing
	// deploy_status_reconcile loop on its next tick; PASS 3 reaps the
	// namespace 6h later via orphanReapReasonFailedOldDeployment.
	orphanKindStuckBuild = "stuck_build_deployment"
)

// Prometheus `reason` label values for instant_orphan_sweep_reaped_total.
// Bounded set — every reap action picks exactly one. The literal strings
// are part of the metric contract; do not change without also updating the
// infra/k8s/prometheus-rules.yaml alert that watches `no_db_row`.
const (
	orphanReapReasonTeamTombstoned       = "team_tombstoned"
	orphanReapReasonNoDBRow              = "no_db_row"
	orphanReapReasonFailedOldDeployment  = "failed_old_deployment"
	orphanReapReasonFailedBuild          = "failed_build"
	orphanReapReasonCustomerNoRow        = "customer_no_row"
	orphanReapReasonStackNoRow           = "stack_no_row"
)

// Grace-period thresholds. Conservative on purpose — easier to extend than
// to undo a wrongful reap. All three are pulled into named constants so
// tests can override via the seam in NewOrphanSweepReconciler if needed.
const (
	// orphanNoDBRowGrace is how old an instant-deploy-* / instant-stack-*
	// namespace must be before PASS 3 will reap it on the "no DB row at all"
	// path. Sized to cover a worst-case in-flight provision that has created
	// the namespace but not yet committed the deployments INSERT.
	orphanNoDBRowGrace = 1 * time.Hour

	// orphanFailedDeploymentGrace is how old a deployments row whose status
	// is 'failed' must be before PASS 3 will reap its namespace. The autopsy
	// row in deployment_events persists forever — only the namespace +
	// running pods are reclaimed, not the operator's forensic trail.
	orphanFailedDeploymentGrace = 6 * time.Hour

	// orphanStuckBuildGrace is how long a deployments row in
	// 'building' / 'deploying' status must have been there before PASS 6
	// will inspect the pod state and, on a sustained ImagePullBackOff /
	// CrashLoopBackOff, flip the row to 'failed'.
	orphanStuckBuildGrace = 30 * time.Minute

	// orphanPodStateCheckTimeout caps each per-deployment pod-list call.
	// Keeps a slow k8s API from stalling the whole sweep when many rows
	// qualify for PASS 6 inspection.
	orphanPodStateCheckTimeout = 5 * time.Second

	// orphanStuckBuildBatchLimit caps how many stuck-build candidates
	// PASS 6 inspects per tick. A healthy steady state has zero; a backlog
	// (a ghcr.io outage that wedged many builds at once) is drained over
	// several ticks rather than spamming the k8s API in one burst.
	orphanStuckBuildBatchLimit = 25
)

// customerNamespacePrefix is the prefix of every per-resource customer
// namespace. The provisioner derives namespace = "instant-customer-<token>"
// for each provisioned db/redis/mongo/queue resource. PASS 4 lists by this
// prefix; the token is the namespace name with the prefix stripped.
//
// Kept here (not reusing entitlement_reconciler.go's redisK8sNsPrefix, which
// has the same value but a redis-scoped name) so PASS 4's intent is explicit
// and a future rename of the redis constant cannot silently break the sweep.
const customerNamespacePrefix = "instant-customer-"

// OrphanSubscriptionCanceler is the narrow seam the reconciler uses to
// cancel a still-live Razorpay subscription belonging to an already-
// tombstoned team. Lifted to an interface so tests inject a fake. The
// contract mirrors the api's SubscriptionCanceler: nil = cancelled (or
// nothing to cancel), non-nil = a real failure the reconciler should
// surface as an orphan_sweep_failed audit row.
type OrphanSubscriptionCanceler interface {
	CancelSubscription(ctx context.Context, subscriptionID string) error
}

// K8sNamespaceLister extends K8sNamespaceDeleter with the cluster-wide
// lists the reconciler's PASS 3 (deploy namespaces), PASS 4 (customer
// namespaces), and PASS 5 (stack namespaces — T6 P0-1) need. The concrete
// k8sNamespaceClient satisfies it (see k8s_namespace_client.go). Kept as a
// separate interface so the executor — which only deletes namespaces it
// already knows by name — does not depend on the list capability.
type K8sNamespaceLister interface {
	K8sNamespaceDeleter
	ListDeployNamespaces(ctx context.Context) ([]string, error)
	ListCustomerNamespaces(ctx context.Context) ([]string, error)
	ListStackNamespaces(ctx context.Context) ([]string, error)
	// GetNamespaceAge returns the elapsed time since the namespace was
	// created. The PASS 3 "no DB row" reap path requires this to enforce
	// the 1h grace window — a freshly-created namespace whose deployments
	// INSERT is still in flight must NOT be reaped. NotFound is treated as
	// (0, nil) — the caller skips the reap (already gone).
	GetNamespaceAge(ctx context.Context, namespace string) (time.Duration, error)
}

// PodStateProvider is the narrow slice of the k8s pod API the
// orphan-sweep reconciler's PASS 6 needs: enumerate the pods of a single
// instant-deploy-<appID> namespace and report each pod's waiting-state
// reason. The seam is named separately from K8sNamespaceLister so a
// reconciler test can supply a pod-state fake without also wiring up the
// cluster-wide List interface.
//
// The concrete production impl wraps the same kubernetes.Clientset used by
// k8sNamespaceClient + k8sAutopsyClient (see k8s_namespace_client.go's
// pass6PodStateClient adapter).
type PodStateProvider interface {
	// ListPodWaitingReasons returns, for every pod in `namespace`, the
	// waiting-state reason of its primary container (or "" when the
	// container is not in Waiting state). The slice is empty when no pods
	// match. NotFound on the namespace is (nil, nil) — the namespace was
	// reaped by another path; PASS 6 then no-ops.
	ListPodWaitingReasons(ctx context.Context, namespace string) ([]string, error)
}

// teamTeardownExecutor is the slice of the team-deletion executor the
// reconciler reuses for PASS 1. Implemented by *TeamDeletionExecutorWorker.
// Defined as an interface so the reconciler test can inject a fake that
// records which teams it was asked to finish.
type teamTeardownExecutor interface {
	processTeam(ctx context.Context, c teamPendingDeletion) error
}

// OrphanSweepReconcilerArgs is the periodic-job payload — empty, every run
// is a full sweep.
type OrphanSweepReconcilerArgs struct{}

// Kind implements river.JobArgs.
func (OrphanSweepReconcilerArgs) Kind() string { return "orphan_sweep_reconciler" }

// OrphanSweepReconciler is the River worker.
type OrphanSweepReconciler struct {
	river.WorkerDefaults[OrphanSweepReconcilerArgs]
	db       *sql.DB
	executor teamTeardownExecutor       // nil = PASS 1 skipped
	canceler OrphanSubscriptionCanceler // nil = PASS 2 skipped
	k8s      K8sNamespaceLister         // nil = PASS 3/4/5 skipped
	pods     PodStateProvider           // nil = PASS 6 skipped
}

// NewOrphanSweepReconciler constructs the reconciler. executor, canceler,
// and k8s may each be nil — the corresponding pass is then skipped with a
// WARN log. In production all three are wired in StartWorkers.
//
// PASS 6 (stuck-build detection) is opted in via WithPodStateProvider —
// nil pods leaves PASS 6 disabled with a single WARN log per tick, matching
// the fail-open posture of every other pass.
func NewOrphanSweepReconciler(
	db *sql.DB,
	executor teamTeardownExecutor,
	canceler OrphanSubscriptionCanceler,
	k8s K8sNamespaceLister,
) *OrphanSweepReconciler {
	return &OrphanSweepReconciler{
		db:       db,
		executor: executor,
		canceler: canceler,
		k8s:      k8s,
	}
}

// WithPodStateProvider wires the PASS 6 stuck-build seam. Call this in
// StartWorkers after constructing the reconciler when the cluster is
// reachable. Not called in CI / docker-compose where k8s is absent — PASS
// 6 then logs a single WARN per tick and returns zero orphans.
func (w *OrphanSweepReconciler) WithPodStateProvider(p PodStateProvider) *OrphanSweepReconciler {
	w.pods = p
	return w
}

// Work runs one full sweep — PASS 1, 2, 3 in order. Money first (the stuck-
// pending teardown and the orphaned-subscription cancel both stop billing)
// then compute (namespaces).
func (w *OrphanSweepReconciler) Work(ctx context.Context, job *river.Job[OrphanSweepReconcilerArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.orphan_sweep_reconciler")
	defer span.End()

	start := time.Now()

	pendingFinished, pendingFailed, err := w.sweepStuckPendingTeams(ctx)
	if err != nil {
		return fmt.Errorf("OrphanSweepReconciler: stuck-pending pass failed: %w", err)
	}

	subsCancelled, subsFailed, err := w.sweepOrphanedSubscriptions(ctx)
	if err != nil {
		return fmt.Errorf("OrphanSweepReconciler: subscription pass failed: %w", err)
	}

	// PASS 3 + PASS 4 + PASS 5 + PASS 6 are fail-open: a forbidden/transient
	// k8s error degrades to a single WARN and a zero-orphan result. They must
	// NEVER fail the job — PASS 1 (money) and PASS 2 (money) have already
	// run, and an ERROR here would have River retry the whole dispatch every
	// ~60s, spamming ERROR logs over a missing RBAC permission. None of the
	// k8s passes return a non-nil error.
	nsDeleted, nsFailed := w.sweepOrphanedNamespaces(ctx)
	custNSDeleted, custNSFailed := w.sweepOrphanedCustomerNamespaces(ctx)
	stackNSDeleted, stackNSFailed := w.sweepOrphanedStackNamespaces(ctx)
	stuckBuildFlipped, stuckBuildFailed := w.sweepStuckBuildDeployments(ctx)

	// #146 (BugBash 2026-05-20 idle-tick noise pass): 15min tick = 96
	// lines/day. An all-zero sweep means the cluster is clean — DEBUG.
	// Any non-zero counter (real reclaim OR failure) is operational
	// signal — INFO.
	level := slog.LevelInfo
	if pendingFinished == 0 && pendingFailed == 0 &&
		subsCancelled == 0 && subsFailed == 0 &&
		nsDeleted == 0 && nsFailed == 0 &&
		custNSDeleted == 0 && custNSFailed == 0 &&
		stackNSDeleted == 0 && stackNSFailed == 0 &&
		stuckBuildFlipped == 0 && stuckBuildFailed == 0 {
		level = slog.LevelDebug
	}
	slog.Log(ctx, level, "jobs.orphan_sweep.completed",
		"pending_teams_finished", pendingFinished,
		"pending_teams_failed", pendingFailed,
		"orphan_subscriptions_cancelled", subsCancelled,
		"orphan_subscriptions_failed", subsFailed,
		"orphan_namespaces_deleted", nsDeleted,
		"orphan_namespaces_failed", nsFailed,
		"orphan_customer_namespaces_deleted", custNSDeleted,
		"orphan_customer_namespaces_failed", custNSFailed,
		"orphan_stack_namespaces_deleted", stackNSDeleted,
		"orphan_stack_namespaces_failed", stackNSFailed,
		"stuck_build_flipped_to_failed", stuckBuildFlipped,
		"stuck_build_failed", stuckBuildFailed,
		"duration_ms", time.Since(start).Milliseconds(),
		"job_id", job.ID,
	)
	return nil
}

// ── PASS 1 — stuck-pending teams ─────────────────────────────────────────

// sweepStuckPendingTeams finds teams in 'deletion_pending' (a destruction
// that began and did not finish) and re-runs the executor's idempotent
// per-team teardown to completion.
func (w *OrphanSweepReconciler) sweepStuckPendingTeams(ctx context.Context) (finished, failed int, err error) {
	if w.executor == nil {
		slog.Warn("jobs.orphan_sweep.pass1_skipped_no_executor")
		return 0, 0, nil
	}

	rows, err := w.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT id, COALESCE(deletion_requested_at, now())
		  FROM teams
		 WHERE status = 'deletion_pending'
		 ORDER BY deletion_requested_at ASC NULLS FIRST
		 LIMIT %d
	`, orphanSweepBatchLimit))
	if err != nil {
		return 0, 0, err
	}
	var candidates []teamPendingDeletion
	for rows.Next() {
		var c teamPendingDeletion
		if scanErr := rows.Scan(&c.teamID, &c.deletionRequestedAt); scanErr != nil {
			_ = rows.Close()
			return 0, 0, fmt.Errorf("scan stuck-pending team: %w", scanErr)
		}
		candidates = append(candidates, c)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return 0, 0, err
	}

	for _, c := range candidates {
		if perr := w.executor.processTeam(ctx, c); perr != nil {
			failed++
			slog.Error("jobs.orphan_sweep.pending_team_retry_failed",
				"team_id", c.teamID.String(), "error", perr)
			w.emitOrphanSweepFailed(ctx, c.teamID, orphanKindStuckPendingTeam,
				c.teamID.String(), perr)
			continue
		}
		finished++
		w.emitOrphanReclaimed(ctx, c.teamID, orphanKindStuckPendingTeam,
			c.teamID.String(), "completed stalled team teardown")
	}
	return finished, failed, nil
}

// ── PASS 2 — orphaned Razorpay subscriptions ─────────────────────────────

// orphanedSub is the projection of a tombstoned/pending team that still
// carries a live subscription id.
type orphanedSub struct {
	teamID         uuid.UUID
	subscriptionID string
}

// sweepOrphanedSubscriptions finds teams that are 'tombstoned' or
// 'deletion_pending' yet still have a non-empty stripe_customer_id (the
// Razorpay subscription id), cancels each subscription, and NULLs the
// column so the next sweep does not re-process it.
//
// This is the "stop the money" backstop: even if the request-time cancel
// and the executor's tombstone both somehow left a live subscription, this
// pass catches it within one orphanSweepInterval.
func (w *OrphanSweepReconciler) sweepOrphanedSubscriptions(ctx context.Context) (cancelled, failed int, err error) {
	if w.canceler == nil {
		slog.Warn("jobs.orphan_sweep.pass2_skipped_no_canceler")
		return 0, 0, nil
	}

	rows, err := w.db.QueryContext(ctx, `
		SELECT id, stripe_customer_id
		  FROM teams
		 WHERE status IN ('tombstoned', 'deletion_pending')
		   AND stripe_customer_id IS NOT NULL
		   AND stripe_customer_id != ''
	`)
	if err != nil {
		return 0, 0, err
	}
	var orphans []orphanedSub
	for rows.Next() {
		var o orphanedSub
		if scanErr := rows.Scan(&o.teamID, &o.subscriptionID); scanErr != nil {
			_ = rows.Close()
			return 0, 0, fmt.Errorf("scan orphaned sub: %w", scanErr)
		}
		orphans = append(orphans, o)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return 0, 0, err
	}

	for _, o := range orphans {
		if cerr := w.canceler.CancelSubscription(ctx, o.subscriptionID); cerr != nil {
			failed++
			slog.Error("jobs.orphan_sweep.subscription_cancel_failed",
				"team_id", o.teamID.String(),
				"subscription_id", o.subscriptionID,
				"error", cerr)
			w.emitOrphanSweepFailed(ctx, o.teamID, orphanKindRazorpaySub,
				o.subscriptionID, cerr)
			continue
		}
		// Cancel succeeded — NULL the column so the orphan is not
		// re-detected. A failure here only means the next sweep retries
		// the (idempotent) cancel, so it is logged, not fatal.
		if _, uerr := w.db.ExecContext(ctx, `
			UPDATE teams SET stripe_customer_id = NULL WHERE id = $1
		`, o.teamID); uerr != nil {
			slog.Warn("jobs.orphan_sweep.subscription_column_clear_failed",
				"team_id", o.teamID.String(), "error", uerr)
		}
		cancelled++
		w.emitOrphanReclaimed(ctx, o.teamID, orphanKindRazorpaySub,
			o.subscriptionID, "cancelled orphaned Razorpay subscription")
	}
	return cancelled, failed, nil
}

// ── PASS 3 — orphaned k8s deploy namespaces ──────────────────────────────

// deployRowSnapshot is the per-app_id projection PASS 3 reads from the
// deployments table — enough state for the sweep to decide between
// "live", "team_tombstoned", "failed_old_deployment", and "no_db_row"
// without re-querying per namespace.
type deployRowSnapshot struct {
	status       string    // deployments.status
	teamStatus   string    // teams.status
	rowCreatedAt time.Time // deployments.created_at — gates the failed_old_deployment reap
}

// sweepOrphanedNamespaces lists every instant-deploy-* namespace and deletes
// the ones whose backing deployment row is owned by a tombstoned /
// deletion_pending team, or has no backing row at all, OR is in
// status='failed' beyond the 6h grace window.
//
// REAP REASONS (every reap increments instant_orphan_sweep_reaped_total
// with one of these labels; ops alerts watch the per-reason rate):
//
//   - team_tombstoned       — the namespace's row exists but its team is
//                             tombstoned / deletion_pending, OR the row's
//                             status is 'deleted'. Reaped immediately —
//                             the team-deletion executor was supposed to
//                             have caught it.
//   - failed_old_deployment — the row exists with status='failed' AND
//                             created_at > 6h ago. The autopsy in
//                             deployment_events persists forever; the
//                             namespace doesn't need to linger.
//   - no_db_row             — the namespace has NO matching deployments
//                             row at all AND the namespace itself is
//                             >1h old (the grace window protects in-flight
//                             provisions where the namespace is created
//                             before the DB INSERT lands). This is the
//                             P0-3 atomic-provision leading indicator —
//                             non-zero rate pages an operator.
//
// FAIL-OPEN. This pass never returns an error. The namespace List is a
// cluster-scoped k8s call that can be denied (RBAC Forbidden) or fail
// transiently (API outage). When that happens the pass logs exactly one
// structured WARN and returns a zero-orphan result so the overall job
// still succeeds — PASS 1 and PASS 2 (the money-stopping passes) have
// already run, and surfacing an error here would have River retry the
// whole dispatch every ~60s, spamming ERROR logs over a missing
// permission. A namespace orphan that goes un-reclaimed for one interval
// is harmless; an ERROR-spamming retry loop is not.
func (w *OrphanSweepReconciler) sweepOrphanedNamespaces(ctx context.Context) (deleted, failed int) {
	if w.k8s == nil {
		slog.Warn("jobs.orphan_sweep.pass3_skipped_no_k8s")
		return 0, 0
	}

	namespaces, err := w.k8s.ListDeployNamespaces(ctx)
	if err != nil {
		// Forbidden (missing RBAC) or any transient k8s error: degrade
		// gracefully. One WARN, no error — the job still succeeds.
		slog.Warn("jobs.orphan_sweep.pass3_namespace_list_failed",
			"error", err.Error(),
			"detail", "namespace-orphan cleanup skipped this sweep; "+
				"PASS 1/2 still ran; check instant-worker RBAC for cluster-scoped namespaces list")
		return 0, 0
	}

	// Read every app_id's row state in one query. The sweep iterates
	// namespaces and looks up the row by app_id — three buckets fall out:
	// live (left alone), reapable-now (team_tombstoned + failed_old),
	// no-row (grace-checked via namespace CreationTimestamp).
	rowsByAppID, err := w.fetchDeployRowsByAppID(ctx)
	if err != nil {
		// Same fail-open posture: a DB blip must not fail the job.
		slog.Warn("jobs.orphan_sweep.pass3_live_app_ids_failed",
			"error", err.Error(),
			"detail", "namespace-orphan cleanup skipped this sweep; PASS 1/2 still ran")
		return 0, 0
	}

	now := time.Now()
	for _, ns := range namespaces {
		appID := ns[len(deployNamespacePrefixTDE):]
		if appID == "" {
			continue
		}
		reason, ok := classifyDeployOrphan(appID, rowsByAppID, now)
		if !ok {
			// Either the row is legitimately live, OR the row's status is
			// 'failed' but newer than the 6h grace window. Leave it.
			continue
		}
		// Reason "no_db_row" needs the namespace-age grace check before
		// we delete. Every other reason is reaped immediately.
		if reason == orphanReapReasonNoDBRow {
			age, ageErr := w.k8s.GetNamespaceAge(ctx, ns)
			if ageErr != nil {
				// Treat any age-lookup failure as "skip this tick" rather
				// than reap-without-grace. The namespace is re-evaluated
				// on the next sweep.
				slog.Warn("jobs.orphan_sweep.namespace_age_lookup_failed",
					"namespace", ns, "error", ageErr.Error(),
					"detail", "skipping reap this sweep; will retry next interval")
				continue
			}
			if age < orphanNoDBRowGrace {
				slog.Debug("jobs.orphan_sweep.no_db_row_within_grace",
					"namespace", ns, "age", age.String(),
					"grace", orphanNoDBRowGrace.String())
				continue
			}
		}

		// Log the proposed action with full evidence BEFORE the delete
		// (constraint #3 from the brief: operator must be able to see what
		// is about to happen in audit_log).
		evidence := buildPass3Evidence(appID, rowsByAppID, reason)
		slog.Info("jobs.orphan_sweep.proposed_reap",
			"namespace", ns,
			"reason", reason,
			"evidence", evidence,
			"action", "deleting namespace + emitting team.orphan_reclaimed",
		)

		if delErr := w.k8s.DeleteNamespace(ctx, ns); delErr != nil {
			failed++
			metrics.OrphanSweepReapFailedTotal.WithLabelValues(reason).Inc()
			slog.Error("jobs.orphan_sweep.namespace_delete_failed",
				"namespace", ns, "reason", reason, "error", delErr)
			w.emitOrphanSweepFailed(ctx, uuid.Nil, orphanKindK8sNamespace, ns, delErr)
			continue
		}
		deleted++
		metrics.OrphanSweepReapedTotal.WithLabelValues(reason).Inc()
		w.emitOrphanReclaimed(ctx, uuid.Nil, orphanKindK8sNamespace, ns,
			"deleted orphaned k8s deploy namespace (reason="+reason+")")
	}
	return deleted, failed
}

// classifyDeployOrphan decides whether (and why) an instant-deploy-<appID>
// namespace is an orphan. Returns (reason, true) if the namespace is
// reapable, (_, false) if it is legitimately live or still inside the
// failed-grace window.
//
// The three reapable reasons map 1:1 to the Prometheus metric labels —
// op alerts in infra/k8s/prometheus-rules.yaml watch each independently.
// Kept as a pure function so the test suite can drive it directly with
// table inputs without standing up the full sweep.
func classifyDeployOrphan(appID string, rowsByAppID map[string]deployRowSnapshot, now time.Time) (string, bool) {
	row, present := rowsByAppID[appID]
	if !present {
		// No deployments row at all. Reap reason is no_db_row — caller
		// applies the namespace-age grace before deleting.
		return orphanReapReasonNoDBRow, true
	}
	// Row exists. Two reapable sub-cases:
	//   1. team_tombstoned: team in a terminal/pending-delete state, OR
	//      the row itself is already status='deleted'.
	//   2. failed_old_deployment: row is status='failed' AND old enough.
	teamGone := row.teamStatus != "active" && row.teamStatus != "deletion_requested"
	rowDeleted := row.status == "deleted"
	if teamGone || rowDeleted {
		return orphanReapReasonTeamTombstoned, true
	}
	if row.status == "failed" && !row.rowCreatedAt.IsZero() &&
		now.Sub(row.rowCreatedAt) >= orphanFailedDeploymentGrace {
		return orphanReapReasonFailedOldDeployment, true
	}
	// Live OR failed-but-within-grace. Leave the namespace alone.
	return "", false
}

// buildPass3Evidence assembles the structured-log evidence block for a
// proposed reap. Includes the deployments row state when present, "absent"
// when no row was found. Operators trace this back via NR Logs to the
// orphan_reclaimed audit row.
func buildPass3Evidence(appID string, rowsByAppID map[string]deployRowSnapshot, reason string) map[string]any {
	row, present := rowsByAppID[appID]
	if !present {
		return map[string]any{
			"app_id":         appID,
			"reason":         reason,
			"db_row_present": false,
		}
	}
	return map[string]any{
		"app_id":             appID,
		"reason":             reason,
		"db_row_present":     true,
		"db_status":          row.status,
		"team_status":        row.teamStatus,
		"db_row_created_at":  row.rowCreatedAt.Format(time.RFC3339),
		"db_row_age_seconds": int64(time.Since(row.rowCreatedAt).Seconds()),
	}
}

// fetchDeployRowsByAppID returns every deployments row's app_id mapped to
// (status, team_status, created_at) — the projection PASS 3 needs.
//
// Crucially this does NOT pre-filter to "active rows" the way the old
// fetchLiveDeployAppIDs did: PASS 3 must see deleted rows to detect that
// the namespace should have been torn down (team_tombstoned reap path).
// The classification happens in classifyDeployOrphan.
func (w *OrphanSweepReconciler) fetchDeployRowsByAppID(ctx context.Context) (map[string]deployRowSnapshot, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT d.app_id, d.status, t.status, d.created_at
		  FROM deployments d
		  JOIN teams t ON t.id = d.team_id
		 WHERE d.app_id IS NOT NULL
		   AND d.app_id != ''
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]deployRowSnapshot)
	for rows.Next() {
		var (
			appID      string
			dStatus    string
			teamStatus string
			createdAt  time.Time
		)
		if scanErr := rows.Scan(&appID, &dStatus, &teamStatus, &createdAt); scanErr != nil {
			return nil, scanErr
		}
		out[appID] = deployRowSnapshot{
			status:       dStatus,
			teamStatus:   teamStatus,
			rowCreatedAt: createdAt,
		}
	}
	return out, rows.Err()
}

// ── PASS 4 — orphaned k8s customer namespaces (MR-P0-1b) ─────────────────

// sweepOrphanedCustomerNamespaces lists every instant-customer-<token>
// namespace and deletes the ones whose <token> has NO non-terminal
// (active/paused/suspended) resources row — i.e. nothing the platform still
// considers a live resource.
//
// THE LEAK THIS CLOSES. Every db/redis/mongo/queue resource gets a dedicated
// instant-customer-<token> namespace. The reaper used to mark a resources
// row 'deleted' even when the backend teardown FAILED (MR-P0-1a); a 'deleted'
// row is terminal and invisible to PASS 1's per-team path, so the namespace —
// still running a live Postgres/Redis pod — was orphaned forever. 188 such
// namespaces leaked in prod. MR-P0-1a stops new ones; this pass reclaims any
// that slip through (and is the recurrence guard).
//
// SAFETY. A namespace is deleted ONLY when its token has no active /paused/
// suspended row. A token that still has such a row is a live resource — left
// alone. The "no backing row at all" case is also an orphan: a namespace
// whose row was hard-deleted or never existed is paid compute with no owner.
//
// FAIL-OPEN. Identical posture to PASS 3: a namespace List failure or a DB
// blip on the live-token query degrades to one WARN and a zero-orphan result.
// The pass never returns an error — PASS 1/2/3 have already run.
func (w *OrphanSweepReconciler) sweepOrphanedCustomerNamespaces(ctx context.Context) (deleted, failed int) {
	if w.k8s == nil {
		slog.Warn("jobs.orphan_sweep.pass4_skipped_no_k8s")
		return 0, 0
	}

	namespaces, err := w.k8s.ListCustomerNamespaces(ctx)
	if err != nil {
		slog.Warn("jobs.orphan_sweep.pass4_namespace_list_failed",
			"error", err.Error(),
			"detail", "customer-namespace orphan cleanup skipped this sweep; "+
				"PASS 1/2/3 still ran; check instant-worker RBAC for cluster-scoped namespaces list")
		return 0, 0
	}
	if len(namespaces) == 0 {
		return 0, 0
	}

	liveTokens, err := w.fetchLiveResourceTokens(ctx)
	if err != nil {
		// A DB blip on the live-token query must NOT cause a delete decision
		// off an empty set — that would tear down every customer namespace.
		// Skip the pass this sweep.
		slog.Warn("jobs.orphan_sweep.pass4_live_tokens_failed",
			"error", err.Error(),
			"detail", "customer-namespace orphan cleanup skipped this sweep; PASS 1/2/3 still ran")
		return 0, 0
	}

	for _, ns := range namespaces {
		token := ns[len(customerNamespacePrefix):]
		if token == "" {
			continue
		}
		if liveTokens[token] {
			continue // a live resource still backs this namespace — leave it
		}
		// Orphan: no active/paused/suspended resources row for this token.
		if delErr := w.k8s.DeleteNamespace(ctx, ns); delErr != nil {
			failed++
			metrics.OrphanSweepReapFailedTotal.WithLabelValues(orphanReapReasonCustomerNoRow).Inc()
			slog.Error("jobs.orphan_sweep.customer_namespace_delete_failed",
				"namespace", ns, "error", delErr)
			w.emitOrphanSweepFailed(ctx, uuid.Nil, orphanKindK8sCustomerNS, ns, delErr)
			continue
		}
		deleted++
		metrics.OrphanSweepReapedTotal.WithLabelValues(orphanReapReasonCustomerNoRow).Inc()
		slog.Info("jobs.orphan_sweep.customer_namespace_reclaimed",
			"namespace", ns,
			"detail", "instant-customer-* namespace with no active resources row deleted (MR-P0-1b)")
		w.emitOrphanReclaimed(ctx, uuid.Nil, orphanKindK8sCustomerNS, ns,
			"deleted orphaned k8s customer namespace (no backing active resource)")
	}
	return deleted, failed
}

// fetchLiveResourceTokens returns the set of resource tokens that still have
// a non-terminal (active / paused / suspended) row in the resources table —
// i.e. every token PASS 4 must NOT reclaim the namespace for.
//
// Crucially this does NOT include 'deleted' or 'expired' (terminal) rows: a
// terminal row's backend is expected to be torn down, so its namespace, if
// still present, IS an orphan. The token column is cast to text so the
// in-Go namespace-suffix comparison is exact.
func (w *OrphanSweepReconciler) fetchLiveResourceTokens(ctx context.Context) (map[string]bool, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT DISTINCT token::text
		  FROM resources
		 WHERE status IN ('active', 'paused', 'suspended')
		   AND token IS NOT NULL
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]bool)
	for rows.Next() {
		var token string
		if scanErr := rows.Scan(&token); scanErr != nil {
			return nil, scanErr
		}
		if token != "" {
			out[token] = true
		}
	}
	return out, rows.Err()
}

// ── PASS 5 — orphaned k8s stack namespaces (T6 P0-1) ─────────────────────

// sweepOrphanedStackNamespaces lists every instant-stack-<id> namespace and
// deletes the ones whose backing `stacks` row is gone — the durable fix for
// the T6 P0-1 leak (BugBash 2026-05-20).
//
// THE LEAK THIS CLOSES. The pre-fix ExpireStacksWorker carried nsPrefix
// "instant-apps-" (derived from cfg.KubeNamespaceApps), but real stack
// namespaces are "instant-stack-<id>". The safety guard in
// deleteK8sNamespace refused every real stack namespace (returning
// nil-success), and ExpireStacksWorker then DELETE'd the `stacks` row,
// leaving the namespace + pods + service + ingress + TLS cert running
// indefinitely with NO DB pointer ever again. PASS 5 is the recurrence
// guard plus the catch-up sweep for pre-fix orphans.
//
// SAFETY. A namespace is deleted ONLY when its <id> has no row in the
// `stacks` table (the row was hard-deleted by the buggy expirer). A
// namespace whose row is still present — even in terminal status — is left
// alone so the per-stack teardown path stays in charge of it.
//
// FAIL-OPEN. Identical to PASS 3/4: a namespace List failure or a DB blip
// on the live-stack-ids query degrades to one WARN and a zero-orphan
// result. The pass never returns an error — PASS 1/2/3/4 have already run.
func (w *OrphanSweepReconciler) sweepOrphanedStackNamespaces(ctx context.Context) (deleted, failed int) {
	if w.k8s == nil {
		slog.Warn("jobs.orphan_sweep.pass5_skipped_no_k8s")
		return 0, 0
	}

	namespaces, err := w.k8s.ListStackNamespaces(ctx)
	if err != nil {
		slog.Warn("jobs.orphan_sweep.pass5_namespace_list_failed",
			"error", err.Error(),
			"detail", "stack-namespace orphan cleanup skipped this sweep; "+
				"PASS 1/2/3/4 still ran; check instant-worker RBAC for cluster-scoped namespaces list")
		return 0, 0
	}
	if len(namespaces) == 0 {
		return 0, 0
	}

	liveStackIDs, err := w.fetchLiveStackIDs(ctx)
	if err != nil {
		// A DB blip on the live-stack-ids query must NOT cause a delete
		// decision off an empty set — that would tear down every stack
		// namespace. Skip the pass this sweep.
		slog.Warn("jobs.orphan_sweep.pass5_live_stack_ids_failed",
			"error", err.Error(),
			"detail", "stack-namespace orphan cleanup skipped this sweep; PASS 1/2/3/4 still ran")
		return 0, 0
	}

	for _, ns := range namespaces {
		// The id portion is everything after the "instant-stack-" prefix.
		// Stack IDs are UUIDs; we don't parse here — the in-Go string
		// comparison against the live-ids set is exact.
		stackID := ns[len(ExpireStacksNamespacePrefix):]
		if stackID == "" {
			continue
		}
		if liveStackIDs[stackID] {
			continue // a row still owns this namespace — leave it
		}
		// Orphan: no stacks row for this id.
		if delErr := w.k8s.DeleteNamespace(ctx, ns); delErr != nil {
			failed++
			metrics.OrphanSweepReapFailedTotal.WithLabelValues(orphanReapReasonStackNoRow).Inc()
			slog.Error("jobs.orphan_sweep.stack_namespace_delete_failed",
				"namespace", ns, "error", delErr)
			w.emitOrphanSweepFailed(ctx, uuid.Nil, orphanKindK8sStackNS, ns, delErr)
			continue
		}
		deleted++
		metrics.OrphanSweepReapedTotal.WithLabelValues(orphanReapReasonStackNoRow).Inc()
		slog.Info("jobs.orphan_sweep.stack_namespace_reclaimed",
			"namespace", ns,
			"detail", "instant-stack-* namespace with no backing stacks row deleted (T6 P0-1)")
		w.emitOrphanReclaimed(ctx, uuid.Nil, orphanKindK8sStackNS, ns,
			"deleted orphaned k8s stack namespace (no backing stacks row)")
	}
	return deleted, failed
}

// fetchLiveStackIDs returns the set of stack ids that still have a row in
// the `stacks` table. Note: unlike PASS 4 (resources), we do NOT filter on
// status — even a terminal-status stacks row pins its namespace so the
// per-stack teardown path owns the delete. The pass is a strict "no row at
// all = orphan" sweep.
func (w *OrphanSweepReconciler) fetchLiveStackIDs(ctx context.Context) (map[string]bool, error) {
	rows, err := w.db.QueryContext(ctx, `SELECT id::text FROM stacks`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]bool)
	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr != nil {
			return nil, scanErr
		}
		if id != "" {
			out[id] = true
		}
	}
	return out, rows.Err()
}

// ── PASS 6 — stuck-build detection (2026-05-20) ──────────────────────────
//
// PASS 6 catches deployments that are stuck in 'building' or 'deploying'
// because the pod cannot pull its image (ImagePullBackOff / ErrImagePull)
// or its container keeps crashing (CrashLoopBackOff). The existing
// deploy_status_reconcile only flips a row to 'failed' when the k8s
// Deployment has DeploymentReplicaFailure=True; a stuck pod never trips
// that condition — UnavailableReplicas stays positive and the row sits at
// 'deploying' forever.
//
// THE SHAPE PASS 6 CATCHES (verbatim case 04dc0b31 from 2026-05-20):
//
//   deployments.status   = 'deploying'
//   deployments.updated_at = >30min ago
//   pod's only container is in Waiting with reason=ImagePullBackOff
//
// THE ACTION
//
//   1. Flip deployments.status to 'failed' (so PASS 3 reaps the namespace
//      6h later via orphanReapReasonFailedOldDeployment).
//   2. Set deployments.error_message describing the stuck reason.
//   3. Emit a structured log with full evidence (audit_log lookup is via
//      the deployment_id in the next reconcile tick).
//   4. Increment instant_orphan_sweep_reaped_total{reason="failed_build"}.
//
// THE FAILURE-AUTOPSY ROW IS NOT WRITTEN HERE. The capture path is shared
// with deploy_status_reconcile.go's captureDeploymentAutopsy; running it
// here would require also wiring the deployAutopsyK8sProvider into the
// orphan-sweep reconciler. Instead PASS 6 flips the status; the next
// deploy_status_reconcile tick (~30s later) sees the row at 'failed' and
// writes the autopsy via its existing per-tick capture loop. One source
// of truth for the autopsy row, two reconcilers cooperating.
//
// FAIL-OPEN. Identical posture to PASS 3/4/5: a pods-list failure on one
// namespace skips that candidate, never fails the job. A DB blip on the
// candidate query degrades to one WARN and a zero-flipped result.
//
// SAFETY. The query joins on `teams` to refuse any row whose team is NOT
// 'active' or 'deletion_requested' — a tombstoned team's deployments are
// PASS 3's territory.
func (w *OrphanSweepReconciler) sweepStuckBuildDeployments(ctx context.Context) (flipped, failed int) {
	if w.pods == nil {
		// Not wired — silent in steady state. The structured WARN comes
		// from StartWorkers once at boot when the seam stays nil.
		return 0, 0
	}

	candidates, err := w.fetchStuckBuildCandidates(ctx)
	if err != nil {
		slog.Warn("jobs.orphan_sweep.pass6_candidates_query_failed",
			"error", err.Error(),
			"detail", "stuck-build sweep skipped this tick; PASS 1/2/3/4/5 still ran")
		return 0, 0
	}
	if len(candidates) == 0 {
		return 0, 0
	}

	for _, c := range candidates {
		ns := deployNamespacePrefixTDE + c.appID
		reasons, lerr := w.listPodWaitingReasonsWithTimeout(ctx, ns)
		if lerr != nil {
			// Per-namespace failure is isolated — one WARN, no error.
			slog.Warn("jobs.orphan_sweep.pass6_pod_list_failed",
				"namespace", ns,
				"deployment_id", c.deploymentID.String(),
				"error", lerr.Error())
			continue
		}
		// Decide: every running pod must be in a stuck-waiting reason for
		// us to flip. A single pod that is "" (Running / ContainerCreating
		// without a Waiting state) means the build is still progressing
		// or has just turned over — leave the row alone.
		if !isStuckBuildState(reasons) {
			continue
		}
		stuckReason := dominantStuckReason(reasons)

		// Log the proposed action with full evidence BEFORE the write.
		slog.Info("jobs.orphan_sweep.pass6_proposed_flip",
			"namespace", ns,
			"deployment_id", c.deploymentID.String(),
			"app_id", c.appID,
			"current_status", c.status,
			"stuck_reason", stuckReason,
			"pod_waiting_reasons", reasons,
			"row_updated_at", c.updatedAt.Format(time.RFC3339),
			"action", "flipping deployments.status to 'failed' + setting error_message",
		)

		errMsg := fmt.Sprintf("orphan_sweep PASS 6: pod stuck in %s for >%s; flipped to failed for namespace reap",
			stuckReason, orphanStuckBuildGrace.String())
		if uerr := w.flipDeploymentToFailed(ctx, c.deploymentID, errMsg); uerr != nil {
			failed++
			metrics.OrphanSweepReapFailedTotal.WithLabelValues(orphanReapReasonFailedBuild).Inc()
			slog.Error("jobs.orphan_sweep.pass6_flip_failed",
				"deployment_id", c.deploymentID.String(),
				"namespace", ns,
				"error", uerr)
			// No audit row — teamID lookup is on c.teamID below.
			w.emitOrphanSweepFailed(ctx, c.teamID, orphanKindStuckBuild,
				c.deploymentID.String(), uerr)
			continue
		}
		flipped++
		metrics.OrphanSweepReapedTotal.WithLabelValues(orphanReapReasonFailedBuild).Inc()
		w.emitOrphanReclaimed(ctx, c.teamID, orphanKindStuckBuild,
			c.deploymentID.String(),
			"flipped stuck-build deployment to failed (reason="+stuckReason+")")
	}
	return flipped, failed
}

// stuckBuildCandidate is the projection PASS 6 reads — minimal columns
// from the deployments + teams join.
type stuckBuildCandidate struct {
	deploymentID uuid.UUID
	teamID       uuid.UUID
	appID        string
	status       string
	updatedAt    time.Time
}

// fetchStuckBuildCandidates returns every deployments row in
// status='building'/'deploying' whose updated_at is older than
// orphanStuckBuildGrace AND whose team is still 'active' or in the
// restorable 'deletion_requested' grace window. Capped by
// orphanStuckBuildBatchLimit so a backlog (e.g. a multi-hour ghcr.io
// outage that wedged hundreds of builds) is drained across ticks rather
// than all at once.
func (w *OrphanSweepReconciler) fetchStuckBuildCandidates(ctx context.Context) ([]stuckBuildCandidate, error) {
	cutoff := time.Now().Add(-orphanStuckBuildGrace)
	rows, err := w.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT d.id, d.team_id, d.app_id, d.status, d.updated_at
		  FROM deployments d
		  JOIN teams t ON t.id = d.team_id
		 WHERE d.status IN ('building', 'deploying')
		   AND d.updated_at < $1
		   AND d.app_id IS NOT NULL AND d.app_id != ''
		   AND t.status IN ('active', 'deletion_requested')
		 ORDER BY d.updated_at ASC
		 LIMIT %d
	`, orphanStuckBuildBatchLimit), cutoff)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []stuckBuildCandidate
	for rows.Next() {
		var c stuckBuildCandidate
		if scanErr := rows.Scan(&c.deploymentID, &c.teamID, &c.appID, &c.status, &c.updatedAt); scanErr != nil {
			return nil, scanErr
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// flipDeploymentToFailed updates a deployments row to status='failed' +
// sets error_message, but ONLY when the row is still in a non-terminal
// build state. The guard prevents a race with deploy_status_reconcile
// that may have already moved the row to 'healthy' or 'stopped' between
// our SELECT and UPDATE.
func (w *OrphanSweepReconciler) flipDeploymentToFailed(ctx context.Context, deploymentID uuid.UUID, errMsg string) error {
	_, err := w.db.ExecContext(ctx, `
		UPDATE deployments
		   SET status = 'failed',
		       error_message = $1,
		       updated_at = now()
		 WHERE id = $2
		   AND status IN ('building', 'deploying')
	`, errMsg, deploymentID)
	if err != nil {
		return fmt.Errorf("flipDeploymentToFailed: %w", err)
	}
	return nil
}

// listPodWaitingReasonsWithTimeout calls the seam under a per-namespace
// timeout so one slow k8s API call cannot stall the whole sweep.
func (w *OrphanSweepReconciler) listPodWaitingReasonsWithTimeout(ctx context.Context, namespace string) ([]string, error) {
	cctx, cancel := context.WithTimeout(ctx, orphanPodStateCheckTimeout)
	defer cancel()
	return w.pods.ListPodWaitingReasons(cctx, namespace)
}

// stuckBuildWaitingReasons is the bounded set of waiting-state reasons PASS 6
// treats as evidence of a stuck build. Anything outside this set leaves the
// row alone — including transient states like ContainerCreating /
// PodInitializing that resolve themselves.
//
// `ErrImagePull` is the precursor to `ImagePullBackOff`; both appear in
// real cluster traces depending on which kubelet sample lands first. We
// accept either as the same failure mode.
var stuckBuildWaitingReasons = map[string]bool{
	"ImagePullBackOff":  true,
	"ErrImagePull":      true,
	"CrashLoopBackOff":  true,
}

// isStuckBuildState reports whether ALL container waiting reasons are in
// the stuckBuildWaitingReasons set. An empty input (no pods at all) is
// treated as not-stuck — the rollout may have just started and pods
// haven't appeared yet, OR the namespace was reaped by another path
// between PASS 3 and PASS 6. A "" reason in any slot is treated as
// progressing (the pod is Running or in ContainerCreating without a
// Waiting state).
func isStuckBuildState(reasons []string) bool {
	if len(reasons) == 0 {
		return false
	}
	for _, r := range reasons {
		if !stuckBuildWaitingReasons[r] {
			return false
		}
	}
	return true
}

// dominantStuckReason picks the most-frequent reason for the structured
// log + error_message. Stable order on ties (first observed wins) so log
// messages are deterministic across re-runs against the same pod set.
func dominantStuckReason(reasons []string) string {
	counts := make(map[string]int)
	order := make([]string, 0, len(reasons))
	for _, r := range reasons {
		if _, seen := counts[r]; !seen {
			order = append(order, r)
		}
		counts[r]++
	}
	best := ""
	bestCount := 0
	for _, r := range order {
		if counts[r] > bestCount {
			best = r
			bestCount = counts[r]
		}
	}
	return best
}

// ── audit emitters ───────────────────────────────────────────────────────

// emitOrphanReclaimed writes a team.orphan_reclaimed audit row. Best-effort:
// a failed insert is logged but never blocks the sweep. teamID may be
// uuid.Nil for cluster-scoped orphans (a namespace with no DB row) — we
// then write the row with a NULL team_id is not allowed by the schema, so
// for Nil we skip the team-scoped insert and rely on the structured log.
func (w *OrphanSweepReconciler) emitOrphanReclaimed(ctx context.Context, teamID uuid.UUID, orphanKind, identifier, action string) {
	meta := map[string]any{
		"orphan_kind": orphanKind,
		"identifier":  identifier,
		"action":      action,
	}
	w.emitOrphanAudit(ctx, teamID, auditKindOrphanReclaimed,
		"orphan reclaimed: "+orphanKind+" — "+action, meta)
}

// emitOrphanSweepFailed writes a team.orphan_sweep_failed audit row — the
// operator alert for an orphan the reconciler could not reclaim.
func (w *OrphanSweepReconciler) emitOrphanSweepFailed(ctx context.Context, teamID uuid.UUID, orphanKind, identifier string, cause error) {
	meta := map[string]any{
		"orphan_kind": orphanKind,
		"identifier":  identifier,
		"error":       cause.Error(),
	}
	w.emitOrphanAudit(ctx, teamID, auditKindOrphanSweepFailed,
		"orphan sweep failed: "+orphanKind+" — operator investigation required", meta)
}

// emitOrphanAudit is the shared INSERT. audit_log.team_id is NOT NULL in
// the schema, so a cluster-scoped orphan (teamID == uuid.Nil — a namespace
// with no backing DB row) cannot be written as a team-scoped row; for that
// case we emit only the structured log and skip the insert. Every team-
// scoped orphan still gets its audit row.
func (w *OrphanSweepReconciler) emitOrphanAudit(ctx context.Context, teamID uuid.UUID, kind, summary string, meta map[string]any) {
	if teamID == uuid.Nil {
		slog.Info("jobs.orphan_sweep.cluster_scoped_event",
			"kind", kind, "summary", summary, "meta", meta)
		return
	}
	metaBytes, _ := json.Marshal(meta)
	if _, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES ($1, $2, $3, $4, $5)
	`, teamID, orphanSweepActor, kind, summary, metaBytes); err != nil {
		slog.Warn("jobs.orphan_sweep.audit_insert_failed",
			"team_id", teamID.String(), "kind", kind, "error", err)
	}
}

// compile-time assertion: the executor satisfies the teardown seam.
var _ teamTeardownExecutor = (*TeamDeletionExecutorWorker)(nil)
