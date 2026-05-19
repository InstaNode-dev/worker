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
// lists the reconciler's PASS 3 (deploy namespaces) and PASS 4 (customer
// namespaces) need. The concrete k8sNamespaceClient satisfies it (see
// k8s_namespace_client.go). Kept as a separate interface so the executor —
// which only deletes namespaces it already knows by name — does not depend
// on the list capability.
type K8sNamespaceLister interface {
	K8sNamespaceDeleter
	ListDeployNamespaces(ctx context.Context) ([]string, error)
	ListCustomerNamespaces(ctx context.Context) ([]string, error)
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
	k8s      K8sNamespaceLister         // nil = PASS 3 skipped
}

// NewOrphanSweepReconciler constructs the reconciler. executor, canceler,
// and k8s may each be nil — the corresponding pass is then skipped with a
// WARN log. In production all three are wired in StartWorkers.
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

	// PASS 3 + PASS 4 are fail-open: a forbidden/transient k8s error degrades
	// to a single WARN and a zero-orphan result. They must NEVER fail the job
	// — PASS 1 (money) and PASS 2 (money) have already run, and an ERROR here
	// would have River retry the whole dispatch every ~60s, spamming ERROR
	// logs over a missing RBAC permission. Neither sweep returns a non-nil
	// error.
	nsDeleted, nsFailed := w.sweepOrphanedNamespaces(ctx)
	custNSDeleted, custNSFailed := w.sweepOrphanedCustomerNamespaces(ctx)

	slog.Info("jobs.orphan_sweep.completed",
		"pending_teams_finished", pendingFinished,
		"pending_teams_failed", pendingFailed,
		"orphan_subscriptions_cancelled", subsCancelled,
		"orphan_subscriptions_failed", subsFailed,
		"orphan_namespaces_deleted", nsDeleted,
		"orphan_namespaces_failed", nsFailed,
		"orphan_customer_namespaces_deleted", custNSDeleted,
		"orphan_customer_namespaces_failed", custNSFailed,
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
			rows.Close()
			return 0, 0, fmt.Errorf("scan stuck-pending team: %w", scanErr)
		}
		candidates = append(candidates, c)
	}
	rows.Close()
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
			rows.Close()
			return 0, 0, fmt.Errorf("scan orphaned sub: %w", scanErr)
		}
		orphans = append(orphans, o)
	}
	rows.Close()
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

// sweepOrphanedNamespaces lists every instant-deploy-* namespace and deletes
// the ones whose backing deployment row is owned by a tombstoned /
// deletion_pending team, or has no backing row at all.
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

	// Build the set of app_ids whose namespace is legitimately live: the
	// deployment row exists AND its team is still active or inside the
	// restorable deletion_requested grace window. Everything else is an
	// orphan.
	liveAppIDs, err := w.fetchLiveDeployAppIDs(ctx)
	if err != nil {
		// Same fail-open posture: a DB blip on the live-app-ids query
		// must not fail the job. Skip the delete decisions this sweep.
		slog.Warn("jobs.orphan_sweep.pass3_live_app_ids_failed",
			"error", err.Error(),
			"detail", "namespace-orphan cleanup skipped this sweep; PASS 1/2 still ran")
		return 0, 0
	}

	for _, ns := range namespaces {
		appID := ns[len(deployNamespacePrefixTDE):]
		if appID == "" {
			continue
		}
		if liveAppIDs[appID] {
			continue // legitimately owned — leave it
		}
		// Orphan: no live owner. Delete the namespace.
		if delErr := w.k8s.DeleteNamespace(ctx, ns); delErr != nil {
			failed++
			slog.Error("jobs.orphan_sweep.namespace_delete_failed",
				"namespace", ns, "error", delErr)
			w.emitOrphanSweepFailed(ctx, uuid.Nil, orphanKindK8sNamespace, ns, delErr)
			continue
		}
		deleted++
		w.emitOrphanReclaimed(ctx, uuid.Nil, orphanKindK8sNamespace, ns,
			"deleted orphaned k8s deploy namespace")
	}
	return deleted, failed
}

// fetchLiveDeployAppIDs returns the set of deployment app_ids that are
// legitimately live — the deployment row exists, is not 'deleted', and its
// team is 'active' or in the still-restorable 'deletion_requested' state.
// A deployment owned by a 'tombstoned' / 'deletion_pending' team is NOT
// live (its namespace is an orphan to be reclaimed).
func (w *OrphanSweepReconciler) fetchLiveDeployAppIDs(ctx context.Context) (map[string]bool, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT d.app_id
		  FROM deployments d
		  JOIN teams t ON t.id = d.team_id
		 WHERE d.app_id IS NOT NULL
		   AND d.app_id != ''
		   AND d.status != 'deleted'
		   AND t.status IN ('active', 'deletion_requested')
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]bool)
	for rows.Next() {
		var appID string
		if scanErr := rows.Scan(&appID); scanErr != nil {
			return nil, scanErr
		}
		out[appID] = true
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
			slog.Error("jobs.orphan_sweep.customer_namespace_delete_failed",
				"namespace", ns, "error", delErr)
			w.emitOrphanSweepFailed(ctx, uuid.Nil, orphanKindK8sCustomerNS, ns, delErr)
			continue
		}
		deleted++
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
	defer rows.Close()
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
