package jobs

// propagation_runner.go — event-driven retry mechanism for the "user
// upgraded but downstream didn't propagate" failure class.
//
// THE GAP THIS CLOSES
// -------------------
// `handleSubscriptionCharged` in the api atomically commits
// (teams.plan_tier, resources.tier) and ResolvePendingCheckout, then
// returns 200 to Razorpay. The actual *backend* regrade (provisioner
// `RegradeResource` → ALTER ROLE … CONNECTION LIMIT, Redis CONFIG SET
// maxmemory) is the entitlement_reconciler's job — which polls every
// ~5 minutes. If the reconciler's sweep fails repeatedly for the same
// team (provisioner outage during the window, a flaky pod) the customer
// is left with "Pro on paper, hobby-grade infra" and nothing alerts:
// the reconciler just logs WARNs.
//
// The api now ALSO writes one row per upgrade into
// `pending_propagations` (migration 058). This worker job is the eager
// consumer: every 30s it picks eligible rows
// (next_attempt_at <= now() AND no terminal timestamp) under
// FOR UPDATE SKIP LOCKED, dispatches them by `kind`, retries with
// exponential backoff on per-resource failure, and dead-letters after
// `propagationMaxAttempts` attempts with a `propagation.dead_lettered`
// audit row + a structured ERROR slog line at CRITICAL severity.
//
// WHY THIS IS SEPARATE FROM entitlement_reconciler
// ------------------------------------------------
// They have DIFFERENT signals and DIFFERENT alert semantics.
//
//   entitlement_reconciler is the unconditional sweep — every active
//     resource on every team gets a 5-min health check. Its drift-
//     correction signal fires when ANY resource ever falls behind its
//     entitlement, not just freshly-upgraded ones.
//
//   propagation_runner is the EAGER, EVENT-DRIVEN consumer. It knows
//     about a SPECIFIC charge that just landed, retries that specific
//     team's resources on a fast cadence (30s), tracks per-team attempt
//     counts in a durable DB row, and dead-letters with a loud audit
//     row + ERROR log when the retries are exhausted.
//
// The two work TOGETHER: when this job's row succeeds and gets
// applied_at stamped, the next entitlement_reconciler sweep finds
// nothing drifted for that team (CONFIG GET / applied_conn_limit query
// both match) and is a no-op. When this job dead-letters, the
// entitlement_reconciler is the eventually-consistent backstop — but
// the alert from `propagation.dead_lettered` has already fired by then.
//
// CONTRACT WITH THE PROVISIONER
// -----------------------------
// Idempotent. The provisioner's RegradeResource is itself idempotent:
// CONFIG GET / applied_conn_limit comparison precede every CONFIG SET /
// ALTER ROLE. Re-running this job against an already-regraded resource
// is a no-op per the provisioner's contract. So a row whose dispatch
// succeeded but whose `applied_at` UPDATE failed (e.g. transient DB
// blip) will retry safely on the next tick.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	commonv1 "instant.dev/proto/common/v1"
)

// ─── named constants ──────────────────────────────────────────────────────────
//
// Per CLAUDE.md ("named constants, not inline strings"): every magic value
// in this file lives here. The backoff schedule and maxAttempts are pinned
// so the dead-letter regression test can assert against them.

// PropagationKind* values mirror the api-side constants in
// instant.dev/internal/models/audit_kinds.go. The worker uses string
// literals (rather than importing the api models package) so the worker
// stays decoupled from the api repo — a future kind added here MUST also
// be added there (and vice-versa). The registry-iterating test
// TestPropagationRunner_EveryKindHasAHandler keeps the two surfaces from
// drifting silently.
const (
	propagationKindTierElevation = "tier_elevation"
)

// AuditKindPropagation* are the audit_log.kind strings the runner emits.
// Mirrors api/internal/models/audit_kinds.go's AuditKindPropagation*
// constants. Same drift contract as propagationKindTierElevation above.
const (
	auditKindPropagationApplied      = "propagation.applied"
	auditKindPropagationRetrying     = "propagation.retrying"
	auditKindPropagationDeadLettered = "propagation.dead_lettered"
)

// propagationActor is the audit_log.actor value the runner writes. Distinct
// from "system" (the entitlement_reconciler / provisioner_reconciler actor)
// so an operator can filter on actor = 'propagation_runner' for runs of
// THIS subsystem.
const propagationActor = "propagation_runner"

// propagationDefaultInterval is the periodic dispatch cadence. 30s matches
// the fastest existing reconciler (deploy_status_reconcile) — the eager
// retry path is supposed to be FAST. Override via PROPAGATION_RUNNER_INTERVAL
// (Go duration string) for tests.
const propagationDefaultInterval = 30 * time.Second

// propagationBatchLimit caps the per-tick fan-out. A backlog drains across
// successive ticks; FOR UPDATE SKIP LOCKED + the partial index on
// (next_attempt_at) keeps the picker cheap.
const propagationBatchLimit = 50

// propagationDispatchTimeout is the per-row dispatch budget. The handler
// itself calls RegradeResource which has its own 30s budget; one row can
// have many resources, so the outer budget is generous.
const propagationDispatchTimeout = 5 * time.Minute

// propagationMaxAttempts is the dead-letter threshold. After this many
// FAILED attempts (each one bumps `attempts`), the next failure transitions
// the row to `failed_at = now()` + emits propagation.dead_lettered. The
// total cumulative backoff to reach the dead-letter point is
// sum(propagationBackoffSchedule[:10]) ≈ 24h33m, which gives a real-world
// upstream outage (Razorpay-side or provisioner-side) ample time to
// recover before we give up.
const propagationMaxAttempts = 10

// propagationBackoffSchedule is the exponential backoff in ascending
// order. attempts=1 → schedule[0]=1m. attempts beyond len(schedule) clamp
// to the final value (24h). Held as a slice (not a function) so the test
// can pin the exact wall-clock at each step and the operator's playbook
// can name the offset.
var propagationBackoffSchedule = []time.Duration{
	1 * time.Minute,
	5 * time.Minute,
	15 * time.Minute,
	30 * time.Minute,
	1 * time.Hour,
	2 * time.Hour,
	4 * time.Hour,
	8 * time.Hour,
	16 * time.Hour,
	24 * time.Hour,
}

// propagationLastErrorMax is the maximum length we persist in
// pending_propagations.last_error. Avoids unbounded growth from a chatty
// gRPC error string.
const propagationLastErrorMax = 1000

// ─── job definition + handler registry ────────────────────────────────────────

// PropagationRunnerArgs is the River job payload. No fields — sweep job.
type PropagationRunnerArgs struct{}

// Kind is the River worker key.
func (PropagationRunnerArgs) Kind() string { return "propagation_runner" }

// PropagationRunnerInterval resolves the periodic dispatch cadence from the
// PROPAGATION_RUNNER_INTERVAL env var. Falls back to propagationDefaultInterval
// when unset/unparseable/non-positive. Mirrors EntitlementReconcileInterval's
// shape exactly so the operator playbook for one applies to the other.
func PropagationRunnerInterval() time.Duration {
	raw := strings.TrimSpace(os.Getenv("PROPAGATION_RUNNER_INTERVAL"))
	if raw == "" {
		return propagationDefaultInterval
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		slog.Warn("jobs.propagation_runner.bad_interval",
			"value", raw,
			"error", err,
			"fallback", propagationDefaultInterval.String(),
		)
		return propagationDefaultInterval
	}
	return d
}

// propagationRegrader is the narrow provisioner surface the runner needs.
// Identical shape to entitlementRegrader (see entitlement_reconciler.go)
// so the existing entitlementRegraderAdapter in workers.go satisfies BOTH
// without re-implementing the bridge.
type propagationRegrader = entitlementRegrader

// propagationHandler dispatches one row of a given kind. Returns nil on
// total success (all per-resource RPCs returned applied OR an idempotent
// no-op skip). Returns a non-nil error to signal "retry me later" — the
// runner increments attempts + schedules the next attempt + persists
// last_error.
type propagationHandler func(ctx context.Context, db *sql.DB, regrader propagationRegrader, plans PlanRegistry, row propagationRow) error

// propagationHandlers is the kind → handler registry. The CI-protected
// TestPropagationRunner_EveryKindHasAHandler iterates this map AND the
// declared propagationKind* constants — adding a kind without a handler
// fails the build (CLAUDE.md rule 18). A handler MUST be idempotent
// (the provisioner is, but additional side-effects added by a future
// handler must preserve that).
var propagationHandlers = map[string]propagationHandler{
	propagationKindTierElevation: handleTierElevation,
}

// propagationKnownKinds is the registry-coverage source for the rule-18
// test. Each declared kind constant appears here exactly once.
var propagationKnownKinds = []string{
	propagationKindTierElevation,
}

// ─── worker ───────────────────────────────────────────────────────────────────

// PropagationRunnerWorker drains the pending_propagations queue.
type PropagationRunnerWorker struct {
	river.WorkerDefaults[PropagationRunnerArgs]
	db       *sql.DB
	plans    PlanRegistry
	regrader propagationRegrader // nil disables dispatch (logs WARN each tick)
	now      func() time.Time    // injectable for tests
}

// NewPropagationRunnerWorker constructs the worker.
//
// regrader may be nil when PROVISIONER_ADDR is unset — the worker then
// WARN-noops each tick (the fail-open posture used by every other
// optional-provisioner-dependency worker here).
func NewPropagationRunnerWorker(db *sql.DB, plans PlanRegistry, regrader propagationRegrader) *PropagationRunnerWorker {
	return &PropagationRunnerWorker{db: db, plans: plans, regrader: regrader, now: time.Now}
}

// propagationRow is one row of the pending_propagations sweep projection.
type propagationRow struct {
	id          uuid.UUID
	kind        string
	teamID      uuid.UUID
	targetTier  sql.NullString
	payload     []byte
	attempts    int
}

// Work executes one tick of the runner.
//
// Picks up to propagationBatchLimit rows whose next_attempt_at has elapsed
// and dispatches them via the per-kind handler. Per-row failures are
// recorded + retried with exponential backoff; a row that exceeds
// propagationMaxAttempts on its NEXT failure is dead-lettered.
func (w *PropagationRunnerWorker) Work(ctx context.Context, job *river.Job[PropagationRunnerArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.propagation_runner")
	defer span.End()

	start := w.now()

	if w.regrader == nil {
		slog.Warn("jobs.propagation_runner.skipped",
			"reason", "no provisioner client configured (PROVISIONER_ADDR unset)",
		)
		return nil
	}

	rows, err := w.pickEligible(ctx)
	if err != nil {
		return fmt.Errorf("PropagationRunnerWorker.pickEligible: %w", err)
	}

	var (
		dispatched   int
		applied      int
		retried      int
		deadLettered int
		unknownKind  int
	)

	for _, row := range rows {
		dispatched++

		handler, ok := propagationHandlers[row.kind]
		if !ok {
			// A kind without a handler is a build-time contract violation
			// (rule 18 test enforces it), but at runtime an old worker
			// image could see a row from a newer api enqueue. Treat as a
			// retryable failure so the row is NOT dead-lettered before
			// the worker rolls forward.
			unknownKind++
			w.markRetry(ctx, row, fmt.Errorf("no handler registered for kind %q", row.kind))
			continue
		}

		// Per-row dispatch under its own timeout — one bad row must not
		// block the whole tick.
		dispatchCtx, cancel := context.WithTimeout(ctx, propagationDispatchTimeout)
		dispatchErr := handler(dispatchCtx, w.db, w.regrader, w.plans, row)
		cancel()

		if dispatchErr == nil {
			if mErr := w.markApplied(ctx, row); mErr != nil {
				// Failed to persist applied_at. The handler succeeded, so
				// the customer's infra IS correct. Log and retry on the
				// next tick — the handler's idempotency keeps re-runs safe.
				retried++
				w.markRetry(ctx, row, fmt.Errorf("markApplied: %w", mErr))
				continue
			}
			applied++
			continue
		}

		// Dispatch failed. If this is attempt N+1 where N == maxAttempts
		// the row dead-letters; otherwise it retries.
		if row.attempts+1 >= propagationMaxAttempts {
			deadLettered++
			w.markDeadLettered(ctx, row, dispatchErr)
			continue
		}
		retried++
		w.markRetry(ctx, row, dispatchErr)
	}

	slog.Info("jobs.propagation_runner.completed",
		"dispatched", dispatched,
		"applied", applied,
		"retried", retried,
		"dead_lettered", deadLettered,
		"unknown_kind", unknownKind,
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}

// pickEligible runs the SELECT … FOR UPDATE SKIP LOCKED that the runner
// uses to claim a batch. Each picked row is implicitly locked for the
// duration of the surrounding transaction; we COMMIT inside the picker
// and rely on the per-row UPDATE predicate (`applied_at IS NULL AND
// failed_at IS NULL AND next_attempt_at <= now()`) on the update side to
// keep concurrent runners safe — a sibling pod that picks the same row
// in the gap between two ticks will harmlessly re-run the idempotent
// handler.
//
// We use the FOR UPDATE SKIP LOCKED clause to keep two concurrent picks
// from claiming the SAME rows (otherwise both pods would dispatch the
// same handler twice in the same tick window).
func (w *PropagationRunnerWorker) pickEligible(ctx context.Context) ([]propagationRow, error) {
	tx, err := w.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	rows, err := tx.QueryContext(ctx, `
		SELECT id, kind, team_id, target_tier, payload, attempts
		  FROM pending_propagations
		 WHERE applied_at IS NULL
		   AND failed_at IS NULL
		   AND next_attempt_at <= now()
		 ORDER BY next_attempt_at
		 FOR UPDATE SKIP LOCKED
		 LIMIT $1
	`, propagationBatchLimit)
	if err != nil {
		return nil, fmt.Errorf("select eligible: %w", err)
	}
	defer rows.Close()

	var out []propagationRow
	for rows.Next() {
		var r propagationRow
		if scanErr := rows.Scan(&r.id, &r.kind, &r.teamID, &r.targetTier, &r.payload, &r.attempts); scanErr != nil {
			slog.Warn("jobs.propagation_runner.scan_failed", "error", scanErr)
			continue
		}
		out = append(out, r)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("rows iteration: %w", rowsErr)
	}
	rows.Close()

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit pick tx: %w", err)
	}
	committed = true
	return out, nil
}

// propagationBackoffFor returns the delay to apply BEFORE the next attempt
// given the row's PRE-increment attempts count. attempts is the failed-attempt
// counter that will be UPDATEd to attempts+1; the index into the schedule is
// (attempts) — i.e. the FIRST failure (attempts goes 0 → 1) uses
// propagationBackoffSchedule[0] (1m). Beyond the schedule length, the final
// entry (24h) is used.
//
// Exported (lower-case-with-test-friendly behaviour) so the test can assert
// the exact step boundaries.
func propagationBackoffFor(attempts int) time.Duration {
	if attempts < 0 {
		attempts = 0
	}
	if attempts >= len(propagationBackoffSchedule) {
		return propagationBackoffSchedule[len(propagationBackoffSchedule)-1]
	}
	return propagationBackoffSchedule[attempts]
}

// markRetry persists attempts+1, schedules next_attempt_at via
// propagationBackoffFor, persists last_error, and emits a DEBUG-level
// propagation.retrying audit row. Best-effort: a DB failure here only
// means the next tick may re-process the same row (idempotent handler
// makes that safe) and the audit row is missing.
func (w *PropagationRunnerWorker) markRetry(ctx context.Context, row propagationRow, dispatchErr error) {
	delay := propagationBackoffFor(row.attempts)
	nextAttempt := w.now().Add(delay)
	lastErr := truncatePropagationError(dispatchErr.Error())

	res, err := w.db.ExecContext(ctx, `
		UPDATE pending_propagations
		   SET attempts        = attempts + 1,
		       last_attempt_at = now(),
		       last_error      = $1,
		       next_attempt_at = $2
		 WHERE id = $3
		   AND applied_at IS NULL
		   AND failed_at IS NULL
	`, lastErr, nextAttempt, row.id)
	if err != nil {
		slog.Error("jobs.propagation_runner.retry_persist_failed",
			"propagation_id", row.id.String(),
			"team_id", row.teamID.String(),
			"kind", row.kind,
			"error", err,
			"dispatch_error", lastErr,
		)
		return
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		// A sibling tick or operator may have already touched the row.
		slog.Info("jobs.propagation_runner.retry_no_op",
			"propagation_id", row.id.String(),
			"note", "row no longer eligible (already terminal)",
		)
		return
	}

	// DEBUG audit row — retries are routine during a Razorpay/provisioner
	// outage; logging at WARN would spam. Operators inspect the row's
	// `attempts` + `last_error` columns directly for diagnosis.
	w.insertPropagationAuditRow(ctx, row, auditKindPropagationRetrying, fmt.Sprintf(
		"%s propagation for team %s retrying (attempt %d/%d)",
		row.kind, row.teamID.String(), row.attempts+1, propagationMaxAttempts,
	), map[string]any{
		"propagation_id":   row.id.String(),
		"kind":             row.kind,
		"team_id":          row.teamID.String(),
		"target_tier":      nullableTierString(row.targetTier),
		"attempts":         row.attempts + 1,
		"max_attempts":     propagationMaxAttempts,
		"next_attempt_at":  nextAttempt.UTC().Format(time.RFC3339),
		"last_error":       lastErr,
	})

	slog.Debug("jobs.propagation_runner.retrying",
		"propagation_id", row.id.String(),
		"team_id", row.teamID.String(),
		"kind", row.kind,
		"attempts", row.attempts+1,
		"max_attempts", propagationMaxAttempts,
		"next_attempt_at", nextAttempt,
		"last_error", lastErr,
	)
}

// markApplied stamps applied_at and clears last_error on the row. Returns
// an error so the caller can fall back to retry semantics — the handler's
// idempotency guarantees a re-run is safe.
func (w *PropagationRunnerWorker) markApplied(ctx context.Context, row propagationRow) error {
	res, err := w.db.ExecContext(ctx, `
		UPDATE pending_propagations
		   SET applied_at      = now(),
		       last_attempt_at = now(),
		       last_error      = NULL
		 WHERE id = $1
		   AND applied_at IS NULL
		   AND failed_at IS NULL
	`, row.id)
	if err != nil {
		return fmt.Errorf("update applied_at: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		// Already applied/failed by a sibling tick; not an error.
		slog.Info("jobs.propagation_runner.applied_no_op",
			"propagation_id", row.id.String(),
			"note", "row already terminal — sibling tick won the race")
		return nil
	}

	// INFO audit + slog — the success ledger event.
	w.insertPropagationAuditRow(ctx, row, auditKindPropagationApplied, fmt.Sprintf(
		"%s propagation for team %s applied (attempts=%d)",
		row.kind, row.teamID.String(), row.attempts,
	), map[string]any{
		"propagation_id":  row.id.String(),
		"kind":            row.kind,
		"team_id":         row.teamID.String(),
		"target_tier":     nullableTierString(row.targetTier),
		"attempts":        row.attempts,
	})

	slog.Info("jobs.propagation_runner.applied",
		"propagation_id", row.id.String(),
		"team_id", row.teamID.String(),
		"kind", row.kind,
		"target_tier", nullableTierString(row.targetTier),
		"attempts", row.attempts,
	)
	return nil
}

// markDeadLettered stamps failed_at, persists the terminal last_error,
// writes the CRITICAL-severity propagation.dead_lettered audit row, and
// emits the structured ERROR slog line the NR alert keys on. This IS the
// alert-able signal; matches the billing.charge_undeliverable pattern.
//
// The kind is intentionally NOT wired into the worker's email forwarder
// (supportedAuditKinds) — a customer whose infra cap silently failed to
// land after paying for Pro deserves a HUMAN follow-up, not an
// automated template. The operator's playbook is to inspect the row's
// `last_error` + the team's resources, fix the underlying issue (often
// a one-off bad provisioner pod), and either (a) DELETE the row to let
// the entitlement_reconciler converge on its 5-min sweep, or (b) reset
// failed_at = NULL + attempts = 0 to re-arm the runner.
func (w *PropagationRunnerWorker) markDeadLettered(ctx context.Context, row propagationRow, dispatchErr error) {
	lastErr := truncatePropagationError(dispatchErr.Error())
	res, err := w.db.ExecContext(ctx, `
		UPDATE pending_propagations
		   SET attempts        = attempts + 1,
		       last_attempt_at = now(),
		       last_error      = $1,
		       failed_at       = now()
		 WHERE id = $2
		   AND applied_at IS NULL
		   AND failed_at IS NULL
	`, lastErr, row.id)
	if err != nil {
		slog.Error("jobs.propagation_runner.dead_letter_persist_failed",
			"propagation_id", row.id.String(),
			"team_id", row.teamID.String(),
			"kind", row.kind,
			"error", err,
			"dispatch_error", lastErr,
		)
		return
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		// Already terminal — sibling tick won the race.
		return
	}

	// age_seconds is best-effort — fetched in a separate query because the
	// runner's pickEligible projection doesn't include created_at (the
	// hot-path SELECT keeps the projection narrow). A miss leaves
	// age_seconds=0 in the metadata; the audit row is still emitted.
	var ageSeconds float64
	var createdAt sql.NullTime
	if qErr := w.db.QueryRowContext(ctx,
		`SELECT created_at FROM pending_propagations WHERE id = $1`, row.id,
	).Scan(&createdAt); qErr == nil && createdAt.Valid {
		ageSeconds = w.now().Sub(createdAt.Time).Seconds()
	}

	meta := map[string]any{
		"propagation_id": row.id.String(),
		"kind":           row.kind,
		"team_id":        row.teamID.String(),
		"target_tier":    nullableTierString(row.targetTier),
		"attempts":       row.attempts + 1,
		"max_attempts":   propagationMaxAttempts,
		"last_error":     lastErr,
		"age_seconds":    ageSeconds,
	}
	summary := fmt.Sprintf(
		"%s propagation for team %s DEAD-LETTERED after %d attempts — operator must reconcile",
		row.kind, row.teamID.String(), propagationMaxAttempts,
	)
	w.insertPropagationAuditRow(ctx, row, auditKindPropagationDeadLettered, summary, meta)

	// CRITICAL severity: this is THE alert. NR Log alert filters on
	// audit_kind='propagation.dead_lettered' OR on the message below.
	slog.Error("jobs.propagation_runner.dead_lettered",
		"propagation_id", row.id.String(),
		"team_id", row.teamID.String(),
		"kind", row.kind,
		"target_tier", nullableTierString(row.targetTier),
		"attempts", row.attempts+1,
		"max_attempts", propagationMaxAttempts,
		"last_error", lastErr,
		"action", "operator must reconcile this team's infra against the resource tier snapshot; see runbook for propagation.dead_lettered",
	)
}

// insertPropagationAuditRow writes one audit_log row, best-effort. A miss
// here only loses the operator-visible ledger entry; the slog line still
// fires, and NR alerts can key on the message string when the audit row
// failed to land.
func (w *PropagationRunnerWorker) insertPropagationAuditRow(ctx context.Context, row propagationRow, kind, summary string, meta map[string]any) {
	metaBytes, mErr := json.Marshal(meta)
	if mErr != nil {
		// Should never happen — meta is a map of primitives.
		slog.Warn("jobs.propagation_runner.audit_meta_marshal_failed",
			"propagation_id", row.id.String(), "error", mErr)
		return
	}
	if _, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES ($1::uuid, $2, $3, $4, $5::jsonb)
	`, row.teamID, propagationActor, kind, summary, metaBytes); err != nil {
		slog.Warn("jobs.propagation_runner.audit_emit_failed",
			"propagation_id", row.id.String(),
			"team_id", row.teamID.String(),
			"kind", kind,
			"error", err,
		)
	}
}

// truncatePropagationError caps the persisted last_error at
// propagationLastErrorMax bytes. Avoids unbounded growth from a chatty
// gRPC error string.
func truncatePropagationError(s string) string {
	if len(s) <= propagationLastErrorMax {
		return s
	}
	return s[:propagationLastErrorMax-3] + "..."
}

// nullableTierString returns target_tier's value or "" when NULL.
func nullableTierString(s sql.NullString) string {
	if !s.Valid {
		return ""
	}
	return s.String
}

// ─── handlers ─────────────────────────────────────────────────────────────────

// handleTierElevation dispatches one 'tier_elevation' propagation row.
//
// For the row's team, iterate every active, non-expired resource and call
// RegradeResource with the resource's per-row tier snapshot (MR-P1-21 — see
// entitlement_reconciler.go for the snapshot-is-entitlement contract). Any
// per-resource gRPC error fails the WHOLE row (so the entire row retries
// with backoff). Per-resource "skip" outcomes (applied=false with
// skip_reason="already correct" / "unsupported resource type" / "backend
// does not support regrade") are NOT treated as failures: they are the
// provisioner's idempotency / type-coverage signal.
//
// Idempotency: re-running this handler is safe because the provisioner's
// RegradeResource does CONFIG GET / applied_conn_limit comparison before
// any CONFIG SET / ALTER ROLE. A resource that already has the correct
// cap returns skip_reason="already correct", which we count as success.
func handleTierElevation(ctx context.Context, db *sql.DB, regrader propagationRegrader, _ PlanRegistry, row propagationRow) error {
	// Pull every active, non-expired resource on the team. Mirror the
	// entitlement_reconciler's filter exactly so behaviour is consistent
	// between the eager (this) and lazy (reconciler) paths.
	rows, err := db.QueryContext(ctx, `
		SELECT r.id, r.token, r.provider_resource_id, r.tier, r.resource_type
		  FROM resources r
		 WHERE r.team_id = $1::uuid
		   AND r.status = 'active'
		   AND (r.expires_at IS NULL OR r.expires_at > now())
		 ORDER BY r.id
	`, row.teamID)
	if err != nil {
		return fmt.Errorf("select team resources: %w", err)
	}
	defer rows.Close()

	type res struct {
		id           uuid.UUID
		token        string
		prid         sql.NullString
		tier         string
		resourceType string
	}
	var resources []res
	for rows.Next() {
		var r res
		if scanErr := rows.Scan(&r.id, &r.token, &r.prid, &r.tier, &r.resourceType); scanErr != nil {
			slog.Warn("jobs.propagation_runner.tier_elevation.scan_failed",
				"propagation_id", row.id.String(), "error", scanErr)
			continue
		}
		resources = append(resources, r)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return fmt.Errorf("iterate resources: %w", rowsErr)
	}
	rows.Close()

	// Empty team is success — there is nothing to regrade. The customer
	// upgraded but hasn't provisioned anything yet; future provisions
	// will pick up the new tier directly from teams.plan_tier.
	if len(resources) == 0 {
		return nil
	}

	var firstErr error
	for _, r := range resources {
		resType, supported := resourceTypeFromString(r.resourceType)
		if !supported {
			// Storage/queue/webhook have no regrade arm — skip with no
			// error. The reconciler does the same.
			continue
		}
		if entitlementEphemeralTiers[r.tier] {
			// Ephemeral resource — never regraded up. Skip silently.
			continue
		}
		out, regErr := regrader.RegradeResource(
			ctx, r.token, r.prid.String, resType, r.tier, row.id.String(),
		)
		if regErr != nil {
			// Capture the first error but keep going — a partial success
			// on the OTHER resources is still progress. The row stays
			// non-terminal and will retry; the provisioner's idempotency
			// makes the re-attempt of the already-applied resources cheap.
			if firstErr == nil {
				firstErr = fmt.Errorf("regrade resource %s (%s): %w", r.id, r.resourceType, regErr)
			}
			continue
		}
		// applied=false is NOT an error — it's the idempotency signal.
		// We log only if the skip_reason indicates an actual provisioner-
		// side problem (not "already correct" / type unsupported).
		if !out.Applied && out.SkipReason != "" &&
			!strings.Contains(out.SkipReason, "already correct") &&
			!strings.Contains(out.SkipReason, "unsupported resource type") &&
			!strings.Contains(out.SkipReason, "backend does not support") {
			slog.Warn("jobs.propagation_runner.tier_elevation.unexpected_skip",
				"propagation_id", row.id.String(),
				"resource_id", r.id.String(),
				"resource_type", r.resourceType,
				"skip_reason", out.SkipReason,
			)
		}
	}
	return firstErr
}

// resourceTypeFromString maps the resources.resource_type column value to
// the gRPC enum. Returns supported=false for resource types that have no
// regrade arm (storage, queue, webhook) — the caller skips those rows
// without treating them as failures.
func resourceTypeFromString(s string) (commonv1.ResourceType, bool) {
	switch s {
	case "postgres":
		return commonv1.ResourceType_RESOURCE_TYPE_POSTGRES, true
	case "redis":
		return commonv1.ResourceType_RESOURCE_TYPE_REDIS, true
	case "mongodb":
		return commonv1.ResourceType_RESOURCE_TYPE_MONGODB, true
	default:
		return commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED, false
	}
}

