package jobs

// e2e_cohort_sweep.go — belt-and-suspenders reaper for STALE synthetic
// test-cohort teams.
//
// PROBLEM SHAPE
//
// CI's E2E suite mints an ephemeral team via the api's /internal/e2e/account
// endpoint (api PR #260). Every such team is tagged teams.is_test_cohort=true
// (api migration 067). The happy path is that the SAME CI run deletes its
// account at the end of the run — but a CI run that crashes, times out, or is
// hard-cancelled mid-flight dies BEFORE it reaps its account. The leftover
// is_test_cohort team then lingers: it holds real provisioned resources +
// k8s namespaces, and it is a stray row in every operator view.
//
// THIS JOB is the durable backstop. Every cohortSweepInterval it finds
// is_test_cohort teams older than cohortSweepTTL that are not already in a
// terminal/destroying state, and purges each one the SAME way the team-
// deletion executor (team_deletion_executor.go) and the api reap path do:
// flip the team to deletion_pending, then run the executor's idempotent
// per-team teardown (deprovision resources, delete k8s namespaces, scrub
// PII, tombstone). It reuses the executor wholesale — no second copy of the
// purge cascade.
//
// SAFETY — this NEVER touches a non-cohort team
//
// The candidate query filters on `is_test_cohort = true`, so by construction
// only cohort teams are ever selected. As a defence in depth (the WHERE
// clause is the single point of failure for "did we pick the right rows"),
// each candidate is RE-CHECKED with isTestCohort() immediately before the
// purge; a row that is no longer is_test_cohort=true is skipped and counted
// under outcome="skipped_not_cohort" (an alert-able disagreement between the
// candidate query and the guard). No real team is is_test_cohort=true today
// (migration 067 defaults every row to false), so in practice this job is
// inert — it only ever matches a future synthetic/CI cohort row.
//
// SHORT TTL
//
// CI runs complete in minutes, so cohortSweepTTL is deliberately short
// (2h). That is long enough that a still-running CI job's freshly-created
// account is never reaped out from under it, yet short enough that a leaked
// account is reclaimed the same hour. The TTL is a named const so the
// integration test can reason about "stale vs fresh" without magic numbers.
//
// IDEMPOTENCY + FAIL-SAFE
//
// processTeam is fully idempotent (a re-run over an already-tombstoned team
// is a no-op). A per-team failure is isolated: it stays in deletion_pending
// for the next tick (or the orphan-sweep reconciler's PASS 1), is counted
// under outcome="failed", and never stops the sweep from attempting the next
// team. A nil executor disables the job with a single WARN (matching every
// other worker's fail-open posture). A top-level candidate-query failure
// returns an error so River retries the dispatch.

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	"instant.dev/worker/internal/metrics"
)

// cohortSweepInterval is how often the job runs. Hourly is the right cadence
// for a CI-account backstop: CI runs finish in minutes, so a leaked account
// older than the (also short) TTL is reclaimed within the hour, while an
// hourly cluster-wide query over the teams table is negligible load.
const cohortSweepInterval = 1 * time.Hour

// cohortSweepTTL is how old an is_test_cohort team must be before this job
// will purge it. Short on purpose — CI runs are minutes, so 2h comfortably
// covers the longest legitimate E2E run without ever reaping an account a
// still-running CI job is actively using.
const cohortSweepTTL = 2 * time.Hour

// cohortSweepBatchLimit caps how many stale cohort teams the job purges per
// run. A healthy steady-state has zero (CI reaps its own accounts); a backlog
// (a CI outage that leaked many runs) is drained a batch per tick rather than
// all at once — the per-team teardown does real deprovision/k8s work.
const cohortSweepBatchLimit = 25

// E2E cohort-sweep outcome labels for instant_e2e_cohort_swept_total. Bounded
// set — every per-team decision picks exactly one. The literal strings are
// part of the metric contract (see metrics.go E2ECohortSweptTotal).
const (
	cohortSweepOutcomeSwept            = "swept"
	cohortSweepOutcomeFailed           = "failed"
	cohortSweepOutcomeSkippedNotCohort = "skipped_not_cohort"
)

// E2ECohortSweepArgs is the periodic-job payload — empty, every run is a full
// sweep.
type E2ECohortSweepArgs struct{}

// Kind implements river.JobArgs.
func (E2ECohortSweepArgs) Kind() string { return "e2e_cohort_sweep" }

// E2ECohortSweepWorker is the River worker.
type E2ECohortSweepWorker struct {
	river.WorkerDefaults[E2ECohortSweepArgs]
	db       *sql.DB
	executor teamTeardownExecutor // nil = job skipped (fail open)
}

// NewE2ECohortSweepWorker constructs the worker. executor may be nil — the
// job then logs a single WARN per tick and reaps nothing, matching the fail-
// open posture of every other worker. In production it is the same
// *TeamDeletionExecutorWorker the orphan-sweep reconciler reuses.
func NewE2ECohortSweepWorker(db *sql.DB, executor teamTeardownExecutor) *E2ECohortSweepWorker {
	return &E2ECohortSweepWorker{db: db, executor: executor}
}

// Work runs one full sweep.
func (w *E2ECohortSweepWorker) Work(ctx context.Context, job *river.Job[E2ECohortSweepArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.e2e_cohort_sweep")
	defer span.End()

	start := time.Now()

	if w.executor == nil {
		slog.Warn("jobs.e2e_cohort_sweep.skipped_no_executor",
			"detail", "team-deletion executor unavailable — stale CI cohort teams will not be reaped this tick")
		return nil
	}

	candidates, err := w.fetchStaleCohortTeams(ctx)
	if err != nil {
		return fmt.Errorf("E2ECohortSweepWorker: candidate query failed: %w", err)
	}

	var swept, failed, skipped int
	for _, c := range candidates {
		switch w.purgeTeam(ctx, c) {
		case cohortSweepOutcomeSwept:
			swept++
		case cohortSweepOutcomeFailed:
			failed++
		case cohortSweepOutcomeSkippedNotCohort:
			skipped++
		}
	}

	// Idle-tick noise discipline (worker convention #1): an all-zero sweep is
	// the steady state (CI reaps its own accounts) — DEBUG. Any non-zero
	// counter is operational signal — INFO.
	level := slog.LevelInfo
	if swept == 0 && failed == 0 && skipped == 0 {
		level = slog.LevelDebug
	}
	slog.Log(ctx, level, "jobs.e2e_cohort_sweep.completed",
		"swept", swept,
		"failed", failed,
		"skipped_not_cohort", skipped,
		"ttl", cohortSweepTTL.String(),
		"duration_ms", time.Since(start).Milliseconds(),
		"job_id", job.ID,
	)
	return nil
}

// fetchStaleCohortTeams returns every is_test_cohort team older than the TTL
// that is not already in a terminal/destroying state. Capped by
// cohortSweepBatchLimit.
//
// status filter: a team already 'tombstoned' is done; one in
// 'deletion_pending' is mid-teardown (the orphan-sweep reconciler's PASS 1
// owns it). We sweep everything else — 'active' (the normal leaked-CI-account
// case) and 'deletion_requested' (a CI run that asked to delete but died
// before the 30-day executor ran; for a CI account we skip the grace).
func (w *E2ECohortSweepWorker) fetchStaleCohortTeams(ctx context.Context) ([]teamPendingDeletion, error) {
	cutoff := time.Now().Add(-cohortSweepTTL)
	rows, err := w.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT id, COALESCE(deletion_requested_at, created_at, now())
		  FROM teams
		 WHERE is_test_cohort = true
		   AND created_at < $1
		   AND status NOT IN ('tombstoned', 'deletion_pending')
		 ORDER BY created_at ASC
		 LIMIT %d
	`, cohortSweepBatchLimit), cutoff)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []teamPendingDeletion
	for rows.Next() {
		var c teamPendingDeletion
		if scanErr := rows.Scan(&c.teamID, &c.deletionRequestedAt); scanErr != nil {
			return nil, fmt.Errorf("scan stale cohort team: %w", scanErr)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// purgeTeam runs the per-team purge and returns the outcome label. It first
// re-asserts the team is still is_test_cohort (defence in depth — the WHERE
// clause already filtered, but a flip between the candidate SELECT and here
// must NEVER let the destructive path touch a non-cohort team), then flips the
// team to deletion_pending and runs the executor's idempotent teardown.
func (w *E2ECohortSweepWorker) purgeTeam(ctx context.Context, c teamPendingDeletion) string {
	// DEFENSIVE GUARD. isTestCohort fails SAFE-FOR-REAL-TEAMS: on any DB
	// error it returns (false, err), and we then SKIP rather than purge — a
	// destructive teardown must never run on an unconfirmed row. Only a row
	// confirmed is_test_cohort=true proceeds.
	flagged, err := isTestCohort(ctx, w.db, c.teamID)
	if err != nil {
		slog.Warn("jobs.e2e_cohort_sweep.cohort_recheck_failed",
			"team_id", c.teamID.String(), "error", err,
			"detail", "could not confirm is_test_cohort at purge time — skipping (never purge an unconfirmed team)")
		metrics.E2ECohortSweptTotal.WithLabelValues(cohortSweepOutcomeSkippedNotCohort).Inc()
		return cohortSweepOutcomeSkippedNotCohort
	}
	if !flagged {
		// The candidate query selected a row the guard says is NOT cohort —
		// a disagreement the alert keys on. Skip; never purge a non-cohort team.
		slog.Error("jobs.e2e_cohort_sweep.non_cohort_candidate_blocked",
			"team_id", c.teamID.String(),
			"detail", "candidate query selected a team that is not is_test_cohort — purge blocked by defensive guard; candidate query may have regressed")
		metrics.E2ECohortSweptTotal.WithLabelValues(cohortSweepOutcomeSkippedNotCohort).Inc()
		return cohortSweepOutcomeSkippedNotCohort
	}

	// Flip to deletion_pending so processTeam's idempotent teardown tombstones
	// it (the executor's step-0 flip only fires from 'deletion_requested'; for
	// a CI account we skip the 30-day grace and set the in-flight state here).
	// Guarded on the cohort flag a SECOND time inside the UPDATE so even a TOCTOU
	// flip between the SELECT/recheck and this write cannot mark a non-cohort
	// team for destruction.
	res, err := w.db.ExecContext(ctx, `
		UPDATE teams
		   SET status = 'deletion_pending'
		 WHERE id = $1
		   AND is_test_cohort = true
		   AND status NOT IN ('tombstoned', 'deletion_pending')
	`, c.teamID)
	if err != nil {
		slog.Error("jobs.e2e_cohort_sweep.mark_deletion_pending_failed",
			"team_id", c.teamID.String(), "error", err)
		metrics.E2ECohortSweptTotal.WithLabelValues(cohortSweepOutcomeFailed).Inc()
		return cohortSweepOutcomeFailed
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// The row changed state underneath us (already pending/tombstoned, or
		// the cohort flag cleared). Re-check guard already passed, so this is a
		// benign race — count as skipped, not failed.
		slog.Warn("jobs.e2e_cohort_sweep.mark_deletion_pending_noop",
			"team_id", c.teamID.String(),
			"detail", "team changed state between candidate select and purge — skipping this tick")
		metrics.E2ECohortSweptTotal.WithLabelValues(cohortSweepOutcomeSkippedNotCohort).Inc()
		return cohortSweepOutcomeSkippedNotCohort
	}

	if perr := w.executor.processTeam(ctx, c); perr != nil {
		slog.Error("jobs.e2e_cohort_sweep.purge_failed",
			"team_id", c.teamID.String(), "error", perr,
			"detail", "team stays in deletion_pending for next tick / orphan-sweep PASS 1")
		metrics.E2ECohortSweptTotal.WithLabelValues(cohortSweepOutcomeFailed).Inc()
		return cohortSweepOutcomeFailed
	}

	slog.Info("jobs.e2e_cohort_sweep.team_swept",
		"team_id", c.teamID.String(),
		"detail", "stale CI test-cohort team purged + tombstoned (belt-and-suspenders backstop)")
	metrics.E2ECohortSweptTotal.WithLabelValues(cohortSweepOutcomeSwept).Inc()
	return cohortSweepOutcomeSwept
}
