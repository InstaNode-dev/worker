package jobs

// propagation_chaos_followups_test.go — CHAOS-DRILL-2026-05-20 F2/F3/F4 tests.
//
// These tests pin the three follow-up fixes from the 2026-05-20 chaos drill:
//
//   F2: TestPropagation_UnknownKind_DeadLettersAtMaxAttempts
//       Without the F2 fix, an `unknown_kind` row escapes the maxAttempts
//       ceiling and retries forever (confirmed live during the drill —
//       chaos_test_unknown_kind reached attempts=10 in 4 minutes without
//       transitioning to failed_at). This test drives a row with
//       attempts == propagationMaxAttempts-1 through one final Work() tick
//       with a kind NOT in propagationHandlers, and asserts that the row
//       dead-letters (single UPDATE … SET failed_at=now()) and the
//       Prometheus counter increments under reason="unknown_kind".
//
//       Registry-iterating per CLAUDE.md rule 18: the test SYNTHESIZES a
//       kind that is guaranteed-not-in-the-registry rather than hardcoding
//       a string — so a future PR that adds a new kind cannot accidentally
//       turn this test into a no-op.
//
//   F3: TestPropagation_DeadLetter_IncrementsMetric
//       Pins the F3 contract: every transition to failed_at increments
//       metrics.PropagationDeadLetteredTotal. Drives a row at
//       attempts == propagationMaxAttempts-1 through a final RegradeResource
//       failure (the modal "max_attempts" path) and asserts the counter
//       moved from 0 to 1 with reason="max_attempts" / kind="tier_elevation".
//
//   F4: TestWorker_RiverConfig_RescueStuckJobsAfterIs25Min
//       Pins the F4 fix: the rescueStuckJobsAfter constant is exactly 25
//       minutes. A future PR that drifts this value to River's default
//       (1h20m) breaks our 80-minute-RTO ceiling silently — this test
//       catches it at build time.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	commonplans "instant.dev/common/plans"

	"instant.dev/worker/internal/metrics"
)

// ─── F2: unknown_kind dead-letters at maxAttempts ─────────────────────────────

func TestPropagation_UnknownKind_DeadLettersAtMaxAttempts(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Registry-iterating per rule 18: synthesize a kind GUARANTEED not in
	// propagationHandlers. A hand-typed "chaos_test_unknown_kind" string
	// would silently start passing if some future PR adds that exact kind
	// to propagationHandlers; deriving from the wall clock means the kind
	// is fresh on every run AND we can prove it.
	syntheticKind := fmt.Sprintf("chaos_unknown_kind_%d", time.Now().UnixNano())
	if _, ok := propagationHandlers[syntheticKind]; ok {
		t.Fatalf("synthetic kind %q is unexpectedly in propagationHandlers — test cannot prove the F2 path", syntheticKind)
	}
	// Defence-in-depth: confirm propagationHandlers is non-empty (otherwise
	// EVERY kind would be unknown — meaningless test).
	if len(propagationHandlers) == 0 {
		t.Fatal("propagationHandlers is empty — F2 test is meaningless against an empty registry")
	}

	// Snapshot the counter so we can compare deltas (other tests may have
	// run first and bumped it).
	before := testutil.ToFloat64(metrics.PropagationDeadLetteredTotal.WithLabelValues("unknown_kind", "unknown_kind"))

	propID := uuid.New()
	teamID := uuid.New()

	// attempts == propagationMaxAttempts-1; one more "no handler" failure
	// should dead-letter (not retry).
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, kind, team_id, target_tier, payload, attempts\s+FROM pending_propagations`).
		WillReturnRows(sqlmock.NewRows(propagationSweepCols).
			AddRow(propID, syntheticKind, teamID, "pro", []byte(`{}`), propagationMaxAttempts-1))
	mock.ExpectCommit()

	// NO handler dispatch — the row's kind is unknown. Straight to
	// markUnknownKindDeadLettered: UPDATE pending_propagations SET failed_at=now().
	mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts\s*=\s*attempts \+ 1,\s+last_attempt_at\s*=\s*now\(\),\s+last_error\s*=\s*\$1,\s+failed_at\s*=\s*now\(\)`).
		WithArgs(sqlmock.AnyArg(), propID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// age_seconds lookup.
	mock.ExpectQuery(`SELECT created_at FROM pending_propagations WHERE id`).
		WithArgs(propID).
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(time.Now().Add(-10 * time.Minute)))
	// audit_log INSERT — propagation.unknown_kind_dead_lettered row.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	stub := &stubPropagationRegrader{}
	w := NewPropagationRunnerWorker(db, commonplans.Default(), stub)

	if wErr := w.Work(context.Background(), fakePropagationJob()); wErr != nil {
		t.Fatalf("Work: %v", wErr)
	}

	if stub.calls != 0 {
		t.Errorf("RegradeResource calls = %d, want 0 — an unknown_kind row must never reach a handler", stub.calls)
	}

	// The F3 contract for the F2 path: dead-letter Prom counter incremented
	// with reason="unknown_kind", kind="unknown_kind" (bounded bucket).
	after := testutil.ToFloat64(metrics.PropagationDeadLetteredTotal.WithLabelValues("unknown_kind", "unknown_kind"))
	if after-before != 1 {
		t.Errorf("PropagationDeadLetteredTotal{reason=unknown_kind,kind=unknown_kind} delta = %.0f, want 1", after-before)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestPropagation_UnknownKind_RetriesBelowMaxAttempts is the companion to the
// above — the pre-F2 behaviour stays the same WHEN attempts is still well
// below the ceiling. Without this companion, the F2 fix could silently
// regress the unknown_kind path into immediate dead-letter (the bug fix
// flipping into a new bug). A row at attempts=0 must retry once, not
// dead-letter.
func TestPropagation_UnknownKind_RetriesBelowMaxAttempts(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	syntheticKind := fmt.Sprintf("chaos_unknown_kind_retry_%d", time.Now().UnixNano())
	if _, ok := propagationHandlers[syntheticKind]; ok {
		t.Fatalf("synthetic kind %q is unexpectedly in propagationHandlers", syntheticKind)
	}

	propID := uuid.New()
	teamID := uuid.New()

	// attempts = 0 → far below the ceiling. Must retry, not dead-letter.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, kind, team_id, target_tier, payload, attempts\s+FROM pending_propagations`).
		WillReturnRows(sqlmock.NewRows(propagationSweepCols).
			AddRow(propID, syntheticKind, teamID, "pro", []byte(`{}`), 0))
	mock.ExpectCommit()

	// markRetry UPDATE — DOES NOT set failed_at.
	expectedNext := time.Date(2026, 5, 20, 12, 1, 0, 0, time.UTC) // backoff[0] = 1m
	mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts\s*=\s*attempts \+ 1,\s+last_attempt_at\s*=\s*now\(\),\s+last_error\s*=\s*\$1,\s+next_attempt_at`).
		WithArgs(sqlmock.AnyArg(), expectedNext, propID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	stub := &stubPropagationRegrader{}
	w := NewPropagationRunnerWorker(db, commonplans.Default(), stub)
	w.now = fixedNow()

	if wErr := w.Work(context.Background(), fakePropagationJob()); wErr != nil {
		t.Fatalf("Work: %v", wErr)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// ─── F3: dead-letter increments Prom counter on the modal path ────────────────

func TestPropagation_DeadLetter_IncrementsMetric(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Use the known tier_elevation kind — this drives the modal real-failure
	// path through markDeadLettered (NOT the F2 unknown_kind path).
	const knownKind = propagationKindTierElevation
	if _, ok := propagationHandlers[knownKind]; !ok {
		t.Fatalf("propagationKindTierElevation is unexpectedly NOT in propagationHandlers — registry contract broken")
	}

	before := testutil.ToFloat64(metrics.PropagationDeadLetteredTotal.WithLabelValues("max_attempts", knownKind))

	propID := uuid.New()
	teamID := uuid.New()
	resID := uuid.New()

	// attempts == propagationMaxAttempts-1; the next failure dead-letters.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, kind, team_id, target_tier, payload, attempts\s+FROM pending_propagations`).
		WillReturnRows(sqlmock.NewRows(propagationSweepCols).
			AddRow(propID, knownKind, teamID, "pro", []byte(`{}`), propagationMaxAttempts-1))
	mock.ExpectCommit()

	// Handler queries the team's resources.
	mock.ExpectQuery(`SELECT r\.id, r\.token, r\.provider_resource_id, r\.tier, r\.resource_type`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows(teamResourcesCols).
			AddRow(resID, "tok-1", sql.NullString{}, "pro", "postgres"))

	// Dead-letter UPDATE (failed_at=now()).
	mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts\s*=\s*attempts \+ 1,\s+last_attempt_at\s*=\s*now\(\),\s+last_error\s*=\s*\$1,\s+failed_at\s*=\s*now\(\)`).
		WithArgs(sqlmock.AnyArg(), propID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT created_at FROM pending_propagations WHERE id`).
		WithArgs(propID).
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(time.Now().Add(-24 * time.Hour)))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Persistent gRPC failure → final attempt dead-letters.
	stub := &stubPropagationRegrader{err: errors.New("provisioner still unreachable")}
	w := NewPropagationRunnerWorker(db, commonplans.Default(), stub)

	if wErr := w.Work(context.Background(), fakePropagationJob()); wErr != nil {
		t.Fatalf("Work: %v", wErr)
	}

	after := testutil.ToFloat64(metrics.PropagationDeadLetteredTotal.WithLabelValues("max_attempts", knownKind))
	if after-before != 1 {
		t.Errorf("PropagationDeadLetteredTotal{reason=max_attempts,kind=%s} delta = %.0f, want 1",
			knownKind, after-before)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// ─── F4: River config carries the exact 25-minute rescue threshold ────────────

func TestWorker_RiverConfig_RescueStuckJobsAfterIs25Min(t *testing.T) {
	// We can't easily exercise the river.NewClient path in a unit test
	// (it requires a live pgx pool). Pin the underlying constant
	// instead — the production wiring at workers.go passes
	// `RescueStuckJobsAfter: rescueStuckJobsAfter` to river.NewClient, so
	// a regression in the constant IS the regression in the field.
	const want = 25 * time.Minute
	if rescueStuckJobsAfter != want {
		t.Errorf("rescueStuckJobsAfter = %s, want %s — CHAOS F4: a regression to River's default (1h20m) caps our worst-case RTO above the 25-minute ceiling agreed in CHAOS-DRILL-2026-05-20",
			rescueStuckJobsAfter, want)
	}

	// Defence-in-depth: the value must be > globalJobTimeout. Without
	// this guard, an over-eager future PR could shrink rescueStuckJobsAfter
	// below globalJobTimeout — and the rescuer would start rescuing
	// in-flight jobs that River's own timeout is about to cancel.
	if rescueStuckJobsAfter <= globalJobTimeout {
		t.Errorf("rescueStuckJobsAfter (%s) must exceed globalJobTimeout (%s) — otherwise the rescuer races River's own timeout",
			rescueStuckJobsAfter, globalJobTimeout)
	}

	// And it must NOT match the River default (which would mean the
	// explicit override silently regressed back). The default is
	// JobTimeout + JobRescuerRescueAfterDefault = 20m + 1h = 1h20m.
	const riverDefaultRescue = 20*time.Minute + 1*time.Hour
	if rescueStuckJobsAfter >= riverDefaultRescue {
		t.Errorf("rescueStuckJobsAfter (%s) is >= River's default (1h20m) — the explicit pin is no longer reducing the RTO",
			rescueStuckJobsAfter)
	}
}
