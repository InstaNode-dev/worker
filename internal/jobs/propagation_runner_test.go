package jobs

// propagation_runner_test.go — sqlmock-driven coverage for the
// pending_propagations runner. Pinned per the standing CI gate (CLAUDE.md
// rule 18 + the brief's "registry-iterating tests"):
//
//   1. TestPropagationRunner_EveryKindHasAHandler — registry-iterating
//      drift guard. Adds a kind constant without a handler → build-time
//      regression test failure (rule 18).
//
//   2. TestPropagationRunner_AppliesEligibleRow — the happy path: an
//      eligible row + a successful regrade lands `applied_at`.
//
//   3. TestPropagationRunner_RetryOnFailure_PersistsBackoff — a
//      RegradeResource failure increments attempts, persists last_error,
//      and schedules next_attempt_at via propagationBackoffFor.
//
//   4. TestPropagationRunner_DeadLettersAfterMaxAttempts — the row whose
//      attempts==maxAttempts-1 and whose dispatch fails transitions to
//      failed_at + emits the propagation.dead_lettered audit row.
//
//   5. TestPropagationRunner_IdempotentReRun_AppliedRowSkipped — a row
//      already stamped applied_at is never picked up again (the partial
//      index's predicate excludes it).
//
//   6. TestPropagationBackoffSchedule_IsMonotonicAndClamps — pins the
//      backoff schedule values + the clamp behaviour beyond
//      len(schedule).

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	commonv1 "instant.dev/proto/common/v1"

	commonplans "instant.dev/common/plans"
)

// fakePropagationJob is the minimal river.Job the runner accepts.
func fakePropagationJob() *river.Job[PropagationRunnerArgs] {
	return &river.Job[PropagationRunnerArgs]{JobRow: &rivertype.JobRow{ID: 999}}
}

// stubPropagationRegrader counts RegradeResource calls and lets the test
// override the outcome per call.
type stubPropagationRegrader struct {
	calls   int
	outcome regradeOutcome
	err     error
}

func (s *stubPropagationRegrader) RegradeResource(
	_ context.Context, _, _ string, _ commonv1.ResourceType, _, _ string,
) (regradeOutcome, error) {
	s.calls++
	return s.outcome, s.err
}

// propagationSweepCols projects the runner's pickEligible query.
var propagationSweepCols = []string{
	"id", "kind", "team_id", "target_tier", "payload", "attempts",
}

// teamResourcesCols projects the handleTierElevation resource scan.
var teamResourcesCols = []string{
	"id", "token", "provider_resource_id", "tier", "resource_type",
}

// fixedNow returns a deterministic clock for the backoff test.
func fixedNow() func() time.Time {
	t := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	return func() time.Time { return t }
}

// ─── Test 1: registry-iterating rule-18 contract ──────────────────────────────

func TestPropagationRunner_EveryKindHasAHandler(t *testing.T) {
	if len(propagationKnownKinds) == 0 {
		t.Fatal("propagationKnownKinds is empty — no propagation kind has been registered, the runner cannot serve any work")
	}
	for _, k := range propagationKnownKinds {
		if _, ok := propagationHandlers[k]; !ok {
			t.Errorf("propagation kind %q is declared in propagationKnownKinds but has NO handler in propagationHandlers — a propagation row of this kind would loop forever on \"no handler registered\" retries until it dead-letters", k)
		}
	}
	// And the reverse: a handler registered for a kind that is NOT in
	// propagationKnownKinds is dead code. The api side cannot enqueue it.
	for k := range propagationHandlers {
		var known bool
		for _, kk := range propagationKnownKinds {
			if kk == k {
				known = true
				break
			}
		}
		if !known {
			t.Errorf("propagation handler registered for kind %q but the kind is NOT in propagationKnownKinds — the api side cannot enqueue this kind, the handler is unreachable", k)
		}
	}
}

// ─── Test 2: happy path — eligible row + success → applied_at ─────────────────

func TestPropagationRunner_AppliesEligibleRow(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	propID := uuid.New()
	teamID := uuid.New()
	resID := uuid.New()

	// pickEligible runs inside a tx.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, kind, team_id, target_tier, payload, attempts\s+FROM pending_propagations`).
		WillReturnRows(sqlmock.NewRows(propagationSweepCols).
			AddRow(propID, propagationKindTierElevation, teamID, "pro", []byte(`{}`), 0))
	// D22-P3 lease bump (2026-05-21): inside the pick tx, push
	// next_attempt_at on the just-picked rows by propagationLeaseDuration
	// so a crash before markApplied/markRetry can't immediately re-stage
	// the row on the next 30s tick.
	mock.ExpectExec(`UPDATE pending_propagations\s+SET next_attempt_at\s*=\s*\$1\s+WHERE id = ANY\(\$2::uuid\[\]\)`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	// Handler queries the team's resources.
	mock.ExpectQuery(`SELECT r\.id, r\.token, r\.provider_resource_id, r\.tier, r\.resource_type`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows(teamResourcesCols).
			AddRow(resID, "tok-1", sql.NullString{}, "pro", "postgres"))

	// markApplied UPDATE.
	mock.ExpectExec(`UPDATE pending_propagations\s+SET applied_at`).
		WithArgs(propID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// audit_log INSERT.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	stub := &stubPropagationRegrader{outcome: regradeOutcome{Applied: true, AppliedConnLimit: 20}}
	w := NewPropagationRunnerWorker(db, commonplans.Default(), stub)
	if wErr := w.Work(context.Background(), fakePropagationJob()); wErr != nil {
		t.Fatalf("Work: %v", wErr)
	}

	if stub.calls != 1 {
		t.Errorf("RegradeResource calls = %d, want 1", stub.calls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// ─── Test 3: retry — dispatch failure persists backoff ────────────────────────

func TestPropagationRunner_RetryOnFailure_PersistsBackoff(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	propID := uuid.New()
	teamID := uuid.New()
	resID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, kind, team_id, target_tier, payload, attempts\s+FROM pending_propagations`).
		WillReturnRows(sqlmock.NewRows(propagationSweepCols).
			AddRow(propID, propagationKindTierElevation, teamID, "pro", []byte(`{}`), 0))
	// D22-P3 lease bump (2026-05-21).
	mock.ExpectExec(`UPDATE pending_propagations\s+SET next_attempt_at\s*=\s*\$1\s+WHERE id = ANY\(\$2::uuid\[\]\)`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	mock.ExpectQuery(`SELECT r\.id, r\.token, r\.provider_resource_id, r\.tier, r\.resource_type`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows(teamResourcesCols).
			AddRow(resID, "tok-1", sql.NullString{}, "pro", "postgres"))

	// markRetry UPDATE: persists last_error + next_attempt_at + attempts+1.
	// The exact next_attempt_at value comes from propagationBackoffFor(0)=1m
	// against the fixed clock.
	expectedNext := time.Date(2026, 5, 20, 12, 1, 0, 0, time.UTC)
	mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts\s*=\s*attempts \+ 1`).
		WithArgs(
			sqlmock.AnyArg(), // last_error truncated string
			expectedNext,
			propID,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// audit_log INSERT for the .retrying row.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	stub := &stubPropagationRegrader{err: errors.New("provisioner down")}
	w := NewPropagationRunnerWorker(db, commonplans.Default(), stub)
	w.now = fixedNow()

	if wErr := w.Work(context.Background(), fakePropagationJob()); wErr != nil {
		t.Fatalf("Work: %v", wErr)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// ─── Test 4: dead-letter after maxAttempts ────────────────────────────────────

func TestPropagationRunner_DeadLettersAfterMaxAttempts(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	propID := uuid.New()
	teamID := uuid.New()
	resID := uuid.New()

	// attempts already at maxAttempts-1; one more failure dead-letters.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, kind, team_id, target_tier, payload, attempts\s+FROM pending_propagations`).
		WillReturnRows(sqlmock.NewRows(propagationSweepCols).
			AddRow(propID, propagationKindTierElevation, teamID, "pro", []byte(`{}`), propagationMaxAttempts-1))
	// D22-P3 lease bump (2026-05-21).
	mock.ExpectExec(`UPDATE pending_propagations\s+SET next_attempt_at\s*=\s*\$1\s+WHERE id = ANY\(\$2::uuid\[\]\)`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	mock.ExpectQuery(`SELECT r\.id, r\.token, r\.provider_resource_id, r\.tier, r\.resource_type`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows(teamResourcesCols).
			AddRow(resID, "tok-1", sql.NullString{}, "pro", "postgres"))

	// markDeadLettered UPDATE: stamps failed_at.
	mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts\s*=\s*attempts \+ 1,\s+last_attempt_at\s*=\s*now\(\),\s+last_error\s*=\s*\$1,\s+failed_at\s*=\s*now\(\)`).
		WithArgs(sqlmock.AnyArg(), propID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// age_seconds lookup query.
	mock.ExpectQuery(`SELECT created_at FROM pending_propagations WHERE id`).
		WithArgs(propID).
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(time.Now().Add(-2 * time.Hour)))
	// audit_log INSERT — propagation.dead_lettered row.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	stub := &stubPropagationRegrader{err: errors.New("still down on attempt 10")}
	w := NewPropagationRunnerWorker(db, commonplans.Default(), stub)

	if wErr := w.Work(context.Background(), fakePropagationJob()); wErr != nil {
		t.Fatalf("Work: %v", wErr)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// ─── Test 5: idempotent re-run — applied_at row is invisible ──────────────────

func TestPropagationRunner_IdempotentReRun_AppliedRowSkipped(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// pickEligible returns ZERO rows — the partial index excludes terminal
	// rows. The handler is never invoked.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, kind, team_id, target_tier, payload, attempts\s+FROM pending_propagations`).
		WillReturnRows(sqlmock.NewRows(propagationSweepCols))
	mock.ExpectCommit()

	stub := &stubPropagationRegrader{outcome: regradeOutcome{Applied: true}}
	w := NewPropagationRunnerWorker(db, commonplans.Default(), stub)

	if wErr := w.Work(context.Background(), fakePropagationJob()); wErr != nil {
		t.Fatalf("Work: %v", wErr)
	}

	if stub.calls != 0 {
		t.Errorf("RegradeResource calls = %d, want 0 — an applied_at row must never be picked up", stub.calls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// ─── Test 6: backoff schedule pin ─────────────────────────────────────────────

func TestPropagationBackoffSchedule_IsMonotonicAndClamps(t *testing.T) {
	// Pin the first few values + the clamp behaviour. If a future PR changes
	// these the operator playbook MUST be updated in lockstep.
	cases := []struct {
		attempts int
		want     time.Duration
	}{
		{0, 1 * time.Minute},
		{1, 5 * time.Minute},
		{2, 15 * time.Minute},
		{3, 30 * time.Minute},
		{len(propagationBackoffSchedule) - 1, propagationBackoffSchedule[len(propagationBackoffSchedule)-1]},
		// Clamp: attempts beyond the schedule length use the FINAL entry.
		{len(propagationBackoffSchedule), propagationBackoffSchedule[len(propagationBackoffSchedule)-1]},
		{len(propagationBackoffSchedule) + 100, propagationBackoffSchedule[len(propagationBackoffSchedule)-1]},
		// Negative attempts clamp to 0.
		{-1, propagationBackoffSchedule[0]},
	}
	for _, c := range cases {
		if got := propagationBackoffFor(c.attempts); got != c.want {
			t.Errorf("propagationBackoffFor(%d) = %s, want %s", c.attempts, got, c.want)
		}
	}

	// And the schedule itself must be monotonically non-decreasing —
	// otherwise the eager retry would speed up after a failure, which
	// defeats the purpose.
	for i := 1; i < len(propagationBackoffSchedule); i++ {
		if propagationBackoffSchedule[i] < propagationBackoffSchedule[i-1] {
			t.Errorf("propagationBackoffSchedule[%d]=%s < schedule[%d]=%s — backoff must be monotonically non-decreasing",
				i, propagationBackoffSchedule[i], i-1, propagationBackoffSchedule[i-1])
		}
	}
}

