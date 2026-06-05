package jobs

// e2e_cohort_sweep_test.go — hermetic (sqlmock) tests for the
// E2ECohortSweepWorker. Internal-package test so it can inject the unexported
// teamTeardownExecutor seam (fakeTeardownExecutor, defined in
// orphan_sweep_reconciler_test.go) and drive every per-team outcome arm
// without a real Postgres.
//
// Scenarios covered:
//   1. nil executor → fail-open no-op (no candidate query issued).
//   2. candidate-query error → Work returns an error (River retries).
//   3. happy path → a stale cohort team is flipped to deletion_pending and
//      handed to the executor (outcome="swept").
//   4. recheck-says-not-cohort → the destructive path is BLOCKED
//      (outcome="skipped_not_cohort"); the executor is NEVER called.
//   5. mark-deletion_pending 0-rows → benign race, skipped (no executor call).
//   6. executor teardown error → outcome="failed"; team left for next tick.

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

const cohortCandidateQueryRe = `SELECT id, COALESCE\(deletion_requested_at, created_at, now\(\)\)\s+FROM teams\s+WHERE is_test_cohort = true`

// cohortRecheckRe matches isTestCohort's SELECT.
const cohortRecheckRe = `SELECT is_test_cohort FROM teams WHERE id = \$1`

// cohortFlipRe matches the deletion_pending UPDATE.
const cohortFlipRe = `UPDATE teams\s+SET status = 'deletion_pending'\s+WHERE id = \$1\s+AND is_test_cohort = true`

func TestE2ECohortSweep_NilExecutor_NoOp(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	// No query expectations registered — a nil executor must short-circuit
	// before the candidate scan.
	w := NewE2ECohortSweepWorker(db, nil)
	if err := w.Work(context.Background(), fakeCohortSweepJob()); err != nil {
		t.Fatalf("Work with nil executor: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected DB calls with nil executor: %v", err)
	}
}

func TestE2ECohortSweep_CandidateQueryError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery(cohortCandidateQueryRe).WillReturnError(errors.New("boom"))

	w := NewE2ECohortSweepWorker(db, &fakeTeardownExecutor{})
	if err := w.Work(context.Background(), fakeCohortSweepJob()); err == nil {
		t.Fatal("Work: want error on candidate-query failure, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestE2ECohortSweep_NoCandidates_IdleTick(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Zero candidates → the all-zero (DEBUG) completion branch, no per-team work.
	mock.ExpectQuery(cohortCandidateQueryRe).
		WillReturnRows(sqlmock.NewRows([]string{"id", "ts"}))

	exec := &fakeTeardownExecutor{}
	w := NewE2ECohortSweepWorker(db, exec)
	if err := w.Work(context.Background(), fakeCohortSweepJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if len(exec.processed) != 0 {
		t.Errorf("executor.processed = %v, want [] (no candidates)", exec.processed)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestE2ECohortSweep_CandidateScanError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	// A row whose column shape (1 col) does not match the 2-col Scan → the
	// row-scan error arm of fetchStaleCohortTeams.
	mock.ExpectQuery(cohortCandidateQueryRe).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(uuid.New()))

	w := NewE2ECohortSweepWorker(db, &fakeTeardownExecutor{})
	if err := w.Work(context.Background(), fakeCohortSweepJob()); err == nil {
		t.Fatal("Work: want error on row-scan failure, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestE2ECohortSweep_HappyPath_Sweeps(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	teamID := uuid.New()
	mock.ExpectQuery(cohortCandidateQueryRe).
		WillReturnRows(sqlmock.NewRows([]string{"id", "ts"}).AddRow(teamID, time.Now()))
	// Recheck → still cohort.
	mock.ExpectQuery(cohortRecheckRe).WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows([]string{"is_test_cohort"}).AddRow(true))
	// Flip → 1 row affected.
	mock.ExpectExec(cohortFlipRe).WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	exec := &fakeTeardownExecutor{}
	w := NewE2ECohortSweepWorker(db, exec)
	if err := w.Work(context.Background(), fakeCohortSweepJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if len(exec.processed) != 1 || exec.processed[0] != teamID {
		t.Errorf("executor.processed = %v, want [%s]", exec.processed, teamID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestE2ECohortSweep_RecheckNotCohort_BlocksPurge(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	teamID := uuid.New()
	mock.ExpectQuery(cohortCandidateQueryRe).
		WillReturnRows(sqlmock.NewRows([]string{"id", "ts"}).AddRow(teamID, time.Now()))
	// Recheck → NOT cohort (the flag flipped under us). The destructive path
	// must be blocked: NO flip UPDATE, NO executor call.
	mock.ExpectQuery(cohortRecheckRe).WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows([]string{"is_test_cohort"}).AddRow(false))

	exec := &fakeTeardownExecutor{}
	w := NewE2ECohortSweepWorker(db, exec)
	if err := w.Work(context.Background(), fakeCohortSweepJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if len(exec.processed) != 0 {
		t.Errorf("executor.processed = %v, want [] (non-cohort team must NOT be purged)", exec.processed)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestE2ECohortSweep_RecheckError_Skips(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	teamID := uuid.New()
	mock.ExpectQuery(cohortCandidateQueryRe).
		WillReturnRows(sqlmock.NewRows([]string{"id", "ts"}).AddRow(teamID, time.Now()))
	mock.ExpectQuery(cohortRecheckRe).WithArgs(teamID).WillReturnError(errors.New("db blip"))

	exec := &fakeTeardownExecutor{}
	w := NewE2ECohortSweepWorker(db, exec)
	if err := w.Work(context.Background(), fakeCohortSweepJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if len(exec.processed) != 0 {
		t.Errorf("executor.processed = %v, want [] (unconfirmed team must NOT be purged)", exec.processed)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestE2ECohortSweep_FlipZeroRows_Skips(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	teamID := uuid.New()
	mock.ExpectQuery(cohortCandidateQueryRe).
		WillReturnRows(sqlmock.NewRows([]string{"id", "ts"}).AddRow(teamID, time.Now()))
	mock.ExpectQuery(cohortRecheckRe).WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows([]string{"is_test_cohort"}).AddRow(true))
	// Flip → 0 rows (state changed under us). Benign race → skip, no executor.
	mock.ExpectExec(cohortFlipRe).WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 0))

	exec := &fakeTeardownExecutor{}
	w := NewE2ECohortSweepWorker(db, exec)
	if err := w.Work(context.Background(), fakeCohortSweepJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if len(exec.processed) != 0 {
		t.Errorf("executor.processed = %v, want [] (0-row flip is a benign race)", exec.processed)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestE2ECohortSweep_FlipError_CountsFailed(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	teamID := uuid.New()
	mock.ExpectQuery(cohortCandidateQueryRe).
		WillReturnRows(sqlmock.NewRows([]string{"id", "ts"}).AddRow(teamID, time.Now()))
	mock.ExpectQuery(cohortRecheckRe).WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows([]string{"is_test_cohort"}).AddRow(true))
	mock.ExpectExec(cohortFlipRe).WithArgs(teamID).WillReturnError(errors.New("flip failed"))

	exec := &fakeTeardownExecutor{}
	w := NewE2ECohortSweepWorker(db, exec)
	if err := w.Work(context.Background(), fakeCohortSweepJob()); err != nil {
		t.Fatalf("Work: %v (per-team failure must NOT fail the sweep)", err)
	}
	if len(exec.processed) != 0 {
		t.Errorf("executor.processed = %v, want [] (flip failed before purge)", exec.processed)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestE2ECohortSweep_ExecutorError_CountsFailed(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	teamID := uuid.New()
	mock.ExpectQuery(cohortCandidateQueryRe).
		WillReturnRows(sqlmock.NewRows([]string{"id", "ts"}).AddRow(teamID, time.Now()))
	mock.ExpectQuery(cohortRecheckRe).WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows([]string{"is_test_cohort"}).AddRow(true))
	mock.ExpectExec(cohortFlipRe).WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	exec := &fakeTeardownExecutor{failFor: map[uuid.UUID]error{teamID: errors.New("teardown boom")}}
	w := NewE2ECohortSweepWorker(db, exec)
	if err := w.Work(context.Background(), fakeCohortSweepJob()); err != nil {
		t.Fatalf("Work: %v (a per-team teardown error must be isolated)", err)
	}
	if len(exec.processed) != 1 {
		t.Errorf("executor.processed = %v, want the team attempted once", exec.processed)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}
