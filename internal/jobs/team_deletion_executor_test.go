package jobs_test

// team_deletion_executor_test.go — hermetic tests for the
// TeamDeletionExecutorWorker. Each scenario seeds the candidate scan via
// sqlmock and asserts the worker's downstream DB writes + audit-log row.
//
// The 2026-05-19 atomic-deletion hardening changed the pipeline:
//   - step 0 flips deletion_requested → deletion_pending BEFORE teardown,
//   - the tombstone tx flips deletion_pending → tombstoned (not
//     deletion_requested → tombstoned),
//   - a k8s namespace-teardown step (skipped when the deleter is nil).
//
// Scenarios covered:
//   1. Happy path — one pending team, one resource, no S3/provisioner/k8s
//      client; step-0 flip + tombstone tx commit + team.tombstoned audit.
//   2. Partial-failure path — the per-team destruction transaction errors
//      mid-way; team stays in deletion_pending (no tombstone UPDATE
//      landed), team.deletion_failed audit row lands with failed_at_step.
//   3. Audit metadata shape — both kinds carry the brief's required keys,
//      including namespaces_deleted.
//   4. PII assertion — the executor issues the UPDATE statements that NULL
//      connection_url + metadata fields, NULL user PII, and flip
//      teams.status to 'tombstoned' from 'deletion_pending'.
//   5. k8s namespace teardown — with a deleter wired in, the executor
//      queries the team's deploy app_ids and deletes each namespace.
//   6. Idempotent re-run — a team already in deletion_pending (a prior run
//      failed) is re-processed: the step-0 flip affects 0 rows (expected)
//      and the pipeline still completes.

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	"instant.dev/worker/internal/jobs"
)

// auditKindTombstonedLiteral / auditKindTeamDeletionFailedLiteral mirror
// the package constants. We assert against the literal strings — they're
// the cross-module contract with the api, so duplication is intentional.
const (
	auditKindTombstonedLiteral         = "team.tombstoned"
	auditKindTeamDeletionFailedLiteral = "team.deletion_failed"
)

// teamDeletionCandidateCols is the column order returned by the
// candidate-scan SELECT in team_deletion_executor.go::fetchCandidates.
var teamDeletionCandidateCols = []string{"id", "deletion_requested_at"}

// teamDeletionResourceCols is the column order returned by fetchTeamResources.
var teamDeletionResourceCols = []string{"id", "token", "resource_type", "provider_resource_id"}

// TestTeamDeletionExecutor_HappyPath — scenario 1 + 3.
// One pending team with one resource → step-0 flip + full tombstone
// pipeline runs to completion + team.tombstoned audit row lands.
func TestTeamDeletionExecutor_HappyPath(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()
	resID := uuid.New()
	deletionRequestedAt := time.Now().UTC().Add(-31 * 24 * time.Hour)

	// Candidate scan: one team past the 30-day window.
	mock.ExpectQuery(`FROM teams\s+WHERE`).
		WillReturnRows(sqlmock.NewRows(teamDeletionCandidateCols).
			AddRow(teamID, deletionRequestedAt))

	// Step 0: flip deletion_requested → deletion_pending.
	mock.ExpectExec(`UPDATE teams\s+SET status = 'deletion_pending'`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Per-team resource fetch.
	mock.ExpectQuery(`FROM resources\s+WHERE team_id`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows(teamDeletionResourceCols).
			AddRow(resID, uuid.New().String(), "postgres", ""))

	// Transaction: BEGIN, three UPDATEs, COMMIT.
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE resources\s+SET connection_url`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE users\s+SET email`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE teams\s+SET status\s+= 'tombstoned'`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	// Audit emit — team.tombstoned with metadata JSONB carrying
	// resource_count_destroyed + namespaces_deleted + s3_bytes_freed +
	// duration_seconds.
	var capturedMeta []byte
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(
			teamID, "system", auditKindTombstonedLiteral,
			sqlmock.AnyArg(),
			&captureTeamDeletionBytesArg{out: &capturedMeta},
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// nil provisioner + nil s3 + nil k8s → those steps are skipped. The DB
	// pipeline still runs to completion.
	w := jobs.NewTeamDeletionExecutorWorker(db, nil, nil, nil, "instant-shared")
	if err := w.Work(context.Background(), fakeJob[jobs.TeamDeletionExecutorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}

	var meta map[string]any
	if err := json.Unmarshal(capturedMeta, &meta); err != nil {
		t.Fatalf("metadata JSON unmarshal: %v (raw: %s)", err, capturedMeta)
	}
	if got := meta["resource_count_destroyed"]; got != float64(1) {
		t.Errorf("metadata.resource_count_destroyed = %v, want 1", got)
	}
	if got := meta["namespaces_deleted"]; got != float64(0) {
		t.Errorf("metadata.namespaces_deleted = %v, want 0 (no k8s client)", got)
	}
	if _, ok := meta["s3_bytes_freed"]; !ok {
		t.Errorf("metadata.s3_bytes_freed missing: %v", meta)
	}
	if _, ok := meta["duration_seconds"]; !ok {
		t.Errorf("metadata.duration_seconds missing: %v", meta)
	}
}

// TestTeamDeletionExecutor_TxFailure_StaysPending — scenario 2.
// Mid-transaction error: the team must NOT be marked tombstoned (the
// rollback discards the flip), and a team.deletion_failed audit row must
// land with failed_at_step in metadata. The team is left in
// deletion_pending — the orphan-sweep reconciler / next sweep retries it.
func TestTeamDeletionExecutor_TxFailure_StaysPending(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()
	resID := uuid.New()
	deletionRequestedAt := time.Now().UTC().Add(-45 * 24 * time.Hour)

	mock.ExpectQuery(`FROM teams\s+WHERE`).
		WillReturnRows(sqlmock.NewRows(teamDeletionCandidateCols).
			AddRow(teamID, deletionRequestedAt))

	// Step 0 flip lands.
	mock.ExpectExec(`UPDATE teams\s+SET status = 'deletion_pending'`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectQuery(`FROM resources\s+WHERE team_id`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows(teamDeletionResourceCols).
			AddRow(resID, uuid.New().String(), "postgres", ""))

	// Tx starts but the second UPDATE (users PII) errors. The rollback
	// happens; the team.deletion_failed audit row is the next INSERT.
	// CRITICALLY: there is NO `UPDATE teams SET status='tombstoned'` —
	// the team is left in deletion_pending, NOT half-deleted.
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE resources\s+SET connection_url`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE users\s+SET email`).
		WithArgs(teamID).
		WillReturnError(errors.New("simulated users update failure"))
	mock.ExpectRollback()

	var capturedMeta []byte
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(
			teamID, "system", auditKindTeamDeletionFailedLiteral,
			sqlmock.AnyArg(),
			&captureTeamDeletionBytesArg{out: &capturedMeta},
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := jobs.NewTeamDeletionExecutorWorker(db, nil, nil, nil, "instant-shared")
	if err := w.Work(context.Background(), fakeJob[jobs.TeamDeletionExecutorArgs]()); err != nil {
		// Per-team errors are isolated; the Work() return is nil.
		t.Fatalf("Work() must NOT return error on per-team failure: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}

	var meta map[string]any
	if err := json.Unmarshal(capturedMeta, &meta); err != nil {
		t.Fatalf("metadata JSON unmarshal: %v (raw: %s)", err, capturedMeta)
	}
	step, _ := meta["failed_at_step"].(string)
	if step == "" {
		t.Errorf("metadata.failed_at_step is empty: %v", meta)
	}
	if got := meta["error"]; got == nil {
		t.Errorf("metadata.error missing: %v", meta)
	}
}

// TestTeamDeletionExecutor_NoCandidates_Noop — guards against the empty
// SELECT case: zero candidates, zero DB writes, no audit rows, no error.
func TestTeamDeletionExecutor_NoCandidates_Noop(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`FROM teams\s+WHERE`).
		WillReturnRows(sqlmock.NewRows(teamDeletionCandidateCols))
	// No further expectations — any other DB call is an error.

	w := jobs.NewTeamDeletionExecutorWorker(db, nil, nil, nil, "")
	if err := w.Work(context.Background(), fakeJob[jobs.TeamDeletionExecutorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestTeamDeletionExecutor_PIIStatements — scenario 4.
// Verifies the SQL statements the executor emits actually contain the
// PII-NULL clauses the brief requires, and that the team flip goes from
// deletion_pending (not deletion_requested).
func TestTeamDeletionExecutor_PIIStatements(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()
	deletionRequestedAt := time.Now().UTC().Add(-90 * 24 * time.Hour)

	mock.ExpectQuery(`FROM teams\s+WHERE`).
		WillReturnRows(sqlmock.NewRows(teamDeletionCandidateCols).
			AddRow(teamID, deletionRequestedAt))

	mock.ExpectExec(`UPDATE teams\s+SET status = 'deletion_pending'`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectQuery(`FROM resources`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows(teamDeletionResourceCols))

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE resources\s+SET connection_url = NULL,\s+key_prefix\s+= '',\s+provider_resource_id = NULL`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`UPDATE users\s+SET email\s+= 'deleted-'.*github_id = NULL,\s+google_id = NULL`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 0))
	// Teams flip: status='tombstoned' guarded on AND status='deletion_pending'.
	mock.ExpectExec(`UPDATE teams\s+SET status\s+= 'tombstoned',\s+tombstoned_at\s+= now\(\),\s+name\s+= NULL,\s+stripe_customer_id\s+= NULL\s+WHERE id = \$1\s+AND status = 'deletion_pending'`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", auditKindTombstonedLiteral,
			sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := jobs.NewTeamDeletionExecutorWorker(db, nil, nil, nil, "")
	if err := w.Work(context.Background(), fakeJob[jobs.TeamDeletionExecutorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// fakeNamespaceDeleter is an in-memory K8sNamespaceDeleter for tests. It
// records every namespace it was asked to delete and which still "exist".
type fakeNamespaceDeleter struct {
	mu       sync.Mutex
	existing map[string]bool // namespace → present
	deleted  []string
	failOn   map[string]error // namespace → error to return on Delete
}

func newFakeNamespaceDeleter(existing ...string) *fakeNamespaceDeleter {
	f := &fakeNamespaceDeleter{existing: map[string]bool{}, failOn: map[string]error{}}
	for _, ns := range existing {
		f.existing[ns] = true
	}
	return f
}

func (f *fakeNamespaceDeleter) DeleteNamespace(_ context.Context, ns string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.failOn[ns]; err != nil {
		return err
	}
	// Idempotent: deleting an already-gone namespace is success.
	delete(f.existing, ns)
	f.deleted = append(f.deleted, ns)
	return nil
}

func (f *fakeNamespaceDeleter) NamespaceExists(_ context.Context, ns string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.existing[ns], nil
}

func (f *fakeNamespaceDeleter) deletedNamespaces() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.deleted))
	copy(out, f.deleted)
	return out
}

// TestTeamDeletionExecutor_DeletesK8sNamespaces — scenario 5.
// With a k8s deleter wired in, the executor queries the team's deployment
// app_ids and deletes the instant-deploy-<appID> namespace for each.
func TestTeamDeletionExecutor_DeletesK8sNamespaces(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()
	deletionRequestedAt := time.Now().UTC().Add(-31 * 24 * time.Hour)
	appID := "app42"
	ns := "instant-deploy-" + appID

	mock.ExpectQuery(`FROM teams\s+WHERE`).
		WillReturnRows(sqlmock.NewRows(teamDeletionCandidateCols).
			AddRow(teamID, deletionRequestedAt))
	mock.ExpectExec(`UPDATE teams\s+SET status = 'deletion_pending'`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`FROM resources\s+WHERE team_id`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows(teamDeletionResourceCols))
	// k8s != nil → the executor queries deployment app_ids.
	mock.ExpectQuery(`SELECT DISTINCT app_id\s+FROM deployments\s+WHERE team_id`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows([]string{"app_id"}).AddRow(appID))

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE resources\s+SET connection_url`).
		WithArgs(teamID).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`UPDATE users\s+SET email`).
		WithArgs(teamID).WillReturnResult(sqlmock.NewResult(0, 0))
	var capturedMeta []byte
	mock.ExpectExec(`UPDATE teams\s+SET status\s+= 'tombstoned'`).
		WithArgs(teamID).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", auditKindTombstonedLiteral,
			sqlmock.AnyArg(), &captureTeamDeletionBytesArg{out: &capturedMeta}).
		WillReturnResult(sqlmock.NewResult(1, 1))

	fakeNS := newFakeNamespaceDeleter(ns)
	w := jobs.NewTeamDeletionExecutorWorker(db, nil, nil, fakeNS, "")
	if err := w.Work(context.Background(), fakeJob[jobs.TeamDeletionExecutorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	deleted := fakeNS.deletedNamespaces()
	if len(deleted) != 1 || deleted[0] != ns {
		t.Errorf("namespace deletes = %v, want [%s]", deleted, ns)
	}
	var meta map[string]any
	_ = json.Unmarshal(capturedMeta, &meta)
	if got := meta["namespaces_deleted"]; got != float64(1) {
		t.Errorf("metadata.namespaces_deleted = %v, want 1", got)
	}
}

// TestTeamDeletionExecutor_PendingRetry_Idempotent — scenario 6.
// A team already in deletion_pending (a previous sweep failed mid-pipeline)
// is re-processed. The step-0 flip affects 0 rows — which is the EXPECTED
// idempotent case — and the pipeline still completes to tombstoned.
func TestTeamDeletionExecutor_PendingRetry_Idempotent(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()
	deletionRequestedAt := time.Now().UTC().Add(-60 * 24 * time.Hour)

	// The candidate scan returns a team already in deletion_pending.
	mock.ExpectQuery(`FROM teams\s+WHERE`).
		WillReturnRows(sqlmock.NewRows(teamDeletionCandidateCols).
			AddRow(teamID, deletionRequestedAt))

	// Step 0: the flip affects 0 rows because the team is ALREADY
	// deletion_pending — that is the idempotent case, the executor proceeds.
	mock.ExpectExec(`UPDATE teams\s+SET status = 'deletion_pending'`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 0)) // 0 rows — already pending

	mock.ExpectQuery(`FROM resources\s+WHERE team_id`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows(teamDeletionResourceCols))

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE resources\s+SET connection_url`).
		WithArgs(teamID).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`UPDATE users\s+SET email`).
		WithArgs(teamID).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`UPDATE teams\s+SET status\s+= 'tombstoned'`).
		WithArgs(teamID).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", auditKindTombstonedLiteral,
			sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := jobs.NewTeamDeletionExecutorWorker(db, nil, nil, nil, "")
	if err := w.Work(context.Background(), fakeJob[jobs.TeamDeletionExecutorArgs]()); err != nil {
		t.Fatalf("idempotent re-run must succeed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// captureTeamDeletionBytesArg captures raw bytes for a JSONB column so
// tests can introspect audit_log.metadata after the worker writes it.
type captureTeamDeletionBytesArg struct {
	out *[]byte
}

func (c *captureTeamDeletionBytesArg) Match(v driver.Value) bool {
	switch b := v.(type) {
	case []byte:
		*c.out = append((*c.out)[:0], b...)
		return true
	case string:
		*c.out = append((*c.out)[:0], []byte(b)...)
		return true
	case nil:
		return true
	}
	return false
}
