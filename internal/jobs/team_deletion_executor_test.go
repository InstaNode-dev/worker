package jobs_test

// team_deletion_executor_test.go — hermetic tests for the
// TeamDeletionExecutorWorker. Each scenario seeds the candidate scan via
// sqlmock and asserts the worker's downstream DB writes + audit-log row.
//
// Scenarios covered:
//   1. Happy path — one pending team with one resource, no S3 client, no
//      provisioner; tx commits, team.tombstoned audit lands.
//   2. Partial-failure path — the per-team destruction transaction errors
//      mid-way; team stays in deletion_requested (no UPDATE landed), and
//      a team.deletion_failed audit row lands with failed_at_step in the
//      metadata.
//   3. Audit metadata shape — both kinds carry the brief's required keys.
//   4. PII assertion — the executor issues the UPDATE statements that NULL
//      connection_url + metadata fields, NULL user PII, and flip
//      teams.status to 'tombstoned'.

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
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

// TestTeamDeletionExecutor_HappyPath — scenario 1.
// One pending team with one resource → full tombstone pipeline runs to
// completion + audit row of kind team.tombstoned lands.
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
	mock.ExpectQuery(`FROM teams\s+WHERE status = 'deletion_requested'`).
		WillReturnRows(sqlmock.NewRows(teamDeletionCandidateCols).
			AddRow(teamID, deletionRequestedAt))

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
	// resource_count_destroyed + s3_bytes_freed + duration_seconds.
	var capturedMeta []byte
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(
			teamID, "system", auditKindTombstonedLiteral,
			sqlmock.AnyArg(),
			&captureTeamDeletionBytesArg{out: &capturedMeta},
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// nil provisioner + nil s3 deleter → those steps are skipped. The DB
	// pipeline still runs to completion.
	w := jobs.NewTeamDeletionExecutorWorker(db, nil, nil, "instant-shared")
	if err := w.Work(context.Background(), fakeJob[jobs.TeamDeletionExecutorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}

	// Scenario 3: assert metadata shape.
	var meta map[string]any
	if err := json.Unmarshal(capturedMeta, &meta); err != nil {
		t.Fatalf("metadata JSON unmarshal: %v (raw: %s)", err, capturedMeta)
	}
	if got := meta["resource_count_destroyed"]; got != float64(1) {
		t.Errorf("metadata.resource_count_destroyed = %v, want 1", got)
	}
	if _, ok := meta["s3_bytes_freed"]; !ok {
		t.Errorf("metadata.s3_bytes_freed missing: %v", meta)
	}
	if _, ok := meta["duration_seconds"]; !ok {
		t.Errorf("metadata.duration_seconds missing: %v", meta)
	}
}

// TestTeamDeletionExecutor_TxFailure_StaysPending — scenario 2.
// Mid-transaction error: the team must NOT be marked tombstoned, and a
// team.deletion_failed audit row must land with failed_at_step in metadata.
func TestTeamDeletionExecutor_TxFailure_StaysPending(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()
	resID := uuid.New()
	deletionRequestedAt := time.Now().UTC().Add(-45 * 24 * time.Hour)

	mock.ExpectQuery(`FROM teams\s+WHERE status = 'deletion_requested'`).
		WillReturnRows(sqlmock.NewRows(teamDeletionCandidateCols).
			AddRow(teamID, deletionRequestedAt))

	mock.ExpectQuery(`FROM resources\s+WHERE team_id`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows(teamDeletionResourceCols).
			AddRow(resID, uuid.New().String(), "postgres", ""))

	// Tx starts but the second UPDATE (users PII) errors. The rollback
	// happens; the team.deletion_failed audit row is the next INSERT.
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

	w := jobs.NewTeamDeletionExecutorWorker(db, nil, nil, "instant-shared")
	if err := w.Work(context.Background(), fakeJob[jobs.TeamDeletionExecutorArgs]()); err != nil {
		// Per-team errors are isolated; the Work() return is nil.
		t.Fatalf("Work() must NOT return error on per-team failure: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}

	// failed_at_step must be present and non-empty.
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

	mock.ExpectQuery(`FROM teams\s+WHERE status = 'deletion_requested'`).
		WillReturnRows(sqlmock.NewRows(teamDeletionCandidateCols))
	// No further expectations — any other DB call is an error.

	w := jobs.NewTeamDeletionExecutorWorker(db, nil, nil, "")
	if err := w.Work(context.Background(), fakeJob[jobs.TeamDeletionExecutorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestTeamDeletionExecutor_PIIStatements — scenario 4.
// Verifies the SQL statements the executor emits actually contain the
// PII-NULL clauses the brief requires. We assert on the regex the SQL
// matcher uses, which is enough to catch a refactor that accidentally
// drops a field.
func TestTeamDeletionExecutor_PIIStatements(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()
	deletionRequestedAt := time.Now().UTC().Add(-90 * 24 * time.Hour)

	mock.ExpectQuery(`FROM teams\s+WHERE status = 'deletion_requested'`).
		WillReturnRows(sqlmock.NewRows(teamDeletionCandidateCols).
			AddRow(teamID, deletionRequestedAt))

	mock.ExpectQuery(`FROM resources`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows(teamDeletionResourceCols))

	mock.ExpectBegin()
	// Resources: connection_url, key_prefix, provider_resource_id all
	// touched. Single regex anchors on the three field names.
	mock.ExpectExec(`UPDATE resources\s+SET connection_url = NULL,\s+key_prefix\s+= '',\s+provider_resource_id = NULL`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 0))
	// Users: email, github_id, google_id all touched.
	mock.ExpectExec(`UPDATE users\s+SET email\s+= 'deleted-'.*github_id = NULL,\s+google_id = NULL`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 0))
	// Teams: status='tombstoned', tombstoned_at=now(), name=NULL,
	// stripe_customer_id=NULL.
	mock.ExpectExec(`UPDATE teams\s+SET status\s+= 'tombstoned',\s+tombstoned_at\s+= now\(\),\s+name\s+= NULL,\s+stripe_customer_id\s+= NULL`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", auditKindTombstonedLiteral,
			sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := jobs.NewTeamDeletionExecutorWorker(db, nil, nil, "")
	if err := w.Work(context.Background(), fakeJob[jobs.TeamDeletionExecutorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// captureTeamDeletionBytesArg captures raw bytes for a JSONB column so
// tests can introspect audit_log.metadata after the worker writes it.
// Mirrors captureBytesArg in expire_imminent_test.go and
// captureChurnBytesArg in churn_predictor_test.go.
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
