package jobs_test

// pending_deletion_expirer_test.go — hermetic tests for the Wave FIX-I
// PendingDeletionExpirerWorker.
//
// The worker has two responsibilities:
//   1. UPDATE … RETURNING the right rows from pending_deletions
//      (status='pending' AND expires_at < now()).
//   2. Emit one audit_log row per expired row with the correct kind
//      (deploy.deletion_expired vs stack.deletion_expired) and
//      metadata.
//
// Backed by sqlmock — no DB connection required. The SQL we assert
// against matches the production query in
// pending_deletion_expirer.go::Work.

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	"instant.dev/worker/internal/jobs"
)

// expiredCols is the column order the worker's UPDATE … RETURNING emits.
// Keep in sync with pending_deletion_expirer.go::Work.
var expiredCols = []string{
	"id", "resource_id", "resource_type", "team_id", "requested_at",
}

// TestPendingDeletionExpirer_FlipsExpiredAndAudits asserts the happy
// path: two rows past their TTL get flipped to 'expired' and each
// receives an audit_log INSERT keyed to the right resource_type.
func TestPendingDeletionExpirer_FlipsExpiredAndAudits(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	deployRowID := uuid.New()
	stackRowID := uuid.New()
	deployResourceID := uuid.New()
	stackResourceID := uuid.New()
	teamID := uuid.New()
	requestedAt := time.Now().UTC().Add(-30 * time.Minute)

	mock.ExpectQuery(`UPDATE pending_deletions[\s\S]*RETURNING`).
		WillReturnRows(sqlmock.NewRows(expiredCols).
			AddRow(deployRowID, deployResourceID, "deploy", teamID, requestedAt).
			AddRow(stackRowID, stackResourceID, "stack", teamID, requestedAt))

	// One audit insert per expired row. The kind differs by resource_type.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", "deploy.deletion_expired", "deploy", "deploy.deletion_expired", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", "stack.deletion_expired", "stack", "stack.deletion_expired", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	worker := jobs.NewPendingDeletionExpirerWorker(db)
	if err := worker.Work(context.Background(), fakeJob[jobs.PendingDeletionExpirerArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestPendingDeletionExpirer_EmptySweepIsNoop asserts the empty-batch
// path: no rows expired this tick → no audit inserts, no error.
func TestPendingDeletionExpirer_EmptySweepIsNoop(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`UPDATE pending_deletions[\s\S]*RETURNING`).
		WillReturnRows(sqlmock.NewRows(expiredCols))

	worker := jobs.NewPendingDeletionExpirerWorker(db)
	if err := worker.Work(context.Background(), fakeJob[jobs.PendingDeletionExpirerArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestPendingDeletionExpirer_AuditMetadataCarriesEmailFields asserts the
// audit_log row written by the expirer carries the metadata fields the
// BrevoForwarder's buildDeployDeletionExpired builder reads
// (resource_id, pending_deletion_id, age_seconds). Without these, the
// resulting Brevo email body has empty {{ params.resource_id }} substitutions.
//
// Added 2026-05-14 as part of the FIX-I/J Resend→Brevo migration.
// Pins the contract between audit producer (this worker) and audit
// consumer (event_email_forwarder.go).
func TestPendingDeletionExpirer_AuditMetadataCarriesEmailFields(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rowID := uuid.New()
	resourceID := uuid.New()
	teamID := uuid.New()
	requestedAt := time.Now().UTC().Add(-30 * time.Minute)

	mock.ExpectQuery(`UPDATE pending_deletions[\s\S]*RETURNING`).
		WillReturnRows(sqlmock.NewRows(expiredCols).
			AddRow(rowID, resourceID, "deploy", teamID, requestedAt))

	// Inspect the metadata JSON via the custom matcher.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(
			teamID, "system", "deploy.deletion_expired", "deploy", "deploy.deletion_expired",
			pendingDeletionMetaMatcher{
				resourceID:        resourceID.String(),
				pendingDeletionID: rowID.String(),
				teamID:            teamID.String(),
			},
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	worker := jobs.NewPendingDeletionExpirerWorker(db)
	if err := worker.Work(context.Background(), fakeJob[jobs.PendingDeletionExpirerArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestPendingDeletionExpirer_AuditFailureNonFatal asserts that a
// transient audit-log insert failure does NOT propagate out of Work
// (the row is still flipped to 'expired' — audit is observability
// gravy, not the source of truth).
func TestPendingDeletionExpirer_AuditFailureNonFatal(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rowID := uuid.New()
	resourceID := uuid.New()
	teamID := uuid.New()
	requestedAt := time.Now().UTC().Add(-20 * time.Minute)

	mock.ExpectQuery(`UPDATE pending_deletions[\s\S]*RETURNING`).
		WillReturnRows(sqlmock.NewRows(expiredCols).
			AddRow(rowID, resourceID, "deploy", teamID, requestedAt))

	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnError(driverErr("audit table full"))

	worker := jobs.NewPendingDeletionExpirerWorker(db)
	if err := worker.Work(context.Background(), fakeJob[jobs.PendingDeletionExpirerArgs]()); err != nil {
		t.Errorf("Work must not propagate an audit insert failure: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// driverErr returns a generic non-nil driver.Error-shaped value for
// the sqlmock WillReturnError path. We don't import driver here so
// just construct a synthetic error from the standard library.
func driverErr(msg string) error {
	return &mockErr{msg: msg}
}

type mockErr struct{ msg string }

func (e *mockErr) Error() string { return e.msg }

// pendingDeletionMetaMatcher inspects the audit_log metadata payload to
// pin that the fields buildDeployDeletionExpired (event_email_mapping.go)
// reads ARE present in the producer's payload.
type pendingDeletionMetaMatcher struct {
	resourceID        string
	pendingDeletionID string
	teamID            string
}

func (m pendingDeletionMetaMatcher) Match(v driver.Value) bool {
	var raw []byte
	switch x := v.(type) {
	case []byte:
		raw = x
	case string:
		raw = []byte(x)
	default:
		return false
	}
	var doc map[string]interface{}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return false
	}
	if asString(doc["resource_id"]) != m.resourceID {
		return false
	}
	if asString(doc["pending_deletion_id"]) != m.pendingDeletionID {
		return false
	}
	if asString(doc["team_id"]) != m.teamID {
		return false
	}
	if _, ok := doc["age_seconds"]; !ok {
		return false
	}
	return true
}

func asString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
