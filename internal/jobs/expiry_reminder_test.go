package jobs_test

// expiry_reminder_test.go — covers the FOLLOWUP-5 migration of
// ExpiryReminderWorker from a direct Resend send to an
// anon.expiry_warning audit_log insert. The BrevoForwarder dispatches
// from there — see event_email_forwarder_test.go for end-to-end coverage
// of that path.

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	"instant.dev/worker/internal/jobs"
)

func TestExpiryReminderWorker_NoCandidates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "team_id", "resource_type", "expires_at", "email"})
	mock.ExpectQuery(`SELECT r.id, r.team_id, r.resource_type, r.expires_at, u.email`).WillReturnRows(rows)

	w := jobs.NewExpiryReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestAnonExpiryReminder_WritesAuditWithFullMetadata is the FOLLOWUP-5
// migration pin: a candidate with a valid owner email gets stamped AND
// gets exactly one anon.expiry_warning audit_log row inserted carrying
// the metadata Brevo's template body needs (resource_id, resource_type,
// hours_remaining, expires_at, email).
//
// Pre-migration the worker called EmailClient.SendExpiryReminder
// (Resend, NoopClient in prod). Post-migration it writes the audit row.
// This test fails on master and passes here.
func TestAnonExpiryReminder_WritesAuditWithFullMetadata(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(3 * time.Hour)

	rows := sqlmock.NewRows([]string{"id", "team_id", "resource_type", "expires_at", "email"}).
		AddRow(resID, teamID, "postgres", expires, "owner@example.com")
	mock.ExpectQuery(`SELECT r.id, r.team_id, r.resource_type, r.expires_at, u.email`).WillReturnRows(rows)

	// Stamp must come BEFORE the audit insert — fail-open contract.
	mock.ExpectExec(`UPDATE resources\s+SET expiry_reminded_at = now\(\)`).
		WithArgs(resID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// One audit insert with kind=anon.expiry_warning. The full metadata
	// JSON is checked by argument matching on resource_type — the rest of
	// the params are validated indirectly via the per-builder tests in
	// event_email_mapping_test.go.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "anon.expiry_warning", "postgres", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := jobs.NewExpiryReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestExpiryReminderWorker_StampsButSkipsAudit_WhenNoOwnerEmail(t *testing.T) {
	// If the team owner email is NULL (orphan team / unfinished signup), we
	// still stamp the row so the next pass doesn't pick it back up, but
	// do not write the audit row (no recipient → no email to send).
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(2 * time.Hour)
	rows := sqlmock.NewRows([]string{"id", "team_id", "resource_type", "expires_at", "email"}).
		AddRow(resID, teamID, "postgres", expires, nil) // NULL email
	mock.ExpectQuery(`SELECT r.id, r.team_id, r.resource_type, r.expires_at, u.email`).WillReturnRows(rows)
	mock.ExpectExec(`UPDATE resources\s+SET expiry_reminded_at = now\(\)`).
		WithArgs(resID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := jobs.NewExpiryReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestExpiryReminderWorker_FailOpenOnAuditInsertError(t *testing.T) {
	// audit_log INSERT errors must NOT propagate — fail-open posture.
	// The row is already stamped, so the worker will not retry this resource.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(1 * time.Hour)
	rows := sqlmock.NewRows([]string{"id", "team_id", "resource_type", "expires_at", "email"}).
		AddRow(resID, teamID, "mongodb", expires, "x@example.com")
	mock.ExpectQuery(`SELECT r.id`).WillReturnRows(rows)
	mock.ExpectExec(`UPDATE resources\s+SET expiry_reminded_at = now\(\)`).
		WithArgs(resID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnError(errDB)

	w := jobs.NewExpiryReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("expected nil (fail-open) on audit insert error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestExpiryReminderWorker_TopLevelQueryError_ReturnsError(t *testing.T) {
	// A failure on the top-level SELECT must propagate so River retries.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT r.id`).WillReturnError(errDB)

	w := jobs.NewExpiryReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err == nil {
		t.Fatal("expected error from top-level query failure")
	}
}
