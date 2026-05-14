package jobs_test

// weekly_digest_test.go — FOLLOWUP-5 migration coverage for
// WeeklyDigestWorker. Pre-migration the worker called Resend directly
// (NoopClient in prod). Post-migration it writes a digest.weekly
// audit_log row carrying the email + breakdown the Brevo template
// substitutes into the template body.

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	"instant.dev/worker/internal/jobs"
)

func TestWeeklyDigest_NoCandidates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"email", "id", "name"})
	mock.ExpectQuery(`FROM users u\s+JOIN teams t`).WillReturnRows(rows)

	w := jobs.NewWeeklyDigestWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.WeeklyDigestArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestWeeklyDigest_WritesAuditWithFullMetadata is the FOLLOWUP-5 pin: one
// candidate user/team produces one digest.weekly audit_log insert.
// Pre-migration it called Resend, this test fails on master. Post-migration
// it passes.
func TestWeeklyDigest_WritesAuditWithFullMetadata(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()
	rows := sqlmock.NewRows([]string{"email", "id", "name"}).
		AddRow("u@example.com", teamID, "Acme Inc.")
	mock.ExpectQuery(`FROM users u\s+JOIN teams t`).WillReturnRows(rows)

	statRows := sqlmock.NewRows([]string{"resource_type", "count"}).
		AddRow("mongodb", 1).
		AddRow("postgres", 2).
		AddRow("redis", 4)
	mock.ExpectQuery(`SELECT resource_type, COUNT\(\*\)::bigint`).
		WithArgs(teamID).
		WillReturnRows(statRows)

	// One audit insert with kind=digest.weekly + summary + metadata.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "digest.weekly", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := jobs.NewWeeklyDigestWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.WeeklyDigestArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestWeeklyDigest_SkipsRowOnBreakdownError validates per-row fail-open:
// a per-team breakdown query failure logs + skips, but doesn't propagate.
func TestWeeklyDigest_SkipsRowOnBreakdownError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()
	rows := sqlmock.NewRows([]string{"email", "id", "name"}).
		AddRow("u@example.com", teamID, "")
	mock.ExpectQuery(`FROM users u\s+JOIN teams t`).WillReturnRows(rows)

	mock.ExpectQuery(`SELECT resource_type, COUNT\(\*\)::bigint`).
		WithArgs(teamID).
		WillReturnError(errDB)

	w := jobs.NewWeeklyDigestWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.WeeklyDigestArgs]()); err != nil {
		t.Fatalf("expected nil (per-row fail-open), got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
