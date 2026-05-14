package jobs_test

// payment_grace_reminder_test.go — hermetic tests for
// PaymentGraceReminderWorker.
//
// The candidate SQL is treated as a black box: each test seeds the
// SELECT result via sqlmock and asserts on the UPDATE + audit_log
// INSERT shape. The cadence predicate (last_reminder_at NULL OR < 6h
// ago) and the active-only filter are documented behaviour rather than
// re-tested at the SQL level — the test "given a candidate row, do we
// stamp + emit?" is what matters.

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	"instant.dev/worker/internal/jobs"
)

var graceReminderRowCols = []string{"id", "team_id", "expires_at"}

// auditKindPaymentGraceReminderLiteral is the literal kind string the
// producer writes. We compare against this literal in assertions but
// never reference the unexported package constant — the literal IS the
// contract the forwarder reads.
const auditKindPaymentGraceReminderLiteral = "payment.grace_reminder"

// TestPaymentGraceReminder_StampsAndEmits covers the happy path: one
// candidate row, the UPDATE stamps last_reminder_at and the audit row
// is inserted with grace_id + hours_remaining + grace_ends_at metadata.
func TestPaymentGraceReminder_StampsAndEmits(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	graceID := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(48 * time.Hour)

	mock.ExpectQuery(`FROM payment_grace_periods`).
		WillReturnRows(sqlmock.NewRows(graceReminderRowCols).AddRow(graceID, teamID, expires))

	// UPDATE stamps last_reminder_at + increments reminders_sent.
	mock.ExpectExec(`UPDATE payment_grace_periods`).
		WithArgs(sqlmock.AnyArg(), graceID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// audit_log INSERT.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", auditKindPaymentGraceReminderLiteral, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := jobs.NewPaymentGraceReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.PaymentGraceReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestPaymentGraceReminder_RaceLostSkipsAudit covers the concurrent-stamp
// path: another worker beat us to the UPDATE, so RowsAffected returns 0
// and we MUST NOT insert the audit row (that would double the customer's
// reminder email).
func TestPaymentGraceReminder_RaceLostSkipsAudit(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	graceID := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(48 * time.Hour)

	mock.ExpectQuery(`FROM payment_grace_periods`).
		WillReturnRows(sqlmock.NewRows(graceReminderRowCols).AddRow(graceID, teamID, expires))

	// UPDATE matches zero rows (another worker won the race).
	mock.ExpectExec(`UPDATE payment_grace_periods`).
		WithArgs(sqlmock.AnyArg(), graceID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// No audit INSERT expected — sqlmock strict mode fails if one fires.

	w := jobs.NewPaymentGraceReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.PaymentGraceReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestPaymentGraceReminder_TopLevelQueryError_Returns covers the
// top-level SELECT failure path: River must see an error so it retries.
func TestPaymentGraceReminder_TopLevelQueryError_Returns(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`FROM payment_grace_periods`).WillReturnError(errors.New("boom"))

	w := jobs.NewPaymentGraceReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.PaymentGraceReminderArgs]()); err == nil {
		t.Fatal("expected error from top-level SELECT, got nil")
	}
}
