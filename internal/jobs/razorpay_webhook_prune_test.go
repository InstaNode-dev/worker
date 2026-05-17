package jobs_test

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"instant.dev/worker/internal/jobs"
)

// TestRazorpayWebhookPruneWorker_DeletesOldRows proves the prune job issues a
// DELETE against razorpay_webhook_events and succeeds.
func TestRazorpayWebhookPruneWorker_DeletesOldRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(`DELETE FROM razorpay_webhook_events`).
		WillReturnResult(sqlmock.NewResult(0, 7))

	w := jobs.NewRazorpayWebhookPruneWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.RazorpayWebhookPruneArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestRazorpayWebhookPruneWorker_DBError_ReturnsError proves a DB failure is
// surfaced so River retries the tick.
func TestRazorpayWebhookPruneWorker_DBError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(`DELETE FROM razorpay_webhook_events`).
		WillReturnError(errDB)

	w := jobs.NewRazorpayWebhookPruneWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.RazorpayWebhookPruneArgs]()); err == nil {
		t.Fatal("expected error from DB failure, got nil")
	}
}
