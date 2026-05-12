package jobs_test

import (
	"context"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"instant.dev/worker/internal/jobs"
)

// fakeExpiryEmailer captures every SendExpiryReminder call so tests can
// assert call count + payload without hitting the live Resend client.
type fakeExpiryEmailer struct {
	mu   sync.Mutex
	sent []sentReminder
	err  error
}

type sentReminder struct {
	to             string
	resourceType   string
	hoursRemaining int
}

func (f *fakeExpiryEmailer) SendExpiryReminder(_ context.Context, to, resourceType string, hoursRemaining int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, sentReminder{to: to, resourceType: resourceType, hoursRemaining: hoursRemaining})
	return f.err
}

func (f *fakeExpiryEmailer) calls() []sentReminder {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]sentReminder, len(f.sent))
	copy(out, f.sent)
	return out
}

func TestExpiryReminderWorker_NoCandidates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "resource_type", "expires_at", "email"})
	mock.ExpectQuery(`SELECT r.id::text, r.resource_type, r.expires_at, u.email`).WillReturnRows(rows)

	emailer := &fakeExpiryEmailer{}
	w := jobs.NewExpiryReminderWorker(db, emailer)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := len(emailer.calls()); got != 0 {
		t.Errorf("expected 0 sends, got %d", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestExpiryReminderWorker_SendsAndStamps(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expires := time.Now().UTC().Add(3 * time.Hour)

	rows := sqlmock.NewRows([]string{"id", "resource_type", "expires_at", "email"}).
		AddRow("res-1", "postgres", expires, "owner-a@example.com").
		AddRow("res-2", "redis", expires.Add(30*time.Minute), "owner-b@example.com")
	mock.ExpectQuery(`SELECT r.id::text, r.resource_type, r.expires_at, u.email`).WillReturnRows(rows)

	// One stamp UPDATE per candidate, stamped BEFORE the send.
	mock.ExpectExec(`UPDATE resources\s+SET expiry_reminded_at = now\(\)`).
		WithArgs("res-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE resources\s+SET expiry_reminded_at = now\(\)`).
		WithArgs("res-2").
		WillReturnResult(sqlmock.NewResult(0, 1))

	emailer := &fakeExpiryEmailer{}
	w := jobs.NewExpiryReminderWorker(db, emailer)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	calls := emailer.calls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 sends, got %d (%+v)", len(calls), calls)
	}
	if calls[0].to != "owner-a@example.com" || calls[0].resourceType != "postgres" {
		t.Errorf("call[0] = %+v", calls[0])
	}
	if calls[1].to != "owner-b@example.com" || calls[1].resourceType != "redis" {
		t.Errorf("call[1] = %+v", calls[1])
	}
	if calls[0].hoursRemaining < 1 || calls[0].hoursRemaining > 4 {
		t.Errorf("call[0].hoursRemaining = %d, want 1..4", calls[0].hoursRemaining)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestExpiryReminderWorker_StampsButSkipsSend_WhenNoOwnerEmail(t *testing.T) {
	// If the team owner email is NULL (orphan team / unfinished signup), we
	// still stamp the row so the next pass doesn't pick it back up. We do
	// not call the emailer.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expires := time.Now().UTC().Add(2 * time.Hour)
	rows := sqlmock.NewRows([]string{"id", "resource_type", "expires_at", "email"}).
		AddRow("res-orphan", "postgres", expires, nil) // NULL email
	mock.ExpectQuery(`SELECT r.id::text, r.resource_type, r.expires_at, u.email`).WillReturnRows(rows)
	mock.ExpectExec(`UPDATE resources\s+SET expiry_reminded_at = now\(\)`).
		WithArgs("res-orphan").
		WillReturnResult(sqlmock.NewResult(0, 1))

	emailer := &fakeExpiryEmailer{}
	w := jobs.NewExpiryReminderWorker(db, emailer)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := len(emailer.calls()); got != 0 {
		t.Errorf("expected 0 sends (no owner email), got %d", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestExpiryReminderWorker_FailOpenOnSendError(t *testing.T) {
	// Resend down: the send returns an error. The row is already stamped,
	// so the worker must NOT propagate the error (top-level success).
	// We will not retry this resource, which is the intentional trade-off
	// documented on ExpiryReminderWorker.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expires := time.Now().UTC().Add(1 * time.Hour)
	rows := sqlmock.NewRows([]string{"id", "resource_type", "expires_at", "email"}).
		AddRow("res-x", "mongodb", expires, "x@example.com")
	mock.ExpectQuery(`SELECT r.id::text`).WillReturnRows(rows)
	mock.ExpectExec(`UPDATE resources\s+SET expiry_reminded_at = now\(\)`).
		WithArgs("res-x").
		WillReturnResult(sqlmock.NewResult(0, 1))

	emailer := &fakeExpiryEmailer{err: errDB}
	w := jobs.NewExpiryReminderWorker(db, emailer)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("expected nil (fail-open) on Resend error, got %v", err)
	}
	if got := len(emailer.calls()); got != 1 {
		t.Errorf("expected exactly 1 attempted send, got %d", got)
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

	mock.ExpectQuery(`SELECT r.id::text`).WillReturnError(errDB)

	w := jobs.NewExpiryReminderWorker(db, &fakeExpiryEmailer{})
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err == nil {
		t.Fatal("expected error from top-level query failure")
	}
}

func TestExpiryReminderWorker_NilEmailer_StampsButSkips(t *testing.T) {
	// Dev mode (no RESEND_API_KEY plumbed through to the worker — emailer
	// is nil). We still stamp so a real cluster won't accidentally email
	// this row when the API key is added later. Trade-off: rows that get
	// stamped during a dev session won't ever be reminded.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expires := time.Now().UTC().Add(2 * time.Hour)
	rows := sqlmock.NewRows([]string{"id", "resource_type", "expires_at", "email"}).
		AddRow("res-z", "redis", expires, "z@example.com")
	mock.ExpectQuery(`SELECT r.id::text`).WillReturnRows(rows)
	mock.ExpectExec(`UPDATE resources\s+SET expiry_reminded_at = now\(\)`).
		WithArgs("res-z").
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := jobs.NewExpiryReminderWorker(db, nil) // nil emailer
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
