package jobs_test

// deployment_expirer_test.go — Wave FIX-J coverage for
// DeploymentExpirerWorker. Asserts the candidate query + guarded
// status=expired UPDATE + email side effect.

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"instant.dev/worker/internal/jobs"
)

type fakeDeployExpirerEmailer struct {
	mu   sync.Mutex
	sent []sentExpiredNotice
	err  error
}

type sentExpiredNotice struct {
	to         string
	deployName string
}

func (f *fakeDeployExpirerEmailer) SendDeployExpired(_ context.Context, to, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, sentExpiredNotice{to: to, deployName: name})
	return f.err
}

func (f *fakeDeployExpirerEmailer) calls() []sentExpiredNotice {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]sentExpiredNotice, len(f.sent))
	copy(out, f.sent)
	return out
}

// TestDeploymentExpirerWorker_NoCandidates: empty result set → no work.
func TestDeploymentExpirerWorker_NoCandidates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "team_id", "app_id", "ttl_policy", "expires_at", "email"})
	mock.ExpectQuery(`SELECT d.id::text, d.team_id::text, d.app_id, d.ttl_policy`).
		WillReturnRows(rows)

	emailer := &fakeDeployExpirerEmailer{}
	w := jobs.NewDeploymentExpirerWorker(db, emailer)
	if err := w.Work(context.Background(), fakeJob[jobs.DeploymentExpirerArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := len(emailer.calls()); got != 0 {
		t.Errorf("expected 0 sends, got %d", got)
	}
}

// TestDeploymentExpirerWorker_ExpiresAndEmails: a single expired row is
// soft-deleted (UPDATE) and emailed.
func TestDeploymentExpirerWorker_ExpiresAndEmails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expires := time.Now().UTC().Add(-1 * time.Hour) // already expired

	rows := sqlmock.NewRows([]string{"id", "team_id", "app_id", "ttl_policy", "expires_at", "email"}).
		AddRow(
			"deploy-x", "33333333-3333-3333-3333-333333333333", "myapp",
			"auto_24h", expires,
			sql.NullString{String: "owner@example.com", Valid: true},
		)
	mock.ExpectQuery(`SELECT d.id::text, d.team_id::text, d.app_id, d.ttl_policy`).
		WillReturnRows(rows)

	// Guarded UPDATE status='expired'.
	mock.ExpectExec(`UPDATE deployments\s+SET status = 'expired'`).
		WithArgs("deploy-x").
		WillReturnResult(sqlmock.NewResult(0, 1))

	emailer := &fakeDeployExpirerEmailer{}
	w := jobs.NewDeploymentExpirerWorker(db, emailer)
	if err := w.Work(context.Background(), fakeJob[jobs.DeploymentExpirerArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	calls := emailer.calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 send, got %d", len(calls))
	}
	if calls[0].to != "owner@example.com" {
		t.Errorf("send to = %q", calls[0].to)
	}
	if calls[0].deployName != "myapp" {
		t.Errorf("deployName = %q; want myapp", calls[0].deployName)
	}
}

// TestDeploymentExpirerWorker_AlreadyExpiredSkipsSend: when the guarded
// UPDATE returns rowsAffected=0 (concurrent expirer/delete won the race),
// the worker must NOT email.
func TestDeploymentExpirerWorker_AlreadyExpiredSkipsSend(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "team_id", "app_id", "ttl_policy", "expires_at", "email"}).
		AddRow(
			"deploy-y", "44444444-4444-4444-4444-444444444444", "raceapp",
			"auto_24h", time.Now().UTC().Add(-1*time.Hour),
			sql.NullString{String: "owner2@example.com", Valid: true},
		)
	mock.ExpectQuery(`SELECT d.id::text, d.team_id::text, d.app_id, d.ttl_policy`).
		WillReturnRows(rows)

	mock.ExpectExec(`UPDATE deployments\s+SET status = 'expired'`).
		WithArgs("deploy-y").
		WillReturnResult(sqlmock.NewResult(0, 0)) // race lost

	emailer := &fakeDeployExpirerEmailer{}
	w := jobs.NewDeploymentExpirerWorker(db, emailer)
	if err := w.Work(context.Background(), fakeJob[jobs.DeploymentExpirerArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := len(emailer.calls()); got != 0 {
		t.Errorf("race-lost path must NOT send: got %d calls", got)
	}
}
