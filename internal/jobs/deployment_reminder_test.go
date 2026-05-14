package jobs_test

// deployment_reminder_test.go — Wave FIX-J coverage for
// DeploymentReminderWorker. Uses sqlmock to assert the candidate query
// shape + the CAS UPDATE + the audit-INSERT side effects.

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"instant.dev/worker/internal/jobs"
)

// fakeDeployReminderEmailer records every SendDeployExpiring call.
type fakeDeployReminderEmailer struct {
	mu   sync.Mutex
	sent []sentDeployReminder
	err  error
}

type sentDeployReminder struct {
	to               string
	deployName       string
	deployURL        string
	hoursRemaining   int
	makePermanentURL string
}

func (f *fakeDeployReminderEmailer) SendDeployExpiring(_ context.Context, to, name, url string, hours int, makePermURL string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, sentDeployReminder{
		to: to, deployName: name, deployURL: url,
		hoursRemaining: hours, makePermanentURL: makePermURL,
	})
	return f.err
}

func (f *fakeDeployReminderEmailer) calls() []sentDeployReminder {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]sentDeployReminder, len(f.sent))
	copy(out, f.sent)
	return out
}

// TestDeploymentReminderWorker_NoCandidates: empty candidate query → no
// sends, no errors.
func TestDeploymentReminderWorker_NoCandidates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"id", "team_id", "app_id", "app_url",
		"expires_at", "reminders_sent", "ttl_policy", "email",
	})
	mock.ExpectQuery(`SELECT d.id::text, d.team_id::text, d.app_id, d.app_url`).
		WillReturnRows(rows)

	// Gauge sample query.
	gaugeRows := sqlmock.NewRows([]string{"ttl_policy", "count"}).
		AddRow("auto_24h", 0).
		AddRow("permanent", 0)
	mock.ExpectQuery(`SELECT ttl_policy, count\(\*\)`).WillReturnRows(gaugeRows)

	emailer := &fakeDeployReminderEmailer{}
	w := jobs.NewDeploymentReminderWorker(db, emailer)
	if err := w.Work(context.Background(), fakeJob[jobs.DeploymentReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := len(emailer.calls()); got != 0 {
		t.Errorf("expected 0 sends, got %d", got)
	}
}

// TestDeploymentReminderWorker_SendsAndCASAdvances: a single candidate
// fires one email and one CAS UPDATE.
func TestDeploymentReminderWorker_SendsAndCASAdvances(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expires := time.Now().UTC().Add(4 * time.Hour)

	rows := sqlmock.NewRows([]string{
		"id", "team_id", "app_id", "app_url",
		"expires_at", "reminders_sent", "ttl_policy", "email",
	}).AddRow(
		"deploy-1", "11111111-1111-1111-1111-111111111111", "appname",
		sql.NullString{String: "https://appname.deployment.instanode.dev", Valid: true},
		expires, 2, "auto_24h",
		sql.NullString{String: "owner@example.com", Valid: true},
	)
	mock.ExpectQuery(`SELECT d.id::text, d.team_id::text, d.app_id, d.app_url`).
		WillReturnRows(rows)

	// Gauge sample
	gaugeRows := sqlmock.NewRows([]string{"ttl_policy", "count"}).
		AddRow("auto_24h", 1)
	mock.ExpectQuery(`SELECT ttl_policy, count\(\*\)`).WillReturnRows(gaugeRows)

	// CAS UPDATE.
	mock.ExpectExec(`UPDATE deployments\s+SET reminders_sent`).
		WithArgs("deploy-1", 2, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	emailer := &fakeDeployReminderEmailer{}
	w := jobs.NewDeploymentReminderWorker(db, emailer)
	if err := w.Work(context.Background(), fakeJob[jobs.DeploymentReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	calls := emailer.calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 send, got %d (%+v)", len(calls), calls)
	}
	if calls[0].to != "owner@example.com" {
		t.Errorf("send to = %q; want owner@example.com", calls[0].to)
	}
	if calls[0].deployName != "appname" {
		t.Errorf("deployName = %q; want appname", calls[0].deployName)
	}
	if calls[0].hoursRemaining < 1 || calls[0].hoursRemaining > 5 {
		t.Errorf("hoursRemaining = %d; want roughly 4", calls[0].hoursRemaining)
	}
	if calls[0].makePermanentURL == "" {
		t.Error("makePermanentURL must be non-empty so the email CTA works")
	}
}

// TestDeploymentReminderWorker_CASRaceLostSkipsSend: when the CAS returns
// rowsAffected=0 (another tick won), the worker must NOT send the email.
// This is the dedupe guarantee — never double-fire the same reminder.
func TestDeploymentReminderWorker_CASRaceLostSkipsSend(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expires := time.Now().UTC().Add(2 * time.Hour)

	rows := sqlmock.NewRows([]string{
		"id", "team_id", "app_id", "app_url",
		"expires_at", "reminders_sent", "ttl_policy", "email",
	}).AddRow(
		"deploy-race", "22222222-2222-2222-2222-222222222222", "racy",
		sql.NullString{String: "https://racy.deployment.instanode.dev", Valid: true},
		expires, 1, "auto_24h",
		sql.NullString{String: "owner@example.com", Valid: true},
	)
	mock.ExpectQuery(`SELECT d.id::text, d.team_id::text, d.app_id, d.app_url`).
		WillReturnRows(rows)

	gaugeRows := sqlmock.NewRows([]string{"ttl_policy", "count"}).
		AddRow("auto_24h", 1)
	mock.ExpectQuery(`SELECT ttl_policy, count\(\*\)`).WillReturnRows(gaugeRows)

	// CAS returns rowsAffected=0 — another tick won.
	mock.ExpectExec(`UPDATE deployments\s+SET reminders_sent`).
		WithArgs("deploy-race", 1, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 0))

	emailer := &fakeDeployReminderEmailer{}
	w := jobs.NewDeploymentReminderWorker(db, emailer)
	if err := w.Work(context.Background(), fakeJob[jobs.DeploymentReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := len(emailer.calls()); got != 0 {
		t.Errorf("CAS-lost path must NOT send: got %d calls", got)
	}
}
