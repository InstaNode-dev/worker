package jobs_test

// deployment_reminder_test.go — Wave FIX-J coverage for
// DeploymentReminderWorker. Uses sqlmock to assert the candidate query
// shape + the CAS UPDATE + the audit-INSERT side effect.
//
// Migration note (2026-05-14, FIX-I/J→Brevo): the worker no longer calls
// EmailClient.SendDeployExpiring directly. The audit_log row IS the
// trigger — the BrevoForwarder drains audit_log on its 60s tick and
// dispatches the actual Brevo POST. The tests below assert the row is
// written with the metadata fields the forwarder's buildDeployExpiringSoon
// builder reads (deploy_url, make_permanent_url, app_id, hours_remaining).

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"instant.dev/worker/internal/jobs"
)

// TestDeploymentReminderWorker_NoCandidates: empty candidate query → no
// audit inserts, no errors.
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

	w := jobs.NewDeploymentReminderWorker(db, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.DeploymentReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestDeploymentReminderWorker_WritesAuditWithFullMetadata is the
// migration-pinning test: a single candidate fires one CAS UPDATE and
// ONE audit_log INSERT carrying every field the BrevoForwarder's
// buildDeployExpiringSoon builder needs (deploy_url, make_permanent_url,
// app_id, hours_remaining). Asserts NO inline email dispatch happens —
// that path was removed in the 2026-05-14 Resend→Brevo migration.
//
// This test MUST fail on master (which still calls SendDeployExpiring
// inline) and pass after the migration ships. Proof the path migrated.
func TestDeploymentReminderWorker_WritesAuditWithFullMetadata(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
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
		WithArgs("deploy-1", 2, sqlmock.AnyArg(), 3 /* maxDeployReminders */).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Audit INSERT — the BrevoForwarder picks this up on its next tick.
	// Use a custom matcher on the metadata arg so we can pin every
	// field the email template body references.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(
			sqlmock.AnyArg(), // team UUID
			sqlmock.AnyArg(), // summary
			deployExpiringSoonMetaMatcher{
				deployID:       "deploy-1",
				appID:          "appname",
				makePermanentSubstr: "/api/v1/deployments/deploy-1/make-permanent",
				deployURLSubstr:     "appname.deployment.instanode.dev",
				reminderIndex:  3, // reminders_sent (2) + 1
			},
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := jobs.NewDeploymentReminderWorker(db, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.DeploymentReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestDeploymentReminderWorker_CASRaceLostNoAudit: when the CAS returns
// rowsAffected=0 (another tick won), the worker must NOT write an audit
// row. This is the dedupe guarantee — never double-fire the same reminder.
func TestDeploymentReminderWorker_CASRaceLostNoAudit(t *testing.T) {
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
		WithArgs("deploy-race", 1, sqlmock.AnyArg(), 3 /* maxDeployReminders */).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// NO INSERT INTO audit_log expected here.

	w := jobs.NewDeploymentReminderWorker(db, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.DeploymentReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// deployExpiringSoonMetaMatcher is a sqlmock argument matcher that
// inspects the JSON metadata payload to assert every field the
// BrevoForwarder's buildDeployExpiringSoon builder reads is present.
type deployExpiringSoonMetaMatcher struct {
	deployID            string
	appID               string
	makePermanentSubstr string
	deployURLSubstr     string
	reminderIndex       int
}

// Match implements sqlmock.Argument. Treats the input as []byte (JSON
// blob) and checks the required keys.
func (m deployExpiringSoonMetaMatcher) Match(v driver.Value) bool {
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
	if asStr(doc["deploy_id"]) != m.deployID {
		return false
	}
	if asStr(doc["app_id"]) != m.appID {
		return false
	}
	if !strings.Contains(asStr(doc["make_permanent_url"]), m.makePermanentSubstr) {
		return false
	}
	if !strings.Contains(asStr(doc["deploy_url"]), m.deployURLSubstr) {
		return false
	}
	// reminder_index decodes as float64 from generic JSON unmarshal.
	idx, ok := doc["reminder_index"].(float64)
	if !ok || int(idx) != m.reminderIndex {
		return false
	}
	return true
}

func asStr(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// TestDeploymentReminder_StageThresholds_Pinned pins the F3 escalating-cadence
// shape (Wave 3 follow-up, 2026-05-21). The earlier F3 fix capped reminders
// at 3 stages but kept a flat 2h cooldown — "Final reminder" landed with
// ~8h still remaining, which reads as another routine ping instead of an
// urgent last warning.
//
// The escalating cadence gates each subsequent reminder on a strictly
// tighter time-to-expiry: Stage 1 fires at T-12h, Stage 2 at T-6h,
// Stage 3 ("Final reminder") at T-1h. This test pins:
//   1. exactly maxDeployReminders thresholds defined (table-shape invariant)
//   2. thresholds STRICTLY decreasing (escalating; equality means flat, fails)
//   3. exact stage values (12h / 6h / 1h) — a future PR that flattens or
//      loosens any stage trips this and forces an explicit conversation
//   4. nextReminderThreshold(N) returns 0 for N >= maxDeployReminders
//      (cleanly stops after the final stage)
//
// Coverage block (CLAUDE.md rule 17):
//   Symptom:        "Final reminder" with 8h still on the clock reads as spam
//   Enumeration:    rg -n 'deployReminderStageThresholds|nextReminderThreshold' internal/jobs/
//   Sites found:    1 var declaration + 1 helper fn + 1 SQL query gate
//   Sites touched:  3 (var, helper, SQL CASE clause); table is single-source
//   Coverage test:  TestDeploymentReminder_StageThresholds_Pinned (this test)
func TestDeploymentReminder_StageThresholds_Pinned(t *testing.T) {
	thresholds := jobs.DeployReminderStageThresholds()

	if got, want := len(thresholds), jobs.MaxDeployReminders; got != want {
		t.Fatalf("len(deployReminderStageThresholds) = %d; want %d (must match maxDeployReminders)", got, want)
	}

	// Pinning: exact stage values. A future PR that touches any of these
	// must update this test deliberately — that's the whole point.
	wantStages := []time.Duration{
		12 * time.Hour, // Stage 1: Heads up
		6 * time.Hour,  // Stage 2: Reminder
		1 * time.Hour,  // Stage 3: Final reminder
	}
	for i, w := range wantStages {
		if thresholds[i] != w {
			t.Errorf("Stage %d threshold = %v; want %v — F3 escalating cadence (Wave 3)", i+1, thresholds[i], w)
		}
	}

	// Invariant: strictly decreasing — the cadence MUST escalate. A flat
	// or non-monotonic schedule means "Final reminder" can fire before
	// "Heads up", which is the bug F3 set out to kill.
	for i := 1; i < len(thresholds); i++ {
		if thresholds[i] >= thresholds[i-1] {
			t.Errorf("invariant violated: thresholds[%d] (%v) >= thresholds[%d] (%v); cadence must STRICTLY escalate (decreasing time-to-expiry per stage)",
				i, thresholds[i], i-1, thresholds[i-1])
		}
	}

	// Boundary: nextReminderThreshold(N) for N out-of-range returns 0
	// (caller is expected to short-circuit on 0 = no further reminder).
	if got := jobs.NextReminderThreshold(-1); got != 0 {
		t.Errorf("NextReminderThreshold(-1) = %v; want 0 (out-of-range)", got)
	}
	if got := jobs.NextReminderThreshold(jobs.MaxDeployReminders); got != 0 {
		t.Errorf("NextReminderThreshold(maxDeployReminders) = %v; want 0 (all stages fired, stop)", got)
	}
	if got := jobs.NextReminderThreshold(jobs.MaxDeployReminders + 5); got != 0 {
		t.Errorf("NextReminderThreshold(N+5) = %v; want 0 (beyond final stage)", got)
	}

	// Boundary: nextReminderThreshold(N) for N in-range returns the i-th stage.
	for i := 0; i < jobs.MaxDeployReminders; i++ {
		if got, want := jobs.NextReminderThreshold(i), thresholds[i]; got != want {
			t.Errorf("NextReminderThreshold(%d) = %v; want %v", i, got, want)
		}
	}
}
