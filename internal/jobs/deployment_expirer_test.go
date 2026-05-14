package jobs_test

// deployment_expirer_test.go — Wave FIX-J coverage for
// DeploymentExpirerWorker. Asserts the candidate query + guarded
// status=expired UPDATE + audit_log INSERT.
//
// Migration note (2026-05-14, FIX-I/J→Brevo): the worker no longer
// calls EmailClient.SendDeployExpired inline. The audit_log row IS
// the trigger — the BrevoForwarder consumes it on its next 60s tick.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"instant.dev/worker/internal/jobs"
)

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

	w := jobs.NewDeploymentExpirerWorker(db, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.DeploymentExpirerArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestDeploymentExpirerWorker_ExpiresAndWritesAudit: a single expired
// row is soft-deleted (UPDATE) and an audit row is written. The audit
// payload must carry app_id so the BrevoForwarder's buildDeployExpired
// builder can populate {{ params.deploy_name }} in the template.
//
// Was previously TestDeploymentExpirerWorker_ExpiresAndEmails (asserted
// inline EmailClient.SendDeployExpired). Migrated 2026-05-14: assertion
// is now on the audit_log INSERT, which is the new trigger surface.
// This test FAILS on master and PASSES post-migration.
func TestDeploymentExpirerWorker_ExpiresAndWritesAudit(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
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

	// audit_log INSERT — assertion is on the metadata payload.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(
			sqlmock.AnyArg(), // team_uuid
			"deploy myapp expired",
			deployExpiredMetaMatcher{deployID: "deploy-x", appID: "myapp", ttlPolicy: "auto_24h"},
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := jobs.NewDeploymentExpirerWorker(db, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.DeploymentExpirerArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The audit insert is fire-and-forget in a goroutine; give it a
	// moment to land before checking expectations.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if err := mock.ExpectationsWereMet(); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations after 500ms wait: %v", err)
	}
}

// TestDeploymentExpirerWorker_AlreadyExpiredSkipsAudit: when the guarded
// UPDATE returns rowsAffected=0 (concurrent expirer/delete won the race),
// the worker must NOT write an audit row (no email duplication).
func TestDeploymentExpirerWorker_AlreadyExpiredSkipsAudit(t *testing.T) {
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

	// No INSERT INTO audit_log expected — race-lost path must NOT emit.

	w := jobs.NewDeploymentExpirerWorker(db, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.DeploymentExpirerArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// deployExpiredMetaMatcher inspects the audit_log metadata JSON blob
// to assert every field the BrevoForwarder's buildDeployExpired builder
// reads is present.
type deployExpiredMetaMatcher struct {
	deployID  string
	appID     string
	ttlPolicy string
}

func (m deployExpiredMetaMatcher) Match(v driver.Value) bool {
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
	if asStr(doc["ttl_policy"]) != m.ttlPolicy {
		return false
	}
	return true
}
