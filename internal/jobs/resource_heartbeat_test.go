package jobs_test

// resource_heartbeat_test.go — hermetic tests for the heartbeat job.
//
// Black-box approach: sqlmock seeds the SELECT result; we assert on the
// UPDATEs and audit_log INSERTs the worker issues.
//
// Scenarios:
//
//   1. Healthy resource (was-NOT-degraded → probe OK) → UPDATE last_seen_at
//      + clear degraded; NO audit row (no state transition).
//   2. Recovery (was-degraded → probe OK) → UPDATE + INSERT
//      audit_log resource.recovered row.
//   3. Failure (was-NOT-degraded → probe FAIL) → UPDATE degraded=true
//      + INSERT audit_log resource.degraded row.
//   4. Empty rowset — no-op.
//   5. Top-level SELECT error — propagates so River retries.
//   6. ProbeSkip — no UPDATE, no audit_log.
//
// All tests use the fakeProber from provisioner_reconciler_test.go.

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	"instant.dev/worker/internal/jobs"
)

// heartbeatRowCols is the column order the heartbeat's SELECT returns.
// Keep in sync with resource_heartbeat.go::Work's SELECT projection.
var heartbeatRowCols = []string{
	"id", "token", "resource_type", "connection_url",
	"team_id_text", "degraded", "last_seen_at",
}

func TestResourceHeartbeat_HealthyNoTransition_NoAuditRow(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	token := uuid.New()
	teamID := uuid.New().String()

	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows(heartbeatRowCols).
			AddRow(resID, token, "postgres", "url", teamID, false, sql.NullTime{Time: time.Now().Add(-30 * time.Minute), Valid: true}))

	// markHealthy: UPDATE only (was-not-degraded → no audit row).
	mock.ExpectExec(`UPDATE resources\s+SET last_seen_at = NOW\(\), degraded = false`).
		WithArgs(resID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Gauge sample query.
	mock.ExpectQuery(`SELECT resource_type, COUNT\(\*\)\s+FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"resource_type", "count"}))

	prober := &fakeProber{outcome: jobs.ProbeReachable}
	w := jobs.NewResourceHeartbeatWorker(db, prober)
	if err := w.Work(context.Background(), fakeJob[jobs.ResourceHeartbeatArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestResourceHeartbeat_RecoveryEmitsAuditRow(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	token := uuid.New()
	teamID := uuid.New().String()

	// wasDegraded=true so probe success triggers a resource.recovered row.
	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows(heartbeatRowCols).
			AddRow(resID, token, "redis", "url", teamID, true, sql.NullTime{Time: time.Now().Add(-1 * time.Hour), Valid: true}))

	mock.ExpectExec(`UPDATE resources\s+SET last_seen_at = NOW\(\), degraded = false`).
		WithArgs(resID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", "resource.recovered", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectQuery(`SELECT resource_type, COUNT\(\*\)\s+FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"resource_type", "count"}))

	prober := &fakeProber{outcome: jobs.ProbeReachable}
	w := jobs.NewResourceHeartbeatWorker(db, prober)
	if err := w.Work(context.Background(), fakeJob[jobs.ResourceHeartbeatArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestResourceHeartbeat_FailureEmitsDegradedAuditRow(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	token := uuid.New()
	teamID := uuid.New().String()

	// wasDegraded=false → probe failure transitions false→true → audit row.
	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows(heartbeatRowCols).
			AddRow(resID, token, "postgres", "url", teamID, false, sql.NullTime{Time: time.Now().Add(-1 * time.Hour), Valid: true}))

	// markDegraded: UPDATE with rows-affected=1 (the flap guard let it
	// through because was-not-degraded), then INSERT audit_log.
	mock.ExpectExec(`UPDATE resources\s+SET degraded = true`).
		WithArgs(resID, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", "resource.degraded", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectQuery(`SELECT resource_type, COUNT\(\*\)\s+FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"resource_type", "count"}).
			AddRow("postgres", int64(1)))

	prober := &fakeProber{outcome: jobs.ProbeUnreachable, err: errors.New("dial tcp 10.0.0.1:5432: i/o timeout")}
	w := jobs.NewResourceHeartbeatWorker(db, prober)
	if err := w.Work(context.Background(), fakeJob[jobs.ResourceHeartbeatArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestResourceHeartbeat_EmptyRowsetIsNoOp(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows(heartbeatRowCols))

	w := jobs.NewResourceHeartbeatWorker(db, &fakeProber{outcome: jobs.ProbeReachable})
	if err := w.Work(context.Background(), fakeJob[jobs.ResourceHeartbeatArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestResourceHeartbeat_TopLevelQueryError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`FROM resources`).WillReturnError(errDB)

	w := jobs.NewResourceHeartbeatWorker(db, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.ResourceHeartbeatArgs]()); err == nil {
		t.Fatal("expected error from top-level SELECT failure, got nil")
	}
}

// TestResourceHeartbeat_ProbeSkipIsNoOp verifies the webhook / unknown-type
// path: ProbeSkip → no UPDATE, no audit row, no flap-spam.
func TestResourceHeartbeat_ProbeSkipIsNoOp(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	token := uuid.New()
	teamID := uuid.New().String()

	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows(heartbeatRowCols).
			AddRow(resID, token, "webhook", "", teamID, false, sql.NullTime{}))

	// No UPDATE / no INSERT — only the gauge sample query at the end.
	mock.ExpectQuery(`SELECT resource_type, COUNT\(\*\)\s+FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"resource_type", "count"}))

	prober := &fakeProber{outcome: jobs.ProbeSkip}
	w := jobs.NewResourceHeartbeatWorker(db, prober)
	if err := w.Work(context.Background(), fakeJob[jobs.ResourceHeartbeatArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
