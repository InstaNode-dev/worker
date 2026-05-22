package jobs

// weekly_digest_coverage_test.go — coverage-lifting tests for the
// WeeklyDigestWorker branches not exercised by the existing
// weekly_digest_test.go (which lives in jobs_test). Targets email.go's
// uncovered branches:
//   - Work() query error
//   - Work() rows.Err()
//   - Work() emitWeeklyDigestAudit error path
//   - Work() scan_users continue branch
//   - buildResourceDigestCounts query/scan/rows error paths
//
// Hermetic — sqlmock only.

import (
	"context"
	"fmt"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

func TestEventEmail_WeeklyDigest_QueryError_ReturnsError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	mock.ExpectQuery(`FROM users u\s+JOIN teams t`).WillReturnError(fmt.Errorf("query down"))

	w := NewWeeklyDigestWorker(db)
	err := w.Work(context.Background(), fakeJobLocal[WeeklyDigestArgs]())
	if err == nil {
		t.Errorf("expected error on outer users query failure")
	}
}

func TestEventEmail_WeeklyDigest_RowsErrAfterIteration(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	teamID := uuid.New()
	rows := sqlmock.NewRows([]string{"email", "id", "name"}).
		AddRow("u@example.com", teamID, "Acme").
		RowError(0, fmt.Errorf("rows err"))
	mock.ExpectQuery(`FROM users u\s+JOIN teams t`).WillReturnRows(rows)

	w := NewWeeklyDigestWorker(db)
	if err := w.Work(context.Background(), fakeJobLocal[WeeklyDigestArgs]()); err == nil {
		t.Errorf("expected error from rows.Err()")
	}
}

func TestEventEmail_WeeklyDigest_ScanError_ContinuesAndDoesNotFail(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	// Force scan error by feeding a non-UUID value into the teamID column.
	rows := sqlmock.NewRows([]string{"email", "id", "name"}).
		AddRow("u@example.com", "not-a-uuid", "Acme")
	mock.ExpectQuery(`FROM users u\s+JOIN teams t`).WillReturnRows(rows)

	w := NewWeeklyDigestWorker(db)
	if err := w.Work(context.Background(), fakeJobLocal[WeeklyDigestArgs]()); err != nil {
		t.Errorf("scan error must be per-row, not propagated; got %v", err)
	}
}

func TestEventEmail_WeeklyDigest_AuditInsertError_LoggedAndContinues(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	teamID := uuid.New()
	rows := sqlmock.NewRows([]string{"email", "id", "name"}).
		AddRow("u@example.com", teamID, "Acme")
	mock.ExpectQuery(`FROM users u\s+JOIN teams t`).WillReturnRows(rows)

	mock.ExpectQuery(`SELECT resource_type, COUNT\(\*\)::bigint`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows([]string{"resource_type", "count"}).AddRow("postgres", int64(1)))

	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnError(fmt.Errorf("audit write down"))

	w := NewWeeklyDigestWorker(db)
	if err := w.Work(context.Background(), fakeJobLocal[WeeklyDigestArgs]()); err != nil {
		t.Errorf("audit insert error must be per-row, not propagated; got %v", err)
	}
}

func TestEventEmail_WeeklyDigest_BuildResourceDigestCounts_ScanError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	teamID := uuid.New()
	// Outer query returns the user/team.
	mock.ExpectQuery(`FROM users u\s+JOIN teams t`).
		WillReturnRows(sqlmock.NewRows([]string{"email", "id", "name"}).
			AddRow("u@example.com", teamID, "Acme"))

	// Per-team breakdown returns a malformed count column so Scan fails.
	mock.ExpectQuery(`SELECT resource_type, COUNT\(\*\)::bigint`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows([]string{"resource_type", "count"}).
			AddRow("postgres", "not-an-int"))

	w := NewWeeklyDigestWorker(db)
	if err := w.Work(context.Background(), fakeJobLocal[WeeklyDigestArgs]()); err != nil {
		t.Errorf("breakdown scan error must be per-row, not propagated; got %v", err)
	}
}

func TestEventEmail_WeeklyDigest_BuildResourceDigestCounts_RowsErr(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	teamID := uuid.New()
	mock.ExpectQuery(`FROM users u\s+JOIN teams t`).
		WillReturnRows(sqlmock.NewRows([]string{"email", "id", "name"}).
			AddRow("u@example.com", teamID, "Acme"))

	rows := sqlmock.NewRows([]string{"resource_type", "count"}).
		AddRow("postgres", int64(1)).
		RowError(0, fmt.Errorf("rows.Err post-iter"))
	mock.ExpectQuery(`SELECT resource_type, COUNT\(\*\)::bigint`).
		WithArgs(teamID).
		WillReturnRows(rows)

	w := NewWeeklyDigestWorker(db)
	if err := w.Work(context.Background(), fakeJobLocal[WeeklyDigestArgs]()); err != nil {
		t.Errorf("rows.Err must be per-row fail-open; got %v", err)
	}
}

// TestEmitWeeklyDigestAudit_DirectInsertError exercises the
// emitWeeklyDigestAudit function in isolation, hitting its error wrap.
func TestEventEmail_EmitWeeklyDigestAudit_DirectInsertError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	teamID := uuid.New()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnError(fmt.Errorf("explicit insert error"))

	err := emitWeeklyDigestAudit(context.Background(), db, teamID, "u@example.com", "Acme",
		[]DigestResourceCount{{ResourceType: "postgres", Count: 1}})
	if err == nil {
		t.Errorf("emitWeeklyDigestAudit must surface insert error")
	}
}
