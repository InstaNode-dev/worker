package jobs

// bugbash2_coverage_internal_test.go — internal (package jobs) coverage for
// the bug-bash batch-2 changes whose lines aren't reachable from the external
// test package: the autopsy deploy.failed dedup-hit branch and the real
// dbGracePeriodOpener.TerminateActiveGracePeriod UPDATE. All hermetic (sqlmock).

import (
	"context"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

// #15: when a deploy.failed audit row already exists for the deployment,
// emitDeployFailedAudit must SKIP the INSERT (idempotent — no duplicate email).
func TestEmitDeployFailedAudit_SkipsWhenAlreadyEmitted(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	depID := uuid.New()

	mock.ExpectQuery(`SELECT team_id FROM deployments`).
		WithArgs(depID).
		WillReturnRows(sqlmock.NewRows([]string{"team_id"}).AddRow(uuid.New()))
	// Dedup probe finds an existing deploy.failed row → no INSERT must follow.
	mock.ExpectQuery(`SELECT EXISTS`).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	if err := emitDeployFailedAudit(context.Background(), db, depID, "BuildFailed", "boom"); err != nil {
		t.Fatalf("emitDeployFailedAudit: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("an INSERT must NOT run when a deploy.failed row already exists: %v", err)
	}
}

// #5: the real dbGracePeriodOpener.TerminateActiveGracePeriod issues the
// status→'terminated' UPDATE; an exec error is wrapped and returned.
func TestDBGracePeriodOpener_TerminateActiveGracePeriod(t *testing.T) {
	teamID := uuid.New()

	t.Run("success", func(t *testing.T) {
		db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
		defer db.Close()
		mock.ExpectExec(`UPDATE payment_grace_periods\s+SET status = 'terminated'`).
			WithArgs(teamID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		d := &dbGracePeriodOpener{db: db}
		if err := d.TerminateActiveGracePeriod(context.Background(), teamID); err != nil {
			t.Fatalf("TerminateActiveGracePeriod: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet: %v", err)
		}
	})

	t.Run("db error wrapped", func(t *testing.T) {
		db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
		defer db.Close()
		mock.ExpectExec(`UPDATE payment_grace_periods`).
			WithArgs(teamID).
			WillReturnError(errors.New("boom"))
		d := &dbGracePeriodOpener{db: db}
		if err := d.TerminateActiveGracePeriod(context.Background(), teamID); err == nil {
			t.Fatal("expected error to propagate")
		}
	})
}
