package jobs

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

// TestScheduler_SQLFilterIncludesAllSupportedTypes pins R2: the candidate
// SELECT must enqueue postgres, vector, mongodb AND redis (was postgres/vector
// only). sqlmock's regexp matcher asserts the issued statement carries every
// supported resource_type — a regression that drops mongodb/redis from the
// filter fails this expectation loudly (root rule 18: registry/SQL drift
// guard). The fixture drives one pro mongodb row to also prove the row makes
// it through the cadence gate to an INSERT.
func TestScheduler_SQLFilterIncludesAllSupportedTypes(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	resID := "fffffff0-1111-2222-3333-444444444444"

	// The SELECT must name all four supported types. A drift back to
	// postgres/vector-only fails this regex match.
	mock.ExpectQuery(`resource_type IN \('postgres', 'vector', 'mongodb', 'redis'\)`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}).
			AddRow(resID, "pro", teamID))
	mock.ExpectExec(`INSERT INTO resource_backups`).
		WithArgs(uuid.MustParse(resID), "pro").
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := NewCustomerBackupSchedulerWorker(db, schedulerPlans())
	w.now = func() time.Time { return time.Date(2026, 6, 10, 14, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("SQL filter does not include all four supported resource types: %v", err)
	}
}

// TestScheduler_EnqueuesMongoAndRedisForProTier — R2 core ask: a pro mongodb
// resource AND a pro redis resource each yield an INSERT (hourly cadence,
// same as pg). Drives both rows in one candidate set so the per-row cadence
// gate is exercised for the new types. The scheduler is type-agnostic post-R2
// (cadence is tier-driven), so this proves the SQL now surfaces them and the
// gate lets them through.
func TestScheduler_EnqueuesMongoAndRedisForProTier(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	mongoID := "11111111-1111-2222-3333-444444444444"
	redisID := "22222222-1111-2222-3333-444444444444"

	mock.ExpectQuery(`SELECT r.id::text, r.tier, r.team_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}).
			AddRow(mongoID, "pro", teamID).
			AddRow(redisID, "pro", teamID))
	// Both rows are pro (hourly) → both INSERT.
	mock.ExpectExec(`INSERT INTO resource_backups`).
		WithArgs(uuid.MustParse(mongoID), "pro").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO resource_backups`).
		WithArgs(uuid.MustParse(redisID), "pro").
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := NewCustomerBackupSchedulerWorker(db, schedulerPlans())
	w.now = func() time.Time { return time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("mongo/redis pro rows not both enqueued hourly: %v", err)
	}
}

// TestScheduler_FreeTierMongoNeverEnqueued — R2 hard requirement preserved:
// a free-tier mongodb/redis resource is NEVER enqueued. The SQL excludes
// anonymous/free up front, so even with mongodb/redis now in the type filter
// the candidate set is empty and no INSERT is issued.
func TestScheduler_FreeTierMongoNeverEnqueued(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// SQL filters anonymous/free → empty result even though the row is a
	// free mongodb resource.
	mock.ExpectQuery(`SELECT r.id::text, r.tier, r.team_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}))
	// No INSERT expected.

	w := NewCustomerBackupSchedulerWorker(db, schedulerPlans())
	w.now = func() time.Time { return time.Date(2026, 6, 10, 14, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestScheduler_FreeMongoGate_SkipsEvenIfRowLeaksThrough — defence-in-depth:
// even if a free mongodb row leaked past the SQL filter, the per-row registry
// cadence gate (cadenceForTier → cadenceNever) vetoes it. No INSERT.
func TestScheduler_FreeMongoGate_SkipsEvenIfRowLeaksThrough(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	resID := "fffffff0-1111-2222-3333-444444444444"

	mock.ExpectQuery(`SELECT r.id::text, r.tier, r.team_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}).
			AddRow(resID, "free", teamID))
	// No INSERT — registry cadence gate must veto the free row regardless of type.

	w := NewCustomerBackupSchedulerWorker(db, schedulerPlans())
	w.now = func() time.Time { return time.Date(2026, 6, 10, 14, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("free mongo row was not skipped by the registry gate: %v", err)
	}
}
