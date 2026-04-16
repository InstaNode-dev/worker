package jobs_test

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"instant.dev/worker/internal/jobs"
)

// fakeJob returns a minimal *river.Job for passing to Work().
// JobRow must be non-nil because workers log job.ID via the embedded *rivertype.JobRow.
func fakeJob[T river.JobArgs]() *river.Job[T] {
	return &river.Job[T]{JobRow: &rivertype.JobRow{ID: 1}}
}

func TestExpireAnonymousWorker_ExpiresStalResources(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// SELECT returns three expired resources.
	rows := sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"}).
		AddRow("id-1", "tok-1", "postgres", "").
		AddRow("id-2", "tok-2", "redis", "").
		AddRow("id-3", "tok-3", "mongodb", "")
	mock.ExpectQuery(`SELECT id::text`).WillReturnRows(rows)

	// One UPDATE per resource (nil provisioner = no deprovision RPC).
	for i := 0; i < 3; i++ {
		mock.ExpectExec(`UPDATE resources SET status = 'deleted'`).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}

	w := jobs.NewExpireAnonymousWorker(db, nil, nil) // nil = skip deprovision
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestExpireAnonymousWorker_ZeroExpired_NoError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Empty result — nothing to expire.
	rows := sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"})
	mock.ExpectQuery(`SELECT id::text`).WillReturnRows(rows)

	w := jobs.NewExpireAnonymousWorker(db, nil, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestExpireAnonymousWorker_DBError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT id::text`).WillReturnError(errDB)

	w := jobs.NewExpireAnonymousWorker(db, nil, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err == nil {
		t.Fatal("expected error, got nil")
	}
}
