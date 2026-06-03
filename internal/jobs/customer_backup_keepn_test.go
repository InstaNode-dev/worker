package jobs

// customer_backup_keepn_test.go — count-based retention ("keep last N healthy
// backups per resource", 2026-06-03 operator request). Covers runKeepLastNSweep:
// every status='ok' backup beyond the newest keepHealthyBackupsPerResource for
// its resource is retired (S3 object deleted + row soft-flagged via s3_key=NULL).

import (
	"context"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// deleteErrStore wraps the in-memory store but always errors on DeleteObject,
// exercising the per-victim S3-failure (skip) branch.
type deleteErrStore struct{ *fakeBackupStore }

func (d deleteErrStore) DeleteObject(_ context.Context, _, _ string) error {
	return errors.New("s3 unavailable")
}

func TestRunKeepLastNSweep_RetiresBeyondCap(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Window query returns the over-cap victims (rn > keep). Two here.
	mock.ExpectQuery(`row_number\(\) OVER`).
		WithArgs(keepHealthyBackupsPerResource).
		WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}).
			AddRow("b6", "backups/r/b6.dump.gz").
			AddRow("b7", "backups/r/b7.dump.gz"))
	mock.ExpectExec(`UPDATE resource_backups`).
		WithArgs("b6").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE resource_backups`).
		WithArgs("b7").WillReturnResult(sqlmock.NewResult(0, 1))

	store := newFakeBackupStore()
	w := &CustomerBackupRunnerWorker{db: db, store: store, bucket: "instant-shared"}
	w.runKeepLastNSweep(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
	if len(store.deletes) != 2 {
		t.Fatalf("expected 2 S3 objects retired, got %d (%v)", len(store.deletes), store.deletes)
	}
}

func TestRunKeepLastNSweep_NoVictimsWithinCap(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Resource has <= keep backups → window query returns nothing → no deletes.
	mock.ExpectQuery(`row_number\(\) OVER`).
		WithArgs(keepHealthyBackupsPerResource).
		WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))

	store := newFakeBackupStore()
	w := &CustomerBackupRunnerWorker{db: db, store: store, bucket: "instant-shared"}
	w.runKeepLastNSweep(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
	if len(store.deletes) != 0 {
		t.Fatalf("expected 0 retired when within cap, got %d", len(store.deletes))
	}
}

func TestRunKeepLastNSweep_QueryError_FailsSoft(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`row_number\(\) OVER`).
		WithArgs(keepHealthyBackupsPerResource).
		WillReturnError(errors.New("db blip"))
	w := &CustomerBackupRunnerWorker{db: db, store: newFakeBackupStore(), bucket: "b"}
	w.runKeepLastNSweep(context.Background()) // must not panic; fails soft
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
}

func TestRunKeepLastNSweep_ScanError_SkipsRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	// NULL s3_key cannot scan into a string → scan error → row skipped.
	mock.ExpectQuery(`row_number\(\) OVER`).
		WithArgs(keepHealthyBackupsPerResource).
		WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}).AddRow("b6", nil))
	store := newFakeBackupStore()
	w := &CustomerBackupRunnerWorker{db: db, store: store, bucket: "b"}
	w.runKeepLastNSweep(context.Background())
	if len(store.deletes) != 0 {
		t.Fatalf("scan-failed row must not be retired, got %d deletes", len(store.deletes))
	}
}

func TestRunKeepLastNSweep_DeleteError_SkipsUpdate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`row_number\(\) OVER`).
		WithArgs(keepHealthyBackupsPerResource).
		WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}).AddRow("b6", "backups/r/b6.dump.gz"))
	// No ExpectExec(UPDATE): an S3 delete failure must skip the row's DB update.
	w := &CustomerBackupRunnerWorker{db: db, store: deleteErrStore{newFakeBackupStore()}, bucket: "b"}
	w.runKeepLastNSweep(context.Background())
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations (UPDATE must NOT run on delete error): %v", err)
	}
}

func TestRunKeepLastNSweep_DBUpdateError_FailsSoft(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`row_number\(\) OVER`).
		WithArgs(keepHealthyBackupsPerResource).
		WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}).AddRow("b6", "backups/r/b6.dump.gz"))
	mock.ExpectExec(`UPDATE resource_backups`).
		WithArgs("b6").WillReturnError(errors.New("update blip"))
	store := newFakeBackupStore()
	w := &CustomerBackupRunnerWorker{db: db, store: store, bucket: "b"}
	w.runKeepLastNSweep(context.Background()) // soft-fails on the update error
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
	if len(store.deletes) != 1 {
		t.Fatalf("object should still be deleted before the failed DB update, got %d", len(store.deletes))
	}
}
