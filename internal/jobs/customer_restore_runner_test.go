package jobs

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

func fakeRestoreJob() *river.Job[CustomerRestoreRunnerArgs] {
	return &river.Job[CustomerRestoreRunnerArgs]{JobRow: &rivertype.JobRow{ID: 1}}
}

// fakePgRestore records what it was fed; the test then asserts both that
// the connection_url was decrypted correctly AND that the gunzipped payload
// matches the bytes the fakePgDump originally produced.
type fakePgRestore struct {
	err     error
	gotConn string
	gotBody []byte
}

func (f *fakePgRestore) Run(_ context.Context, connURL string, r io.Reader) error {
	f.gotConn = connURL
	if f.err != nil {
		return f.err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.gotBody = data
	return nil
}

// gzipFor returns a gzip-compressed bytes-buffer of payload, suitable for
// pre-seeding the fakeBackupStore so the restore runner has something to
// download.
func gzipFor(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(payload); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

// TestRestoreRunner_HappyPath — claim, download, gunzip, pg_restore,
// finalize. Verifies the gunzip step works by asserting the fake
// pg_restore saw the original (pre-gzip) payload bytes.
func TestRestoreRunner_HappyPath(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	restoreID := "rrrrrrr0-1111-2222-3333-444444444444"
	resID := "22222222-2222-2222-2222-222222222222"
	backupID := "11111111-1111-1111-1111-111111111111"
	teamID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	s3Key := "backups/tok-abc/" + backupID + ".dump.gz"
	plainConn := "postgres://u:p@host/db"
	encConn := encryptForTest(t, plainConn)

	mock.ExpectQuery(`SELECT rr.id::text`).
		WithArgs(restoreBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "backup_id", "s3_key",
			"connection_url", "resource_type", "token", "team_id",
		}).AddRow(restoreID, resID, backupID, s3Key, encConn, "postgres", "tok-abc", teamID))

	mock.ExpectQuery(`UPDATE resource_restores\s+SET status = 'running'`).
		WithArgs(restoreID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(restoreID))

	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Finalize.
	mock.ExpectExec(`UPDATE resource_restores\s+SET status = 'ok'`).
		WithArgs(restoreID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// restore.succeeded audit.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	store := newFakeBackupStore()
	payload := []byte("PG-RESTORE-PAYLOAD")
	store.objects["instant-shared/"+s3Key] = gzipFor(t, payload)

	pgr := &fakePgRestore{}
	w := &CustomerRestoreRunnerWorker{
		db:        db,
		store:     store,
		pgRestore: pgr,
		bucket:    "instant-shared",
		aesKey:    testAESKeyHex,
		now:       time.Now,
		timeout:   time.Minute,
		batchN:    restoreBatchSize,
	}

	if err := w.Work(context.Background(), fakeRestoreJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	if pgr.gotConn != plainConn {
		t.Errorf("pg_restore conn = %q; want decrypted %q", pgr.gotConn, plainConn)
	}
	if !bytes.Equal(pgr.gotBody, payload) {
		t.Errorf("pg_restore body = %q; want %q (gunzip step is broken)", pgr.gotBody, payload)
	}
}

// TestRestoreRunner_NullS3Key_FailsWithExplicitMessage — retention may
// have purged the backup between the api's check and the runner picking it
// up. The row goes to 'failed' with the explicit reason.
func TestRestoreRunner_NullS3Key_FailsWithExplicitMessage(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	restoreID := "rrrrrrr0-1111-2222-3333-444444444444"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	encConn := encryptForTest(t, "postgres://u:p@host/db")

	mock.ExpectQuery(`SELECT rr.id::text`).
		WithArgs(restoreBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "backup_id", "s3_key",
			"connection_url", "resource_type", "token", "team_id",
		}).AddRow(restoreID, resID, "bk", nil, encConn, "postgres", "tok-abc", teamID))

	mock.ExpectQuery(`UPDATE resource_restores\s+SET status = 'running'`).
		WithArgs(restoreID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(restoreID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE resource_restores\s+SET status = 'failed'`).
		WithArgs(restoreID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))

	w := &CustomerRestoreRunnerWorker{
		db:        db,
		store:     newFakeBackupStore(),
		pgRestore: &fakePgRestore{},
		bucket:    "instant-shared",
		aesKey:    testAESKeyHex,
		now:       time.Now,
		timeout:   time.Minute,
		batchN:    restoreBatchSize,
	}

	if err := w.Work(context.Background(), fakeRestoreJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestRestoreRunner_PgRestoreFails_MarksFailed — pg_restore subprocess
// failure surfaces as status='failed' + audit row.
func TestRestoreRunner_PgRestoreFails_MarksFailed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	restoreID := "rrrrrrr0-1111-2222-3333-444444444444"
	resID := "22222222-2222-2222-2222-222222222222"
	backupID := "11111111-1111-1111-1111-111111111111"
	teamID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	s3Key := "backups/tok-abc/" + backupID + ".dump.gz"
	encConn := encryptForTest(t, "postgres://u:p@host/db")

	mock.ExpectQuery(`SELECT rr.id::text`).
		WithArgs(restoreBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "backup_id", "s3_key",
			"connection_url", "resource_type", "token", "team_id",
		}).AddRow(restoreID, resID, backupID, s3Key, encConn, "postgres", "tok-abc", teamID))

	mock.ExpectQuery(`UPDATE resource_restores\s+SET status = 'running'`).
		WithArgs(restoreID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(restoreID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(`UPDATE resource_restores\s+SET status = 'failed'`).
		WithArgs(restoreID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))

	store := newFakeBackupStore()
	store.objects["instant-shared/"+s3Key] = gzipFor(t, []byte("PG-RESTORE-PAYLOAD"))

	w := &CustomerRestoreRunnerWorker{
		db:        db,
		store:     store,
		pgRestore: &fakePgRestore{err: errors.New("pg_restore: relation already exists")},
		bucket:    "instant-shared",
		aesKey:    testAESKeyHex,
		now:       time.Now,
		timeout:   time.Minute,
		batchN:    restoreBatchSize,
	}

	if err := w.Work(context.Background(), fakeRestoreJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestRestoreRunner_NilStore_NoOp — same fail-open behavior as the backup
// runner. Worker without store config = silent skip.
func TestRestoreRunner_NilStore_NoOp(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	w := &CustomerRestoreRunnerWorker{
		db:     db,
		store:  nil,
		aesKey: testAESKeyHex,
		now:    time.Now,
	}
	if err := w.Work(context.Background(), fakeRestoreJob()); err != nil {
		t.Fatalf("expected no-op, got %v", err)
	}
}
