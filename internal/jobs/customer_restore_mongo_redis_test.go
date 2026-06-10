package jobs

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

// fakeMongoRestore mirrors fakePgRestore: records the conn + gunzipped body
// so the test can assert the runner dispatched a mongodb row to mongorestore
// (not pg_restore) and that the gunzip step round-trips.
type fakeMongoRestore struct {
	err     error
	gotConn string
	gotBody []byte
}

func (f *fakeMongoRestore) Run(_ context.Context, connURL string, r io.Reader) error {
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

// TestRestoreForResourceType_Dispatch — the restore runner picks pg_restore
// for postgres/vector, mongorestore for mongodb, and returns nil + an
// explicit reason for redis (not yet supported) and unknown types.
func TestRestoreForResourceType_Dispatch(t *testing.T) {
	w := &CustomerRestoreRunnerWorker{
		pgRestore:    &fakePgRestore{},
		mongoRestore: &fakeMongoRestore{},
	}
	for _, rt := range []string{"postgres", "vector", "mongodb"} {
		fn, reason := w.restoreForResourceType(rt)
		if fn == nil {
			t.Errorf("restoreForResourceType(%q) = nil (reason %q); want a restorer", rt, reason)
		}
		if reason != "" {
			t.Errorf("restoreForResourceType(%q) reason = %q; want empty", rt, reason)
		}
	}
	// Redis restore is the tracked R2 follow-up — nil + a customer-readable reason.
	if fn, reason := w.restoreForResourceType("redis"); fn != nil || reason == "" {
		t.Errorf("restoreForResourceType(redis) = (%v, %q); want (nil, non-empty follow-up reason)", fn != nil, reason)
	}
	// Unknown type → nil + reason.
	if fn, reason := w.restoreForResourceType("webhook"); fn != nil || reason == "" {
		t.Errorf("restoreForResourceType(webhook) = (%v, %q); want (nil, non-empty)", fn != nil, reason)
	}
	// Nil mongo restorer (misconfigured boot) → nil + reason, never panic.
	wNoMongo := &CustomerRestoreRunnerWorker{pgRestore: &fakePgRestore{}}
	if fn, reason := wNoMongo.restoreForResourceType("mongodb"); fn != nil || reason == "" {
		t.Errorf("nil mongoRestore: got (%v, %q); want (nil, non-empty)", fn != nil, reason)
	}
}

// TestRestoreRunner_MongoHappyPath — a mongodb restore row downloads the
// gzipped archive, verifies sha256, gunzips, and feeds mongorestore (NOT
// pg_restore). R2 proof that "1-click restore" is real for Mongo.
func TestRestoreRunner_MongoHappyPath(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	restoreID := "rrrrrrr0-1111-2222-3333-444444444444"
	resID := "22222222-2222-2222-2222-222222222222"
	backupID := "11111111-1111-1111-1111-111111111111"
	teamID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	s3Key := "backups/tok-mongo/" + backupID + ".dump.gz"
	plainConn := "mongodb://u:p@host:27017/db"
	encConn := encryptForTest(t, plainConn)

	store := newFakeBackupStore()
	payload := []byte("BSON-ARCHIVE-PAYLOAD")
	gzBytes := gzipFor(t, payload)
	store.objects["instant-shared/"+s3Key] = gzBytes
	storedSHA := sha256Hex(gzBytes)

	mock.ExpectQuery(`SELECT rr.id::text`).
		WithArgs(restoreBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "backup_id", "s3_key", "sha256",
			"connection_url", "resource_type", "token", "team_id",
		}).AddRow(restoreID, resID, backupID, s3Key, storedSHA, encConn, "mongodb", "tok-mongo", teamID))
	mock.ExpectQuery(`UPDATE resource_restores\s+SET status = 'running'`).
		WithArgs(restoreID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(restoreID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE resource_restores\s+SET status = 'ok'`).
		WithArgs(restoreID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))

	mr := &fakeMongoRestore{}
	// A pg restorer that errors if (incorrectly) invoked for a mongo row.
	w := &CustomerRestoreRunnerWorker{
		db:           db,
		store:        store,
		pgRestore:    &fakePgRestore{err: errPgDispatchGuard{}},
		mongoRestore: mr,
		bucket:       "instant-shared",
		aesKey:       testAESKeyHex,
		now:          time.Now,
		timeout:      time.Minute,
		batchN:       restoreBatchSize,
	}

	if err := w.Work(context.Background(), fakeRestoreJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	if mr.gotConn != plainConn {
		t.Errorf("mongorestore conn = %q; want decrypted %q", mr.gotConn, plainConn)
	}
	if !bytes.Equal(mr.gotBody, payload) {
		t.Errorf("mongorestore body = %q; want %q (gunzip step broken)", mr.gotBody, payload)
	}
}

// TestRestoreRunner_RedisUnsupported_MarksFailed — a redis restore row is
// marked failed with the explicit "not yet supported" reason BEFORE any S3
// download (the dispatch guard runs early). Confirms the R2 follow-up posture:
// redis backups exist + are downloadable, but in-place restore isn't wired.
func TestRestoreRunner_RedisUnsupported_MarksFailed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	restoreID := "rrrrrrr0-1111-2222-3333-444444444444"
	resID := "22222222-2222-2222-2222-222222222222"
	backupID := "11111111-1111-1111-1111-111111111111"
	teamID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	s3Key := "backups/tok-redis/" + backupID + ".dump.gz"
	encConn := encryptForTest(t, "redis://:pw@host:6379/0")

	mock.ExpectQuery(`SELECT rr.id::text`).
		WithArgs(restoreBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "backup_id", "s3_key", "sha256",
			"connection_url", "resource_type", "token", "team_id",
		}).AddRow(restoreID, resID, backupID, s3Key, "", encConn, "redis", "tok-redis", teamID))
	mock.ExpectQuery(`UPDATE resource_restores\s+SET status = 'running'`).
		WithArgs(restoreID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(restoreID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	// markRestoreFailed path — status=failed + audit row. NO download/finalize.
	mock.ExpectExec(`UPDATE resource_restores\s+SET status = 'failed'`).
		WithArgs(restoreID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))

	store := newFakeBackupStore()
	store.objects["instant-shared/"+s3Key] = gzipFor(t, []byte("rdb"))
	w := &CustomerRestoreRunnerWorker{
		db:           db,
		store:        store,
		pgRestore:    &fakePgRestore{},
		mongoRestore: &fakeMongoRestore{},
		bucket:       "instant-shared",
		aesKey:       testAESKeyHex,
		now:          time.Now,
		timeout:      time.Minute,
		batchN:       restoreBatchSize,
	}

	if err := w.Work(context.Background(), fakeRestoreJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("redis restore was not marked failed with the unsupported reason: %v", err)
	}
	// The S3 object must NOT have been downloaded/deleted — dispatch guard
	// runs before download.
	if len(store.deletes) != 0 {
		t.Errorf("redis restore touched S3 (%d deletes); guard should run before download", len(store.deletes))
	}
}
