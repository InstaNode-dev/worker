package jobs

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"instant.dev/worker/internal/metrics"
)

// expectBackupRetentionTail mocks the trailing per-tick retention sweep: one
// empty SELECT per tier name (five tier names in the fallback list). Mirrors
// the tail every other runner test mocks. The keep-last-N sweep that follows
// is deliberately NOT mocked — sqlmock returns an error for the unmocked
// query, which runKeepLastNSweep swallows (fail-soft), exactly as the
// pre-existing TestRunner_HappyPath relies on.
func expectBackupRetentionTail(mock sqlmock.Sqlmock) {
	for i := 0; i < 5; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key\s+FROM resource_backups`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}
}

// runBackupHappyPath drives one resource of the given type through the runner
// and asserts (a) the right dumper was hit with the decrypted conn and (b) the
// gzip+upload landed one object. Shared by the mongo + redis tests so the
// pipeline contract is asserted identically across types.
func runBackupHappyPath(t *testing.T, resourceType string, dumper interface {
	connSeen() string
}) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backupID := "11111111-1111-1111-1111-111111111111"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	token := "tok-" + resourceType
	plainConn := "scheme://u:p@host/db"
	encConn := encryptForTest(t, plainConn)

	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text, b.resource_id::text, b.tier_at_backup`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}).AddRow(backupID, resID, "pro", "scheduled", token, encConn, resourceType, teamID))
	mock.ExpectQuery(`UPDATE resource_backups\s+SET status = 'running'`).
		WithArgs(backupID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(backupID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'ok'`).
		WithArgs(backupID, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	expectBackupRetentionTail(mock)

	store := newFakeBackupStore()
	w := &CustomerBackupRunnerWorker{
		db:      db,
		store:   store,
		pgDump:  &fakePgDump{err: errAssertNotPg},
		bucket:  "instant-shared",
		prefix:  "backups",
		aesKey:  testAESKeyHex,
		now:     time.Now,
		timeout: time.Minute,
		batchN:  backupBatchSize,
	}
	switch resourceType {
	case resourceTypeMongoDB:
		w.mongoDump = dumper.(*fakeMongoDump)
	case resourceTypeRedis:
		w.redisDump = dumper.(*fakeRedisDump)
	}

	before := testutil.ToFloat64(metrics.CustomerBackupByTypeTotal.WithLabelValues(resourceType, "ok"))

	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	if got := dumper.connSeen(); got != plainConn {
		t.Errorf("%s dumper.connSeen() = %q; want decrypted %q", resourceType, got, plainConn)
	}
	wantKey := "backups/" + token + "/" + backupID + ".dump.gz"
	if len(store.uploads) != 1 || store.uploads[0].key != wantKey {
		t.Fatalf("%s uploads = %+v; want one object keyed %q", resourceType, store.uploads, wantKey)
	}
	after := testutil.ToFloat64(metrics.CustomerBackupByTypeTotal.WithLabelValues(resourceType, "ok"))
	if after != before+1 {
		t.Errorf("CustomerBackupByTypeTotal{%s,ok} = %v; want %v (+1)", resourceType, after, before+1)
	}
}

// errAssertNotPg is wired into the pg dumper for the mongo/redis happy-path
// tests: if the runner mis-dispatches a mongodb/redis row to pg_dump, the
// failure surfaces loudly rather than silently producing a pg backup.
var errAssertNotPg = errPgDispatchGuard{}

type errPgDispatchGuard struct{}

func (errPgDispatchGuard) Error() string {
	return "pg dumper was invoked for a non-postgres resource_type — dispatch regressed"
}

// connSeen adapters so runBackupHappyPath can read back the conn the fake
// dumper recorded without a type switch at the call site.
func (f *fakeMongoDump) connSeen() string { return f.gotConn }
func (f *fakeRedisDump) connSeen() string { return f.gotConn }

// TestRunner_MongoHappyPath — a mongodb resource is dumped via the mongo
// strategy (NOT pg_dump), gzipped, uploaded, finalized, and the per-type
// metric is incremented. This is the R2 core proof for Mongo.
func TestRunner_MongoHappyPath(t *testing.T) {
	runBackupHappyPath(t, resourceTypeMongoDB, &fakeMongoDump{payload: []byte("BSONARCHIVE")})
}

// TestRunner_RedisHappyPath — a redis resource is dumped via the redis
// strategy (NOT pg_dump), gzipped, uploaded, finalized, per-type metric
// incremented. R2 core proof for Redis.
func TestRunner_RedisHappyPath(t *testing.T) {
	runBackupHappyPath(t, resourceTypeRedis, &fakeRedisDump{payload: []byte("REDIS0011RDBBODY")})
}

// TestRunner_UnsupportedType_MarksFailed — a manual backup against a type the
// ladder can't dump (e.g. a webhook resource) marks the row failed with a
// 'config' reason rather than panicking or hanging. Defence-in-depth: the
// scheduler SQL never enqueues these, but a direct api call could.
func TestRunner_UnsupportedType_MarksFailed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backupID := "11111111-1111-1111-1111-111111111111"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	encConn := encryptForTest(t, "scheme://u:p@host/db")

	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}).AddRow(backupID, resID, "pro", "manual", "tok", encConn, "webhook", teamID))
	mock.ExpectQuery(`UPDATE resource_backups\s+SET status = 'running'`).
		WithArgs(backupID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(backupID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	// markFailed path: status=failed + audit row.
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'failed'`).
		WithArgs(backupID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	expectBackupRetentionTail(mock)

	w := &CustomerBackupRunnerWorker{
		db:      db,
		store:   newFakeBackupStore(),
		pgDump:  &fakePgDump{payload: []byte("x")},
		bucket:  "instant-shared",
		prefix:  "backups",
		aesKey:  testAESKeyHex,
		now:     time.Now,
		timeout: time.Minute,
		batchN:  backupBatchSize,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
