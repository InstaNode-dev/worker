package jobs_test

import (
	"context"
	"sync"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	minio "github.com/minio/minio-go/v7"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"instant.dev/worker/internal/jobs"
)

// fakeObjectDeleter is a fake S3BackupDeleter used to assert the storage-
// expiry path actually drives an object delete against the object store.
// It records the ListObjects prefixes it was asked for and emits the
// configured objects into RemoveObjects.
type fakeObjectDeleter struct {
	mu             sync.Mutex
	listedPrefixes []string
	listedBuckets  []string
	objects        []minio.ObjectInfo // streamed into both List + Remove
	removeCalled   bool
}

func (f *fakeObjectDeleter) ListObjects(_ context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	f.mu.Lock()
	f.listedPrefixes = append(f.listedPrefixes, opts.Prefix)
	f.listedBuckets = append(f.listedBuckets, bucket)
	f.mu.Unlock()
	ch := make(chan minio.ObjectInfo)
	go func() {
		defer close(ch)
		for _, o := range f.objects {
			ch <- o
		}
	}()
	return ch
}

func (f *fakeObjectDeleter) RemoveObjects(_ context.Context, _ string, objectsCh <-chan minio.ObjectInfo, _ minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError {
	f.mu.Lock()
	f.removeCalled = true
	f.mu.Unlock()
	out := make(chan minio.RemoveObjectError)
	go func() {
		defer close(out)
		// Drain the input channel so the producer goroutine completes.
		for range objectsCh {
		}
	}()
	return out
}

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

// TestExpireAnonymousWorker_ExpiresPausedAndSuspended proves the expiry query
// (and the mark-deleted UPDATE) cover paused/suspended anonymous resources, not
// just status='active'. Regression guard for the P2 bug where a paused or
// suspended anonymous resource whose 24h TTL had elapsed was never expired —
// the SELECT filtered status='active' only, orphaning the physical resource.
// TTL must win over lifecycle state.
func TestExpireAnonymousWorker_ExpiresPausedAndSuspended(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// The SELECT must include the paused/suspended statuses, not just active.
	mock.ExpectQuery(`status IN \('active', 'paused', 'suspended'\)`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"}).
			AddRow("id-paused", "tok-p", "postgres", "").
			AddRow("id-susp", "tok-s", "redis", ""))

	// The mark-deleted UPDATE must be guarded on the same expanded status set
	// so a paused/suspended row actually transitions to 'deleted'.
	for i := 0; i < 2; i++ {
		mock.ExpectExec(`UPDATE resources SET status = 'deleted'\s+WHERE id = \$1 AND status IN \('active', 'paused', 'suspended'\)`).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	// The trailing active-anon count query.
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	w := jobs.NewExpireAnonymousWorker(db, nil, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestExpireAnonymousWorker_StorageExpiry_DeletesObjects proves a storage
// resource's objects are actually deleted on expiry via the wired
// S3-compatible object deleter — regression for the bug where the storage
// path was a pure no-op on the DO Spaces backend (the row flipped to
// 'deleted' but the tenant's objects were never removed). It also asserts
// the deleter is asked for the canonical provider_resource_id-derived prefix,
// not a hand-rolled one.
func TestExpireAnonymousWorker_StorageExpiry_DeletesObjects(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// One expired storage resource. provider_resource_id is the canonical
	// object prefix stamped at provision time — minioObjectPrefix uses it
	// verbatim (appending a trailing slash).
	const providerResourceID = "stor_fulltoken_abc"
	mock.ExpectQuery(`SELECT id::text`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"}).
			AddRow("id-stor", "tok-stor", "storage", providerResourceID))
	mock.ExpectExec(`UPDATE resources SET status = 'deleted'`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	deleter := &fakeObjectDeleter{
		objects: []minio.ObjectInfo{{Key: providerResourceID + "/file1"}},
	}
	w := jobs.NewExpireAnonymousWorker(db, nil, nil).
		WithObjectDeleter(deleter, "instant-shared")
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}

	if !deleter.removeCalled {
		t.Error("expected RemoveObjects to be invoked for the expired storage resource")
	}
	if len(deleter.listedPrefixes) != 1 {
		t.Fatalf("expected exactly one ListObjects call; got %d", len(deleter.listedPrefixes))
	}
	// The deleter must be driven against the canonical provider_resource_id
	// prefix (with the trailing slash minioObjectPrefix appends), NOT a
	// hand-rolled scheme.
	if got, want := deleter.listedPrefixes[0], providerResourceID+"/"; got != want {
		t.Errorf("ListObjects prefix = %q; want %q (canonical provider_resource_id prefix)", got, want)
	}
	if got, want := deleter.listedBuckets[0], "instant-shared"; got != want {
		t.Errorf("ListObjects bucket = %q; want %q", got, want)
	}
}

// TestExpireAnonymousWorker_StorageExpiry_NoDeleterWarns proves that when no
// object deleter is wired (OBJECT_STORE_* unset — CI / docker-compose), the
// storage-expiry path does NOT silently no-op: the row still flips to
// 'deleted' (so the expiry sweep makes progress) and the missing cleanup
// path is left visible via a WARN. The test asserts the row transition still
// happens; the WARN itself is a slog side-effect verified by inspection.
func TestExpireAnonymousWorker_StorageExpiry_NoDeleterWarns(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT id::text`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"}).
			AddRow("id-stor", "tok-stor", "storage", "stor_xyz"))
	mock.ExpectExec(`UPDATE resources SET status = 'deleted'`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	// No deleter wired (nil) — the storage case logs a WARN, the row still
	// transitions to 'deleted'.
	w := jobs.NewExpireAnonymousWorker(db, nil, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
