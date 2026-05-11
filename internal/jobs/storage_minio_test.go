package jobs

import (
	"context"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	minio "github.com/minio/minio-go/v7"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	commonv1 "instant.dev/proto/common/v1"
)

// fakeMinIOJob is the in-package twin of jobs_test.fakeJob — needed because
// this test file lives in package jobs (so it can reach the unexported
// minioObjectLister + newMinIOScannerWithClient test seams).
func fakeMinIOJob() *river.Job[UpdateStorageBytesArgs] {
	return &river.Job[UpdateStorageBytesArgs]{JobRow: &rivertype.JobRow{ID: 1}}
}

// fakeProvForMinIOTest is a StorageBytesProvider stub used only by the
// storage-row-skipped-when-scanner-nil test below. It never sees a call
// because that test feeds in only a storage row.
type fakeProvForMinIOTest struct{}

func (*fakeProvForMinIOTest) StorageBytes(_ context.Context, _, _ string, _ commonv1.ResourceType) (int64, error) {
	return 0, errors.New("unexpected provisioner call for storage row")
}

// fakeMinIOClient implements minioObjectLister. It returns canned objects /
// multipart parts so the scanner can be exercised without dialing a real
// MinIO server.
type fakeMinIOClient struct {
	bucketExists       bool
	bucketExistsErr    error
	objects            []minio.ObjectInfo
	listErr            error
	multipartParts     []minio.ObjectMultipartInfo
	gotPrefix          string
	gotBucket          string
	gotMultipartBucket string
	gotMultipartPrefix string
}

func (f *fakeMinIOClient) BucketExists(_ context.Context, bucket string) (bool, error) {
	f.gotBucket = bucket
	return f.bucketExists, f.bucketExistsErr
}

func (f *fakeMinIOClient) ListObjects(_ context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	f.gotBucket = bucket
	f.gotPrefix = opts.Prefix
	ch := make(chan minio.ObjectInfo, len(f.objects)+1)
	go func() {
		defer close(ch)
		if f.listErr != nil {
			ch <- minio.ObjectInfo{Err: f.listErr}
			return
		}
		for _, o := range f.objects {
			ch <- o
		}
	}()
	return ch
}

func (f *fakeMinIOClient) ListIncompleteUploads(_ context.Context, bucket, prefix string, _ bool) <-chan minio.ObjectMultipartInfo {
	f.gotMultipartBucket = bucket
	f.gotMultipartPrefix = prefix
	ch := make(chan minio.ObjectMultipartInfo, len(f.multipartParts)+1)
	go func() {
		defer close(ch)
		for _, p := range f.multipartParts {
			ch <- p
		}
	}()
	return ch
}

// TestMinIOScanner_SumsObjectSizes is the core unit: feed in N objects
// totaling X bytes and assert the scanner returns X.
func TestMinIOScanner_SumsObjectSizes(t *testing.T) {
	fake := &fakeMinIOClient{
		bucketExists: true,
		objects: []minio.ObjectInfo{
			{Key: "a1b2c3d4/file-1.bin", Size: 1024},
			{Key: "a1b2c3d4/file-2.bin", Size: 2048},
			{Key: "a1b2c3d4/sub/file-3.bin", Size: 4096},
		},
	}
	scanner := newMinIOScannerWithClient(fake, "instant-shared")
	bytes, err := scanner.StorageBytes(context.Background(), "a1b2c3d4deadbeef", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	const want = int64(1024 + 2048 + 4096)
	if bytes != want {
		t.Errorf("StorageBytes = %d; want %d", bytes, want)
	}
	if fake.gotPrefix != "a1b2c3d4/" {
		t.Errorf("ListObjects prefix = %q; want %q (must match api/internal/providers/storage/local.go convention)", fake.gotPrefix, "a1b2c3d4/")
	}
	if fake.gotBucket != "instant-shared" {
		t.Errorf("ListObjects bucket = %q; want %q", fake.gotBucket, "instant-shared")
	}
}

// TestMinIOScanner_IncludesIncompleteMultipart verifies that in-flight
// multipart uploads count toward the tenant's storage_bytes (mirrors the
// provisioner-side scanner).
func TestMinIOScanner_IncludesIncompleteMultipart(t *testing.T) {
	fake := &fakeMinIOClient{
		bucketExists: true,
		objects: []minio.ObjectInfo{
			{Key: "abcd1234/done.bin", Size: 100},
		},
		multipartParts: []minio.ObjectMultipartInfo{
			{Key: "abcd1234/in-flight.bin", Size: 500},
			{Key: "abcd1234/another.bin", Size: 50},
		},
	}
	scanner := newMinIOScannerWithClient(fake, "instant-shared")
	bytes, err := scanner.StorageBytes(context.Background(), "abcd1234ffffffff", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	const want = int64(100 + 500 + 50)
	if bytes != want {
		t.Errorf("StorageBytes = %d; want %d", bytes, want)
	}
}

// TestMinIOScanner_SkipsDeleteMarkersAndDirPlaceholders covers the two
// "don't count" cases — delete markers and zero-byte "directory" keys.
func TestMinIOScanner_SkipsDeleteMarkersAndDirPlaceholders(t *testing.T) {
	fake := &fakeMinIOClient{
		bucketExists: true,
		objects: []minio.ObjectInfo{
			{Key: "abcd1234/real.bin", Size: 200},
			{Key: "abcd1234/tombstoned.bin", Size: 999, IsDeleteMarker: true},
			{Key: "abcd1234/folder/", Size: 0},
		},
	}
	scanner := newMinIOScannerWithClient(fake, "instant-shared")
	bytes, err := scanner.StorageBytes(context.Background(), "abcd1234ffffffff", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bytes != 200 {
		t.Errorf("StorageBytes = %d; want %d (delete marker and dir placeholder must be skipped)", bytes, 200)
	}
}

// TestMinIOScanner_BucketMissing surfaces a hard error so the caller can
// log and fail-open rather than persist a spurious zero.
func TestMinIOScanner_BucketMissing(t *testing.T) {
	fake := &fakeMinIOClient{bucketExists: false}
	scanner := newMinIOScannerWithClient(fake, "instant-shared")
	if _, err := scanner.StorageBytes(context.Background(), "abcd1234ffffffff", ""); err == nil {
		t.Fatal("expected error when bucket does not exist")
	}
}

// TestMinIOScanner_ProviderResourceIDOverride verifies that a non-empty
// provider_resource_id takes precedence over the token-derived prefix.
func TestMinIOScanner_ProviderResourceIDOverride(t *testing.T) {
	fake := &fakeMinIOClient{bucketExists: true}
	scanner := newMinIOScannerWithClient(fake, "instant-shared")
	if _, err := scanner.StorageBytes(context.Background(), "ignored-token", "custom-prefix"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.gotPrefix != "custom-prefix/" {
		t.Errorf("ListObjects prefix = %q; want %q (provider_resource_id must win, with trailing slash appended)", fake.gotPrefix, "custom-prefix/")
	}
}

// TestUpdateStorageBytesWorker_MinIOResource_PersistsTotal exercises the
// full worker pipeline for a storage row: SELECT returns one resource of
// type 'storage', the MinIO scanner reports N bytes, and the worker writes
// storage_bytes = N back to the platform DB.
//
// This is the test the task spec calls for: "mock minio-go client with N
// objects totaling X bytes, asserts the worker updates storage_bytes = X
// in a sqlmock'd DB".
func TestUpdateStorageBytesWorker_MinIOResource_PersistsTotal(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resourceID := "11111111-2222-3333-4444-555555555555"
	const totalBytes = int64(1024 + 2048 + 4096) // 7168

	rows := sqlmock.NewRows([]string{"id", "token", "resource_type", "tier", "provider_resource_id"}).
		AddRow(resourceID, "a1b2c3d4deadbeef", "storage", "hobby", "")
	mock.ExpectQuery(`SELECT id, token`).WillReturnRows(rows)

	mock.ExpectExec(`UPDATE resources SET storage_bytes`).
		WithArgs(totalBytes, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	fake := &fakeMinIOClient{
		bucketExists: true,
		objects: []minio.ObjectInfo{
			{Key: "a1b2c3d4/one.bin", Size: 1024},
			{Key: "a1b2c3d4/two.bin", Size: 2048},
			{Key: "a1b2c3d4/three.bin", Size: 4096},
		},
	}
	scanner := newMinIOScannerWithClient(fake, "instant-shared")

	// provClient is nil — only the MinIO path should be exercised.
	w := NewUpdateStorageBytesWorker(db, nil, scanner)
	if err := w.Work(context.Background(), fakeMinIOJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestUpdateStorageBytesWorker_MinIOScannerError_FailOpen ensures a MinIO
// listing failure is logged and skipped (no UPDATE), not propagated as a
// job error — matches the provisioner-error fail-open behavior on the
// other types.
func TestUpdateStorageBytesWorker_MinIOScannerError_FailOpen(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resourceID := "11111111-2222-3333-4444-555555555555"
	rows := sqlmock.NewRows([]string{"id", "token", "resource_type", "tier", "provider_resource_id"}).
		AddRow(resourceID, "a1b2c3d4deadbeef", "storage", "hobby", "")
	mock.ExpectQuery(`SELECT id, token`).WillReturnRows(rows)
	// No UPDATE expected — scanner error must short-circuit.

	fake := &fakeMinIOClient{
		bucketExists: true,
		listErr:      errors.New("minio temporarily unreachable"),
	}
	scanner := newMinIOScannerWithClient(fake, "instant-shared")

	w := NewUpdateStorageBytesWorker(db, nil, scanner)
	if err := w.Work(context.Background(), fakeMinIOJob()); err != nil {
		t.Fatalf("expected nil (fail-open on scanner error), got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestUpdateStorageBytesWorker_StorageRowSkippedWhenScannerNil verifies
// that storage rows are silently skipped (fail-open) when no scanner is
// configured. Mirrors the provisioner-nil behavior for the other types.
func TestUpdateStorageBytesWorker_StorageRowSkippedWhenScannerNil(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resourceID := "11111111-2222-3333-4444-555555555555"
	rows := sqlmock.NewRows([]string{"id", "token", "resource_type", "tier", "provider_resource_id"}).
		AddRow(resourceID, "a1b2c3d4deadbeef", "storage", "hobby", "")
	mock.ExpectQuery(`SELECT id, token`).WillReturnRows(rows)
	// No UPDATE expected — nil scanner must short-circuit storage rows.

	// provClient non-nil (so the worker doesn't no-op), minioScanner nil.
	prov := &fakeProvForMinIOTest{}
	w := NewUpdateStorageBytesWorker(db, prov, nil)
	if err := w.Work(context.Background(), fakeMinIOJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
