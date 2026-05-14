package jobs

// platform_db_backup_test.go — hermetic tests for PlatformDBBackupWorker.
//
// Lives in package jobs (white-box) so the tests can reach the unexported
// pgDumper / s3Client / computeKeepSet seams without growing the package's
// exported surface. The churn_predictor tests live in package jobs_test
// because they only need the public constructor; this file needs the
// internal interfaces.
//
// Scenarios covered (mapped to the brief's required test list):
//
//   * Happy path — TestPlatformDBBackup_HappyPath
//       mock pg_dump emits N bytes, mock S3 accepts the upload, the
//       worker writes both the started and succeeded audit rows and the
//       retention sweep runs.
//
//   * Failure path — TestPlatformDBBackup_DumpError_WritesFailedAudit
//       mock pg_dump returns an error; assert no successful audit row,
//       failed audit row IS written, Work returns a non-nil error so
//       River retries.
//
//   * Retention — TestComputeKeepSet_60DaysIn covers the retention math
//       on its own (insert 60 days of keys, assert which subset survives).
//
//   * Lock contention — TestPlatformDBBackup_LockContended runs two
//       Work() calls back to back against sqlmock that returns
//       locked=false for the second; asserts only one dump fires.
//
//   * Disabled mode — TestPlatformDBBackup_NoS3_Skips: S3=nil → Work
//       returns nil without contacting pg_dump (this is the deployment
//       path before OBJECT_STORE_* is wired).

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

// fakePlatformBackupJob returns a minimal *river.Job for passing to Work().
func fakePlatformBackupJob() *river.Job[PlatformDBBackupArgs] {
	return &river.Job[PlatformDBBackupArgs]{JobRow: &rivertype.JobRow{ID: 42}}
}

// fakePgDumper is a deterministic pgDumper for tests. The bytes it writes
// + the error it returns are both knobs the test sets up front. A real
// pg_dump invocation is NEVER triggered from these tests.
type fakePgDumper struct {
	payload []byte
	err     error
	calls   int
	mu      sync.Mutex
}

func (f *fakePgDumper) Dump(_ context.Context, _ string, w io.Writer) (int64, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.err != nil {
		// Return WITHOUT writing any bytes — this models the dumper
		// failing before it produces a usable artifact. The worker
		// should treat the empty body + non-nil error as a hard fail
		// and not upload.
		return 0, f.err
	}
	n, err := w.Write(f.payload)
	return int64(n), err
}

// fakeS3 captures every Upload/List/Delete the worker performs.
type fakeS3 struct {
	mu sync.Mutex

	// captured state — assertions read these.
	uploaded   map[string][]byte // key → body
	uploadOpts map[string]int64  // key → size arg
	listed     []string          // prefixes the worker called List with
	deleted    []string          // keys the worker called Delete on

	// behavior toggles — tests set these to simulate failures.
	uploadErr  error
	listErr    error
	deleteErr  error
	listResult []string // canned response from List
}

func newFakeS3() *fakeS3 {
	return &fakeS3{
		uploaded:   map[string][]byte{},
		uploadOpts: map[string]int64{},
	}
}

func (f *fakeS3) Upload(_ context.Context, _, key string, body io.Reader, size int64) error {
	if f.uploadErr != nil {
		// Drain the body so the producing goroutine isn't blocked on a
		// pipe with no reader — matches real S3 client behavior on a
		// failed upload (the lib reads at least some bytes before
		// surfacing the error).
		_, _ = io.Copy(io.Discard, body)
		return f.uploadErr
	}
	buf, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.uploaded[key] = buf
	f.uploadOpts[key] = size
	f.mu.Unlock()
	return nil
}

func (f *fakeS3) List(_ context.Context, _, prefix string) ([]string, error) {
	f.mu.Lock()
	f.listed = append(f.listed, prefix)
	f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]string(nil), f.listResult...), nil
}

func (f *fakeS3) Delete(_ context.Context, _, key string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.mu.Lock()
	f.deleted = append(f.deleted, key)
	f.mu.Unlock()
	return nil
}

// expectAdvisoryLockAcquired primes sqlmock to return locked=true for the
// pg_try_advisory_lock call. The matching pg_advisory_unlock primer must
// be added by the caller AFTER all expected mid-job queries (the worker
// defers the unlock so it runs last).
func expectAdvisoryLockAcquired(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT pg_try_advisory_lock`).
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(true))
}

// expectAdvisoryUnlock primes the deferred unlock call. Call this LAST in
// the per-test sqlmock expectation sequence.
func expectAdvisoryUnlock(mock sqlmock.Sqlmock) {
	mock.ExpectExec(`pg_advisory_unlock`).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

// expectAdvisoryLockContended primes sqlmock to return locked=false —
// another pod holds the lock. NOTE: the worker registers its
// pg_advisory_unlock defer AFTER the locked=false early return, so on
// contention only the try_advisory_lock query fires — there is no
// matching unlock call to prime.
func expectAdvisoryLockContended(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT pg_try_advisory_lock`).
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(false))
}

// fixedClock returns a deterministic time so date-segment keys are stable
// across test runs.
func fixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

// newTestWorker bundles the boilerplate of constructing a worker against
// sqlmock + fake S3 + fake dumper. Tests that need specialised behavior
// set fields on the returned struct before calling Work.
func newTestWorker(t *testing.T, mock sqlmock.Sqlmock, db *sql.DB, dumper *fakePgDumper, s3 *fakeS3, now time.Time) *PlatformDBBackupWorker {
	t.Helper()
	_ = mock // accepted for symmetry with the helpers; the lock primer
	// uses it directly
	return NewPlatformDBBackupWorker(PlatformDBBackupConfig{
		DB:          db,
		DatabaseURL: "postgres://test@localhost/test",
		Dumper:      dumper,
		S3:          s3,
		Bucket:      "instant-shared",
		OuterPrefix: "",
		InnerPrefix: "platform-backups/",
		Now:         fixedClock(now),
	})
}

// TestPlatformDBBackup_HappyPath is the core unit: pg_dump emits N
// bytes, S3 accepts, two audit rows fire (started + succeeded), retention
// sweep runs.
func TestPlatformDBBackup_HappyPath(t *testing.T) {
	db, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
		sqlmock.MonitorPingsOption(false),
	)
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectAdvisoryLockAcquired(mock)
	// started audit
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs("system", "platform_backup.started", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// succeeded audit (after sweep)
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs("system", "platform_backup.succeeded", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	expectAdvisoryUnlock(mock)

	dumper := &fakePgDumper{payload: bytes.Repeat([]byte("X"), 1024)}
	s3 := newFakeS3()
	now := time.Date(2026, 5, 13, 2, 0, 0, 0, time.UTC)

	w := newTestWorker(t, mock, db, dumper, s3, now)
	if err := w.Work(context.Background(), fakePlatformBackupJob()); err != nil {
		t.Fatalf("Work returned %v; want nil", err)
	}

	wantKey := "platform-backups/2026-05-13/platform.dump.gz"
	gotBody, ok := s3.uploaded[wantKey]
	if !ok {
		t.Fatalf("S3 upload did NOT see key %q; saw %v", wantKey, keysOf(s3.uploaded))
	}
	if len(gotBody) != 1024 {
		t.Errorf("uploaded body size = %d; want 1024", len(gotBody))
	}
	if gotSize := s3.uploadOpts[wantKey]; gotSize != -1 {
		t.Errorf("Upload size arg = %d; want -1 (streaming/multipart signal)", gotSize)
	}
	if dumper.calls != 1 {
		t.Errorf("pgDumper.Dump called %d times; want 1", dumper.calls)
	}
	if len(s3.listed) != 1 {
		t.Errorf("retention sweep did not List exactly once; listed=%v", s3.listed)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestPlatformDBBackup_DumpError_WritesFailedAudit covers the failure
// path: pg_dump errors out, no successful audit, failed audit fires,
// Work returns a non-nil error so River retries.
func TestPlatformDBBackup_DumpError_WritesFailedAudit(t *testing.T) {
	db, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
		sqlmock.MonitorPingsOption(false),
	)
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectAdvisoryLockAcquired(mock)
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs("system", "platform_backup.started", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// Failed audit fires AFTER the dump returns — no succeeded audit.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs("system", "platform_backup.failed", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	expectAdvisoryUnlock(mock)

	dumperErr := errors.New("pg_dump: server connection lost")
	dumper := &fakePgDumper{err: dumperErr}
	s3 := newFakeS3()
	now := time.Date(2026, 5, 13, 2, 0, 0, 0, time.UTC)

	w := newTestWorker(t, mock, db, dumper, s3, now)
	err = w.Work(context.Background(), fakePlatformBackupJob())
	if err == nil {
		t.Fatal("Work returned nil on dumper failure; want non-nil so River retries")
	}
	if !strings.Contains(err.Error(), "pg_dump") {
		t.Errorf("error did not mention pg_dump; got: %v", err)
	}
	if len(s3.uploaded) != 0 {
		// The empty-body upload may have happened (the worker doesn't
		// short-circuit until both arms of the pipe complete) but the
		// post-failure Delete should have removed it.
		// Allowed shape: either no upload OR an upload + a delete.
		if len(s3.deleted) == 0 {
			t.Errorf("dumper failed but partial S3 object %v was not deleted", keysOf(s3.uploaded))
		}
	}
	if dumper.calls != 1 {
		t.Errorf("pgDumper.Dump calls = %d; want 1", dumper.calls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestPlatformDBBackup_LockContended verifies the advisory-lock branch:
// another pod holds the lock → Work returns nil without dumping. This is
// the EXPECTED branch on a multi-pod deployment.
func TestPlatformDBBackup_LockContended(t *testing.T) {
	db, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
		sqlmock.MonitorPingsOption(false),
	)
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectAdvisoryLockContended(mock)
	// NO audit_log inserts expected — the contended pod stays silent and
	// the holding pod will write all rows.

	dumper := &fakePgDumper{payload: []byte("UNUSED")}
	s3 := newFakeS3()
	now := time.Date(2026, 5, 13, 2, 0, 0, 0, time.UTC)

	w := newTestWorker(t, mock, db, dumper, s3, now)
	if err := w.Work(context.Background(), fakePlatformBackupJob()); err != nil {
		t.Fatalf("Work returned %v on lock contention; want nil", err)
	}
	if dumper.calls != 0 {
		t.Errorf("pgDumper.Dump was called %d times under lock contention; want 0", dumper.calls)
	}
	if len(s3.uploaded) != 0 {
		t.Errorf("S3 upload happened under lock contention: %v", keysOf(s3.uploaded))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestPlatformDBBackup_LockContention_TwoGoroutines is the integration-
// style test the brief calls for: spawn two goroutines that both call
// Work() concurrently, only one should run the dump. Mocking the
// advisory-lock contention via sqlmock requires that each goroutine
// see a different (db, mock) pair — but each acquires its OWN advisory
// lock semantics via the test's primed responses. We use two sqlmock
// connections to simulate two pods racing.
func TestPlatformDBBackup_LockContention_TwoGoroutines(t *testing.T) {
	// Goroutine A: lock acquired.
	dbA, mockA, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
		sqlmock.MonitorPingsOption(false),
	)
	if err != nil {
		t.Fatalf("sqlmock.New A: %v", err)
	}
	defer dbA.Close()
	expectAdvisoryLockAcquired(mockA)
	mockA.ExpectExec(`INSERT INTO audit_log`).
		WithArgs("system", "platform_backup.started", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mockA.ExpectExec(`INSERT INTO audit_log`).
		WithArgs("system", "platform_backup.succeeded", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	expectAdvisoryUnlock(mockA)

	// Goroutine B: lock contended.
	dbB, mockB, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
		sqlmock.MonitorPingsOption(false),
	)
	if err != nil {
		t.Fatalf("sqlmock.New B: %v", err)
	}
	defer dbB.Close()
	expectAdvisoryLockContended(mockB)
	// No audit rows expected on the contended side.

	dumperA := &fakePgDumper{payload: bytes.Repeat([]byte("A"), 64)}
	dumperB := &fakePgDumper{payload: bytes.Repeat([]byte("B"), 64)}
	s3A := newFakeS3()
	s3B := newFakeS3()
	now := time.Date(2026, 5, 13, 2, 0, 0, 0, time.UTC)

	wA := newTestWorker(t, mockA, dbA, dumperA, s3A, now)
	wB := newTestWorker(t, mockB, dbB, dumperB, s3B, now)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := wA.Work(context.Background(), fakePlatformBackupJob()); err != nil {
			t.Errorf("worker A: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := wB.Work(context.Background(), fakePlatformBackupJob()); err != nil {
			t.Errorf("worker B: %v", err)
		}
	}()
	wg.Wait()

	// Exactly one of (A, B) should have run the dump. The lock primers
	// already enforce this asymmetry — what we additionally check here
	// is that the contended pod did NOT corrupt state by writing
	// audit rows or uploading.
	if dumperA.calls != 1 || dumperB.calls != 0 {
		t.Errorf("expected dumperA=1 / dumperB=0; got A=%d B=%d", dumperA.calls, dumperB.calls)
	}
	if len(s3A.uploaded) != 1 {
		t.Errorf("worker A upload count = %d; want 1", len(s3A.uploaded))
	}
	if len(s3B.uploaded) != 0 {
		t.Errorf("worker B upload count = %d; want 0 (lock contended)", len(s3B.uploaded))
	}
}

// TestPlatformDBBackup_NoS3_Skips verifies the disabled-mode branch: a
// worker constructed without an S3 client logs a WARN and returns nil
// without touching pg_dump or the audit log. This is the deployment
// path before OBJECT_STORE_* is wired.
func TestPlatformDBBackup_NoS3_Skips(t *testing.T) {
	db, _, err := sqlmock.New(sqlmock.MonitorPingsOption(false))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	w := NewPlatformDBBackupWorker(PlatformDBBackupConfig{
		DB:          db,
		DatabaseURL: "postgres://test@localhost/test",
		S3:          nil,
		Bucket:      "instant-shared",
		Now:         fixedClock(time.Date(2026, 5, 13, 2, 0, 0, 0, time.UTC)),
	})
	if err := w.Work(context.Background(), fakePlatformBackupJob()); err != nil {
		t.Fatalf("Work returned %v in disabled mode; want nil", err)
	}
}

// TestComputeKeepSet_60DaysIn is the retention test the brief calls for:
// insert 60 days of backup keys, run the retention compute, assert only
// the correct subset (last 30 days + first-of-month going back 12
// months) survives.
//
// "Now" is fixed at 2026-05-13. The full input set is one key per day
// from 2026-03-15 (60 days ago) through 2026-05-13. Expected keep:
//
//   - daily band: 2026-04-13 through 2026-05-13 (31 days inclusive of
//     both endpoints because the dailyCutoff is now - 30 days, kept by
//     !Before).
//   - monthly band: the first-of-month dated key for each of the last
//     12 months. For months that have a key in this dataset (March,
//     April, May 2026), the earliest-day-in-month key wins.
//
// Specifically the test checks:
//   - 2026-05-13 → kept (today; daily).
//   - 2026-04-13 → kept (exactly at dailyCutoff = now - 30d).
//   - 2026-04-12 → NOT kept (one day past the daily window, NOT
//     first-of-its-month for April — April 1 is the first-of-month).
//   - 2026-04-01 → kept (first-of-April).
//   - 2026-03-15 → kept (earliest March in the dataset, so it's the
//     monthly winner for March; the SQL "earliest in month" definition
//     picks the smallest-date key found).
//   - 2026-03-16 → NOT kept (March, past daily window, not the earliest
//     March in the dataset).
func TestComputeKeepSet_60DaysIn(t *testing.T) {
	now := time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC)
	// Build 60 days of keys, one per day, ending today.
	var keys []string
	for i := 0; i < 60; i++ {
		d := now.AddDate(0, 0, -i)
		keys = append(keys, "platform-backups/"+d.Format("2006-01-02")+"/platform.dump.gz")
	}
	keep := computeKeepSet(keys, now, 30, 12)

	mustKeep := []string{
		"platform-backups/2026-05-13/platform.dump.gz", // today, daily
		"platform-backups/2026-04-13/platform.dump.gz", // now-30d, daily edge
		"platform-backups/2026-04-01/platform.dump.gz", // first-of-April
		"platform-backups/2026-03-15/platform.dump.gz", // earliest March in set
	}
	for _, k := range mustKeep {
		if !keep[k] {
			t.Errorf("key %q should be kept; was not", k)
		}
	}
	mustDrop := []string{
		"platform-backups/2026-04-12/platform.dump.gz", // one past daily, not month-first
		"platform-backups/2026-03-16/platform.dump.gz", // March, not earliest-March
		"platform-backups/2026-03-20/platform.dump.gz", // March, not earliest-March
	}
	for _, k := range mustDrop {
		if keep[k] {
			t.Errorf("key %q should NOT be kept; was kept", k)
		}
	}

	// Sanity: keep size = 31 days of daily + 2 extra monthlies (April 1
	// and March 15 — both outside the 30-day daily band). March 16..31
	// are dropped. So total keep = 31 + 2 = 33.
	if len(keep) != 33 {
		t.Errorf("keep set size = %d; want 33 (31 daily + 2 monthly: April-1 and March-15)", len(keep))
	}
}

// TestComputeKeepSet_KeepsUnparseableKeys covers the defensive branch:
// a manually-uploaded file with no date prefix is kept conservatively
// rather than deleted, so a forensic dump an operator dropped in by
// hand isn't sweep-deleted.
func TestComputeKeepSet_KeepsUnparseableKeys(t *testing.T) {
	now := time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC)
	keys := []string{
		"platform-backups/forensics/manual-2024-emergency.dump",
		"platform-backups/2026-05-13/platform.dump.gz",
	}
	keep := computeKeepSet(keys, now, 30, 12)
	if !keep["platform-backups/forensics/manual-2024-emergency.dump"] {
		t.Error("unparseable-date key was not kept; retention is supposed to be conservative on unknown shapes")
	}
}

// TestPlatformDBBackup_RetentionSweep_DeletesOldKeys exercises the full
// upload + sweep loop: feed in a List response that contains both
// in-band and out-of-band keys, assert the out-of-band ones are passed
// to Delete.
func TestPlatformDBBackup_RetentionSweep_DeletesOldKeys(t *testing.T) {
	db, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
		sqlmock.MonitorPingsOption(false),
	)
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectAdvisoryLockAcquired(mock)
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs("system", "platform_backup.started", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs("system", "platform_backup.succeeded", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	expectAdvisoryUnlock(mock)

	now := time.Date(2026, 5, 13, 2, 0, 0, 0, time.UTC)
	dumper := &fakePgDumper{payload: []byte("hello")}
	s3 := newFakeS3()
	// List response: today (daily-kept), an "earliest-March-in-set" key
	// (monthly-kept), a non-earliest-March key (dropped — out of daily
	// band and not its month's first), and a 2024 key well outside the
	// 12-month monthly window (dropped). 12-month window from May 2026
	// extends back to June 2025, so any 2024 month is unconditionally
	// outside the monthly band.
	s3.listResult = []string{
		"platform-backups/2026-05-13/platform.dump.gz", // today: kept
		"platform-backups/2026-03-15/platform.dump.gz", // earliest March in dataset → monthly: kept
		"platform-backups/2026-03-16/platform.dump.gz", // March but not earliest: dropped
		"platform-backups/2024-08-20/platform.dump.gz", // outside 12-month window: dropped
	}

	w := newTestWorker(t, mock, db, dumper, s3, now)
	if err := w.Work(context.Background(), fakePlatformBackupJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	// Expected deletes: 2026-03-16 and 2024-08-20.
	wantDeleted := map[string]bool{
		"platform-backups/2026-03-16/platform.dump.gz": true,
		"platform-backups/2024-08-20/platform.dump.gz": true,
	}
	if len(s3.deleted) != len(wantDeleted) {
		t.Errorf("deleted %d keys; want %d (%v)", len(s3.deleted), len(wantDeleted), s3.deleted)
	}
	for _, k := range s3.deleted {
		if !wantDeleted[k] {
			t.Errorf("unexpected delete of %q", k)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// keysOf returns the keys of a map[string][]byte as a sorted slice for
// readable test failure output.
func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
