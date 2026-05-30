package jobs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"instant.dev/common/crypto"
)

// testAESKeyHex matches the api side's test fixture pattern: a 64-char hex
// string (= 32 bytes for AES-256). Hand-rolled here so the test file
// doesn't take a dependency on api/testhelpers.
const testAESKeyHex = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

func fakeRunnerJob() *river.Job[CustomerBackupRunnerArgs] {
	return &river.Job[CustomerBackupRunnerArgs]{JobRow: &rivertype.JobRow{ID: 1}}
}

// fakeBackupStore is the in-memory BackupObjectStore used by every backup-
// pipeline test. Captures uploads/downloads/deletes so assertions can pin
// exact object keys + payload bytes.
type fakeBackupStore struct {
	mu       sync.Mutex
	objects  map[string][]byte // key: "bucket/objectKey"
	uploads  []fakeUpload
	deletes  []string
	uploadFn func(ctx context.Context, bucket, key string, r io.Reader) (int64, error)
}

type fakeUpload struct {
	bucket string
	key    string
	size   int64
}

func newFakeBackupStore() *fakeBackupStore {
	return &fakeBackupStore{objects: map[string][]byte{}}
}

func (f *fakeBackupStore) Upload(ctx context.Context, bucket, key string, r io.Reader) (int64, error) {
	if f.uploadFn != nil {
		return f.uploadFn(ctx, bucket, key, r)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[bucket+"/"+key] = data
	f.uploads = append(f.uploads, fakeUpload{bucket: bucket, key: key, size: int64(len(data))})
	return int64(len(data)), nil
}

func (f *fakeBackupStore) Download(_ context.Context, bucket, key string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.objects[bucket+"/"+key]
	if !ok {
		return nil, errors.New("not found")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *fakeBackupStore) DeleteObject(_ context.Context, bucket, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, bucket+"/"+key)
	f.deletes = append(f.deletes, bucket+"/"+key)
	return nil
}

// fakePgDump mimics pg_dump output by writing a fixed test payload. The
// runner gzips this then streams to the store; the test asserts the gzip
// bytes round-trip correctly.
type fakePgDump struct {
	payload []byte
	err     error
	gotConn string
}

func (f *fakePgDump) Run(_ context.Context, connURL string, w io.Writer) error {
	f.gotConn = connURL
	if f.err != nil {
		return f.err
	}
	_, err := w.Write(f.payload)
	return err
}

// encryptForTest mirrors the api's encrypt-at-rest pattern so we can stuff a
// realistic-looking ciphertext into the mocked resources.connection_url
// scan value.
func encryptForTest(t *testing.T, plain string) string {
	t.Helper()
	key, err := crypto.ParseAESKey(testAESKeyHex)
	if err != nil {
		t.Fatalf("ParseAESKey: %v", err)
	}
	enc, err := crypto.Encrypt(key, plain)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	return enc
}

// TestRunner_HappyPath — claim, dump, upload, finalize, audit row written.
func TestRunner_HappyPath(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backupID := "11111111-1111-1111-1111-111111111111"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	token := "tok-abc-123"
	plainConn := "postgres://u:p@host/db"
	encConn := encryptForTest(t, plainConn)

	// P2-W4: stuck-row recovery runs first every tick — reset orphaned
	// 'running' rows back to 'pending'. Here it touches nothing.
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// SELECT pending — one row.
	mock.ExpectQuery(`SELECT b.id::text, b.resource_id::text, b.tier_at_backup`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}).AddRow(backupID, resID, "pro", "scheduled", token, encConn, "postgres", teamID))

	// Atomic claim returns the id.
	mock.ExpectQuery(`UPDATE resource_backups\s+SET status = 'running'`).
		WithArgs(backupID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(backupID))

	// backup.started audit row.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Finalize UPDATE — status=ok. FIX-H #59: adds sha256 column.
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'ok'`).
		WithArgs(backupID, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// backup.succeeded audit row.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Retention sweep — one empty SELECT per tier. We don't enforce arg
	// values (cutoff is time-dependent) but we DO need each query mocked
	// because the sweep loops all five tier names.
	for i := 0; i < 5; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key\s+FROM resource_backups`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	store := newFakeBackupStore()
	dump := &fakePgDump{payload: []byte("PGDMPDATA")}

	w := &CustomerBackupRunnerWorker{
		db:      db,
		store:   store,
		pgDump:  dump,
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

	// Object key contract: <prefix>/<token>/<backup-id>.dump.gz
	wantKey := "backups/" + token + "/" + backupID + ".dump.gz"
	if len(store.uploads) != 1 {
		t.Fatalf("uploads = %d; want 1", len(store.uploads))
	}
	if store.uploads[0].key != wantKey {
		t.Errorf("uploaded key = %q; want %q", store.uploads[0].key, wantKey)
	}
	if store.uploads[0].bucket != "instant-shared" {
		t.Errorf("uploaded bucket = %q; want %q", store.uploads[0].bucket, "instant-shared")
	}
	if dump.gotConn != plainConn {
		t.Errorf("pgDump.gotConn = %q; want decrypted %q", dump.gotConn, plainConn)
	}
}

// TestRunner_NilStore_NoOp — fail-open when the object store isn't
// configured (dev env without OBJECT_STORE_ENDPOINT).
func TestRunner_NilStore_NoOp(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	w := &CustomerBackupRunnerWorker{
		db:     db,
		store:  nil,
		aesKey: testAESKeyHex,
		now:    time.Now,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("expected no-op, got %v", err)
	}
}

// TestRunner_EmptyAESKey_NoOp — same fail-open shape.
func TestRunner_EmptyAESKey_NoOp(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	w := &CustomerBackupRunnerWorker{
		db:     db,
		store:  newFakeBackupStore(),
		aesKey: "",
		now:    time.Now,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("expected no-op, got %v", err)
	}
}

// TestRunner_PgDumpFails_MarksFailed — when pg_dump errors, the row goes
// to 'failed' with the error_summary captured, AND the half-uploaded
// object is cleaned up.
func TestRunner_PgDumpFails_MarksFailed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backupID := "11111111-1111-1111-1111-111111111111"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	encConn := encryptForTest(t, "postgres://u:p@host/db")

	// P2-W4: stuck-row recovery runs first every tick.
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}).AddRow(backupID, resID, "pro", "scheduled", "tok", encConn, "postgres", teamID))

	mock.ExpectQuery(`UPDATE resource_backups\s+SET status = 'running'`).
		WithArgs(backupID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(backupID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))

	// Failure path: row marked 'failed' with error_summary; audit row.
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'failed'`).
		WithArgs(backupID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))

	// Retention sweep still runs.
	for i := 0; i < 5; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key\s+FROM resource_backups`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	store := newFakeBackupStore()
	dump := &fakePgDump{err: errors.New("pg_dump: server unavailable")}

	w := &CustomerBackupRunnerWorker{
		db:      db,
		store:   store,
		pgDump:  dump,
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
	if len(store.deletes) != 1 {
		t.Errorf("deletes = %d; want 1 (cleanup of partial upload)", len(store.deletes))
	}
}

// TestRunner_ClaimRace_SkipsSilently — when another worker already grabbed
// the row, our UPDATE returns 0 rows and we skip without writing anything.
func TestRunner_ClaimRace_SkipsSilently(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backupID := "11111111-1111-1111-1111-111111111111"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	encConn := encryptForTest(t, "postgres://u:p@host/db")

	// P2-W4: stuck-row recovery runs first every tick.
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}).AddRow(backupID, resID, "pro", "scheduled", "tok", encConn, "postgres", teamID))

	// Claim returns no rows (sql.ErrNoRows path).
	mock.ExpectQuery(`UPDATE resource_backups\s+SET status = 'running'`).
		WithArgs(backupID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	// No follow-up writes. Retention sweep still runs.
	for i := 0; i < 5; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key\s+FROM resource_backups`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

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

// TestRunner_RecoversStuckRunningRows pins BugBash P2-W4: every Work()
// tick MUST first reset backup rows orphaned at status='running' (a
// runner pod killed after the atomic claim but before finalize) back to
// 'pending'. Without it the row is unreachable forever — the pending
// sweep only selects status='pending' — and a manual backup is silently
// lost. The recovery UPDATE is gated on started_at older than the
// per-run timeout so a genuinely in-flight backup is never reclaimed.
func TestRunner_RecoversStuckRunningRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// The stuck-row recovery UPDATE — reports it reset 2 orphaned rows.
	// It MUST run before the pending-row SELECT.
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 2))

	// Pending sweep finds nothing else this tick.
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}))

	// Retention sweep still loops the five tier names.
	for i := 0; i < 5; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key\s+FROM resource_backups`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

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
		t.Errorf("stuck-row recovery did not run before the pending sweep — P2-W4 regressed: %v", err)
	}
}

// TestBackupObjectKey — pure function contract.
func TestBackupObjectKey(t *testing.T) {
	got := backupObjectKey("backups", "tok-abc", "bk-uuid")
	want := "backups/tok-abc/bk-uuid.dump.gz"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	// Trailing slash in prefix is tolerated.
	if backupObjectKey("backups/", "tok", "id") != "backups/tok/id.dump.gz" {
		t.Errorf("prefix trailing slash not normalized")
	}
	// Empty prefix defaults to "backups".
	if backupObjectKey("", "tok", "id") != "backups/tok/id.dump.gz" {
		t.Errorf("empty prefix should default")
	}
}

// fakeBackupPlanRegistry is a minimal BackupPlanRegistry for tests. Maps
// tier→days exactly, with a `tiers` list for the sweep iteration order.
// Keeping the fake here (rather than embedding *commonplans.Registry) lets
// the test pin retention contract to declared values rather than the
// embedded default plans.yaml — if a future plans.yaml change accidentally
// flips a tier, the test fails on the assertion, not on a moved goalpost.
type fakeBackupPlanRegistry struct {
	days  map[string]int
	tiers []string
}

func (f *fakeBackupPlanRegistry) BackupRetentionDays(tier string) int {
	if d, ok := f.days[tier]; ok {
		return d
	}
	return 0
}

func (f *fakeBackupPlanRegistry) TierNames() []string { return f.tiers }

// TestRetentionDaysFromRegistry — per-tier retention read straight from
// the plans Registry (not a hardcoded switch). PROD-FIX-C regression:
// hobby_plus must resolve to 14 days, anonymous/free to 0 (delete-now),
// and the rest to their plans.yaml-declared values.
func TestRetentionDaysFromRegistry(t *testing.T) {
	reg := &fakeBackupPlanRegistry{days: map[string]int{
		"anonymous":         0,
		"free":              0,
		"hobby":             7,
		"hobby_yearly":      7,
		"hobby_plus":        14, // The regression target — was 7 under hardcoded switch.
		"hobby_plus_yearly": 14,
		"pro":               30,
		"pro_yearly":        30,
		"growth":            30,
		"team":              90,
		"team_yearly":       90,
	}}
	cases := []struct {
		tier string
		want int
	}{
		{"anonymous", 0},
		{"free", 0},
		{"hobby", 7},
		{"hobby_yearly", 7},
		{"hobby_plus", 14},
		{"hobby_plus_yearly", 14},
		{"pro", 30},
		{"pro_yearly", 30},
		{"growth", 30},
		{"team", 90},
		{"team_yearly", 90},
	}
	for _, c := range cases {
		if got := retentionDaysForTier(reg, c.tier); got != c.want {
			t.Errorf("tier=%q: got %d, want %d", c.tier, got, c.want)
		}
	}
}

// TestRetentionDaysForTier_NilRegistryFallback — when boot is misconfigured
// (registry nil), retentionDaysForTier returns the legacy 7-day default
// instead of 0 (which would delete every backup). Belt-and-suspenders for
// the deploy pipeline.
func TestRetentionDaysForTier_NilRegistryFallback(t *testing.T) {
	if got := retentionDaysForTier(nil, "pro"); got != 7 {
		t.Errorf("nil registry: got %d, want 7 (legacy default)", got)
	}
	if got := retentionDaysForTier(nil, ""); got != 7 {
		t.Errorf("nil registry + empty tier: got %d, want 7", got)
	}
}

// TestRetentionDaysForTier_NegativeIsFallback — defensive: plans.yaml
// uses -1 for "unlimited" on count fields, never for retention. If a
// future hand-edit slips a -1 in, we WARN and use the 7-day fallback
// rather than computing a future cutoff (which would skip every row).
func TestRetentionDaysForTier_NegativeIsFallback(t *testing.T) {
	reg := &fakeBackupPlanRegistry{days: map[string]int{"weird": -1}}
	if got := retentionDaysForTier(reg, "weird"); got != 7 {
		t.Errorf("negative registry value: got %d, want 7 (defensive fallback)", got)
	}
}

// TestRetentionCutoff_ZeroDaysIsNow — retention=0 produces cutoff=now,
// so the SQL `created_at < cutoff` predicate matches every row of that
// tier. This is the right semantic for retention=0 ("we don't take
// backups") — leaked rows from a prior tier get swept on next tick
// instead of persisting forever.
func TestRetentionCutoff_ZeroDaysIsNow(t *testing.T) {
	reg := &fakeBackupPlanRegistry{days: map[string]int{"anonymous": 0}}
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	cutoff := retentionCutoff(reg, "anonymous", now)
	if !cutoff.Equal(now.UTC()) {
		t.Errorf("retention=0: cutoff = %v, want %v (delete-now semantics)", cutoff, now.UTC())
	}
}

// TestRetentionCutoff_PositiveDaysIsBackInTime — retention=N days means
// cutoff is N*24h before now. Pro tier = 30d → cutoff exactly 30 days
// back from the supplied now.
func TestRetentionCutoff_PositiveDaysIsBackInTime(t *testing.T) {
	reg := &fakeBackupPlanRegistry{days: map[string]int{"pro": 30}}
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	want := now.Add(-30 * 24 * time.Hour)
	got := retentionCutoff(reg, "pro", now)
	if !got.Equal(want) {
		t.Errorf("pro 30d: cutoff = %v, want %v", got, want)
	}
}

// installFakePgDump writes a shell-script "pg_dump" into a TempDir, prepends
// it to PATH for the test's lifetime, and returns the script path so the
// test can read back the recorded argv + env after invocation. The fake
// prints argv to <dir>/argv.txt and env's PGPASSWORD value to
// <dir>/pgpassword.txt, then exits 0 (success path) or 1 if the caller
// passes failExitCode=true.
//
// Used by TestRealPgDumpRunner_* and TestDefaultPgDumpExec_*: those tests
// exercise the SEC-WORKER FINDING-1 + FINDING-2 PGPASSWORD-env branches
// in customer_backup_runner.go + platform_db_backup.go which require
// actually spawning a pg_dump-named process.
func installFakePgDump(t *testing.T, failExitCode bool) (dir string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake pg_dump script is shell-based; worker runs on linux/darwin only")
	}
	dir = t.TempDir()
	exitCode := "0"
	if failExitCode {
		exitCode = "1"
	}
	// The script:
	//   1. Writes every argv element (one per line) to argv.txt
	//   2. Writes PGPASSWORD (or empty string) to pgpassword.txt
	//   3. Writes a tiny stdout payload so callers that pipe stdout see bytes
	//   4. Exits 0 (success) or 1 (caller-controlled failure)
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$@\" > \"" + dir + "/argv.txt\"\n" +
		"printf '%s' \"${PGPASSWORD:-}\" > \"" + dir + "/pgpassword.txt\"\n" +
		"printf 'fakepgdumpbody'\n" +
		"exit " + exitCode + "\n"
	path := filepath.Join(dir, "pg_dump")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake pg_dump: %v", err)
	}
	oldPATH := os.Getenv("PATH")
	t.Setenv("PATH", dir+string(os.PathListSeparator)+oldPATH)
	return dir
}

func readFakePgDumpRecord(t *testing.T, dir string) (argv []string, pgpassword string) {
	t.Helper()
	argvBytes, err := os.ReadFile(filepath.Join(dir, "argv.txt"))
	if err != nil {
		t.Fatalf("read argv.txt: %v", err)
	}
	// Strip the trailing newline before splitting so the last entry isn't "".
	argvStr := string(bytes.TrimRight(argvBytes, "\n"))
	pgpassword = mustReadString(t, filepath.Join(dir, "pgpassword.txt"))
	if argvStr == "" {
		return nil, pgpassword
	}
	parts := bytes.Split([]byte(argvStr), []byte("\n"))
	argv = make([]string, len(parts))
	for i, b := range parts {
		argv[i] = string(b)
	}
	return argv, pgpassword
}

func mustReadString(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// TestRealPgDumpRunner_Run_PasswordMovesToEnv pins SEC-WORKER FINDING-2:
// realPgDumpRunner must strip the password out of connURL and pass it via
// PGPASSWORD env, NOT inside argv. This covers customer_backup_runner.go
// lines 125-127 (the `if pw != ""` env-setting branch).
func TestRealPgDumpRunner_Run_PasswordMovesToEnv(t *testing.T) {
	dir := installFakePgDump(t, false)

	const secret = "super-secret-pw-ZZZ"
	connURL := "postgres://doadmin:" + secret + "@db.example.com:25060/app?sslmode=require"

	var out bytes.Buffer
	if err := (realPgDumpRunner{}).Run(context.Background(), connURL, &out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.String() != "fakepgdumpbody" {
		t.Errorf("stdout payload: got %q, want %q", out.String(), "fakepgdumpbody")
	}

	argv, pgpassword := readFakePgDumpRecord(t, dir)

	// PGPASSWORD env must carry the secret.
	if pgpassword != secret {
		t.Errorf("PGPASSWORD env: got %q, want %q", pgpassword, secret)
	}
	// argv must NOT contain the literal password anywhere — this is THE
	// security promise the PR is shipping.
	for _, a := range argv {
		if bytes.Contains([]byte(a), []byte(secret)) {
			t.Errorf("argv leaks password: %q (full argv: %q)", a, argv)
		}
	}
	// argv MUST still carry the stripped DSN (with userinfo password removed).
	found := false
	for _, a := range argv {
		if a == "postgres://doadmin@db.example.com:25060/app?sslmode=require" {
			found = true
		}
	}
	if !found {
		t.Errorf("argv missing stripped DSN; got: %q", argv)
	}
}

// TestRealPgDumpRunner_Run_MalformedURLFailOpen pins the fail-open branch:
// if splitPGPassword returns an error, the runner falls back to the original
// connURL with no PGPASSWORD env. Covers customer_backup_runner.go lines
// 116-119 (the `if splitErr != nil { dsn = connURL; pw = "" }` branch).
func TestRealPgDumpRunner_Run_MalformedURLFailOpen(t *testing.T) {
	dir := installFakePgDump(t, false)

	// Same shape that splitPGPassword's TestSplitPGPassword malformed_url_fail_open
	// case proves returns an error from url.Parse.
	const malformed = "::::not a url"

	var out bytes.Buffer
	if err := (realPgDumpRunner{}).Run(context.Background(), malformed, &out); err != nil {
		t.Fatalf("Run on malformed URL: %v", err)
	}

	argv, pgpassword := readFakePgDumpRecord(t, dir)

	// Fail-open: no PGPASSWORD env is set because pw == "".
	if pgpassword != "" {
		t.Errorf("PGPASSWORD on fail-open: got %q, want empty", pgpassword)
	}
	// The original malformed URL is passed through to pg_dump argv unchanged
	// (this is the "no regression" promise — same code path it has always run).
	found := false
	for _, a := range argv {
		if a == malformed {
			found = true
		}
	}
	if !found {
		t.Errorf("argv missing fail-open passthrough URL %q; got: %q", malformed, argv)
	}
}
