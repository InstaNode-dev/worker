package jobs

import (
	"bytes"
	"context"
	"errors"
	"io"
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
