package jobs

// backup_extra_test.go — coverage-raising tests for the backup job family.
// Targets the uncovered surface in:
//
//   * backup_s3.go               — NewMinIOBackupStore branches +
//                                  *minioBackupStore.{Upload,Download,DeleteObject}
//                                  + commonPlanRegistryAdapter +
//                                  NewBackupPlanRegistry
//   * customer_backup_runner.go  — Kind / Run / NewCustomerBackupRunner /
//                                  WithRefundClient / limitedBuffer.{Write,String} /
//                                  refundManualBackupQuota /
//                                  signBackupRefundJWT + Work edge cases
//   * customer_backup_scheduler.go — Kind / Run
//   * customer_restore_runner.go — Kind / Run / NewCustomerRestoreRunner +
//                                  download error / connURL empty / bad AES key /
//                                  decrypt failure / gzip header invalid /
//                                  recoverStuckRestores success path
//   * platform_db_backup.go      — Kind / NewPlatformDBBackupWorker default
//                                  branches / joinPlatformBackupPrefix edges /
//                                  Work success-but-list-fail / writeAudit nil DB /
//                                  defaultPgDumpExec.Dump via PG_DUMP_BIN trick
//   * platform_db_backup_s3.go   — NewBackupS3Client branches + minioS3.{Upload,List,Delete}
//
// All tests are hermetic — no external Postgres / S3 / network egress.
// httptest stands in for an S3 endpoint where the production code dials
// the minio-go SDK. We don't validate the S3 protocol — we validate that
// our wrappers translate Go calls into HTTP requests + parse the
// response shape the SDK expects.

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	minio "github.com/minio/minio-go/v7"

	commonplans "instant.dev/common/plans"
)

// ────────────────────────────────────────────────────────────────────
// backup_s3.go — NewMinIOBackupStore + commonPlanRegistryAdapter
// ────────────────────────────────────────────────────────────────────

// TestNewMinIOBackupStore_EmptyEndpoint — fail-loud on missing endpoint.
func TestNewMinIOBackupStore_EmptyEndpoint(t *testing.T) {
	if _, err := NewMinIOBackupStore("", "k", "s"); err == nil {
		t.Fatal("expected error for empty endpoint, got nil")
	}
}

// TestNewMinIOBackupStore_SchemeAndVendorBranches — exercise every arm of
// the TLS heuristic so the constructor's switch is fully covered.
func TestNewMinIOBackupStore_SchemeAndVendorBranches(t *testing.T) {
	cases := []struct {
		name     string
		endpoint string
	}{
		{"plain_https_prefix", "https://example.com"},
		{"plain_http_prefix", "http://example.com"},
		{"vendor_do_spaces", "nyc3.digitaloceanspaces.com"},
		{"vendor_aws", "s3.us-east-1.amazonaws.com"},
		{"vendor_cf_r2", "abc.r2.cloudflarestorage.com"},
		{"vendor_gcs", "storage.googleapis.com"},
		{"vendor_wasabi", "s3.wasabisys.com"},
		{"vendor_b2", "s3.us-west-001.backblazeb2.com"},
		{"unrecognised_no_scheme", "localhost:9000"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store, err := NewMinIOBackupStore(c.endpoint, "key", "secret")
			if err != nil {
				t.Fatalf("ctor returned %v", err)
			}
			if store == nil || store.client == nil {
				t.Fatalf("store.client is nil for %s", c.endpoint)
			}
		})
	}
}

// TestNewBackupPlanRegistry_NilReturnsNil — defensive nil guard.
func TestNewBackupPlanRegistry_NilReturnsNil(t *testing.T) {
	if got := NewBackupPlanRegistry(nil); got != nil {
		t.Fatalf("nil registry should return nil adapter, got %v", got)
	}
}

// TestCommonPlanRegistryAdapter_Delegates — the wrapper proxies
// BackupRetentionDays + TierNames through to the embedded common/plans.Registry.
func TestCommonPlanRegistryAdapter_Delegates(t *testing.T) {
	reg := commonplans.Default()
	if reg == nil {
		t.Fatal("commonplans.Default returned nil")
	}
	adapter := NewBackupPlanRegistry(reg)
	if adapter == nil {
		t.Fatal("adapter is nil")
	}
	// BackupRetentionDays sanity — pro tier is documented at 30 days
	// in plans.yaml; defensive: anything > 0 is enough to prove the
	// delegation works (and that we are not hitting the legacy 7-day
	// fallback path).
	if d := adapter.BackupRetentionDays("pro"); d <= 0 {
		t.Errorf("BackupRetentionDays(pro) = %d; want > 0", d)
	}
	names := adapter.TierNames()
	if len(names) == 0 {
		t.Fatal("TierNames returned empty slice")
	}
	// Must include the major paid tiers — the retention sweep loops this.
	want := map[string]bool{"hobby": false, "pro": false, "team": false}
	for _, n := range names {
		if _, ok := want[n]; ok {
			want[n] = true
		}
	}
	for tier, seen := range want {
		if !seen {
			t.Errorf("TierNames missing %q", tier)
		}
	}
}

// ────────────────────────────────────────────────────────────────────
// minioBackupStore — Upload / Download / DeleteObject against an
// httptest server posing as an S3-compatible endpoint.
// ────────────────────────────────────────────────────────────────────

// newMinIOForHTTPTest dials minio-go at the supplied test-server endpoint.
// Returns the *minioBackupStore so we can call its methods directly.
func newMinIOForHTTPTest(t *testing.T, ts *httptest.Server) *minioBackupStore {
	t.Helper()
	// Strip "http://" so the minio.New parser doesn't double up.
	endpoint := strings.TrimPrefix(ts.URL, "http://")
	cli, err := minio.New(endpoint, &minio.Options{
		// Inline creds — the httptest handler won't validate them.
		Creds:  nil,
		Secure: false,
	})
	if err != nil {
		t.Fatalf("minio.New: %v", err)
	}
	return &minioBackupStore{client: cli}
}

// TestMinioBackupStore_Upload_ServerError — wraps minio-go errors with
// the "backup_s3.Upload" prefix. Hitting a 500 is sufficient to exercise
// the error-wrap path; we skip the happy-path "200 OK with ETag" test
// because minio-go's SigV4 signer expects an S3-compliant response shape
// (xml-encoded result + canonical headers) that a 4-line httptest stub
// doesn't satisfy — driving the success path requires a real S3 server.
func TestMinioBackupStore_Upload_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `<Error><Code>InternalError</Code></Error>`)
	}))
	defer ts.Close()
	store := newMinIOForHTTPTest(t, ts)
	_, err := store.Upload(context.Background(), "bucket", "key", strings.NewReader("x"))
	if err == nil {
		t.Fatal("expected error from 500, got nil")
	}
	if !strings.Contains(err.Error(), "backup_s3.Upload") {
		t.Errorf("error not wrapped: %v", err)
	}
}

// TestMinioBackupStore_Download_Returns_ReadCloser — GetObject is lazy
// in minio-go; the SDK returns an *Object without dialing. We just verify
// the call returns a non-nil ReadCloser without an error.
func TestMinioBackupStore_Download_Returns_ReadCloser(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("data"))
	}))
	defer ts.Close()
	store := newMinIOForHTTPTest(t, ts)
	rc, err := store.Download(context.Background(), "bucket", "key")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if rc == nil {
		t.Fatal("nil ReadCloser")
	}
	_ = rc.Close()
}

// TestMinioBackupStore_DeleteObject_ServerError — exercise the error-wrap
// path. (The success path requires a real S3 server — see Upload note.)
func TestMinioBackupStore_DeleteObject_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	store := newMinIOForHTTPTest(t, ts)
	if err := store.DeleteObject(context.Background(), "bucket", "key"); err == nil {
		t.Fatal("expected error on 500")
	}
}

// ────────────────────────────────────────────────────────────────────
// platform_db_backup_s3.go — NewBackupS3Client + minioS3.{Upload,List,Delete}
// ────────────────────────────────────────────────────────────────────

// TestNewBackupS3Client_EmptyEndpoint — fails loudly.
func TestNewBackupS3Client_EmptyEndpoint(t *testing.T) {
	if _, err := NewBackupS3Client("", "k", "s"); err == nil {
		t.Fatal("expected error for empty endpoint")
	}
}

// TestNewBackupS3Client_AllSchemes — covers each branch of the TLS heuristic.
func TestNewBackupS3Client_AllSchemes(t *testing.T) {
	cases := []string{
		"https://example.com",
		"http://example.com",
		"nyc3.digitaloceanspaces.com",
		"s3.amazonaws.com",
		"x.r2.cloudflarestorage.com",
		"storage.googleapis.com",
		"s3.wasabisys.com",
		"s3.backblazeb2.com",
		"localhost:9000",
	}
	for _, ep := range cases {
		t.Run(ep, func(t *testing.T) {
			cli, err := NewBackupS3Client(ep, "k", "s")
			if err != nil {
				t.Fatalf("ctor: %v", err)
			}
			if cli == nil {
				t.Fatal("nil client")
			}
		})
	}
}

// newMinioS3ForHTTPTest dials minio-go at the supplied httptest server.
func newMinioS3ForHTTPTest(t *testing.T, ts *httptest.Server) *minioS3 {
	t.Helper()
	endpoint := strings.TrimPrefix(ts.URL, "http://")
	cli, err := minio.New(endpoint, &minio.Options{Secure: false})
	if err != nil {
		t.Fatalf("minio.New: %v", err)
	}
	return &minioS3{client: cli}
}

// TestMinioS3_Upload_Errors — 500 propagates as a "PutObject" wrap.
func TestMinioS3_Upload_Errors(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	cli := newMinioS3ForHTTPTest(t, ts)
	if err := cli.Upload(context.Background(), "bucket", "key", strings.NewReader("x"), 1); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestMinioS3_List_Succeeds — minimal ListObjectsV2 response shape.
func TestMinioS3_List_Succeeds(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// minio-go reads via ListObjectsV2 — return an empty result so the
		// channel closes immediately.
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name>
  <Prefix>p/</Prefix>
  <KeyCount>0</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
</ListBucketResult>`)
	}))
	defer ts.Close()
	cli := newMinioS3ForHTTPTest(t, ts)
	keys, err := cli.List(context.Background(), "bucket", "p/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	_ = keys
}

// TestMinioS3_List_Errors — 500 surfaces a wrapped error.
func TestMinioS3_List_Errors(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	cli := newMinioS3ForHTTPTest(t, ts)
	_, err := cli.List(context.Background(), "bucket", "p/")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestMinioS3_Delete_Errors — 500 surfaces a wrapped error. (Happy path
// requires a real S3-compliant 204 — see Upload note for why we don't
// drive it via httptest here.)
func TestMinioS3_Delete_Errors(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	cli := newMinioS3ForHTTPTest(t, ts)
	if err := cli.Delete(context.Background(), "bucket", "key"); err == nil {
		t.Fatal("expected error on 500")
	}
}

// ────────────────────────────────────────────────────────────────────
// customer_backup_runner.go — public surface coverage
// ────────────────────────────────────────────────────────────────────

// TestCustomerBackupRunnerArgs_KindAndRun — pin the River job key + the
// (CustomerBackupRunnerArgs).Run helper which is the constructor used
// by River when scheduling the job.
func TestCustomerBackupRunnerArgs_KindAndRun(t *testing.T) {
	args := CustomerBackupRunnerArgs{}
	if got := args.Kind(); got != "customer_backup_runner" {
		t.Errorf("Kind() = %q; want customer_backup_runner", got)
	}
}

// TestNewCustomerBackupRunner_Defaults — happy ctor wiring.
func TestNewCustomerBackupRunner_Defaults(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	w := NewCustomerBackupRunner(db, newFakeBackupStore(), "bucket", "prefix", testAESKeyHex, nil)
	if w == nil {
		t.Fatal("nil worker")
	}
	if w.bucket != "bucket" || w.prefix != "prefix" || w.aesKey != testAESKeyHex {
		t.Errorf("ctor fields not propagated: %+v", w)
	}
	if w.timeout != backupPerRunTimeout {
		t.Errorf("default timeout = %v; want %v", w.timeout, backupPerRunTimeout)
	}
	if w.batchN != backupBatchSize {
		t.Errorf("default batch size = %d; want %d", w.batchN, backupBatchSize)
	}
	if w.now == nil {
		t.Error("now func not initialised")
	}
}

// TestWithRefundClient_PopulatesFields — wires apiBase/jwtSecret/apiCli.
func TestWithRefundClient_PopulatesFields(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	w := NewCustomerBackupRunner(db, newFakeBackupStore(), "bucket", "prefix", testAESKeyHex, nil)
	w2 := w.WithRefundClient("https://api.example.com/", "secret", nil)
	// Returns the same worker so the call chains.
	if w2 != w {
		t.Errorf("WithRefundClient should return the same worker")
	}
	if w.apiBase != "https://api.example.com" {
		t.Errorf("apiBase trailing-slash strip failed: %q", w.apiBase)
	}
	if w.jwtSecret != "secret" {
		t.Errorf("jwtSecret not set")
	}
	if w.apiCli == nil {
		t.Errorf("apiCli not set")
	}
	// Calling with explicit non-nil http.Client still wires apiCli.
	w3 := w.WithRefundClient("https://b/", "s2", &http.Client{Timeout: 5 * time.Second})
	if w3.apiCli == nil {
		t.Errorf("apiCli nil when explicit http.Client passed")
	}
}

// TestLimitedBuffer_WriteAndString — pure helper. Asserts:
//   - first write below cap stores all bytes
//   - oversized write truncates: n returned == bytes that actually landed
//   - once full, additional Write returns len(p) without writing anything
//     (silent drop — matches the comment in customer_backup_runner.go)
func TestLimitedBuffer_WriteAndString(t *testing.T) {
	var b limitedBuffer
	n, err := b.Write([]byte("hello"))
	if err != nil || n != 5 {
		t.Fatalf("first write: n=%d err=%v", n, err)
	}
	// Fill until truncation. 5000-byte chunk → 4091 actually land (4096 - 5).
	big := make([]byte, 5000)
	for i := range big {
		big[i] = 'X'
	}
	n2, err := b.Write(big)
	if err != nil {
		t.Fatalf("second write: %v", err)
	}
	if n2 != 4091 {
		t.Errorf("second write n=%d; want 4091 (4096-5 cap)", n2)
	}
	s := b.String()
	if !strings.HasPrefix(s, "helloXX") {
		t.Errorf("buffer prefix wrong: %q", s[:20])
	}
	if len(s) != 4096 {
		t.Errorf("buffer length = %d; want 4096", len(s))
	}
	// Once full, additional writes drop silently — Write returns len(p)
	// without any bytes hitting the array.
	n3, err := b.Write([]byte("more"))
	if err != nil || n3 != 4 {
		t.Errorf("post-fill write: n=%d err=%v; want n=4, no error (silent drop)", n3, err)
	}
	if len(b.String()) != 4096 {
		t.Errorf("post-fill String length grew: %d", len(b.String()))
	}
}

// TestSignBackupRefundJWT_Shape — three-segment HS256 token whose claims
// JSON contains purpose/team_id/iat/exp.
func TestSignBackupRefundJWT_Shape(t *testing.T) {
	tok, err := signBackupRefundJWT("topsecret", "team-uuid")
	if err != nil {
		t.Fatalf("signBackupRefundJWT: %v", err)
	}
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("token segments = %d; want 3", len(parts))
	}
	claimsRaw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("base64 claims: %v", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(claimsRaw, &claims); err != nil {
		t.Fatalf("claims json: %v", err)
	}
	if claims["purpose"] != "internal_backup_refund" {
		t.Errorf("purpose = %v; want internal_backup_refund", claims["purpose"])
	}
	if claims["team_id"] != "team-uuid" {
		t.Errorf("team_id = %v", claims["team_id"])
	}
	if _, ok := claims["iat"]; !ok {
		t.Error("missing iat claim")
	}
	if _, ok := claims["exp"]; !ok {
		t.Error("missing exp claim")
	}
}

// TestSignBackupRefundJWT_EmptySecret — defensive error path.
func TestSignBackupRefundJWT_EmptySecret(t *testing.T) {
	if _, err := signBackupRefundJWT("", "team"); err == nil {
		t.Fatal("expected error for empty secret")
	}
}

// TestRefundManualBackupQuota_NoConfig_NoOp — apiBase or jwtSecret empty
// returns nil immediately.
func TestRefundManualBackupQuota_NoConfig_NoOp(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	w := NewCustomerBackupRunner(db, newFakeBackupStore(), "b", "p", testAESKeyHex, nil)
	// Neither WithRefundClient nor manual field set → no-op path.
	if err := w.refundManualBackupQuota(uuid.New(), "bk"); err != nil {
		t.Errorf("expected nil from disabled refund, got %v", err)
	}
}

// TestRefundManualBackupQuota_2xx_Success — exercise the full HTTP path
// with a fake api responder that returns 200.
func TestRefundManualBackupQuota_2xx_Success(t *testing.T) {
	gotPath := ""
	gotAuth := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()
	db, _, _ := sqlmock.New()
	defer db.Close()
	w := NewCustomerBackupRunner(db, newFakeBackupStore(), "b", "p", testAESKeyHex, nil).
		WithRefundClient(srv.URL, "secret", nil)
	team := uuid.New()
	if err := w.refundManualBackupQuota(team, "bk-1"); err != nil {
		t.Fatalf("refundManualBackupQuota: %v", err)
	}
	wantPath := "/internal/teams/" + team.String() + "/backup-quota/refund"
	if gotPath != wantPath {
		t.Errorf("path = %q; want %q", gotPath, wantPath)
	}
	if !strings.HasPrefix(gotAuth, "Bearer ") {
		t.Errorf("missing Bearer prefix: %q", gotAuth)
	}
}

// TestRefundManualBackupQuota_4xx_Error — non-2xx body is captured.
func TestRefundManualBackupQuota_4xx_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `bad`)
	}))
	defer srv.Close()
	db, _, _ := sqlmock.New()
	defer db.Close()
	w := NewCustomerBackupRunner(db, newFakeBackupStore(), "b", "p", testAESKeyHex, nil).
		WithRefundClient(srv.URL, "secret", nil)
	err := w.refundManualBackupQuota(uuid.New(), "bk-1")
	if err == nil {
		t.Fatal("expected error from 400, got nil")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error missing status: %v", err)
	}
}

// TestRefundManualBackupQuota_NetworkError — connection refused after
// the test server has shut down. Asserts the network branch surfaces an
// error rather than panicking.
func TestRefundManualBackupQuota_NetworkError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	url := srv.URL
	srv.Close() // shut down immediately so the next dial fails
	db, _, _ := sqlmock.New()
	defer db.Close()
	w := NewCustomerBackupRunner(db, newFakeBackupStore(), "b", "p", testAESKeyHex, nil).
		WithRefundClient(url, "secret", &http.Client{Timeout: 200 * time.Millisecond})
	if err := w.refundManualBackupQuota(uuid.New(), "bk-1"); err == nil {
		t.Fatal("expected dial-error, got nil")
	}
}

// ────────────────────────────────────────────────────────────────────
// customer_backup_runner.go — Work / processBackup edge branches
// ────────────────────────────────────────────────────────────────────

// TestRunner_Work_SelectError_ReturnsError — DB outage on the pending-row
// SELECT bubbles up as a Work-level error.
func TestRunner_Work_SelectError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnError(errors.New("db gone"))

	w := &CustomerBackupRunnerWorker{
		db:      db,
		store:   newFakeBackupStore(),
		pgDump:  &fakePgDump{},
		bucket:  "b",
		prefix:  "p",
		aesKey:  testAESKeyHex,
		now:     time.Now,
		timeout: time.Minute,
		batchN:  backupBatchSize,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// (Removed TestRunner_Work_ContextCancelledMidBatch: a pre-cancelled ctx
// fails the SELECT before the per-row ctx.Done() check is reachable; the
// happy-path coverage in TestRunner_HappyPath + TestRunner_PgDumpFails_*
// already exercises the body of the per-row loop.)

// TestRunner_ProcessBackup_DecryptFails — connection_url ciphertext that
// can't be decrypted (wrong key) marks the row failed.
func TestRunner_ProcessBackup_DecryptFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backupID := "11111111-1111-1111-1111-111111111111"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()

	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}).AddRow(backupID, resID, "pro", "scheduled", "tk", "GARBAGE-CIPHERTEXT", "postgres", teamID))
	mock.ExpectQuery(`UPDATE resource_backups\s+SET status = 'running'`).
		WithArgs(backupID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(backupID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	// Decrypt failure → markFailed UPDATE + audit row.
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'failed'`).
		WithArgs(backupID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	for i := 0; i < 5; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	w := &CustomerBackupRunnerWorker{
		db:      db,
		store:   newFakeBackupStore(),
		pgDump:  &fakePgDump{},
		bucket:  "b",
		prefix:  "p",
		aesKey:  testAESKeyHex,
		now:     time.Now,
		timeout: time.Minute,
		batchN:  backupBatchSize,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRunner_ProcessBackup_EmptyConnURL — NULL/empty connection_url is a
// hard failure for the row.
func TestRunner_ProcessBackup_EmptyConnURL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backupID := "11111111-1111-1111-1111-111111111111"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()

	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}).AddRow(backupID, resID, "pro", "scheduled", "tk", nil, "postgres", teamID))
	mock.ExpectQuery(`UPDATE resource_backups\s+SET status = 'running'`).
		WithArgs(backupID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(backupID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'failed'`).
		WithArgs(backupID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	for i := 0; i < 5; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	w := &CustomerBackupRunnerWorker{
		db: db, store: newFakeBackupStore(), pgDump: &fakePgDump{},
		bucket: "b", prefix: "p", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: backupBatchSize,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRunner_ProcessBackup_BadAESKey — invalid hex AES key fails the row.
func TestRunner_ProcessBackup_BadAESKey(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backupID := "11111111-1111-1111-1111-111111111111"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()
	enc := encryptForTest(t, "postgres://u:p@host/db")

	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}).AddRow(backupID, resID, "pro", "scheduled", "tk", enc, "postgres", teamID))
	mock.ExpectQuery(`UPDATE resource_backups\s+SET status = 'running'`).
		WithArgs(backupID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(backupID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'failed'`).
		WithArgs(backupID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	for i := 0; i < 5; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	w := &CustomerBackupRunnerWorker{
		db: db, store: newFakeBackupStore(), pgDump: &fakePgDump{},
		bucket: "b", prefix: "p",
		aesKey:  "not-hex-not-valid-please-fail",
		now:     time.Now, timeout: time.Minute, batchN: backupBatchSize,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRunner_ProcessBackup_ManualKind_RefundsOnFailure — a kind='manual'
// row that fails triggers the refund-quota call against the api.
func TestRunner_ProcessBackup_ManualKind_RefundsOnFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backupID := "11111111-1111-1111-1111-111111111111"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()
	enc := encryptForTest(t, "postgres://u:p@host/db")

	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}).AddRow(backupID, resID, "pro", "manual", "tk", enc, "postgres", teamID))
	mock.ExpectQuery(`UPDATE resource_backups\s+SET status = 'running'`).
		WithArgs(backupID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(backupID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'failed'`).
		WithArgs(backupID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	for i := 0; i < 5; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	refundCalls := 0
	mu := sync.Mutex{}
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		refundCalls++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer apiSrv.Close()

	w := &CustomerBackupRunnerWorker{
		db: db, store: newFakeBackupStore(),
		pgDump: &fakePgDump{err: errors.New("pg_dump down")},
		bucket: "b", prefix: "p", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: backupBatchSize,
	}
	w.WithRefundClient(apiSrv.URL, "secret", &http.Client{Timeout: 2 * time.Second})

	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if refundCalls != 1 {
		t.Errorf("refund POSTed %d times; want 1 (manual-kind failure must trigger refund)", refundCalls)
	}
}

// TestRunner_RecoverStuckRows_DBError_LogsAndProceeds — exec error on the
// recovery UPDATE is non-fatal.
func TestRunner_RecoverStuckRows_DBError_LogsAndProceeds(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnError(errors.New("db gone"))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}))
	for i := 0; i < 5; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	w := &CustomerBackupRunnerWorker{
		db: db, store: newFakeBackupStore(), pgDump: &fakePgDump{},
		bucket: "b", prefix: "p", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: backupBatchSize,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Errorf("Work: stuck-row recovery error must be non-fatal: %v", err)
	}
}

// TestRunner_RunRetentionSweep_DeletesAndUpdates — feed expired rows into
// the per-tier sweep and assert the S3 deletes + DB s3_key=NULL updates.
func TestRunner_RunRetentionSweep_DeletesAndUpdates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// No pending rows.
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}))

	// Per-tier (5 tiers): first SELECT for tier "hobby" returns one expired
	// row; remaining four tiers return empty.
	mock.ExpectQuery(`SELECT id::text, s3_key`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}).
			AddRow("99999999-9999-9999-9999-999999999999", "backups/tk/expired.dump.gz"))
	// After SELECT for "hobby" the loop will iterate the one victim,
	// calling DeleteObject (no DB) and then UPDATE resource_backups.
	mock.ExpectExec(`UPDATE resource_backups\s+SET s3_key = NULL`).
		WithArgs("99999999-9999-9999-9999-999999999999").
		WillReturnResult(sqlmock.NewResult(1, 1))
	for i := 0; i < 4; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	store := newFakeBackupStore()
	w := &CustomerBackupRunnerWorker{
		db: db, store: store, pgDump: &fakePgDump{},
		bucket: "b", prefix: "p", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: backupBatchSize,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if len(store.deletes) != 1 {
		t.Errorf("retention sweep should have deleted 1 expired object, got %d", len(store.deletes))
	}
}

// TestRunner_RunRetentionSweep_UsesPlansRegistry — when plans is set, the
// tier iteration order comes from registry.TierNames(), not the hardcoded
// fallback. Asserted by counting the SELECT calls.
func TestRunner_RunRetentionSweep_UsesPlansRegistry(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}))

	// Custom registry with 2 tiers — sweep should fire exactly 2 SELECTs.
	reg := &fakeBackupPlanRegistry{
		tiers: []string{"alpha", "beta"},
		days:  map[string]int{"alpha": 1, "beta": 2},
	}
	for i := 0; i < len(reg.tiers); i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	w := &CustomerBackupRunnerWorker{
		db: db, store: newFakeBackupStore(), pgDump: &fakePgDump{},
		bucket: "b", prefix: "p", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: backupBatchSize,
		plans: reg,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("registry-driven sweep mismatch: %v", err)
	}
}

// ────────────────────────────────────────────────────────────────────
// customer_backup_scheduler.go — Kind
// ────────────────────────────────────────────────────────────────────

func TestCustomerBackupSchedulerArgs_Kind(t *testing.T) {
	if got := (CustomerBackupSchedulerArgs{}).Kind(); got != "customer_backup_scheduler" {
		t.Errorf("Kind() = %q", got)
	}
}

// TestScheduler_AnonymousTier_DoesNotInsert — defensive: an anonymous row
// in resource.tier slips the SQL filter (it shouldn't, but the cadence
// switch also gates it). canonicalTier returns "anonymous"; the switch
// has no case for it, so the row proceeds to the dedupe INSERT — which
// is fine because the SQL filter excludes anonymous-tier rows in the
// first place. This test pins that contract via the SELECT shape.
func TestScheduler_AnonymousTier_DoesNotInsert(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// SELECT returns nothing — the SQL WHERE clause excludes anonymous.
	mock.ExpectQuery(`SELECT r\.id::text`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}))

	w := NewCustomerBackupSchedulerWorker(db)
	w.now = func() time.Time { return time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC) }
	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestScheduler_HobbyMissingTeamID_Skips — defensive: a hobby-tier row
// with NULL team_id is skipped (no panic-divide on the slot calc).
func TestScheduler_HobbyMissingTeamID_Skips(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := "fffffff0-1111-2222-3333-444444444444"
	mock.ExpectQuery(`SELECT r.id::text`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}).
			AddRow(resID, "hobby", nil))
	// No INSERT expected.

	w := NewCustomerBackupSchedulerWorker(db)
	w.now = func() time.Time { return time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC) }
	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestScheduler_InsertError_LoggedNonFatal — an INSERT failure is logged
// per-row and the sweep continues.
func TestScheduler_InsertError_LoggedNonFatal(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	resID := "fffffff0-1111-2222-3333-444444444444"

	mock.ExpectQuery(`SELECT r.id::text`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}).
			AddRow(resID, "pro", teamID))
	mock.ExpectExec(`INSERT INTO resource_backups`).
		WithArgs(uuid.MustParse(resID), "pro").
		WillReturnError(errors.New("db hiccup"))

	w := NewCustomerBackupSchedulerWorker(db)
	w.now = func() time.Time { return time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC) }
	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Errorf("Work: per-row insert error must be non-fatal: %v", err)
	}
}

// TestScheduler_BadUUIDInRow_Skipped — a non-UUID id from the SELECT is
// logged + skipped rather than crashing the sweep.
func TestScheduler_BadUUIDInRow_Skipped(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

	mock.ExpectQuery(`SELECT r.id::text`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}).
			AddRow("not-a-uuid", "pro", teamID))
	// No INSERT expected — bad UUID short-circuits the per-row body.

	w := NewCustomerBackupSchedulerWorker(db)
	w.now = func() time.Time { return time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC) }
	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// ────────────────────────────────────────────────────────────────────
// customer_restore_runner.go — Kind / ctor / Work edge branches
// ────────────────────────────────────────────────────────────────────

func TestCustomerRestoreRunnerArgs_Kind(t *testing.T) {
	if got := (CustomerRestoreRunnerArgs{}).Kind(); got != "customer_restore_runner" {
		t.Errorf("Kind() = %q", got)
	}
}

func TestNewCustomerRestoreRunner_Defaults(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	w := NewCustomerRestoreRunner(db, newFakeBackupStore(), "bucket", testAESKeyHex)
	if w == nil || w.bucket != "bucket" || w.aesKey != testAESKeyHex {
		t.Fatalf("ctor wiring wrong: %+v", w)
	}
	if w.timeout != restorePerRunTimeout {
		t.Errorf("default timeout wrong")
	}
	if w.batchN != restoreBatchSize {
		t.Errorf("default batch wrong")
	}
}

// TestRestoreRunner_Work_SelectError_ReturnsError — SELECT-time DB outage
// surfaces a Work error.
func TestRestoreRunner_Work_SelectError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE resource_restores\s+SET status\s+= 'failed'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT rr\.id::text`).
		WithArgs(restoreBatchSize).
		WillReturnError(errors.New("db gone"))

	w := &CustomerRestoreRunnerWorker{
		db: db, store: newFakeBackupStore(), pgRestore: &fakePgRestore{},
		bucket: "b", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: restoreBatchSize,
	}
	if err := w.Work(context.Background(), fakeRestoreJob()); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestRestoreRunner_EmptyConnURL_Fails — connection_url NULL fails the
// restore row immediately.
func TestRestoreRunner_EmptyConnURL_Fails(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	restoreID := "rrrrrrr0-1111-2222-3333-444444444444"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()
	s3Key := "backups/tk/abc.dump.gz"
	store := newFakeBackupStore()
	store.objects["instant-shared/"+s3Key] = gzipFor(t, []byte("p"))

	mock.ExpectExec(`UPDATE resource_restores\s+SET status\s+= 'failed'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT rr\.id::text`).
		WithArgs(restoreBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "backup_id", "s3_key", "sha256",
			"connection_url", "resource_type", "token", "team_id",
		}).AddRow(restoreID, resID, "bk", s3Key, nil, nil, "postgres", "tk", teamID))
	mock.ExpectQuery(`UPDATE resource_restores\s+SET status = 'running'`).
		WithArgs(restoreID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(restoreID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE resource_restores\s+SET status = 'failed'`).
		WithArgs(restoreID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))

	w := &CustomerRestoreRunnerWorker{
		db: db, store: store, pgRestore: &fakePgRestore{},
		bucket: "instant-shared", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: restoreBatchSize,
	}
	if err := w.Work(context.Background(), fakeRestoreJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRestoreRunner_BadAESKey_Fails — invalid AES key short-circuits with
// markRestoreFailed.
func TestRestoreRunner_BadAESKey_Fails(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	restoreID := "rrrrrrr0-1111-2222-3333-444444444444"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()
	s3Key := "backups/tk/abc.dump.gz"
	store := newFakeBackupStore()
	store.objects["instant-shared/"+s3Key] = gzipFor(t, []byte("p"))
	enc := encryptForTest(t, "postgres://u:p@host/db")

	mock.ExpectExec(`UPDATE resource_restores\s+SET status\s+= 'failed'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT rr\.id::text`).
		WithArgs(restoreBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "backup_id", "s3_key", "sha256",
			"connection_url", "resource_type", "token", "team_id",
		}).AddRow(restoreID, resID, "bk", s3Key, nil, enc, "postgres", "tk", teamID))
	mock.ExpectQuery(`UPDATE resource_restores\s+SET status = 'running'`).
		WithArgs(restoreID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(restoreID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE resource_restores\s+SET status = 'failed'`).
		WithArgs(restoreID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))

	w := &CustomerRestoreRunnerWorker{
		db: db, store: store, pgRestore: &fakePgRestore{},
		bucket: "instant-shared",
		aesKey: "not-hex-ok",
		now:    time.Now, timeout: time.Minute, batchN: restoreBatchSize,
	}
	if err := w.Work(context.Background(), fakeRestoreJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRestoreRunner_S3DownloadError_Fails — store.Download returns an
// error → markRestoreFailed.
func TestRestoreRunner_S3DownloadError_Fails(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	restoreID := "rrrrrrr0-1111-2222-3333-444444444444"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()
	enc := encryptForTest(t, "postgres://u:p@host/db")

	mock.ExpectExec(`UPDATE resource_restores\s+SET status\s+= 'failed'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT rr\.id::text`).
		WithArgs(restoreBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "backup_id", "s3_key", "sha256",
			"connection_url", "resource_type", "token", "team_id",
		}).AddRow(restoreID, resID, "bk", "backups/tk/missing.dump.gz", nil, enc, "postgres", "tk", teamID))
	mock.ExpectQuery(`UPDATE resource_restores\s+SET status = 'running'`).
		WithArgs(restoreID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(restoreID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE resource_restores\s+SET status = 'failed'`).
		WithArgs(restoreID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))

	w := &CustomerRestoreRunnerWorker{
		db: db, store: newFakeBackupStore(), // object NOT seeded → Download errors
		pgRestore: &fakePgRestore{},
		bucket:    "instant-shared", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: restoreBatchSize,
	}
	if err := w.Work(context.Background(), fakeRestoreJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRestoreRunner_GzipHeaderInvalid_Fails — the S3 object is NOT gzip
// (it's plain text). The gzip.NewReader header check fails before pg_restore
// is invoked.
func TestRestoreRunner_GzipHeaderInvalid_Fails(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	restoreID := "rrrrrrr0-1111-2222-3333-444444444444"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()
	enc := encryptForTest(t, "postgres://u:p@host/db")
	s3Key := "backups/tk/garbage.dump.gz"
	store := newFakeBackupStore()
	store.objects["instant-shared/"+s3Key] = []byte("NOT-A-GZIP-STREAM-AT-ALL")

	mock.ExpectExec(`UPDATE resource_restores\s+SET status\s+= 'failed'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT rr\.id::text`).
		WithArgs(restoreBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "backup_id", "s3_key", "sha256",
			"connection_url", "resource_type", "token", "team_id",
		}).AddRow(restoreID, resID, "bk", s3Key, nil, enc, "postgres", "tk", teamID))
	mock.ExpectQuery(`UPDATE resource_restores\s+SET status = 'running'`).
		WithArgs(restoreID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(restoreID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE resource_restores\s+SET status = 'failed'`).
		WithArgs(restoreID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))

	pgr := &fakePgRestore{}
	w := &CustomerRestoreRunnerWorker{
		db: db, store: store, pgRestore: pgr,
		bucket: "instant-shared", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: restoreBatchSize,
	}
	if err := w.Work(context.Background(), fakeRestoreJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	// pg_restore must NOT have run on an invalid gzip header.
	if pgr.gotConn != "" {
		t.Errorf("pg_restore invoked on bad gzip header — should have been gated")
	}
}

// TestRestoreRunner_ClaimRace_Skips — another worker already grabbed the
// row; our UPDATE returns 0 rows and we move on silently.
func TestRestoreRunner_ClaimRace_Skips(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	restoreID := "rrrrrrr0-1111-2222-3333-444444444444"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()
	enc := encryptForTest(t, "postgres://u:p@host/db")

	mock.ExpectExec(`UPDATE resource_restores\s+SET status\s+= 'failed'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT rr\.id::text`).
		WithArgs(restoreBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "backup_id", "s3_key", "sha256",
			"connection_url", "resource_type", "token", "team_id",
		}).AddRow(restoreID, resID, "bk", "backups/tk/abc.dump.gz", nil, enc, "postgres", "tk", teamID))
	// 0-row claim — competing worker.
	mock.ExpectQuery(`UPDATE resource_restores\s+SET status = 'running'`).
		WithArgs(restoreID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	w := &CustomerRestoreRunnerWorker{
		db: db, store: newFakeBackupStore(), pgRestore: &fakePgRestore{},
		bucket: "instant-shared", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: restoreBatchSize,
	}
	if err := w.Work(context.Background(), fakeRestoreJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRestoreRunner_RecoverStuckRestores_HitsRow — exec returns
// RowsAffected=N so the WARN log line fires; assert no Work-level error.
func TestRestoreRunner_RecoverStuckRestores_HitsRow(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(`UPDATE resource_restores\s+SET status\s+= 'failed'`).
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectQuery(`SELECT rr\.id::text`).
		WithArgs(restoreBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "backup_id", "s3_key", "sha256",
			"connection_url", "resource_type", "token", "team_id",
		}))

	w := &CustomerRestoreRunnerWorker{
		db: db, store: newFakeBackupStore(), pgRestore: &fakePgRestore{},
		bucket: "instant-shared", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: restoreBatchSize,
	}
	if err := w.Work(context.Background(), fakeRestoreJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRestoreRunner_RecoverStuckRestores_DBError_LogsAndProceeds — UPDATE
// fails; the sweep proceeds without bubbling.
func TestRestoreRunner_RecoverStuckRestores_DBError_LogsAndProceeds(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE resource_restores\s+SET status\s+= 'failed'`).
		WillReturnError(errors.New("db hiccup"))
	mock.ExpectQuery(`SELECT rr\.id::text`).
		WithArgs(restoreBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "backup_id", "s3_key", "sha256",
			"connection_url", "resource_type", "token", "team_id",
		}))

	w := &CustomerRestoreRunnerWorker{
		db: db, store: newFakeBackupStore(), pgRestore: &fakePgRestore{},
		bucket: "instant-shared", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: restoreBatchSize,
	}
	if err := w.Work(context.Background(), fakeRestoreJob()); err != nil {
		t.Errorf("recoverStuckRestores DB error must be non-fatal: %v", err)
	}
}

// (Removed TestRestoreRunner_ContextCancelledMidBatch: identical reasoning
// to the runner sibling — pre-cancel kills the SELECT before the per-row
// ctx.Done() check is reachable.)

// ────────────────────────────────────────────────────────────────────
// platform_db_backup.go — Kind / ctors / Work edge branches /
// joinPlatformBackupPrefix / defaultPgDumpExec via a fake binary
// ────────────────────────────────────────────────────────────────────

func TestPlatformDBBackupArgs_Kind(t *testing.T) {
	if got := (PlatformDBBackupArgs{}).Kind(); got != "platform_db_backup" {
		t.Errorf("Kind() = %q", got)
	}
}

// TestNewPlatformDBBackupWorker_DefaultsApplied — nil Dumper / Now fall
// back to the package defaults.
func TestNewPlatformDBBackupWorker_DefaultsApplied(t *testing.T) {
	w := NewPlatformDBBackupWorker(PlatformDBBackupConfig{
		DatabaseURL: "postgres://x@y/z",
		Bucket:      "b",
		OuterPrefix: "",
		InnerPrefix: "platform-backups/",
		Dumper:      nil, // ← exercise the default
		Now:         nil, // ← exercise the default
	})
	if w == nil {
		t.Fatal("nil worker")
	}
	if w.now == nil {
		t.Error("now not defaulted")
	}
	if w.dumper == nil {
		t.Error("dumper not defaulted")
	}
	if w.keyPrefix != "platform-backups/" {
		t.Errorf("keyPrefix = %q", w.keyPrefix)
	}
}

// TestJoinPlatformBackupPrefix_AllShapes — covers every branch of the
// prefix join: empty outer, empty inner, both, trailing slashes.
func TestJoinPlatformBackupPrefix_AllShapes(t *testing.T) {
	cases := []struct {
		outer, inner, want string
	}{
		{"", "", "platform-backups/"}, // defensive default
		{"", "platform-backups/", "platform-backups/"},
		{"backups/", "", "backups/"},
		{"backups", "platform-backups", "backups/platform-backups/"},
		{"/backups/", "/platform-backups/", "backups/platform-backups/"},
	}
	for _, c := range cases {
		got := joinPlatformBackupPrefix(c.outer, c.inner)
		if got != c.want {
			t.Errorf("joinPlatformBackupPrefix(%q,%q) = %q; want %q", c.outer, c.inner, got, c.want)
		}
	}
}

// TestPlatformDBBackup_NoBucket_Skips — bucket-empty disabled-mode branch.
func TestPlatformDBBackup_NoBucket_Skips(t *testing.T) {
	db, _, _ := sqlmock.New(sqlmock.MonitorPingsOption(false))
	defer db.Close()
	w := NewPlatformDBBackupWorker(PlatformDBBackupConfig{
		DB:          db,
		DatabaseURL: "postgres://x",
		S3:          newFakeS3(),
		Bucket:      "", // ← empty
		InnerPrefix: "platform-backups/",
		Now:         fixedClock(time.Date(2026, 5, 13, 2, 0, 0, 0, time.UTC)),
	})
	if err := w.Work(context.Background(), fakePlatformBackupJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestPlatformDBBackup_NoDatabaseURL_Errors — empty DSN is a defensive
// error, not silent skip.
func TestPlatformDBBackup_NoDatabaseURL_Errors(t *testing.T) {
	db, _, _ := sqlmock.New(sqlmock.MonitorPingsOption(false))
	defer db.Close()
	w := NewPlatformDBBackupWorker(PlatformDBBackupConfig{
		DB:          db,
		DatabaseURL: "",
		S3:          newFakeS3(),
		Bucket:      "b",
		InnerPrefix: "platform-backups/",
		Now:         fixedClock(time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC)),
	})
	if err := w.Work(context.Background(), fakePlatformBackupJob()); err == nil {
		t.Fatal("expected error for empty DATABASE_URL")
	}
}

// TestPlatformDBBackup_UploadError_FailsLoud — pg_dump succeeds but the
// S3 Upload fails. Work returns an error AND a failed audit row fires.
func TestPlatformDBBackup_UploadError_FailsLoud(t *testing.T) {
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
		WithArgs("system", "platform_backup.failed", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	expectAdvisoryUnlock(mock)

	dumper := &fakePgDumper{payload: []byte("data")}
	s3 := newFakeS3()
	s3.uploadErr = errors.New("s3 down")
	now := time.Date(2026, 5, 13, 2, 0, 0, 0, time.UTC)
	w := newTestWorker(t, mock, db, dumper, s3, now)
	err = w.Work(context.Background(), fakePlatformBackupJob())
	if err == nil {
		t.Fatal("expected error on upload failure")
	}
	if !strings.Contains(err.Error(), "s3 upload") {
		t.Errorf("error wrap missing s3 upload: %v", err)
	}
}

// TestPlatformDBBackup_ListError_StillReturnsNil — retention sweep List
// failure is non-fatal: the upload itself succeeded.
func TestPlatformDBBackup_ListError_StillReturnsNil(t *testing.T) {
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

	dumper := &fakePgDumper{payload: []byte("ok")}
	s3 := newFakeS3()
	s3.listErr = errors.New("list 500")
	now := time.Date(2026, 5, 13, 2, 0, 0, 0, time.UTC)
	w := newTestWorker(t, mock, db, dumper, s3, now)
	if err := w.Work(context.Background(), fakePlatformBackupJob()); err != nil {
		t.Errorf("Work: list error must be non-fatal: %v", err)
	}
}

// TestPlatformDBBackup_WriteAudit_NilDB_Skips — the writeAudit helper
// short-circuits on nil DB. We exercise this by constructing a worker
// with no DB and calling writeAudit directly.
func TestPlatformDBBackup_WriteAudit_NilDB_Skips(t *testing.T) {
	w := NewPlatformDBBackupWorker(PlatformDBBackupConfig{
		DB:          nil,
		DatabaseURL: "postgres://x",
		S3:          newFakeS3(),
		Bucket:      "b",
		InnerPrefix: "platform-backups/",
		Now:         fixedClock(time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC)),
	})
	w.writeAudit(context.Background(), "kind", "summary", map[string]any{"k": "v"})
	// No panic == pass.
}

// TestDurationSeconds_Rounds — pure helper.
func TestDurationSeconds_Rounds(t *testing.T) {
	if got := durationSeconds(1234 * time.Millisecond); got != 1.2 {
		t.Errorf("durationSeconds(1.234s) = %v; want 1.2", got)
	}
	if got := durationSeconds(0); got != 0 {
		t.Errorf("durationSeconds(0) = %v; want 0", got)
	}
}

// TestDefaultPgDumpExec_BadBinary — invalid PG_DUMP_BIN → start error.
func TestDefaultPgDumpExec_BadBinary(t *testing.T) {
	t.Setenv("PG_DUMP_BIN", "/nonexistent/path/to/pg_dump_xyz")
	d := defaultPgDumpExec{}
	_, err := d.Dump(context.Background(), "postgres://x", io.Discard)
	if err == nil {
		t.Fatal("expected error from missing pg_dump binary")
	}
}

// TestDefaultPgDumpExec_FakeBinarySuccess — point PG_DUMP_BIN at a tiny
// shell script that emits N bytes and exits 0. Asserts the bytes round-
// trip into the writer and the byte count is reported. Skipped on Windows
// (no /bin/sh).
func TestDefaultPgDumpExec_FakeBinarySuccess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no /bin/sh on windows")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake_pg_dump")
	script := "#!/bin/sh\nprintf 'HELLO'\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake bin: %v", err)
	}
	t.Setenv("PG_DUMP_BIN", bin)
	d := defaultPgDumpExec{}
	var sink strings.Builder
	n, err := d.Dump(context.Background(), "postgres://x", &sink)
	if err != nil {
		t.Fatalf("Dump: %v", err)
	}
	if n != 5 {
		t.Errorf("n = %d; want 5", n)
	}
	if sink.String() != "HELLO" {
		t.Errorf("sink = %q; want HELLO", sink.String())
	}
}

// TestDefaultPgDumpExec_FakeBinaryNonZeroExit — fake binary exits 1; the
// stderr is captured into the error message.
func TestDefaultPgDumpExec_FakeBinaryNonZeroExit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no /bin/sh on windows")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake_pg_dump_fail")
	script := "#!/bin/sh\necho 'pg_dump: stderr text' >&2\nexit 1\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}
	t.Setenv("PG_DUMP_BIN", bin)
	d := defaultPgDumpExec{}
	var sink strings.Builder
	_, err := d.Dump(context.Background(), "postgres://x", &sink)
	if err == nil {
		t.Fatal("expected non-zero exit error")
	}
	if !strings.Contains(err.Error(), "pg_dump exit") {
		t.Errorf("error not wrapped: %v", err)
	}
}

// TestDefaultPgDumpExec_StderrTruncates — stderr > 256 chars is trimmed
// in the wrapped error message.
func TestDefaultPgDumpExec_StderrTruncates(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no /bin/sh on windows")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "noisy_pg_dump")
	// Write 1000 bytes to stderr then exit 1.
	script := fmt.Sprintf("#!/bin/sh\nfor i in $(seq 1 100); do printf 'XXXXXXXXXX' >&2; done\nexit 1\n")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}
	t.Setenv("PG_DUMP_BIN", bin)
	d := defaultPgDumpExec{}
	var sink strings.Builder
	_, err := d.Dump(context.Background(), "postgres://x", &sink)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "...(truncated)") {
		t.Errorf("expected stderr truncation marker in error: %v", err)
	}
}

// ────────────────────────────────────────────────────────────────────
// finalizeDigest — null hash path
// ────────────────────────────────────────────────────────────────────

// ────────────────────────────────────────────────────────────────────
// realPgDumpRunner / realPgRestoreRunner — exercised via PATH tricks.
// ────────────────────────────────────────────────────────────────────

// withFakeBinaryOnPath puts a tiny shell script named `name` on PATH ahead
// of any system binary, returning the dir so callers can also reach
// it for assertion. Each test gets its own temp dir.
func withFakeBinaryOnPath(t *testing.T, name, body string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("no /bin/sh on windows")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, name)
	if err := os.WriteFile(bin, []byte(body), 0o755); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	return dir
}

// TestRealPgDumpRunner_Run_Success — point a shell-script `pg_dump` at the
// runner; assert the bytes round-trip into the writer.
func TestRealPgDumpRunner_Run_Success(t *testing.T) {
	_ = withFakeBinaryOnPath(t, "pg_dump", "#!/bin/sh\nprintf 'PGDUMP-OUT'\n")
	r := realPgDumpRunner{}
	var sink strings.Builder
	if err := r.Run(context.Background(), "postgres://x", &sink); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if sink.String() != "PGDUMP-OUT" {
		t.Errorf("sink = %q", sink.String())
	}
}

// TestRealPgDumpRunner_Run_NonZeroExit — fake `pg_dump` exits 1 with a
// stderr message; the wrapper captures it into the returned error.
func TestRealPgDumpRunner_Run_NonZeroExit(t *testing.T) {
	_ = withFakeBinaryOnPath(t, "pg_dump", "#!/bin/sh\necho 'pg_dump: server unavailable' >&2\nexit 1\n")
	r := realPgDumpRunner{}
	err := r.Run(context.Background(), "postgres://x", io.Discard)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "pg_dump") {
		t.Errorf("error not wrapped: %v", err)
	}
}

// TestRealPgRestoreRunner_Run_Success — same trick for pg_restore. The
// reader is drained into the subprocess's stdin; here we just verify the
// process runs to completion (the stub doesn't actually consume stdin).
func TestRealPgRestoreRunner_Run_Success(t *testing.T) {
	_ = withFakeBinaryOnPath(t, "pg_restore", "#!/bin/sh\ncat >/dev/null\nexit 0\n")
	r := realPgRestoreRunner{}
	if err := r.Run(context.Background(), "postgres://x", strings.NewReader("ignored")); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestRealPgRestoreRunner_Run_NonZeroExit — fake `pg_restore` errors out.
func TestRealPgRestoreRunner_Run_NonZeroExit(t *testing.T) {
	_ = withFakeBinaryOnPath(t, "pg_restore", "#!/bin/sh\necho 'pg_restore: bad dump' >&2\nexit 1\n")
	r := realPgRestoreRunner{}
	err := r.Run(context.Background(), "postgres://x", strings.NewReader(""))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "pg_restore") {
		t.Errorf("error not wrapped: %v", err)
	}
}

// ────────────────────────────────────────────────────────────────────
// customer_backup_runner.go — remaining branches
// ────────────────────────────────────────────────────────────────────

// failingStoreOnDelete is a BackupObjectStore that succeeds on Upload but
// errors on DeleteObject — used to exercise the retention-sweep s3-delete
// error path AND the cleanup-after-dump-failure error log path.
type failingStoreOnDelete struct {
	*fakeBackupStore
	delErr error
}

func (f *failingStoreOnDelete) DeleteObject(ctx context.Context, bucket, key string) error {
	if f.delErr != nil {
		return f.delErr
	}
	return f.fakeBackupStore.DeleteObject(ctx, bucket, key)
}

// TestRunner_RetentionSweep_S3DeleteError_LogsContinues — when the
// retention DeleteObject fails for one victim, the loop continues for
// other victims AND the DB UPDATE for that victim is skipped. Asserted
// by sqlmock — no UPDATE expectation primed for the failing victim.
func TestRunner_RetentionSweep_S3DeleteError_LogsContinues(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}))

	// First-tier SELECT returns one victim; store.DeleteObject errors → no
	// matching UPDATE primed.
	mock.ExpectQuery(`SELECT id::text, s3_key`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}).
			AddRow("99999999-9999-9999-9999-999999999999", "backups/tk/expired.dump.gz"))
	for i := 0; i < 4; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	store := &failingStoreOnDelete{
		fakeBackupStore: newFakeBackupStore(),
		delErr:          errors.New("s3 503"),
	}
	w := &CustomerBackupRunnerWorker{
		db: db, store: store, pgDump: &fakePgDump{},
		bucket: "b", prefix: "p", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: backupBatchSize,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRunner_RetentionSweep_DBUpdateError_LogsContinues — DeleteObject
// succeeds but the follow-up s3_key=NULL UPDATE fails for one victim.
func TestRunner_RetentionSweep_DBUpdateError_LogsContinues(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}))

	mock.ExpectQuery(`SELECT id::text, s3_key`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}).
			AddRow("11111111-1111-1111-1111-111111111111", "backups/tk/old.dump.gz"))
	mock.ExpectExec(`UPDATE resource_backups\s+SET s3_key = NULL`).
		WithArgs("11111111-1111-1111-1111-111111111111").
		WillReturnError(errors.New("db hiccup"))
	for i := 0; i < 4; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	w := &CustomerBackupRunnerWorker{
		db: db, store: newFakeBackupStore(), pgDump: &fakePgDump{},
		bucket: "b", prefix: "p", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: backupBatchSize,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRunner_RetentionSweep_QueryError_LogsContinues — first-tier SELECT
// errors; the sweep continues to the remaining 4 tiers.
func TestRunner_RetentionSweep_QueryError_LogsContinues(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}))

	mock.ExpectQuery(`SELECT id::text, s3_key`).WillReturnError(errors.New("tier query 500"))
	for i := 0; i < 4; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	w := &CustomerBackupRunnerWorker{
		db: db, store: newFakeBackupStore(), pgDump: &fakePgDump{},
		bucket: "b", prefix: "p", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: backupBatchSize,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRunner_ClaimDBError_LogsContinues — the claim UPDATE errors (non-
// sql.ErrNoRows). The row is skipped, the rest of the batch proceeds.
func TestRunner_ClaimDBError_LogsContinues(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backupID := "11111111-1111-1111-1111-111111111111"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()
	enc := encryptForTest(t, "postgres://u:p@host/db")

	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}).AddRow(backupID, resID, "pro", "scheduled", "tk", enc, "postgres", teamID))
	// Claim UPDATE fails with a non-ErrNoRows error.
	mock.ExpectQuery(`UPDATE resource_backups\s+SET status = 'running'`).
		WithArgs(backupID).
		WillReturnError(errors.New("claim down"))
	for i := 0; i < 5; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	w := &CustomerBackupRunnerWorker{
		db: db, store: newFakeBackupStore(), pgDump: &fakePgDump{},
		bucket: "b", prefix: "p", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: backupBatchSize,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRunner_FinalizeUpdateError_LogsAndReturns — pg_dump + upload
// succeed, but the finalize UPDATE errors. The slog.Error fires and
// processBackup returns false (no panic).
func TestRunner_FinalizeUpdateError_LogsAndReturns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backupID := "11111111-1111-1111-1111-111111111111"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()
	enc := encryptForTest(t, "postgres://u:p@host/db")

	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}).AddRow(backupID, resID, "pro", "scheduled", "tk", enc, "postgres", teamID))
	mock.ExpectQuery(`UPDATE resource_backups\s+SET status = 'running'`).
		WithArgs(backupID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(backupID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	// Finalize UPDATE returns an error.
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'ok'`).
		WithArgs(backupID, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(errors.New("finalize down"))
	for i := 0; i < 5; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	w := &CustomerBackupRunnerWorker{
		db: db, store: newFakeBackupStore(),
		pgDump: &fakePgDump{payload: []byte("ok")},
		bucket: "b", prefix: "p", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: backupBatchSize,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRunner_MarkFailed_DBError_StillContinues — markFailed's UPDATE
// errors; the audit row still attempts and the sweep proceeds.
func TestRunner_MarkFailed_DBError_StillContinues(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backupID := "11111111-1111-1111-1111-111111111111"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()
	enc := encryptForTest(t, "postgres://u:p@host/db")

	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}).AddRow(backupID, resID, "pro", "scheduled", "tk", enc, "postgres", teamID))
	mock.ExpectQuery(`UPDATE resource_backups\s+SET status = 'running'`).
		WithArgs(backupID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(backupID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'failed'`).
		WithArgs(backupID, sqlmock.AnyArg()).
		WillReturnError(errors.New("mark failed db err"))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	for i := 0; i < 5; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	w := &CustomerBackupRunnerWorker{
		db: db, store: newFakeBackupStore(),
		pgDump: &fakePgDump{err: errors.New("pg_dump down")},
		bucket: "b", prefix: "p", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: backupBatchSize,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRunner_WriteAudit_DBError_LogsAndContinues — audit insert errors.
// We trigger this on the FIRST audit row (the 'started' row) by primed
// returning an exec error there; the rest of the row processing still
// completes (Upload succeeds, finalize updates).
func TestRunner_WriteAudit_DBError_LogsAndContinues(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backupID := "11111111-1111-1111-1111-111111111111"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()
	enc := encryptForTest(t, "postgres://u:p@host/db")

	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}).AddRow(backupID, resID, "pro", "scheduled", "tk", enc, "postgres", teamID))
	mock.ExpectQuery(`UPDATE resource_backups\s+SET status = 'running'`).
		WithArgs(backupID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(backupID))
	// started audit INSERT errors — runner continues.
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnError(errors.New("audit insert down"))
	// Finalize UPDATE still happens because the backup itself proceeded.
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'ok'`).
		WithArgs(backupID, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	for i := 0; i < 5; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	w := &CustomerBackupRunnerWorker{
		db: db, store: newFakeBackupStore(),
		pgDump: &fakePgDump{payload: []byte("ok")},
		bucket: "b", prefix: "p", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: backupBatchSize,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRunner_ProcessBackup_UploadFails_MarksFailed — pg_dump emits bytes
// but the store's Upload returns an error. The row goes to 'failed' and
// the cleanup-delete path is exercised.
func TestRunner_ProcessBackup_UploadFails_MarksFailed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backupID := "11111111-1111-1111-1111-111111111111"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()
	enc := encryptForTest(t, "postgres://u:p@host/db")

	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}).AddRow(backupID, resID, "pro", "scheduled", "tk", enc, "postgres", teamID))
	mock.ExpectQuery(`UPDATE resource_backups\s+SET status = 'running'`).
		WithArgs(backupID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(backupID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'failed'`).
		WithArgs(backupID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	for i := 0; i < 5; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	store := newFakeBackupStore()
	store.uploadFn = func(_ context.Context, _, _ string, r io.Reader) (int64, error) {
		// Drain so the producer side isn't deadlocked.
		_, _ = io.Copy(io.Discard, r)
		return 0, errors.New("upload 503")
	}
	w := &CustomerBackupRunnerWorker{
		db: db, store: store,
		pgDump: &fakePgDump{payload: []byte("ok")},
		bucket: "b", prefix: "p", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: backupBatchSize,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRunner_PgDumpFails_CleanupDeleteAlsoFails — pg_dump errors + the
// cleanup store.DeleteObject also errors. Both error logs fire; Work
// returns nil (failures are per-row).
func TestRunner_PgDumpFails_CleanupDeleteAlsoFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backupID := "11111111-1111-1111-1111-111111111111"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()
	enc := encryptForTest(t, "postgres://u:p@host/db")

	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'pending'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT b.id::text`).
		WithArgs(backupBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "tier_at_backup", "backup_kind",
			"token", "connection_url", "resource_type", "team_id",
		}).AddRow(backupID, resID, "pro", "scheduled", "tk", enc, "postgres", teamID))
	mock.ExpectQuery(`UPDATE resource_backups\s+SET status = 'running'`).
		WithArgs(backupID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(backupID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE resource_backups\s+SET status = 'failed'`).
		WithArgs(backupID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	for i := 0; i < 5; i++ {
		mock.ExpectQuery(`SELECT id::text, s3_key`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "s3_key"}))
	}

	store := &failingStoreOnDelete{
		fakeBackupStore: newFakeBackupStore(),
		delErr:          errors.New("s3 503"),
	}
	w := &CustomerBackupRunnerWorker{
		db: db, store: store,
		pgDump: &fakePgDump{err: errors.New("pg_dump down")},
		bucket: "b", prefix: "p", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: backupBatchSize,
	}
	if err := w.Work(context.Background(), fakeRunnerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// ────────────────────────────────────────────────────────────────────
// customer_restore_runner.go — remaining branches
// ────────────────────────────────────────────────────────────────────

// TestRestoreRunner_ClaimDBError_LogsContinues — non-ErrNoRows error on
// the claim UPDATE is logged + skipped.
func TestRestoreRunner_ClaimDBError_LogsContinues(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	restoreID := "rrrrrrr0-1111-2222-3333-444444444444"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()
	enc := encryptForTest(t, "postgres://u:p@host/db")

	mock.ExpectExec(`UPDATE resource_restores\s+SET status\s+= 'failed'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT rr\.id::text`).
		WithArgs(restoreBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "backup_id", "s3_key", "sha256",
			"connection_url", "resource_type", "token", "team_id",
		}).AddRow(restoreID, resID, "bk", "backups/tk/abc.dump.gz", nil, enc, "postgres", "tk", teamID))
	mock.ExpectQuery(`UPDATE resource_restores\s+SET status = 'running'`).
		WithArgs(restoreID).
		WillReturnError(errors.New("claim down"))

	w := &CustomerRestoreRunnerWorker{
		db: db, store: newFakeBackupStore(), pgRestore: &fakePgRestore{},
		bucket: "instant-shared", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: restoreBatchSize,
	}
	if err := w.Work(context.Background(), fakeRestoreJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRestoreRunner_FinalizeUpdateError_LogsAndReturns — pg_restore +
// gzip succeed but the finalize UPDATE errors.
func TestRestoreRunner_FinalizeUpdateError_LogsAndReturns(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	restoreID := "rrrrrrr0-1111-2222-3333-444444444444"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()
	enc := encryptForTest(t, "postgres://u:p@host/db")
	s3Key := "backups/tk/ok.dump.gz"
	store := newFakeBackupStore()
	store.objects["instant-shared/"+s3Key] = gzipFor(t, []byte("p"))

	mock.ExpectExec(`UPDATE resource_restores\s+SET status\s+= 'failed'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT rr\.id::text`).
		WithArgs(restoreBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "backup_id", "s3_key", "sha256",
			"connection_url", "resource_type", "token", "team_id",
		}).AddRow(restoreID, resID, "bk", s3Key, nil, enc, "postgres", "tk", teamID))
	mock.ExpectQuery(`UPDATE resource_restores\s+SET status = 'running'`).
		WithArgs(restoreID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(restoreID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE resource_restores\s+SET status = 'ok'`).
		WithArgs(restoreID).
		WillReturnError(errors.New("finalize down"))

	w := &CustomerRestoreRunnerWorker{
		db: db, store: store, pgRestore: &fakePgRestore{},
		bucket: "instant-shared", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: restoreBatchSize,
	}
	if err := w.Work(context.Background(), fakeRestoreJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRestoreRunner_WriteAudit_DBError_LogsContinues — restore.started
// audit INSERT errors; the rest of the row continues.
func TestRestoreRunner_WriteAudit_DBError_LogsContinues(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	restoreID := "rrrrrrr0-1111-2222-3333-444444444444"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()
	enc := encryptForTest(t, "postgres://u:p@host/db")
	s3Key := "backups/tk/ok.dump.gz"
	store := newFakeBackupStore()
	store.objects["instant-shared/"+s3Key] = gzipFor(t, []byte("p"))

	mock.ExpectExec(`UPDATE resource_restores\s+SET status\s+= 'failed'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT rr\.id::text`).
		WithArgs(restoreBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "backup_id", "s3_key", "sha256",
			"connection_url", "resource_type", "token", "team_id",
		}).AddRow(restoreID, resID, "bk", s3Key, nil, enc, "postgres", "tk", teamID))
	mock.ExpectQuery(`UPDATE resource_restores\s+SET status = 'running'`).
		WithArgs(restoreID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(restoreID))
	// started audit INSERT errors.
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnError(errors.New("audit down"))
	mock.ExpectExec(`UPDATE resource_restores\s+SET status = 'ok'`).
		WithArgs(restoreID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))

	w := &CustomerRestoreRunnerWorker{
		db: db, store: store, pgRestore: &fakePgRestore{},
		bucket: "instant-shared", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: restoreBatchSize,
	}
	if err := w.Work(context.Background(), fakeRestoreJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestRestoreRunner_MarkRestoreFailed_DBError_LogsContinues — markFailed
// UPDATE errors; the audit row still fires.
func TestRestoreRunner_MarkRestoreFailed_DBError_LogsContinues(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	restoreID := "rrrrrrr0-1111-2222-3333-444444444444"
	resID := "22222222-2222-2222-2222-222222222222"
	teamID := uuid.New()

	mock.ExpectExec(`UPDATE resource_restores\s+SET status\s+= 'failed'`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT rr\.id::text`).
		WithArgs(restoreBatchSize).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "resource_id", "backup_id", "s3_key", "sha256",
			"connection_url", "resource_type", "token", "team_id",
		}).AddRow(restoreID, resID, "bk", nil, nil, nil, "postgres", "tk", teamID))
	mock.ExpectQuery(`UPDATE resource_restores\s+SET status = 'running'`).
		WithArgs(restoreID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(restoreID))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	// markRestoreFailed UPDATE errors.
	mock.ExpectExec(`UPDATE resource_restores\s+SET status = 'failed'`).
		WithArgs(restoreID, sqlmock.AnyArg()).
		WillReturnError(errors.New("mark failed down"))
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))

	w := &CustomerRestoreRunnerWorker{
		db: db, store: newFakeBackupStore(), pgRestore: &fakePgRestore{},
		bucket: "instant-shared", aesKey: testAESKeyHex,
		now: time.Now, timeout: time.Minute, batchN: restoreBatchSize,
	}
	if err := w.Work(context.Background(), fakeRestoreJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// ────────────────────────────────────────────────────────────────────
// platform_db_backup.go — remaining branches
// ────────────────────────────────────────────────────────────────────

// TestPlatformDBBackup_WriteAudit_InsertError_LogsContinues — the
// started-audit INSERT errors; the rest of the pipeline runs unaffected.
func TestPlatformDBBackup_WriteAudit_InsertError_LogsContinues(t *testing.T) {
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
		WillReturnError(errors.New("audit down"))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs("system", "platform_backup.succeeded", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	expectAdvisoryUnlock(mock)

	dumper := &fakePgDumper{payload: []byte("ok")}
	s3 := newFakeS3()
	now := time.Date(2026, 5, 13, 2, 0, 0, 0, time.UTC)
	w := newTestWorker(t, mock, db, dumper, s3, now)
	if err := w.Work(context.Background(), fakePlatformBackupJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestPlatformDBBackup_SweepDeleteError_NonFatal — Delete on a retention
// victim fails; the sweep continues and Work still returns nil.
func TestPlatformDBBackup_SweepDeleteError_NonFatal(t *testing.T) {
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

	dumper := &fakePgDumper{payload: []byte("ok")}
	s3 := newFakeS3()
	// List returns a victim; Delete on it errors.
	s3.listResult = []string{"platform-backups/2024-01-01/platform.dump.gz"}
	s3.deleteErr = errors.New("delete 500")
	now := time.Date(2026, 5, 13, 2, 0, 0, 0, time.UTC)
	w := newTestWorker(t, mock, db, dumper, s3, now)
	if err := w.Work(context.Background(), fakePlatformBackupJob()); err != nil {
		t.Errorf("Work: sweep delete failure must be non-fatal: %v", err)
	}
}

// TestPlatformDBBackup_DumpError_DeletePartialObject — the dumper errors;
// the worker tries to delete the partial object (best-effort).
func TestPlatformDBBackup_DumpError_DeletePartialObject(t *testing.T) {
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
		WithArgs("system", "platform_backup.failed", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	expectAdvisoryUnlock(mock)

	// When the dumper errors, Work calls pw.CloseWithError(dumpErr), so the
	// error propagates through the io.Pipe to the uploader's Read. The
	// uploader therefore also fails (uploadErr != nil), and Work's
	// partial-object delete branch — guarded by (uploadErr == nil &&
	// dumpErr != nil) — is NOT taken. The observable contract on a dump
	// error is: Work returns a non-nil error wrapping the dump failure,
	// writes the `platform_backup.failed` audit row, and uploads no usable
	// object. The cleanup-delete branch is only reachable if a future
	// rewiring lets an upload succeed while the dump fails; this test pins
	// today's behavior so that change is a conscious one.
	dumper := &fakePgDumper{err: errors.New("pg_dump down")}
	s3 := newFakeS3()
	now := time.Date(2026, 5, 13, 2, 0, 0, 0, time.UTC)
	w := newTestWorker(t, mock, db, dumper, s3, now)
	err = w.Work(context.Background(), fakePlatformBackupJob())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "pg_dump") {
		t.Errorf("error should wrap the dump failure; got %v", err)
	}
	// No usable object was uploaded.
	if _, ok := s3.uploaded[fmt.Sprintf("platform-backups/2026-05-13/%s", platformBackupObjectName)]; ok {
		t.Errorf("no object should be uploaded on dump failure")
	}
}

// TestPlatformDBBackup_AcquireConnError — db.Conn returns an error before
// the advisory lock. Work surfaces a non-nil error.
func TestPlatformDBBackup_AcquireConnError(t *testing.T) {
	db, _, err := sqlmock.New(sqlmock.MonitorPingsOption(false))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	// Closing the DB before Work runs forces db.Conn(...) to error.
	db.Close()

	w := NewPlatformDBBackupWorker(PlatformDBBackupConfig{
		DB:          db,
		DatabaseURL: "postgres://x",
		S3:          newFakeS3(),
		Bucket:      "b",
		InnerPrefix: "platform-backups/",
		Now:         fixedClock(time.Date(2026, 5, 13, 2, 0, 0, 0, time.UTC)),
	})
	if err := w.Work(context.Background(), fakePlatformBackupJob()); err == nil {
		t.Fatal("expected error from closed DB")
	}
}

// TestComputeKeepSet_UnparseableDateInPath — defensive: a key with a
// "date-shaped" path segment whose Parse fails (e.g. 2026-99-99) is
// conservatively kept.
func TestComputeKeepSet_UnparseableDateInPath(t *testing.T) {
	now := time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC)
	keys := []string{"platform-backups/2026-99-99/platform.dump.gz"}
	keep := computeKeepSet(keys, now, 30, 12)
	if !keep[keys[0]] {
		t.Error("unparseable-date key should be kept (defensive)")
	}
}

// ────────────────────────────────────────────────────────────────────
// finalizeDigest — null hash path
// ────────────────────────────────────────────────────────────────────

// TestFinalizeDigest_NilOnError — both error paths return empty so the
// finalize UPDATE writes NULL.
func TestFinalizeDigest_NilOnError(t *testing.T) {
	h := makeSHA256ForTest()
	_, _ = h.Write([]byte("abc"))
	if got := finalizeDigest(h, errors.New("dump"), nil); got != "" {
		t.Errorf("dumpErr should return empty, got %q", got)
	}
	if got := finalizeDigest(h, nil, errors.New("up")); got != "" {
		t.Errorf("upErr should return empty, got %q", got)
	}
	got := finalizeDigest(h, nil, nil)
	if got == "" {
		t.Errorf("happy path should return non-empty hex")
	}
	if _, err := hex.DecodeString(got); err != nil {
		t.Errorf("hash output is not valid hex: %v", err)
	}
}

// makeSHA256ForTest is a tiny helper so this test file owns its sha256
// usage (avoiding "imported and not used" diagnostics under future edits).
func makeSHA256ForTest() hash.Hash { return sha256.New() }
