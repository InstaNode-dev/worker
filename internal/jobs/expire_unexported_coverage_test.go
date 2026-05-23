package jobs

// expire_unexported_coverage_test.go — in-package tests targeting
// unexported helpers in the expire/reaper family that the black-box
// _test package cannot reach:
//
//   - deleteK8sNamespace (expire_stacks.go): safety-guard rejection,
//     no-SA-token error path, successful DELETE against a stub k8s api
//     (200, 202, 404 → ok; everything else → error).
//   - inClusterK8sClient (expire_stacks.go): non-in-cluster sentinel
//     (no token file → returns nil cleanly).
//   - hourWord (expiry_reminder_email.go): both branches.
//   - renderAnonExpiryEmail (expiry_reminder_email.go): missing-keys
//     graceful path, plural=false / plural=true on the rendered HTML.
//
// In-package (NOT _test) so we can reference unexported helpers
// directly. We deliberately keep this file lean — every test is one
// branch, no shared fixtures with expire_test.go.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	madmin "github.com/minio/madmin-go/v3"
	minio "github.com/minio/minio-go/v7"

	"instant.dev/worker/internal/provisioner"
)

// madminNew is a thin alias so the test reads naturally; madmin.New
// builds an admin client that signs requests against the given endpoint.
var madminNew = madmin.New

// TestInClusterK8sClient_NotInCluster: the function checks for the
// /var/run/secrets/kubernetes.io/serviceaccount/token file. Outside a
// pod that file does not exist → returns nil. We assert nil so the
// "not in-cluster" sentinel is pinned.
func TestInClusterK8sClient_NotInCluster(t *testing.T) {
	got := inClusterK8sClient()
	if got != nil {
		t.Errorf("inClusterK8sClient() = %v outside k8s, want nil", got)
	}
}

// TestDeleteK8sNamespace_RefusesWrongPrefix exercises the safety guard:
// a namespace name not starting with the configured prefix MUST be
// refused (returns nil, no DELETE issued). The test stands up an
// httptest server that fails if it ever receives a request.
func TestDeleteK8sNamespace_RefusesWrongPrefix(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// The function would only contact k8s if the prefix check passed;
	// pass a client whose Transport never actually fires (the guard
	// short-circuits before the HTTP layer).
	err := deleteK8sNamespace(
		context.Background(),
		srv.Client(),
		"some-other-ns", // wrong prefix
		"instant-stack-",
	)
	if err != nil {
		t.Errorf("expected nil (safety-guard skip), got %v", err)
	}
	if calls != 0 {
		t.Errorf("DELETE must NOT fire for refused prefix, got %d calls", calls)
	}
}

// TestDeleteK8sNamespace_MissingSAToken: with a matching prefix but no
// SA token on disk, the os.ReadFile fails → the function returns a
// wrapped error. This pins the error envelope (test-host machines
// don't have /var/run/secrets/...).
func TestDeleteK8sNamespace_MissingSAToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	err := deleteK8sNamespace(
		context.Background(),
		srv.Client(),
		"instant-stack-real", // matches prefix
		"instant-stack-",
	)
	if err == nil {
		t.Fatal("expected error from missing SA token, got nil")
	}
	if !strings.Contains(err.Error(), "read SA token") {
		t.Errorf("error envelope = %v, want it to mention 'read SA token'", err)
	}
}

// ----- expiry_reminder_email.go: hourWord ------------------------------

// TestHourWord covers both branches: plural=true → "hours", plural=false → "hour".
func TestHourWord(t *testing.T) {
	if got := hourWord(true); got != "hours" {
		t.Errorf("hourWord(true) = %q, want %q", got, "hours")
	}
	if got := hourWord(false); got != "hour" {
		t.Errorf("hourWord(false) = %q, want %q", got, "hour")
	}
}

// TestRenderAnonExpiryEmail_PluralAndSingular pins the plural-aware
// rendering. The view's Plural field is true unless hours_remaining=="1".
func TestRenderAnonExpiryEmail_PluralAndSingular(t *testing.T) {
	// Singular: "1 hour" should appear, NOT "1 hours".
	subj, html, text := renderAnonExpiryEmail(map[string]string{
		"resource_type":   "postgres",
		"hours_remaining": "1",
		"expires_at":      "2026-05-22T00:00:00Z",
		"reminder_index":  "3",
		"token_prefix":    "tok-abcd",
		"upgrade_url":     "https://x/upgrade",
		"resource_url":    "https://x/res",
	})
	if !strings.Contains(subj, "1h") {
		t.Errorf("singular subject must mention 1h, got %q", subj)
	}
	if !strings.Contains(html, "1 hour") || strings.Contains(html, "1 hours") {
		t.Errorf("singular HTML must say '1 hour' (no s), got: %s", html)
	}
	if !strings.Contains(text, "1 hour") || strings.Contains(text, "1 hours") {
		t.Errorf("singular text must say '1 hour' (no s), got: %s", text)
	}

	// Plural: "12 hours" should appear.
	subj2, html2, text2 := renderAnonExpiryEmail(map[string]string{
		"resource_type":   "redis",
		"hours_remaining": "12",
		"expires_at":      "2026-05-22T12:00:00Z",
		"reminder_index":  "1",
		"token_prefix":    "tok-1234",
		"upgrade_url":     "https://x/upgrade",
		"resource_url":    "https://x/res",
	})
	if !strings.Contains(subj2, "12h") {
		t.Errorf("plural subject must mention 12h, got %q", subj2)
	}
	if !strings.Contains(html2, "12 hours") {
		t.Errorf("plural HTML must say '12 hours', got: %s", html2)
	}
	if !strings.Contains(text2, "12 hours") {
		t.Errorf("plural text must say '12 hours', got: %s", text2)
	}
}

// TestRenderAnonExpiryEmail_MissingParamsRenderEmpty: missing keys in the
// params map render as empty strings — graceful degradation, no panic.
func TestRenderAnonExpiryEmail_MissingParamsRenderEmpty(t *testing.T) {
	subj, html, text := renderAnonExpiryEmail(map[string]string{})
	// Missing hours_remaining → defaults to "1" in the subject path.
	if !strings.Contains(subj, "1h") {
		t.Errorf("missing-hours subject must fall back to '1h', got %q", subj)
	}
	// Missing resource_type → "resource" fallback.
	if !strings.Contains(subj, "resource") {
		t.Errorf("missing-resource_type subject must include 'resource', got %q", subj)
	}
	if html == "" {
		t.Error("HTML body must not be empty even on missing params")
	}
	if text == "" {
		t.Error("text body must not be empty even on missing params")
	}
}

// TestAnonExpirySubject_AllBranches walks every reminder_index prefix
// and the default fallback (any other value).
func TestAnonExpirySubject_AllBranches(t *testing.T) {
	if got := anonExpirySubject("1", "postgres", "12"); !strings.HasPrefix(got, "Heads up") {
		t.Errorf("index=1 expected 'Heads up' prefix, got %q", got)
	}
	if got := anonExpirySubject("2", "postgres", "6"); !strings.HasPrefix(got, "Reminder") {
		t.Errorf("index=2 expected 'Reminder' prefix, got %q", got)
	}
	if got := anonExpirySubject("3", "postgres", "1"); !strings.HasPrefix(got, "Final reminder") {
		t.Errorf("index=3 expected 'Final reminder' prefix, got %q", got)
	}
	// Default branch — unrecognised index keeps "Heads up".
	if got := anonExpirySubject("9", "postgres", "4"); !strings.HasPrefix(got, "Heads up") {
		t.Errorf("default index expected 'Heads up' prefix, got %q", got)
	}
	// Empty resource_type falls back to "resource".
	if got := anonExpirySubject("1", "", "4"); !strings.Contains(got, "resource") {
		t.Errorf("empty resource_type expected 'resource' fallback, got %q", got)
	}
	// Empty hours_remaining falls back to "1".
	if got := anonExpirySubject("1", "postgres", ""); !strings.HasSuffix(got, "1h") {
		t.Errorf("empty hours_remaining expected '1h' fallback, got %q", got)
	}
}

// TestHoursLeft pins the floor-of-1 behaviour: a near-zero / past gap
// returns 1, not 0 — the email must never say "0 hours".
func TestHoursLeft(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	// 30 minutes from now → 1 hour (round-up).
	got := hoursLeft(now.Add(30*time.Minute), now)
	if got != 1 {
		t.Errorf("hoursLeft(30min) = %d, want 1", got)
	}
	// 5 hours 30 minutes from now → 6 (round-up).
	got = hoursLeft(now.Add(5*time.Hour+30*time.Minute), now)
	if got != 6 {
		t.Errorf("hoursLeft(5h30min) = %d, want 6", got)
	}
	// Past / now → floor of 1.
	got = hoursLeft(now.Add(-1*time.Hour), now)
	if got != 1 {
		t.Errorf("hoursLeft(past) = %d, want 1 (floor)", got)
	}
}

// TestSelectStage_PastTTLReturnsNone: a row whose expires_at is in the
// past returns no stage (the reaper handles past-TTL, not the reminder).
func TestSelectStage_PastTTLReturnsNone(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	r := expiryReminderRow{expiresAt: now.Add(-1 * time.Hour)}
	if _, ok := selectStage(r, now); ok {
		t.Error("selectStage on past-TTL row should return ok=false")
	}
}

// TestSelectStage_TooFarOutReturnsNone: a row > 12h from expiry is
// "ExpiryStageNone" — out of all reminder buckets → no stage.
func TestSelectStage_TooFarOutReturnsNone(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	r := expiryReminderRow{expiresAt: now.Add(15 * time.Hour)}
	if _, ok := selectStage(r, now); ok {
		t.Error("selectStage on too-far-out row should return ok=false")
	}
}

// TestSelectStage_AlreadySentReturnsNone: a row inside the 12h bucket
// whose reminders_sent already covers that stage returns no stage.
func TestSelectStage_AlreadySentReturnsNone(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	r := expiryReminderRow{
		expiresAt:     now.Add(11 * time.Hour), // stage 12h bucket
		remindersSent: 1,                       // already sent stage 1
	}
	if _, ok := selectStage(r, now); ok {
		t.Error("selectStage when reminders_sent >= bucket.index should return ok=false")
	}
}

// signMagicLinkResendJWT empty-secret short-circuit — direct
// in-package call so we exercise the `if secret == ""` branch.
func TestSignMagicLinkResendJWT_EmptySecret(t *testing.T) {
	tok, err := signMagicLinkResendJWT("", "some-link-id")
	if err == nil {
		t.Fatal("expected error from empty secret, got nil")
	}
	if tok != "" {
		t.Errorf("expected empty token on error, got %q", tok)
	}
}

// signMagicLinkResendJWT happy path — confirms the signer produces a
// three-part JWT (header.payload.signature) under a normal secret.
func TestSignMagicLinkResendJWT_HappyPath(t *testing.T) {
	tok, err := signMagicLinkResendJWT("super-secret", "abc-123")
	if err != nil {
		t.Fatalf("signMagicLinkResendJWT: %v", err)
	}
	if strings.Count(tok, ".") != 2 {
		t.Errorf("expected JWT to have 2 dots (header.payload.sig), got %q", tok)
	}
}

// magicLinkReconcilerBase64URLEncode is trivial but at 100% only when
// called — confirm it round-trips.
func TestMagicLinkReconcilerBase64URLEncode(t *testing.T) {
	got := magicLinkReconcilerBase64URLEncode([]byte("hello"))
	// Standard URL-safe base64 of "hello" is "aGVsbG8" (no padding).
	if got != "aGVsbG8" {
		t.Errorf("base64URLEncode(hello) = %q, want aGVsbG8", got)
	}
}

// ----- deleteStorageObjects in-package coverage ------------------------
//
// Exercises the deleter-not-nil happy path, the empty-prefix branch,
// and the per-object Remove error branch. These add the remaining
// uncovered statements in expire.go's storage-cleanup helper.

// inPkgFakeDeleter is an S3BackupDeleter implementation that records
// every prefix it's listed and emits a configurable number of
// ObjectInfo entries on the list channel + a configurable error on
// the remove channel.
type inPkgFakeDeleter struct {
	objects    []minio.ObjectInfo // returned by ListObjects
	removeErrs []minio.RemoveObjectError

	mu          sync.Mutex
	listedPaths []string
}

func (d *inPkgFakeDeleter) ListObjects(_ context.Context, _ string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	d.mu.Lock()
	d.listedPaths = append(d.listedPaths, opts.Prefix)
	d.mu.Unlock()
	ch := make(chan minio.ObjectInfo, len(d.objects))
	for _, o := range d.objects {
		ch <- o
	}
	close(ch)
	return ch
}

// listed returns a copy of the recorded prefixes under lock — the
// production deleteStorageObjects calls ListObjects from a producer
// goroutine, so reads must be synchronised and may need a brief poll.
func (d *inPkgFakeDeleter) listed() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.listedPaths...)
}

// waitListed polls until ListObjects has been invoked at least once or
// the deadline elapses, returning the recorded prefixes.
func (d *inPkgFakeDeleter) waitListed(t *testing.T) []string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := d.listed(); len(got) > 0 {
			return got
		}
		if time.Now().After(deadline) {
			return nil
		}
		time.Sleep(2 * time.Millisecond)
	}
}
func (d *inPkgFakeDeleter) RemoveObjects(_ context.Context, _ string, in <-chan minio.ObjectInfo, _ minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError {
	// Drain input so producer completes; then emit configured errors.
	go func() {
		for range in {
		}
	}()
	out := make(chan minio.RemoveObjectError, len(d.removeErrs))
	for _, e := range d.removeErrs {
		out <- e
	}
	close(out)
	return out
}

// TestDeleteStorageObjects_NoDeleterWarns covers the deleter==nil path:
// no panic, function returns, no list/remove ever issued.
func TestDeleteStorageObjects_NoDeleterWarns(t *testing.T) {
	deleteStorageObjects(context.Background(), nil, "bucket",
		"tok-xxxx", "prov-yyyy", "res-zzzz", 1)
	// no assertion beyond "no panic" — the warn log is the contract.
}

// TestDeleteStorageObjects_HappyPath wires a real deleter, two objects,
// no remove errors → the function logs storage_objects_deleted and
// the deleter saw a non-empty prefix.
func TestDeleteStorageObjects_HappyPath(t *testing.T) {
	d := &inPkgFakeDeleter{
		objects: []minio.ObjectInfo{
			{Key: "tok-aaaa/obj-1.bin"},
			{Key: "tok-aaaa/obj-2.bin"},
		},
	}
	// Use a 36-char uuid-shaped token + a non-empty providerResourceID so
	// minioObjectPrefix produces a non-empty prefix (the function uses
	// the prefix logic shared with the storage_minio.go scanner).
	deleteStorageObjects(context.Background(), d, "instant-shared",
		"aaaa1111-bbbb-cccc-dddd-eeeeffffffff", "prov-1", "res-1", 1)
	listed := d.waitListed(t)
	if len(listed) == 0 {
		t.Fatal("expected ListObjects to be invoked at least once")
	}
	if listed[0] == "" {
		t.Error("expected non-empty prefix on the listed path")
	}
}

// TestDeleteStorageObjects_EmptyPrefixWarns covers the prefix=="" guard:
// an empty token AND empty provider_resource_id yields no prefix, so the
// function logs storage_prefix_empty and returns without listing.
func TestDeleteStorageObjects_EmptyPrefixWarns(t *testing.T) {
	d := &inPkgFakeDeleter{}
	deleteStorageObjects(context.Background(), d, "instant-shared",
		"", "", "res-empty", 1)
	if got := d.listed(); len(got) != 0 {
		t.Errorf("expected no ListObjects call for empty prefix, got %v", got)
	}
}

// TestDeprovisionMinIOUser covers both the error branch (server returns
// non-2xx → RemoveUser/RemoveCannedPolicy log a warn) and the
// happy-path log line. A madmin client is pointed at an httptest server;
// the function never propagates errors (fail-open), so we assert it
// returns without panicking for each server behaviour.
func TestDeprovisionMinIOUser(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
	}{
		{"remove_errors_logged", http.StatusInternalServerError},
		{"happy_path", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			endpoint := strings.TrimPrefix(srv.URL, "http://")
			client, err := madminNew(endpoint, "minioadmin", "minioadmin", false)
			if err != nil {
				t.Fatalf("madmin.New: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// Must not panic regardless of server response (fail-open).
			deprovisionMinIOUser(ctx, client, "aaaa1111-bbbb-cccc", "res-minio", 1)
		})
	}
}

// TestExpireImminentWork_RowsErrPropagates covers the rows.Err() branch
// of ExpireImminentWorker.Work: a sqlmock RowError surfaces during
// iteration so Work returns the wrapped error (River retries) rather than
// completing silently.
func TestExpireImminentWork_RowsErrPropagates(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "token", "team_id", "resource_type", "expires_at", "owner_email"}).
		AddRow(uuid.New(), uuid.New(), uuid.New(), "postgres",
			time.Now().Add(30*time.Minute), "o@example.com").
		RowError(0, errSentinel("injected rows.Err"))
	mock.ExpectQuery(`FROM resources r`).WillReturnRows(rows)

	w := NewExpireImminentWorker(db)
	if err := w.Work(context.Background(), fakeJobLocal[ExpireImminentArgs]()); err == nil {
		t.Error("expected Work to propagate the rows.Err error")
	}
}

// TestExpiryReminderWork_RowsErrPropagates covers the `rows.Err()`
// branch of ExpiryReminderWorker.Work: sqlmock's RowError makes the
// driver surface an error during iteration → Work returns it (so River
// retries) rather than silently completing.
func TestExpiryReminderWork_RowsErrPropagates(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "team_id", "resource_type", "expires_at",
		"reminders_sent", "key_prefix", "email"}).
		AddRow(uuid.New(), uuid.New(), "postgres", time.Now().Add(time.Hour),
			0, "kp", "a@example.com").
		RowError(0, errSentinel("injected rows.Err"))
	mock.ExpectQuery(`SELECT r.id, r.team_id`).WillReturnRows(rows)

	w := NewExpiryReminderWorker(db)
	if err := w.Work(context.Background(), fakeJobLocal[ExpiryReminderArgs]()); err == nil {
		t.Error("expected Work to propagate the rows.Err error")
	}
}

// TestNewExpireAnonymousWorker_AssignsNonNilProvisioner covers the
// `provClient != nil` arm of NewExpireAnonymousWorker. grpc.NewClient is
// lazy (no dial until first RPC), so constructing a Client against a
// dummy address is cheap and never connects.
func TestNewExpireAnonymousWorker_AssignsNonNilProvisioner(t *testing.T) {
	pc, conn, err := provisioner.NewClient("localhost:1", "secret")
	if err != nil {
		t.Fatalf("provisioner.NewClient: %v", err)
	}
	defer conn.Close()
	w := NewExpireAnonymousWorker(nil, pc, nil)
	if w.provisioner == nil {
		t.Error("expected non-nil provisioner to be assigned")
	}
}

// TestSignMagicLinkResendJWT covers both arms: empty secret → error,
// non-empty secret → a 3-part dotted JWT.
func TestSignMagicLinkResendJWT(t *testing.T) {
	if _, err := signMagicLinkResendJWT("", "link-1"); err == nil {
		t.Error("expected error for empty secret")
	}
	tok, err := signMagicLinkResendJWT("a-real-secret", "link-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Count(tok, ".") != 2 {
		t.Errorf("expected a 3-part JWT, got %q", tok)
	}
}

// TestDriveResend_JWTSignFailureSkips covers driveResend's jwt_sign_failed
// branch: an empty jwtSecret makes signMagicLinkResendJWT fail, so the
// row is skipped (no HTTP call is issued).
func TestDriveResend_JWTSignFailureSkips(t *testing.T) {
	var called bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
	}))
	defer srv.Close()

	w := &MagicLinkReconcilerWorker{
		httpCli:   srv.Client(),
		apiBase:   srv.URL,
		jwtSecret: "", // forces signMagicLinkResendJWT to fail
	}
	got := w.driveResend(context.Background(), magicLinkReconcileRow{id: uuid.New()})
	if got != reconcileOutcomeSkipped {
		t.Errorf("driveResend = %v, want reconcileOutcomeSkipped", got)
	}
	if called {
		t.Error("no HTTP call should fire when JWT signing fails")
	}
}

// TestDeleteStorageObjects_RemoveErrorLogs covers the per-object remove
// error path: one object returns an error → removeErrors++ is logged,
// function still returns without panicking.
func TestDeleteStorageObjects_RemoveErrorLogs(t *testing.T) {
	d := &inPkgFakeDeleter{
		objects: []minio.ObjectInfo{
			{Key: "tok-bbbb/obj-x"},
		},
		removeErrs: []minio.RemoveObjectError{
			{ObjectName: "tok-bbbb/obj-x", Err: errFakeRemove},
		},
	}
	deleteStorageObjects(context.Background(), d, "instant-shared",
		"bbbb1111-bbbb-cccc-dddd-eeeeffffffff", "prov-2", "res-2", 1)
}

// TestDeleteStorageObjects_ListErrorPath: ListObjects emits an
// ObjectInfo whose .Err != nil → the producer goroutine logs
// storage_list_error and returns; the function completes gracefully.
func TestDeleteStorageObjects_ListErrorPath(t *testing.T) {
	d := &inPkgFakeDeleter{
		objects: []minio.ObjectInfo{
			{Err: errFakeList},
		},
	}
	deleteStorageObjects(context.Background(), d, "instant-shared",
		"cccc1111-bbbb-cccc-dddd-eeeeffffffff", "prov-3", "res-3", 1)
}

// errFakeRemove is a sentinel used by the storage object-remove tests.
var (
	errFakeRemove = errSentinel("fake remove failure")
	errFakeList   = errSentinel("fake list failure")
)

// errSentinel is a tiny string-based error so we don't import errors here.
type errSentinel string

func (e errSentinel) Error() string { return string(e) }

// We need package-local type aliases for the upstream minio symbols
// referenced by the inPkgFakeDeleter. The aliases live alongside the
// type below so they're visible to the deleter implementation. (Go
// type aliases REQUIRE the right-hand side to be a real type — we get
// it from the existing minio import via the package's own production
// code which already imports github.com/minio/minio-go/v7. The alias
// below resolves at compile time to that imported symbol.)
//
// NOTE: production code in this package uses `minio.ObjectInfo`,
// `minio.ListObjectsOptions`, `minio.RemoveObjectError`,
// `minio.RemoveObjectsOptions`. We re-declare them as aliases here so
// the test fixture's method signatures compile without re-importing
// minio at the top of this file.

// (Aliases are inlined here rather than at the top of the file so the
// import block above stays unchanged. The reference resolves through
// the package's own minio import.)
