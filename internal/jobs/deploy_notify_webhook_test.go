package jobs

// deploy_notify_webhook_test.go — hermetic tests for
// DeployNotifyWebhookWorker. The unit-of-test is "given an audit_log
// row + a vault URL, do we POST the right payload, advance the cursor,
// and write delivery_failed rows on failure?".
//
// Tests live in package `jobs` (not `jobs_test`) so they can reach the
// test-only newDeployNotifyWebhookWorkerForTest constructor and the
// in-memory cursor store. Mirrors event_email_forwarder_test.go.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

// memDeployNotifyCursor is an in-memory deployNotifyCursorStore for tests.
type memDeployNotifyCursor struct {
	mu      sync.Mutex
	current deployNotifyCursor
}

func (m *memDeployNotifyCursor) read(ctx context.Context) (deployNotifyCursor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current, nil
}

func (m *memDeployNotifyCursor) write(ctx context.Context, c deployNotifyCursor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.current = c
	return nil
}

func fakeDeployNotifyJob() *river.Job[DeployNotifyWebhookArgs] {
	return &river.Job[DeployNotifyWebhookArgs]{JobRow: &rivertype.JobRow{ID: 1}}
}

// TestDeployNotifyWebhook_HappyPath_PostsAndAdvancesCursor covers the
// success path: one audit row, one vault URL, one 200 OK from the
// receiver, cursor advances past the row, no delivery_failed audit.
func TestDeployNotifyWebhook_HappyPath_PostsAndAdvancesCursor(t *testing.T) {
	// Spin a real httptest receiver so we exercise the full HTTP path.
	var (
		gotIdempotencyKey string
		gotPayload        map[string]any
		hits              int32
	)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		gotIdempotencyKey = r.Header.Get("Idempotency-Key")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotPayload)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Resolve srv.URL's hostname (typically 127.0.0.1) — the SSRF gate
	// would reject 127.x. Use a stub resolver + scheme rewrite so we
	// can exercise the path with a real server. We point the URL at
	// a public-looking literal that the stub resolver maps to the
	// loopback test server, and override the SSRF block list for this
	// test. Simpler: bypass validateDeployNotifyURL by feeding a URL
	// that DOES validate (we accept https + non-private), then rewrite
	// the HTTP client's Transport to dial srv instead. That keeps the
	// SSRF gate exercised in failure-path tests below.
	publicURL := "https://notify.example.test/hook"
	prevResolver := deployNotifyResolver
	defer func() { deployNotifyResolver = prevResolver }()
	deployNotifyResolver = func(host string) ([]net.IP, error) {
		// Pretend the public-looking host resolves to a clean public IP;
		// the actual dial is hijacked by the transport below.
		return []net.IP{net.ParseIP("203.0.113.10")}, nil
	}

	cli := &http.Client{Transport: redirectToServerTransport(srv)}

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := "11111111-1111-1111-1111-111111111111"
	auditID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	createdAt := time.Now().UTC()
	mock.ExpectQuery(`FROM audit_log a`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "team_id", "kind", "metadata", "created_at"}).
			AddRow(auditID, teamID, "deploy.healthy", []byte(`{"deploy_id":"dep_42"}`), createdAt))

	// vault lookup returns the URL (plaintext bytes — default decryptor
	// is the identity function).
	mock.ExpectQuery(`FROM vault_secrets`).
		WithArgs(teamID, deployNotifyVaultEnv, deployNotifyVaultKey).
		WillReturnRows(sqlmock.NewRows([]string{"encrypted_value"}).AddRow([]byte(publicURL)))

	cursor := &memDeployNotifyCursor{}
	w := newDeployNotifyWebhookWorkerForTest(db, cursor, cli)

	if err := w.Work(context.Background(), fakeDeployNotifyJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}

	if hits != 1 {
		t.Fatalf("expected 1 receiver hit, got %d", hits)
	}
	if gotIdempotencyKey != auditID {
		t.Errorf("Idempotency-Key = %q, want %q", gotIdempotencyKey, auditID)
	}
	if gotPayload["kind"] != "deploy.healthy" {
		t.Errorf("payload.kind = %v, want deploy.healthy", gotPayload["kind"])
	}
	if gotPayload["deploy_id"] != "dep_42" {
		t.Errorf("payload.deploy_id = %v, want dep_42", gotPayload["deploy_id"])
	}
	if gotPayload["team_id"] != teamID {
		t.Errorf("payload.team_id = %v, want %s", gotPayload["team_id"], teamID)
	}
	if cursor.current.ID != auditID {
		t.Errorf("cursor.ID = %q, want %q", cursor.current.ID, auditID)
	}
}

// TestDeployNotifyWebhook_NoVaultEntry_AdvancesCursor covers the
// no-webhook-configured path: the audit row exists but the vault lookup
// returns sql.ErrNoRows. We should NOT POST anywhere, NOT write a
// delivery_failed row, and the cursor should still advance.
func TestDeployNotifyWebhook_NoVaultEntry_AdvancesCursor(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := "22222222-2222-2222-2222-222222222222"
	auditID := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	createdAt := time.Now().UTC()
	mock.ExpectQuery(`FROM audit_log a`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "team_id", "kind", "metadata", "created_at"}).
			AddRow(auditID, teamID, "deploy.created", []byte(`{}`), createdAt))

	mock.ExpectQuery(`FROM vault_secrets`).
		WithArgs(teamID, deployNotifyVaultEnv, deployNotifyVaultKey).
		WillReturnRows(sqlmock.NewRows([]string{"encrypted_value"})) // empty rowset → ErrNoRows path

	cursor := &memDeployNotifyCursor{}
	w := newDeployNotifyWebhookWorkerForTest(db, cursor, &http.Client{})
	if err := w.Work(context.Background(), fakeDeployNotifyJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
	if cursor.current.ID != auditID {
		t.Errorf("cursor.ID = %q, want %q (advance even when no webhook)", cursor.current.ID, auditID)
	}
}

// TestDeployNotifyWebhook_AllAttemptsFail_EmitsDeliveryFailed covers the
// failure path: every attempt returns 500. We expect the worker to retry
// up to deployNotifyMaxAttempts, then write a deploy_notify.delivery_failed
// audit row and advance the cursor.
func TestDeployNotifyWebhook_AllAttemptsFail_EmitsDeliveryFailed(t *testing.T) {
	var hits int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	prevResolver := deployNotifyResolver
	defer func() { deployNotifyResolver = prevResolver }()
	deployNotifyResolver = func(host string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("203.0.113.11")}, nil
	}

	cli := &http.Client{Transport: redirectToServerTransport(srv)}
	publicURL := "https://broken.example.test/hook"

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := "33333333-3333-3333-3333-333333333333"
	auditID := "cccccccc-cccc-cccc-cccc-cccccccccccc"
	createdAt := time.Now().UTC()
	mock.ExpectQuery(`FROM audit_log a`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "team_id", "kind", "metadata", "created_at"}).
			AddRow(auditID, teamID, "deploy.failed", []byte(`{"deploy_id":"dep_99"}`), createdAt))
	mock.ExpectQuery(`FROM vault_secrets`).
		WithArgs(teamID, deployNotifyVaultEnv, deployNotifyVaultKey).
		WillReturnRows(sqlmock.NewRows([]string{"encrypted_value"}).AddRow([]byte(publicURL)))

	// Expect the delivery_failed audit row to be inserted with the
	// failing kind in metadata.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, deployNotifyActor, deployNotifyDeliveryFailedKind, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	cursor := &memDeployNotifyCursor{}
	w := newDeployNotifyWebhookWorkerForTest(db, cursor, cli)
	if err := w.Work(context.Background(), fakeDeployNotifyJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
	if hits < int32(deployNotifyMaxAttempts) {
		t.Errorf("expected at least %d receiver hits (max attempts), got %d", deployNotifyMaxAttempts, hits)
	}
	if cursor.current.ID != auditID {
		t.Errorf("cursor.ID = %q, want %q (advance after delivery failure)", cursor.current.ID, auditID)
	}
}

// TestDeployNotifyWebhook_SSRFRejected_DoesNotPOST covers the SSRF
// guard: a URL whose hostname resolves to a private IP must be rejected
// before any HTTP traffic leaves the worker. The cursor still advances
// (the URL won't get better on retry) and no delivery_failed row is
// emitted (this is operator config, not a delivery failure).
func TestDeployNotifyWebhook_SSRFRejected_DoesNotPOST(t *testing.T) {
	var hits int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	prevResolver := deployNotifyResolver
	defer func() { deployNotifyResolver = prevResolver }()
	// Resolve to a private (RFC1918) IP — SSRF gate should reject.
	deployNotifyResolver = func(host string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("10.0.0.5")}, nil
	}

	cli := &http.Client{Transport: redirectToServerTransport(srv)}

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := "44444444-4444-4444-4444-444444444444"
	auditID := "dddddddd-dddd-dddd-dddd-dddddddddddd"
	createdAt := time.Now().UTC()
	mock.ExpectQuery(`FROM audit_log a`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "team_id", "kind", "metadata", "created_at"}).
			AddRow(auditID, teamID, "deploy.created", []byte(`{}`), createdAt))
	mock.ExpectQuery(`FROM vault_secrets`).
		WithArgs(teamID, deployNotifyVaultEnv, deployNotifyVaultKey).
		WillReturnRows(sqlmock.NewRows([]string{"encrypted_value"}).AddRow([]byte("https://internal-host.example.test/hook")))

	cursor := &memDeployNotifyCursor{}
	w := newDeployNotifyWebhookWorkerForTest(db, cursor, cli)
	if err := w.Work(context.Background(), fakeDeployNotifyJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
	if hits != 0 {
		t.Errorf("expected 0 receiver hits (SSRF gate blocks), got %d", hits)
	}
	if cursor.current.ID != auditID {
		t.Errorf("cursor.ID = %q, want %q (advance past rejected URL)", cursor.current.ID, auditID)
	}
}

// TestDeployNotifyWebhook_TopLevelQueryError_ReturnsError verifies that
// a fatal SELECT failure propagates so River retries. Per-row failures
// are fail-open (logged) per the contract; the top-level query is the
// one River-visible error path.
func TestDeployNotifyWebhook_TopLevelQueryError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`FROM audit_log a`).WillReturnError(errors.New("simulated DB failure"))

	cursor := &memDeployNotifyCursor{}
	w := newDeployNotifyWebhookWorkerForTest(db, cursor, &http.Client{})
	if err := w.Work(context.Background(), fakeDeployNotifyJob()); err == nil {
		t.Fatal("expected error from top-level SELECT failure, got nil")
	}
}

// TestValidateDeployNotifyURL_BlocksPrivateAndScheme covers the SSRF +
// scheme gate as a focused unit test. Pure-function — no DB, no HTTP.
func TestValidateDeployNotifyURL_BlocksPrivateAndScheme(t *testing.T) {
	prev := deployNotifyResolver
	defer func() { deployNotifyResolver = prev }()
	deployNotifyResolver = func(host string) ([]net.IP, error) {
		switch host {
		case "private.example.test":
			return []net.IP{net.ParseIP("10.1.2.3")}, nil
		case "public.example.test":
			return []net.IP{net.ParseIP("8.8.8.8")}, nil
		case "metadata.example.test":
			return []net.IP{net.ParseIP("169.254.169.254")}, nil
		case "mixed.example.test":
			// One public, one private — must reject the whole URL.
			return []net.IP{net.ParseIP("8.8.8.8"), net.ParseIP("192.168.1.1")}, nil
		}
		return nil, errors.New("nxdomain")
	}

	cases := []struct {
		name string
		url  string
		// wantError: validation must fail.
		wantError bool
		// wantTransient: the failure must be classified transient
		// (errDeployNotifyTransient) so the dispatch loop holds the
		// cursor. Only meaningful when wantError is true.
		wantTransient bool
		// wantIPs: expected count of vetted IPs returned on success.
		wantIPs int
	}{
		{name: "http rejected", url: "http://public.example.test/hook", wantError: true},
		{name: "localhost literal rejected", url: "https://localhost/hook", wantError: true},
		{name: "loopback ip literal rejected", url: "https://127.0.0.1/hook", wantError: true},
		{name: "rfc1918 literal rejected", url: "https://10.0.0.1/hook", wantError: true},
		{name: "link-local rejected", url: "https://169.254.169.254/hook", wantError: true},
		{name: "private dns rejected", url: "https://private.example.test/hook", wantError: true},
		{name: "metadata dns rejected", url: "https://metadata.example.test/hook", wantError: true},
		{name: "mixed dns rejected", url: "https://mixed.example.test/hook", wantError: true},
		// W3 T3: an unresolvable host is a TRANSIENT failure — must be
		// tagged errDeployNotifyTransient so the cursor is held, not advanced.
		{name: "nxdomain is transient", url: "https://nope.example.test/hook", wantError: true, wantTransient: true},
		{name: "public ok", url: "https://public.example.test/hook", wantIPs: 1},
		{name: "public ip literal ok", url: "https://8.8.8.8/hook", wantIPs: 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ips, err := validateDeployNotifyURL(c.url)
			if (err != nil) != c.wantError {
				t.Fatalf("validateDeployNotifyURL(%q): err=%v, wantError=%v", c.url, err, c.wantError)
			}
			if c.wantError {
				gotTransient := errors.Is(err, errDeployNotifyTransient)
				if gotTransient != c.wantTransient {
					t.Errorf("validateDeployNotifyURL(%q): transient=%v, want %v", c.url, gotTransient, c.wantTransient)
				}
				return
			}
			if len(ips) != c.wantIPs {
				t.Errorf("validateDeployNotifyURL(%q): returned %d vetted IPs, want %d", c.url, len(ips), c.wantIPs)
			}
		})
	}
}

// redirectToServerTransport returns an http.RoundTripper that rewrites
// every request's host:scheme to point at srv (an httptest server) while
// preserving the request path. We use this so the SSRF gate sees a
// public-looking URL but the actual TCP dial lands at the loopback test
// server. Without it we'd have to disable the SSRF gate in every happy-
// path test, which would let a real bug in the gate slip past us.
func redirectToServerTransport(srv *httptest.Server) http.RoundTripper {
	target, _ := url.Parse(srv.URL)
	srvTransport := srv.Client().Transport
	return roundTripFn(func(req *http.Request) (*http.Response, error) {
		req2 := req.Clone(req.Context())
		req2.URL.Scheme = target.Scheme
		req2.URL.Host = target.Host
		req2.Host = target.Host
		return srvTransport.RoundTrip(req2)
	})
}

type roundTripFn func(*http.Request) (*http.Response, error)

func (f roundTripFn) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestLookupWebhookURL_PinsProductionEnv is the BugBash 2026-05-18 W3 T3
// regression guard: lookupWebhookURL MUST bind vault_secrets.env to
// 'production'. Before the fix the query had no env predicate, so a team
// that set DEPLOY_NOTIFY_WEBHOOK_URL under more than one env could have a
// cross-env URL surface via the version-DESC ordering.
//
// The mock's WithArgs asserts the exact (team_id, env, key) tuple — if a
// future edit drops the env binding the arg count mismatches and this test
// fails. We additionally assert deployNotifyVaultEnv is "production" so the
// constant itself can't silently drift.
func TestLookupWebhookURL_PinsProductionEnv(t *testing.T) {
	if deployNotifyVaultEnv != "production" {
		t.Fatalf("deployNotifyVaultEnv = %q, want \"production\" — the dispatcher must read the production vault bucket", deployNotifyVaultEnv)
	}

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := "22222222-2222-2222-2222-222222222222"
	wantURL := "https://notify.example.test/prod-hook"

	// The mock only matches when all three args — team_id, env, key — are
	// bound in that order. A query missing the env predicate would pass
	// only 2 args and fail ExpectationsWereMet.
	mock.ExpectQuery(`FROM vault_secrets`).
		WithArgs(teamID, deployNotifyVaultEnv, deployNotifyVaultKey).
		WillReturnRows(sqlmock.NewRows([]string{"encrypted_value"}).AddRow([]byte(wantURL)))

	w := newDeployNotifyWebhookWorkerForTest(db, &memDeployNotifyCursor{}, nil)
	got, err := w.lookupWebhookURL(context.Background(), teamID)
	if err != nil {
		t.Fatalf("lookupWebhookURL: %v", err)
	}
	if got != wantURL {
		t.Errorf("lookupWebhookURL = %q, want %q", got, wantURL)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations (env predicate likely missing): %v", err)
	}
}
