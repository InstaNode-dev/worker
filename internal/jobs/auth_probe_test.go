package jobs_test

// auth_probe_test.go — hermetic tests for AuthProbeWorker (AUTH-004).
//
// Each test stands up an httptest.Server that simulates one failure mode
// (or the happy path) of the api's /auth/email/start + /auth/exchange +
// /auth/me surface, then asserts:
//   - the per-leg outcome metric is bumped with the right (leg, result)
//     label combination,
//   - an audit_log row is inserted on result=fail (and NOT on pass/degraded).
//
// The metric path is exercised through a fakeAuthProbeMetrics capture
// rather than scraping the real /metrics registry — the registry is
// process-global and cross-test pollution would make assertions flaky.

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"instant.dev/worker/internal/jobs"
)

// fakeAuthProbeMetrics captures every IncOutcome / ObserveLatency call.
// Thread-safe because the worker calls them sequentially per tick but a
// test may make multiple ticks in parallel.
type fakeAuthProbeMetrics struct {
	mu        sync.Mutex
	outcomes  []fakeOutcome
	latencies []fakeLatency
}

type fakeOutcome struct{ leg, result string }
type fakeLatency struct {
	leg string
	d   time.Duration
}

func (f *fakeAuthProbeMetrics) IncOutcome(leg, result string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.outcomes = append(f.outcomes, fakeOutcome{leg, result})
}

func (f *fakeAuthProbeMetrics) ObserveLatency(leg string, d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latencies = append(f.latencies, fakeLatency{leg, d})
}

func (f *fakeAuthProbeMetrics) outcomeFor(leg string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, o := range f.outcomes {
		if o.leg == leg {
			return o.result
		}
	}
	return ""
}

// authProbeBaseConfig returns a config wired against the test server.
// Sets BearerToken so leg 3 actually fires (rather than skipping with
// result=degraded).
func authProbeBaseConfig(baseURL string) jobs.AuthProbeConfig {
	return jobs.AuthProbeConfig{
		BaseURL:     baseURL,
		Email:       "probe-auth-prod@instanode.dev",
		ReturnTo:    "https://instanode.dev/login/callback",
		Origin:      "https://instanode.dev",
		BearerToken: "test-bearer-token",
	}
}

// happyHandler is a minimal api stand-in: every leg returns the
// success-shaped response with all required CORS headers.
func happyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/auth/email/start" && r.Method == http.MethodPost:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"ok":true}`))
		case r.URL.Path == "/auth/exchange":
			w.Header().Set("Access-Control-Allow-Origin", "https://instanode.dev")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			// POST without cookie — api responds 400 cookie_missing_or_expired,
			// CORS headers still attached.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"cookie_missing_or_expired"}`))
		case r.URL.Path == "/auth/me" && r.Method == http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"email":"probe-auth-prod@instanode.dev","user_id":"u-probe"}`))
		default:
			http.NotFound(w, r)
		}
	})
}

// TestAuthProbe_HappyPath_AllLegsPass — every leg returns the expected
// success shape; expect result=pass for all 3 legs, no audit_log rows,
// 3 latency observations.
func TestAuthProbe_HappyPath_AllLegsPass(t *testing.T) {
	srv := httptest.NewServer(happyHandler())
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	fm := &fakeAuthProbeMetrics{}
	w := jobs.NewAuthProbeWorker(db, srv.Client(), fm, authProbeBaseConfig(srv.URL))
	if err := w.Work(context.Background(), fakeJob[jobs.AuthProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	for _, leg := range []string{"email_start", "exchange_headers", "me"} {
		if got := fm.outcomeFor(leg); got != "pass" {
			t.Errorf("leg=%s outcome: want pass, got %q", leg, got)
		}
	}
	if len(fm.latencies) != 3 {
		t.Errorf("latency observations: want 3, got %d", len(fm.latencies))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected DB activity: %v", err)
	}
}

// TestAuthProbe_ExchangeHeadersMissing_FailsLeg2 — the regression class
// this prober exists to catch. /auth/exchange responds 400 with NO
// Access-Control-Allow-Credentials header (the api PR #198 bug). Expect
// leg=exchange_headers result=fail + an audit_log insert.
func TestAuthProbe_ExchangeHeadersMissing_FailsLeg2(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/email/start":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/auth/exchange":
			// CORS Allow-Origin set but Allow-Credentials MISSING — the AUTH-004 bug.
			w.Header().Set("Access-Control-Allow-Origin", "https://instanode.dev")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
		case "/auth/me":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"email":"u@e.com"}`))
		}
	}))
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	// Expect exactly one audit_log insert for the exchange_headers fail.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	fm := &fakeAuthProbeMetrics{}
	w := jobs.NewAuthProbeWorker(db, srv.Client(), fm, authProbeBaseConfig(srv.URL))
	if err := w.Work(context.Background(), fakeJob[jobs.AuthProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := fm.outcomeFor("exchange_headers"); got != "fail" {
		t.Errorf("exchange_headers outcome: want fail, got %q", got)
	}
	if got := fm.outcomeFor("email_start"); got != "pass" {
		t.Errorf("email_start outcome: want pass, got %q", got)
	}
	if got := fm.outcomeFor("me"); got != "pass" {
		t.Errorf("me outcome: want pass, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("audit_log expectation: %v", err)
	}
}

// TestAuthProbe_PreflightRejected_FailsLeg2 — the OPTIONS /auth/exchange
// preflight returns 403 (the PR #151 bug: a header-mismatch caused the
// preflight allow-list middleware to reject it). Expect leg fail.
func TestAuthProbe_PreflightRejected_FailsLeg2(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/email/start":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/auth/exchange":
			if r.Method == http.MethodOptions {
				// The PreflightAllowlist middleware rejected the preflight.
				w.WriteHeader(http.StatusForbidden)
				return
			}
			// POST never reached — but stub a sensible answer just in case.
			w.WriteHeader(http.StatusBadRequest)
		case "/auth/me":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"email":"u@e.com"}`))
		}
	}))
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	fm := &fakeAuthProbeMetrics{}
	w := jobs.NewAuthProbeWorker(db, srv.Client(), fm, authProbeBaseConfig(srv.URL))
	if err := w.Work(context.Background(), fakeJob[jobs.AuthProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := fm.outcomeFor("exchange_headers"); got != "fail" {
		t.Errorf("exchange_headers outcome: want fail, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("audit_log expectation: %v", err)
	}
}

// TestAuthProbe_EmailStart5xx_FailsLeg1 — /auth/email/start returns 503;
// expect leg=email_start result=fail + audit_log insert. The other legs
// still run (sequential, not short-circuit).
func TestAuthProbe_EmailStart5xx_FailsLeg1(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/email/start":
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"upstream"}`))
		case "/auth/exchange":
			w.Header().Set("Access-Control-Allow-Origin", "https://instanode.dev")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
		case "/auth/me":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"email":"u@e.com"}`))
		}
	}))
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	fm := &fakeAuthProbeMetrics{}
	w := jobs.NewAuthProbeWorker(db, srv.Client(), fm, authProbeBaseConfig(srv.URL))
	if err := w.Work(context.Background(), fakeJob[jobs.AuthProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := fm.outcomeFor("email_start"); got != "fail" {
		t.Errorf("email_start outcome: want fail, got %q", got)
	}
	if got := fm.outcomeFor("exchange_headers"); got != "pass" {
		t.Errorf("exchange_headers outcome: want pass, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("audit_log expectation: %v", err)
	}
}

// TestAuthProbe_MeReturns401_FailsLeg3 — the probe bearer token is stale;
// /auth/me returns 401. Expect leg=me result=fail + audit_log insert.
func TestAuthProbe_MeReturns401_FailsLeg3(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/email/start":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/auth/exchange":
			w.Header().Set("Access-Control-Allow-Origin", "https://instanode.dev")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
		case "/auth/me":
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
		}
	}))
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	fm := &fakeAuthProbeMetrics{}
	w := jobs.NewAuthProbeWorker(db, srv.Client(), fm, authProbeBaseConfig(srv.URL))
	if err := w.Work(context.Background(), fakeJob[jobs.AuthProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := fm.outcomeFor("me"); got != "fail" {
		t.Errorf("me outcome: want fail, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("audit_log expectation: %v", err)
	}
}

// TestAuthProbe_BearerUnset_LegMeDegraded — AUTH_PROBE_BEARER_TOKEN
// empty; leg 3 should report result=degraded (config drift, not outage)
// and NOT write an audit_log row.
func TestAuthProbe_BearerUnset_LegMeDegraded(t *testing.T) {
	srv := httptest.NewServer(happyHandler())
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	cfg := authProbeBaseConfig(srv.URL)
	cfg.BearerToken = ""
	fm := &fakeAuthProbeMetrics{}
	w := jobs.NewAuthProbeWorker(db, srv.Client(), fm, cfg)
	if err := w.Work(context.Background(), fakeJob[jobs.AuthProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := fm.outcomeFor("me"); got != "degraded" {
		t.Errorf("me outcome: want degraded, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected DB activity: %v", err)
	}
}

// TestAuthProbe_NilDB_FailsLogButContinues — db nil should still emit
// the metric and run all legs; the audit_log write is skipped silently.
func TestAuthProbe_NilDB_FailsLogButContinues(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/email/start":
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/auth/exchange":
			w.Header().Set("Access-Control-Allow-Origin", "https://instanode.dev")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
		case "/auth/me":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"email":"u@e.com"}`))
		}
	}))
	defer srv.Close()

	fm := &fakeAuthProbeMetrics{}
	var nilDB *sql.DB
	w := jobs.NewAuthProbeWorker(nilDB, srv.Client(), fm, authProbeBaseConfig(srv.URL))
	if err := w.Work(context.Background(), fakeJob[jobs.AuthProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if got := fm.outcomeFor("email_start"); got != "fail" {
		t.Errorf("email_start outcome: want fail, got %q", got)
	}
}

// TestAuthProbe_DNSFailure_FailsLeg1NoLatency — point base URL at an
// invalid host so DNS fails. Leg returns result=fail with NO latency
// observation (DNS errors shouldn't pollute the histogram).
func TestAuthProbe_DNSFailure_FailsLeg1NoLatency(t *testing.T) {
	httpCli := &http.Client{Timeout: 1 * time.Second}
	fm := &fakeAuthProbeMetrics{}
	cfg := authProbeBaseConfig("http://this-host-does-not-resolve.invalid")
	w := jobs.NewAuthProbeWorker(nil, httpCli, fm, cfg)
	if err := w.Work(context.Background(), fakeJob[jobs.AuthProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if got := fm.outcomeFor("email_start"); got != "fail" {
		t.Errorf("email_start outcome: want fail (DNS), got %q", got)
	}
	// DNS failures must NOT emit a latency observation — otherwise the
	// histogram fills with "0s" timeouts and skews the P50/P99 tile.
	for _, l := range fm.latencies {
		if l.leg == "email_start" {
			t.Errorf("email_start emitted latency on DNS failure: %v (should be omitted)", l.d)
		}
	}
}

// TestValidateAuthProbeBaseURL — startup-time URL validation.
func TestValidateAuthProbeBaseURL(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{"empty ok (uses default)", "", false},
		{"https ok", "https://api.instanode.dev", false},
		{"http ok (dev)", "http://localhost:8080", false},
		{"ftp rejected", "ftp://example.com", true},
		{"missing host", "https://", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := jobs.ValidateAuthProbeBaseURL(tc.in)
			if tc.wantErr != (err != nil) {
				t.Errorf("ValidateAuthProbeBaseURL(%q) err = %v, wantErr = %v", tc.in, err, tc.wantErr)
			}
		})
	}
}

// TestAuthProbeConfig_Defaults — empty config gets filled with the prod
// defaults; non-empty fields are preserved; trailing slash on BaseURL is
// trimmed so leg URLs don't end up with `//auth/...`.
func TestAuthProbeConfig_Defaults(t *testing.T) {
	in := jobs.AuthProbeConfig{BaseURL: "https://example.com/"}
	out := in.Defaults()
	if out.BaseURL != "https://example.com" {
		t.Errorf("BaseURL trailing slash not trimmed: %q", out.BaseURL)
	}
	if out.Email == "" || out.ReturnTo == "" || out.Origin == "" {
		t.Errorf("defaults not applied: %+v", out)
	}
}

// TestAuthProbe_WrongOriginHeader_FailsLeg2 — the api echoes an origin
// that's NOT on our prod allow-list (e.g. a misconfigured CORS upstream
// or a CDN injecting "*"). Expect leg fail.
func TestAuthProbe_WrongOriginHeader_FailsLeg2(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/email/start":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/auth/exchange":
			// Wildcard origin — invalid for credentialed requests.
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
		case "/auth/me":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"email":"u@e.com"}`))
		}
	}))
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	fm := &fakeAuthProbeMetrics{}
	w := jobs.NewAuthProbeWorker(db, srv.Client(), fm, authProbeBaseConfig(srv.URL))
	if err := w.Work(context.Background(), fakeJob[jobs.AuthProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if got := fm.outcomeFor("exchange_headers"); got != "fail" {
		t.Errorf("exchange_headers outcome: want fail, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("audit_log expectation: %v", err)
	}
}

// TestAuthProbe_EmailStartBodyMissingOK_Fail — endpoint returns 202 but
// body is `{"ok":false}`; assert leg fails on body assertion (not status).
func TestAuthProbe_EmailStartBodyMissingOK_Fail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/email/start":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"ok":false}`))
		case "/auth/exchange":
			w.Header().Set("Access-Control-Allow-Origin", "https://instanode.dev")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
		case "/auth/me":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"email":"u@e.com"}`))
		}
	}))
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	fm := &fakeAuthProbeMetrics{}
	w := jobs.NewAuthProbeWorker(db, srv.Client(), fm, authProbeBaseConfig(srv.URL))
	if err := w.Work(context.Background(), fakeJob[jobs.AuthProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if got := fm.outcomeFor("email_start"); got != "fail" {
		t.Errorf("email_start outcome: want fail, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("audit_log expectation: %v", err)
	}
}

// TestAuthProbe_PromMetricsAdapter — exercise the production
// AuthProbePromMetrics so the adapter's methods are covered. We only
// assert it doesn't panic — the Prom registry is process-global and
// asserting the increment here would couple the test to other tests'
// side effects.
func TestAuthProbe_PromMetricsAdapter(t *testing.T) {
	m := jobs.AuthProbePromMetrics{}
	m.IncOutcome("email_start", "pass")
	m.ObserveLatency("email_start", 12*time.Millisecond)
}

// guardCompileTime ensures the fakeAuthProbeMetrics conforms to the
// AuthProbeMetrics interface — a regression in the interface signature
// fails this test (and the rest of the file) at compile time.
var _ jobs.AuthProbeMetrics = (*fakeAuthProbeMetrics)(nil)

// guard that the strings test helper compiles.
var _ = strings.Contains
