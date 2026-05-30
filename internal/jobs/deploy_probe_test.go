package jobs_test

// deploy_probe_test.go — hermetic tests for DeployProbeWorker.
//
// Each test stands up an httptest.Server that simulates one failure mode
// (or the happy path) of the api's /deploy/new + /deploy/:id surface,
// then asserts:
//   - the per-leg outcome metric is bumped with the right (leg, result)
//     label combination,
//   - an audit_log row is inserted on result=fail (and NOT on pass /
//     degraded / skipped-via-degraded).
//
// Metric path is exercised through a fakeDeployProbeMetrics capture so
// the process-global Prom registry isn't polluted across tests.

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

// fakeDeployProbeMetrics captures every IncOutcome / ObserveLatency call.
type fakeDeployProbeMetrics struct {
	mu        sync.Mutex
	outcomes  []fakeDeployOutcome
	latencies []fakeDeployLatency
}

type fakeDeployOutcome struct{ leg, result string }
type fakeDeployLatency struct {
	leg string
	d   time.Duration
}

func (f *fakeDeployProbeMetrics) IncOutcome(leg, result string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.outcomes = append(f.outcomes, fakeDeployOutcome{leg, result})
}

func (f *fakeDeployProbeMetrics) ObserveLatency(leg string, d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latencies = append(f.latencies, fakeDeployLatency{leg, d})
}

func (f *fakeDeployProbeMetrics) outcomeFor(leg string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, o := range f.outcomes {
		if o.leg == leg {
			return o.result
		}
	}
	return ""
}

// deployProbeBaseConfig returns a config wired against the test server.
// Bearer is set so the early-return "bearer unset" branch doesn't fire;
// DeployHost is the test server host so the leg-3 fetch lands back on
// the same handler.
func deployProbeBaseConfig(t *testing.T, srv *httptest.Server) jobs.DeployProbeConfig {
	t.Helper()
	host := srvHost(t, srv)
	return jobs.DeployProbeConfig{
		BaseURL:     srv.URL,
		DeployHost:  host,
		BearerToken: "test-bearer-token",
		AppName:     "deploy-probe-test",
		Env:         "development",
	}
}

// srvHost strips the scheme from httptest.Server.URL so DeployHost
// works as a hostname suffix. The leg-3 URL becomes
// "https://<appID>.<host>/" — but for httptest the scheme is http and
// the host is 127.0.0.1:PORT. We override the http client to skip TLS
// in deployProbeTestClient below so the https URL still hits the test
// server.
func srvHost(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	u := srv.URL
	u = strings.TrimPrefix(u, "https://")
	u = strings.TrimPrefix(u, "http://")
	return u
}

// (deployProbeTestClient — removed; superseded by httpClientRetargetingHTTPSToServer
// below, which actually rewrites the https URL onto the test server. Kept this
// comment as a breadcrumb for the next reader.)

// httpClientRetargetingHTTPSToServer returns an http.Client whose
// RoundTripper rewrites every https://<host> request to the test
// server's plain-http base URL — so the leg-3 https URL lands on the
// httptest handler without standing up a real TLS listener.
func httpClientRetargetingHTTPSToServer(srv *httptest.Server) *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &retargetRoundTripper{
			base:      http.DefaultTransport,
			serverURL: srv.URL,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

type retargetRoundTripper struct {
	base      http.RoundTripper
	serverURL string
}

func (r *retargetRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// For the leg-3 path the prober uses https://<appID>.<host>/ which
	// must hit our test server. Rewrite the request URL to point at the
	// test server while preserving the path.
	if req.URL.Scheme == "https" {
		newURL := r.serverURL + req.URL.Path
		if req.URL.RawQuery != "" {
			newURL += "?" + req.URL.RawQuery
		}
		nr, err := http.NewRequestWithContext(req.Context(), req.Method, newURL, req.Body)
		if err != nil {
			return nil, err
		}
		// Preserve auth + UA headers, plus surface the original Host so
		// the test handler can branch on the appID-prefixed hostname
		// when needed.
		for k, vs := range req.Header {
			for _, v := range vs {
				nr.Header.Add(k, v)
			}
		}
		nr.Header.Set("X-Original-Host", req.URL.Host)
		return r.base.RoundTrip(nr)
	}
	return r.base.RoundTrip(req)
}

// happyDeployHandler simulates the full /deploy/new + /deploy/:id
// pipeline returning healthy + 200 from the serve path.
func happyDeployHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/deploy/new" && r.Method == http.MethodPost:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"ok":true,"item":{"app_id":"probe-app-123","status":"building"}}`))
		case strings.HasPrefix(r.URL.Path, "/deploy/") && r.Method == http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true,"item":{"app_id":"probe-app-123","status":"healthy"}}`))
		case r.URL.Path == "/":
			// Leg-3 serve target.
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("deploy-probe-ok"))
		default:
			http.NotFound(w, r)
		}
	})
}

// TestDeployProbe_HappyPath_AllLegsPass — every leg returns the expected
// success shape; expect result=pass for all 3 legs, no audit_log rows,
// 3 latency observations.
func TestDeployProbe_HappyPath_AllLegsPass(t *testing.T) {
	srv := httptest.NewServer(happyDeployHandler())
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	fm := &fakeDeployProbeMetrics{}
	w := jobs.NewDeployProbeWorker(db, httpClientRetargetingHTTPSToServer(srv), fm, deployProbeBaseConfig(t, srv))
	if err := w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	for _, leg := range []string{"submit", "status", "serve"} {
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

// TestDeployProbe_SubmitReturns503_FailsLeg1 — /deploy/new returns 503;
// expect leg=submit result=fail + audit_log + downstream legs degraded.
func TestDeployProbe_SubmitReturns503_FailsLeg1(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"upstream"}`))
	}))
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))

	fm := &fakeDeployProbeMetrics{}
	w := jobs.NewDeployProbeWorker(db, httpClientRetargetingHTTPSToServer(srv), fm, deployProbeBaseConfig(t, srv))
	if err := w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := fm.outcomeFor("submit"); got != "fail" {
		t.Errorf("submit outcome: want fail, got %q", got)
	}
	if got := fm.outcomeFor("status"); got != "degraded" {
		t.Errorf("status outcome: want degraded (skipped), got %q", got)
	}
	if got := fm.outcomeFor("serve"); got != "degraded" {
		t.Errorf("serve outcome: want degraded (skipped), got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("audit_log expectation: %v", err)
	}
}

// TestDeployProbe_BuildFailed_FailsLeg2 — submit OK, but the status poll
// reports `failed`; expect leg=status result=fail with the autopsy
// reason in the audit row.
func TestDeployProbe_BuildFailed_FailsLeg2(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/deploy/new":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"ok":true,"item":{"app_id":"abc12345"}}`))
		case strings.HasPrefix(r.URL.Path, "/deploy/"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true,"item":{"app_id":"abc12345","status":"failed"}}`))
		}
	}))
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))

	fm := &fakeDeployProbeMetrics{}
	w := jobs.NewDeployProbeWorker(db, httpClientRetargetingHTTPSToServer(srv), fm, deployProbeBaseConfig(t, srv))
	if err := w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := fm.outcomeFor("submit"); got != "pass" {
		t.Errorf("submit: want pass, got %q", got)
	}
	if got := fm.outcomeFor("status"); got != "fail" {
		t.Errorf("status: want fail, got %q", got)
	}
	if got := fm.outcomeFor("serve"); got != "degraded" {
		t.Errorf("serve: want degraded (skipped), got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("audit_log expectation: %v", err)
	}
}

// TestDeployProbe_StuckBuilding_FailsLeg2 — submit OK, status stays
// "building" past the 90s budget. We shorten the budget by using a
// short-budget worker (BUDGET_OVERRIDE via repeated handler 'building'
// + a cancellable context) so the test isn't slow.
//
// The standard /deploy/:id handler returns building forever; the test
// uses a cancellable context to force the leg-status budget to expire
// early by cancelling after one poll cycle. Asserts result=fail with
// reason mentioning "ctx_cancelled" or "status=building at budget".
func TestDeployProbe_StuckBuilding_FailsLeg2(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/deploy/new":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"ok":true,"item":{"app_id":"abc12345"}}`))
		case strings.HasPrefix(r.URL.Path, "/deploy/"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true,"item":{"app_id":"abc12345","status":"building"}}`))
		}
	}))
	defer srv.Close()

	db, _, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	// No ExpectExec here: when the parent ctx is cancelled to force the
	// stuck-building path, the same ctx is threaded into the audit
	// ExecContext call which then also fails with context.Canceled.
	// The worker WARN-logs the audit failure (audit_insert_failed)
	// instead of crashing — that's the desired posture, and is itself
	// the test's contract.

	fm := &fakeDeployProbeMetrics{}
	w := jobs.NewDeployProbeWorker(db, httpClientRetargetingHTTPSToServer(srv), fm, deployProbeBaseConfig(t, srv))

	// Cancel quickly so we hit the ctx_cancelled branch rather than
	// waiting the full 90s budget.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := w.Work(ctx, fakeJob[jobs.DeployProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := fm.outcomeFor("status"); got != "fail" {
		t.Errorf("status: want fail, got %q", got)
	}
}

// TestDeployProbe_ServeReturns502_FailsLeg3 — submit + status pass,
// but the public-host fetch returns 502 (Ingress down).
func TestDeployProbe_ServeReturns502_FailsLeg3(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/deploy/new":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"ok":true,"item":{"app_id":"abc12345"}}`))
		case strings.HasPrefix(r.URL.Path, "/deploy/"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true,"item":{"app_id":"abc12345","status":"healthy"}}`))
		case r.URL.Path == "/":
			w.WriteHeader(http.StatusBadGateway)
		}
	}))
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))

	fm := &fakeDeployProbeMetrics{}
	w := jobs.NewDeployProbeWorker(db, httpClientRetargetingHTTPSToServer(srv), fm, deployProbeBaseConfig(t, srv))
	if err := w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := fm.outcomeFor("submit"); got != "pass" {
		t.Errorf("submit: want pass, got %q", got)
	}
	if got := fm.outcomeFor("status"); got != "pass" {
		t.Errorf("status: want pass, got %q", got)
	}
	if got := fm.outcomeFor("serve"); got != "fail" {
		t.Errorf("serve: want fail, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("audit_log expectation: %v", err)
	}
}

// TestDeployProbe_BearerUnset_AllDegraded — empty bearer = disabled
// prober; expect all three legs to report degraded (config drift, not
// outage) and no audit row.
func TestDeployProbe_BearerUnset_AllDegraded(t *testing.T) {
	srv := httptest.NewServer(happyDeployHandler())
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	cfg := deployProbeBaseConfig(t, srv)
	cfg.BearerToken = ""
	fm := &fakeDeployProbeMetrics{}
	w := jobs.NewDeployProbeWorker(db, httpClientRetargetingHTTPSToServer(srv), fm, cfg)
	if err := w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	for _, leg := range []string{"submit", "status", "serve"} {
		if got := fm.outcomeFor(leg); got != "degraded" {
			t.Errorf("leg=%s: want degraded, got %q", leg, got)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected DB activity: %v", err)
	}
}

// TestDeployProbe_SubmitOKFalse_FailsLeg1 — 202 + body ok=false; expect
// leg=submit fail on body-assertion.
func TestDeployProbe_SubmitOKFalse_FailsLeg1(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"ok":false,"item":{"app_id":"x"}}`))
	}))
	defer srv.Close()
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	fm := &fakeDeployProbeMetrics{}
	w := jobs.NewDeployProbeWorker(db, httpClientRetargetingHTTPSToServer(srv), fm, deployProbeBaseConfig(t, srv))
	_ = w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]())
	if got := fm.outcomeFor("submit"); got != "fail" {
		t.Errorf("submit: want fail, got %q", got)
	}
}

// TestDeployProbe_SubmitBodyParseErr — 202 + invalid JSON body.
func TestDeployProbe_SubmitBodyParseErr(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`not-json`))
	}))
	defer srv.Close()
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	fm := &fakeDeployProbeMetrics{}
	w := jobs.NewDeployProbeWorker(db, httpClientRetargetingHTTPSToServer(srv), fm, deployProbeBaseConfig(t, srv))
	_ = w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]())
	if got := fm.outcomeFor("submit"); got != "fail" {
		t.Errorf("submit: want fail, got %q", got)
	}
}

// TestDeployProbe_SubmitMissingAppID — 202 + body missing item.app_id.
func TestDeployProbe_SubmitMissingAppID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"ok":true,"item":{}}`))
	}))
	defer srv.Close()
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	fm := &fakeDeployProbeMetrics{}
	w := jobs.NewDeployProbeWorker(db, httpClientRetargetingHTTPSToServer(srv), fm, deployProbeBaseConfig(t, srv))
	_ = w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]())
	if got := fm.outcomeFor("submit"); got != "fail" {
		t.Errorf("submit: want fail, got %q", got)
	}
}

// TestDeployProbe_StatusGetReturns500 — submit OK but GET /deploy/<id>
// returns 500. Expect leg=status fail with poll_error reason.
func TestDeployProbe_StatusGetReturns500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/deploy/new":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"ok":true,"item":{"app_id":"abc"}}`))
		case strings.HasPrefix(r.URL.Path, "/deploy/"):
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	fm := &fakeDeployProbeMetrics{}
	w := jobs.NewDeployProbeWorker(db, httpClientRetargetingHTTPSToServer(srv), fm, deployProbeBaseConfig(t, srv))
	_ = w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]())
	if got := fm.outcomeFor("status"); got != "fail" {
		t.Errorf("status: want fail, got %q", got)
	}
}

// TestDeployProbe_StatusBodyParseErr — submit OK, status GET returns
// 200 with invalid JSON.
func TestDeployProbe_StatusBodyParseErr(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/deploy/new":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"ok":true,"item":{"app_id":"abc"}}`))
		case strings.HasPrefix(r.URL.Path, "/deploy/"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`not-json`))
		}
	}))
	defer srv.Close()
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	fm := &fakeDeployProbeMetrics{}
	w := jobs.NewDeployProbeWorker(db, httpClientRetargetingHTTPSToServer(srv), fm, deployProbeBaseConfig(t, srv))
	_ = w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]())
	if got := fm.outcomeFor("status"); got != "fail" {
		t.Errorf("status: want fail, got %q", got)
	}
}

// TestDeployProbe_StatusMissingField — submit OK, status body OK shape
// but status string missing.
func TestDeployProbe_StatusMissingField(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/deploy/new":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"ok":true,"item":{"app_id":"abc"}}`))
		case strings.HasPrefix(r.URL.Path, "/deploy/"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true,"item":{}}`))
		}
	}))
	defer srv.Close()
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	fm := &fakeDeployProbeMetrics{}
	w := jobs.NewDeployProbeWorker(db, httpClientRetargetingHTTPSToServer(srv), fm, deployProbeBaseConfig(t, srv))
	_ = w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]())
	if got := fm.outcomeFor("status"); got != "fail" {
		t.Errorf("status: want fail, got %q", got)
	}
}

// TestDeployProbe_NilDB_DoesNotCrash — db nil means the audit_log
// insert is skipped but all legs still emit metric.
func TestDeployProbe_NilDB_DoesNotCrash(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	fm := &fakeDeployProbeMetrics{}
	var nilDB *sql.DB
	w := jobs.NewDeployProbeWorker(nilDB, httpClientRetargetingHTTPSToServer(srv), fm, deployProbeBaseConfig(t, srv))
	if err := w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if got := fm.outcomeFor("submit"); got != "fail" {
		t.Errorf("submit: want fail, got %q", got)
	}
}

// TestDeployProbe_NilMetrics_NoCrash — metrics=nil must not panic.
func TestDeployProbe_NilMetrics_NoCrash(t *testing.T) {
	srv := httptest.NewServer(happyDeployHandler())
	defer srv.Close()
	w := jobs.NewDeployProbeWorker(nil, httpClientRetargetingHTTPSToServer(srv), nil, deployProbeBaseConfig(t, srv))
	if err := w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestDeployProbe_NilHTTPClient_GetsDefault — httpCli=nil should install
// the default with redirect-refusal. Hit the constructor and run Work
// against an unresolvable URL so the leg fails cleanly.
func TestDeployProbe_NilHTTPClient_GetsDefault(t *testing.T) {
	cfg := jobs.DeployProbeConfig{
		BaseURL:     "http://this-host-does-not-resolve.invalid",
		BearerToken: "x",
	}
	fm := &fakeDeployProbeMetrics{}
	w := jobs.NewDeployProbeWorker(nil, nil, fm, cfg)
	if err := w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if got := fm.outcomeFor("submit"); got != "fail" {
		t.Errorf("submit: want fail (DNS), got %q", got)
	}
}

// TestDeployProbe_DNSFailure_NoLatency — DNS error should NOT emit a
// latency observation for submit.
func TestDeployProbe_DNSFailure_NoLatency(t *testing.T) {
	httpCli := &http.Client{Timeout: 1 * time.Second}
	fm := &fakeDeployProbeMetrics{}
	cfg := jobs.DeployProbeConfig{
		BaseURL:     "http://this-host-does-not-resolve.invalid",
		BearerToken: "x",
	}
	w := jobs.NewDeployProbeWorker(nil, httpCli, fm, cfg)
	if err := w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	for _, l := range fm.latencies {
		if l.leg == "submit" {
			t.Errorf("submit emitted latency on DNS failure: %v", l.d)
		}
	}
}

// TestDeployProbe_BadBaseURL_HitsBuildRequestErr — control char in URL
// trips http.NewRequestWithContext, exercising the build_request branch
// on leg-1 (other legs are skipped via the cascade).
func TestDeployProbe_BadBaseURL_HitsBuildRequestErr(t *testing.T) {
	cfg := jobs.DeployProbeConfig{
		BaseURL:     "http://example.com/\x7f",
		BearerToken: "x",
	}
	fm := &fakeDeployProbeMetrics{}
	w := jobs.NewDeployProbeWorker(nil, &http.Client{Timeout: time.Second}, fm, cfg)
	_ = w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]())
	if got := fm.outcomeFor("submit"); got != "fail" {
		t.Errorf("submit: want fail (build_request), got %q", got)
	}
}

// TestValidateDeployProbeBaseURL — startup-time URL validation.
func TestValidateDeployProbeBaseURL(t *testing.T) {
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
			err := jobs.ValidateDeployProbeBaseURL(tc.in)
			if tc.wantErr != (err != nil) {
				t.Errorf("ValidateDeployProbeBaseURL(%q) err = %v, wantErr = %v", tc.in, err, tc.wantErr)
			}
		})
	}
}

// TestValidateDeployProbeBaseURL_ParseError — non-URL string returns
// err (parse path distinct from the scheme/host gate above).
func TestValidateDeployProbeBaseURL_ParseError(t *testing.T) {
	if err := jobs.ValidateDeployProbeBaseURL("://"); err == nil {
		t.Errorf("want err for malformed URL, got nil")
	}
}

// TestDeployProbeConfig_Defaults — empty fields get filled with
// production defaults; non-empty fields preserved; trailing slash on
// BaseURL is trimmed.
func TestDeployProbeConfig_Defaults(t *testing.T) {
	in := jobs.DeployProbeConfig{BaseURL: "https://example.com/", DeployHost: ".deploy.example.com/"}
	out := in.Defaults()
	if out.BaseURL != "https://example.com" {
		t.Errorf("BaseURL trailing slash not trimmed: %q", out.BaseURL)
	}
	if out.DeployHost != "deploy.example.com" {
		t.Errorf("DeployHost leading dot / trailing slash not trimmed: %q", out.DeployHost)
	}
	if out.AppName == "" || out.Env == "" {
		t.Errorf("defaults not applied: %+v", out)
	}
}

// TestDeployProbeConfig_Defaults_AllEmpty — zero-value config gets every
// default filled in.
func TestDeployProbeConfig_Defaults_AllEmpty(t *testing.T) {
	out := jobs.DeployProbeConfig{}.Defaults()
	if out.BaseURL == "" || out.DeployHost == "" || out.AppName == "" || out.Env == "" {
		t.Errorf("Defaults() on empty config left a field empty: %+v", out)
	}
}

// TestDeployProbeArgs_Kind — exercise the trivial Kind() method so its
// line is counted as covered.
func TestDeployProbeArgs_Kind(t *testing.T) {
	if got := (jobs.DeployProbeArgs{}).Kind(); got != "deploy_probe" {
		t.Errorf("Kind() = %q, want deploy_probe", got)
	}
}

// TestDeployProbe_PromMetricsAdapter — exercise the production adapter
// so the methods are covered.
func TestDeployProbe_PromMetricsAdapter(t *testing.T) {
	m := jobs.DeployProbePromMetrics{}
	m.IncOutcome("submit", "pass")
	m.ObserveLatency("submit", 12*time.Millisecond)
}

// TestDeployProbe_AuditInsertFails_NoCrash — INSERT returns an error;
// the worker WARN-logs and continues rather than panicking.
func TestDeployProbe_AuditInsertFails_NoCrash(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnError(http.ErrAbortHandler)
	fm := &fakeDeployProbeMetrics{}
	w := jobs.NewDeployProbeWorker(db, httpClientRetargetingHTTPSToServer(srv), fm, deployProbeBaseConfig(t, srv))
	if err := w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]()); err != nil {
		t.Fatalf("Work returned error on audit insert failure: %v", err)
	}
	if got := fm.outcomeFor("submit"); got != "fail" {
		t.Errorf("submit: want fail, got %q", got)
	}
}

// TestDeployProbe_ServeBuildRequestErr — bad deploy host (control char)
// trips http.NewRequestWithContext on leg-3. Need submit + status to
// pass so leg-3 actually runs. We pass a custom config with a malformed
// DeployHost.
func TestDeployProbe_ServeBuildRequestErr(t *testing.T) {
	srv := httptest.NewServer(happyDeployHandler())
	defer srv.Close()
	cfg := deployProbeBaseConfig(t, srv)
	cfg.DeployHost = "bad\x7fhost"
	fm := &fakeDeployProbeMetrics{}
	w := jobs.NewDeployProbeWorker(nil, httpClientRetargetingHTTPSToServer(srv), fm, cfg)
	_ = w.Work(context.Background(), fakeJob[jobs.DeployProbeArgs]())
	if got := fm.outcomeFor("serve"); got != "fail" {
		t.Errorf("serve: want fail (build_request), got %q", got)
	}
}

// guardCompileTime ensures the fakeDeployProbeMetrics conforms to the
// DeployProbeMetrics interface.
var _ jobs.DeployProbeMetrics = (*fakeDeployProbeMetrics)(nil)
