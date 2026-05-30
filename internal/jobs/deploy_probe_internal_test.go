package jobs

// deploy_probe_internal_test.go — white-box tests for unexported
// helpers in deploy_probe.go that the black-box test package can't
// reach. Kept in a separate file so the rest of the test suite stays
// in jobs_test. Mirrors auth_probe_internal_test.go in shape.
//
// The injected `*Budget` / `pollInterval` knobs on DeployProbeWorker
// are unexported, so the only way to drive the degraded-latency +
// after-deadline branches in a unit test is from inside the jobs
// package — this file owns those tests.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestBuildDeployProbeNginxTarball_RoundTrips — buildDeployProbeNginxTarball
// success path; assert the returned bytes are a valid gzipped tar with a
// single Dockerfile entry carrying the FROM + EXPOSE lines. Catches an
// accidental drop of either Dockerfile line (would make every prod tick
// fail Kaniko with "no FROM").
func TestBuildDeployProbeNginxTarball_RoundTrips(t *testing.T) {
	out := buildDeployProbeNginxTarball()
	gr, err := gzip.NewReader(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	tr := tar.NewReader(gr)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("tar.Next: %v", err)
	}
	if hdr.Name != "Dockerfile" {
		t.Errorf("first entry name: got %q, want Dockerfile", hdr.Name)
	}
	contents, err := io.ReadAll(tr)
	if err != nil {
		t.Fatalf("io.ReadAll: %v", err)
	}
	if !strings.Contains(string(contents), "FROM nginx:alpine") {
		t.Errorf("Dockerfile missing FROM: %q", string(contents))
	}
	if !strings.Contains(string(contents), "EXPOSE 80") {
		t.Errorf("Dockerfile missing EXPOSE: %q", string(contents))
	}
}

// TestBuildDeployProbeMultipart_Shape — buildDeployProbeMultipart's
// success path. Asserts the multipart body carries `tarball`, `name`,
// `port=80`, `env`, and `redeploy=true` — the exact field set the api's
// /deploy/new handler reads. A missing field here would silently flip
// the prober to "fresh deploy" (no redeploy) and burn one slot per tick.
func TestBuildDeployProbeMultipart_Shape(t *testing.T) {
	body, contentType := buildDeployProbeMultipart("probe-name", "development")
	if !strings.HasPrefix(contentType, "multipart/form-data; boundary=") {
		t.Errorf("contentType: %q", contentType)
	}
	raw := body.String()
	for _, want := range []string{
		`name="tarball"`,
		`filename="app.tar.gz"`,
		`name="name"`, "probe-name",
		`name="port"`, "80",
		`name="env"`, "development",
		`name="redeploy"`, "true",
	} {
		if !strings.Contains(raw, want) {
			t.Errorf("multipart body missing %q", want)
		}
	}
}

// TestRecordLeg_AllBranches — recordLeg's three branches (pass /
// degraded / fail) emit different slog levels. The fail branch on a
// nil-DB worker exercises the audit-skip path. We can't observe the
// log lines from a unit test without reaching into slog's handler,
// so we just assert the function doesn't panic and the metric is bumped.
func TestRecordLeg_AllBranches(t *testing.T) {
	fm := &capturingDeployMetrics{}
	w := &DeployProbeWorker{
		cfg:     DeployProbeConfig{BaseURL: "http://x", DeployHost: "y"}.Defaults(),
		metrics: fm,
	}
	ctx := context.Background()

	w.recordLeg(ctx, deployProbeLegSubmit, deployProbeLegResult{
		result: deployProbeResultPass, latency: time.Millisecond, observeLatency: true,
	})
	w.recordLeg(ctx, deployProbeLegStatus, deployProbeLegResult{
		result: deployProbeResultDegraded, reason: "slow", latency: time.Second,
	})
	// Fail branch with nil DB — writes ERROR log line, skips audit insert
	// without crashing.
	w.recordLeg(ctx, deployProbeLegServe, deployProbeLegResult{
		result: deployProbeResultFail, reason: "boom", httpStatus: 502,
	})
	if len(fm.outcomes) != 3 {
		t.Errorf("want 3 metric emits, got %d", len(fm.outcomes))
	}
}

// capturingDeployMetrics is a local fake for the internal-test package
// (the black-box _test file's fakeDeployProbeMetrics lives in jobs_test
// and isn't reachable here).
type capturingDeployMetrics struct {
	outcomes []string
}

func (c *capturingDeployMetrics) IncOutcome(leg, result string) {
	c.outcomes = append(c.outcomes, leg+"="+result)
}
func (c *capturingDeployMetrics) ObserveLatency(string, time.Duration) {}

// TestLegStatus_CtxCancelledAtTopOfLoop — drives the ctx.Err() check
// at the very top of legStatus's for-loop. With a pre-cancelled ctx
// the first iteration's check fires and returns fail with
// reason="ctx_cancelled" (distinct from "ctx_cancelled_during_poll").
func TestLegStatus_CtxCancelledAtTopOfLoop(t *testing.T) {
	w := &DeployProbeWorker{
		cfg: DeployProbeConfig{
			BaseURL: "http://does-not-matter.invalid", BearerToken: "x",
		}.Defaults(),
		httpCli: &http.Client{Timeout: time.Second},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel
	r := w.legStatus(ctx, "appid")
	if r.result != deployProbeResultFail {
		t.Errorf("result: got %q, want fail", r.result)
	}
	if !strings.HasPrefix(r.reason, "ctx_cancelled") {
		t.Errorf("reason: got %q, want ctx_cancelled prefix", r.reason)
	}
}

// TestLegStatus_BudgetElapsedBranch — drives the "status=%q at budget"
// branch using injected statusBudget=1ms + pollInterval=1ms so a single
// time.After cycle puts us past the deadline. Requires the test server
// to keep returning building so the default branch hits the
// after-deadline check.
func TestLegStatus_BudgetElapsedBranch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"item":{"status":"building"}}`))
	}))
	defer srv.Close()
	w := &DeployProbeWorker{
		cfg:          DeployProbeConfig{BaseURL: srv.URL, BearerToken: "x"}.Defaults(),
		httpCli:      srv.Client(),
		statusBudget: 1 * time.Millisecond,
		pollInterval: 1 * time.Millisecond,
	}
	r := w.legStatus(context.Background(), "appid")
	if r.result != deployProbeResultFail {
		t.Errorf("result: got %q, want fail", r.result)
	}
	if !strings.Contains(r.reason, "at budget") {
		t.Errorf("reason: got %q, want 'at budget' substring", r.reason)
	}
}

// TestLegStatus_PollIntervalSleepCompletes — exercises the
// `case <-time.After(pollInterval)` branch. With injected
// pollInterval=1ms and statusBudget=200ms the first iteration's
// time.After fires (covering the case), the loop continues, then the
// budget elapses → fail.
func TestLegStatus_PollIntervalSleepCompletes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"item":{"status":"building"}}`))
	}))
	defer srv.Close()
	w := &DeployProbeWorker{
		cfg:          DeployProbeConfig{BaseURL: srv.URL, BearerToken: "x"}.Defaults(),
		httpCli:      srv.Client(),
		statusBudget: 50 * time.Millisecond,
		pollInterval: 1 * time.Millisecond,
	}
	r := w.legStatus(context.Background(), "appid")
	if r.result != deployProbeResultFail {
		t.Errorf("result: got %q, want fail", r.result)
	}
}

// TestLegSubmit_DegradedBranch — drives the submit-latency degraded
// branch. Inject submitBudget=1ns so any real-world latency crosses
// the budget; the server still returns a valid response so the leg
// result is degraded (not fail).
func TestLegSubmit_DegradedBranch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"ok":true,"item":{"app_id":"a"}}`))
	}))
	defer srv.Close()
	w := &DeployProbeWorker{
		cfg:          DeployProbeConfig{BaseURL: srv.URL, BearerToken: "x"}.Defaults(),
		httpCli:      srv.Client(),
		submitBudget: 1 * time.Nanosecond,
	}
	r, appID := w.legSubmit(context.Background())
	if r.result != deployProbeResultDegraded {
		t.Errorf("result: got %q, want degraded", r.result)
	}
	if appID != "a" {
		t.Errorf("appID: got %q, want a (degraded still returns the id)", appID)
	}
}

// TestLegServe_DegradedBranch — drives the serve-latency degraded
// branch with serveBudget=1ns. The request itself uses the http
// client's own timeout (not the budget), so any response that lands
// in finite time crosses the 1ns budget and trips degraded.
func TestLegServe_DegradedBranch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	srvHostStr := strings.TrimPrefix(srv.URL, "http://")
	srvHostStr = strings.TrimPrefix(srvHostStr, "https://")
	httpCli := &http.Client{
		Timeout:   2 * time.Second,
		Transport: &serveRetargetTransport{base: http.DefaultTransport, serverURL: srv.URL},
	}
	w := &DeployProbeWorker{
		cfg: DeployProbeConfig{
			BaseURL: srv.URL, DeployHost: srvHostStr, BearerToken: "x",
		}.Defaults(),
		httpCli:     httpCli,
		serveBudget: 1 * time.Nanosecond,
	}
	r := w.legServe(context.Background(), "appid")
	if r.result != deployProbeResultDegraded {
		t.Errorf("result: got %q, want degraded", r.result)
	}
	if !strings.Contains(r.reason, "over budget=") {
		t.Errorf("reason: got %q, want 'over budget=' substring", r.reason)
	}
}

// serveRetargetTransport rewrites https://*.<host> requests to the
// test server's plain-http base URL so the leg-3 URL lands on the
// httptest handler without standing up TLS.
type serveRetargetTransport struct {
	base      http.RoundTripper
	serverURL string
}

func (r *serveRetargetTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme == "https" {
		newURL := r.serverURL + req.URL.Path
		if req.URL.RawQuery != "" {
			newURL += "?" + req.URL.RawQuery
		}
		nr, err := http.NewRequestWithContext(req.Context(), req.Method, newURL, req.Body)
		if err != nil {
			return nil, err
		}
		for k, vs := range req.Header {
			for _, v := range vs {
				nr.Header.Add(k, v)
			}
		}
		return r.base.RoundTrip(nr)
	}
	return r.base.RoundTrip(req)
}

// TestFetchDeployStatus_BuildRequestErr — drive
// http.NewRequestWithContext to error by passing a malformed URL.
// Exercises the defensive build_request branch.
func TestFetchDeployStatus_BuildRequestErr(t *testing.T) {
	w := &DeployProbeWorker{
		cfg:     DeployProbeConfig{BaseURL: "http://x", BearerToken: "x"}.Defaults(),
		httpCli: &http.Client{Timeout: time.Second},
	}
	_, _, err := w.fetchDeployStatus(context.Background(), "http://x/\x7f")
	if err == nil {
		t.Errorf("want err, got nil")
	}
}

// TestFetchDeployStatus_HTTPErr — DNS-fail transport error.
func TestFetchDeployStatus_HTTPErr(t *testing.T) {
	w := &DeployProbeWorker{
		cfg:     DeployProbeConfig{BaseURL: "http://x", BearerToken: "x"}.Defaults(),
		httpCli: &http.Client{Timeout: 200 * time.Millisecond},
	}
	_, _, err := w.fetchDeployStatus(context.Background(), "http://this-does-not-resolve.invalid/")
	if err == nil {
		t.Errorf("want err, got nil")
	}
}

// TestLegServe_HTTPErr — DNS-unresolvable host trips httpCli.Do with
// a transport error. Exercises the http_error branch on the serve leg.
func TestLegServe_HTTPErr(t *testing.T) {
	w := &DeployProbeWorker{
		cfg: DeployProbeConfig{
			BaseURL:     "http://x",
			DeployHost:  "this-does-not-resolve.invalid",
			BearerToken: "x",
		}.Defaults(),
		httpCli: &http.Client{Timeout: 200 * time.Millisecond},
	}
	r := w.legServe(context.Background(), "appid")
	if r.result != deployProbeResultFail {
		t.Errorf("result: got %q, want fail", r.result)
	}
	if !strings.Contains(r.reason, "http_error") {
		t.Errorf("reason: got %q, want http_error prefix", r.reason)
	}
}

// TestLegServe_BuildRequestErr — control char in DeployHost trips
// http.NewRequestWithContext, exercising the build_request branch.
func TestLegServe_BuildRequestErr(t *testing.T) {
	w := &DeployProbeWorker{
		cfg: DeployProbeConfig{
			BaseURL:     "http://x",
			DeployHost:  "bad\x7fhost",
			BearerToken: "x",
		}.Defaults(),
		httpCli: &http.Client{Timeout: time.Second},
	}
	r := w.legServe(context.Background(), "appid")
	if r.result != deployProbeResultFail {
		t.Errorf("result: got %q, want fail", r.result)
	}
	if !strings.Contains(r.reason, "build_request") {
		t.Errorf("reason: got %q, want build_request prefix", r.reason)
	}
}

// TestEffectiveBudgetHelpers_ZeroFallback — each helper returns the
// package-level constant when the per-worker field is zero, and the
// per-worker value when non-zero.
func TestEffectiveBudgetHelpers_ZeroFallback(t *testing.T) {
	w := &DeployProbeWorker{}
	if got := w.effectiveSubmitBudget(); got != deployProbeSubmitBudget {
		t.Errorf("submit fallback: got %v, want %v", got, deployProbeSubmitBudget)
	}
	if got := w.effectiveStatusBudget(); got != deployProbeStatusBudget {
		t.Errorf("status fallback: got %v, want %v", got, deployProbeStatusBudget)
	}
	if got := w.effectiveServeBudget(); got != deployProbeServeBudget {
		t.Errorf("serve fallback: got %v, want %v", got, deployProbeServeBudget)
	}
	if got := w.effectivePollInterval(); got != deployProbePollInterval {
		t.Errorf("poll fallback: got %v, want %v", got, deployProbePollInterval)
	}

	w2 := &DeployProbeWorker{
		submitBudget: 1 * time.Second,
		statusBudget: 2 * time.Second,
		serveBudget:  3 * time.Second,
		pollInterval: 4 * time.Second,
	}
	if got := w2.effectiveSubmitBudget(); got != time.Second {
		t.Errorf("submit override: got %v, want 1s", got)
	}
	if got := w2.effectiveStatusBudget(); got != 2*time.Second {
		t.Errorf("status override: got %v, want 2s", got)
	}
	if got := w2.effectiveServeBudget(); got != 3*time.Second {
		t.Errorf("serve override: got %v, want 3s", got)
	}
	if got := w2.effectivePollInterval(); got != 4*time.Second {
		t.Errorf("poll override: got %v, want 4s", got)
	}
}

// TestNewDeployProbeWorker_CheckRedirectClosure — drive the default
// client's CheckRedirect closure by 302-ing the submit POST against an
// httptest server. The closure returns http.ErrUseLastResponse so the
// prober observes the 302 directly as non-2xx (fail).
func TestNewDeployProbeWorker_CheckRedirectClosure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "/elsewhere")
		w.WriteHeader(http.StatusFound)
	}))
	defer srv.Close()
	w := NewDeployProbeWorker(nil, nil, nil, DeployProbeConfig{
		BaseURL:     srv.URL,
		BearerToken: "x",
	})
	r, _ := w.legSubmit(context.Background())
	if r.result != deployProbeResultFail {
		t.Errorf("result: got %q, want fail (302 is non-2xx)", r.result)
	}
}
