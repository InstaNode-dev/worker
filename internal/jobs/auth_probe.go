package jobs

// auth_probe.go — AUTH-004 synthetic prober for the prod login loop.
//
// Background — three stacked failures hid broken prod login for ~24h:
//
//   1. Client-side POST /auth/exchange never shipped (instanode-web PR #150)
//   2. Client added `Accept: application/json` → forced CORS preflight
//      that was rejected (PR #151)
//   3. api /auth/exchange response missing `Access-Control-Allow-Credentials:
//      true`, so the browser dropped the response body even on 200 (api PR
//      #198)
//
// Each failure was only catchable by driving a real browser against prod.
// uptime_prober.go / real_prober.go cover the lower layers (TCP / TLS /
// 200 OK) but neither asserts the CORS contract that the browser-side
// /auth/exchange flow depends on. This job adds that assertion as a
// 5-minute periodic synthetic so the next regression in this chain pages
// inside 10 minutes instead of waiting for a user to report broken sign-in.
//
// The prober runs three legs against a configured base URL (defaults to
// https://api.instanode.dev):
//
//   1. POST /auth/email/start with a dedicated synthetic email. The
//      magic-link receiver returns 202 whether or not the email exists
//      (security: no email-enumeration oracle), so we don't need a real
//      user. Asserts 202, body `{"ok":true}`, latency < 2s.
//
//   2. OPTIONS + POST /auth/exchange with `Origin: https://instanode.dev`
//      to drive the SAME preflight a browser would. Asserts the response
//      carries `Access-Control-Allow-Origin` ∈ {https://instanode.dev,
//      https://www.instanode.dev} AND `Access-Control-Allow-Credentials:
//      true`. Status code MAY be 4xx (no cookie attached) — that's fine.
//      The CORS headers are the contract; their absence is the bug we
//      just fixed.
//
//   3. GET /auth/me with a known-good Bearer token (configured via env).
//      Asserts 200 + body has `email`. When the bearer is unset the leg
//      is skipped with result="degraded" — operator hasn't wired the
//      probe-account token. Other legs still run.
//
// Failure handling per leg:
//   - Emits `instant_auth_probe_outcome_total{leg, result}` counter.
//   - Emits `instant_auth_probe_latency_seconds{leg}` histogram (only on
//     a real HTTP response — DNS / TCP errors omit the observation).
//   - On result="fail", writes a row to `audit_log` (kind=auth_probe_failed)
//     AND emits a structured `auth_probe_failed leg=... reason=...` log
//     line that NR can alert on.
//
// SCOPE: the prober is intentionally minimal. It does NOT mint a synthetic
// exchange-cookie JWT (which would require sharing JWT_SECRET with the
// worker pod and adding a probe-only claim to the api whitelist). It
// asserts the CORS HEADER contract on /auth/exchange — the precise
// surface the AUTH-004 chain broke. A future leg can add cookie-based
// round-trip once a probe-only claim is whitelisted in the api.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	"instant.dev/worker/internal/metrics"
)

// AuthProbePromMetrics is the production AuthProbeMetrics implementation
// — emits to the Prom counter + histogram registered in
// internal/metrics/metrics.go. Stateless; a single instance is shared
// across the worker.
type AuthProbePromMetrics struct{}

// IncOutcome bumps instant_auth_probe_outcome_total{leg, result}.
func (AuthProbePromMetrics) IncOutcome(leg, result string) {
	metrics.AuthProbeOutcomeTotal.WithLabelValues(leg, result).Inc()
}

// ObserveLatency records on instant_auth_probe_latency_seconds{leg}.
func (AuthProbePromMetrics) ObserveLatency(leg string, d time.Duration) {
	metrics.AuthProbeLatencySeconds.WithLabelValues(leg).Observe(d.Seconds())
}

// authProbeInterval is the dispatch cadence. 5 minutes is the brief's
// requested value and the trade-off knee: short enough that a regression
// pages inside the 10-minute alert window, long enough that a sustained
// outage doesn't generate a per-second flood (3 legs × 12 ticks/hour =
// 36 probe requests/hour — negligible against /auth/email/start's own
// per-IP rate-limit budget of 5/hr/IP, since the worker hits from a
// stable internal source).
const authProbeInterval = 5 * time.Minute

// authProbeHTTPTimeout caps any single HTTP request. Each leg has its own
// latency budget (2s email_start, 1s exchange_headers, 1s me) but this is
// the hard ceiling — a TCP black-hole at the load balancer can't pin a
// goroutine past this value. The per-leg budget is enforced separately
// via context deadlines.
const authProbeHTTPTimeout = 10 * time.Second

// authProbeLegLatencyBudgets is the per-leg latency budget. Crossing the
// budget is recorded as result="degraded" (separate from result="pass"
// or result="fail") so a slow-but-correct response is still distinguishable
// from a real outage in the metric.
var authProbeLegLatencyBudgets = map[string]time.Duration{
	authProbeLegEmailStart:      2 * time.Second,
	authProbeLegExchangeHeaders: 1 * time.Second,
	authProbeLegMe:              1 * time.Second,
}

// authProbeLeg* are the three leg names emitted as the `leg` Prometheus
// label and the `leg=` log key. Constants (rather than inline strings)
// so the test asserts the exact label values the alert NRQL keys on.
const (
	authProbeLegEmailStart      = "email_start"
	authProbeLegExchangeHeaders = "exchange_headers"
	authProbeLegMe              = "me"
)

// authProbeResult* are the outcome enum values emitted as the `result`
// label.
//
//	pass     — leg met all assertions inside its latency budget.
//	fail     — leg failed an assertion (wrong status, missing header,
//	           DNS / TCP error). Triggers audit_log row + structured
//	           ERROR log line + NR alert.
//	degraded — leg passed assertions but crossed its latency budget,
//	           OR is configured-off (e.g. probe bearer missing). Tracked
//	           separately so a slow-but-working endpoint doesn't page.
const (
	authProbeResultPass     = "pass"
	authProbeResultFail     = "fail"
	authProbeResultDegraded = "degraded"
)

// authProbeDefaultBaseURL is the production api host probed by default.
// Overridable via AUTH_PROBE_BASE_URL so a dev/staging worker probes its
// own cluster's api rather than prod (the same convention as
// UPTIME_PROBE_API_URL in uptime_prober.go).
const authProbeDefaultBaseURL = "https://api.instanode.dev"

// authProbeDefaultEmail is the synthetic identity used for leg 1. The
// magic-link receiver returns 202 whether or not the email exists, so
// there's no requirement that this resolve to a real user — but choosing
// a stable address means /auth/email/start's per-email rate limiter sees
// the prober as one client across ticks (consistent telemetry baseline).
// Overridable via AUTH_PROBE_EMAIL.
const authProbeDefaultEmail = "probe-auth-prod@instanode.dev"

// authProbeDefaultReturnTo is the return_to URL embedded in the magic-
// link POST. The api validates this against an allow-list (https origins
// only in prod); the dashboard /login/callback is always on the allow-list.
const authProbeDefaultReturnTo = "https://instanode.dev/login/callback"

// authProbeDefaultOrigin is the Origin header sent with the /auth/exchange
// preflight + POST. Must match an entry on the api's CORS allow-list, else
// the preflight's `Access-Control-Allow-Origin` will be absent (which is
// itself the bug we're guarding against — picking a known-allowed origin
// ensures a real regression of the ACAC header is what trips the alert,
// not a misconfigured probe origin).
const authProbeDefaultOrigin = "https://instanode.dev"

// authProbeAllowedOrigins is the set of values we accept on the
// `Access-Control-Allow-Origin` response header from /auth/exchange.
// Mirrors api/internal/router/router.go's corsAllowOrigins prod set
// (localhost ports are dev-only and intentionally not listed here — a
// localhost origin echoed in prod would itself be a misconfiguration).
var authProbeAllowedOrigins = map[string]bool{
	"https://instanode.dev":     true,
	"https://www.instanode.dev": true,
}

// auditKindAuthProbeFailed is the audit_log kind emitted on probe
// failure. Operators correlate `audit_log` rows + structured log lines
// + NR alert on this kind for a single triage entry-point.
const auditKindAuthProbeFailed = "auth_probe_failed"

// authProbeActor is the actor string written to audit_log so a join on
// `actor = 'system:auth_probe'` enumerates every probe failure across
// time. Distinct from other worker actors (system:reaper, system:billing)
// so the surface is unambiguous in the audit feed.
const authProbeActor = "system:auth_probe"

// AuthProbeArgs is the River job payload — no fields, every tick is a
// full 3-leg sweep against the configured base URL.
type AuthProbeArgs struct{}

// Kind is the River worker key.
func (AuthProbeArgs) Kind() string { return "auth_probe" }

// AuthProbeMetrics is the narrow surface the worker uses to emit
// outcome counters + latency observations. Extracted as an interface so
// tests can capture emissions without scraping the real /metrics
// registry (avoids cross-test cardinality leaks).
type AuthProbeMetrics interface {
	// IncOutcome bumps `instant_auth_probe_outcome_total{leg, result}` by 1.
	IncOutcome(leg, result string)
	// ObserveLatency records an observation on
	// `instant_auth_probe_latency_seconds{leg}`. Called only when an HTTP
	// response was received (DNS / TCP errors omit the observation so
	// the histogram isn't polluted with "0s" timeouts).
	ObserveLatency(leg string, d time.Duration)
}

// AuthProbeConfig bundles the runtime tunables. All fields are
// optional — Defaults() fills the gaps. Extracted so main.go can wire
// once and tests can override per-case.
type AuthProbeConfig struct {
	BaseURL     string // default: authProbeDefaultBaseURL
	Email       string // default: authProbeDefaultEmail
	ReturnTo    string // default: authProbeDefaultReturnTo
	Origin      string // default: authProbeDefaultOrigin
	BearerToken string // optional — leg 3 skipped (result=degraded) when empty
}

// Defaults fills empty fields with their authProbeDefault* counterparts.
// Returns a copy so the caller's input is not mutated (tests pass a
// shared cfg across cases).
func (c AuthProbeConfig) Defaults() AuthProbeConfig {
	out := c
	if out.BaseURL == "" {
		out.BaseURL = authProbeDefaultBaseURL
	}
	if out.Email == "" {
		out.Email = authProbeDefaultEmail
	}
	if out.ReturnTo == "" {
		out.ReturnTo = authProbeDefaultReturnTo
	}
	if out.Origin == "" {
		out.Origin = authProbeDefaultOrigin
	}
	out.BaseURL = strings.TrimRight(out.BaseURL, "/")
	return out
}

// AuthProbeWorker is the River worker. db is used only for audit_log
// insertions on fail outcomes (nil disables the audit row but the leg
// still runs + metric still emits — fail-open). httpCli is used for all
// HTTP probes; nil installs a default with the global timeout.
type AuthProbeWorker struct {
	river.WorkerDefaults[AuthProbeArgs]
	db      *sql.DB
	httpCli *http.Client
	metrics AuthProbeMetrics
	cfg     AuthProbeConfig
}

// NewAuthProbeWorker constructs the worker. metrics is required — pass
// the production AuthProbePromMetrics (registered via init() in this
// file) or a test fake.
func NewAuthProbeWorker(db *sql.DB, httpCli *http.Client, metrics AuthProbeMetrics, cfg AuthProbeConfig) *AuthProbeWorker {
	if httpCli == nil {
		httpCli = &http.Client{
			Timeout: authProbeHTTPTimeout,
			// CheckRedirect: refuse redirects on every leg — a probe that
			// silently follows a 302 to a different host would mask a
			// misrouted DNS / load-balancer config change.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	return &AuthProbeWorker{
		db:      db,
		httpCli: httpCli,
		metrics: metrics,
		cfg:     cfg.Defaults(),
	}
}

// Work runs one sweep of all three legs. Each leg runs sequentially (not
// in parallel) so a slow leg doesn't artificially mask another leg's
// latency in the histogram (parallelism would inflate every leg's
// observed wall-clock by the slowest peer). The total per-tick budget is
// roughly sum(authProbeLegLatencyBudgets) + http timeouts ≈ 14s — well
// inside the 5-min cadence.
//
// Returns nil unconditionally: River retrying the periodic job would
// just queue the next tick faster than the cadence; the metric +
// audit_log already capture the failure for the operator.
func (w *AuthProbeWorker) Work(ctx context.Context, job *river.Job[AuthProbeArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.auth_probe")
	defer span.End()

	start := time.Now()

	emailRes := w.legEmailStart(ctx)
	w.recordLeg(ctx, authProbeLegEmailStart, emailRes)

	exchangeRes := w.legExchangeHeaders(ctx)
	w.recordLeg(ctx, authProbeLegExchangeHeaders, exchangeRes)

	meRes := w.legMe(ctx)
	w.recordLeg(ctx, authProbeLegMe, meRes)

	slog.Info("jobs.auth_probe.completed",
		"email_start", emailRes.result,
		"exchange_headers", exchangeRes.result,
		"me", meRes.result,
		"duration_ms", time.Since(start).Milliseconds(),
		"job_id", job.ID,
	)
	return nil
}

// authProbeLegResult bundles one leg's outcome for the recordLeg
// dispatcher. observeLatency is true when the leg should record a
// histogram observation (i.e. an HTTP response was actually received —
// a DNS-fail leg has no meaningful latency to record).
type authProbeLegResult struct {
	result          string
	reason          string
	latency         time.Duration
	observeLatency  bool
	httpStatus      int
	relevantHeaders map[string]string
}

// recordLeg emits the per-leg metric + audit_log + structured log line.
// Extracted so the three legs share one taxonomy: leg-result-reason is
// the unit of operator triage.
func (w *AuthProbeWorker) recordLeg(ctx context.Context, leg string, r authProbeLegResult) {
	if w.metrics != nil {
		w.metrics.IncOutcome(leg, r.result)
		if r.observeLatency {
			w.metrics.ObserveLatency(leg, r.latency)
		}
	}
	if r.result == authProbeResultFail {
		w.emitAuthProbeFailed(ctx, leg, r)
		return
	}
	if r.result == authProbeResultDegraded {
		slog.Warn("auth_probe_degraded",
			"leg", leg,
			"reason", r.reason,
			"latency_ms", r.latency.Milliseconds(),
			"http_status", r.httpStatus,
		)
		return
	}
	slog.Debug("auth_probe_pass",
		"leg", leg,
		"latency_ms", r.latency.Milliseconds(),
		"http_status", r.httpStatus,
	)
}

// emitAuthProbeFailed writes the failure audit row + the structured
// ERROR log line. The log line key (`auth_probe_failed`) is what NR
// alerts on as a fallback when the Prometheus metric path is itself
// down (e.g. /metrics scrape blocked). Same row content lives on both
// surfaces for cross-correlation.
func (w *AuthProbeWorker) emitAuthProbeFailed(ctx context.Context, leg string, r authProbeLegResult) {
	slog.Error("auth_probe_failed",
		"leg", leg,
		"reason", r.reason,
		"http_status", r.httpStatus,
		"latency_ms", r.latency.Milliseconds(),
		"headers", r.relevantHeaders,
	)
	if w.db == nil {
		return
	}
	meta := map[string]any{
		"leg":              leg,
		"reason":           r.reason,
		"http_status":      r.httpStatus,
		"latency_ms":       r.latency.Milliseconds(),
		"relevant_headers": r.relevantHeaders,
		"base_url":         w.cfg.BaseURL,
	}
	metaBytes, _ := json.Marshal(meta)
	summary := fmt.Sprintf("auth probe leg=%s failed: %s", leg, r.reason)
	// team_id is NULL — probe failures are platform-level, not tenant-scoped.
	if _, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES (NULL, $1, $2, $3, $4)
	`, authProbeActor, auditKindAuthProbeFailed, summary, metaBytes); err != nil {
		slog.Warn("jobs.auth_probe.audit_insert_failed",
			"leg", leg,
			"error", err,
		)
	}
}

// legEmailStart drives leg 1: POST /auth/email/start with the synthetic
// email. The magic-link receiver returns 202 (with body `{"ok":true}`)
// whether or not the email is a real user — there's no email-enumeration
// oracle by design, which is also what makes this leg safe to run from
// the prober without provisioning a real probe-account upfront.
func (w *AuthProbeWorker) legEmailStart(ctx context.Context) authProbeLegResult {
	budget := authProbeLegLatencyBudgets[authProbeLegEmailStart]
	body, err := json.Marshal(map[string]string{
		"email":     w.cfg.Email,
		"return_to": w.cfg.ReturnTo,
	})
	if err != nil {
		return authProbeLegResult{result: authProbeResultFail, reason: "marshal: " + err.Error()}
	}
	target := w.cfg.BaseURL + "/auth/email/start"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, strings.NewReader(string(body)))
	if err != nil {
		return authProbeLegResult{result: authProbeResultFail, reason: "build_request: " + err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "instanode-auth-probe/1")

	start := time.Now()
	resp, err := w.httpCli.Do(req)
	latency := time.Since(start)
	if err != nil {
		return authProbeLegResult{
			result:  authProbeResultFail,
			reason:  "http_error: " + err.Error(),
			latency: latency,
		}
	}
	defer func() { _ = resp.Body.Close() }()

	// Read at most 4 KiB — the /auth/email/start envelope is ~30 bytes;
	// anything larger is an error envelope we still want to surface.
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	r := authProbeLegResult{
		latency:        latency,
		observeLatency: true,
		httpStatus:     resp.StatusCode,
	}
	if resp.StatusCode != http.StatusAccepted {
		r.result = authProbeResultFail
		r.reason = fmt.Sprintf("status=%d (want 202); body=%s", resp.StatusCode, truncateForLog(string(respBody), 256))
		return r
	}
	var parsed struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		r.result = authProbeResultFail
		r.reason = "body_parse: " + err.Error() + "; raw=" + truncateForLog(string(respBody), 256)
		return r
	}
	if !parsed.OK {
		r.result = authProbeResultFail
		r.reason = "body ok=false; raw=" + truncateForLog(string(respBody), 256)
		return r
	}
	if latency > budget {
		r.result = authProbeResultDegraded
		r.reason = fmt.Sprintf("latency=%dms over budget=%dms", latency.Milliseconds(), budget.Milliseconds())
		return r
	}
	r.result = authProbeResultPass
	return r
}

// legExchangeHeaders drives leg 2: assert the CORS contract on
// /auth/exchange. Two requests:
//
//  1. OPTIONS /auth/exchange with the preflight headers a real browser
//     sends. Asserts: 2xx/3xx, ACAO ∈ allow-list, ACAC=true.
//  2. POST /auth/exchange with Origin set. The request has NO cookie so
//     the api will respond 4xx (cookie_missing_or_expired) — that's
//     fine. The CORS headers MUST still be present on the response
//     (Fiber's cors middleware emits them on every response, error or
//     not). The 4xx is the expected-fail path; absence of ACAC on the
//     response is the bug we just fixed.
func (w *AuthProbeWorker) legExchangeHeaders(ctx context.Context) authProbeLegResult {
	budget := authProbeLegLatencyBudgets[authProbeLegExchangeHeaders]
	target := w.cfg.BaseURL + "/auth/exchange"

	start := time.Now()
	// (1) Preflight.
	preflightReq, err := http.NewRequestWithContext(ctx, http.MethodOptions, target, nil)
	if err != nil {
		return authProbeLegResult{result: authProbeResultFail, reason: "build_preflight: " + err.Error()}
	}
	preflightReq.Header.Set("Origin", w.cfg.Origin)
	preflightReq.Header.Set("Access-Control-Request-Method", "POST")
	preflightReq.Header.Set("Access-Control-Request-Headers", "content-type")
	preflightReq.Header.Set("User-Agent", "instanode-auth-probe/1")

	preflightResp, err := w.httpCli.Do(preflightReq)
	if err != nil {
		return authProbeLegResult{
			result:  authProbeResultFail,
			reason:  "preflight_http_error: " + err.Error(),
			latency: time.Since(start),
		}
	}
	preflightBody, _ := io.ReadAll(io.LimitReader(preflightResp.Body, 1024))
	_ = preflightResp.Body.Close()

	if preflightResp.StatusCode < 200 || preflightResp.StatusCode >= 400 {
		return authProbeLegResult{
			result:         authProbeResultFail,
			reason:         fmt.Sprintf("preflight_status=%d (want 2xx/3xx); body=%s", preflightResp.StatusCode, truncateForLog(string(preflightBody), 256)),
			latency:        time.Since(start),
			observeLatency: true,
			httpStatus:     preflightResp.StatusCode,
			relevantHeaders: map[string]string{
				"Access-Control-Allow-Origin":      preflightResp.Header.Get("Access-Control-Allow-Origin"),
				"Access-Control-Allow-Credentials": preflightResp.Header.Get("Access-Control-Allow-Credentials"),
				"Access-Control-Allow-Methods":     preflightResp.Header.Get("Access-Control-Allow-Methods"),
			},
		}
	}
	if reason, ok := assertCORSHeaders(preflightResp.Header); !ok {
		return authProbeLegResult{
			result:         authProbeResultFail,
			reason:         "preflight_" + reason,
			latency:        time.Since(start),
			observeLatency: true,
			httpStatus:     preflightResp.StatusCode,
			relevantHeaders: map[string]string{
				"Access-Control-Allow-Origin":      preflightResp.Header.Get("Access-Control-Allow-Origin"),
				"Access-Control-Allow-Credentials": preflightResp.Header.Get("Access-Control-Allow-Credentials"),
			},
		}
	}

	// (2) Real POST (without cookie). Any 4xx is fine — we only care
	// about the headers.
	postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, target, strings.NewReader(""))
	if err != nil {
		return authProbeLegResult{result: authProbeResultFail, reason: "build_post: " + err.Error()}
	}
	postReq.Header.Set("Origin", w.cfg.Origin)
	postReq.Header.Set("Content-Type", "application/json")
	postReq.Header.Set("User-Agent", "instanode-auth-probe/1")

	postResp, err := w.httpCli.Do(postReq)
	if err != nil {
		return authProbeLegResult{
			result:  authProbeResultFail,
			reason:  "post_http_error: " + err.Error(),
			latency: time.Since(start),
		}
	}
	defer func() { _ = postResp.Body.Close() }()
	_, _ = io.ReadAll(io.LimitReader(postResp.Body, 1024))

	latency := time.Since(start)
	headers := map[string]string{
		"Access-Control-Allow-Origin":      postResp.Header.Get("Access-Control-Allow-Origin"),
		"Access-Control-Allow-Credentials": postResp.Header.Get("Access-Control-Allow-Credentials"),
	}

	if reason, ok := assertCORSHeaders(postResp.Header); !ok {
		return authProbeLegResult{
			result:          authProbeResultFail,
			reason:          "post_" + reason,
			latency:         latency,
			observeLatency:  true,
			httpStatus:      postResp.StatusCode,
			relevantHeaders: headers,
		}
	}
	if latency > budget {
		return authProbeLegResult{
			result:          authProbeResultDegraded,
			reason:          fmt.Sprintf("latency=%dms over budget=%dms", latency.Milliseconds(), budget.Milliseconds()),
			latency:         latency,
			observeLatency:  true,
			httpStatus:      postResp.StatusCode,
			relevantHeaders: headers,
		}
	}
	return authProbeLegResult{
		result:          authProbeResultPass,
		latency:         latency,
		observeLatency:  true,
		httpStatus:      postResp.StatusCode,
		relevantHeaders: headers,
	}
}

// assertCORSHeaders checks the two headers whose absence/wrong-value was
// the AUTH-004 chain's root cause. Returns (reason, false) on failure
// where reason is a stable string the alert can key on; (anything, true)
// on success.
func assertCORSHeaders(h http.Header) (string, bool) {
	acao := h.Get("Access-Control-Allow-Origin")
	if !authProbeAllowedOrigins[acao] {
		return "missing_or_wrong_acao: got=" + acao, false
	}
	acac := h.Get("Access-Control-Allow-Credentials")
	if !strings.EqualFold(acac, "true") {
		return "missing_or_wrong_acac: got=" + acac, false
	}
	return "", true
}

// legMe drives leg 3: GET /auth/me with a known-good Bearer token. The
// bearer comes from AUTH_PROBE_BEARER_TOKEN — a long-lived JWT minted
// for the probe service account. When unset, the leg is skipped with
// result="degraded" so the operator sees the gap in monitoring without
// failing the alert (a missing probe token is config drift, not a real
// outage).
func (w *AuthProbeWorker) legMe(ctx context.Context) authProbeLegResult {
	budget := authProbeLegLatencyBudgets[authProbeLegMe]
	if w.cfg.BearerToken == "" {
		return authProbeLegResult{
			result: authProbeResultDegraded,
			reason: "AUTH_PROBE_BEARER_TOKEN unset — leg skipped",
		}
	}
	target := w.cfg.BaseURL + "/auth/me"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return authProbeLegResult{result: authProbeResultFail, reason: "build_request: " + err.Error()}
	}
	req.Header.Set("Authorization", "Bearer "+w.cfg.BearerToken)
	req.Header.Set("User-Agent", "instanode-auth-probe/1")

	start := time.Now()
	resp, err := w.httpCli.Do(req)
	latency := time.Since(start)
	if err != nil {
		return authProbeLegResult{
			result:  authProbeResultFail,
			reason:  "http_error: " + err.Error(),
			latency: latency,
		}
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	r := authProbeLegResult{
		latency:        latency,
		observeLatency: true,
		httpStatus:     resp.StatusCode,
	}
	if resp.StatusCode != http.StatusOK {
		r.result = authProbeResultFail
		r.reason = fmt.Sprintf("status=%d (want 200); body=%s", resp.StatusCode, truncateForLog(string(respBody), 256))
		return r
	}
	var parsed struct {
		Email string `json:"email"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		r.result = authProbeResultFail
		r.reason = "body_parse: " + err.Error() + "; raw=" + truncateForLog(string(respBody), 256)
		return r
	}
	if parsed.Email == "" {
		r.result = authProbeResultFail
		r.reason = "body missing email field; raw=" + truncateForLog(string(respBody), 256)
		return r
	}
	if latency > budget {
		r.result = authProbeResultDegraded
		r.reason = fmt.Sprintf("latency=%dms over budget=%dms", latency.Milliseconds(), budget.Milliseconds())
		return r
	}
	r.result = authProbeResultPass
	return r
}

// truncateForLog clamps a string to a max length so a giant response
// body doesn't blow out the audit_log metadata or the log line.
func truncateForLog(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "...[truncated]"
}

// ValidateAuthProbeBaseURL is a startup-time sanity check for the
// AUTH_PROBE_BASE_URL env var. Returns an error iff the URL is set but
// unparseable; an empty value is accepted (Defaults() fills in the
// production host). Exported so main.go can fail-fast on a typo rather
// than discovering the bad URL on the first tick.
func ValidateAuthProbeBaseURL(raw string) error {
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("AUTH_PROBE_BASE_URL parse: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("AUTH_PROBE_BASE_URL must be http(s)")
	}
	if u.Host == "" {
		return errors.New("AUTH_PROBE_BASE_URL missing host")
	}
	return nil
}
