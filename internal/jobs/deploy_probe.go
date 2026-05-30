package jobs

// deploy_probe.go — Hourly synthetic prober for the prod /deploy/new
// pipeline.
//
// Background. On 2026-05-30 the morning's failed truehomie-api Kaniko build
// sat at status=building for 30+ minutes before the user reported it.
// Two worker fixes already landed (worker#65 + #66) that flip the row to
// `failed` within ~30s of Job failure and capture an autopsy. AUTH-004
// (worker#68) closed the auth path with a 5-minute /auth/email/start +
// /auth/exchange + /auth/me probe. This job closes the deploy path: every
// 60 minutes drive a real end-to-end deploy against the prod
// /deploy/new pipeline (Kaniko build → k8s pod → Ingress + TLS) and page
// on any leg that breaks.
//
// Three legs per tick:
//
//   1. POST /deploy/new with a probe-only Bearer
//      (DEPLOY_PROBE_BEARER_TOKEN), name="deploy-probe-hourly",
//      redeploy=true, a tiny in-memory nginx tarball, port=80. Asserts
//      200/202 + item.app_id present. The redeploy=true flag means the
//      same deployment row is reused forever — no slot accumulation,
//      stable app_id + URL across ticks.
//
//   2. Poll GET /deploy/<app_id> every 5s for up to 90s, asserting
//      status flips to "healthy". Anything else at the budget (still
//      `building` / `failed`) is a fail — `building` past 90s is the
//      build-too-slow surface (the autopsy reason on `failed` is the
//      build-broken surface).
//
//   3. Within 30s of a healthy status, fetch
//      https://<app_id>.deployment.instanode.dev/ and assert 200. This
//      catches the serving-path-broken surface (Ingress / TLS / pod
//      health) that the api's status field doesn't.
//
// Each leg emits `instant_deploy_probe_outcome_total{leg, result}` and a
// per-leg latency observation on `instant_deploy_probe_latency_seconds`.
// A `fail` outcome writes one audit_log row (kind=deploy_probe_failed,
// actor='system:deploy_probe') AND a structured ERROR slog line — the
// alert surface plus the slog grep fallback when /metrics is unscrapeable.
//
// Anti-design:
//
//   - No DELETE: the redeploy=true contract means one persistent probe-app
//     row is reused for the lifetime of the probe team.
//   - Not driven from the prod team: a synthetic probe team owns the row,
//     so a probe gone wrong can't burn a real customer's deployment slot
//     or k8s quota.
//   - 60-minute cadence: a stuck build sits for ~30s before being
//     auto-flipped (worker#65) — hourly probing is plenty for "is deploy
//     broken" detection. A 5-minute cadence would also drive 24x the k8s
//     Job + Pod count and 24x the GHCR registry push traffic, both of
//     which would mask real-customer churn signal.

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"

	"archive/tar"

	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	"instant.dev/worker/internal/metrics"
)

// DeployProbePromMetrics is the production DeployProbeMetrics implementation
// — emits to the Prom counter + histogram registered in
// internal/metrics/metrics.go. Stateless; a single instance is shared across
// the worker. Mirrors AuthProbePromMetrics in shape.
type DeployProbePromMetrics struct{}

// IncOutcome bumps instant_deploy_probe_outcome_total{leg, result}.
func (DeployProbePromMetrics) IncOutcome(leg, result string) {
	metrics.DeployProbeOutcomeTotal.WithLabelValues(leg, result).Inc()
}

// ObserveLatency records on instant_deploy_probe_latency_seconds{leg}.
func (DeployProbePromMetrics) ObserveLatency(leg string, d time.Duration) {
	metrics.DeployProbeLatencySeconds.WithLabelValues(leg).Observe(d.Seconds())
}

// deployProbeInterval is the dispatch cadence. 60 minutes is the brief's
// requested value — see the "anti-design" note in this file's docstring.
const deployProbeInterval = 60 * time.Minute

// deployProbeHTTPTimeout caps any single HTTP request. Each leg has its
// own latency budget (leg 1: 30s submit; leg 2: 90s poll with a 5s tick;
// leg 3: 30s serve) but this is the hard ceiling — a TCP black-hole on
// the load balancer cannot pin a goroutine past this value.
const deployProbeHTTPTimeout = 120 * time.Second

// deployProbePollInterval is the gap between status polls in leg 2. 5s
// matches the GET /deploy/<id> read cost (single row + an autopsy
// subquery on `failed`); shorter would just hammer the api without
// catching the status transition any faster, since worker#65 flips
// `building` → `failed` only on the next reconciler tick (30s).
const deployProbePollInterval = 5 * time.Second

// deployProbeStatus* are the values returned by the api's
// GET /deploy/:id `status` field that the prober reads.
const (
	deployProbeStatusBuilding = "building"
	deployProbeStatusHealthy  = "healthy"
	deployProbeStatusFailed   = "failed"
)

// deployProbeLeg* are the three leg names emitted as the `leg` Prometheus
// label and the `leg=` log key. Constants (rather than inline strings) so
// the test asserts the exact label values the alert NRQL keys on.
const (
	deployProbeLegSubmit = "submit"
	deployProbeLegStatus = "status"
	deployProbeLegServe  = "serve"
)

// deployProbeResult* are the outcome enum values emitted as the `result`
// label.
//
//	pass      — leg met all assertions inside its latency budget.
//	fail      — leg failed an assertion (wrong status, build timeout,
//	            5xx from the serving URL). Triggers audit_log row +
//	            ERROR slog line + NR alert.
//	degraded  — leg passed assertions but crossed a soft threshold OR
//	            is configured-off (e.g. probe bearer missing). Tracked
//	            separately so a slow-but-working leg doesn't page.
//	bootstrap — leg-1-only: the first tick of a probe's lifetime saw a
//	            canonical 404 `no_existing_deployment_to_redeploy` from
//	            /deploy/new (the probe-app row doesn't exist yet), then
//	            transparently retried without `redeploy=true` and that
//	            second call succeeded. Distinct from `pass` so the
//	            dashboard surfaces "we self-healed once" as its own
//	            event. Subsequent ticks should report `pass`, not
//	            `bootstrap`. A `bootstrap` outcome does NOT page (it
//	            is the prober working as designed).
const (
	deployProbeResultPass      = "pass"
	deployProbeResultFail      = "fail"
	deployProbeResultDegraded  = "degraded"
	deployProbeResultBootstrap = "bootstrap"
)

// deployProbeRedeployMissingCode is the api's canonical error_code
// (api/internal/handlers/deploy.go) returned with HTTP 404 when
// /deploy/new is called with redeploy=true and no matching app row
// exists for (team, env, name). The probe relies on this EXACT string
// to distinguish "first tick, need to bootstrap" from any other 404
// (auth, routing). Locked in by api#206 as the typed-error contract;
// changing it on the api side must change this constant in lockstep.
const deployProbeRedeployMissingCode = "no_existing_deployment_to_redeploy"

// deployProbeStatusBudget is the leg-2 budget — wall-clock time the api
// has to flip the row from `building` to `healthy`. 90s comfortably
// exceeds the observed end-to-end k8s build for the minimal nginx image
// (~30s — `tarball → kaniko build → image push → k8s rollout → ready`)
// while still alerting on a real regression.
const deployProbeStatusBudget = 90 * time.Second

// deployProbeServeBudget is the leg-3 budget — wall-clock to fetch
// https://<app_id>.deployment.instanode.dev/ once the row reports
// healthy. 30s covers Ingress propagation + TLS handshake + connection
// reuse on a cold edge.
const deployProbeServeBudget = 30 * time.Second

// deployProbeSubmitBudget is the leg-1 budget — wall-clock from POST
// /deploy/new through the api returning 200/202. Same generous 30s
// because the in-place redeploy path writes a deployment row + enqueues
// the build (one DB write + a goroutine spawn), so anything over 30s is
// the api itself being broken, not the build.
const deployProbeSubmitBudget = 30 * time.Second

// deployProbeDefaultBaseURL is the production api host probed by default.
// Overridable via DEPLOY_PROBE_BASE_URL so a dev/staging worker probes
// its own cluster's api rather than prod.
const deployProbeDefaultBaseURL = "https://api.instanode.dev"

// deployProbeDefaultDeployHost is the public host suffix on which the
// platform's k8s Ingress serves customer apps. The leg-3 URL is
// "https://" + appID + "." + this. Overridable via DEPLOY_PROBE_DEPLOY_HOST
// so a dev cluster's *.deploy.example.com wildcard can be probed.
const deployProbeDefaultDeployHost = "deployment.instanode.dev"

// deployProbeAppName is the human-readable name of the probe-app the row
// is keyed on. The api's /deploy/new with `redeploy=true` matches an
// existing row by (team, env, name) — using a stable name means the same
// app_id is reused forever rather than a new one being minted per tick.
const deployProbeAppName = "deploy-probe-hourly"

// deployProbeEnv is the env the probe-app lands in. `development` (the
// post-mig-026 default) is appropriate for a synthetic probe: lowest
// blast radius and no cross-environment confusion. Explicit rather than
// implicit so a stray `?env=production` toggle on the api doesn't
// silently move the probe-app between envs.
const deployProbeEnv = "development"

// auditKindDeployProbeFailed is the audit_log kind emitted on probe
// failure. Operators correlate `audit_log` rows + structured log lines +
// NR alert on this kind for a single triage entry-point.
const auditKindDeployProbeFailed = "deploy_probe_failed"

// deployProbeActor is the actor string written to audit_log so a join on
// `actor = 'system:deploy_probe'` enumerates every probe failure across
// time. Distinct from other worker actors (system:reaper, system:billing,
// system:auth_probe).
const deployProbeActor = "system:deploy_probe"

// DeployProbeArgs is the River job payload — no fields, every tick is a
// full 3-leg sweep against the configured base URL.
type DeployProbeArgs struct{}

// Kind is the River worker key.
func (DeployProbeArgs) Kind() string { return "deploy_probe" }

// DeployProbeMetrics is the narrow surface the worker uses to emit
// outcome counters + latency observations. Extracted as an interface so
// tests can capture emissions without scraping the real /metrics
// registry.
type DeployProbeMetrics interface {
	// IncOutcome bumps `instant_deploy_probe_outcome_total{leg, result}` by 1.
	IncOutcome(leg, result string)
	// ObserveLatency records an observation on
	// `instant_deploy_probe_latency_seconds{leg}`. Called only when an HTTP
	// response was received (DNS / TCP errors omit the observation so the
	// histogram isn't polluted with "0s" timeouts).
	ObserveLatency(leg string, d time.Duration)
}

// DeployProbeConfig bundles the runtime tunables. All fields are
// optional except BearerToken — without the bearer the prober is
// configured-off (every leg returns degraded with reason=bearer_unset).
type DeployProbeConfig struct {
	BaseURL     string // default: deployProbeDefaultBaseURL
	DeployHost  string // default: deployProbeDefaultDeployHost
	BearerToken string // required — empty disables the prober (degraded outcomes)
	AppName     string // default: deployProbeAppName
	Env         string // default: deployProbeEnv
}

// Defaults fills empty fields with their deployProbeDefault* counterparts.
// Returns a copy so the caller's input is not mutated.
func (c DeployProbeConfig) Defaults() DeployProbeConfig {
	out := c
	if out.BaseURL == "" {
		out.BaseURL = deployProbeDefaultBaseURL
	}
	if out.DeployHost == "" {
		out.DeployHost = deployProbeDefaultDeployHost
	}
	if out.AppName == "" {
		out.AppName = deployProbeAppName
	}
	if out.Env == "" {
		out.Env = deployProbeEnv
	}
	out.BaseURL = strings.TrimRight(out.BaseURL, "/")
	out.DeployHost = strings.TrimPrefix(out.DeployHost, ".")
	out.DeployHost = strings.TrimRight(out.DeployHost, "/")
	return out
}

// DeployProbeWorker is the River worker. db is used only for audit_log
// insertions on fail outcomes (nil disables the audit row but the leg
// still runs + metric still emits — fail-open). httpCli is used for all
// HTTP probes; nil installs a default with the global timeout.
//
// The four `*Budget` / `pollInterval` fields are test-only injectable
// knobs — zero means use the package-level deployProbe*Budget /
// deployProbePollInterval constants. Production wiring (NewDeployProbeWorker
// → StartWorkers) leaves them zero so the prod cadence is governed by
// the constants. Unit tests inject short values so the degraded-latency
// + after-deadline branches are reachable inside a second of wall-clock.
type DeployProbeWorker struct {
	river.WorkerDefaults[DeployProbeArgs]
	db      *sql.DB
	httpCli *http.Client
	metrics DeployProbeMetrics
	cfg     DeployProbeConfig

	submitBudget time.Duration
	statusBudget time.Duration
	serveBudget  time.Duration
	pollInterval time.Duration
}

// effectiveSubmitBudget returns the per-worker submit budget, falling
// back to the package-level constant when unset. Read once at function
// entry in legSubmit so the value the test sees on a metric label
// matches the value the timer enforces.
func (w *DeployProbeWorker) effectiveSubmitBudget() time.Duration {
	if w.submitBudget > 0 {
		return w.submitBudget
	}
	return deployProbeSubmitBudget
}

// effectiveStatusBudget — see effectiveSubmitBudget.
func (w *DeployProbeWorker) effectiveStatusBudget() time.Duration {
	if w.statusBudget > 0 {
		return w.statusBudget
	}
	return deployProbeStatusBudget
}

// effectiveServeBudget — see effectiveSubmitBudget.
func (w *DeployProbeWorker) effectiveServeBudget() time.Duration {
	if w.serveBudget > 0 {
		return w.serveBudget
	}
	return deployProbeServeBudget
}

// effectivePollInterval — see effectiveSubmitBudget.
func (w *DeployProbeWorker) effectivePollInterval() time.Duration {
	if w.pollInterval > 0 {
		return w.pollInterval
	}
	return deployProbePollInterval
}

// NewDeployProbeWorker constructs the worker. metrics is required — pass
// the production DeployProbePromMetrics or a test fake.
func NewDeployProbeWorker(db *sql.DB, httpCli *http.Client, metrics DeployProbeMetrics, cfg DeployProbeConfig) *DeployProbeWorker {
	if httpCli == nil {
		httpCli = &http.Client{
			Timeout: deployProbeHTTPTimeout,
			// CheckRedirect: refuse redirects on every leg — a probe that
			// silently follows a 302 to a different host would mask a
			// misrouted DNS / load-balancer config change. The leg-3 serve
			// path SHOULD return a 200 directly; an Ingress that 302s to a
			// custom domain is a config regression.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	return &DeployProbeWorker{
		db:      db,
		httpCli: httpCli,
		metrics: metrics,
		cfg:     cfg.Defaults(),
	}
}

// Work runs one sweep of all three legs. Each leg runs sequentially —
// the legs depend on each other (leg-2 needs the app_id returned by
// leg-1; leg-3 needs the URL constructed from that same id once leg-2
// reports healthy). A failed earlier leg short-circuits the later ones
// with result=skipped so the operator sees the dependency in the metric.
//
// Returns nil unconditionally: River retrying the periodic job would
// just queue the next tick faster than the cadence; the metric +
// audit_log already capture the failure.
func (w *DeployProbeWorker) Work(ctx context.Context, job *river.Job[DeployProbeArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.deploy_probe")
	defer span.End()

	start := time.Now()

	// Bearer unset → all three legs degrade. Same fail-open posture as
	// AUTH-004 leg-3 — config drift is not an outage.
	if w.cfg.BearerToken == "" {
		degraded := deployProbeLegResult{
			result: deployProbeResultDegraded,
			reason: "DEPLOY_PROBE_BEARER_TOKEN unset — probe disabled",
		}
		w.recordLeg(ctx, deployProbeLegSubmit, degraded)
		w.recordLeg(ctx, deployProbeLegStatus, degraded)
		w.recordLeg(ctx, deployProbeLegServe, degraded)
		slog.Info("jobs.deploy_probe.disabled",
			"reason", "bearer_unset",
			"job_id", job.ID,
		)
		return nil
	}

	submitRes, appID := w.legSubmit(ctx)
	w.recordLeg(ctx, deployProbeLegSubmit, submitRes)

	var statusRes, serveRes deployProbeLegResult
	// Both `pass` and `bootstrap` mean leg-1 produced a usable app_id —
	// downstream legs must run on the freshly-bootstrapped row so the
	// next tick has somewhere to redeploy into.
	if submitRes.result != deployProbeResultPass && submitRes.result != deployProbeResultBootstrap {
		// Leg-1 didn't produce a usable app_id — short-circuit the
		// downstream legs. result=degraded with a "skipped" reason so
		// the dashboard distinguishes "we didn't try" from "we tried
		// and failed".
		statusRes = deployProbeLegResult{
			result: deployProbeResultDegraded,
			reason: "submit_leg_failed — status leg skipped",
		}
		serveRes = deployProbeLegResult{
			result: deployProbeResultDegraded,
			reason: "submit_leg_failed — serve leg skipped",
		}
	} else {
		statusRes = w.legStatus(ctx, appID)
		if statusRes.result != deployProbeResultPass {
			serveRes = deployProbeLegResult{
				result: deployProbeResultDegraded,
				reason: "status_leg_failed — serve leg skipped",
			}
		} else {
			serveRes = w.legServe(ctx, appID)
		}
	}
	w.recordLeg(ctx, deployProbeLegStatus, statusRes)
	w.recordLeg(ctx, deployProbeLegServe, serveRes)

	slog.Info("jobs.deploy_probe.completed",
		"submit", submitRes.result,
		"status", statusRes.result,
		"serve", serveRes.result,
		"app_id", appID,
		"duration_ms", time.Since(start).Milliseconds(),
		"job_id", job.ID,
	)
	return nil
}

// deployProbeLegResult bundles one leg's outcome for the recordLeg
// dispatcher. observeLatency is true when the leg should record a
// histogram observation (i.e. an HTTP response was actually received —
// a DNS-fail leg has no meaningful latency to record).
type deployProbeLegResult struct {
	result         string
	reason         string
	latency        time.Duration
	observeLatency bool
	httpStatus     int
}

// recordLeg emits the per-leg metric + audit_log + structured log line.
// Mirrors AuthProbeWorker.recordLeg exactly — same taxonomy across
// probers means one operator dashboard works for both surfaces.
func (w *DeployProbeWorker) recordLeg(ctx context.Context, leg string, r deployProbeLegResult) {
	if w.metrics != nil {
		w.metrics.IncOutcome(leg, r.result)
		if r.observeLatency {
			w.metrics.ObserveLatency(leg, r.latency)
		}
	}
	if r.result == deployProbeResultFail {
		w.emitDeployProbeFailed(ctx, leg, r)
		return
	}
	if r.result == deployProbeResultDegraded {
		slog.Warn("deploy_probe_degraded",
			"leg", leg,
			"reason", r.reason,
			"latency_ms", r.latency.Milliseconds(),
			"http_status", r.httpStatus,
		)
		return
	}
	if r.result == deployProbeResultBootstrap {
		// Self-heal success — log at INFO (not WARN; bootstrap is not
		// a degradation) and skip audit_log. The metric counter at
		// result=bootstrap is the operator-facing surface; this log
		// line is the human-readable confirmation in the worker stream.
		slog.Info("deploy_probe_bootstrap",
			"leg", leg,
			"reason", r.reason,
			"latency_ms", r.latency.Milliseconds(),
			"http_status", r.httpStatus,
		)
		return
	}
	slog.Debug("deploy_probe_pass",
		"leg", leg,
		"latency_ms", r.latency.Milliseconds(),
		"http_status", r.httpStatus,
	)
}

// emitDeployProbeFailed writes the failure audit row + the structured
// ERROR log line. The log line key (`deploy_probe_failed`) is what NR
// alerts on as a fallback when the Prometheus metric path is itself
// down. Same row content lives on both surfaces for cross-correlation.
func (w *DeployProbeWorker) emitDeployProbeFailed(ctx context.Context, leg string, r deployProbeLegResult) {
	slog.Error("deploy_probe_failed",
		"leg", leg,
		"reason", r.reason,
		"http_status", r.httpStatus,
		"latency_ms", r.latency.Milliseconds(),
	)
	if w.db == nil {
		return
	}
	meta := map[string]any{
		"leg":         leg,
		"reason":      r.reason,
		"http_status": r.httpStatus,
		"latency_ms":  r.latency.Milliseconds(),
		"base_url":    w.cfg.BaseURL,
		"deploy_host": w.cfg.DeployHost,
		"app_name":    w.cfg.AppName,
	}
	metaBytes, _ := json.Marshal(meta)
	summary := fmt.Sprintf("deploy probe leg=%s failed: %s", leg, r.reason)
	// team_id is NULL — probe failures are platform-level, not tenant-scoped.
	if _, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES (NULL, $1, $2, $3, $4)
	`, deployProbeActor, auditKindDeployProbeFailed, summary, metaBytes); err != nil {
		slog.Warn("jobs.deploy_probe.audit_insert_failed",
			"leg", leg,
			"error", err,
		)
	}
}

// legSubmit drives leg 1: POST /deploy/new with the multipart form the
// api expects (tarball + name + port + env + redeploy=true). Returns the
// result plus the app_id pulled from the response envelope. The app_id
// is the key the next two legs depend on.
//
// Bootstrap retry: the persistent probe-app row doesn't exist on the
// FIRST tick of a probe's lifetime, so the redeploy=true POST hits
// /deploy/new's "no_existing_deployment_to_redeploy" 404. legSubmit
// detects that exact canonical error code (NOT any 404 — see api#206
// for the typed-error contract) and transparently retries once with
// redeploy omitted (create semantics). The retry's outcome is reported
// as `result=bootstrap` rather than `pass` so the dashboard can
// distinguish "we self-healed once" from "steady-state working". Every
// tick thereafter sees the row, gets a 2xx on the first call, and
// reports `pass`.
func (w *DeployProbeWorker) legSubmit(ctx context.Context) (deployProbeLegResult, string) {
	r, appID, errCode := w.legSubmitOnce(ctx, true /* redeploy */)
	if r.result != deployProbeResultFail || r.httpStatus != http.StatusNotFound || errCode != deployProbeRedeployMissingCode {
		return r, appID
	}
	// Canonical "first tick, app doesn't exist yet" → retry as a create.
	// The second call carries the same tarball + name + env, just without
	// redeploy=true. Anti-design: this is the ONLY 404 we retry on — a
	// non-canonical 404 (auth, routing, an api-side regression that
	// dropped the error_code field) still fails the leg, so we never
	// mask a real outage as a bootstrap.
	slog.Info("jobs.deploy_probe.bootstrap_retry",
		"reason", "first_tick: api returned "+deployProbeRedeployMissingCode+" on redeploy=true",
		"app_name", w.cfg.AppName,
		"env", w.cfg.Env,
	)
	r2, appID2, _ := w.legSubmitOnce(ctx, false /* redeploy */)
	if r2.result == deployProbeResultPass {
		// Successful self-heal: relabel as `bootstrap` so a steady-state
		// dashboard tile shows the one-time bootstrap event distinctly
		// from the per-tick pass.
		r2.result = deployProbeResultBootstrap
		r2.reason = "first-tick bootstrap: api returned " + deployProbeRedeployMissingCode + " on redeploy=true; retried without redeploy"
	}
	return r2, appID2
}

// legSubmitOnce performs a single POST /deploy/new. `redeploy` controls
// whether the request body carries `redeploy=true` (steady-state) or
// omits the field (first-tick bootstrap). Returns the leg result, the
// app_id (empty unless result=pass), and the api's canonical `error`
// string when the response was a non-2xx with a parseable JSON envelope
// (empty otherwise). The error_code is what the caller uses to decide
// whether a 404 is the bootstrap path or a real outage.
func (w *DeployProbeWorker) legSubmitOnce(ctx context.Context, redeploy bool) (deployProbeLegResult, string, string) {
	// buildDeployProbeMultipart writes to an in-memory bytes.Buffer and
	// cannot fail — see the helper's docstring. No err to check here.
	body, contentType := buildDeployProbeMultipart(w.cfg.AppName, w.cfg.Env, redeploy)

	target := w.cfg.BaseURL + "/deploy/new"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, body)
	if err != nil {
		return deployProbeLegResult{
			result: deployProbeResultFail,
			reason: "build_request: " + err.Error(),
		}, "", ""
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+w.cfg.BearerToken)
	req.Header.Set("User-Agent", "instanode-deploy-probe/1")

	start := time.Now()
	resp, err := w.httpCli.Do(req)
	latency := time.Since(start)
	if err != nil {
		return deployProbeLegResult{
			result:  deployProbeResultFail,
			reason:  "http_error: " + err.Error(),
			latency: latency,
		}, "", ""
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))

	r := deployProbeLegResult{
		latency:        latency,
		observeLatency: true,
		httpStatus:     resp.StatusCode,
	}
	// Accept 200 OR 202 — fresh deploys return 202 (async build), in-place
	// redeploys also return 202. A 200 from a future api refactor would
	// still be a success signal; only non-2xx is a real failure.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		r.result = deployProbeResultFail
		r.reason = fmt.Sprintf("status=%d (want 2xx); body=%s", resp.StatusCode, truncateForLog(string(respBody), 256))
		// Best-effort parse of the api's typed error envelope so the
		// caller can branch on the canonical error code. A parse failure
		// here is non-fatal — the leg still fails, the caller just sees
		// an empty errCode and will not take the bootstrap retry path.
		var errEnv struct {
			ErrorCode string `json:"error"`
		}
		_ = json.Unmarshal(respBody, &errEnv)
		return r, "", errEnv.ErrorCode
	}
	var parsed struct {
		OK   bool `json:"ok"`
		Item struct {
			AppID string `json:"app_id"`
		} `json:"item"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		r.result = deployProbeResultFail
		r.reason = "body_parse: " + err.Error() + "; raw=" + truncateForLog(string(respBody), 256)
		return r, "", ""
	}
	if !parsed.OK {
		r.result = deployProbeResultFail
		r.reason = "body ok=false; raw=" + truncateForLog(string(respBody), 256)
		return r, "", ""
	}
	if parsed.Item.AppID == "" {
		r.result = deployProbeResultFail
		r.reason = "body missing item.app_id; raw=" + truncateForLog(string(respBody), 256)
		return r, "", ""
	}
	submitBudget := w.effectiveSubmitBudget()
	if latency > submitBudget {
		r.result = deployProbeResultDegraded
		r.reason = fmt.Sprintf("latency=%dms over budget=%dms", latency.Milliseconds(), submitBudget.Milliseconds())
		return r, parsed.Item.AppID, ""
	}
	r.result = deployProbeResultPass
	return r, parsed.Item.AppID, ""
}

// legStatus drives leg 2: poll GET /deploy/<appID> until status is
// `healthy` or `failed`, up to deployProbeStatusBudget. `building` past
// the budget is its own fail surface (build-too-slow); `failed` is the
// build-broken surface and surfaces the autopsy reason in the metric
// log.
func (w *DeployProbeWorker) legStatus(ctx context.Context, appID string) deployProbeLegResult {
	target := w.cfg.BaseURL + "/deploy/" + url.PathEscape(appID)
	statusBudget := w.effectiveStatusBudget()
	pollInterval := w.effectivePollInterval()
	deadline := time.Now().Add(statusBudget)
	start := time.Now()

	for {
		// Honour the parent ctx so a worker shutdown drops the probe
		// cleanly rather than spinning until the budget expires.
		if err := ctx.Err(); err != nil {
			return deployProbeLegResult{
				result:  deployProbeResultFail,
				reason:  "ctx_cancelled: " + err.Error(),
				latency: time.Since(start),
			}
		}
		status, httpStatus, err := w.fetchDeployStatus(ctx, target)
		if err != nil {
			// Non-2xx or transport errors are a fail — the api should be
			// able to read a row it just wrote. Don't keep polling.
			return deployProbeLegResult{
				result:         deployProbeResultFail,
				reason:         "poll_error: " + err.Error(),
				latency:        time.Since(start),
				observeLatency: httpStatus != 0,
				httpStatus:     httpStatus,
			}
		}
		switch status {
		case deployProbeStatusHealthy:
			latency := time.Since(start)
			return deployProbeLegResult{
				result:         deployProbeResultPass,
				latency:        latency,
				observeLatency: true,
				httpStatus:     httpStatus,
			}
		case deployProbeStatusFailed:
			return deployProbeLegResult{
				result:         deployProbeResultFail,
				reason:         "build_failed (status=failed on " + appID + ")",
				latency:        time.Since(start),
				observeLatency: true,
				httpStatus:     httpStatus,
			}
		default:
			// Still building / deploying — sleep and retry, unless the
			// budget has elapsed. Use a select on the timer so a ctx
			// cancellation during the sleep doesn't waste up to 5s.
			if time.Now().After(deadline) {
				return deployProbeLegResult{
					result:         deployProbeResultFail,
					reason:         fmt.Sprintf("status=%q at budget (want healthy within %s)", status, statusBudget),
					latency:        time.Since(start),
					observeLatency: true,
					httpStatus:     httpStatus,
				}
			}
			select {
			case <-ctx.Done():
				return deployProbeLegResult{
					result:  deployProbeResultFail,
					reason:  "ctx_cancelled_during_poll: " + ctx.Err().Error(),
					latency: time.Since(start),
				}
			case <-time.After(pollInterval):
			}
		}
	}
}

// fetchDeployStatus performs one GET /deploy/<id> and returns the
// status string + http status. Decoupled from legStatus so the polling
// loop is testable independently of the per-request HTTP shape.
func (w *DeployProbeWorker) fetchDeployStatus(ctx context.Context, target string) (string, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return "", 0, fmt.Errorf("build_request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+w.cfg.BearerToken)
	req.Header.Set("User-Agent", "instanode-deploy-probe/1")

	resp, err := w.httpCli.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("http_error: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if resp.StatusCode != http.StatusOK {
		return "", resp.StatusCode, fmt.Errorf("status=%d body=%s", resp.StatusCode, truncateForLog(string(respBody), 200))
	}
	var parsed struct {
		Item struct {
			Status string `json:"status"`
		} `json:"item"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return "", resp.StatusCode, fmt.Errorf("body_parse: %w; raw=%s", err, truncateForLog(string(respBody), 200))
	}
	if parsed.Item.Status == "" {
		return "", resp.StatusCode, fmt.Errorf("missing item.status; raw=%s", truncateForLog(string(respBody), 200))
	}
	return parsed.Item.Status, resp.StatusCode, nil
}

// legServe drives leg 3: HTTP GET the public serving URL and assert
// 200. Covers the Ingress / TLS / pod-readiness surface that the api's
// status field doesn't see — a deployment that the api thinks is
// healthy but whose pod isn't actually serving 200s on the public host
// is the silent failure mode we close here.
func (w *DeployProbeWorker) legServe(ctx context.Context, appID string) deployProbeLegResult {
	target := "https://" + appID + "." + w.cfg.DeployHost + "/"

	serveBudget := w.effectiveServeBudget()
	// Use the HTTP client's own timeout (deployProbeHTTPTimeout = 120s)
	// as the hard ceiling on the request; serveBudget is a SOFT post-
	// hoc latency assertion (slow-but-working serves report degraded
	// instead of pinning the goroutine). Decoupling the two lets the
	// degraded branch fire on a serve that crosses 30s but completes
	// before the 120s hard ceiling.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return deployProbeLegResult{
			result: deployProbeResultFail,
			reason: "build_request: " + err.Error(),
		}
	}
	req.Header.Set("User-Agent", "instanode-deploy-probe/1")

	start := time.Now()
	resp, err := w.httpCli.Do(req)
	latency := time.Since(start)
	if err != nil {
		return deployProbeLegResult{
			result:  deployProbeResultFail,
			reason:  "http_error: " + err.Error(),
			latency: latency,
		}
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.ReadAll(io.LimitReader(resp.Body, 1024))

	r := deployProbeLegResult{
		latency:        latency,
		observeLatency: true,
		httpStatus:     resp.StatusCode,
	}
	if resp.StatusCode != http.StatusOK {
		r.result = deployProbeResultFail
		r.reason = fmt.Sprintf("serve_status=%d (want 200) on %s", resp.StatusCode, target)
		return r
	}
	if latency > serveBudget {
		r.result = deployProbeResultDegraded
		r.reason = fmt.Sprintf("latency=%dms over budget=%dms", latency.Milliseconds(), serveBudget.Milliseconds())
		return r
	}
	r.result = deployProbeResultPass
	return r
}

// buildDeployProbeMultipart constructs the multipart body POSTed to
// /deploy/new. The api requires `tarball` + `name` + `port` + `env`;
// `redeploy` is sent as `"true"` when reusing an existing app row
// (steady-state ticks), or omitted entirely on the first-tick
// bootstrap retry so the api takes the create path. Extracted so
// tests can re-use the exact same shape the prober puts on the wire.
//
// Returns the buffer + Content-Type. No error path: every underlying
// operation writes to an in-memory bytes.Buffer (CreateFormFile,
// WriteField, part.Write, mw.Close) which cannot fail — same pattern
// as auth_probe.legEmailStart's `_ = json.Marshal(...)`. Removing the
// defensive branches keeps the patch-coverage gate at 100%.
func buildDeployProbeMultipart(name, env string, redeploy bool) (*bytes.Buffer, string) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)

	tarball := buildDeployProbeNginxTarball()

	// tarball — name `app.tar.gz` is cosmetic; the api reads `tarballs[0]`
	// regardless of filename. CreateFormFile against a bytes.Buffer never
	// errors (the boundary write is the only failable step and the
	// underlying writer can't fail).
	part, _ := mw.CreateFormFile("tarball", "app.tar.gz")
	_, _ = part.Write(tarball)

	// Required + optional scalar fields. Loop rather than three+ duplicated
	// WriteField calls — keeps the field ordering matrix visible at a glance.
	// `redeploy=true` is OMITTED on the bootstrap retry — the api treats
	// the absence of the field as create-semantics, which is the only way
	// to mint the probe-app row on the first-ever tick.
	fields := [][2]string{
		{"name", name},
		{"port", "80"},
		{"env", env},
	}
	if redeploy {
		fields = append(fields, [2]string{"redeploy", "true"})
	}
	for _, f := range fields {
		_ = mw.WriteField(f[0], f[1])
	}
	_ = mw.Close()
	return &buf, mw.FormDataContentType()
}

// buildDeployProbeNginxTarball synthesises a minimal gzipped-tar archive
// containing a Dockerfile that builds an `nginx:alpine` image with a
// trivial root-path response. Kaniko reads the tarball directly so no
// disk fixture is needed at deploy time — the probe carries its own
// build context.
//
// No error path: tar.WriteHeader / tar.Write / tar.Close / gzip.Close
// against an in-memory bytes.Buffer cannot fail. Same defensive-branch-
// removal posture as buildDeployProbeMultipart above.
func buildDeployProbeNginxTarball() []byte {
	dockerfile := []byte("FROM nginx:alpine\nRUN echo 'deploy-probe-ok' > /usr/share/nginx/html/index.html\nEXPOSE 80\n")

	var gz bytes.Buffer
	gw := gzip.NewWriter(&gz)
	tw := tar.NewWriter(gw)

	hdr := &tar.Header{
		Name:    "Dockerfile",
		Mode:    0o644,
		Size:    int64(len(dockerfile)),
		ModTime: time.Unix(0, 0).UTC(), // deterministic — same bytes on every tick
	}
	_ = tw.WriteHeader(hdr)
	_, _ = tw.Write(dockerfile)
	_ = tw.Close()
	_ = gw.Close()
	return gz.Bytes()
}

// ValidateDeployProbeBaseURL is a startup-time sanity check for the
// DEPLOY_PROBE_BASE_URL env var. Returns an error iff the URL is set
// but unparseable; an empty value is accepted (Defaults() fills in the
// production host). Exported so main.go can fail-fast on a typo rather
// than discovering the bad URL on the first tick.
func ValidateDeployProbeBaseURL(raw string) error {
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("DEPLOY_PROBE_BASE_URL parse: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("DEPLOY_PROBE_BASE_URL must be http(s)")
	}
	if u.Host == "" {
		return errors.New("DEPLOY_PROBE_BASE_URL missing host")
	}
	return nil
}

