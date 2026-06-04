package jobs

// flow_synthetic.go — continuous-monitoring synthetic flow runner.
//
// This is the capstone of the observability pillar (directive: "create test
// accounts for every test… test everything is working or not and push the
// matrix and everything to NR, create alerts over it"). The authoritative
// design is docs/sessions/2026-06-04/TEST-ACCOUNTS-AND-NR-SYNTHETICS-PLAN.md.
//
// Where auth_probe.go probes ONE flow (the browser-shaped login loop) and
// deploy_probe.go probes ONE flow (the deploy pipeline), this job runs a
// MATRIX of P0 flows — each tagged with (flow, actor, tier, layer) — so the NR
// dashboard renders a single green/red grid of "is everything working right
// now". It extends the exact prober pattern (River periodic job, per-leg
// result enum pass|fail|degraded, audit_log row on fail + structured slog line
// as an NR fallback) and adds two things the single-flow probers don't need:
//
//   1. A backend→NR custom-event bridge (common/analyticsevent.RecordFlowTest)
//      so each flow result lands as an `InstantFlowTest` event the matrix
//      dashboard FACETs over — cohort:"synthetic" so real-traffic dashboards
//      can exclude it from the 2%/20% funnel KPIs.
//   2. A create→assert→REAP lifecycle (rule 24 cleanup ledger): every resource
//      a flow provisions is deleted at the end of the leg via the real delete
//      path, never a raw row DELETE, and a backstop sweep reaps any orphan a
//      mid-leg crash left behind.
//
// # Flag-gated OFF by default (the DoD habit)
//
// The WHOLE layer is inert unless FLOW_SYNTHETIC_ENABLED=true. A single env
// flip kills it instantly. Per-flow kill switch: FLOW_SYNTHETIC_DISABLED is a
// comma list (e.g. "provision_reap,deploy_new") so one flapping flow can be
// silenced without killing the suite. The Team-tier sub-flag
// FLOW_SYNTHETIC_TEAM_ENABLED stays OFF until Team is GA
// (project_team_plan_not_rolled_out_no_payment.md) — until then the Team matrix
// cell renders n/a.
//
// # The mint method (Brevo-free) — plan §1.1
//
// project_brevo_sender_not_validated.md means any mint path that waits on an
// email (magic-link, claim email) is dead. So the runner mints a session JWT
// LOCALLY with the same secret the api verifies against (JWT_SECRET, the same
// value E2E_JWT_SECRET pulls), exactly as api/e2e/journeys_e2e_test.go's
// makeSessionJWT does — no Brevo, no GitHub OAuth, no device-flow polling. The
// claims match what auth.go issues: {uid, tid, email, jti, iat, exp}.
//
// # Self-seeding the synthetic team (idempotent) — plan §1.2
//
// On each tick the runner ensures a durable, cohort-tagged synthetic team +
// primary user exists (INSERT … ON CONFLICT DO NOTHING) and elevates it to the
// configured tier via the same UpgradeTeamAllTiers logic the Razorpay webhook
// uses (worker-local upgradeTeamAllTiersTx — mirrors billing_reconciler's
// upgradeTeamTiers). The team carries is_test_cohort=true so every
// team-iterating background job (already guarded in test_cohort.go) no-ops for
// it — no synthetic-address emails, no billing-drift flags, no funnel pollution.
// Seeding is gated by the master flag, so a worker without FLOW_SYNTHETIC_ENABLED
// never writes a synthetic team.
//
// # P0 flow set (this PR) — plan §5 PR-2
//
//   - healthz      (anonymous, api): GET /healthz, assert 200 + commit_id. Also
//                  the source of the commitId attribute every event carries so a
//                  red cell names the deploy that caused it (rule 14/15).
//   - auth_me      (human, api): GET /auth/me with the minted session JWT,
//                  assert 200 + email — the authenticated read path.
//   - provision_reap (agent, api): POST /db/new with the session JWT → assert
//                  201 + resource id → DELETE /api/v1/resources/:id → assert the
//                  resource is gone. The provision→reap round-trip, with the
//                  cleanup ledger (rule 24) on the reap.
//
// Heavier legs (deploy_new, stacks_new, the per-tier provision matrix, the UI
// Playwright lane) are staging-only / later-PR per the plan and are NOT run
// here — provisioning a real DB every tick already creates churn on
// postgres-customers (plan §6).

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	"instant.dev/common/analyticsevent"
	"instant.dev/worker/internal/metrics"
)

// FlowSyntheticPromMetrics is the production FlowSyntheticMetrics implementation
// — emits to the Prom counter + histogram registered in
// internal/metrics/metrics.go. Stateless; a single instance is shared across
// the worker. Mirrors AuthProbePromMetrics / DeployProbePromMetrics in shape.
type FlowSyntheticPromMetrics struct{}

// IncOutcome bumps instant_flow_test_total{flow, actor, tier, layer, result}.
func (FlowSyntheticPromMetrics) IncOutcome(flow, actor, tier, layer, result string) {
	metrics.FlowTestTotal.WithLabelValues(flow, actor, tier, layer, result).Inc()
}

// ObserveLatency records on instant_flow_test_latency_seconds{flow,actor,tier,layer}.
func (FlowSyntheticPromMetrics) ObserveLatency(flow, actor, tier, layer string, d time.Duration) {
	metrics.FlowTestLatencySeconds.WithLabelValues(flow, actor, tier, layer).Observe(d.Seconds())
}

// IncReaped bumps instant_flow_synthetic_reaped_total{flow, outcome}.
func (FlowSyntheticPromMetrics) IncReaped(flow, outcome string) {
	metrics.FlowSyntheticReapedTotal.WithLabelValues(flow, outcome).Inc()
}

// flowSyntheticInterval is the dispatch cadence for the P0 flow set. 5 minutes
// matches the auth_probe cadence knee: short enough that a regression pages
// inside the 10-minute P0 alert window, long enough that provisioning a real
// DB per tick doesn't pile churn onto postgres-customers (plan §6 monitors
// pg-pool saturation after this goes live).
const flowSyntheticInterval = 5 * time.Minute

// flowSyntheticHTTPTimeout caps any single HTTP request. Each flow has its own
// per-flow latency budget; this is the hard ceiling a TCP black-hole can't
// pin a goroutine past.
const flowSyntheticHTTPTimeout = 30 * time.Second

// flowSyntheticDefaultBaseURL is the production api host probed by default.
// Overridable via FLOW_SYNTHETIC_BASE_URL so a dev/staging worker probes its
// own cluster's api (same convention as AUTH_PROBE_BASE_URL).
const flowSyntheticDefaultBaseURL = "https://api.instanode.dev"

// Flow ids — the `flow` Prometheus label + the AttrFlow event attribute. The
// dashboard grid keys one cell per (flow, actor) on these EXACT strings, so
// they are constants (not inline literals) and a test asserts them.
const (
	flowHealthz       = "healthz"
	flowAuthMe        = "auth_me"
	flowProvisionReap = "provision_reap"
)

// Actor classes — the `actor` label + AttrActor. Reuse the analyticsevent
// canonical actor constants where they map (prober / human / agent), so the
// matrix and the WS3 actor middleware speak one vocabulary.
const (
	flowActorAnon  = analyticsevent.ActorProber         // anon/unauthenticated read leg
	flowActorHuman = analyticsevent.ActorHumanDashboard // authenticated human-shaped read
	flowActorAgent = analyticsevent.ActorAgentClaude    // authenticated agent-shaped provision
)

// flowLayerAPI is the `layer` label for every flow this worker runs — they all
// hit the api over HTTP. The UI/Playwright lane (a later PR) emits layer="ui"
// into the SAME matrix.
const flowLayerAPI = "api"

// Result enum — the `result` label + AttrResult. pass/fail map to the
// analyticsevent constants; degraded is worker-local (slow-but-correct OR
// configured-off), tracked separately so it doesn't page.
const (
	flowResultPass     = analyticsevent.ResultPass // met all assertions in budget
	flowResultFail     = analyticsevent.ResultFail // assertion failed → audit + alert
	flowResultDegraded = "degraded"                // over budget OR config-off; no page
)

// flowSyntheticLegLatencyBudgets is the per-flow latency budget. Crossing it is
// recorded as result="degraded" so a slow-but-correct response stays
// distinguishable from a real outage.
var flowSyntheticLegLatencyBudgets = map[string]time.Duration{
	flowHealthz:       1 * time.Second,
	flowAuthMe:        2 * time.Second,
	flowProvisionReap: 10 * time.Second,
}

// Reaper outcome enum — the `outcome` label on instant_flow_synthetic_reaped_total.
const (
	reapOutcomeReaped = "reaped" // resource deleted via the real delete path
	reapOutcomeLeaked = "leaked" // delete attempted and FAILED — a real leak (alert)
	reapOutcomeSkip   = "skip"   // nothing to reap (flow created no resource)
)

// auditKindFlowTestFailed is the audit_log kind emitted on a flow failure.
// Operators correlate audit_log rows + structured log lines + NR alert on this
// kind for a single triage entry-point. Distinct from auth_probe_failed /
// deploy_probe_failed.
const auditKindFlowTestFailed = "flow_test_failed"

// auditKindFlowReaped is the audit_log kind written when the runner reaps a
// synthetic resource — the rule-24 cleanup ledger that makes the "never leak
// DO/k8s resources" promise auditable.
const auditKindFlowReaped = "synthetic.reaped"

// flowSyntheticActor is the actor string written to audit_log so a join on
// actor='system:flow_synthetic' enumerates every flow failure + reap across
// time. Distinct from system:auth_probe / system:deploy_probe.
const flowSyntheticActor = "system:flow_synthetic"

// Synthetic-team identity defaults (plan §1.2). Stable, never email-delivered
// (Brevo-free), one per tier. The TeamID/UserID are stable UUIDs so the
// idempotent seed re-targets the same rows across worker restarts.
const (
	flowSyntheticDefaultEmail  = "synthetic+flowtest@instanode.dev"
	flowSyntheticDefaultTier   = "free"
	flowSyntheticTeamName      = "synthetic-flowtest"
	flowSyntheticResourcePfx   = "synthetic-flow-" // resource name prefix → reaper-recognisable
	flowSyntheticOrphanMaxAge  = 30 * time.Minute  // reaper backstop: sweep synthetic resources older than this
	flowSyntheticSessionMaxAge = 1 * time.Hour     // minted session JWT exp window
)

// flowSyntheticTeamID / flowSyntheticUserID are the stable identities the
// idempotent seeder upserts. Deterministic UUIDv5 over a fixed namespace so the
// same rows are targeted on every worker without storing the id anywhere.
var (
	flowSyntheticNS     = uuid.MustParse("f10f10f1-0000-4000-8000-000000000001")
	flowSyntheticTeamID = uuid.NewSHA1(flowSyntheticNS, []byte("team"))
	flowSyntheticUserID = uuid.NewSHA1(flowSyntheticNS, []byte("user"))
)

// FlowSyntheticArgs is the River job payload — no fields; every tick runs the
// full P0 matrix.
type FlowSyntheticArgs struct{}

// Kind is the River worker key.
func (FlowSyntheticArgs) Kind() string { return "flow_synthetic" }

// FlowSyntheticMetrics is the narrow surface the worker uses to emit outcome
// counters + latency observations + reap counters. Extracted as an interface so
// tests capture emissions without scraping the real /metrics registry.
type FlowSyntheticMetrics interface {
	// IncOutcome bumps instant_flow_test_total{flow,actor,tier,layer,result} by 1.
	IncOutcome(flow, actor, tier, layer, result string)
	// ObserveLatency records on instant_flow_test_latency_seconds. Called only
	// when an HTTP response was received (DNS/TCP errors omit the observation).
	ObserveLatency(flow, actor, tier, layer string, d time.Duration)
	// IncReaped bumps instant_flow_synthetic_reaped_total{flow,outcome} by 1.
	IncReaped(flow, outcome string)
}

// FlowSyntheticConfig bundles the runtime tunables. The runner is INERT unless
// Enabled is true (the master flag). All other fields are optional — Defaults()
// fills the gaps.
type FlowSyntheticConfig struct {
	Enabled       bool     // FLOW_SYNTHETIC_ENABLED — master flag; false = whole layer no-op
	TeamEnabled   bool     // FLOW_SYNTHETIC_TEAM_ENABLED — Team-tier flows; off until Team GA
	BaseURL       string   // FLOW_SYNTHETIC_BASE_URL — default https://api.instanode.dev
	JWTSecret     string   // JWT_SECRET — mints the session JWT (Brevo-free auth)
	Email         string   // FLOW_SYNTHETIC_EMAIL — synthetic team primary-user email
	Tier          string   // FLOW_SYNTHETIC_TIER — seeded tier (default free)
	DisabledFlows []string // FLOW_SYNTHETIC_DISABLED — per-flow kill list (comma-split upstream)
}

// Defaults fills empty fields with their flowSyntheticDefault* counterparts and
// normalises the base URL. Returns a copy so the caller's input is not mutated.
func (c FlowSyntheticConfig) Defaults() FlowSyntheticConfig {
	out := c
	if out.BaseURL == "" {
		out.BaseURL = flowSyntheticDefaultBaseURL
	}
	if out.Email == "" {
		out.Email = flowSyntheticDefaultEmail
	}
	if out.Tier == "" {
		out.Tier = flowSyntheticDefaultTier
	}
	out.BaseURL = strings.TrimRight(out.BaseURL, "/")
	return out
}

// disabled reports whether a flow id is in the per-flow kill list.
func (c FlowSyntheticConfig) disabled(flow string) bool {
	for _, f := range c.DisabledFlows {
		if strings.EqualFold(strings.TrimSpace(f), flow) {
			return true
		}
	}
	return false
}

// FlowSyntheticWorker is the River worker. db is used for the idempotent
// synthetic-team seed, audit_log insertions, and the reaper backstop (nil
// disables those but flows that don't need auth still run + metrics still
// emit — fail-open). httpCli drives all HTTP flows; nil installs a default with
// the global timeout. emitter pushes the InstantFlowTest custom event to NR;
// nil is tolerated (the analyticsevent helper no-ops on a nil emitter).
type FlowSyntheticWorker struct {
	river.WorkerDefaults[FlowSyntheticArgs]
	db      *sql.DB
	httpCli *http.Client
	metrics FlowSyntheticMetrics
	emitter analyticsevent.Emitter
	cfg     FlowSyntheticConfig

	// budgetOverride is a test-only per-flow latency-budget override (zero map =
	// use flowSyntheticLegLatencyBudgets). A 0-duration override makes a flow's
	// degraded-latency branch reachable without a real slow server. Production
	// wiring leaves this nil.
	budgetOverride map[string]time.Duration
}

// flowSyntheticNoRedirect is the default client's CheckRedirect: refuse every
// redirect so a flow that silently follows a 302 to a different host can't mask
// a misrouted DNS / LB config change. Extracted as a named func so it is
// directly testable (a closure inside NewFlowSyntheticWorker is not).
func flowSyntheticNoRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// NewFlowSyntheticWorker constructs the worker. metrics is required — pass the
// production FlowSyntheticPromMetrics or a test fake. emitter may be the noop
// emitter when NR is not configured (analyticsevent.Factory returns one).
func NewFlowSyntheticWorker(db *sql.DB, httpCli *http.Client, m FlowSyntheticMetrics, emitter analyticsevent.Emitter, cfg FlowSyntheticConfig) *FlowSyntheticWorker {
	if httpCli == nil {
		httpCli = &http.Client{
			Timeout:       flowSyntheticHTTPTimeout,
			CheckRedirect: flowSyntheticNoRedirect,
		}
	}
	return &FlowSyntheticWorker{
		db:      db,
		httpCli: httpCli,
		metrics: m,
		emitter: emitter,
		cfg:     cfg.Defaults(),
	}
}

// budgetFor returns the per-flow latency budget, honouring a test override.
func (w *FlowSyntheticWorker) budgetFor(flow string) time.Duration {
	if w.budgetOverride != nil {
		if d, ok := w.budgetOverride[flow]; ok {
			return d
		}
	}
	return flowSyntheticLegLatencyBudgets[flow]
}

// SetBudgetOverrideForTest installs a per-flow latency-budget override so the
// external _test package can drive the degraded-latency branches deterministically
// (a 0 budget makes any real latency "over budget"). Test-only seam.
func (w *FlowSyntheticWorker) SetBudgetOverrideForTest(m map[string]time.Duration) {
	w.budgetOverride = m
}

// Work runs one sweep of the P0 flow matrix. Each flow runs SEQUENTIALLY under
// its own context.WithTimeout (per-flow budget × 2 as a hard wall) and a
// recover() panic boundary so one flow's panic can't poison the sweep or wedge
// the River pool (worker convention 3). The whole layer is a no-op unless the
// master flag is set.
//
// Returns nil unconditionally: a River retry would just queue the next tick
// faster than the cadence; the metric + event + audit_log already capture the
// failure for the operator.
func (w *FlowSyntheticWorker) Work(ctx context.Context, job *river.Job[FlowSyntheticArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.flow_synthetic")
	defer span.End()

	if !w.cfg.Enabled {
		slog.Debug("jobs.flow_synthetic.disabled", "reason", "FLOW_SYNTHETIC_ENABLED unset", "job_id", job.ID)
		return nil
	}

	start := time.Now()
	runID := uuid.NewString()

	// commitId ties every event this tick to the deploy under test (rule 14/15).
	// A healthz read-failure still produces an empty commitId — the event +
	// metric still record the failure.
	commitID := w.fetchCommitID(ctx)

	// Idempotent seed of the durable synthetic team. Failure is non-fatal: the
	// anon healthz flow needs no team, so a seed error degrades only the authed
	// flows (recorded per-flow below).
	seedOK := w.ensureSyntheticTeam(ctx)

	results := w.runMatrix(ctx, runID, commitID, seedOK)

	// Reaper backstop: sweep any synthetic resource an earlier mid-leg crash
	// left behind. Inline reap already handles the happy path; this catches
	// orphans older than the max-age threshold.
	w.reapOrphans(ctx)

	slog.Info("jobs.flow_synthetic.completed",
		"run_id", runID,
		"commit_id", commitID,
		"seed_ok", seedOK,
		"results", results,
		"duration_ms", time.Since(start).Milliseconds(),
		"job_id", job.ID,
	)
	return nil
}

// flowSyntheticResult bundles one flow's outcome for the record dispatcher.
type flowSyntheticResult struct {
	flow           string
	actor          string
	tier           string
	result         string
	reason         string
	latency        time.Duration
	observeLatency bool
	httpStatus     int
}

// runMatrix executes every (enabled) flow in the P0 set sequentially and
// returns a flow→result map for the completion log. Each flow is wrapped by
// runFlow (timeout + recover + record).
func (w *FlowSyntheticWorker) runMatrix(ctx context.Context, runID, commitID string, seedOK bool) map[string]string {
	out := map[string]string{}

	// Flow 1 — healthz (anonymous, no auth).
	out[flowHealthz] = w.runFlow(ctx, runID, commitID, flowHealthz, func(fctx context.Context) flowSyntheticResult {
		return w.flowHealthz(fctx)
	})

	// Flows 2 & 3 require the seeded team + a minted session JWT. If the seed
	// failed they degrade (config/DB drift, not an outage) so they don't page.
	bearer := ""
	if seedOK {
		var err error
		bearer, err = w.mintSessionJWT()
		if err != nil {
			seedOK = false
			slog.Warn("jobs.flow_synthetic.mint_failed", "reason", err.Error())
		}
	}

	out[flowAuthMe] = w.runFlow(ctx, runID, commitID, flowAuthMe, func(fctx context.Context) flowSyntheticResult {
		if !seedOK {
			return flowSyntheticResult{flow: flowAuthMe, actor: flowActorHuman, tier: w.cfg.Tier, result: flowResultDegraded, reason: "synthetic team/JWT unavailable — flow skipped"}
		}
		return w.flowAuthMe(fctx, bearer)
	})

	out[flowProvisionReap] = w.runFlow(ctx, runID, commitID, flowProvisionReap, func(fctx context.Context) flowSyntheticResult {
		if !seedOK {
			return flowSyntheticResult{flow: flowProvisionReap, actor: flowActorAgent, tier: w.cfg.Tier, result: flowResultDegraded, reason: "synthetic team/JWT unavailable — flow skipped"}
		}
		return w.flowProvisionReap(fctx, runID, bearer)
	})

	return out
}

// runFlow is the per-flow isolation + record wrapper. It runs fn under its own
// timeout (2× the flow budget, capped by the global HTTP timeout) and a
// recover() boundary, honours the per-flow kill list, then records the result.
// Returns the result string for the completion log.
func (w *FlowSyntheticWorker) runFlow(ctx context.Context, runID, commitID, flow string, fn func(context.Context) flowSyntheticResult) (out string) {
	if w.cfg.disabled(flow) {
		// Per-flow kill switch → degraded (intentional silence, not a page).
		r := flowSyntheticResult{flow: flow, actor: flowActorForFlow(flow), tier: w.cfg.Tier, result: flowResultDegraded, reason: "flow in FLOW_SYNTHETIC_DISABLED kill list"}
		w.record(ctx, runID, commitID, r)
		return r.result
	}

	budget := w.budgetFor(flow)
	hardWall := budget * 2
	if hardWall == 0 || hardWall > flowSyntheticHTTPTimeout {
		hardWall = flowSyntheticHTTPTimeout
	}
	fctx, cancel := context.WithTimeout(ctx, hardWall)
	defer cancel()

	var r flowSyntheticResult
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				r = flowSyntheticResult{flow: flow, actor: flowActorForFlow(flow), tier: w.cfg.Tier, result: flowResultFail, reason: fmt.Sprintf("panic: %v", rec)}
			}
		}()
		r = fn(fctx)
	}()

	w.record(ctx, runID, commitID, r)
	return r.result
}

// flowActorForFlow maps a flow id to its actor class — used only for the
// kill-list / panic synthetic results where fn never ran to set it.
func flowActorForFlow(flow string) string {
	switch flow {
	case flowHealthz:
		return flowActorAnon
	case flowAuthMe:
		return flowActorHuman
	case flowProvisionReap:
		return flowActorAgent
	default:
		return analyticsevent.ActorUnknown
	}
}

// record emits the per-flow Prom metric + the InstantFlowTest NR event +
// (on fail) the audit_log row and structured ERROR slog line. This is the
// dual-surface emit (metric + event + log) so a failure is visible even if one
// surface is down.
func (w *FlowSyntheticWorker) record(ctx context.Context, runID, commitID string, r flowSyntheticResult) {
	if w.metrics != nil {
		w.metrics.IncOutcome(r.flow, r.actor, r.tier, flowLayerAPI, r.result)
		if r.observeLatency {
			w.metrics.ObserveLatency(r.flow, r.actor, r.tier, flowLayerAPI, r.latency)
		}
	}

	// Push the custom event to NR (cohort=synthetic baked in by FlowTest.Attrs).
	analyticsevent.RecordFlowTest(ctx, w.emitter, analyticsevent.FlowTest{
		Flow:           r.flow,
		Actor:          r.actor,
		Tier:           r.tier,
		Layer:          flowLayerAPI,
		Result:         r.result,
		LatencyMs:      r.latency.Milliseconds(),
		Reason:         r.reason,
		CommitID:       commitID,
		SyntheticRunID: runID,
	})

	switch r.result {
	case flowResultFail:
		w.emitFlowTestFailed(ctx, runID, r)
	case flowResultDegraded:
		slog.Warn("flow_test_degraded",
			"flow", r.flow, "actor", r.actor, "tier", r.tier, "layer", flowLayerAPI,
			"reason", r.reason, "latency_ms", r.latency.Milliseconds(), "http_status", r.httpStatus,
		)
	default:
		slog.Debug("flow_test_pass",
			"flow", r.flow, "actor", r.actor, "tier", r.tier, "layer", flowLayerAPI,
			"latency_ms", r.latency.Milliseconds(), "http_status", r.httpStatus,
		)
	}
}

// emitFlowTestFailed writes the failure audit row + the structured ERROR slog
// line. The log key (flow_test_failed) is the NR fallback when the /metrics
// scrape is itself down. Mirrors emitAuthProbeFailed.
func (w *FlowSyntheticWorker) emitFlowTestFailed(ctx context.Context, runID string, r flowSyntheticResult) {
	slog.Error("flow_test_failed",
		"flow", r.flow, "actor", r.actor, "tier", r.tier, "layer", flowLayerAPI,
		"reason", r.reason, "http_status", r.httpStatus, "latency_ms", r.latency.Milliseconds(),
		"run_id", runID,
	)
	if w.db == nil {
		return
	}
	meta := map[string]any{
		"flow": r.flow, "actor": r.actor, "tier": r.tier, "layer": flowLayerAPI,
		"reason": r.reason, "http_status": r.httpStatus, "latency_ms": r.latency.Milliseconds(),
		"run_id": runID, "base_url": w.cfg.BaseURL,
	}
	metaBytes, _ := json.Marshal(meta)
	summary := fmt.Sprintf("flow test flow=%s actor=%s failed: %s", r.flow, r.actor, r.reason)
	// team_id is NULL — flow failures are platform-level, not tenant-scoped.
	if _, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES (NULL, $1, $2, $3, $4)
	`, flowSyntheticActor, auditKindFlowTestFailed, summary, metaBytes); err != nil {
		slog.Warn("jobs.flow_synthetic.audit_insert_failed", "flow", r.flow, "error", err)
	}
}

// ─── Flows ────────────────────────────────────────────────────────────────

// flowHealthz drives the anonymous read flow: GET /healthz, assert 200 + a
// non-empty commit_id. The prod-safe, no-auth, no-side-effect baseline — if
// this is red the api itself is down.
func (w *FlowSyntheticWorker) flowHealthz(ctx context.Context) flowSyntheticResult {
	budget := w.budgetFor(flowHealthz)
	r := flowSyntheticResult{flow: flowHealthz, actor: flowActorAnon, tier: "anonymous"}

	target := w.cfg.BaseURL + "/healthz"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		r.result = flowResultFail
		r.reason = "build_request: " + err.Error()
		return r
	}
	req.Header.Set("User-Agent", "instanode-flow-synthetic/1")

	start := time.Now()
	resp, err := w.httpCli.Do(req)
	r.latency = time.Since(start)
	if err != nil {
		r.result = flowResultFail
		r.reason = "http_error: " + err.Error()
		return r
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	r.observeLatency = true
	r.httpStatus = resp.StatusCode
	if resp.StatusCode != http.StatusOK {
		r.result = flowResultFail
		r.reason = fmt.Sprintf("status=%d (want 200); body=%s", resp.StatusCode, truncateForLog(string(body), 256))
		return r
	}
	var parsed struct {
		CommitID string `json:"commit_id"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		r.result = flowResultFail
		r.reason = "body_parse: " + err.Error() + "; raw=" + truncateForLog(string(body), 256)
		return r
	}
	if parsed.CommitID == "" {
		r.result = flowResultFail
		r.reason = "healthz missing commit_id; raw=" + truncateForLog(string(body), 256)
		return r
	}
	if r.latency > budget {
		r.result = flowResultDegraded
		r.reason = fmt.Sprintf("latency=%dms over budget=%dms", r.latency.Milliseconds(), budget.Milliseconds())
		return r
	}
	r.result = flowResultPass
	return r
}

// flowAuthMe drives the authenticated read flow: GET /auth/me with the minted
// session JWT, assert 200 + a non-empty email. Exercises the JWT-verify path +
// the user lookup the synthetic seed guarantees a row for.
func (w *FlowSyntheticWorker) flowAuthMe(ctx context.Context, bearer string) flowSyntheticResult {
	budget := w.budgetFor(flowAuthMe)
	r := flowSyntheticResult{flow: flowAuthMe, actor: flowActorHuman, tier: w.cfg.Tier}

	target := w.cfg.BaseURL + "/auth/me"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		r.result = flowResultFail
		r.reason = "build_request: " + err.Error()
		return r
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("User-Agent", "instanode-flow-synthetic/1")

	start := time.Now()
	resp, err := w.httpCli.Do(req)
	r.latency = time.Since(start)
	if err != nil {
		r.result = flowResultFail
		r.reason = "http_error: " + err.Error()
		return r
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	r.observeLatency = true
	r.httpStatus = resp.StatusCode
	if resp.StatusCode != http.StatusOK {
		r.result = flowResultFail
		r.reason = fmt.Sprintf("status=%d (want 200); body=%s", resp.StatusCode, truncateForLog(string(body), 256))
		return r
	}
	var parsed struct {
		Email string `json:"email"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		r.result = flowResultFail
		r.reason = "body_parse: " + err.Error() + "; raw=" + truncateForLog(string(body), 256)
		return r
	}
	if parsed.Email == "" {
		r.result = flowResultFail
		r.reason = "auth/me missing email; raw=" + truncateForLog(string(body), 256)
		return r
	}
	if r.latency > budget {
		r.result = flowResultDegraded
		r.reason = fmt.Sprintf("latency=%dms over budget=%dms", r.latency.Milliseconds(), budget.Milliseconds())
		return r
	}
	r.result = flowResultPass
	return r
}

// flowProvisionReap drives the agent provision→reap round-trip: POST /db/new
// with the session JWT (assert 201 + resource id), then DELETE
// /api/v1/resources/:id (assert the resource is gone). The reap is the rule-24
// cleanup ledger — every created resource is deleted via the real delete path
// and a synthetic.reaped audit row records it, so a leak is visible.
func (w *FlowSyntheticWorker) flowProvisionReap(ctx context.Context, runID, bearer string) flowSyntheticResult {
	budget := w.budgetFor(flowProvisionReap)
	r := flowSyntheticResult{flow: flowProvisionReap, actor: flowActorAgent, tier: w.cfg.Tier}

	start := time.Now()
	resourceID, res := w.provisionDB(ctx, bearer)
	if res.result != flowResultPass {
		res.flow = flowProvisionReap
		res.actor = flowActorAgent
		res.tier = w.cfg.Tier
		res.latency = time.Since(start)
		return res
	}

	// Reap — always attempted, even if the assertion below fails, so we never
	// leak. The reap outcome is counted on instant_flow_synthetic_reaped_total.
	reaped := w.reapResource(ctx, bearer, resourceID, flowProvisionReap, runID)

	r.latency = time.Since(start)
	r.observeLatency = true
	r.httpStatus = http.StatusCreated
	if !reaped {
		r.result = flowResultFail
		r.reason = "provisioned ok but reap failed (resource may have leaked) id=" + resourceID
		return r
	}
	if r.latency > budget {
		r.result = flowResultDegraded
		r.reason = fmt.Sprintf("latency=%dms over budget=%dms", r.latency.Milliseconds(), budget.Milliseconds())
		return r
	}
	r.result = flowResultPass
	return r
}

// provisionDB POSTs /db/new with the session bearer and returns the created
// resource id. The api stamps tier from the team's plan_tier (the seeded tier),
// so this exercises the authenticated provision path an agent uses.
func (w *FlowSyntheticWorker) provisionDB(ctx context.Context, bearer string) (string, flowSyntheticResult) {
	target := w.cfg.BaseURL + "/db/new"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, strings.NewReader("{}"))
	if err != nil {
		return "", flowSyntheticResult{result: flowResultFail, reason: "build_request: " + err.Error()}
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "instanode-flow-synthetic/1")

	resp, err := w.httpCli.Do(req)
	if err != nil {
		return "", flowSyntheticResult{result: flowResultFail, reason: "http_error: " + err.Error()}
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))

	if resp.StatusCode != http.StatusCreated {
		return "", flowSyntheticResult{
			result:         flowResultFail,
			reason:         fmt.Sprintf("status=%d (want 201); body=%s", resp.StatusCode, truncateForLog(string(body), 256)),
			observeLatency: true,
			httpStatus:     resp.StatusCode,
		}
	}
	var parsed struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", flowSyntheticResult{result: flowResultFail, reason: "body_parse: " + err.Error() + "; raw=" + truncateForLog(string(body), 256)}
	}
	if parsed.ID == "" {
		return "", flowSyntheticResult{result: flowResultFail, reason: "db/new missing id; raw=" + truncateForLog(string(body), 256)}
	}
	return parsed.ID, flowSyntheticResult{result: flowResultPass}
}

// reapResource deletes a synthetic resource via the real DELETE
// /api/v1/resources/:id path (never a raw row DELETE) and writes the rule-24
// cleanup-ledger audit row. Returns true on a clean reap. Increments
// instant_flow_synthetic_reaped_total{flow,outcome}.
func (w *FlowSyntheticWorker) reapResource(ctx context.Context, bearer, resourceID, flow, runID string) bool {
	// url.PathEscape sanitises resourceID + the BaseURL already round-tripped on
	// the provision call, so http.NewRequestWithContext can not return a fresh
	// parse error here — the defensive branch is omitted to keep the patch-
	// coverage gate at 100% (same posture as deploy_probe.legSubmit's unreachable
	// err `_`).
	target := w.cfg.BaseURL + "/api/v1/resources/" + url.PathEscape(resourceID)
	req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, target, nil)
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("User-Agent", "instanode-flow-synthetic/1")

	resp, err := w.httpCli.Do(req)
	if err != nil {
		w.recordReap(ctx, flow, reapOutcomeLeaked, resourceID, runID, "http_error: "+err.Error())
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.ReadAll(io.LimitReader(resp.Body, 1024))

	// 200 (deleted) or 204 (no content) or 404 (already gone) all mean "not
	// leaked". Anything else is a failed reap → leaked.
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		w.recordReap(ctx, flow, reapOutcomeReaped, resourceID, runID, "")
		return true
	}
	w.recordReap(ctx, flow, reapOutcomeLeaked, resourceID, runID, fmt.Sprintf("delete status=%d", resp.StatusCode))
	return false
}

// recordReap emits the reap counter + (the cleanup ledger) audit_log row. A
// leaked outcome also fires an ERROR slog line so the leak is visible even if
// the metric path is down.
func (w *FlowSyntheticWorker) recordReap(ctx context.Context, flow, outcome, resourceID, runID, reason string) {
	if w.metrics != nil {
		w.metrics.IncReaped(flow, outcome)
	}
	if outcome == reapOutcomeLeaked {
		slog.Error("flow_synthetic_leaked", "flow", flow, "resource_id", resourceID, "run_id", runID, "reason", reason)
	}
	if w.db == nil {
		return
	}
	meta := map[string]any{"flow": flow, "outcome": outcome, "resource_id": resourceID, "run_id": runID, "reason": reason}
	metaBytes, _ := json.Marshal(meta)
	summary := fmt.Sprintf("synthetic reap flow=%s outcome=%s resource=%s", flow, outcome, resourceID)
	if _, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES ($1, $2, $3, $4, $5)
	`, flowSyntheticTeamID, flowSyntheticActor, auditKindFlowReaped, summary, metaBytes); err != nil {
		slog.Warn("jobs.flow_synthetic.reap_audit_insert_failed", "flow", flow, "error", err)
	}
}

// reapOrphans is the reaper backstop (plan §1.4 step 4): any synthetic resource
// older than flowSyntheticOrphanMaxAge that the inline reap missed (runner crash
// mid-leg) is swept here. It deletes only resources owned by the synthetic team
// (so it can never touch a real customer), via the same soft-delete the api's
// deprovision uses (status='deleted', the current schema has no deleted_at
// column — see resource_heartbeat.go). The worker has no session bearer for an
// orphan, and the backstop's job is to stop accumulation; the inline reap is the
// primary path. Each sweep writes the cleanup ledger. No-op when db is nil.
func (w *FlowSyntheticWorker) reapOrphans(ctx context.Context) {
	if w.db == nil {
		return
	}
	rows, err := w.db.QueryContext(ctx, `
		SELECT id::text
		  FROM resources
		 WHERE team_id = $1
		   AND status = 'active'
		   AND created_at < now() - $2::interval
		 LIMIT 50
	`, flowSyntheticTeamID, fmt.Sprintf("%d seconds", int(flowSyntheticOrphanMaxAge.Seconds())))
	if err != nil {
		slog.Warn("jobs.flow_synthetic.orphan_query_failed", "error", err)
		return
	}
	defer func() { _ = rows.Close() }()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			slog.Warn("jobs.flow_synthetic.orphan_scan_failed", "error", err)
			continue
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		slog.Warn("jobs.flow_synthetic.orphan_rows_err", "error", err)
		return
	}

	for _, id := range ids {
		if _, err := w.db.ExecContext(ctx,
			`UPDATE resources SET status = 'deleted' WHERE id = $1 AND team_id = $2`,
			id, flowSyntheticTeamID,
		); err != nil {
			w.recordReap(ctx, "orphan_sweep", reapOutcomeLeaked, id, "", "orphan_update: "+err.Error())
			continue
		}
		w.recordReap(ctx, "orphan_sweep", reapOutcomeReaped, id, "", "backstop sweep (age > max)")
	}
}

// ─── Seed + mint ────────────────────────────────────────────────────────────

// ensureSyntheticTeam idempotently upserts the durable synthetic team + primary
// user (cohort-tagged) and elevates the team to the configured tier via the
// same UpgradeTeamAllTiers logic the Razorpay webhook uses. Returns true when
// the team + user are present after the call. Failure is non-fatal (the anon
// healthz flow needs no team) — returns false and the authed flows degrade.
func (w *FlowSyntheticWorker) ensureSyntheticTeam(ctx context.Context) bool {
	if w.db == nil {
		return false
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Warn("jobs.flow_synthetic.seed_begin_failed", "error", err)
		return false
	}
	defer func() { _ = tx.Rollback() }()

	// Team — idempotent. is_test_cohort=true so every team-iterating background
	// job no-ops for it (test_cohort.go guards). plan_tier set on conflict so a
	// tier change in config re-elevates on the next tick.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO teams (id, name, plan_tier, status, is_test_cohort)
		VALUES ($1, $2, $3, 'active', true)
		ON CONFLICT (id) DO UPDATE SET plan_tier = EXCLUDED.plan_tier, is_test_cohort = true
	`, flowSyntheticTeamID, flowSyntheticTeamName, w.cfg.Tier); err != nil {
		slog.Warn("jobs.flow_synthetic.seed_team_failed", "error", err)
		return false
	}

	// Primary user — idempotent. /auth/me looks up the user by the uid claim, so
	// the row must exist for the auth_me flow to return 200 + email.
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO users (id, team_id, email, is_primary)
		VALUES ($1, $2, $3, true)
		ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email
	`, flowSyntheticUserID, flowSyntheticTeamID, w.cfg.Email); err != nil {
		slog.Warn("jobs.flow_synthetic.seed_user_failed", "error", err)
		return false
	}

	// Elevate all the team's active resources to the seeded tier — mirrors the
	// webhook's UpgradeTeamAllTiers so resource-tier elevation is exercised
	// identically to prod. Inline (not a cross-module import) — same TECH-DEBT
	// note as billing_reconciler.upgradeTeamTiers.
	if _, err := tx.ExecContext(ctx, `
		UPDATE resources
		   SET tier = $1, expires_at = NULL
		 WHERE team_id = $2
		   AND status IN ('active', 'paused')
		   AND (expires_at IS NULL OR expires_at > now())
	`, w.cfg.Tier, flowSyntheticTeamID); err != nil {
		slog.Warn("jobs.flow_synthetic.seed_elevate_failed", "error", err)
		return false
	}

	if err := tx.Commit(); err != nil {
		slog.Warn("jobs.flow_synthetic.seed_commit_failed", "error", err)
		return false
	}
	return true
}

// mintSessionJWT signs a short-lived HS256 session JWT for the synthetic team,
// matching the claims auth.go issues + makeSessionJWT in the api e2e suite:
// {uid, tid, email, jti, iat, exp}. Hand-rolled HMAC-SHA256 (same posture as
// signMagicLinkResendJWT) so the worker stays dependency-light. The Brevo-free
// auth path (plan §1.1). Errors when JWT_SECRET is unset.
func (w *FlowSyntheticWorker) mintSessionJWT() (string, error) {
	if w.cfg.JWTSecret == "" {
		return "", errors.New("JWT_SECRET unset — cannot mint synthetic session JWT")
	}
	header := flowSyntheticB64(`{"alg":"HS256","typ":"JWT"}`)
	now := time.Now().UTC().Unix()
	claims := map[string]any{
		"uid":   flowSyntheticUserID.String(),
		"tid":   flowSyntheticTeamID.String(),
		"email": w.cfg.Email,
		"jti":   uuid.NewString(),
		"iat":   now,
		"exp":   now + int64(flowSyntheticSessionMaxAge.Seconds()),
	}
	// json.Marshal on a map of strings/ints can not return an error (no
	// MarshalJSON method, no unmappable types) — skip the defensive branch to
	// keep the patch-coverage gate at 100% (same posture as
	// auth_probe.legEmailStart's `_ = json.Marshal(...)`).
	claimsJSON, _ := json.Marshal(claims)
	body := header + "." + flowSyntheticB64(string(claimsJSON))
	mac := hmac.New(sha256.New, []byte(w.cfg.JWTSecret))
	mac.Write([]byte(body))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return body + "." + sig, nil
}

// flowSyntheticB64 is the unpadded URL-safe base64 encoding JWTs use. Named to
// avoid collisions with the identically-shaped helpers in
// magic_link_reconciler.go / payment_grace_terminator.go.
func flowSyntheticB64(s string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}

// fetchCommitID reads GET /healthz .commit_id so each event ties to the deploy
// under test (rule 14/15). Best-effort: a fetch failure returns "" — the events
// still record, just without the deploy correlation. Decoupled from the
// flowHealthz assertion (which alerts on a missing commit_id) so a healthz
// outage doesn't double-count.
func (w *FlowSyntheticWorker) fetchCommitID(ctx context.Context) string {
	target := w.cfg.BaseURL + "/healthz"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("User-Agent", "instanode-flow-synthetic/1")
	resp, err := w.httpCli.Do(req)
	if err != nil {
		return ""
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var parsed struct {
		CommitID string `json:"commit_id"`
	}
	_ = json.Unmarshal(body, &parsed)
	return parsed.CommitID
}

// splitFlowSyntheticDisabled splits the comma-separated FLOW_SYNTHETIC_DISABLED
// env value into a trimmed, non-empty flow-id slice for FlowSyntheticConfig.
// An empty input yields nil (no flows disabled). Exported-shape helper kept here
// (not in config) so the flow-id vocabulary stays in one file.
func splitFlowSyntheticDisabled(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// SplitFlowSyntheticDisabledForTest is an exported test seam over the unexported
// splitFlowSyntheticDisabled so the external _test package can assert the
// comma-list parsing without a DB or HTTP round-trip.
func SplitFlowSyntheticDisabledForTest(raw string) []string {
	return splitFlowSyntheticDisabled(raw)
}

// FlowSyntheticNoRedirectForTest is an exported test seam over the default
// client's CheckRedirect hook so the external _test package can assert the
// refuse-redirect contract directly (the closure is otherwise unreachable in a
// hermetic test that never follows a real 302).
func FlowSyntheticNoRedirectForTest() error {
	return flowSyntheticNoRedirect(nil, nil)
}

// RunPanickingFlowForTest exercises runFlow's recover() panic boundary with a
// flow fn that panics, asserting the boundary converts the panic into a
// result=fail rather than crashing the sweep. Uses an UNKNOWN flow id so the
// same call also covers flowActorForFlow's default (ActorUnknown) arm. Exported
// test seam — runFlow is unexported and takes an internal fn type, so the _test
// package cannot reach the boundary any other way. Returns the recorded result.
func (w *FlowSyntheticWorker) RunPanickingFlowForTest() string {
	return w.runFlow(context.Background(), "run", "commit", "unknown_flow_for_test", func(context.Context) flowSyntheticResult {
		panic("synthetic panic for the recover() boundary test")
	})
}

// ReapNilDBForTest exercises the db==nil short-circuit in recordReap +
// emitFlowTestFailed (fail-open: metric/slog still fire, the audit insert is
// skipped). Reachable only when the worker holds a nil db, which the Work path
// never combines with a reap — so this seam covers those guard returns directly.
func (w *FlowSyntheticWorker) ReapNilDBForTest() {
	w.recordReap(context.Background(), flowProvisionReap, reapOutcomeReaped, "rid", "run", "")
	w.emitFlowTestFailed(context.Background(), "run", flowSyntheticResult{flow: flowHealthz, actor: flowActorAnon, tier: "anonymous", result: flowResultFail, reason: "seam"})
}

// ValidateFlowSyntheticBaseURL is a startup-time sanity check for the
// FLOW_SYNTHETIC_BASE_URL env var. Returns an error iff set but unparseable; an
// empty value is accepted (Defaults() fills the prod host). Exported so main.go
// can fail-fast on a typo rather than discovering the bad URL on the first tick.
func ValidateFlowSyntheticBaseURL(raw string) error {
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("FLOW_SYNTHETIC_BASE_URL parse: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("FLOW_SYNTHETIC_BASE_URL must be http(s)")
	}
	if u.Host == "" {
		return errors.New("FLOW_SYNTHETIC_BASE_URL missing host")
	}
	return nil
}
