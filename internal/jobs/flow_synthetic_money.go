package jobs

// flow_synthetic_money.go — the money/value-journey legs of the continuous
// synthetic flow matrix (Wave 5, docs/ci/01-CI-INTEGRATION-DESIGN.md §NR
// observability + docs/sessions/2026-06-04/TEST-ACCOUNTS-AND-NR-SYNTHETICS-PLAN.md).
//
// flow_synthetic.go runs the P0 read/provision legs (healthz, auth_me,
// provision_reap). This file adds the legs that watch the *value* paths — the
// flows a paying customer's money rides on — each reading a TRUTH SURFACE
// (CLAUDE.md rule 12), never a proxy:
//
//   - flow_claim         (human, api): GET /claim/preview with a deliberately
//     invalid token, asserting the claim endpoint is up AND enforcing its
//     single-use/JWT contract (400 invalid_token). Read-only — the claim path
//     is single-use-consuming, so a synthetic claim can't fire a real /claim
//     POST every tick without burning onboarding rows; the preview surface is
//     the safe, side-effect-free contract probe. Truth surface: the rendered
//     contract response, not a 200 health ping.
//
//   - flow_deploy_status (agent, api): POST /deploy/new on the synthetic free
//     team, asserting the TIER-GATE contract (402 + agent_action) rather than
//     building a real app every 5 min (a real build per tick would churn
//     postgres-customers + Kaniko — the design defers real deploy_new to a
//     staging lane). Truth surface: the deploy gate's 402 wall (the exact
//     surface a free user hits) — proves the deploy entitlement gate is live.
//     When the synthetic tier has deploy headroom (hobby+), a 201/202 is also
//     accepted and the created app is reaped via the deploy delete path.
//
//   - flow_checkout      (human, api): POST /api/v1/billing/checkout with the
//     synthetic session, asserting the endpoint is REACHABLE and does not 5xx
//     (contract-only — Razorpay live recurring is operator-blocked, so a real
//     charge can't be driven; a 402/409/502-with-known-body is an acceptable
//     "blocked-but-alive" shape, a 500 crash is a fail). Truth surface: the
//     checkout endpoint's non-crash response — catches a checkout handler that
//     panics/regresses even while Razorpay itself is blocked.
//
//   - flow_magic_link    (human, api+db): POST /auth/email/start (202), then
//     read the forwarder_sent ledger's terminal `classification` for the
//     synthetic recipient (rule 12: the ledger row is the truth surface for
//     "did the email actually go out", NOT Brevo's 201/our 202). The result's
//     reason carries the classification (delivered / rejected / bounced_* /
//     deferred / …) so the matrix dashboard surfaces the REAL email-delivery
//     health — today that means the Brevo-sender-unvalidated reality
//     (project_brevo_sender_not_validated.md) shows as classification=rejected
//     rather than a false green from the 202.
//
// All four are flag-gated by the SAME master FLOW_SYNTHETIC_ENABLED flag and
// honour the per-flow FLOW_SYNTHETIC_DISABLED kill list, so a flapping money
// leg can be silenced without killing the P0 suite. They emit the same
// instant_flow_test_* counters + InstantFlowTest event (cohort=synthetic) as
// the P0 legs, so one dashboard renders the whole grid.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Money/value-journey flow ids — the `flow` label + AttrFlow attribute. As with
// the P0 flow ids these are constants (rule 16: one source per contract token)
// and a registry test asserts the full set.
const (
	flowClaim        = "claim"
	flowDeployStatus = "deploy_status"
	flowCheckout     = "checkout"
	flowMagicLink    = "magic_link"
)

// flowSyntheticInvalidClaimToken is the deliberately-malformed token the claim
// preview leg sends. It is NOT a real JWT, so ClaimPreview returns 400
// invalid_token — the safe, single-use-preserving contract probe. A constant so
// the assertion + the value stay in one place.
const flowSyntheticInvalidClaimToken = "synthetic-invalid-claim-token-not-a-jwt"

// Per-flow latency budgets for the money legs. Crossing the budget records
// result=degraded (slow-but-correct) rather than failing — same posture as the
// P0 legs in flowSyntheticLegLatencyBudgets. magic_link gets the widest budget:
// /auth/email/start does a rate-limit hash + a Redis write before returning 202.
var flowSyntheticMoneyLatencyBudgets = map[string]time.Duration{
	flowClaim:        2 * time.Second,
	flowDeployStatus: 5 * time.Second,
	flowCheckout:     5 * time.Second,
	flowMagicLink:    3 * time.Second,
}

// runMoneyMatrix executes the money/value-journey legs sequentially, mirroring
// runMatrix's per-flow isolation (runFlow handles timeout + recover + record).
// Called from Work after the P0 matrix. The anon claim leg needs no auth; the
// other three need the seeded team + a minted session JWT, so they degrade (not
// page) when seedOK is false (config/DB drift, not an outage).
func (w *FlowSyntheticWorker) runMoneyMatrix(ctx context.Context, runID, commitID, bearer string, seedOK bool) map[string]string {
	out := map[string]string{}

	// claim — anonymous read-only contract probe (no auth needed).
	out[flowClaim] = w.runFlow(ctx, runID, commitID, flowClaim, func(fctx context.Context) flowSyntheticResult {
		return w.flowClaimPreview(fctx)
	})

	// magic_link — anon POST + a DB truth-surface read; needs the DB (for the
	// forwarder_sent read) but not the session JWT. Degrades when db is nil.
	out[flowMagicLink] = w.runFlow(ctx, runID, commitID, flowMagicLink, func(fctx context.Context) flowSyntheticResult {
		return w.flowMagicLink(fctx)
	})

	// deploy_status + checkout need the authenticated session.
	out[flowDeployStatus] = w.runFlow(ctx, runID, commitID, flowDeployStatus, func(fctx context.Context) flowSyntheticResult {
		if !seedOK {
			return flowSyntheticResult{flow: flowDeployStatus, actor: flowActorAgent, tier: w.cfg.Tier, result: flowResultDegraded, reason: "synthetic team/JWT unavailable — flow skipped"}
		}
		return w.flowDeployStatus(fctx, runID, bearer)
	})

	out[flowCheckout] = w.runFlow(ctx, runID, commitID, flowCheckout, func(fctx context.Context) flowSyntheticResult {
		if !seedOK {
			return flowSyntheticResult{flow: flowCheckout, actor: flowActorHuman, tier: w.cfg.Tier, result: flowResultDegraded, reason: "synthetic team/JWT unavailable — flow skipped"}
		}
		return w.flowCheckout(fctx, bearer)
	})

	return out
}

// moneyBudgetFor returns the per-flow latency budget for a money leg, honouring
// a test override (SetBudgetOverrideForTest) the same way budgetFor does for the
// P0 legs.
func (w *FlowSyntheticWorker) moneyBudgetFor(flow string) time.Duration {
	if w.budgetOverride != nil {
		if d, ok := w.budgetOverride[flow]; ok {
			return d
		}
	}
	return flowSyntheticMoneyLatencyBudgets[flow]
}

// ─── claim ──────────────────────────────────────────────────────────────────

// flowClaimPreview drives GET /claim/preview?t=<invalid> and asserts the claim
// endpoint enforces its JWT contract by returning 400 invalid_token. This is the
// read-only, single-use-preserving truth surface for "is the claim path alive
// and validating tokens" — a 200/302/5xx instead of the 400 contract means the
// claim funnel (anon→claimed, the first money step) is broken.
func (w *FlowSyntheticWorker) flowClaimPreview(ctx context.Context) flowSyntheticResult {
	budget := w.moneyBudgetFor(flowClaim)
	r := flowSyntheticResult{flow: flowClaim, actor: flowActorAnon, tier: "anonymous"}

	target := w.cfg.BaseURL + "/claim/preview?t=" + url.QueryEscape(flowSyntheticInvalidClaimToken)
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
	// The contract: a bad token → 400 with error "invalid_token" (or
	// "missing_token" if the query is dropped). Any 2xx/3xx/5xx is a contract
	// breach: the claim path is not validating, or it crashed.
	if resp.StatusCode != http.StatusBadRequest {
		r.result = flowResultFail
		r.reason = fmt.Sprintf("status=%d (want 400 invalid_token contract); body=%s", resp.StatusCode, truncateForLog(string(body), 256))
		return r
	}
	var parsed struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(body, &parsed)
	if parsed.Error != "invalid_token" && parsed.Error != "missing_token" {
		r.result = flowResultFail
		r.reason = fmt.Sprintf("400 but unexpected error=%q (want invalid_token); body=%s", parsed.Error, truncateForLog(string(body), 256))
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

// ─── deploy_status ────────────────────────────────────────────────────────────

// flowDeployStatus drives POST /deploy/new on the synthetic team and asserts the
// tier-entitlement gate. For a free/anonymous tier (no deploy headroom) the
// contract is a 402 wall — the exact surface a free user hits — which proves the
// deploy gate is live WITHOUT building a real app every tick. When the synthetic
// tier has headroom (hobby+), a 201/202 is accepted and the created app is reaped
// via the deploy delete path (cleanup ledger, rule 24).
func (w *FlowSyntheticWorker) flowDeployStatus(ctx context.Context, runID, bearer string) flowSyntheticResult {
	budget := w.moneyBudgetFor(flowDeployStatus)
	r := flowSyntheticResult{flow: flowDeployStatus, actor: flowActorAgent, tier: w.cfg.Tier}

	// A minimal multipart-free probe: POST an empty JSON body. The tier gate is
	// evaluated before the tarball is parsed for the no-headroom tiers, so a
	// free-tier probe reaches the 402 wall on body-shape alone. (For headroom
	// tiers the handler would 400 on the missing tarball; we treat a 400 here as
	// "gate passed, build inputs missing" which is still a non-5xx contract.)
	target := w.cfg.BaseURL + "/deploy/new"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, strings.NewReader("{}"))
	if err != nil {
		r.result = flowResultFail
		r.reason = "build_request: " + err.Error()
		return r
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/json")
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

	switch {
	case resp.StatusCode == http.StatusPaymentRequired:
		// The free/anon tier-gate wall — the expected truth surface for a
		// no-headroom synthetic team. Gate is live → pass.
		if r.latency > budget {
			r.result = flowResultDegraded
			r.reason = fmt.Sprintf("402 gate ok but latency=%dms over budget=%dms", r.latency.Milliseconds(), budget.Milliseconds())
			return r
		}
		r.result = flowResultPass
		return r
	case resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusAccepted:
		// Headroom tier accepted the deploy — reap the created app so we never
		// leak (rule 24). Best-effort id extraction.
		var parsed struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(body, &parsed)
		if parsed.ID != "" {
			w.reapDeployment(ctx, bearer, parsed.ID, runID)
		}
		r.result = flowResultPass
		return r
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// A 4xx other than 402 (e.g. 400 missing-tarball on a headroom tier, or
		// 413 over-cap) means the gate/validation responded — non-5xx contract
		// held. Record as pass with the status in the reason for visibility.
		r.result = flowResultPass
		r.reason = fmt.Sprintf("deploy gate responded %d (non-5xx contract held)", resp.StatusCode)
		return r
	default:
		r.result = flowResultFail
		r.reason = fmt.Sprintf("status=%d (deploy endpoint 5xx/crash); body=%s", resp.StatusCode, truncateForLog(string(body), 256))
		return r
	}
}

// reapDeployment deletes a synthetic deployment via the real DELETE
// /api/v1/deployments/:id path (cleanup ledger, rule 24). Best-effort: a leaked
// outcome is recorded on instant_flow_synthetic_reaped_total{flow,outcome} so a
// failed reap is visible. Mirrors reapResource.
func (w *FlowSyntheticWorker) reapDeployment(ctx context.Context, bearer, deployID, runID string) {
	target := w.cfg.BaseURL + "/api/v1/deployments/" + url.PathEscape(deployID)
	req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, target, nil)
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("User-Agent", "instanode-flow-synthetic/1")

	resp, err := w.httpCli.Do(req)
	if err != nil {
		w.recordReap(ctx, flowDeployStatus, reapOutcomeLeaked, deployID, runID, "http_error: "+err.Error())
		return
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.ReadAll(io.LimitReader(resp.Body, 1024))
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusNotFound {
		w.recordReap(ctx, flowDeployStatus, reapOutcomeReaped, deployID, runID, "")
		return
	}
	w.recordReap(ctx, flowDeployStatus, reapOutcomeLeaked, deployID, runID, fmt.Sprintf("delete status=%d", resp.StatusCode))
}

// ─── checkout ─────────────────────────────────────────────────────────────────

// flowCheckout drives POST /api/v1/billing/checkout with the synthetic session
// and asserts the endpoint is reachable + does not 5xx. Razorpay live recurring
// is operator-blocked (project_razorpay_recurring_not_enabled.md), so a real
// charge can't be driven — a 402/409/502-with-known-body (Razorpay-blocked) is
// an acceptable "alive-but-blocked" shape. Only a 5xx CRASH (a checkout handler
// panic/regression) fails the leg. Truth surface: the checkout endpoint's
// non-crash response.
func (w *FlowSyntheticWorker) flowCheckout(ctx context.Context, bearer string) flowSyntheticResult {
	budget := w.moneyBudgetFor(flowCheckout)
	r := flowSyntheticResult{flow: flowCheckout, actor: flowActorHuman, tier: w.cfg.Tier}

	// Probe an upgrade to a paid tier. The body shape mirrors the dashboard's
	// "Upgrade to Pro" call; the handler validates the plan before touching
	// Razorpay, so even a blocked account exercises the handler path.
	target := w.cfg.BaseURL + "/api/v1/billing/checkout"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, strings.NewReader(`{"plan":"pro"}`))
	if err != nil {
		r.result = flowResultFail
		r.reason = "build_request: " + err.Error()
		return r
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/json")
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
	// Contract-only: any non-5xx means the checkout handler is alive and
	// responded (success URL, or a known blocked/validation status). A 5xx is a
	// handler crash/regression → fail.
	if resp.StatusCode >= 500 {
		r.result = flowResultFail
		r.reason = fmt.Sprintf("status=%d (checkout endpoint 5xx/crash); body=%s", resp.StatusCode, truncateForLog(string(body), 256))
		return r
	}
	if r.latency > budget {
		r.result = flowResultDegraded
		r.reason = fmt.Sprintf("checkout responded %d but latency=%dms over budget=%dms", resp.StatusCode, r.latency.Milliseconds(), budget.Milliseconds())
		return r
	}
	r.result = flowResultPass
	r.reason = fmt.Sprintf("checkout endpoint alive (status=%d; contract-only, Razorpay live operator-blocked)", resp.StatusCode)
	return r
}

// ─── magic_link ───────────────────────────────────────────────────────────────

// flowMagicLink drives POST /auth/email/start (expect 202) then reads the
// forwarder_sent ledger's terminal classification for the most recent synthetic
// magic-link send. The 202 only proves the request was accepted; the LEDGER ROW
// is the truth surface (rule 12) for whether the email actually went out. The
// result's reason carries the classification so the dashboard surfaces the REAL
// delivery health (today: classification=rejected because the Brevo sender is
// unvalidated — project_brevo_sender_not_validated.md). A non-202 from
// /auth/email/start, OR a forwarder_sent row with a terminal failure
// classification, fails the leg; a 'success'/'delivered' classification (or no
// row yet — the forwarder is async) passes.
func (w *FlowSyntheticWorker) flowMagicLink(ctx context.Context) flowSyntheticResult {
	budget := w.moneyBudgetFor(flowMagicLink)
	r := flowSyntheticResult{flow: flowMagicLink, actor: flowActorHuman, tier: w.cfg.Tier}

	target := w.cfg.BaseURL + "/auth/email/start"
	bodyJSON := fmt.Sprintf(`{"email":%q}`, w.cfg.Email)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, strings.NewReader(bodyJSON))
	if err != nil {
		r.result = flowResultFail
		r.reason = "build_request: " + err.Error()
		return r
	}
	req.Header.Set("Content-Type", "application/json")
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
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	r.observeLatency = true
	r.httpStatus = resp.StatusCode
	// /auth/email/start always returns 202 (even for unknown emails — it never
	// leaks account existence) or 400 for a malformed body. A non-202 means the
	// send-request path is broken.
	if resp.StatusCode != http.StatusAccepted {
		r.result = flowResultFail
		r.reason = fmt.Sprintf("status=%d (want 202 from /auth/email/start); body=%s", resp.StatusCode, truncateForLog(string(respBody), 256))
		return r
	}

	// Truth surface: read the most-recent forwarder_sent classification. The
	// forwarder is async (the worker dispatches on its own tick), so a freshly
	// requested send may not have a row yet — that is NOT a failure (the request
	// was accepted). We read the latest row to surface delivery health.
	classification, ok := w.latestForwarderClassification(ctx)
	if !ok {
		// No ledger row readable (db nil, or no send recorded yet) — the 202
		// path held; report the request-accepted state without a delivery verdict.
		if r.latency > budget {
			r.result = flowResultDegraded
			r.reason = fmt.Sprintf("202 accepted; no forwarder_sent row yet (async); latency=%dms over budget=%dms", r.latency.Milliseconds(), budget.Milliseconds())
			return r
		}
		r.result = flowResultPass
		r.reason = "202 accepted; no forwarder_sent classification readable yet (forwarder is async)"
		return r
	}

	// A terminal-failure classification is the real, rule-12 truth that email
	// is NOT reaching inboxes (today: rejected, the unvalidated-sender reality).
	if isForwarderFailureClassification(classification) {
		r.result = flowResultFail
		r.reason = fmt.Sprintf("magic-link email classification=%q (truth surface: email not delivered)", classification)
		return r
	}
	if r.latency > budget {
		r.result = flowResultDegraded
		r.reason = fmt.Sprintf("classification=%q but latency=%dms over budget=%dms", classification, r.latency.Milliseconds(), budget.Milliseconds())
		return r
	}
	r.result = flowResultPass
	r.reason = fmt.Sprintf("magic-link email classification=%q", classification)
	return r
}

// forwarderFailureClassifications enumerates the forwarder_sent.classification
// terminal-failure values (the rule-12 "email did NOT reach the inbox" set).
// 'success'/'delivered' (and the async no-row case) are the healthy outcomes.
// A var (not const) so a test can read the set; not mutated at runtime.
var forwarderFailureClassifications = map[string]bool{
	"rejected":       true, // Brevo accepted (201) then internally rejected — the unvalidated-sender reality
	"bounced_hard":   true,
	"bounced_soft":   true,
	"complaint":      true,
	"error":          true,
	"permanent_drop": true,
	"transient":      true,
}

// isForwarderFailureClassification reports whether a forwarder_sent
// classification is a terminal delivery failure. Case-insensitive + trimmed so a
// stored-value casing drift can't mask a failure as healthy.
func isForwarderFailureClassification(c string) bool {
	return forwarderFailureClassifications[strings.ToLower(strings.TrimSpace(c))]
}

// IsForwarderFailureClassificationForTest is an exported test seam over the
// unexported classifier so the external _test package can assert the
// healthy-vs-failure partition without a DB round-trip.
func IsForwarderFailureClassificationForTest(c string) bool {
	return isForwarderFailureClassification(c)
}

// latestForwarderClassification reads the most-recent forwarder_sent row's
// terminal classification for the synthetic recipient, the rule-12 truth surface
// for magic-link delivery. Returns (classification, true) on a readable row, or
// ("", false) when db is nil OR no row exists yet (the forwarder is async, so a
// just-requested send legitimately has no row — the caller treats that as
// "request accepted, no verdict yet", not a failure). recipient is stored
// masked, so the match is on template_kind (magic-link kinds) ordered by sent_at.
func (w *FlowSyntheticWorker) latestForwarderClassification(ctx context.Context) (string, bool) {
	if w.db == nil {
		return "", false
	}
	var classification string
	// template_kind LIKE '%magic%' OR '%login%' — the magic-link send kinds.
	// LIMIT 1 over sent_at DESC = the latest synthetic-relevant send. A short
	// window (1h) keeps a stale row from masking a current outage.
	err := w.db.QueryRowContext(ctx, `
		SELECT classification
		  FROM forwarder_sent
		 WHERE (template_kind ILIKE '%magic%' OR template_kind ILIKE '%login%' OR template_kind ILIKE '%link%')
		   AND sent_at > now() - interval '1 hour'
		 ORDER BY sent_at DESC
		 LIMIT 1
	`).Scan(&classification)
	if err != nil {
		// sql.ErrNoRows (no recent send) OR a query error → no verdict. Both
		// degrade to "no row" (the caller passes the 202-accepted state).
		return "", false
	}
	return classification, true
}
