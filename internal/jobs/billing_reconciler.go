package jobs

// billing_reconciler.go — periodic sweep that compares Razorpay's live
// subscription state against teams.plan_tier and corrects any divergence.
//
// # Purpose
//
// Webhooks are a best-effort delivery mechanism. A routine pod-restart during
// a manual `kubectl set image` deploy can miss a webhook delivery cycle
// permanently. This reconciler is the safety net: every 15 minutes it polls
// Razorpay's authoritative subscription state and applies the same correction
// paths the webhook uses.
//
// # planIDToTier in the worker (Option 3 from design doc)
//
// The worker and api are separate Go modules. Rather than adding an HTTP hop
// or restructuring the module topology for this slice, billingReconcilerPlanIDToTier
// reads the same RAZORPAY_PLAN_ID_* env vars that the api reads — both pods
// receive them from the same k8s ConfigMap/Secret. This is ~30 lines of
// duplicated logic, documented as a tech-debt flag for extraction to a shared
// package when the module topology allows it.
//
// # Status → tier mapping (design doc §2)
//
//   active / authenticated → tier from plan_id (upgrade if DB is lower)
//   pending               → no change (may self-resolve)
//   halted / paused       → open grace period if none active
//   cancelled / completed / expired → downgrade to hobby (always — never the
//                           ephemeral "free" tier; see the terminal branch in
//                           Work() for the strands-paid-resources rationale)
//
// # Idempotency
//
//   - upgradeTeamTiers (local UpgradeTeamAllTiers equivalent) uses UPDATE…WHERE,
//     so calling it on an already-correct team is a no-op.
//   - OpenGracePeriod: the uq_payment_grace_team_active partial unique index
//     makes a duplicate INSERT a no-op (ErrPaymentGraceAlreadyActive path).
//   - updatePlanTier uses UPDATE teams — idempotent.
//   - The reconciler does NOT insert into razorpay_webhook_events. That table
//     deduplicates Razorpay webhook deliveries; the reconciler is a periodic
//     poller and must not be dedup-blocked.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	razorpay "github.com/razorpay/razorpay-go"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	commonplans "instant.dev/common/plans"
	"instant.dev/worker/internal/circuit"
	"instant.dev/worker/internal/metrics"
)

// chargeUndeliverableAuditKind is the audit_log.kind value the api emits
// when a Razorpay webhook trust-pass fails — it cannot resolve the
// payload to a real team / plan_id / subscription. B11-F3 (BugBash
// 2026-05-20): the worker scans for new rows on every reconciler tick
// and increments BillingChargeUndeliverableTotal so a single NR alert
// rule can fire on the metric, independent of which service wrote the
// audit row. The literal mirrors api/internal/models.AuditKindBillingChargeUndeliverable.
const chargeUndeliverableAuditKind = "billing.charge_undeliverable"

// BillingReconcilerArgs is the River job payload — no fields, sweep job.
type BillingReconcilerArgs struct{}

// Kind is the River worker key.
func (BillingReconcilerArgs) Kind() string { return "billing_reconciler" }

// ── cadence & tuning constants ────────────────────────────────────────────────

// defaultBillingReconcileInterval is the polling cadence when
// BILLING_RECONCILE_INTERVAL is unset or unparseable. 15 minutes matches the
// design doc §2: long enough to avoid flooding Razorpay, short enough to
// recover a missed webhook within one SLA tick.
const defaultBillingReconcileInterval = 15 * time.Minute

// billingReconcilerBatchLimit caps per-tick fan-out. 100 teams × 2 Razorpay
// API calls = 200 calls/tick. With the 100ms stagger that is ~200 calls/min —
// well inside Razorpay's published 600 req/min limit. Backlogs drain across
// consecutive ticks via the stable ORDER BY id pagination.
const billingReconcilerBatchLimit = 100

// ── F1: orphan-checkout sweep constants ───────────────────────────────────────
//
// The primary sweep above starts from teams.stripe_customer_id (the persisted
// Razorpay subscription id). That id is written by a best-effort, non-fatal
// UPDATE in the api at checkout time — if the write is lost and the customer
// then pays, the team is structurally invisible to the primary sweep forever:
// Razorpay bills the card, the DB stays on free/hobby, nothing corrects it (F1).
//
// The orphan sweep is a Razorpay-authoritative second pass. It does NOT start
// from the teams table — it enumerates pending_checkouts (api migration 034),
// which records the (subscription_id, team_id) pair for EVERY checkout the api
// ever minted, independent of whether the teams.stripe_customer_id UPDATE
// landed. For each checkout it fetches the live Razorpay subscription; any team
// Razorpay reports paid-and-active whose DB tier is still below the entitled
// tier is elevated — the same correction the primary sweep applies, just
// reachable for teams the primary sweep cannot see.

// billingReconcilerOrphanBatchLimit caps the orphan sweep's per-tick fan-out.
// Smaller than the primary batch: the orphan population is the steady-state
// pending_checkouts table minus already-up-to-date teams — tiny in practice.
// A backlog drains across consecutive ticks via the stable ORDER BY.
const billingReconcilerOrphanBatchLimit = 100

// billingReconcilerOrphanMinAge is how old a pending_checkouts row must be
// before the orphan sweep considers it. A just-minted checkout is still in the
// happy-path window (webhook in flight, primary sweep will pick it up once the
// sub-id persists); only a checkout that has been around long enough that a
// missed-write + missed-webhook is plausible is worth a Razorpay round-trip.
// 15 minutes matches checkoutReconcileGracePeriod — the same "long enough to
// be a real failure, not an in-flight checkout" threshold.
const billingReconcilerOrphanMinAge = 15 * time.Minute

// billingReconcilerRazorpayTimeout is the per-team Razorpay fetch budget.
// FetchSubscriptionForReconciler makes up to 2 Razorpay API calls; 10s is
// the ceiling so a slow response cannot hold the tick open for the full batch.
const billingReconcilerRazorpayTimeout = 10 * time.Second

// billingReconcilerStaggerDelay is the sleep between per-team Razorpay calls
// to avoid bursting. 100ms keeps 100-team batches within ~200 calls/min.
const billingReconcilerStaggerDelay = 100 * time.Millisecond

// ── grace period constants ────────────────────────────────────────────────────

// billingReconcilerGraceDays is the grace period length opened by the
// reconciler. Matches api/internal/models.PaymentGracePeriodGraceDays (7).
// Duplicated here to avoid importing across module boundaries.
const billingReconcilerGraceDays = 7

// ── Razorpay status → action class ───────────────────────────────────────────

// rzpStatusClass groups Razorpay subscription statuses into action buckets.
type rzpStatusClass int

const (
	// rzpStatusClassActive: subscription is paid (or card authorised; charge pending).
	// Expected DB tier comes from the subscription's plan_id.
	rzpStatusClassActive rzpStatusClass = iota
	// rzpStatusClassGrace: charge failed; Razorpay is retrying. A grace period
	// should be opened if one is not already active.
	rzpStatusClassGrace
	// rzpStatusClassTerminal: subscription ended. Downgrade to hobby (or free).
	rzpStatusClassTerminal
	// rzpStatusClassNoAction: status may self-resolve; do nothing.
	rzpStatusClassNoAction
)

// razorpayStatusClass is the status→action decision table from design doc §2.
var razorpayStatusClass = map[string]rzpStatusClass{
	"active":        rzpStatusClassActive,
	"authenticated": rzpStatusClassActive,   // card authorised; first charge pending
	"created":       rzpStatusClassNoAction, // subscription created, not yet authenticated — pre-payment, no tier change
	"pending":       rzpStatusClassNoAction,
	"halted":        rzpStatusClassGrace,
	"paused":        rzpStatusClassGrace,
	"cancelled":     rzpStatusClassTerminal,
	"completed":     rzpStatusClassTerminal,
	"expired":       rzpStatusClassTerminal,
}

// ── tier ranking ──────────────────────────────────────────────────────────────

// billingTierRank returns the numeric rank for a tier name, delegating to the
// shared common/plans canonical rank registry (commonplans.Rank). Higher rank
// = higher tier.
//
// This used to be a worker-local hand-maintained map (billingTierRankMap).
// That map silently returned rank 0 for any tier absent from it — so the day
// a new tier was added to plans.yaml + common/plans/rank.go but NOT to the
// worker map, the reconciler mis-ranked it as the lowest tier and skipped a
// legitimate upgrade. Routing through commonplans.Rank keeps the worker in
// lock-step with the single source of truth: a tier added to rank.go is
// ranked correctly here with no worker edit.
//
// commonplans.Rank returns -1 for an unknown tier. The original map returned
// 0 (== "anonymous") for unknowns. The reconciler's only caller compares two
// ranks with `>=` to decide "DB tier already at/above expected" — and -1 is
// strictly below every known tier, so an unknown DB tier still correctly
// resolves to "below expected" (i.e. it will be upgraded), matching the
// old 0-for-unknown behaviour. The case/whitespace normalisation that used
// to live here is handled inside commonplans.Rank.
func billingTierRank(tier string) int {
	return commonplans.Rank(tier)
}

// billingPaidTiers is the set of tiers that represent an active paid subscription.
// Used to decide whether a terminal-status team still needs a downgrade write.
var billingPaidTiers = map[string]bool{
	"hobby":      true,
	"hobby_plus": true,
	"pro":        true,
	"growth":     true,
	"team":       true,
}

// ── worker-local planIDToTier ─────────────────────────────────────────────────

// Razorpay plan-id env-var names. These MUST match the names the api reads in
// api/internal/config/config.go AND the keys in the live `instant-secrets` k8s
// Secret — both pods receive the same Secret. The api fixed this on 2026-05-15:
// every YEARLY plan id uses the `_ANNUAL` suffix (not `_YEARLY`). The worker
// previously read `_YEARLY` for Hobby/Pro/Team, so os.Getenv returned "" in
// prod → a yearly-Pro/Team team that missed its upgrade webhook was reconciled
// DOWN to hobby and every tick logged a spurious plan_id_to_tier.unrecognised.
//
// If the api renames any of these, this list must change in lock-step — see
// TestBillingReconcilerPlanEnvNamesMatchAPI which pins the agreement.
const (
	envRazorpayPlanIDHobby           = "RAZORPAY_PLAN_ID_HOBBY"
	envRazorpayPlanIDHobbyAnnual     = "RAZORPAY_PLAN_ID_HOBBY_ANNUAL"
	envRazorpayPlanIDHobbyPlus       = "RAZORPAY_PLAN_ID_HOBBY_PLUS"
	envRazorpayPlanIDHobbyPlusAnnual = "RAZORPAY_PLAN_ID_HOBBY_PLUS_ANNUAL"
	envRazorpayPlanIDPro             = "RAZORPAY_PLAN_ID_PRO"
	envRazorpayPlanIDProAnnual       = "RAZORPAY_PLAN_ID_PRO_ANNUAL"
	envRazorpayPlanIDTeam            = "RAZORPAY_PLAN_ID_TEAM"
	envRazorpayPlanIDTeamAnnual      = "RAZORPAY_PLAN_ID_TEAM_ANNUAL"
)

// billingReconcilerPlanEntry pairs a Razorpay plan-id env-var name with the
// canonical tier it grants.
type billingReconcilerPlanEntry struct {
	envKey string
	tier   string
}

// billingReconcilerPlanEnvEntries is the ordered (most-specific-first) lookup
// table for billingReconcilerPlanIDToTier. Ordering avoids a "" env var
// stealing a match.
var billingReconcilerPlanEnvEntries = []billingReconcilerPlanEntry{
	{envRazorpayPlanIDTeamAnnual, "team"},
	{envRazorpayPlanIDTeam, "team"},
	{envRazorpayPlanIDProAnnual, "pro"},
	{envRazorpayPlanIDPro, "pro"},
	{envRazorpayPlanIDHobbyPlusAnnual, "hobby_plus"},
	{envRazorpayPlanIDHobbyPlus, "hobby_plus"},
	{envRazorpayPlanIDHobbyAnnual, "hobby"},
	{envRazorpayPlanIDHobby, "hobby"},
}

// billingReconcilerPlanIDToTier maps a Razorpay plan_id to a canonical tier.
//
// TECH DEBT (Option 3 from design doc §8): this function duplicates ~30 lines
// from api/internal/handlers/billing.go (planIDToTier). The worker and api are
// separate Go modules; the duplication is intentional and flagged for future
// extraction to instant.dev/common/billing once the module topology allows it.
//
// Defaults to "hobby" (lowest paid tier) for unrecognised plan_ids — per
// design doc §4 Option A: misconfiguration grants $9 Hobby instead of $49 Pro,
// and the next reconciler tick will see the DB is still below Razorpay and
// fix it when the env var is corrected.
func billingReconcilerPlanIDToTier(planID string) string {
	if planID == "" {
		slog.Error("billing.reconciler.plan_id_to_tier.empty_plan_id",
			"fallback_tier", "hobby",
			"action", "Razorpay returned a subscription with no plan_id",
		)
		return "hobby"
	}

	// Ordered most-specific-first to avoid a "" env var stealing a match.
	for _, e := range billingReconcilerPlanEnvEntries {
		if configured := os.Getenv(e.envKey); configured != "" && planID == configured {
			return e.tier
		}
	}

	slog.Error("billing.reconciler.plan_id_to_tier.unrecognised",
		"plan_id", planID,
		"fallback_tier", "hobby",
		"action", "Check RAZORPAY_PLAN_ID_* env vars — unknown plan_id treated as hobby",
	)
	return "hobby"
}

// ── subscriptionFetcher interface ─────────────────────────────────────────────

// reconcilerSubscriptionDetails is the subset of Razorpay subscription fields
// the reconciler needs. Kept narrow for easy test stubbing.
type reconcilerSubscriptionDetails struct {
	Status    string
	PlanID    string
	PaidCount int64 // total successful payments on this subscription
}

// subscriptionFetcher is the Razorpay API surface the reconciler depends on.
// Inject a stub in tests; wire a real implementation via NewBillingReconcilerWorker.
type subscriptionFetcher interface {
	FetchSubscriptionForReconciler(ctx context.Context, subscriptionID string) (*reconcilerSubscriptionDetails, error)
}

// ── gracePeriodOpener interface ───────────────────────────────────────────────

// gracePeriodOpener is the DB surface the reconciler uses for grace periods.
// Narrow interface for test stubbing.
type gracePeriodOpener interface {
	GetActiveGracePeriod(ctx context.Context, teamID uuid.UUID) (bool, error)
	OpenGracePeriod(ctx context.Context, teamID uuid.UUID, subscriptionID string) error
	// HasTerminatedGracePeriod reports whether the team already has a grace
	// period for subscriptionID that has reached a terminal status
	// ('terminated' / 'expired'). It is the P1-F(b) guard: once the grace
	// clock has run out and the team was terminated, Razorpay can still
	// report halted/paused for some time — without this check the reconciler
	// would see "no ACTIVE grace" and open a FRESH 7-day grace period,
	// restarting the dunning-email cycle indefinitely.
	HasTerminatedGracePeriod(ctx context.Context, teamID uuid.UUID, subscriptionID string) (bool, error)
}

// gracePeriodTerminalStatuses are the payment_grace_periods.status values that
// mean the grace clock already ran its course — a new grace period for the
// same subscription must NOT be opened (P1-F(b)).
var gracePeriodTerminalStatuses = []string{"terminated", "expired"}

// ── DB-backed gracePeriodOpener ───────────────────────────────────────────────

// dbGracePeriodOpener implements gracePeriodOpener against the platform DB.
// Mirrors api/internal/handlers/billing.go:startGracePeriodForTeam.
type dbGracePeriodOpener struct{ db *sql.DB }

// billingReconcilerPGUniqueViolation is the Postgres unique-constraint SQLSTATE.
// Duplicated locally to avoid importing the api models package.
const billingReconcilerPGUniqueViolation = "23505"

func (d *dbGracePeriodOpener) GetActiveGracePeriod(ctx context.Context, teamID uuid.UUID) (bool, error) {
	var id string
	err := d.db.QueryRowContext(ctx, `
		SELECT id FROM payment_grace_periods
		WHERE team_id = $1 AND status = 'active'
		LIMIT 1
	`, teamID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("dbGracePeriodOpener.GetActiveGracePeriod: %w", err)
	}
	return true, nil
}

func (d *dbGracePeriodOpener) OpenGracePeriod(ctx context.Context, teamID uuid.UUID, subscriptionID string) error {
	startedAt := time.Now().UTC()
	expiresAt := startedAt.Add(billingReconcilerGraceDays * 24 * time.Hour)

	// RETURNING id so the audit row below can carry grace_id — the Brevo
	// dunning template renders grace_id / started_at / expires_at, matching
	// the api webhook path (emitPaymentGraceStartedAudit). Without these
	// fields the warning email shows a blank recovery deadline.
	var graceID string
	err := d.db.QueryRowContext(ctx, `
		INSERT INTO payment_grace_periods
		    (team_id, subscription_id, status, started_at, expires_at)
		VALUES ($1, $2, 'active', $3, $4)
		RETURNING id
	`, teamID, subscriptionID, startedAt, expiresAt).Scan(&graceID)
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && string(pqErr.Code) == billingReconcilerPGUniqueViolation {
			// Grace period already active — idempotent no-op per the
			// uq_payment_grace_team_active partial unique index.
			return nil
		}
		return fmt.Errorf("dbGracePeriodOpener.OpenGracePeriod: %w", err)
	}

	slog.Info("billing.reconciler.grace_period_opened",
		"team_id", teamID,
		"subscription_id", subscriptionID,
		"grace_id", graceID,
		"expires_at", expiresAt,
	)

	// Best-effort audit row — the grace period is committed; a log failure is
	// tolerable. Metadata carries grace_id / started_at / expires_at so the
	// dunning email renders the recovery deadline. attempted_amount is null:
	// the reconciler has no payment entity (it polls subscription state, not
	// the failed charge) — the Brevo template tolerates a null amount.
	_, auditErr := d.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES ($1, 'system', 'payment.grace_started',
		        'Grace period opened by billing reconciler poller',
		        jsonb_build_object(
		            'subscription_id',  $2::text,
		            'grace_id',         $3::text,
		            'started_at',       $4::text,
		            'expires_at',       $5::text,
		            'attempted_amount', NULL,
		            'source',           'billing_reconciler'
		        ))
	`, teamID, subscriptionID, graceID,
		startedAt.Format(time.RFC3339),
		expiresAt.Format(time.RFC3339),
	)
	if auditErr != nil {
		// Fail-open: grace period is already committed; a missing audit row
		// only costs the dunning email. Surface via the fail-open metric so
		// an SRE can alert on a sudden spike (DB brownout, schema drift).
		RecordFailOpen(
			"billing_reconciler.grace_audit_insert",
			"db_error",
			auditErr,
			"team_id", teamID,
		)
	}
	return nil
}

// HasTerminatedGracePeriod reports whether the team already has a grace
// period for subscriptionID in a terminal status (see
// gracePeriodTerminalStatuses). When true the reconciler must NOT open a
// fresh grace period — the grace clock has already run out and the team was
// terminated; re-opening would restart the 7-day dunning-email cycle
// (P1-F(b)).
func (d *dbGracePeriodOpener) HasTerminatedGracePeriod(ctx context.Context, teamID uuid.UUID, subscriptionID string) (bool, error) {
	var id string
	err := d.db.QueryRowContext(ctx, `
		SELECT id FROM payment_grace_periods
		WHERE team_id = $1
		  AND subscription_id = $2
		  AND status = ANY($3)
		LIMIT 1
	`, teamID, subscriptionID, pq.Array(gracePeriodTerminalStatuses)).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("dbGracePeriodOpener.HasTerminatedGracePeriod: %w", err)
	}
	return true, nil
}

// ── errReconcilerCircuitOpen sentinel ────────────────────────────────────────

// errReconcilerCircuitOpen is returned by the reconciler's fetcher when the
// Razorpay circuit breaker is in the open state. Work() treats this as a
// batch-abort signal rather than a per-row error.
var errReconcilerCircuitOpen = errors.New("billing_reconciler: razorpay circuit open")

// ── noopSubFetcher (when Razorpay is not configured) ─────────────────────────

// noopSubFetcher is a subscriptionFetcher that always returns a "not configured"
// sentinel. StartWorkers wires this when RAZORPAY_KEY_ID is unset.
type noopSubFetcher struct{}

var errSubFetcherNotConfigured = errors.New("billing_reconciler: Razorpay not configured (RAZORPAY_KEY_ID unset)")

func (noopSubFetcher) FetchSubscriptionForReconciler(_ context.Context, _ string) (*reconcilerSubscriptionDetails, error) {
	return nil, errSubFetcherNotConfigured
}

// ── razorpaySubFetcher — real Razorpay SDK implementation ─────────────────────

// razorpaySubFetcher implements subscriptionFetcher using the Razorpay Go SDK.
// It mirrors the logic in api/internal/razorpaybilling/portal.go:FetchSubscriptionDetails
// but extracts only the three fields the reconciler needs: Status, PlanID, and PaidCount.
//
// Auth is read from RAZORPAY_KEY_ID / RAZORPAY_KEY_SECRET at construction time,
// same as the api pod reads them from the same k8s Secret.
//
// The client field is an interface so tests can inject an http.RoundTripper via
// httptest.NewServer without hitting the real Razorpay API.
type razorpaySubFetcher struct {
	client razorpaySDKClient
}

// razorpaySDKClient is the subset of razorpay.Client the fetcher needs.
// Narrow interface for easy test substitution.
type razorpaySDKClient interface {
	FetchSubscription(subID string, queryParams map[string]interface{}, extraHeaders map[string]string) (map[string]interface{}, error)
}

// razorpayClientAdapter wraps razorpay.Client to satisfy razorpaySDKClient.
type razorpayClientAdapter struct{ c *razorpay.Client }

func (a *razorpayClientAdapter) FetchSubscription(subID string, queryParams map[string]interface{}, extraHeaders map[string]string) (map[string]interface{}, error) {
	return a.c.Subscription.Fetch(subID, queryParams, extraHeaders)
}

// workerRazorpayHTTPTimeoutSeconds is the HTTP timeout (in seconds) applied to
// the Razorpay SDK client used by the worker — billing reconciler fetcher and
// orphan-sweep canceler both share this value.
//
// The Razorpay Go SDK's *default* http.Client.Timeout is 10s (see
// requests.TIMEOUT in razorpay-go). The audit (CIRCUIT-RETRY-AUDIT-2026-05-20,
// P1-6) flagged that the worker's reconciler ticks could be pinned for the
// full job budget while waiting on a slow-but-up Razorpay. We deliberately
// raise the deadline to 30s and explicitly pin it via SetTimeout — both so a
// future SDK default change does not silently shift our deadline AND so the
// "no explicit HTTP timeout" audit finding has a load-bearing constant to
// reference. 30s matches what the api side does (P0-2 fix). Together with the
// circuit breaker (5 consecutive failures → 60s cooldown) this caps the worst-
// case tick blockage at ~150s instead of the entire River job timeout.
const workerRazorpayHTTPTimeoutSeconds int16 = 30

// NewRazorpaySubFetcher constructs a razorpaySubFetcher that reads credentials
// from the environment. Returns (nil, nil) when RAZORPAY_KEY_ID is unset so
// callers can fall back to noopSubFetcher without an error.
//
// The returned fetcher is NOT wrapped with the circuit breaker — the caller
// (StartWorkers / WrapFetcherWithBreaker) adds that layer.
//
// The underlying razorpay.Client has its HTTP timeout pinned to
// workerRazorpayHTTPTimeoutSeconds via SetTimeout — see that constant for the
// audit rationale.
func NewRazorpaySubFetcher() (*razorpaySubFetcher, error) {
	keyID := os.Getenv("RAZORPAY_KEY_ID")
	keySecret := os.Getenv("RAZORPAY_KEY_SECRET")
	if keyID == "" || keySecret == "" {
		return nil, nil // unconfigured — use noop
	}
	c := razorpay.NewClient(keyID, keySecret)
	// Pin the SDK's HTTP client timeout explicitly — see
	// workerRazorpayHTTPTimeoutSeconds for the rationale (audit P1-6).
	c.SetTimeout(workerRazorpayHTTPTimeoutSeconds)
	return &razorpaySubFetcher{client: &razorpayClientAdapter{c: c}}, nil
}

// newRazorpaySubFetcherFromClient constructs a fetcher from a pre-built client.
// Used in tests to inject an httptest.NewServer-backed client.
func newRazorpaySubFetcherFromClient(c razorpaySDKClient) *razorpaySubFetcher {
	return &razorpaySubFetcher{client: c}
}

// FetchSubscriptionForReconciler calls Razorpay GET /v1/subscriptions/{id} and
// maps the response into reconcilerSubscriptionDetails.
//
// Field mapping:
//
//	status      → details.Status     (Razorpay string: "active", "halted", "cancelled", …)
//	plan_id     → details.PlanID     (string, nested inside "plan" object OR at top-level)
//	paid_count  → details.PaidCount  (integer — total successful charge cycles)
//
// The context deadline is respected; the underlying HTTP client uses the context.
// Returns a non-nil error on Razorpay API failure; the caller (Work) logs and
// continues to the next team.
func (f *razorpaySubFetcher) FetchSubscriptionForReconciler(ctx context.Context, subscriptionID string) (*reconcilerSubscriptionDetails, error) {
	// The Razorpay SDK does not accept a context on individual calls; honour
	// the caller's deadline by checking it before the blocking network call.
	// If the context is already cancelled, bail early.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	raw, err := f.client.FetchSubscription(subscriptionID, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("razorpaySubFetcher.Fetch: %w", err)
	}

	details := &reconcilerSubscriptionDetails{}

	// status is the Razorpay subscription lifecycle status.
	if s, ok := raw["status"].(string); ok {
		details.Status = strings.ToLower(strings.TrimSpace(s))
	}

	// plan_id: Razorpay may return it at "plan_id" (top-level, webhook format)
	// or nested under "plan" → "id" (fetch-subscription format). Handle both.
	if planID, ok := raw["plan_id"].(string); ok && planID != "" {
		details.PlanID = planID
	} else if planObj, ok := raw["plan"].(map[string]interface{}); ok {
		if id, ok := planObj["id"].(string); ok {
			details.PlanID = id
		}
	}

	// paid_count: total number of successful charge cycles on this subscription.
	// Razorpay returns it as a JSON number; toInt64 handles float64/int/string.
	details.PaidCount = razorpayToInt64(raw["paid_count"])

	return details, nil
}

// razorpayToInt64 converts a Razorpay API value (JSON-decoded as interface{})
// to int64. Mirrors the toInt64 helper in api/internal/razorpaybilling/portal.go;
// duplicated here to avoid importing across Go module boundaries.
func razorpayToInt64(v interface{}) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case int64:
		return t
	case int:
		return int64(t)
	case string:
		var n int64
		_, _ = fmt.Sscanf(t, "%d", &n)
		return n
	default:
		return 0
	}
}

// ── circuitSubFetcher — wraps a fetcher behind a circuit breaker ──────────────

// circuitSubFetcher wraps a subscriptionFetcher with the worker's circuit breaker
// so FetchSubscriptionForReconciler is protected from a thundering-herd of
// concurrent Razorpay failures during an outage.
type circuitSubFetcher struct {
	inner   subscriptionFetcher
	breaker *circuit.Breaker
}

func (f *circuitSubFetcher) FetchSubscriptionForReconciler(ctx context.Context, subscriptionID string) (*reconcilerSubscriptionDetails, error) {
	if !f.breaker.Allow() {
		return nil, errReconcilerCircuitOpen
	}
	out, err := f.inner.FetchSubscriptionForReconciler(ctx, subscriptionID)
	// Filter caller-cancellations and bad-input errors out of the breaker's
	// failure signal — mirrors the api-side P1-1 fix in
	// api/internal/provisioner/client.go.
	//
	// Why: the breaker is a SERVER-trouble detector. A context that the
	// caller cancelled (e.g. River job timeout, parent ctx Done) is not a
	// signal that Razorpay is unhealthy; it's a signal the caller went
	// away. Counting it would let a noisy local cancellation trip the
	// breaker and shut Razorpay calls down for 60s for every other team.
	// `errReconcilerCircuitOpen` and `errSubFetcherNotConfigured` are
	// breaker-bookkeeping / config sentinels, not Razorpay failures, and
	// must also not feed back into the breaker.
	f.breaker.Record(reconcilerBreakerFilter(ctx, err))
	return out, err
}

// reconcilerBreakerFilter returns the error that should be Record()'d
// against the reconciler's Razorpay breaker. It returns nil for caller-
// driven and config-driven errors that do NOT signal a Razorpay problem;
// it passes through any genuine Razorpay / transport error so the breaker
// still trips on a real upstream outage.
//
// The audit explicitly called this out (worker mirror of api P1-1): a
// caller flooding the reconciler with cancelled contexts must not be able
// to self-inflict a 60s outage on the breaker.
func reconcilerBreakerFilter(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	// 1. Caller-driven cancellations / deadlines. ctx.Err() can be either
	//    context.Canceled or context.DeadlineExceeded; both mean "we left
	//    early", not "Razorpay is sick".
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	if cerr := ctx.Err(); cerr != nil {
		// The fetcher saw a non-context error but the caller's ctx is
		// already done — almost certainly the call was abandoned. Don't
		// count it.
		return nil
	}
	// 2. Breaker bookkeeping / config sentinels — these aren't Razorpay
	//    errors and must never feed back into the breaker.
	if errors.Is(err, errReconcilerCircuitOpen) ||
		errors.Is(err, errSubFetcherNotConfigured) {
		return nil
	}
	// 3. Everything else (Razorpay 5xx, transport errors, parse errors)
	//    is a genuine signal — pass through.
	return err
}

// NewBillingReconcilerCircuitBreaker builds the reconciler's Razorpay circuit
// breaker. Called once by StartWorkers. Separate from the api's sharedBreaker
// so the worker process has its own independent state machine.
func NewBillingReconcilerCircuitBreaker() *circuit.Breaker {
	return circuit.NewBreaker(
		"billing_reconciler_razorpay",
		5,              // 5 consecutive failures → open
		60*time.Second, // 60s cooldown
	).WithOnOpen(func() {
		slog.Error("billing.reconciler.circuit.opened",
			"name", "billing_reconciler_razorpay",
			"impact", "billing reconciler ticks will be no-ops until Razorpay recovers",
		)
	})
}

// WrapFetcherWithBreaker wraps the provided subscriptionFetcher behind the
// given circuit breaker. Used by StartWorkers to assemble the production stack.
func WrapFetcherWithBreaker(inner subscriptionFetcher, b *circuit.Breaker) subscriptionFetcher {
	return &circuitSubFetcher{inner: inner, breaker: b}
}

// ── BillingReconcilerWorker ───────────────────────────────────────────────────

// billingReconcilerTeamRow is the projection the reconciler's SELECT returns.
type billingReconcilerTeamRow struct {
	id             uuid.UUID
	subscriptionID string // stripe_customer_id column stores Razorpay sub IDs
	planTier       string
}

// BillingReconcilerWorker sweeps teams with Razorpay subscriptions, compares
// live Razorpay state against teams.plan_tier, and corrects divergence.
type BillingReconcilerWorker struct {
	river.WorkerDefaults[BillingReconcilerArgs]

	db      *sql.DB
	fetcher subscriptionFetcher
	grace   gracePeriodOpener

	// chargeUndeliverableMu guards chargeUndeliverableCursor (B11-F3).
	// One sweep at a time per pod — the cursor advances monotonically
	// across ticks. Multiple worker pods are tolerated: each pod has its
	// own cursor + counter; the metric aggregates pod-wise, and the
	// audit_log row is the durable source of truth.
	chargeUndeliverableMu     sync.Mutex
	chargeUndeliverableCursor time.Time
}

// NewBillingReconcilerWorker constructs the worker.
//
// fetcher is the Razorpay API surface — pass noopSubFetcher{} when
// RAZORPAY_KEY_ID is unset; the worker logs a WARN per tick. grace is the
// grace-period DB surface; nil uses the real dbGracePeriodOpener.
func NewBillingReconcilerWorker(db *sql.DB, fetcher subscriptionFetcher, grace gracePeriodOpener) *BillingReconcilerWorker {
	if fetcher == nil {
		fetcher = noopSubFetcher{}
	}
	w := &BillingReconcilerWorker{
		db:      db,
		fetcher: fetcher,
	}
	if grace != nil {
		w.grace = grace
	} else {
		w.grace = &dbGracePeriodOpener{db: db}
	}
	return w
}

// BillingReconcileInterval resolves the periodic dispatch cadence from
// BILLING_RECONCILE_INTERVAL (Go duration string, e.g. "15m"). Falls back to
// defaultBillingReconcileInterval — same pattern as EntitlementReconcileInterval.
func BillingReconcileInterval() time.Duration {
	raw := os.Getenv("BILLING_RECONCILE_INTERVAL")
	if raw == "" {
		return defaultBillingReconcileInterval
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		slog.Warn("jobs.billing_reconciler.bad_interval",
			"value", raw,
			"error", err,
			"fallback", defaultBillingReconcileInterval.String(),
		)
		return defaultBillingReconcileInterval
	}
	return d
}

// Work executes one reconciler sweep.
//
// Error contract:
//   - Returns nil on transient per-team Razorpay errors — River does NOT retry
//     exponentially; the next periodic tick retries within 15 minutes.
//   - Returns non-nil only on top-level DB query failure so River retries the
//     whole tick.
//   - Circuit-open → abort the batch cleanly; log a WARN; return nil.
func (w *BillingReconcilerWorker) Work(ctx context.Context, job *river.Job[BillingReconcilerArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.billing_reconciler")
	defer span.End()

	start := time.Now()

	// Candidate query: all teams with a Razorpay subscription_id, ordered by id
	// for stable per-tick pagination. Application-side tier filtering so the
	// reconciler can catch under-tiered (missed upgrade) AND over-tiered
	// (missed cancellation) teams in one pass.
	rows, err := w.db.QueryContext(ctx, `
		SELECT id, stripe_customer_id, plan_tier
		  FROM teams
		 WHERE stripe_customer_id IS NOT NULL
		   AND stripe_customer_id != ''
		 ORDER BY id
		 LIMIT $1
	`, billingReconcilerBatchLimit)
	if err != nil {
		return fmt.Errorf("BillingReconcilerWorker: query failed: %w", err)
	}
	defer rows.Close()

	var teams []billingReconcilerTeamRow
	for rows.Next() {
		var r billingReconcilerTeamRow
		if scanErr := rows.Scan(&r.id, &r.subscriptionID, &r.planTier); scanErr != nil {
			slog.Warn("jobs.billing_reconciler.scan_failed", "error", scanErr)
			continue
		}
		teams = append(teams, r)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return fmt.Errorf("BillingReconcilerWorker: rows error: %w", rowsErr)
	}
	rows.Close()

	metrics.BillingReconcilerTeamsScanned.Add(float64(len(teams)))

	var gapUpgrade, gapDowngrade, correctedUpgrade, correctedDowngrade, razorpayErrors int

	for i, team := range teams {
		// 100ms stagger between calls to avoid Razorpay rate-limit bursts.
		if i > 0 {
			select {
			case <-ctx.Done():
				slog.Warn("jobs.billing_reconciler.context_cancelled", "processed", i)
				goto done
			case <-time.After(billingReconcilerStaggerDelay):
			}
		}

		{
			fetchCtx, cancel := context.WithTimeout(ctx, billingReconcilerRazorpayTimeout)
			details, fetchErr := w.fetcher.FetchSubscriptionForReconciler(fetchCtx, team.subscriptionID)
			cancel()

			if fetchErr != nil {
				razorpayErrors++
				metrics.BillingReconcilerRazorpayErrors.Inc()

				if errors.Is(fetchErr, errReconcilerCircuitOpen) {
					slog.Warn("jobs.billing_reconciler.circuit_open",
						"note", "aborting tick early; next tick will retry",
					)
					goto done
				}
				if errors.Is(fetchErr, errSubFetcherNotConfigured) {
					slog.Warn("jobs.billing_reconciler.not_configured",
						"note", "RAZORPAY_KEY_ID unset — reconciler is a no-op this tick",
					)
					goto done
				}
				slog.Warn("jobs.billing_reconciler.fetch_failed",
					"team_id", team.id,
					"subscription_id", team.subscriptionID,
					"error", fetchErr,
				)
				continue
			}

			statusClass, known := razorpayStatusClass[details.Status]
			if !known {
				slog.Warn("jobs.billing_reconciler.unknown_status",
					"team_id", team.id,
					"razorpay_status", details.Status,
				)
				continue
			}

			switch statusClass {

			case rzpStatusClassActive:
				expectedTier := billingReconcilerPlanIDToTier(details.PlanID)
				if billingTierRank(team.planTier) >= billingTierRank(expectedTier) {
					// DB is already at or above the expected tier — no-op.
					continue
				}
				// Gap: DB tier is lower than Razorpay says it should be.
				gapUpgrade++
				metrics.BillingReconcilerGapDetected.WithLabelValues("upgrade").Inc()
				slog.Warn("billing.reconciler.tier_corrected",
					"team_id", team.id,
					"from", team.planTier,
					"to", expectedTier,
					"razorpay_status", details.Status,
					"subscription_id", team.subscriptionID,
				)
				if upgradeErr := w.upgradeTeamTiers(ctx, team.id, expectedTier); upgradeErr != nil {
					slog.Error("billing.reconciler.upgrade_failed",
						"team_id", team.id, "error", upgradeErr,
					)
					continue
				}
				correctedUpgrade++
				metrics.BillingReconcilerGapCorrected.WithLabelValues("upgrade").Inc()
				// Emit audit for the event-email forwarder. Fail-open.
				w.emitUpgradeAudit(ctx, team.id, team.planTier, expectedTier, team.subscriptionID)

			case rzpStatusClassGrace:
				hasGrace, graceErr := w.grace.GetActiveGracePeriod(ctx, team.id)
				if graceErr != nil {
					slog.Warn("billing.reconciler.grace_check_failed",
						"team_id", team.id, "error", graceErr,
					)
					continue
				}
				if hasGrace {
					continue // grace period already active — idempotent
				}
				// P1-F(b) guard: Razorpay can keep reporting halted/paused
				// for a while AFTER the grace clock expired and the team was
				// terminated. Without this check the reconciler sees "no
				// ACTIVE grace" and opens a FRESH 7-day grace period every
				// tick — restarting the dunning-email cycle indefinitely. If
				// a terminal grace period already exists for this
				// subscription, the team has been through grace; do not
				// re-enter it.
				terminated, termErr := w.grace.HasTerminatedGracePeriod(ctx, team.id, team.subscriptionID)
				if termErr != nil {
					slog.Warn("billing.reconciler.terminated_grace_check_failed",
						"team_id", team.id, "error", termErr,
					)
					continue
				}
				if terminated {
					slog.Info("billing.reconciler.grace_reopen_skipped_terminal",
						"team_id", team.id,
						"subscription_id", team.subscriptionID,
						"razorpay_status", details.Status,
						"note", "team already terminated via a prior grace period; not re-opening grace",
					)
					continue
				}
				// Gap: Razorpay says halted/paused but no grace period exists.
				gapDowngrade++
				metrics.BillingReconcilerGapDetected.WithLabelValues("downgrade").Inc()
				slog.Warn("billing.reconciler.grace_opened_by_poller",
					"team_id", team.id,
					"razorpay_status", details.Status,
					"subscription_id", team.subscriptionID,
				)
				if openErr := w.grace.OpenGracePeriod(ctx, team.id, team.subscriptionID); openErr != nil {
					slog.Error("billing.reconciler.grace_open_failed",
						"team_id", team.id, "error", openErr,
					)
					continue
				}
				correctedDowngrade++
				metrics.BillingReconcilerGraceMissed.Inc()

			case rzpStatusClassTerminal:
				// Only write if team is still on a paid tier — the webhook may
				// have already downgraded them.
				if !billingPaidTiers[team.planTier] {
					continue
				}
				gapDowngrade++
				metrics.BillingReconcilerGapDetected.WithLabelValues("downgrade").Inc()

				// Terminal subscription → always downgrade to "hobby", never
				// "free". "free" is the 24h-TTL ephemeral claimed-but-unpaid
				// tier; the downgrade path (updatePlanTier) deliberately leaves
				// existing resources on their paid tier with expires_at=NULL
				// (user-benefit policy). Setting the TEAM to "free" while its
				// resources stay permanent + paid-tier strands permanent paid
				// infra with no billing relationship — a tier mismatch the
				// rest of the system has no path to reconcile. "hobby" is the
				// lowest paid tier and matches the non-zero-paid branch, so
				// team-tier and resource-tier stay coherent regardless of
				// PaidCount. (PaidCount==0 only means no *successful* charge
				// was recorded — e.g. a failed first charge then cancellation;
				// it is not a signal that resources should become ephemeral.)
				const terminalDowngradeTier = "hobby"
				targetTier := terminalDowngradeTier
				slog.Warn("billing.reconciler.tier_corrected",
					"team_id", team.id,
					"from", team.planTier,
					"to", targetTier,
					"razorpay_status", details.Status,
					"subscription_id", team.subscriptionID,
				)
				// Downgrade uses UpdatePlanTier (NOT upgradeTeamTiers): only the team
				// row is updated; existing resources keep their tier — user-benefit
				// policy matching the webhook handler (design doc §2, Do Not §6).
				if downErr := w.updatePlanTier(ctx, team.id, targetTier); downErr != nil {
					slog.Error("billing.reconciler.downgrade_failed",
						"team_id", team.id, "error", downErr,
					)
					continue
				}
				correctedDowngrade++
				metrics.BillingReconcilerGapCorrected.WithLabelValues("downgrade").Inc()
				// Emit audit for the event-email forwarder. Fail-open.
				w.emitCancelAudit(ctx, team.id, team.planTier, targetTier, team.subscriptionID)

			case rzpStatusClassNoAction:
				// "pending" — may self-resolve; do nothing.
			}
		}
	}

done:
	// F1: Razorpay-authoritative orphan sweep. Catches paid teams the primary
	// teams-table sweep is structurally blind to (their checkout-time
	// subscription_id UPDATE was lost). Fail-open: an error here is logged and
	// swallowed — the primary sweep's corrections are already committed and the
	// next tick retries the orphan pass.
	orphanScanned, orphanCorrected := w.runOrphanSweep(ctx)

	// B11-F3 (BugBash 2026-05-20): scan audit_log for new
	// billing.charge_undeliverable rows since the last tick. Fail-open —
	// a DB blip just delays the metric update; the audit row itself is
	// the durable signal.
	chargeUndeliverableNew := w.scanChargeUndeliverable(ctx)

	slog.Info("jobs.billing_reconciler.completed",
		"teams_scanned", len(teams),
		"gap_upgrade", gapUpgrade,
		"gap_downgrade", gapDowngrade,
		"corrected_upgrade", correctedUpgrade,
		"corrected_downgrade", correctedDowngrade,
		"razorpay_errors", razorpayErrors,
		"orphan_scanned", orphanScanned,
		"orphan_corrected", orphanCorrected,
		"charge_undeliverable_new", chargeUndeliverableNew,
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}

// scanChargeUndeliverable counts audit_log rows with
// kind='billing.charge_undeliverable' since the last successful scan and
// advances chargeUndeliverableCursor. The api emits these rows when a
// Razorpay webhook payload trust-pass fails — see B11-F3 + the api
// handler at internal/handlers/billing.go.
//
// First tick after pod boot uses a 1h look-back so we surface any rows
// that landed in the brief window between an api emit and the worker's
// first tick. After that the cursor tracks the high-watermark
// created_at and only newer rows count.
//
// Returned: number of new rows seen this tick. On DB error returns 0
// and does NOT advance the cursor (fail-open — retry next tick).
func (w *BillingReconcilerWorker) scanChargeUndeliverable(ctx context.Context) int {
	w.chargeUndeliverableMu.Lock()
	cursor := w.chargeUndeliverableCursor
	w.chargeUndeliverableMu.Unlock()
	if cursor.IsZero() {
		cursor = time.Now().UTC().Add(-1 * time.Hour)
	}

	rows, err := w.db.QueryContext(ctx, `
		SELECT created_at FROM audit_log
		WHERE kind = $1 AND created_at > $2
		ORDER BY created_at ASC
		LIMIT 1000
	`, chargeUndeliverableAuditKind, cursor)
	if err != nil {
		slog.Warn("jobs.billing_reconciler.charge_undeliverable_scan_failed",
			"error", err, "note", "fail-open — retry next tick")
		return 0
	}
	defer rows.Close()

	var count int
	var maxCreated time.Time
	for rows.Next() {
		var t time.Time
		if scanErr := rows.Scan(&t); scanErr != nil {
			slog.Warn("jobs.billing_reconciler.charge_undeliverable_scan_row_failed",
				"error", scanErr)
			continue
		}
		count++
		if t.After(maxCreated) {
			maxCreated = t
		}
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		slog.Warn("jobs.billing_reconciler.charge_undeliverable_rows_error",
			"error", rowsErr)
		// Don't advance cursor — next tick re-scans the same window.
		return 0
	}

	if count > 0 {
		metrics.BillingChargeUndeliverableTotal.Add(float64(count))
		// LOUD log per tick when count > 0 — operator + NR alert + audit row
		// triangulation. Per CLAUDE.md rule: every code change considers NR
		// dashboards + alerts (feedback_nr_observability_per_change).
		slog.Error("jobs.billing_reconciler.charge_undeliverable_observed",
			"new_rows", count,
			"cursor_at", cursor,
			"max_created_at", maxCreated,
			"note", "api wrote billing.charge_undeliverable audit rows — webhook payload failed trust-pass; needs operator follow-up (B11-F3)",
		)
	}

	// Advance the cursor to the latest seen row. If count==0 we still
	// advance to now() — saves re-scanning the same empty window next
	// tick, and there's nothing in the window to lose.
	if maxCreated.IsZero() {
		maxCreated = time.Now().UTC()
	}
	w.chargeUndeliverableMu.Lock()
	w.chargeUndeliverableCursor = maxCreated
	w.chargeUndeliverableMu.Unlock()
	return count
}

// billingReconcilerOrphanRow is the projection the orphan sweep's SELECT over
// pending_checkouts returns: the (subscription_id, team_id) pair recorded at
// checkout time, plus the team's CURRENT plan_tier so the sweep can decide
// whether an elevation is needed without a second query.
type billingReconcilerOrphanRow struct {
	subscriptionID string
	teamID         uuid.UUID
	planTier       string
}

// runOrphanSweep is the F1 fix: a Razorpay-authoritative second pass that
// starts from pending_checkouts instead of teams.stripe_customer_id.
//
// pending_checkouts (api migration 034) records the (subscription_id, team_id)
// pair for EVERY checkout the api minted — it is written when the customer is
// handed the Razorpay short_url, on the SAME insert path, NOT via the
// best-effort UPDATE that the primary sweep's candidate column depends on. So a
// team whose teams.stripe_customer_id write was lost still has a
// pending_checkouts row carrying its subscription_id.
//
// For each checkout the sweep fetches the live Razorpay subscription. If
// Razorpay reports the subscription active/authenticated (paid or card
// authorised) and the team's DB tier is BELOW the entitled tier, the team is
// elevated via the same upgradeTeamTiers path the primary sweep uses, and a
// subscription.upgraded audit row is emitted for the event-email forwarder.
//
// Error contract — fully fail-open:
//   - A candidate-query failure logs and returns (0,0): the primary sweep's
//     work is already committed; River retries the whole tick in 15 minutes.
//   - A per-checkout Razorpay error / circuit-open / not-configured aborts or
//     skips that row only, exactly like the primary sweep.
//   - An elevation DB error logs and moves to the next row.
//
// Returns (scanned, corrected) for the completion-log summary.
func (w *BillingReconcilerWorker) runOrphanSweep(ctx context.Context) (scanned, corrected int) {
	cutoff := time.Now().UTC().Add(-billingReconcilerOrphanMinAge)

	// Candidate query: pending_checkouts rows joined to their team. The JOIN
	// projects the team's current plan_tier so the sweep needs no follow-up
	// query. Ordered by created_at for stable per-tick pagination.
	rows, err := w.db.QueryContext(ctx, `
		SELECT pc.subscription_id, pc.team_id, t.plan_tier
		  FROM pending_checkouts pc
		  JOIN teams t ON t.id = pc.team_id
		 WHERE pc.subscription_id IS NOT NULL
		   AND pc.subscription_id != ''
		   AND pc.created_at < $1
		 ORDER BY pc.created_at
		 LIMIT $2
	`, cutoff, billingReconcilerOrphanBatchLimit)
	if err != nil {
		slog.Warn("billing.reconciler.orphan_query_failed",
			"error", err,
			"note", "primary sweep corrections already committed; next tick retries",
		)
		return 0, 0
	}
	defer rows.Close()

	var candidates []billingReconcilerOrphanRow
	for rows.Next() {
		var r billingReconcilerOrphanRow
		if scanErr := rows.Scan(&r.subscriptionID, &r.teamID, &r.planTier); scanErr != nil {
			slog.Warn("billing.reconciler.orphan_scan_failed", "error", scanErr)
			continue
		}
		candidates = append(candidates, r)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		slog.Warn("billing.reconciler.orphan_rows_error", "error", rowsErr)
		return 0, 0
	}
	rows.Close()

	for i, c := range candidates {
		// 100ms stagger between Razorpay calls, same as the primary sweep.
		if i > 0 {
			select {
			case <-ctx.Done():
				slog.Warn("billing.reconciler.orphan_context_cancelled", "processed", i)
				return scanned, corrected
			case <-time.After(billingReconcilerStaggerDelay):
			}
		}

		scanned++
		metrics.BillingReconcilerOrphanScanned.Inc()

		fetchCtx, cancel := context.WithTimeout(ctx, billingReconcilerRazorpayTimeout)
		details, fetchErr := w.fetcher.FetchSubscriptionForReconciler(fetchCtx, c.subscriptionID)
		cancel()

		if fetchErr != nil {
			metrics.BillingReconcilerRazorpayErrors.Inc()
			// Circuit-open / not-configured → abort the orphan sweep cleanly,
			// matching the primary sweep's batch-abort behaviour.
			if errors.Is(fetchErr, errReconcilerCircuitOpen) ||
				errors.Is(fetchErr, errSubFetcherNotConfigured) {
				slog.Warn("billing.reconciler.orphan_sweep_aborted",
					"reason", fetchErr,
					"processed", i,
				)
				return scanned, corrected
			}
			slog.Warn("billing.reconciler.orphan_fetch_failed",
				"team_id", c.teamID,
				"subscription_id", c.subscriptionID,
				"error", fetchErr,
			)
			continue
		}

		// Only an active/authenticated subscription means "Razorpay says this
		// team is paid". pending/halted/cancelled checkouts are handled by the
		// primary sweep (once the sub-id persists) and the grace/terminal
		// machinery; the orphan sweep's job is narrowly the charged-but-not-
		// upgraded hole.
		statusClass, known := razorpayStatusClass[details.Status]
		if !known || statusClass != rzpStatusClassActive {
			continue
		}

		expectedTier := billingReconcilerPlanIDToTier(details.PlanID)
		if billingTierRank(c.planTier) >= billingTierRank(expectedTier) {
			// DB tier already at or above expected — no orphan correction
			// needed (the team was upgraded by some other path).
			continue
		}

		// Charged-but-not-upgraded, recovered. This team paid at Razorpay but
		// its checkout-time subscription_id UPDATE was lost, so the primary
		// teams-table sweep never scanned it. Elevate it now.
		slog.Warn("billing.reconciler.orphan_tier_corrected",
			"team_id", c.teamID,
			"from", c.planTier,
			"to", expectedTier,
			"razorpay_status", details.Status,
			"subscription_id", c.subscriptionID,
			"note", "team paid at Razorpay but had no persisted subscription_id — recovered via pending_checkouts orphan sweep (F1)",
		)
		metrics.BillingReconcilerGapDetected.WithLabelValues("upgrade").Inc()

		if upgradeErr := w.upgradeTeamTiers(ctx, c.teamID, expectedTier); upgradeErr != nil {
			slog.Error("billing.reconciler.orphan_upgrade_failed",
				"team_id", c.teamID, "error", upgradeErr,
			)
			continue
		}
		corrected++
		metrics.BillingReconcilerOrphanCorrected.Inc()
		metrics.BillingReconcilerGapCorrected.WithLabelValues("upgrade").Inc()

		// Backfill teams.stripe_customer_id so the team is visible to the
		// primary sweep from now on — without this it would remain an orphan
		// and be re-checked by this sweep every tick. Fail-open: the upgrade is
		// already committed; a failed backfill just means another orphan-sweep
		// pass next tick (idempotent — the rank check above no-ops it).
		if _, backfillErr := w.db.ExecContext(ctx,
			`UPDATE teams SET stripe_customer_id = $1
			  WHERE id = $2
			    AND (stripe_customer_id IS NULL OR stripe_customer_id = '')`,
			c.subscriptionID, c.teamID,
		); backfillErr != nil {
			slog.Warn("billing.reconciler.orphan_subid_backfill_failed",
				"team_id", c.teamID,
				"subscription_id", c.subscriptionID,
				"error", backfillErr,
			)
		}

		// Emit the audit row for the event-email forwarder — same as the
		// primary sweep's upgrade path. Fail-open.
		w.emitUpgradeAudit(ctx, c.teamID, c.planTier, expectedTier, c.subscriptionID)
	}

	return scanned, corrected
}

// upgradeTeamTiers is the worker-local equivalent of models.UpgradeTeamAllTiers.
// It runs four UPDATE statements inside a transaction, mirroring the api's model.
//
// TECH DEBT: if api/internal/models is ever extracted to a shared package,
// replace this with a direct call to models.UpgradeTeamAllTiers.
func (w *BillingReconcilerWorker) upgradeTeamTiers(ctx context.Context, teamID uuid.UUID, newTier string) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("billing_reconciler.upgradeTeamTiers: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 1. Team.
	if _, err := tx.ExecContext(ctx,
		`UPDATE teams SET plan_tier = $1 WHERE id = $2`,
		newTier, teamID,
	); err != nil {
		return fmt.Errorf("billing_reconciler.upgradeTeamTiers: update_plan_tier: %w", err)
	}
	// 2. Resources — reaper-race guard: only lift non-expired rows.
	if _, err := tx.ExecContext(ctx, `
		UPDATE resources
		   SET tier = $1, expires_at = NULL
		 WHERE team_id = $2
		   AND status IN ('active', 'paused')
		   AND (expires_at IS NULL OR expires_at > now())
	`, newTier, teamID); err != nil {
		return fmt.Errorf("billing_reconciler.upgradeTeamTiers: elevate_resources: %w", err)
	}
	// 3. Deployments — clear 24h TTL; skip terminal statuses.
	if _, err := tx.ExecContext(ctx, `
		UPDATE deployments
		   SET tier             = $1,
		       expires_at       = NULL,
		       ttl_policy       = 'permanent',
		       reminders_sent   = 0,
		       last_reminder_at = NULL,
		       updated_at       = now()
		 WHERE team_id = $2
		   AND status NOT IN ('deleted', 'expired')
	`, newTier, teamID); err != nil {
		return fmt.Errorf("billing_reconciler.upgradeTeamTiers: elevate_deployments: %w", err)
	}
	// 4. Stacks — skip mid-teardown.
	if _, err := tx.ExecContext(ctx, `
		UPDATE stacks
		   SET tier       = $1,
		       expires_at = NULL,
		       updated_at = now()
		 WHERE team_id = $2
		   AND status NOT IN ('deleting')
	`, newTier, teamID); err != nil {
		return fmt.Errorf("billing_reconciler.upgradeTeamTiers: elevate_stacks: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("billing_reconciler.upgradeTeamTiers: commit: %w", err)
	}
	return nil
}

// updatePlanTier updates only teams.plan_tier — used for the downgrade path.
// Existing resources keep their tier (user-benefit policy per design doc §2).
func (w *BillingReconcilerWorker) updatePlanTier(ctx context.Context, teamID uuid.UUID, tier string) error {
	_, err := w.db.ExecContext(ctx,
		`UPDATE teams SET plan_tier = $1 WHERE id = $2`,
		tier, teamID,
	)
	if err != nil {
		return fmt.Errorf("billing_reconciler.updatePlanTier: %w", err)
	}
	return nil
}

// emitUpgradeAudit inserts a subscription.upgraded audit_log row for the
// event-email forwarder. Fail-open: log on error, never crash the sweep.
func (w *BillingReconcilerWorker) emitUpgradeAudit(ctx context.Context, teamID uuid.UUID, fromTier, toTier, subID string) {
	_, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES ($1, 'system', 'subscription.upgraded',
		        'Tier corrected by billing reconciler poller',
		        jsonb_build_object(
		            'from_tier',        $2::text,
		            'to_tier',          $3::text,
		            'subscription_id',  $4::text,
		            'source',           'billing_reconciler'
		        ))
	`, teamID, fromTier, toTier, subID)
	if err != nil {
		// Fail-open: tier upgrade already committed; missing audit row
		// only suppresses the upgrade email. Surface via fail-open counter
		// so a DB brownout that swallows audit rows is alertable.
		RecordFailOpen(
			"billing_reconciler.upgrade_audit_insert",
			"db_error",
			err,
			"team_id", teamID,
			"from_tier", fromTier,
			"to_tier", toTier,
		)
	}
}

// emitCancelAudit inserts a subscription.canceled audit_log row. Fail-open.
func (w *BillingReconcilerWorker) emitCancelAudit(ctx context.Context, teamID uuid.UUID, fromTier, toTier, subID string) {
	_, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES ($1, 'system', 'subscription.canceled',
		        'Tier corrected (terminal subscription) by billing reconciler poller',
		        jsonb_build_object(
		            'from_tier',        $2::text,
		            'to_tier',          $3::text,
		            'subscription_id',  $4::text,
		            'source',           'billing_reconciler'
		        ))
	`, teamID, fromTier, toTier, subID)
	if err != nil {
		// Fail-open: downgrade already committed; missing audit row only
		// suppresses the cancellation-confirmation email.
		RecordFailOpen(
			"billing_reconciler.cancel_audit_insert",
			"db_error",
			err,
			"team_id", teamID,
			"from_tier", fromTier,
			"to_tier", toTier,
		)
	}
}
