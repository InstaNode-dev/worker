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
//   cancelled / completed / expired → downgrade to hobby (or free if paidCount==0)
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
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	razorpay "github.com/razorpay/razorpay-go"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	"instant.dev/worker/internal/circuit"
	"instant.dev/worker/internal/metrics"
)

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

// billingTierRankMap assigns a numeric rank to each tier. Higher rank = higher
// tier. Tiers absent from the map get rank 0 (treated as unknown / lowest).
var billingTierRankMap = map[string]int{
	"anonymous":  0,
	"free":       1,
	"hobby":      2,
	"hobby_plus": 3,
	"pro":        4,
	"growth":     5,
	"team":       6,
}

// billingTierRank returns the numeric rank for a tier name.
func billingTierRank(tier string) int {
	return billingTierRankMap[strings.ToLower(strings.TrimSpace(tier))]
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
}

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
		slog.Warn("billing.reconciler.grace_audit_insert_failed",
			"team_id", teamID, "error", auditErr,
		)
	}
	return nil
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

// NewRazorpaySubFetcher constructs a razorpaySubFetcher that reads credentials
// from the environment. Returns (nil, nil) when RAZORPAY_KEY_ID is unset so
// callers can fall back to noopSubFetcher without an error.
//
// The returned fetcher is NOT wrapped with the circuit breaker — the caller
// (StartWorkers / WrapFetcherWithBreaker) adds that layer.
func NewRazorpaySubFetcher() (*razorpaySubFetcher, error) {
	keyID := os.Getenv("RAZORPAY_KEY_ID")
	keySecret := os.Getenv("RAZORPAY_KEY_SECRET")
	if keyID == "" || keySecret == "" {
		return nil, nil // unconfigured — use noop
	}
	c := razorpay.NewClient(keyID, keySecret)
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
	f.breaker.Record(err)
	return out, err
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

				targetTier := "hobby"
				if details.PaidCount == 0 {
					targetTier = "free"
				}
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
	slog.Info("jobs.billing_reconciler.completed",
		"teams_scanned", len(teams),
		"gap_upgrade", gapUpgrade,
		"gap_downgrade", gapDowngrade,
		"corrected_upgrade", correctedUpgrade,
		"corrected_downgrade", correctedDowngrade,
		"razorpay_errors", razorpayErrors,
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
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
		slog.Warn("billing.reconciler.upgrade_audit_failed",
			"team_id", teamID, "error", err,
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
		slog.Warn("billing.reconciler.cancel_audit_failed",
			"team_id", teamID, "error", err,
		)
	}
}
