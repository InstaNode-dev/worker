package jobs

// checkout_reconcile.go — periodic sweep that catches the "Razorpay
// checkout started but never completed, and Razorpay sent NO webhook at
// all" failure mode.
//
// The motivating incident: a live Pro upgrade failed on Razorpay's hosted
// checkout page WITHOUT a payment object ever being created. Razorpay
// therefore emitted zero webhooks — the api never learned the checkout
// existed, and the customer got no email. The api's webhook handler can
// only react to deliveries it actually receives; it is structurally blind
// to a checkout that produced no event. This worker is the ONLY mechanism
// that catches that case.
//
// Contract (api migration 034 — pending_checkouts):
//
//	pending_checkouts(
//	  subscription_id      TEXT PK,
//	  team_id              UUID,
//	  customer_email       TEXT,
//	  plan_tier            TEXT,
//	  created_at           TIMESTAMPTZ,
//	  resolved_at          TIMESTAMPTZ,   -- api webhook sets this on activate/charge
//	  failure_notified_at  TIMESTAMPTZ )  -- THIS worker sets this after emailing
//
// The api inserts a pending_checkouts row when it hands the customer a
// Razorpay checkout short_url, and stamps resolved_at from its webhook
// handler the moment the subscription activates/charges. A row that is
// still resolved_at IS NULL fifteen minutes after created_at is a checkout
// the customer almost certainly abandoned (or that failed on Razorpay's
// page) — so we email them a "your upgrade didn't complete" nudge.
//
// Why a sweep, not a fan-out: the population (checkouts in the last 15min
// that haven't resolved) is tiny — most checkouts either complete in
// seconds or are abandoned and never heard from again. One table sweep per
// tick is far cheaper than scheduling a per-checkout River job at
// short_url-mint time.
//
// Email delivery: like every other lifecycle email in this worker, this
// job does NOT call the email provider directly. It writes a
// `checkout.abandoned` audit_log row; the EventEmailForwarder
// (event_email_forwarder.go) drains that row into the configured provider
// on its next 60s tick. Keeping the trigger and the send pipeline
// separated means a Brevo outage doesn't pin the reconciler cadence. The
// `checkout.abandoned` kind is Go-rendered (renderCheckoutAbandoned in
// lifecycle_emails.go) per CLAUDE rule 70 — no Brevo dashboard template.
//
// Best-effort Razorpay double-check: if a subscriptionFetcher is wired
// (the worker already constructs one for the billing reconciler), this job
// GETs the subscription before emailing. If Razorpay reports it
// active/authenticated or with a non-zero paid_count, the webhook was
// merely delayed (not absent) — we stamp resolved_at and SKIP the email,
// avoiding a false-positive "you didn't complete" message to a customer
// who actually did. If no fetcher is wired (RAZORPAY_KEY_ID unset), the
// double-check is skipped entirely and the 15-minute-no-resolved_at
// heuristic stands on its own — it is sufficient.
//
// Idempotency: each candidate row is claimed with FOR UPDATE SKIP LOCKED
// inside a per-row transaction, and the failure_notified_at stamp is
// written in that same transaction. A sibling replica's concurrent tick
// either skips the locked row or sees failure_notified_at already set on
// its next SELECT — so each customer is emailed at most once.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
)

// CheckoutReconcileArgs is the River job payload — no fields (sweep).
type CheckoutReconcileArgs struct{}

// Kind is the River worker key.
func (CheckoutReconcileArgs) Kind() string { return "checkout_reconcile" }

// checkoutReconcileInterval is the dispatch cadence. 5min is frequent
// enough that an abandoned-checkout email lands within ~20min of the
// customer giving up (15min grace + up to one 5min tick + the forwarder's
// 60s drain), and infrequent enough that the Razorpay double-check spend
// stays trivial.
const checkoutReconcileInterval = 5 * time.Minute

// checkoutReconcileGracePeriod is how long a checkout may sit unresolved
// before it is treated as abandoned. Held as a const distinct from the
// dispatch interval so tightening the sweep cadence never accidentally
// shortens the customer-facing grace window. Matches the brief: "created_at
// < now() - interval '15 minutes'".
const checkoutReconcileGracePeriod = 15 * time.Minute

// checkoutReconcileBatchLimit caps per-tick fan-out. The unresolved-checkout
// population is tiny in steady state; this is belt-and-braces against a
// runaway backlog (e.g. after a long worker outage).
const checkoutReconcileBatchLimit = 200

// checkoutReconcileRazorpayTimeout is the per-row Razorpay fetch budget for
// the best-effort double-check. Kept short — a slow Razorpay API must not
// stall the sweep; on timeout we fall through to the heuristic and email.
const checkoutReconcileRazorpayTimeout = 8 * time.Second

// auditKindCheckoutAbandoned is the audit_log.kind this job writes. The
// EventEmailForwarder's SQL filter (supportedAuditKinds) must include this
// literal, and event_email_mapping.go must carry a matching builder +
// Go renderer — asserted by TestEventEmail_EverySupportedKindFullyWired.
const auditKindCheckoutAbandoned = "checkout.abandoned"

// checkoutReconcileActor — system-actor convention shared across the
// worker's periodic emitters.
const checkoutReconcileActor = "system"

// CheckoutReconcileWorker scans pending_checkouts for abandoned checkouts.
type CheckoutReconcileWorker struct {
	river.WorkerDefaults[CheckoutReconcileArgs]
	db *sql.DB
	// fetcher is the best-effort Razorpay double-check. May be nil (or a
	// noop) when RAZORPAY_KEY_ID is unset — in that case the double-check
	// is skipped and the 15-minute heuristic stands alone. Reuses the same
	// subscriptionFetcher interface the billing reconciler depends on.
	fetcher subscriptionFetcher
}

// NewCheckoutReconcileWorker constructs the worker. Pass a nil fetcher to
// disable the Razorpay double-check (the heuristic is then authoritative).
func NewCheckoutReconcileWorker(db *sql.DB, fetcher subscriptionFetcher) *CheckoutReconcileWorker {
	return &CheckoutReconcileWorker{db: db, fetcher: fetcher}
}

// checkoutRow is the projection the worker reads from pending_checkouts.
type checkoutRow struct {
	subscriptionID string
	teamID         string
	customerEmail  string
	planTier       string
}

// Work runs one sweep.
func (w *CheckoutReconcileWorker) Work(ctx context.Context, job *river.Job[CheckoutReconcileArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.checkout_reconcile")
	defer span.End()

	start := time.Now()
	cutoff := time.Now().UTC().Add(-checkoutReconcileGracePeriod)

	rows, err := w.db.QueryContext(ctx, `
		SELECT subscription_id, team_id, customer_email, plan_tier
		FROM pending_checkouts
		WHERE resolved_at IS NULL
		  AND failure_notified_at IS NULL
		  AND created_at < $1
		ORDER BY created_at ASC
		LIMIT $2
	`, cutoff, checkoutReconcileBatchLimit)
	if err != nil {
		return fmt.Errorf("CheckoutReconcileWorker: candidate query failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var candidates []checkoutRow
	for rows.Next() {
		var r checkoutRow
		if scanErr := rows.Scan(&r.subscriptionID, &r.teamID, &r.customerEmail, &r.planTier); scanErr != nil {
			slog.Warn("jobs.checkout_reconcile.scan_failed", "error", scanErr)
			continue
		}
		candidates = append(candidates, r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("CheckoutReconcileWorker: rows error: %w", err)
	}
	_ = rows.Close()

	if len(candidates) == 0 {
		// P1-1 (BugBash 2026-05-19): idle tick — zero candidates carries
		// no operational signal. Demoted INFO → DEBUG; liveness is
		// covered by jobs.middleware.work_ok. INFO is reserved for a
		// tick that actually did work.
		slog.Debug("jobs.checkout_reconcile.completed",
			"emailed", 0,
			"resolved_late", 0,
			"skipped", 0,
			"candidates", 0,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil
	}

	var emailed, resolvedLate, skipped int
	for _, r := range candidates {
		outcome := w.processCandidate(ctx, r)
		switch outcome {
		case checkoutOutcomeEmailed:
			emailed++
		case checkoutOutcomeResolvedLate:
			resolvedLate++
		default:
			skipped++
		}
	}

	slog.Info("jobs.checkout_reconcile.completed",
		"emailed", emailed,
		"resolved_late", resolvedLate,
		"skipped", skipped,
		"candidates", len(candidates),
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}

// checkoutOutcome enumerates the per-candidate result for the tick summary.
type checkoutOutcome int

const (
	// checkoutOutcomeSkipped — nothing was written (already claimed by a
	// sibling replica, or a transient DB error logged and swallowed).
	checkoutOutcomeSkipped checkoutOutcome = iota
	// checkoutOutcomeEmailed — a checkout.abandoned audit row was written
	// and failure_notified_at stamped.
	checkoutOutcomeEmailed
	// checkoutOutcomeResolvedLate — the Razorpay double-check found the
	// subscription active; resolved_at was stamped and no email was sent.
	checkoutOutcomeResolvedLate
)

// processCandidate handles one abandoned-checkout candidate end to end:
// optional Razorpay double-check, then either resolve-late or email.
// Fail-open — any error is logged and the candidate is skipped (a later
// tick retries the still-unresolved row).
func (w *CheckoutReconcileWorker) processCandidate(ctx context.Context, r checkoutRow) checkoutOutcome {
	// Best-effort Razorpay double-check. Skipped entirely when no fetcher
	// is wired — the 15-minute heuristic is sufficient on its own.
	if w.fetcher != nil {
		fetchCtx, cancel := context.WithTimeout(ctx, checkoutReconcileRazorpayTimeout)
		details, fetchErr := w.fetcher.FetchSubscriptionForReconciler(fetchCtx, r.subscriptionID)
		cancel()
		switch {
		case fetchErr != nil:
			// Razorpay unreachable / not configured — fall through to the
			// heuristic and email. Logged at INFO: this is expected when
			// RAZORPAY_KEY_ID is unset (noopSubFetcher returns an error).
			slog.Info("jobs.checkout_reconcile.razorpay_check_unavailable",
				"subscription_id", r.subscriptionID,
				"team_id", r.teamID,
				"error", fetchErr,
				"note", "falling back to 15-minute heuristic",
			)
		case details != nil && checkoutSubscriptionLooksResolved(details):
			// The webhook was merely delayed, not absent — the subscription
			// IS live. Stamp resolved_at and skip the email so the customer
			// never gets a false "you didn't complete" message.
			if err := w.markResolved(ctx, r.subscriptionID); err != nil {
				slog.Warn("jobs.checkout_reconcile.mark_resolved_failed",
					"subscription_id", r.subscriptionID,
					"team_id", r.teamID,
					"error", err,
				)
				return checkoutOutcomeSkipped
			}
			slog.Info("jobs.checkout_reconcile.resolved_late",
				"subscription_id", r.subscriptionID,
				"team_id", r.teamID,
				"razorpay_status", details.Status,
				"paid_count", details.PaidCount,
				"note", "Razorpay reports the subscription active — webhook was delayed; no email sent",
			)
			return checkoutOutcomeResolvedLate
		}
	}

	if err := w.emailAbandonedCheckout(ctx, r); err != nil {
		slog.Warn("jobs.checkout_reconcile.email_failed",
			"subscription_id", r.subscriptionID,
			"team_id", r.teamID,
			"error", err,
		)
		return checkoutOutcomeSkipped
	}
	return checkoutOutcomeEmailed
}

// checkoutSubscriptionLooksResolved reports whether a fetched Razorpay
// subscription should be treated as "the upgrade actually succeeded".
// Either an active/authenticated status (charge done or card authorised)
// or any successful charge cycle (paid_count > 0) counts. Reuses the
// razorpayStatusClass decision table so this stays in lock-step with the
// billing reconciler's notion of "active".
//
// The empty-status guard matters: razorpayStatusClass is a Go map, so an
// unknown OR missing status key returns the zero value — which is
// rzpStatusClassActive (iota 0). Without the explicit "" check a Razorpay
// response with no status field (or a fetch that populated nothing) would
// be mis-classified as active and the customer would never get the
// abandoned-checkout email. Treat "no status" as "not resolved".
func checkoutSubscriptionLooksResolved(d *reconcilerSubscriptionDetails) bool {
	if d == nil {
		return false
	}
	if d.PaidCount > 0 {
		return true
	}
	if d.Status == "" {
		return false
	}
	class, known := razorpayStatusClass[d.Status]
	return known && class == rzpStatusClassActive
}

// markResolved stamps resolved_at on a pending_checkouts row whose
// subscription the Razorpay double-check found active. Guarded on
// resolved_at IS NULL so a racing api webhook that already stamped it is a
// harmless no-op.
func (w *CheckoutReconcileWorker) markResolved(ctx context.Context, subscriptionID string) error {
	_, err := w.db.ExecContext(ctx, `
		UPDATE pending_checkouts
		   SET resolved_at = now()
		 WHERE subscription_id = $1
		   AND resolved_at IS NULL
	`, subscriptionID)
	if err != nil {
		return fmt.Errorf("markResolved: %w", err)
	}
	return nil
}

// emailAbandonedCheckout writes the checkout.abandoned audit_log row and
// stamps failure_notified_at — both inside ONE transaction that claims the
// row with FOR UPDATE SKIP LOCKED. The lock is what makes the job safe
// under replicas:2: a sibling tick that reaches the same row either skips
// it (locked) or, once committed, sees failure_notified_at already set on
// its next candidate SELECT.
//
// The audit row's metadata carries `email` so the EventEmailForwarder can
// resolve the recipient — pending_checkouts customers may have no users
// row yet (the upgrade never completed), so the forwarder's users JOIN
// would yield nothing; metadata.email is the COALESCE fallback it relies
// on (see event_email_forwarder.go fetchBatch).
func (w *CheckoutReconcileWorker) emailAbandonedCheckout(ctx context.Context, r checkoutRow) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("emailAbandonedCheckout: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Claim the row. SKIP LOCKED so a sibling replica's concurrent tick
	// moves on instead of blocking; the resolved_at / failure_notified_at
	// re-check guards against a row that changed state between the outer
	// candidate SELECT and this lock.
	var subscriptionID string
	err = tx.QueryRowContext(ctx, `
		SELECT subscription_id
		FROM pending_checkouts
		WHERE subscription_id = $1
		  AND resolved_at IS NULL
		  AND failure_notified_at IS NULL
		FOR UPDATE SKIP LOCKED
	`, r.subscriptionID).Scan(&subscriptionID)
	if errors.Is(err, sql.ErrNoRows) {
		// Already claimed by a sibling replica, or resolved/notified
		// between the candidate SELECT and now — nothing to do.
		return nil
	}
	if err != nil {
		return fmt.Errorf("emailAbandonedCheckout: claim row: %w", err)
	}

	// Write the audit_log row that triggers the email. resource_type is
	// left empty — a checkout is not a resource. The metadata shape is
	// read back by buildCheckoutAbandoned (event_email_mapping.go).
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, resource_type, summary, metadata)
		VALUES ($1, $2, $3, '', $4,
		        jsonb_build_object(
		            'email',           $5::text,
		            'subscription_id', $6::text,
		            'plan_tier',       $7::text
		        ))
	`, r.teamID, checkoutReconcileActor, auditKindCheckoutAbandoned,
		"Razorpay checkout abandoned — upgrade did not complete",
		r.customerEmail, r.subscriptionID, r.planTier); err != nil {
		return fmt.Errorf("emailAbandonedCheckout: audit insert: %w", err)
	}

	// Stamp failure_notified_at in the SAME transaction as the audit row
	// so the "email triggered" fact and the "don't email again" fact
	// commit atomically — there is no window where one lands without the
	// other.
	if _, err := tx.ExecContext(ctx, `
		UPDATE pending_checkouts
		   SET failure_notified_at = now()
		 WHERE subscription_id = $1
	`, r.subscriptionID); err != nil {
		return fmt.Errorf("emailAbandonedCheckout: stamp failure_notified_at: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("emailAbandonedCheckout: commit: %w", err)
	}

	slog.Info("jobs.checkout_reconcile.emailed",
		"subscription_id", r.subscriptionID,
		"team_id", r.teamID,
		"plan_tier", r.planTier,
		"note", "checkout.abandoned audit_log row written; EventEmailForwarder will dispatch the email",
	)
	return nil
}
