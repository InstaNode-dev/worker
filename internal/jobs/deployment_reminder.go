package jobs

// deployment_reminder.go — Wave FIX-J reminder worker.
//
// Every 60s, scan deployments where:
//   - expires_at IS NOT NULL
//   - ttl_policy != 'permanent'
//   - status NOT IN ('deleted', 'expired')
//   - expires_at falls within the next 12h
//   - reminders_sent < maxDeployReminders (3)
//   - last_reminder_at IS NULL OR last_reminder_at < now() - 2h  (cooldown)
//
// F3 (BugBash 2026-05-19, escalation tightened Wave 3 / 2026-05-21):
// the cadence used to be SIX identical emails over the final 12h
// (T+12/14/16/18/20/22h) — read as spam. The first F3 fix cut it to
// three subjects, but the spacing was still flat (2h cooldown), so
// "Final reminder" landed with ~8h remaining instead of right before
// expiry. Wave 3 changed the spacing too: each subsequent reminder is
// gated on a strictly tighter time-to-expiry (12h → 6h → 1h), so
// "Final reminder" actually fires near expiry. See
// deployReminderStageThresholds + maxDeployReminders below, and the
// reminder_index-keyed subject in renderDeployExpiringSoon.
//
// For each candidate, CAS-advance reminders_sent (so two ticks don't fire
// the same reminder twice), then write a deploy.expiring_soon audit_log
// row carrying every param the email template needs. The BrevoForwarder
// (event_email_forwarder.go) picks up that audit row on its next 60s tick
// and POSTs to Brevo. The CAS happens BEFORE the audit insert for the
// same reason as ExpiryReminderWorker — we accept "never send" over
// "send twice".
//
// Email-send migration (2026-05-14, FIX-I/J→Brevo migration):
//   - PREVIOUSLY: this worker called EmailClient.SendDeployExpiring
//     directly. In production RESEND_API_KEY was unset, so all sends went
//     to NoopClient and customers never got reminders.
//   - NOW: the audit_log row IS the trigger. event_email_forwarder.go
//     joins users + audit_log, calls eventEmailBuilders[kind] to build
//     params, and POSTs to Brevo using BREVO_TEMPLATE_IDS[kind].
//   - The metadata MUST carry every field the email body needs
//     (deploy_url, make_permanent_url, app_id) — see emitDeployExpiringSoonAudit
//     below.
//
// Cadence in practice (Wave 3 escalating cadence): a deploy that lands
// at T0 with auto_24h TTL fires Stage 1 ("Heads up") at T+12h (12h to
// expiry), Stage 2 ("Reminder") at T+18h (6h to expiry), and Stage 3
// ("Final reminder") at T+23h (1h to expiry). The 2h cooldown is now
// belt-and-suspenders against tick-straddle double-fire; the per-stage
// time-to-expiry gate (deployReminderStageThresholds) does the heavy
// lifting.
//
// Audit kind: deploy.expiring_soon. Metadata: {deploy_id, team_id,
// reminder_index, hours_remaining, expires_at, app_id, deploy_url,
// make_permanent_url}.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	"instant.dev/worker/internal/metrics"
)

// maxDeployReminders caps how many deploy-expiry reminders fire per
// deployment (F3, BugBash 2026-05-19). 3 stages — "Heads up" / "Reminder"
// / "Final reminder" — matching the anon.expiry_warning escalating
// cadence. Was 6, which produced six identical emails over 12h.
const maxDeployReminders = 3

// deployReminderStageThresholds defines the per-stage time-to-expiry
// thresholds for an *actually escalating* cadence (F3 follow-up,
// Wave 3 / BugBash 2026-05-21).
//
// Previously the worker used a flat 2h cooldown gate inside a 12h
// lookahead, producing reminders at roughly T-12h/T-10h/T-8h before
// expiry — "Final reminder" landed with 8h still on the clock, which
// reads as another routine ping, not an urgent last warning.
//
// The new thresholds gate each reminder on the time-remaining-to-expiry,
// so the gap narrows as expiry approaches and "Final reminder" actually
// fires near expiry. Index i corresponds to reminders_sent = i (i.e.
// the i-th stage to fire, 0-indexed).
//
//	[0] = 12h  → Stage 1 "Heads up"        : fires when expires_at - now <= 12h
//	[1] = 6h   → Stage 2 "Reminder"        : fires when expires_at - now <= 6h
//	[2] = 1h   → Stage 3 "Final reminder"  : fires when expires_at - now <= 1h
//
// For a deploy with the maximum supported TTL (auto_24h), Stage 1 fires
// once the deploy is past the 12h-remaining mark; for shorter TTLs the
// earlier stages collapse forward (a 4h deploy fires Stage 1 immediately,
// then Stage 2 at T-6h-capped-to-now, etc.). The MIN-cooldown of one
// candidate-tick window (60s) prevents accidental double-fire when two
// thresholds straddle the same tick.
//
// MUST satisfy: deployReminderStageThresholds[i] > deployReminderStageThresholds[i+1]
// (strictly decreasing). See TestDeploymentReminder_StageThresholds_Pinned.
var deployReminderStageThresholds = [maxDeployReminders]time.Duration{
	12 * time.Hour, // Stage 1: Heads up
	6 * time.Hour,  // Stage 2: Reminder
	1 * time.Hour,  // Stage 3: Final reminder
}

// nextReminderThreshold returns the time-to-expiry threshold that gates
// the NEXT reminder for a deployment that has already fired
// `remindersSent` reminders. Returns 0 if no further reminder is allowed
// (i.e. all stages have already fired).
//
// Exported via package boundary for unit tests; nothing in production
// calls this — the SQL query inlines the equivalent CASE expression.
func nextReminderThreshold(remindersSent int) time.Duration {
	if remindersSent < 0 || remindersSent >= maxDeployReminders {
		return 0
	}
	return deployReminderStageThresholds[remindersSent]
}

// DeploymentReminderArgs is the River job payload (no fields — runs as a sweep).
type DeploymentReminderArgs struct{}

// Kind is the River job kind.
func (DeploymentReminderArgs) Kind() string { return "deployment_reminder" }

// DeploymentReminderWorker scans for deployments approaching their TTL
// and writes deploy.expiring_soon audit_log rows. Idempotent across ticks
// via the CAS guard on (reminders_sent, last_reminder_at). The actual
// email dispatch is handled by event_email_forwarder.go (BrevoForwarder).
type DeploymentReminderWorker struct {
	river.WorkerDefaults[DeploymentReminderArgs]
	db *sql.DB

	// lookahead is the warning window (default 12h).
	lookahead time.Duration
	// cooldown is the minimum gap between two reminders on the same
	// deployment (default 2h).
	cooldown time.Duration
}

// NewDeploymentReminderWorker constructs the worker. The email argument
// is accepted (and ignored) to preserve the existing call site signature
// in workers.go while the Resend→Brevo migration ships; emails are now
// dispatched by event_email_forwarder.go off the audit_log row this worker
// writes. The parameter will be removed in a follow-up once all callers
// are updated.
func NewDeploymentReminderWorker(db *sql.DB, _ any) *DeploymentReminderWorker {
	return &DeploymentReminderWorker{
		db:        db,
		lookahead: 12 * time.Hour,
		cooldown:  2 * time.Hour,
	}
}

// deployReminderRow is the projection scanned out of the candidate query.
type deployReminderRow struct {
	id             string // deployments.id::text
	teamID         string
	appID          string
	appURL         sql.NullString
	expiresAt      time.Time
	remindersSent  int
	ttlPolicy      string
	ownerEmail     sql.NullString
}

// Work executes one reminder sweep.
//
// Error semantics: a top-level DB failure returns an error so River
// retries. Per-row failures are logged but never propagate — one bad
// row never blocks the rest.
func (w *DeploymentReminderWorker) Work(ctx context.Context, job *river.Job[DeploymentReminderArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.deployment_reminder")
	defer span.End()

	start := time.Now()
	now := time.Now().UTC()
	windowEnd := now.Add(w.lookahead)
	cooldownBefore := now.Add(-w.cooldown)

	// Per-stage thresholds in seconds (Postgres interval-arithmetic friendly).
	// Index in SQL is `reminders_sent` (0/1/2 → stage 1/2/3). The CASE picks
	// the threshold that gates the *next* reminder. F3 follow-up Wave 3:
	// candidates now ALSO require `expires_at - now <= stage_threshold`, so
	// Stage 3 ("Final reminder") fires only inside the final 1h, not 8h out.
	stage1Sec := int64(deployReminderStageThresholds[0].Seconds())
	stage2Sec := int64(deployReminderStageThresholds[1].Seconds())
	stage3Sec := int64(deployReminderStageThresholds[2].Seconds())

	// Candidate query — joins users(team_id) to fetch the primary email so a
	// single round-trip carries everything we need to send the email +
	// stamp the row. LIMIT 500 caps fan-out per tick; the worker runs every
	// 60s so a 1000-deploy queue would drain in 2 ticks (no real backlog risk).
	//
	// The (d.expires_at - $1) <= stage_threshold predicate enforces the
	// actually-escalating cadence: each subsequent reminder is gated on a
	// strictly tighter time-to-expiry. The 2h cooldown ($3) is still a
	// belt-and-suspenders against accidental double-fire within one
	// stage's window if a tick straddles the threshold.
	rows, err := w.db.QueryContext(ctx, `
		SELECT d.id::text, d.team_id::text, d.app_id, d.app_url,
		       d.expires_at, d.reminders_sent, d.ttl_policy, u.email
		FROM deployments d
		LEFT JOIN users u ON u.team_id = d.team_id AND u.is_primary = true
		WHERE d.expires_at IS NOT NULL
		  AND d.ttl_policy != 'permanent'
		  AND d.status NOT IN ('deleted', 'expired')
		  AND d.expires_at > $1
		  AND d.expires_at <= $2
		  AND d.reminders_sent < $4
		  AND (d.last_reminder_at IS NULL OR d.last_reminder_at <= $3)
		  AND (d.expires_at - $1) <= (
		    CASE d.reminders_sent
		      WHEN 0 THEN make_interval(secs => $5)
		      WHEN 1 THEN make_interval(secs => $6)
		      WHEN 2 THEN make_interval(secs => $7)
		      ELSE make_interval(secs => 0)
		    END
		  )
		ORDER BY d.expires_at ASC
		LIMIT 500
	`, now, windowEnd, cooldownBefore, maxDeployReminders, stage1Sec, stage2Sec, stage3Sec)
	if err != nil {
		return fmt.Errorf("DeploymentReminderWorker: query failed: %w", err)
	}
	defer rows.Close()

	var candidates []deployReminderRow
	for rows.Next() {
		var r deployReminderRow
		if scanErr := rows.Scan(&r.id, &r.teamID, &r.appID, &r.appURL,
			&r.expiresAt, &r.remindersSent, &r.ttlPolicy, &r.ownerEmail); scanErr != nil {
			slog.Warn("jobs.deployment_reminder.scan_failed", "error", scanErr)
			continue
		}
		candidates = append(candidates, r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("DeploymentReminderWorker: rows error: %w", err)
	}
	rows.Close()

	// Sample TTL state for the gauge — counts apply to BOTH the
	// candidates-in-window set and the broader population. The gauge
	// label split by ttl_policy lets the NR dashboard show "auto_24h
	// deploys live" vs "permanent deploys live".
	w.sampleTTLGauge(ctx)

	if len(candidates) == 0 {
		// P1-1 (BugBash 2026-05-19): idle tick — demoted INFO → DEBUG.
		// deployment_reminder runs every 60s; an idle INFO every minute
		// is heartbeat noise. Liveness via jobs.middleware.work_ok.
		slog.Debug("jobs.deployment_reminder.completed",
			"sent", 0,
			"candidates", 0,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil
	}

	metrics.DeployExpiringSoonTotal.Add(float64(len(candidates)))

	var sent, skipped int
	for _, r := range candidates {
		// CAS-advance reminders_sent + last_reminder_at. If another tick
		// already advanced the row, this returns rowsAffected=0 and we
		// short-circuit — eliminates the duplicate-fire race.
		advanced, casErr := advanceReminderCAS(ctx, w.db, r.id, r.remindersSent, w.cooldown)
		if casErr != nil {
			slog.Warn("jobs.deployment_reminder.cas_failed",
				"deploy_id", r.id, "error", casErr)
			skipped++
			continue
		}
		if !advanced {
			// Another tick won the CAS race; skip this row.
			skipped++
			continue
		}

		// Compute hours remaining (always >= 1 — "expires in 0h" reads as
		// already-gone). Used in the email subject + audit metadata.
		hoursRemaining := int(math.Ceil(r.expiresAt.Sub(now).Hours()))
		if hoursRemaining < 1 {
			hoursRemaining = 1
		}

		// Audit emit — the BrevoForwarder picks this up on its next tick
		// and POSTs to Brevo using BREVO_TEMPLATE_IDS["deploy.expiring_soon"].
		// The metadata MUST carry every field the email template needs
		// (deploy_url, make_permanent_url, app_id, hours_remaining).
		deployURL := r.appURL.String
		if deployURL == "" {
			deployURL = "https://" + r.appID + ".deployment.instanode.dev"
		}
		makePermanentURL := "https://api.instanode.dev/api/v1/deployments/" + r.id + "/make-permanent"
		// B19-FIND-2 (BugBash 2026-05-20): pass parent ctx so the audit
		// INSERT inherits the tracer span — the audit row appears in the
		// same NR trace as the reminder tick that produced it.
		emitDeployExpiringSoonAudit(ctx, w.db, r, hoursRemaining, deployURL, makePermanentURL)

		if !r.ownerEmail.Valid || r.ownerEmail.String == "" {
			// The forwarder's LEFT JOIN users will resolve a recipient at
			// dispatch time — we don't block the audit on it here.
			slog.Info("jobs.deployment_reminder.audited_no_owner",
				"deploy_id", r.id, "team_id", r.teamID,
				"note", "forwarder will resolve recipient at dispatch")
		}

		slog.Info("jobs.deployment_reminder.audited",
			"deploy_id", r.id,
			"hours_remaining", hoursRemaining,
			"reminder_index", r.remindersSent+1,
			"note", "audit_log row written; BrevoForwarder will dispatch the email",
		)
		metrics.DeployRemindersSentTotal.Inc()
		sent++
	}

	slog.Info("jobs.deployment_reminder.completed",
		"sent", sent,
		"skipped", skipped,
		"candidates", len(candidates),
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}

// sampleTTLGauge updates the per-policy gauge for the NR dashboard.
// Best-effort: a query failure is logged but doesn't block the rest of
// the sweep. Aggregations need explicit caching + freshness reasoning
// (per feedback memory) — this gauge has a 60s freshness window because
// it's sampled on every reminder tick.
func (w *DeploymentReminderWorker) sampleTTLGauge(ctx context.Context) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT ttl_policy, count(*)
		FROM deployments
		WHERE status NOT IN ('deleted', 'expired')
		GROUP BY ttl_policy
	`)
	if err != nil {
		slog.Warn("jobs.deployment_reminder.gauge_sample_failed", "error", err)
		return
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var policy string
		var n int
		if scanErr := rows.Scan(&policy, &n); scanErr != nil {
			continue
		}
		metrics.DeployTTLStateGauge.WithLabelValues(policy).Set(float64(n))
		seen[policy] = true
	}
	// Zero-out policies that had rows on a previous tick but now don't
	// — prevents stale gauges from sticking around forever.
	for _, p := range []string{"auto_24h", "permanent", "custom"} {
		if !seen[p] {
			metrics.DeployTTLStateGauge.WithLabelValues(p).Set(0)
		}
	}
}

// advanceReminderCAS issues the CAS UPDATE that protects against two ticks
// firing the same reminder. Returns true when this caller WAS the one to
// advance the row (and is therefore responsible for the email send).
//
// expectedRemindersSent is the value the caller read from the candidate
// query. The CAS only succeeds when the row's reminders_sent still
// equals that value AND the cooldown gate is still satisfied.
func advanceReminderCAS(ctx context.Context, db *sql.DB, deployIDStr string, expectedRemindersSent int, cooldown time.Duration) (bool, error) {
	cooldownBefore := time.Now().UTC().Add(-cooldown)
	res, err := db.ExecContext(ctx, `
		UPDATE deployments
		SET reminders_sent = reminders_sent + 1,
		    last_reminder_at = now()
		WHERE id = $1
		  AND reminders_sent = $2
		  AND reminders_sent < $4
		  AND (last_reminder_at IS NULL OR last_reminder_at <= $3)
	`, deployIDStr, expectedRemindersSent, cooldownBefore, maxDeployReminders)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// emitDeployExpiringSoonAudit writes one row to audit_log for a reminder.
// Mirrors the api's emitDeployAudit pattern.
//
// Migration note (2026-05-14, FIX-I/J→Brevo): this audit row IS the
// trigger for the customer email — the BrevoForwarder (see
// event_email_forwarder.go) drains audit_log every 60s, joins users to
// resolve the recipient, calls buildDeployExpiringSoon to build params,
// and POSTs to Brevo using BREVO_TEMPLATE_IDS["deploy.expiring_soon"].
// The metadata fields below MUST stay in sync with buildDeployExpiringSoon
// in event_email_mapping.go.
//
// IMPORTANT: this function is now SYNCHRONOUS (was previously goroutine'd).
// The reason: the CAS guard in advanceReminderCAS only protects against
// duplicate sends, NOT against "CAS succeeded but audit insert never
// happened" — and dropping the audit means dropping the email. Running
// the insert synchronously means a transient DB blip surfaces as a logged
// WARN; the CAS still holds, so the row won't be re-tried this cycle.
// The acceptable failure mode is "one missed reminder" not "no reminder
// for this run".
//
// B19-FIND-2 (BugBash 2026-05-20): accept a parent ctx so the tracer
// span from Work() is propagated into the audit INSERT. The 3s deadline
// is now derived from context.WithoutCancel(parent) — keeps trace
// metadata, decouples cancellation so an outer tick boundary doesn't
// kill a synchronous audit insert mid-flight.
func emitDeployExpiringSoonAudit(parent context.Context, db *sql.DB, r deployReminderRow, hoursRemaining int, deployURL, makePermanentURL string) {
	meta, _ := json.Marshal(map[string]any{
		"deploy_id":          r.id,
		"team_id":            r.teamID,
		"app_id":             r.appID,
		"reminder_index":     r.remindersSent + 1,
		"hours_remaining":    hoursRemaining,
		"expires_at":         r.expiresAt.UTC().Format(time.RFC3339),
		"deploy_url":         deployURL,
		"make_permanent_url": makePermanentURL,
	})
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 3*time.Second)
	defer cancel()
	teamUUID, parseErr := uuid.Parse(r.teamID)
	if parseErr != nil {
		slog.Warn("jobs.deployment_reminder.audit.bad_team_id",
			"team_id", r.teamID, "error", parseErr)
		return
	}
	_, err := db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, kind, actor, resource_type, summary, metadata)
		VALUES ($1, 'deploy.expiring_soon', 'system', 'deploy', $2, $3)
	`, teamUUID, "deploy "+r.appID+" expiring soon", meta)
	if err != nil {
		slog.Warn("jobs.deployment_reminder.audit.insert_failed",
			"deploy_id", r.id, "error", err)
	}
}
