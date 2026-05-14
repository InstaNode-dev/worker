package jobs

// deployment_reminder.go — Wave FIX-J reminder worker.
//
// Every 60s, scan deployments where:
//   - expires_at IS NOT NULL
//   - ttl_policy != 'permanent'
//   - status NOT IN ('deleted', 'expired')
//   - expires_at falls within the next 12h
//   - reminders_sent < 6
//   - last_reminder_at IS NULL OR last_reminder_at < now() - 2h  (cooldown)
//
// For each candidate, CAS-advance reminders_sent (so two ticks don't fire
// the same reminder twice), then dispatch an email AND write a
// deploy.expiring_soon audit_log row. The CAS happens BEFORE the email
// send for the same reason as ExpiryReminderWorker — we accept "never send"
// over "send twice", because the dashboard expiry banner is the
// authoritative warning surface.
//
// Cadence in practice: a deploy that lands at T0 with auto_24h TTL fires
// reminders at T+12h, T+14h, T+16h, T+18h, T+20h, T+22h — six emails over
// the final 12h.
//
// Audit kind: deploy.expiring_soon. Metadata: {deploy_id, team_id,
// reminder_index, hours_remaining, expires_at}.

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

// DeploymentReminderArgs is the River job payload (no fields — runs as a sweep).
type DeploymentReminderArgs struct{}

// Kind is the River job kind.
func (DeploymentReminderArgs) Kind() string { return "deployment_reminder" }

// DeployReminderEmailer is the minimum email surface needed for the
// reminder worker. Extracted as an interface so tests can supply a fake.
type DeployReminderEmailer interface {
	SendDeployExpiring(ctx context.Context, to, deployName, deployURL string, hoursRemaining int, makePermanentURL string) error
}

// DeploymentReminderWorker scans for deployments approaching their TTL
// and dispatches reminder emails + audit rows. Idempotent across ticks
// via the CAS guard on (reminders_sent, last_reminder_at).
type DeploymentReminderWorker struct {
	river.WorkerDefaults[DeploymentReminderArgs]
	db    *sql.DB
	email DeployReminderEmailer

	// lookahead is the warning window (default 12h).
	lookahead time.Duration
	// cooldown is the minimum gap between two reminders on the same
	// deployment (default 2h).
	cooldown time.Duration
}

// NewDeploymentReminderWorker constructs the worker. Pass nil email to
// run in dev (the row will still be CAS-advanced so subsequent ticks don't
// re-attempt).
func NewDeploymentReminderWorker(db *sql.DB, email DeployReminderEmailer) *DeploymentReminderWorker {
	return &DeploymentReminderWorker{
		db:        db,
		email:     email,
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

	// Candidate query — joins users(team_id) to fetch the primary email so a
	// single round-trip carries everything we need to send the email +
	// stamp the row. LIMIT 500 caps fan-out per tick; the worker runs every
	// 60s so a 1000-deploy queue would drain in 2 ticks (no real backlog risk).
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
		  AND d.reminders_sent < 6
		  AND (d.last_reminder_at IS NULL OR d.last_reminder_at <= $3)
		ORDER BY d.expires_at ASC
		LIMIT 500
	`, now, windowEnd, cooldownBefore)
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
		slog.Info("jobs.deployment_reminder.completed",
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
		// short-circuit before sending the email — eliminates the
		// duplicate-send race.
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

		// Audit emit BEFORE the email send. Even if the email send fails,
		// the audit row records that the worker entered the dispatch path
		// — useful for debugging "did we even try to email this user?".
		emitDeployExpiringSoonAudit(w.db, r, hoursRemaining)

		if !r.ownerEmail.Valid || r.ownerEmail.String == "" {
			slog.Warn("jobs.deployment_reminder.no_owner_email",
				"deploy_id", r.id, "team_id", r.teamID,
				"note", "row CAS-advanced; subsequent ticks will skip it")
			skipped++
			continue
		}

		if w.email == nil {
			// Dev mode (no email backend wired). CAS already advanced;
			// treat as a successful skip.
			slog.Info("jobs.deployment_reminder.email_disabled",
				"deploy_id", r.id, "to", r.ownerEmail.String)
			skipped++
			continue
		}

		deployURL := r.appURL.String
		if deployURL == "" {
			deployURL = "https://" + r.appID + ".deployment.instanode.dev"
		}
		makePermanentURL := "https://api.instanode.dev/api/v1/deployments/" + r.id + "/make-permanent"
		if sendErr := w.email.SendDeployExpiring(ctx,
			r.ownerEmail.String, r.appID, deployURL,
			hoursRemaining, makePermanentURL,
		); sendErr != nil {
			slog.Error("jobs.deployment_reminder.send_failed",
				"deploy_id", r.id, "to", r.ownerEmail.String, "error", sendErr,
				"note", "CAS already advanced; will not retry this row this cycle")
			skipped++
			continue
		}

		slog.Info("jobs.deployment_reminder.sent",
			"deploy_id", r.id, "to", r.ownerEmail.String,
			"hours_remaining", hoursRemaining,
			"reminder_index", r.remindersSent+1,
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
		  AND reminders_sent < 6
		  AND (last_reminder_at IS NULL OR last_reminder_at <= $3)
	`, deployIDStr, expectedRemindersSent, cooldownBefore)
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
// Best-effort, fire-and-forget — errors are logged but never bubble up.
// Mirrors the api's emitDeployAudit pattern.
func emitDeployExpiringSoonAudit(db *sql.DB, r deployReminderRow, hoursRemaining int) {
	meta, _ := json.Marshal(map[string]any{
		"deploy_id":       r.id,
		"team_id":         r.teamID,
		"reminder_index":  r.remindersSent + 1,
		"hours_remaining": hoursRemaining,
		"expires_at":      r.expiresAt.UTC().Format(time.RFC3339),
	})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
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
	}()
}
