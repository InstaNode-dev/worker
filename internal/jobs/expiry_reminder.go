package jobs

// expiry_reminder.go — periodic scan that warns free-tier users that
// their 24h-TTL resource is about to expire.
//
// Cadence (2026-05-15 rework): up to three reminders per resource at
// the 12h, 6h, and 1h marks before expires_at. Each stage advances
// resources.reminders_sent via CAS so two concurrent sweeps cannot
// double-fire the same stage. last_reminder_at provides a 30-min
// cooldown floor as a safety net against overlap (e.g. if a TTL gets
// bumped after a reminder fires).
//
// Why 3 stages instead of 1:
//   - The legacy single reminder fired inside a 4h window and
//     stamped expiry_reminded_at; users who provisioned a resource
//     and then context-switched routinely missed it.
//   - 12h / 6h / 1h gives one nudge at end-of-day, one at "soon",
//     and one final urgent warning. Anything more becomes spam.
//   - Mirrors deployment_reminder.go's cadence shape but with a
//     fixed 3-stage schedule keyed on time-to-expiry rather than
//     a cooldown counter — the resource TTL is short enough (24h)
//     that staged thresholds beat a count+cooldown.
//
// Stage bucketing (hours remaining → stage index):
//   (12h, 6h]  → stage 1 ("First reminder, 12h to go")
//   (6h,  1h]  → stage 2 ("Halfway, 6h to go")
//   (1h,   0]  → stage 3 ("Final, 1h to go")
//
// Email channel: this worker does NOT call the email provider
// directly. It writes one anon.expiry_warning audit_log row per
// stage. event_email_forwarder.go drains the row on its next 60s
// tick and dispatches via Brevo using BREVO_TEMPLATE_IDS["anon.expiry_warning"].
// Brevo template should read {{ params.hours_remaining }},
// {{ params.reminder_index }}, {{ params.resource_type }},
// {{ params.expires_at }}, {{ params.token_prefix }},
// {{ params.upgrade_url }}, {{ params.resource_url }}.
//
// Dedupe contract: per stage, at most one email. CAS on
// resources.reminders_sent is the dedupe surface. We stamp the
// counter BEFORE writing the audit row — if the audit insert
// fails, the row is logged + skipped. We accept "never send" over
// "send twice" because duplicates erode trust faster than a missed
// nudge. (Same posture as the legacy single-stamp version.)

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
)

// ExpiryReminderArgs is the River job payload — no fields, runs as a sweep.
type ExpiryReminderArgs struct{}

// Kind is the River job kind.
func (ExpiryReminderArgs) Kind() string { return "expiry_reminder" }

// reminderStage describes one bucket in the 12h/6h/1h schedule.
type reminderStage struct {
	index          int           // 1-based; matches reminder_index in the email
	expiresWithin  time.Duration // resource fires this stage when expires_at <= now + expiresWithin
	mustHaveSent   int           // CAS guard: only fire if reminders_sent == this value
	label          string        // logging label, also flows into the email as stage_label
}

// reminderSchedule is the canonical stage table. Ordered most-distant
// to most-imminent so a single sweep can detect & advance every stage
// a resource is eligible for. In practice one tick fires at most one
// stage per resource because the sweep cadence (30 min) is denser
// than the gap between stages.
var reminderSchedule = []reminderStage{
	{index: 1, expiresWithin: 12 * time.Hour, mustHaveSent: 0, label: "stage_12h"},
	{index: 2, expiresWithin: 6 * time.Hour, mustHaveSent: 1, label: "stage_6h"},
	{index: 3, expiresWithin: 1 * time.Hour, mustHaveSent: 2, label: "stage_1h"},
}

// reminderCooldown is the minimum wall-clock gap between two
// dispatches on the same resource. Belt-and-braces — the CAS on
// mustHaveSent is the primary dedupe surface. The cooldown only
// kicks in if a TTL bump pushes a resource that already received
// stage N back into stage N+1's window before enough time has
// elapsed.
const reminderCooldown = 30 * time.Minute

// ExpiryReminderWorker scans every sweep for free-tier resources
// whose expires_at falls inside any of the configured stage
// windows and writes one anon.expiry_warning audit_log row per
// (resource, stage). Dedupe is enforced by the CAS on
// resources.reminders_sent. Email dispatch happens out-of-band
// in event_email_forwarder.go.
type ExpiryReminderWorker struct {
	river.WorkerDefaults[ExpiryReminderArgs]
	db *sql.DB

	// dashboardBaseURL is the origin used to render resource detail
	// and upgrade links in the email body. Defaults to instanode.dev.
	dashboardBaseURL string
}

// NewExpiryReminderWorker constructs the worker. dashboardBaseURL may
// be empty; the constructor falls back to "https://instanode.dev".
func NewExpiryReminderWorker(db *sql.DB) *ExpiryReminderWorker {
	return &ExpiryReminderWorker{
		db:               db,
		dashboardBaseURL: "https://instanode.dev",
	}
}

// expiryReminderRow is the projection of resources + users used by the worker.
// reminders_sent and key_prefix are added in 046_resources_reminder_stages.sql
// / pre-existing 006_key_prefix.sql respectively.
type expiryReminderRow struct {
	resourceID    uuid.UUID
	teamID        uuid.UUID
	resourceType  string
	expiresAt     time.Time
	remindersSent int
	keyPrefix     sql.NullString
	ownerEmail    sql.NullString
}

// Work executes one sweep across all three stage windows.
//
// Error semantics:
//   - Top-level DB failure returns an error so River retries.
//   - Per-row failures are logged but never propagate (one bad row
//     never blocks the rest).
//   - audit_log INSERT failures are fail-open: the row is already
//     stamped, so the worker will not retry that resource.
func (w *ExpiryReminderWorker) Work(ctx context.Context, job *river.Job[ExpiryReminderArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.expiry_reminder")
	defer span.End()

	start := time.Now()
	now := time.Now().UTC()
	// The outermost window is the largest stage threshold — anything
	// outside this window is too far from expiry to be a candidate
	// for any stage. Inner windows are checked per-stage below.
	windowEnd := now.Add(reminderSchedule[0].expiresWithin)
	cooldownBefore := now.Add(-reminderCooldown)

	rows, err := w.db.QueryContext(ctx, `
		SELECT r.id, r.team_id, r.resource_type, r.expires_at,
		       r.reminders_sent, r.key_prefix, u.email
		FROM resources r
		LEFT JOIN users u ON u.team_id = r.team_id
		WHERE r.team_id IS NOT NULL
		  AND r.tier = 'free'
		  AND r.status = 'active'
		  AND r.expires_at IS NOT NULL
		  AND r.expires_at > $1
		  AND r.expires_at <= $2
		  AND r.reminders_sent < 3
		  AND (r.last_reminder_at IS NULL OR r.last_reminder_at < $3)
		ORDER BY r.expires_at ASC
		LIMIT 500
	`, now, windowEnd, cooldownBefore)
	if err != nil {
		return fmt.Errorf("ExpiryReminderWorker: query failed: %w", err)
	}
	defer rows.Close()

	var candidates []expiryReminderRow
	for rows.Next() {
		var r expiryReminderRow
		if scanErr := rows.Scan(&r.resourceID, &r.teamID, &r.resourceType, &r.expiresAt,
			&r.remindersSent, &r.keyPrefix, &r.ownerEmail); scanErr != nil {
			slog.Warn("jobs.expiry_reminder.scan_failed", "error", scanErr)
			continue
		}
		candidates = append(candidates, r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("ExpiryReminderWorker: rows error: %w", err)
	}
	rows.Close()

	if len(candidates) == 0 {
		slog.Info("jobs.expiry_reminder.completed",
			"emitted", 0,
			"candidates", 0,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil
	}

	var emitted, skipped int
	for _, r := range candidates {
		stage, ok := selectStage(r, now)
		if !ok {
			// Resource is inside the outermost window but not yet
			// inside the next stage's bucket — wait for a later tick.
			slog.Debug("jobs.expiry_reminder.not_yet_eligible",
				"resource_id", r.resourceID.String(),
				"reminders_sent", r.remindersSent,
				"hours_remaining", r.expiresAt.Sub(now).Hours(),
			)
			continue
		}

		hoursRemaining := hoursLeft(r.expiresAt, now)

		// CAS-advance: only stamp if reminders_sent is still at the
		// expected predecessor value. Two concurrent sweeps can't both
		// satisfy this for the same resource — exactly one wins.
		stampRes, stampErr := w.db.ExecContext(ctx, `
			UPDATE resources
			SET reminders_sent = $1,
			    last_reminder_at = now(),
			    expiry_reminded_at = COALESCE(expiry_reminded_at, now())
			WHERE id = $2 AND reminders_sent = $3
		`, stage.index, r.resourceID, stage.mustHaveSent)
		if stampErr != nil {
			slog.Error("jobs.expiry_reminder.stamp_failed",
				"resource_id", r.resourceID.String(),
				"stage", stage.label,
				"error", stampErr,
			)
			continue
		}
		affected, _ := stampRes.RowsAffected()
		if affected == 0 {
			// Another worker advanced the counter between SELECT and
			// UPDATE. Skip without logging an error — this is the CAS
			// contract working correctly.
			continue
		}

		if !r.ownerEmail.Valid || r.ownerEmail.String == "" {
			slog.Warn("jobs.expiry_reminder.no_owner_email",
				"resource_id", r.resourceID.String(),
				"stage", stage.label,
				"note", "stamp advanced; no email recipient available",
			)
			skipped++
			continue
		}

		if auditErr := w.emitAnonExpiryWarningAudit(ctx, r, stage, hoursRemaining); auditErr != nil {
			slog.Error("jobs.expiry_reminder.audit_insert_failed",
				"resource_id", r.resourceID.String(),
				"stage", stage.label,
				"to", r.ownerEmail.String,
				"error", auditErr,
			)
			skipped++
			continue
		}

		slog.Info("jobs.expiry_reminder.audited",
			"resource_id", r.resourceID.String(),
			"stage", stage.label,
			"reminder_index", stage.index,
			"to", r.ownerEmail.String,
			"resource_type", r.resourceType,
			"hours_remaining", hoursRemaining,
		)
		emitted++
	}

	slog.Info("jobs.expiry_reminder.completed",
		"emitted", emitted,
		"skipped", skipped,
		"candidates", len(candidates),
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}

// selectStage picks the stage a row is currently eligible for, given
// reminders_sent and time-to-expiry. Returns (stage, true) when a stage
// matches, (_, false) otherwise.
//
// A row inside the (12h, 1h] window with reminders_sent=0 fires stage 1.
// A row inside the (6h, 1h] window with reminders_sent=1 fires stage 2.
// A row inside the (1h, 0] window with reminders_sent=2 fires stage 3.
// A row inside (12h, 6h] with reminders_sent=1 is not eligible (stage 2
// not yet reached); the next tick re-evaluates.
func selectStage(r expiryReminderRow, now time.Time) (reminderStage, bool) {
	remaining := r.expiresAt.Sub(now)
	if remaining <= 0 {
		return reminderStage{}, false
	}
	for _, s := range reminderSchedule {
		if r.remindersSent != s.mustHaveSent {
			continue
		}
		if remaining <= s.expiresWithin {
			return s, true
		}
	}
	return reminderStage{}, false
}

// hoursLeft rounds the gap up to whole hours, with a floor of 1 so
// the email never says "0 hours". The legacy worker had the same
// floor (it just always rendered the floor due to a Brevo template bug).
func hoursLeft(expires, now time.Time) int {
	delta := expires.Sub(now)
	if delta <= time.Hour {
		return 1
	}
	hours := int(delta.Hours())
	if delta-time.Duration(hours)*time.Hour > 0 {
		hours++
	}
	if hours < 1 {
		hours = 1
	}
	return hours
}

// emitAnonExpiryWarningAudit writes one anon.expiry_warning audit_log
// row carrying every field the Brevo template needs to render the
// email body. The forwarder picks the row up on its next 60s tick.
//
// Metadata shape (each key MUST be a string — copyMetaStr in
// event_email_mapping.go does not coerce):
//
//	resource_id     — UUID of the expiring resource
//	resource_type   — postgres / redis / mongodb / etc.
//	hours_remaining — int >= 1 stringified
//	expires_at      — RFC3339 timestamp (UTC)
//	reminder_index  — "1" | "2" | "3"
//	stage_label     — human label ("stage_12h" / "stage_6h" / "stage_1h")
//	token_prefix    — first 8 chars of the token (key_prefix) for the
//	                  recipient to identify the resource; never the full
//	                  token. Empty string when the column is null.
//	upgrade_url     — link to the dashboard billing page with hobby preselected
//	resource_url    — link to the resource detail page in the dashboard
//	email           — recipient address (also resolved separately by the
//	                  forwarder; including here lets template substitution
//	                  short-circuit if the recipient join ever drops)
func (w *ExpiryReminderWorker) emitAnonExpiryWarningAudit(ctx context.Context, r expiryReminderRow, stage reminderStage, hoursRemaining int) error {
	tokenPrefix := ""
	if r.keyPrefix.Valid {
		tokenPrefix = r.keyPrefix.String
	}

	base := strings.TrimRight(w.dashboardBaseURL, "/")
	upgradeURL := base + "/app/billing?upgrade=hobby&source=expiry_reminder&stage=" + stage.label
	resourceURL := base + "/app/resources/" + r.resourceID.String()

	meta, _ := json.Marshal(map[string]string{
		"resource_id":     r.resourceID.String(),
		"resource_type":   r.resourceType,
		"hours_remaining": fmt.Sprintf("%d", hoursRemaining),
		"expires_at":      r.expiresAt.UTC().Format(time.RFC3339),
		"reminder_index":  fmt.Sprintf("%d", stage.index),
		"stage_label":     stage.label,
		"token_prefix":    tokenPrefix,
		"upgrade_url":     upgradeURL,
		"resource_url":    resourceURL,
		"email":           r.ownerEmail.String,
	})

	summary := fmt.Sprintf("%s resource expiring in %dh (reminder %d/3, claim to keep)",
		htmlEscape(r.resourceType), hoursRemaining, stage.index)

	_, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, kind, actor, resource_type, summary, metadata)
		VALUES ($1, $2, 'system', $3, $4, $5)
	`, r.teamID, auditKindAnonExpiryWarning, r.resourceType, summary, meta)
	if err != nil {
		return fmt.Errorf("emitAnonExpiryWarningAudit insert: %w", err)
	}
	return nil
}
