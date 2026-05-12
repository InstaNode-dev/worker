package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
)

// ExpiryReminderArgs is the River job payload — no fields, runs as a sweep.
type ExpiryReminderArgs struct{}

// Kind is the River job kind.
func (ExpiryReminderArgs) Kind() string { return "expiry_reminder" }

// ExpiryReminderEmailer is the minimum surface the worker needs from the
// EmailClient. Extracted as an interface so tests can supply a fake without
// pulling in the live Resend SDK.
type ExpiryReminderEmailer interface {
	SendExpiryReminder(ctx context.Context, to, resourceType string, hoursRemaining int) error
}

// ExpiryReminderWorker scans hourly for claimed-but-unpaid (tier='free') resources
// whose expires_at falls inside the next ExpiryReminderWindow (default 4h) and
// emails the team's owner a one-shot reminder.
//
// Dedupe is enforced in the database via resources.expiry_reminded_at:
//   1. SELECT filters out rows whose expiry_reminded_at IS NOT NULL.
//   2. After (or before) the email send, the row is stamped with now().
//
// We stamp expiry_reminded_at BEFORE attempting the Resend call. Stamping
// after the call would risk double-sending if the worker crashes mid-send;
// stamping before risks NOT sending if Resend is down. We accept "never
// send" over "send twice" because the dashboard expiry banner is the
// authoritative warning surface — email is a soft nudge, and duplicate
// reminders erode trust faster than a single missed one.
type ExpiryReminderWorker struct {
	river.WorkerDefaults[ExpiryReminderArgs]
	db    *sql.DB
	email ExpiryReminderEmailer

	// ReminderWindow is the look-ahead window. Resources whose expires_at
	// falls within (now, now+ReminderWindow] are candidates. Default 4h.
	reminderWindow time.Duration
}

// NewExpiryReminderWorker constructs the worker. Pass nil email to disable
// sends (the row will still be stamped so the next pass doesn't re-attempt —
// useful when RESEND_API_KEY is unset in dev).
func NewExpiryReminderWorker(db *sql.DB, email ExpiryReminderEmailer) *ExpiryReminderWorker {
	return &ExpiryReminderWorker{
		db:             db,
		email:          email,
		reminderWindow: 4 * time.Hour,
	}
}

// expiryReminderRow is the projection of resources + users used by the worker.
type expiryReminderRow struct {
	resourceID   string
	resourceType string
	expiresAt    time.Time
	ownerEmail   sql.NullString
}

// Work executes one sweep.
//
// Returned error semantics match the surrounding workers (expire.go,
// quota.go): a top-level DB failure returns an error so River retries;
// per-row failures are logged but never propagate, so one bad row never
// blocks the rest of the batch.
func (w *ExpiryReminderWorker) Work(ctx context.Context, job *river.Job[ExpiryReminderArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.expiry_reminder")
	defer span.End()

	start := time.Now()
	now := time.Now().UTC()
	windowEnd := now.Add(w.reminderWindow)

	// Step 1: candidate query. We LEFT JOIN users(team_id) so a team without
	// any registered owner email still surfaces — we log + skip the send but
	// stamp the row so the next pass doesn't re-evaluate it.
	//
	// LIMIT 500 caps the per-tick fan-out; a worker-restart will pick up
	// the rest on the next 1h tick. The reminder window is 4h, so we have
	// plenty of headroom before any row would slip past unprocessed.
	rows, err := w.db.QueryContext(ctx, `
		SELECT r.id::text, r.resource_type, r.expires_at, u.email
		FROM resources r
		LEFT JOIN users u ON u.team_id = r.team_id
		WHERE r.team_id IS NOT NULL
		  AND r.tier = 'free'
		  AND r.status = 'active'
		  AND r.expires_at IS NOT NULL
		  AND r.expires_at > $1
		  AND r.expires_at <= $2
		  AND r.expiry_reminded_at IS NULL
		ORDER BY r.expires_at ASC
		LIMIT 500
	`, now, windowEnd)
	if err != nil {
		return fmt.Errorf("ExpiryReminderWorker: query failed: %w", err)
	}
	defer rows.Close()

	var candidates []expiryReminderRow
	for rows.Next() {
		var r expiryReminderRow
		if scanErr := rows.Scan(&r.resourceID, &r.resourceType, &r.expiresAt, &r.ownerEmail); scanErr != nil {
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
			"sent", 0,
			"candidates", 0,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil
	}

	var sent, skipped int
	for _, r := range candidates {
		// Stamp first — see worker doc above. Even if the email send
		// fails or the team has no owner email, we never re-attempt
		// this row. Errors here are logged and the row is skipped.
		if _, stampErr := w.db.ExecContext(ctx, `
			UPDATE resources
			SET expiry_reminded_at = now()
			WHERE id = $1 AND expiry_reminded_at IS NULL
		`, r.resourceID); stampErr != nil {
			slog.Error("jobs.expiry_reminder.stamp_failed",
				"resource_id", r.resourceID,
				"error", stampErr,
			)
			continue
		}

		if !r.ownerEmail.Valid || r.ownerEmail.String == "" {
			slog.Warn("jobs.expiry_reminder.no_owner_email",
				"resource_id", r.resourceID,
				"note", "row stamped to prevent re-evaluation",
			)
			skipped++
			continue
		}

		if w.email == nil {
			// Dev mode (no Resend key plumbed). We've already stamped
			// the row; treat this as a successful skip so the row
			// won't be retried.
			slog.Info("jobs.expiry_reminder.email_disabled",
				"resource_id", r.resourceID,
				"to", r.ownerEmail.String,
			)
			skipped++
			continue
		}

		hoursRemaining := int(math.Ceil(r.expiresAt.Sub(now).Hours()))
		if hoursRemaining < 1 {
			hoursRemaining = 1
		}

		if sendErr := w.email.SendExpiryReminder(ctx, r.ownerEmail.String, r.resourceType, hoursRemaining); sendErr != nil {
			// Fail open: Resend transient outage shouldn't block other
			// rows. The stamp has already been recorded so we won't
			// retry — see worker doc above for the rationale.
			slog.Error("jobs.expiry_reminder.send_failed",
				"resource_id", r.resourceID,
				"to", r.ownerEmail.String,
				"error", sendErr,
			)
			skipped++
			continue
		}

		slog.Info("jobs.expiry_reminder.sent",
			"resource_id", r.resourceID,
			"to", r.ownerEmail.String,
			"resource_type", r.resourceType,
			"hours_remaining", hoursRemaining,
		)
		sent++
	}

	slog.Info("jobs.expiry_reminder.completed",
		"sent", sent,
		"skipped", skipped,
		"candidates", len(candidates),
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}
