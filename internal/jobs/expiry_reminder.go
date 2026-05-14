package jobs

// expiry_reminder.go — periodic scan that warns anonymous-tier users their
// 24h-TTL resource is about to disappear.
//
// Migration note (2026-05-14, FOLLOWUP-5): this worker previously called
// EmailClient.SendExpiryReminder directly, which routed to the Resend
// SDK. In production RESEND_API_KEY was unset so every send went to
// NoopClient and customers never got the reminder. It now writes an
// anon.expiry_warning audit_log row instead; the BrevoForwarder (see
// event_email_forwarder.go) drains the row on its next 60s tick and POSTs
// to Brevo using BREVO_TEMPLATE_IDS["anon.expiry_warning"].
//
// Why anon.expiry_warning is a SEPARATE kind from resource.expiry_imminent
// (which expire_imminent.go emits for AUTHENTICATED resources):
//
//   - Audience differs. anon.expiry_warning targets claimed-but-unpaid
//     free-tier resources — the agent_action copy says "claim to keep".
//     resource.expiry_imminent targets paid-tier resources that are
//     hitting their tier ceiling — the agent_action copy says "upgrade
//     to keep".
//   - Dedupe surface differs. anon.expiry_warning uses the resources
//     table's expiry_reminded_at column (one stamp per resource per
//     lifecycle). resource.expiry_imminent uses an audit_log NOT IN
//     subquery (12h window across the audit table).
//
// Dedupe contract (anon flavour): one email per resource per lifecycle.
// resources.expiry_reminded_at is stamped BEFORE the audit insert. If the
// insert fails, the row is logged + skipped — we'll never re-attempt this
// resource. We accept "never send" over "send twice" because email is a
// soft nudge and duplicates erode trust faster than a single missed one.

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
)

// ExpiryReminderArgs is the River job payload — no fields, runs as a sweep.
type ExpiryReminderArgs struct{}

// Kind is the River job kind.
func (ExpiryReminderArgs) Kind() string { return "expiry_reminder" }

// ExpiryReminderWorker scans hourly for claimed-but-unpaid (tier='free')
// resources whose expires_at falls inside the next 4h and writes one
// anon.expiry_warning audit_log row per candidate.
//
// Dedupe is enforced in the database via resources.expiry_reminded_at:
//  1. SELECT filters out rows whose expiry_reminded_at IS NOT NULL.
//  2. We stamp expiry_reminded_at BEFORE writing the audit row.
//
// Stamping before insert means a transient DB blip on the audit insert
// will leave the row stamped-but-unaudited (never reminded). We accept
// that over the double-send risk of stamping after insert.
type ExpiryReminderWorker struct {
	river.WorkerDefaults[ExpiryReminderArgs]
	db *sql.DB

	// reminderWindow is the look-ahead window. Resources whose expires_at
	// falls within (now, now+reminderWindow] are candidates. Default 4h.
	reminderWindow time.Duration
}

// NewExpiryReminderWorker constructs the worker.
func NewExpiryReminderWorker(db *sql.DB) *ExpiryReminderWorker {
	return &ExpiryReminderWorker{
		db:             db,
		reminderWindow: 4 * time.Hour,
	}
}

// expiryReminderRow is the projection of resources + users used by the worker.
type expiryReminderRow struct {
	resourceID   uuid.UUID
	teamID       uuid.UUID
	resourceType string
	expiresAt    time.Time
	ownerEmail   sql.NullString
}

// Work executes one sweep.
//
// Returned error semantics match the surrounding workers (expire.go,
// expire_imminent.go): a top-level DB failure returns an error so River
// retries; per-row failures are logged but never propagate.
func (w *ExpiryReminderWorker) Work(ctx context.Context, job *river.Job[ExpiryReminderArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.expiry_reminder")
	defer span.End()

	start := time.Now()
	now := time.Now().UTC()
	windowEnd := now.Add(w.reminderWindow)

	rows, err := w.db.QueryContext(ctx, `
		SELECT r.id, r.team_id, r.resource_type, r.expires_at, u.email
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
		if scanErr := rows.Scan(&r.resourceID, &r.teamID, &r.resourceType, &r.expiresAt, &r.ownerEmail); scanErr != nil {
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
		// Stamp first — see worker doc above. Even if the audit insert
		// fails or the team has no owner email, we never re-attempt
		// this row.
		if _, stampErr := w.db.ExecContext(ctx, `
			UPDATE resources
			SET expiry_reminded_at = now()
			WHERE id = $1 AND expiry_reminded_at IS NULL
		`, r.resourceID); stampErr != nil {
			slog.Error("jobs.expiry_reminder.stamp_failed",
				"resource_id", r.resourceID.String(),
				"error", stampErr,
			)
			continue
		}

		if !r.ownerEmail.Valid || r.ownerEmail.String == "" {
			slog.Warn("jobs.expiry_reminder.no_owner_email",
				"resource_id", r.resourceID.String(),
				"note", "row stamped to prevent re-evaluation",
			)
			skipped++
			continue
		}

		hoursRemaining := int(math.Ceil(r.expiresAt.Sub(now).Hours()))
		if hoursRemaining < 1 {
			hoursRemaining = 1
		}

		if auditErr := emitAnonExpiryWarningAudit(ctx, w.db, r, hoursRemaining); auditErr != nil {
			// Fail open: a transient audit_log insert failure shouldn't
			// block other rows. The stamp has already been recorded so
			// we won't retry this resource.
			slog.Error("jobs.expiry_reminder.audit_insert_failed",
				"resource_id", r.resourceID.String(),
				"to", r.ownerEmail.String,
				"error", auditErr,
			)
			skipped++
			continue
		}

		slog.Info("jobs.expiry_reminder.audited",
			"resource_id", r.resourceID.String(),
			"to", r.ownerEmail.String,
			"resource_type", r.resourceType,
			"hours_remaining", hoursRemaining,
			"note", "audit_log row written; BrevoForwarder will dispatch the email",
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

// emitAnonExpiryWarningAudit writes one anon.expiry_warning audit_log row
// carrying every field buildAnonExpiryWarning (event_email_mapping.go) reads
// into the Brevo template params.
//
// Metadata shape:
//   resource_id     — UUID of the expiring resource (also dedupe surface)
//   resource_type   — postgres / redis / mongodb / etc.
//   hours_remaining — int >= 1; rounded up
//   expires_at      — RFC3339 timestamp
//   email           — recipient address (forwarder resolves separately, but
//                     including here lets template substitution short-circuit)
func emitAnonExpiryWarningAudit(ctx context.Context, db *sql.DB, r expiryReminderRow, hoursRemaining int) error {
	meta, _ := json.Marshal(map[string]any{
		"resource_id":     r.resourceID.String(),
		"resource_type":   r.resourceType,
		"hours_remaining": hoursRemaining,
		"expires_at":      r.expiresAt.UTC().Format(time.RFC3339),
		"email":           r.ownerEmail.String,
	})

	summary := fmt.Sprintf("%s resource expiring in %dh (claim to keep)",
		htmlEscape(r.resourceType), hoursRemaining)

	_, err := db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, kind, actor, resource_type, summary, metadata)
		VALUES ($1, $2, 'system', $3, $4, $5)
	`, r.teamID, auditKindAnonExpiryWarning, r.resourceType, summary, meta)
	if err != nil {
		return fmt.Errorf("emitAnonExpiryWarningAudit insert: %w", err)
	}
	return nil
}
