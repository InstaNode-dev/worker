package jobs

// payment_grace_reminder.go — periodic job that walks the
// payment_grace_periods table looking for teams in 'active' grace whose
// last reminder was sent >6h ago (or never), and writes a
// `payment.grace_reminder` audit_log row plus stamps last_reminder_at.
//
// Cadence: every 6h (per the user directive "we should be giving regular
// warning in every six hours"). At the maximum 7-day grace window that
// gives ~28 reminders, matching the schema comment in migration 027.
//
// Why this is a sweep, not a fan-out: the population (teams currently in
// grace) is tiny — single digits to low hundreds in steady-state, since
// the funnel is "card declined → 7d grace → recover or terminate" and
// most teams recover within hours. A single table sweep per tick is
// cheaper than scheduling per-team River jobs.
//
// Idempotency: per-team UPDATE … RETURNING with a WHERE clause that
// re-checks the 6h cadence at write time. If two ticks race (e.g. one is
// retried by River after a transient failure) only the first one
// satisfies the WHERE and writes the audit row — the second sees zero
// rows affected and skips silently. This is the same pattern
// expiry_reminder.go uses with the resources.expiry_reminded_at column.
//
// Brevo delivery: this job does NOT call Brevo. The audit_log row IS the
// trigger — the event_email_forwarder (event_email_forwarder.go) drains
// payment.grace_reminder rows into the configured email provider on its
// own 60s cadence. Keeping the trigger and the send pipeline separated
// means a Brevo outage doesn't pin the reminder cadence (the trigger
// still fires, the forwarder retries).

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

// PaymentGraceReminderArgs is the River job payload — no fields.
type PaymentGraceReminderArgs struct{}

// Kind is the River worker key.
func (PaymentGraceReminderArgs) Kind() string { return "payment_grace_reminder" }

// paymentGraceReminderInterval is the dispatch cadence. 6h matches the
// minimum reminder gap; the WHERE clause in the candidate query enforces
// the actual per-team gap so a stray re-fire never doubles a reminder.
const paymentGraceReminderInterval = 6 * time.Hour

// paymentGraceReminderGap is the minimum interval between two reminders
// for the same team. Per the brief: "every six hours". Kept as a const
// (not derived from the interval above) so a future refactor that runs
// the sweep more often doesn't accidentally tighten the customer-facing
// cadence.
const paymentGraceReminderGap = 6 * time.Hour

// paymentGraceReminderBatchLimit caps per-tick fan-out. The population
// is small in practice; this is belt-and-braces against a runaway state.
const paymentGraceReminderBatchLimit = 500

// auditKindPaymentGraceReminder is the audit_log.kind value this job
// writes. Matches api/internal/models.AuditKindPaymentGraceReminder.
// Centralised here because the worker module does not import api models.
const auditKindPaymentGraceReminder = "payment.grace_reminder"

// paymentGraceReminderActor — system-actor convention shared across the
// worker's periodic emitters.
const paymentGraceReminderActor = "system"

// PaymentGraceReminderWorker scans the dunning table for due reminders.
type PaymentGraceReminderWorker struct {
	river.WorkerDefaults[PaymentGraceReminderArgs]
	db *sql.DB
}

// NewPaymentGraceReminderWorker constructs the worker.
func NewPaymentGraceReminderWorker(db *sql.DB) *PaymentGraceReminderWorker {
	return &PaymentGraceReminderWorker{db: db}
}

// paymentGraceReminderRow is the projection the worker reads.
type paymentGraceReminderRow struct {
	id        uuid.UUID
	teamID    uuid.UUID
	expiresAt time.Time
}

// Work runs one sweep.
func (w *PaymentGraceReminderWorker) Work(ctx context.Context, job *river.Job[PaymentGraceReminderArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.payment_grace_reminder")
	defer span.End()

	start := time.Now()
	now := time.Now().UTC()
	cutoff := now.Add(-paymentGraceReminderGap)

	// Candidate query: active grace, expires_at still in the future
	// (terminator handles past-expiry), and either no reminder yet or
	// the last reminder is older than the gap. Sorted by oldest-reminder
	// first so a backlog drains in FIFO order.
	rows, err := w.db.QueryContext(ctx, `
		SELECT id, team_id, expires_at
		FROM payment_grace_periods
		WHERE status = 'active'
		  AND expires_at > $1
		  AND (last_reminder_at IS NULL OR last_reminder_at < $2)
		ORDER BY last_reminder_at ASC NULLS FIRST, started_at ASC
		LIMIT $3
	`, now, cutoff, paymentGraceReminderBatchLimit)
	if err != nil {
		return fmt.Errorf("PaymentGraceReminderWorker: query failed: %w", err)
	}
	defer rows.Close()

	var candidates []paymentGraceReminderRow
	for rows.Next() {
		var r paymentGraceReminderRow
		if scanErr := rows.Scan(&r.id, &r.teamID, &r.expiresAt); scanErr != nil {
			slog.Warn("jobs.payment_grace_reminder.scan_failed", "error", scanErr)
			continue
		}
		candidates = append(candidates, r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("PaymentGraceReminderWorker: rows error: %w", err)
	}
	rows.Close()

	if len(candidates) == 0 {
		slog.Info("jobs.payment_grace_reminder.completed",
			"reminded", 0,
			"candidates", 0,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil
	}

	var reminded, skipped int
	for _, r := range candidates {
		// Atomic stamp: UPDATE … WHERE status='active' AND the cadence
		// is still due. If another worker (or this worker on retry)
		// already stamped the row, the WHERE matches zero rows and we
		// skip the audit emit. This is the same pattern used by
		// expiry_reminder.go.
		res, upErr := w.db.ExecContext(ctx, `
			UPDATE payment_grace_periods
			   SET last_reminder_at = $1,
			       reminders_sent   = reminders_sent + 1
			 WHERE id = $2
			   AND status = 'active'
			   AND (last_reminder_at IS NULL OR last_reminder_at < $3)
		`, now, r.id, cutoff)
		if upErr != nil {
			slog.Warn("jobs.payment_grace_reminder.update_failed",
				"grace_id", r.id.String(),
				"team_id", r.teamID.String(),
				"error", upErr,
			)
			skipped++
			continue
		}
		n, rowsErr := res.RowsAffected()
		if rowsErr != nil || n == 0 {
			// Either the row moved out of 'active' between SELECT and
			// UPDATE (recovered / terminated) or another worker beat
			// us to the stamp. Either way: nothing to do.
			skipped++
			continue
		}

		hoursRemaining := math.Round(r.expiresAt.Sub(now).Hours()*10) / 10
		meta := map[string]any{
			"grace_id":         r.id.String(),
			"hours_remaining":  hoursRemaining,
			"grace_ends_at":    r.expiresAt.UTC().Format(time.RFC3339),
		}
		metaBytes, mErr := json.Marshal(meta)
		if mErr != nil {
			slog.Error("jobs.payment_grace_reminder.metadata_marshal_failed",
				"grace_id", r.id.String(),
				"error", mErr,
			)
			skipped++
			continue
		}
		summary := fmt.Sprintf("payment grace reminder — %.1fh remaining", hoursRemaining)
		if _, insErr := w.db.ExecContext(ctx, `
			INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
			VALUES ($1, $2, $3, $4, $5)
		`, r.teamID, paymentGraceReminderActor, auditKindPaymentGraceReminder, summary, metaBytes); insErr != nil {
			// The UPDATE above already stamped last_reminder_at, so the
			// next sweep won't re-attempt this row for another 6h. The
			// audit row miss means the customer won't get an email for
			// this slot — log loudly so an operator can intervene.
			slog.Error("jobs.payment_grace_reminder.audit_insert_failed",
				"grace_id", r.id.String(),
				"team_id", r.teamID.String(),
				"error", insErr,
			)
			skipped++
			continue
		}
		reminded++
		slog.Info("jobs.payment_grace_reminder.reminded",
			"grace_id", r.id.String(),
			"team_id", r.teamID.String(),
			"hours_remaining", hoursRemaining,
		)
	}

	slog.Info("jobs.payment_grace_reminder.completed",
		"reminded", reminded,
		"skipped", skipped,
		"candidates", len(candidates),
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}
