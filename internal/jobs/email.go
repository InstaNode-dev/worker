package jobs

// email.go — Resend→Brevo migration tail (FOLLOWUP-5, 2026-05-14).
//
// The legacy EmailClient (thin Resend wrapper + four lifecycle methods —
// SendTrialWarning / SendTrialExpired / SendWeeklyDigest / SendExpiryReminder)
// was deleted as part of this file's previous revision. All live lifecycle
// emails now flow through the audit_log → event_email_forwarder → Brevo
// pipeline (see event_email_forwarder.go + event_email_mapping.go).
//
// What lives in this file post-migration:
//   - TrialExpiry* removed entirely — per project memory rule
//     `project_no_trial_pay_day_one.md`, the platform has NO trial period.
//     There is anonymous (24h TTL, free) and there are paid tiers from
//     day one. The TrialExpiryWorker scanned for trial_ends_at columns
//     that never get written in practice, so every tick was a no-op
//     candidate scan that emitted exactly zero emails — pure dead code.
//   - WeeklyDigestWorker stays — it's a live feature. Refactored to write
//     a digest.weekly audit_log row instead of calling Resend directly.
//     The BrevoForwarder picks the row up on its next 60s tick and POSTs
//     to Brevo using BREVO_TEMPLATE_IDS["digest.weekly"].
//   - The shared DigestResourceCount type + buildResourceDigestCounts
//     helper used by WeeklyDigestWorker stays here for organisational
//     proximity. ExpiryReminderWorker lives in expiry_reminder.go and is
//     also rewritten to emit anon.expiry_warning audit_log rows.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
)

// DigestResourceCount holds per-resource-type counts for the weekly digest.
// Exported so unit tests in package `jobs_test` can build expected payloads.
type DigestResourceCount struct {
	ResourceType string
	Count        int64
}

// htmlEscape was used by the old Resend HTML body builders. Kept here as a
// package-private helper because expiry_reminder.go references it for
// summary-line composition. Trivial replace-chain, no allocations on the
// fast path (most resource_type values pass through unchanged).
func htmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, `"`, "&quot;")
	return s
}

// ---------------------------------------------------------------------------
// WeeklyDigestWorker — runs every Monday at 08:00 UTC
// ---------------------------------------------------------------------------

// WeeklyDigestArgs is the River job payload (empty — runs as a sweep).
type WeeklyDigestArgs struct{}

// Kind is the River job kind.
func (WeeklyDigestArgs) Kind() string { return "weekly_digest" }

// WeeklyDigestWorker iterates over non-anonymous teams and writes one
// digest.weekly audit_log row per team-user pair. The BrevoForwarder
// drains the rows on its next 60s tick. We never call Brevo (or any
// email provider) from this worker — the audit row IS the trigger.
//
// Migration note (2026-05-14, FOLLOWUP-5): previously this worker called
// EmailClient.SendWeeklyDigest directly. In production RESEND_API_KEY was
// unset, so every send went to NoopClient and customers got nothing.
// The audit-log route guarantees the email actually fires via the Brevo
// provider — see event_email_forwarder.go for the dispatch loop.
type WeeklyDigestWorker struct {
	river.WorkerDefaults[WeeklyDigestArgs]
	db *sql.DB
}

// NewWeeklyDigestWorker constructs a WeeklyDigestWorker. Kept exported so
// jobs_test (external package) can build a worker without depending on the
// unexported field; also matches the constructor convention used by the
// rest of the worker package.
func NewWeeklyDigestWorker(db *sql.DB) *WeeklyDigestWorker {
	return &WeeklyDigestWorker{db: db}
}

// Work iterates over all non-anonymous teams and writes one digest.weekly
// audit_log row per (team, user) — the BrevoForwarder picks it up on its
// next tick and POSTs to Brevo using BREVO_TEMPLATE_IDS["digest.weekly"].
func (w *WeeklyDigestWorker) Work(ctx context.Context, job *river.Job[WeeklyDigestArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.weekly_digest")
	defer span.End()

	rows, err := w.db.QueryContext(ctx, `
		SELECT u.email, t.id, COALESCE(t.name, '')
		FROM users u
		JOIN teams t ON t.id = u.team_id
		WHERE t.plan_tier != 'anonymous'
		ORDER BY u.email
	`)
	if err != nil {
		return fmt.Errorf("weekly_digest.Work query users: %w", err)
	}
	defer rows.Close()

	type userTeam struct {
		email    string
		teamID   uuid.UUID
		teamName string
	}
	var targets []userTeam

	for rows.Next() {
		var ut userTeam
		if err := rows.Scan(&ut.email, &ut.teamID, &ut.teamName); err != nil {
			slog.Error("jobs.weekly_digest.scan_users", "error", err)
			continue
		}
		targets = append(targets, ut)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("weekly_digest.Work rows: %w", err)
	}

	var emitted, skipped int
	for _, ut := range targets {
		stats, err := w.buildResourceDigestCounts(ctx, ut.teamID)
		if err != nil {
			slog.Error("jobs.weekly_digest.build_stats",
				"team_id", ut.teamID,
				"email", ut.email,
				"error", err,
			)
			skipped++
			continue
		}

		if err := emitWeeklyDigestAudit(ctx, w.db, ut.teamID, ut.email, ut.teamName, stats); err != nil {
			slog.Error("jobs.weekly_digest.audit_insert_failed",
				"team_id", ut.teamID,
				"email", ut.email,
				"error", err,
			)
			skipped++
			continue
		}

		slog.Info("jobs.weekly_digest.audited",
			"team_id", ut.teamID,
			"email", ut.email,
			"resource_type_rows", len(stats),
			"note", "audit_log row written; BrevoForwarder will dispatch the email",
		)
		emitted++
	}

	slog.Info("jobs.weekly_digest.completed",
		"emitted", emitted,
		"skipped", skipped,
		"candidates", len(targets),
	)
	return nil
}

func (w *WeeklyDigestWorker) buildResourceDigestCounts(ctx context.Context, teamID uuid.UUID) ([]DigestResourceCount, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT resource_type, COUNT(*)::bigint
		FROM resources
		WHERE team_id = $1
		  AND resource_type IN ('postgres', 'redis', 'mongodb', 'queue', 'storage', 'webhook')
		  AND status != 'deleted'
		GROUP BY resource_type
		ORDER BY resource_type ASC
	`, teamID)
	if err != nil {
		return nil, fmt.Errorf("buildResourceDigestCounts query: %w", err)
	}
	defer rows.Close()

	var stats []DigestResourceCount
	for rows.Next() {
		var s DigestResourceCount
		if err := rows.Scan(&s.ResourceType, &s.Count); err != nil {
			return nil, fmt.Errorf("buildResourceDigestCounts scan: %w", err)
		}
		stats = append(stats, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("buildResourceDigestCounts rows: %w", err)
	}
	return stats, nil
}

// emitWeeklyDigestAudit writes one digest.weekly audit_log row carrying
// every field the Brevo template body needs (email, team name, the
// per-resource-type count breakdown). Surfaces are kept narrow: the
// summary is a one-line human string the Brevo template can fall back
// to if a per-row count is missing.
//
// Metadata shape — buildDigestWeekly (event_email_mapping.go) reads these:
//   email                   — recipient address (also the forwarder's resolver)
//   team_name               — display name for the email greeting (may be "")
//   total_active_resources  — sum across all resource_types; 0 means "empty week"
//   resource_breakdown      — JSON-encoded array of {resource_type, count}
//                             (templates that don't render a table can ignore)
func emitWeeklyDigestAudit(ctx context.Context, db *sql.DB, teamID uuid.UUID, email, teamName string, stats []DigestResourceCount) error {
	var total int64
	for _, s := range stats {
		total += s.Count
	}

	breakdown, err := json.Marshal(stats)
	if err != nil {
		// json.Marshal on a slice of primitive-struct values can't fail
		// in practice. If it ever does we still want the audit row to
		// land, so fall back to an empty array.
		slog.Warn("jobs.weekly_digest.breakdown_marshal_failed",
			"team_id", teamID, "error", err)
		breakdown = []byte(`[]`)
	}

	meta, _ := json.Marshal(map[string]any{
		"email":                  email,
		"team_name":              teamName,
		"total_active_resources": total,
		"resource_breakdown":     string(breakdown),
	})

	summary := fmt.Sprintf("weekly digest: %d active resources across %d type(s)",
		total, len(stats))

	_, err = db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, kind, actor, resource_type, summary, metadata)
		VALUES ($1, $2, 'system', '', $3, $4)
	`, teamID, auditKindDigestWeekly, summary, meta)
	if err != nil {
		return fmt.Errorf("emitWeeklyDigestAudit insert: %w", err)
	}
	return nil
}

