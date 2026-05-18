package jobs

// churn_predictor.go — daily scan that writes a `churn.risk_flagged` audit_log
// row for every team that looks like it's drifting toward churn. The Brevo
// forwarder picks the row up on its next tick and triggers the
// "we_miss_you" lifecycle email via the configured provider.
//
// Churn-risk definition (per the brief):
//
//   1. team.plan_tier IS NOT 'team' — Team customers get hand-rolled
//      outreach, not an automated email.
//   2. MAX(audit_log.created_at) for "activity" kinds is more than
//      churnInactivityWindow ago — i.e. the team has done nothing in 7+
//      days. "Nothing" means: no provision, no deploy, no vault op, no
//      experiment, no login. See activity-kind discussion below.
//   3. The team still has active resources (resources.status='active').
//      A team that has already churned to zero resources is past the
//      "we miss you" window — there's nothing left to retain.
//   4. The team has not been flagged inside churnDedupeWindow. One
//      "we miss you" email per 30 days is the design intent; flagging
//      faster would feel pestering, slower would miss customers who
//      drifted away after an initial 30-day silence.
//
// ─── Activity-kind matching: known production gap ─────────────────────
//
// The brief enumerates activity kinds as:
//   provision, deploy.*, vault.*, experiment.*, login
//
// Only `provision`, `experiment.conversion`, and `env_policy.updated`
// are actually emitted as audit_log rows in the API today (grep
// confirmed across api/internal/handlers/). The `deploy.*`, `vault.*`,
// and `login` kinds are listed as intended values in the
// audit_log table's `kind` column comment (migration 012_audit_log.sql)
// but no producer writes them yet.
//
// We match the brief's full list via SQL `kind LIKE` patterns:
//
//   kind = 'provision'  OR  kind = 'login'
//   OR kind LIKE 'deploy%'
//   OR kind LIKE 'vault.%'
//   OR kind LIKE 'experiment.%'
//
// Forward-compatible: the day a deploy handler starts writing
// `deploy.created`, this job picks it up with no churn-predictor change.
// Today's behaviour: activity = "any `provision` row" OR
// "any `experiment.conversion` row". Operators should be aware that
// "no login activity in 7d" cannot mean "user hasn't logged in" until
// the auth path emits an audit row.
//
// ─── Schema notes ────────────────────────────────────────────────────
//
// The owner's email is resolved via `users.is_primary = true` — the
// canonical primary user of the team. Migration 029
// (uq_users_one_primary_per_team) guarantees exactly one such row per
// team. This matches the convention in expire_imminent.go,
// expiry_reminder.go and deployment_expirer.go — consistent across the
// worker.

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

// ChurnPredictorArgs is the River job payload — no fields, runs as a sweep.
type ChurnPredictorArgs struct{}

// Kind is the River worker key.
func (ChurnPredictorArgs) Kind() string { return "churn_predictor" }

// churnPredictorInterval is the periodic dispatch cadence — daily.
// The 30-day dedupe window means cadence below ~daily produces no
// additional flags; daily gives at most one chance per day to catch a
// team that crossed the 7-day threshold yesterday.
const churnPredictorInterval = 24 * time.Hour

// churnInactivityWindow is the silence period that defines a churn
// candidate. 7 days is the brief's threshold — long enough that a
// vacation or busy week doesn't trigger a "we miss you" email
// (most active dev workflows touch the platform at least weekly),
// short enough that a drifting customer gets reached before they
// fully detach.
const churnInactivityWindow = 7 * 24 * time.Hour

// churnDedupeWindow is the minimum gap between two churn.risk_flagged
// rows for the same team. 30 days is the brief's window. Rationale:
//
//   * Slower than 30d would let a team get re-flagged month after month
//     without intervention — operationally noisy and emotionally
//     repetitive ("you again missed us") for the customer.
//   * Faster than 30d (e.g. weekly) would risk sending the same
//     "we miss you" copy four times in a month to a customer who has
//     decided not to return; the perception cost (spammy) outweighs
//     the recall benefit.
//   * 30d also aligns with monthly billing cycles — most B2B SaaS
//     reactivation playbooks rate-limit on a monthly cadence for the
//     same reason.
//
// A 35-day-flagged team that's still silent gets re-flagged on the
// 31st day; this is intentional and the second-pass test pins it.
const churnDedupeWindow = 30 * 24 * time.Hour

// churnRiskKind is the audit_log.kind value written by this job.
// The Brevo forwarder maps this exact string to a template id via
// the BREVO_TEMPLATE_IDS env-var JSON object.
const churnRiskKind = "churn.risk_flagged"

// churnRiskActor is the audit_log.actor value for system-written rows.
// Matches the convention used by quota_wall_nudge.go and expire_imminent.go.
const churnRiskActor = "system"

// churnPredictorBatchLimit caps the per-tick fan-out. A backlog larger
// than this drains across consecutive daily runs; the 30-day dedupe
// absorbs any reordering. 500 matches expire_imminent's limit and is
// comfortable for a single daily scan.
const churnPredictorBatchLimit = 500

// ChurnPredictorWorker scans every non-Team team for churn-risk signal
// (7+ days inactivity, still has resources, not recently flagged) and
// writes one churn.risk_flagged audit_log row per qualifying team.
type ChurnPredictorWorker struct {
	river.WorkerDefaults[ChurnPredictorArgs]
	db *sql.DB
}

// NewChurnPredictorWorker constructs a ChurnPredictorWorker.
func NewChurnPredictorWorker(db *sql.DB) *ChurnPredictorWorker {
	return &ChurnPredictorWorker{db: db}
}

// churnCandidateRow is the projection the worker scans over.
type churnCandidateRow struct {
	teamID              uuid.UUID
	planTier            string
	ownerEmail          sql.NullString
	lastActivity        sql.NullTime
	teamCreatedAt       time.Time
	activeResourceCount int64
}

// Work executes one daily sweep.
//
// Returned error semantics match the rest of the worker: a top-level
// query failure returns an error so River retries; per-row failures
// (insert errors, missing owner email) are logged and skipped, so one
// bad row never blocks the rest of the batch.
func (w *ChurnPredictorWorker) Work(ctx context.Context, job *river.Job[ChurnPredictorArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.churn_predictor")
	defer span.End()

	start := time.Now()
	now := time.Now().UTC()
	inactivityCutoff := now.Add(-churnInactivityWindow)
	dedupeCutoff := now.Add(-churnDedupeWindow)

	// Single-query candidate scan — no N+1.
	//
	// The LEFT JOIN on users … is_primary=true picks the team's canonical
	// primary user (same convention as expire_imminent.go and
	// expiry_reminder.go). Migration 029 (uq_users_one_primary_per_team)
	// guarantees exactly one match. A team with no primary user surfaces
	// as NULL email; we skip the row at the application layer (see per-row
	// handling below) so the brief's "include email in metadata" contract
	// is preserved.
	//
	// The LEFT JOIN on audit_log carries activity kinds via LIKE
	// patterns so the query is forward-compatible the day deploy/vault/
	// login start emitting rows. See the file-level comment for the
	// production-gap discussion.
	//
	// HAVING is unavoidable here — we filter on aggregates
	// (MAX(activity), COUNT(resources)) plus an EXISTS NOT-clause for
	// dedupe. The 30-day dedupe lives in the WHERE-NOT-EXISTS clause
	// so the GROUP BY only includes teams we haven't recently flagged.
	//
	// P2-13 (BugBash 2026-05-18): the HAVING clause MUST tolerate a NULL
	// MAX(a.created_at). A team that signed up and went silent on day one
	// has ZERO matching activity rows, so MAX(a.created_at) is NULL.
	// `NULL < $3` evaluates to NULL (treated as false) — the bare
	// `MAX(a.created_at) < $3` predicate silently excluded the single
	// most churn-prone cohort. The `OR MAX(a.created_at) IS NULL` arm
	// keeps zero-activity teams in the result set; the per-row scan loop
	// below already falls back to teams.created_at via the
	// `lastActivity.Valid == false` branch.
	rows, err := w.db.QueryContext(ctx, `
		SELECT
			t.id            AS team_id,
			t.plan_tier     AS plan_tier,
			COALESCE(u.email, '') AS owner_email,
			MAX(a.created_at)     AS last_activity,
			t.created_at          AS team_created_at,
			COUNT(r.id)           AS active_resource_count
		FROM teams t
		LEFT JOIN users u ON u.team_id = t.id AND u.is_primary = true
		LEFT JOIN audit_log a ON a.team_id = t.id
			AND (
				a.kind = 'provision'
				OR a.kind = 'login'
				OR a.kind LIKE 'deploy%'
				OR a.kind LIKE 'vault.%'
				OR a.kind LIKE 'experiment.%'
			)
		LEFT JOIN resources r ON r.team_id = t.id AND r.status = 'active'
		WHERE t.plan_tier != 'team'
		  AND NOT EXISTS (
			SELECT 1 FROM audit_log f
			WHERE f.team_id = t.id
			  AND f.kind = $1
			  AND f.created_at > $2
		  )
		GROUP BY t.id, t.plan_tier, t.created_at, u.email
		HAVING (MAX(a.created_at) < $3 OR MAX(a.created_at) IS NULL)
		  AND COUNT(r.id) > 0
		ORDER BY MAX(a.created_at) ASC NULLS FIRST
		LIMIT $4
	`, churnRiskKind, dedupeCutoff, inactivityCutoff, churnPredictorBatchLimit)
	if err != nil {
		return fmt.Errorf("ChurnPredictorWorker: query failed: %w", err)
	}
	defer rows.Close()

	var candidates []churnCandidateRow
	for rows.Next() {
		var r churnCandidateRow
		var emailStr string
		if scanErr := rows.Scan(&r.teamID, &r.planTier, &emailStr, &r.lastActivity, &r.teamCreatedAt, &r.activeResourceCount); scanErr != nil {
			slog.Warn("jobs.churn_predictor.scan_failed", "error", scanErr)
			continue
		}
		if emailStr != "" {
			r.ownerEmail = sql.NullString{String: emailStr, Valid: true}
		}
		candidates = append(candidates, r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("ChurnPredictorWorker: rows error: %w", err)
	}
	rows.Close()

	if len(candidates) == 0 {
		slog.Info("jobs.churn_predictor.completed",
			"flagged", 0,
			"candidates", 0,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil
	}

	var flagged, skipped int
	for _, r := range candidates {
		// Defensive: a team can exist without a primary user (orphan /
		// pre-signup). Brevo keys events on email, so a churn.risk_flagged
		// row with no addressable email is dead weight — skip and log so an
		// operator can investigate.
		if !r.ownerEmail.Valid || r.ownerEmail.String == "" {
			slog.Warn("jobs.churn_predictor.no_owner_email",
				"team_id", r.teamID.String(),
				"plan_tier", r.planTier,
				"note", "skipped — no email to address",
			)
			skipped++
			continue
		}

		// last_activity may be NULL — a team that signed up and went
		// silent on day one has zero matching activity rows, so
		// MAX(a.created_at) is NULL. P2-13 (BugBash 2026-05-18): the
		// HAVING clause now keeps these rows (`OR MAX(...) IS NULL`),
		// so this branch IS reachable and must produce a sane number.
		// Fall back to teams.created_at — "days since the team was
		// created" is the correct inactivity measure for a team that
		// never logged a single activity event. The prior code used
		// the zero time.Time{}, yielding a nonsensical ~739000-day
		// figure that leaked into the email metadata.
		var daysSince float64
		if r.lastActivity.Valid {
			daysSince = math.Round(now.Sub(r.lastActivity.Time).Hours()/24*10) / 10
		} else {
			daysSince = math.Round(now.Sub(r.teamCreatedAt).Hours()/24*10) / 10
		}
		if daysSince < 0 {
			daysSince = 0
		}

		summary := fmt.Sprintf(
			"%s-tier team inactive for %.1f days with %d active resources — flagged for retention outreach",
			r.planTier, daysSince, r.activeResourceCount,
		)

		// Metadata carries the contract the Brevo forwarder reads from
		// (event_email_mapping.go::buildChurnRiskFlagged). Every field
		// the brief enumerated lives here: tier, last_activity_days_ago,
		// active_resource_count, email.
		meta := map[string]any{
			"tier":                    r.planTier,
			"last_activity_days_ago":  daysSince,
			"active_resource_count":   r.activeResourceCount,
			"email":                   r.ownerEmail.String,
		}
		metaBytes, mErr := json.Marshal(meta)
		if mErr != nil {
			// json.Marshal on a map[string]any of primitives can't
			// fail in practice; treat as a logged skip just in case.
			slog.Error("jobs.churn_predictor.metadata_marshal_failed",
				"team_id", r.teamID.String(),
				"error", mErr,
			)
			skipped++
			continue
		}

		if _, insErr := w.db.ExecContext(ctx, `
			INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
			VALUES ($1, $2, $3, $4, $5)
		`, r.teamID, churnRiskActor, churnRiskKind, summary, metaBytes); insErr != nil {
			slog.Error("jobs.churn_predictor.insert_failed",
				"team_id", r.teamID.String(),
				"error", insErr,
			)
			skipped++
			continue
		}

		flagged++
		slog.Info("jobs.churn_predictor.flagged",
			"team_id", r.teamID.String(),
			"plan_tier", r.planTier,
			"last_activity_days_ago", daysSince,
			"active_resource_count", r.activeResourceCount,
			"to", r.ownerEmail.String,
		)
	}

	slog.Info("jobs.churn_predictor.completed",
		"flagged", flagged,
		"skipped", skipped,
		"candidates", len(candidates),
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}
