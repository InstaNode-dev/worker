package jobs

// expire_imminent.go — periodic scan that emits a `resource.expiry_imminent`
// audit_log row for every authenticated resource whose expires_at falls inside
// the next hour but is not yet expired. The Loops event forwarder
// (loops_event_forwarder.go) picks the row up on its next tick and triggers
// the "resource_expiring_soon" lifecycle email via Brevo / Loops.
//
// Why this is a SEPARATE River job from ExpireAnonymousWorker and
// ExpiryReminderWorker:
//
//  1. ExpireAnonymousWorker (expire.go) acts on rows that are ALREADY past
//     their TTL — it deprovisions and marks the row deleted. That window
//     (expires_at < now) is disjoint from this job's (now < expires_at <
//     now + 1h), so co-mingling the queries would force confusing branching.
//
//  2. ExpiryReminderWorker (expiry_reminder.go) sends a direct Resend email
//     and dedupes via the `resources.expiry_reminded_at` column. Its delivery
//     channel and dedupe surface are both different from this job's. Putting
//     them in the same worker would conflate two independent contracts —
//     "email sent via Resend, stamped on the row" vs "audit row written,
//     forwarded via Loops".
//
//  3. The audit_log surface is the canonical lifecycle-event spine
//     (loops_event_mapping.go). Every other lifecycle event in that mapping
//     has its own producer; making this one a peer keeps the architecture
//     symmetric.
//
// Dedupe contract — 12h window:
//
//   Most free-anonymous resources have a 24h TTL, so a single warning ~1h
//   before expiry is the design intent. A 12h dedupe window covers the
//   entire pre-expiry zone for a 24h resource (we will not warn twice on
//   the same resource), while still letting longer-lived resources receive
//   a fresh warning if they get extended past 12h and re-enter the 1h
//   pre-expiry window later. 1h would be too tight (a slow worker tick or
//   a clock skew could re-fire); 24h matches but offers no safety margin
//   against a TTL bump that pushes a resource back into the window.
//
// Anonymous resources (team_id IS NULL) are skipped: there is no team /
// no users / no email address. The expiry warning is an email channel and
// anonymous tokens are agent-only ephemeral creds.

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

// ExpireImminentArgs is the River job payload — no fields, runs as a sweep.
type ExpireImminentArgs struct{}

// Kind is the River job kind.
func (ExpireImminentArgs) Kind() string { return "expire_imminent" }

// expireImminentInterval is the periodic dispatch cadence. Ten minutes is
// the brief's requirement: fast enough that a resource one tick past the
// 1h pre-expiry boundary still gets at least ~5 warnings worth of headroom
// before expiry (1h / 10min = 6 ticks), slow enough that the full-table
// scan does not become a hot path on the platform DB.
const expireImminentInterval = 10 * time.Minute

// expireImminentWindow is how far ahead of now() the scan looks. A resource
// becomes a candidate when its expires_at falls inside (now, now+window].
const expireImminentWindow = 1 * time.Hour

// expireImminentDedupeWindow is the minimum gap between two audit rows
// for the same resource. See the file-level comment for the 12h rationale.
const expireImminentDedupeWindow = 12 * time.Hour

// expireImminentActor is the audit_log.actor value for system-written rows.
// Matches the convention used by quota_wall_nudge.go.
const expireImminentActor = "system"

// expireImminentBatchLimit caps the per-tick fan-out. A worker restart will
// pick up the rest on the next 10-minute tick; the dedupe window absorbs
// any reordering. 500 is comfortable for a single periodic scan and matches
// the limit used by ExpiryReminderWorker.
const expireImminentBatchLimit = 500

// ExpireImminentWorker scans for soon-to-expire authenticated resources
// and writes one resource.expiry_imminent audit_log row per resource per
// 12h window. The Loops forwarder converts each row into a Brevo lifecycle
// email (event = resource_expiring_soon).
type ExpireImminentWorker struct {
	river.WorkerDefaults[ExpireImminentArgs]
	db *sql.DB
}

// NewExpireImminentWorker constructs an ExpireImminentWorker.
func NewExpireImminentWorker(db *sql.DB) *ExpireImminentWorker {
	return &ExpireImminentWorker{db: db}
}

// expireImminentRow is the projection of resources + users used by the worker.
type expireImminentRow struct {
	resourceID   uuid.UUID
	token        uuid.UUID
	teamID       uuid.UUID
	resourceType string
	expiresAt    time.Time
	ownerEmail   sql.NullString
}

// Work executes one sweep.
//
// Returned error semantics match expire.go / expiry_reminder.go: a top-level
// query failure returns an error so River retries; per-row failures (insert
// errors, missing owner email) are logged but never propagate, so one bad
// row never blocks the rest of the batch.
func (w *ExpireImminentWorker) Work(ctx context.Context, job *river.Job[ExpireImminentArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.expire_imminent")
	defer span.End()

	start := time.Now()
	now := time.Now().UTC()
	windowEnd := now.Add(expireImminentWindow)
	dedupeCutoff := now.Add(-expireImminentDedupeWindow)

	// Candidate query.
	//
	// The NOT IN clause is the freshness window: at most one
	// resource.expiry_imminent row per resource per 12h. The metadata
	// JSONB carries resource_id (set by the INSERT below) so the
	// subquery can join back. cast to uuid so the NOT IN comparison
	// matches r.id's type.
	//
	// LEFT JOIN LATERAL picks the oldest user on the team — same
	// convention as loops_event_forwarder.go's owner_email resolution.
	// A team with no users surfaces as NULL email; we skip the row
	// (see per-row handling below).
	//
	// LIMIT bounds per-tick fan-out; the 12h dedupe absorbs spillover.
	rows, err := w.db.QueryContext(ctx, `
		SELECT
			r.id,
			r.token,
			r.team_id,
			r.resource_type,
			r.expires_at,
			COALESCE(u.email, '') AS owner_email
		FROM resources r
		LEFT JOIN LATERAL (
			SELECT email
			FROM users
			WHERE team_id = r.team_id
			ORDER BY created_at ASC
			LIMIT 1
		) u ON true
		WHERE r.team_id IS NOT NULL
		  AND r.status = 'active'
		  AND r.expires_at IS NOT NULL
		  AND r.expires_at > $1
		  AND r.expires_at < $2
		  AND r.id NOT IN (
			SELECT (metadata->>'resource_id')::uuid
			FROM audit_log
			WHERE kind = $3
			  AND created_at > $4
			  AND metadata ? 'resource_id'
		  )
		ORDER BY r.expires_at ASC
		LIMIT $5
	`, now, windowEnd, auditKindResourceExpiryImminent, dedupeCutoff, expireImminentBatchLimit)
	if err != nil {
		return fmt.Errorf("ExpireImminentWorker: query failed: %w", err)
	}
	defer rows.Close()

	var candidates []expireImminentRow
	for rows.Next() {
		var r expireImminentRow
		var emailStr string
		if scanErr := rows.Scan(&r.resourceID, &r.token, &r.teamID, &r.resourceType, &r.expiresAt, &emailStr); scanErr != nil {
			slog.Warn("jobs.expire_imminent.scan_failed", "error", scanErr)
			continue
		}
		if emailStr != "" {
			r.ownerEmail = sql.NullString{String: emailStr, Valid: true}
		}
		candidates = append(candidates, r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("ExpireImminentWorker: rows error: %w", err)
	}
	rows.Close()

	if len(candidates) == 0 {
		slog.Info("jobs.expire_imminent.completed",
			"emitted", 0,
			"candidates", 0,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil
	}

	var emitted, skipped int
	for _, r := range candidates {
		// Defensive: a team can exist without users (orphan / pre-signup).
		// Loops keys events on email, so an audit row with no email is
		// dead weight — skip and log so an operator can investigate.
		if !r.ownerEmail.Valid || r.ownerEmail.String == "" {
			slog.Warn("jobs.expire_imminent.no_owner_email",
				"resource_id", r.resourceID.String(),
				"team_id", r.teamID.String(),
				"note", "skipped — no email to address",
			)
			skipped++
			continue
		}

		// Round to nearest 0.1 hour (per brief).
		hoursRemaining := math.Round(r.expiresAt.Sub(now).Hours()*10) / 10

		summary := fmt.Sprintf("%s resource expiring in %.1fh", r.resourceType, hoursRemaining)

		// Metadata carries the contract Brevo / Loops reads from
		// (loops_event_mapping.go::buildResourceExpiring). The
		// resource_id field is also the dedupe key the next sweep's
		// NOT IN subquery joins on, so it must be a parseable uuid.
		meta := map[string]any{
			"resource_id":     r.resourceID.String(),
			"resource_type":   r.resourceType,
			"expires_at":      r.expiresAt.UTC().Format(time.RFC3339),
			"hours_remaining": hoursRemaining,
			"email":           r.ownerEmail.String,
			"token":           r.token.String(),
		}
		metaBytes, mErr := json.Marshal(meta)
		if mErr != nil {
			// json.Marshal on a map[string]any of primitives can't
			// fail in practice; treat as a logged skip just in case.
			slog.Error("jobs.expire_imminent.metadata_marshal_failed",
				"resource_id", r.resourceID.String(),
				"error", mErr,
			)
			skipped++
			continue
		}

		if _, insErr := w.db.ExecContext(ctx, `
			INSERT INTO audit_log (team_id, actor, kind, summary, metadata, resource_type)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, r.teamID, expireImminentActor, auditKindResourceExpiryImminent, summary, metaBytes, r.resourceType); insErr != nil {
			slog.Error("jobs.expire_imminent.insert_failed",
				"resource_id", r.resourceID.String(),
				"team_id", r.teamID.String(),
				"error", insErr,
			)
			skipped++
			continue
		}

		emitted++
		slog.Info("jobs.expire_imminent.emitted",
			"resource_id", r.resourceID.String(),
			"resource_type", r.resourceType,
			"hours_remaining", hoursRemaining,
			"to", r.ownerEmail.String,
		)
	}

	slog.Info("jobs.expire_imminent.completed",
		"emitted", emitted,
		"skipped", skipped,
		"candidates", len(candidates),
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}

