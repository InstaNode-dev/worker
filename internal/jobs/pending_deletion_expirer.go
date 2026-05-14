package jobs

// pending_deletion_expirer.go — periodic job that expires stale
// pending_deletions rows. Wave FIX-I.
//
// The api inserts a pending_deletions row with status='pending' on
// every paid-tier DELETE that lands. The row carries a 15-minute
// expires_at (configurable via DELETION_CONFIRMATION_TTL_MINUTES on
// the api). If the user doesn't click the email link in time, this
// worker flips status='pending' → 'expired' so:
//
//   - the resource_id remains active (no destruction without explicit
//     human confirmation — the slot stays consumed on purpose);
//   - the partial index on (resource_id, resource_type) WHERE
//     status='pending' clears, so a fresh DELETE can mint a new email.
//
// Cadence: every 60s. Same as the magic_link_reconciler — short enough
// that an expired row vacates the dedup index promptly, long enough
// that a temporary worker outage doesn't strand users.
//
// SCOPE NOTE: the worker module (instant.dev/worker) is a separate Go
// module from the api (instant.dev), so we cannot import api/models.
// The SQL inlined here mirrors the model layer's ExpireOldPendingDeletions
// — keep these in sync if either side changes the column shape. We also
// emit one audit_log row per expired row directly (no api round-trip)
// because audit_log is in the same DB the worker connects to; this
// avoids the JWT-signing dance the magic-link reconciler needs.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
)

// pendingDeletionExpirerInterval is the dispatch cadence. 60s mirrors
// the magic_link_reconciler — frequent enough that an expired row
// doesn't block a fresh deletion request for long, infrequent enough
// that an empty sweep is cheap.
const pendingDeletionExpirerInterval = 60 * time.Second

// pendingDeletionExpirerBatchCap caps per-tick fan-out so a sudden
// surge of expirations (e.g. after a long email-backend outage where
// hundreds of rows piled up) can't take an entire worker pod offline
// inside one tick. A real outage drains across multiple ticks; that's
// fine because the rows are already past their TTL — there's no SLA
// to hit.
const pendingDeletionExpirerBatchCap = 200

// PendingDeletionExpirerArgs is the River job payload. No fields —
// every run is a full table sweep against the partial expires_at
// index from migration 044.
type PendingDeletionExpirerArgs struct{}

// Kind is the River worker key. Matches the convention used by the
// other reconcilers in this package (snake_case, no namespace).
func (PendingDeletionExpirerArgs) Kind() string { return "pending_deletion_expirer" }

// PendingDeletionExpirerWorker is the River worker. The single
// dependency is the platform DB — all the work is local SQL and an
// audit_log insert per expired row.
type PendingDeletionExpirerWorker struct {
	river.WorkerDefaults[PendingDeletionExpirerArgs]
	db *sql.DB
}

// NewPendingDeletionExpirerWorker constructs the worker. Single
// argument keeps the wiring trivial — see workers.go for the
// registration line.
func NewPendingDeletionExpirerWorker(db *sql.DB) *PendingDeletionExpirerWorker {
	return &PendingDeletionExpirerWorker{db: db}
}

// expiredRow is the projection the sweeper reads. Mirrors the api
// model's ExpiredPendingDeletion shape, duplicated here because the
// worker module is independent of the api module.
type expiredPendingDeletionRow struct {
	ID           uuid.UUID
	ResourceID   uuid.UUID
	ResourceType string
	TeamID       uuid.UUID
	RequestedAt  time.Time
}

// Work runs one sweep.
//
// Returns nil on every "expected" outcome (empty batch, transient
// audit insert error) so River doesn't retry the periodic job and
// pile work onto the next tick. Only a hard DB error on the UPDATE
// statement propagates — River's tracing surfaces those, and the
// next 60s tick re-attempts naturally regardless.
func (w *PendingDeletionExpirerWorker) Work(ctx context.Context, job *river.Job[PendingDeletionExpirerArgs]) error {
	start := time.Now()

	// Atomic UPDATE … RETURNING — flips the rows and reads them in one
	// round-trip so we can't accidentally double-audit on a concurrent
	// run (the per-row CAS guarantees one writer wins). The LIMIT-like
	// cap is enforced via a subquery so very-large outage scenarios
	// don't stall the whole tick.
	rows, err := w.db.QueryContext(ctx, `
		UPDATE pending_deletions
		SET status = 'expired'
		WHERE id IN (
			SELECT id FROM pending_deletions
			WHERE status = 'pending' AND expires_at < now()
			ORDER BY expires_at ASC
			LIMIT $1
		)
		RETURNING id, resource_id, resource_type, team_id, requested_at
	`, pendingDeletionExpirerBatchCap)
	if err != nil {
		return fmt.Errorf("pending_deletion_expirer: sweep failed: %w", err)
	}
	defer rows.Close()

	var expired []expiredPendingDeletionRow
	for rows.Next() {
		var r expiredPendingDeletionRow
		if scanErr := rows.Scan(&r.ID, &r.ResourceID, &r.ResourceType, &r.TeamID, &r.RequestedAt); scanErr != nil {
			slog.Warn("jobs.pending_deletion_expirer.scan_failed", "error", scanErr)
			continue
		}
		expired = append(expired, r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("pending_deletion_expirer: rows iter: %w", err)
	}

	if len(expired) == 0 {
		slog.Info("jobs.pending_deletion_expirer.completed",
			"expired", 0,
			"duration_ms", time.Since(start).Milliseconds(),
			"job_id", job.ID,
		)
		return nil
	}

	// One audit row per expiry. The api side reads these to populate
	// the customer-facing audit timeline and the NR dashboard tiles —
	// see api/internal/models/audit_kinds.go for the
	// deploy.deletion_expired / stack.deletion_expired definitions.
	for _, r := range expired {
		w.emitExpiryAudit(ctx, r)
	}

	slog.Info("jobs.pending_deletion_expirer.completed",
		"expired", len(expired),
		"duration_ms", time.Since(start).Milliseconds(),
		"job_id", job.ID,
	)
	return nil
}

// emitExpiryAudit writes the deploy.deletion_expired (or
// stack.deletion_expired) row. Best-effort — a failed insert is
// logged at WARN but never propagates because the user-visible
// state (row flipped to 'expired') is already correct; the audit
// is observability gravy, not the source of truth.
//
// Audit kind strings are inlined here (rather than imported from api)
// because the worker module doesn't import api/models. The strings
// MUST match the constants in api/internal/models/audit_kinds.go —
// drift would split NR dashboard counters across two kinds.
func (w *PendingDeletionExpirerWorker) emitExpiryAudit(ctx context.Context, r expiredPendingDeletionRow) {
	var kind, resourceType string
	switch r.ResourceType {
	case "stack":
		kind = "stack.deletion_expired"
		resourceType = "stack"
	default:
		kind = "deploy.deletion_expired"
		resourceType = "deploy"
	}

	meta := map[string]any{
		"team_id":             r.TeamID.String(),
		"resource_id":         r.ResourceID.String(),
		"pending_deletion_id": r.ID.String(),
		"age_seconds":         int64(time.Since(r.RequestedAt).Seconds()),
	}
	metaBlob, _ := json.Marshal(meta)

	_, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, resource_type, summary, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, r.TeamID, "system", kind, resourceType, kind, metaBlob)
	if err != nil {
		slog.Warn("jobs.pending_deletion_expirer.audit_failed",
			"kind", kind,
			"pending_id", r.ID,
			"resource_id", r.ResourceID,
			"error", err,
		)
	}
}
