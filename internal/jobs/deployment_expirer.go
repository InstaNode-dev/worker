package jobs

// deployment_expirer.go — Wave FIX-J expirer worker.
//
// Every 60s, scan deployments where:
//   - expires_at < now()
//   - ttl_policy != 'permanent'
//   - status NOT IN ('deleted', 'expired')
//
// For each candidate:
//   1. Soft-delete: UPDATE status = 'expired' (we don't hard-delete the row
//      so the dashboard can still show "expired" cards with the audit trail).
//   2. Fire-and-forget the existing deploy-deprovision path (Phase 6's
//      compute.k8s.Delete). The expirer runs without a compute client to
//      keep the worker module's import graph small — actual teardown
//      happens via the api's existing periodic reconciler picking up the
//      'expired' status.
//   3. Emit a deploy.expired audit_log row carrying the metadata the
//      email template needs (app_id, expires_at, ttl_policy). The
//      BrevoForwarder (event_email_forwarder.go) drains it on its next
//      60s tick and POSTs to Brevo.
//
// Email-send migration (2026-05-14, FIX-I/J→Brevo migration):
//   PREVIOUSLY: this worker called EmailClient.SendDeployExpired inline.
//   In production RESEND_API_KEY was unset, so all sends went to
//   NoopClient and customers never got the "your deploy was removed"
//   notice. The audit row IS now the trigger.
//
// Idempotent across ticks: once status='expired' the row no longer matches
// the candidate predicate, so it can't be processed twice.
//
// Distinct from ExpireAnonymousWorker (which deprovisions ANONYMOUS resources
// — different namespace, different physical teardown path). Deploy teardown
// goes through the api's compute provider, not this worker, so the worker
// stays decoupled from the k8s SDK.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	"instant.dev/worker/internal/metrics"
)

// DeploymentExpirerArgs is the River job payload (no fields — runs as a sweep).
type DeploymentExpirerArgs struct{}

// Kind is the River job kind.
func (DeploymentExpirerArgs) Kind() string { return "deployment_expirer" }

// DeploymentExpirerWorker runs the periodic sweep. Emails are dispatched
// by event_email_forwarder.go off the audit_log row this worker writes
// (migrated 2026-05-14, FIX-I/J→Brevo migration).
type DeploymentExpirerWorker struct {
	river.WorkerDefaults[DeploymentExpirerArgs]
	db *sql.DB
}

// NewDeploymentExpirerWorker constructs the worker. The email argument
// is accepted (and ignored) to preserve the existing call-site signature
// in workers.go while the Resend→Brevo migration ships; it will be
// removed in a follow-up once all callers are updated.
func NewDeploymentExpirerWorker(db *sql.DB, _ any) *DeploymentExpirerWorker {
	return &DeploymentExpirerWorker{db: db}
}

// deployExpirerRow is the projection of deployments + users used by the expirer.
type deployExpirerRow struct {
	id         string
	teamID     string
	appID      string
	ttlPolicy  string
	expiresAt  time.Time
	ownerEmail sql.NullString
}

// Work runs one expirer sweep.
func (w *DeploymentExpirerWorker) Work(ctx context.Context, job *river.Job[DeploymentExpirerArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.deployment_expirer")
	defer span.End()

	start := time.Now()
	now := time.Now().UTC()

	rows, err := w.db.QueryContext(ctx, `
		SELECT d.id::text, d.team_id::text, d.app_id, d.ttl_policy,
		       d.expires_at, u.email
		FROM deployments d
		LEFT JOIN users u ON u.team_id = d.team_id AND u.is_primary = true
		WHERE d.expires_at IS NOT NULL
		  AND d.ttl_policy != 'permanent'
		  AND d.status NOT IN ('deleted', 'expired')
		  AND d.expires_at < $1
		ORDER BY d.expires_at ASC
		LIMIT 200
	`, now)
	if err != nil {
		return fmt.Errorf("DeploymentExpirerWorker: query failed: %w", err)
	}
	defer rows.Close()

	var candidates []deployExpirerRow
	for rows.Next() {
		var r deployExpirerRow
		if scanErr := rows.Scan(&r.id, &r.teamID, &r.appID, &r.ttlPolicy,
			&r.expiresAt, &r.ownerEmail); scanErr != nil {
			slog.Warn("jobs.deployment_expirer.scan_failed", "error", scanErr)
			continue
		}
		candidates = append(candidates, r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("DeploymentExpirerWorker: rows error: %w", err)
	}
	rows.Close()

	if len(candidates) == 0 {
		// T21 P1-1 (BugBash 2026-05-20): idle-tick demoted INFO→DEBUG.
		slog.Debug("jobs.deployment_expirer.completed",
			"expired", 0, "candidates", 0,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil
	}

	var expired, skipped int
	for _, r := range candidates {
		// Step 1: soft-delete. Use a guarded UPDATE so a race with the api's
		// DELETE path doesn't accidentally double-process the row.
		res, execErr := w.db.ExecContext(ctx, `
			UPDATE deployments
			SET status = 'expired', updated_at = now()
			WHERE id = $1 AND status NOT IN ('deleted', 'expired')
		`, r.id)
		if execErr != nil {
			slog.Error("jobs.deployment_expirer.update_failed",
				"deploy_id", r.id, "error", execErr)
			skipped++
			continue
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			// Already deleted/expired by another path. Not a fault.
			skipped++
			continue
		}

		// Step 2: audit emit. The BrevoForwarder drains audit_log every
		// 60s and dispatches the "your deploy was removed" email —
		// migrated 2026-05-14 from inline EmailClient.SendDeployExpired.
		//
		// B19-FIND-2 (BugBash 2026-05-20): pass the parent ctx so trace
		// metadata (span/trace ID) propagates into the fire-and-forget
		// audit goroutine. The audit emit itself decouples cancellation
		// via context.WithoutCancel so the 3s budget survives the parent
		// job's tick boundary.
		emitDeployExpiredAudit(ctx, w.db, r)
		metrics.DeployExpiredTotal.Inc()

		slog.Info("jobs.deployment_expirer.expired",
			"deploy_id", r.id, "team_id", r.teamID,
			"app_id", r.appID, "ttl_policy", r.ttlPolicy,
			"expires_at", r.expiresAt,
		)
		expired++
	}

	slog.Info("jobs.deployment_expirer.completed",
		"expired", expired,
		"skipped", skipped,
		"candidates", len(candidates),
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}

// emitDeployExpiredAudit writes the deploy.expired audit_log row. The
// BrevoForwarder consumes this row to dispatch the "your deploy expired"
// email — see buildDeployExpired in event_email_mapping.go. The metadata
// MUST stay in sync with that builder.
//
// Best-effort, fire-and-forget per existing convention.
//
// B19-FIND-2 (BugBash 2026-05-20): accepts a parent ctx so the trace
// metadata (span/trace ID added by the Work tracer span) propagates into
// the fire-and-forget goroutine. The audit's own 3s deadline is built on
// context.WithoutCancel(parent) — keeps the trace baggage but decouples
// cancellation, so the parent tick ending doesn't kill the in-flight
// INSERT and lose the audit row.
func emitDeployExpiredAudit(parent context.Context, db *sql.DB, r deployExpirerRow) {
	meta, _ := json.Marshal(map[string]any{
		"deploy_id":  r.id,
		"team_id":    r.teamID,
		"app_id":     r.appID,
		"expires_at": r.expiresAt.UTC().Format(time.RFC3339),
		"ttl_policy": r.ttlPolicy,
	})
	// Fire-and-forget audit emit — routed through SafeGo (P1-B) so a panic
	// in the INSERT path is recovered + counted instead of crashing the pod.
	SafeGo("deployment_expirer.audit", func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 3*time.Second)
		defer cancel()
		teamUUID, parseErr := uuid.Parse(r.teamID)
		if parseErr != nil {
			slog.Warn("jobs.deployment_expirer.audit.bad_team_id",
				"team_id", r.teamID, "error", parseErr)
			return
		}
		_, err := db.ExecContext(ctx, `
			INSERT INTO audit_log (team_id, kind, actor, resource_type, summary, metadata)
			VALUES ($1, 'deploy.expired', 'system', 'deploy', $2, $3)
		`, teamUUID, "deploy "+r.appID+" expired", meta)
		if err != nil {
			slog.Warn("jobs.deployment_expirer.audit.insert_failed",
				"deploy_id", r.id, "error", err)
		}
	})
}
