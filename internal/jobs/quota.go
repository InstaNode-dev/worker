package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
)

// ── EnforceStorageQuotaWorker ─────────────────────────────────────────────────

// EnforceStorageQuotaArgs holds arguments for the storage quota enforcement job.
type EnforceStorageQuotaArgs struct{}

func (EnforceStorageQuotaArgs) Kind() string { return "enforce_storage_quota" }

// PlanRegistry is the minimal interface needed from plans.Registry across
// all worker jobs. Methods are added here as new jobs need them; the live
// *commonplans.Registry satisfies every method, so a superset is always
// safe — test mocks just need the new method stubbed.
type PlanRegistry interface {
	StorageLimitMB(tier, service string) int
	// ConnectionsLimit is consumed by QuotaWallNudgeWorker.
	ConnectionsLimit(tier, service string) int
	// ProvisionLimit is consumed by QuotaWallNudgeWorker.
	ProvisionLimit(tier string) int
}

// StorageQuotaChecker is the minimal interface needed for storage quota checks.
type StorageQuotaChecker interface {
	CheckStorageQuota(ctx context.Context, db *sql.DB, resourceID uuid.UUID, limitMB int) (int64, bool, error)
}

// resourceStatusSuspended / resourceStatusActive are named constants for the
// two status values touched by the suspend/unsuspend lifecycle. Using constants
// avoids scattered string literals and makes the "suspended → active" transition
// greppable. (CLAUDE.md: "Use named constants, not inline strings".)
const (
	resourceStatusSuspended = "suspended"
	resourceStatusActive    = "active"
)

// EnforceStorageQuotaWorker checks all active resources against their plan's
// storage limit and suspends those that have exceeded it. It also scans
// currently-suspended resources and unsuspends those whose usage has dropped
// back below the limit.
//
// Two loops per Work() run:
//  1. Suspend loop — scans status='active' postgres/redis/mongodb resources,
//     calls the provisioner-side infra revoke (REVOKE CONNECT / ACL SETUSER off
//     / revokeRolesFromUser), then flips status to 'suspended'.
//  2. Unsuspend loop — scans status='suspended' resources, re-checks storage,
//     re-grants infra access, and flips status back to 'active' for those now
//     under limit.
//
// Both loops fail-open on infra errors (connectivity issues with the customer
// DB / Redis / Mongo only affect the row flip as a fallback — the status flip
// still lands). Fail-open matches CLAUDE.md convention #1.
type EnforceStorageQuotaWorker struct {
	river.WorkerDefaults[EnforceStorageQuotaArgs]
	db      *sql.DB
	plans   PlanRegistry
	revoker ResourceInfraRevoker // nil = no infra revoke (status-row only)
}

// NewEnforceStorageQuotaWorker constructs an EnforceStorageQuotaWorker.
// Pass nil for revoker to skip infra-level revoke/grant (status flip still
// occurs). In production, pass NewDirectResourceRevoker(...) built from cfg.
func NewEnforceStorageQuotaWorker(db *sql.DB, plans PlanRegistry, revoker ResourceInfraRevoker) *EnforceStorageQuotaWorker {
	return &EnforceStorageQuotaWorker{db: db, plans: plans, revoker: revoker}
}

// Work scans all active postgres/redis/mongodb resources and suspends those
// over their quota, then unsuspends any suspended resources that are now back
// under their quota.
func (w *EnforceStorageQuotaWorker) Work(ctx context.Context, job *river.Job[EnforceStorageQuotaArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.enforce_storage_quota")
	defer span.End()

	suspended, err := w.runSuspendLoop(ctx)
	if err != nil {
		return err
	}

	unsuspended, err := w.runUnsuspendLoop(ctx)
	if err != nil {
		// Don't fail the job for unsuspend errors — suspends already landed.
		slog.Error("jobs.enforce_storage_quota.unsuspend_loop_error", "error", err)
	}

	var jobID int64
	if job.JobRow != nil {
		jobID = job.ID
	}
	slog.Info("jobs.enforce_storage_quota.completed",
		"suspended_count", suspended,
		"unsuspended_count", unsuspended,
		"job_id", jobID,
	)
	return nil
}

// runSuspendLoop scans status='active' resources, suspends over-quota ones,
// and returns the count of newly suspended resources.
func (w *EnforceStorageQuotaWorker) runSuspendLoop(ctx context.Context) (int, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT id, token, resource_type, tier, storage_bytes
		FROM resources
		WHERE status = $1
		  AND resource_type IN ('postgres', 'redis', 'mongodb')
		ORDER BY created_at
	`, resourceStatusActive)
	if err != nil {
		return 0, fmt.Errorf("EnforceStorageQuotaWorker.suspendLoop: query failed: %w", err)
	}
	defer rows.Close()

	checked, suspended := 0, 0

	for rows.Next() {
		var (
			id           string
			token        string
			resourceType string
			tier         string
			storageBytes int64
		)
		if scanErr := rows.Scan(&id, &token, &resourceType, &tier, &storageBytes); scanErr != nil {
			slog.Error("jobs.enforce_storage_quota.scan_error", "error", scanErr)
			continue
		}
		checked++

		limitMB := w.plans.StorageLimitMB(tier, resourceType)
		if limitMB == -1 {
			continue // unlimited tier — never suspend
		}

		uid, parseErr := uuid.Parse(id)
		if parseErr != nil {
			slog.Error("jobs.enforce_storage_quota.invalid_uuid", "id", id, "error", parseErr)
			continue
		}

		_, exceeded, checkErr := checkStorageQuota(ctx, w.db, uid, limitMB)
		if checkErr != nil {
			slog.Error("jobs.enforce_storage_quota.check_error",
				"resource_id", id,
				"error", checkErr,
			)
			continue // fail open — don't suspend on check error
		}

		if !exceeded {
			continue
		}

		// Infra revoke FIRST, then status flip — matches the iron-rule order
		// from api/internal/handlers/resource.go Pause(): "provider-side FIRST
		// so the row stays active if infra fails; row flip is the commit."
		// Here we invert slightly: infra revoke is fail-open (logged warning,
		// not a hard error) so we always proceed to the status flip. This is
		// intentional: a row marked 'suspended' blocks new provisions from the
		// API even when the infra revoke is not available (customer DB down).
		if w.revoker != nil {
			if revokeErr := w.revoker.RevokeAccess(ctx, resourceType, token, ""); revokeErr != nil {
				// revoker implementations are fail-open (return nil on infra
				// error, log a WARN). A non-nil error here is unexpected —
				// log it but don't abort the row update.
				slog.Error("jobs.enforce_storage_quota.revoke_error",
					"resource_id", id, "token", token, "resource_type", resourceType,
					"error", revokeErr,
				)
			}
		}

		_, updateErr := w.db.ExecContext(ctx, `
			UPDATE resources SET status = $1
			WHERE id = $2 AND status = $3
		`, resourceStatusSuspended, id, resourceStatusActive)
		if updateErr != nil {
			slog.Error("jobs.enforce_storage_quota.suspend_failed",
				"resource_id", id,
				"error", updateErr,
			)
			continue
		}

		slog.Warn("jobs.enforce_storage_quota.suspended",
			"resource_id", id,
			"token", token,
			"resource_type", resourceType,
			"tier", tier,
			"storage_bytes", storageBytes,
			"limit_mb", limitMB,
		)
		suspended++
	}
	if err := rows.Err(); err != nil {
		return suspended, fmt.Errorf("EnforceStorageQuotaWorker.suspendLoop: rows error: %w", err)
	}

	slog.Info("jobs.enforce_storage_quota.suspend_loop_done",
		"checked_count", checked,
		"suspended_count", suspended,
	)
	return suspended, nil
}

// runUnsuspendLoop scans status='suspended' resources, re-checks quota, and
// re-grants infra access + flips to 'active' for those now under their limit.
// Returns the count of newly unsuspended resources.
func (w *EnforceStorageQuotaWorker) runUnsuspendLoop(ctx context.Context) (int, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT id, token, resource_type, tier, storage_bytes
		FROM resources
		WHERE status = $1
		  AND resource_type IN ('postgres', 'redis', 'mongodb')
		ORDER BY created_at
	`, resourceStatusSuspended)
	if err != nil {
		return 0, fmt.Errorf("EnforceStorageQuotaWorker.unsuspendLoop: query failed: %w", err)
	}
	defer rows.Close()

	unsuspended := 0

	for rows.Next() {
		var (
			id           string
			token        string
			resourceType string
			tier         string
			storageBytes int64
		)
		if scanErr := rows.Scan(&id, &token, &resourceType, &tier, &storageBytes); scanErr != nil {
			slog.Error("jobs.enforce_storage_quota.unsuspend_scan_error", "error", scanErr)
			continue
		}

		limitMB := w.plans.StorageLimitMB(tier, resourceType)
		if limitMB == -1 {
			// Unlimited tier shouldn't have been suspended, but unsuspend
			// eagerly to self-heal any historical bad state.
			limitMB = 0 // treat as exceeded=false below
		}

		uid, parseErr := uuid.Parse(id)
		if parseErr != nil {
			slog.Error("jobs.enforce_storage_quota.unsuspend_invalid_uuid", "id", id, "error", parseErr)
			continue
		}

		var exceeded bool
		if limitMB > 0 {
			_, exceeded, err = checkStorageQuota(ctx, w.db, uid, limitMB)
			if err != nil {
				slog.Error("jobs.enforce_storage_quota.unsuspend_check_error",
					"resource_id", id, "error", err)
				continue // fail open — don't unsuspend on check error
			}
		}
		// limitMB == 0 means unlimited; exceeded stays false → will unsuspend.

		if exceeded {
			continue // still over quota — remain suspended
		}

		// Re-grant infra access before flipping the status row.
		if w.revoker != nil {
			if grantErr := w.revoker.GrantAccess(ctx, resourceType, token, ""); grantErr != nil {
				slog.Error("jobs.enforce_storage_quota.grant_error",
					"resource_id", id, "token", token, "resource_type", resourceType,
					"error", grantErr,
				)
				// Non-nil means unexpected path — still proceed with row flip.
			}
		}

		_, updateErr := w.db.ExecContext(ctx, `
			UPDATE resources SET status = $1
			WHERE id = $2 AND status = $3
		`, resourceStatusActive, id, resourceStatusSuspended)
		if updateErr != nil {
			slog.Error("jobs.enforce_storage_quota.unsuspend_failed",
				"resource_id", id, "error", updateErr)
			continue
		}

		slog.Info("jobs.enforce_storage_quota.unsuspended",
			"resource_id", id,
			"token", token,
			"resource_type", resourceType,
			"tier", tier,
			"storage_bytes", storageBytes,
			"limit_mb", limitMB,
		)
		unsuspended++
	}
	if err := rows.Err(); err != nil {
		return unsuspended, fmt.Errorf("EnforceStorageQuotaWorker.unsuspendLoop: rows error: %w", err)
	}

	return unsuspended, nil
}

// checkStorageQuota reads resources.storage_bytes and compares to limitMB.
// Returns (bytesUsed, exceeded, error). Fails open on DB error.
func checkStorageQuota(ctx context.Context, db *sql.DB, resourceID uuid.UUID, limitMB int) (int64, bool, error) {
	if limitMB == -1 {
		return 0, false, nil
	}

	var bytesUsed int64
	err := db.QueryRowContext(ctx,
		`SELECT storage_bytes FROM resources WHERE id = $1`,
		resourceID,
	).Scan(&bytesUsed)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		slog.Error("quota.storage.db_error", "resource_id", resourceID, "error", err)
		return 0, false, fmt.Errorf("checkStorageQuota: %w", err)
	}

	limitBytes := int64(limitMB) * 1024 * 1024
	return bytesUsed, bytesUsed >= limitBytes, nil
}
