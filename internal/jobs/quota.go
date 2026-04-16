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

// PlanRegistry is the minimal interface needed from plans.Registry.
type PlanRegistry interface {
	StorageLimitMB(tier, service string) int
}

// StorageQuotaChecker is the minimal interface needed for storage quota checks.
type StorageQuotaChecker interface {
	CheckStorageQuota(ctx context.Context, db *sql.DB, resourceID uuid.UUID, limitMB int) (int64, bool, error)
}

// EnforceStorageQuotaWorker checks all active resources against their plan's
// storage limit and suspends those that have exceeded it.
type EnforceStorageQuotaWorker struct {
	river.WorkerDefaults[EnforceStorageQuotaArgs]
	db    *sql.DB
	plans PlanRegistry
}

// NewEnforceStorageQuotaWorker constructs an EnforceStorageQuotaWorker.
func NewEnforceStorageQuotaWorker(db *sql.DB, plans PlanRegistry) *EnforceStorageQuotaWorker {
	return &EnforceStorageQuotaWorker{db: db, plans: plans}
}

// Work scans all active postgres/redis/mongodb resources and suspends those over their quota.
func (w *EnforceStorageQuotaWorker) Work(ctx context.Context, job *river.Job[EnforceStorageQuotaArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.enforce_storage_quota")
	defer span.End()

	rows, err := w.db.QueryContext(ctx, `
		SELECT id, token, resource_type, tier, storage_bytes
		FROM resources
		WHERE status = 'active'
		  AND resource_type IN ('postgres', 'redis', 'mongodb')
		ORDER BY created_at
	`)
	if err != nil {
		return fmt.Errorf("EnforceStorageQuotaWorker: query failed: %w", err)
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
			continue // unlimited tier — skip
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

		_, updateErr := w.db.ExecContext(ctx, `
			UPDATE resources SET status = 'suspended'
			WHERE id = $1 AND status = 'active'
		`, id)
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
		return fmt.Errorf("EnforceStorageQuotaWorker: rows error: %w", err)
	}

	var jobID int64
	if job.JobRow != nil {
		jobID = job.ID
	}
	slog.Info("jobs.enforce_storage_quota.completed",
		"checked_count", checked,
		"suspended_count", suspended,
		"job_id", jobID,
	)
	return nil
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
