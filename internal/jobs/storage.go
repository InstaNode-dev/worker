package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
	commonv1 "instant.dev/proto/common/v1"
)

// UpdateStorageBytesArgs holds arguments for the storage bytes update job.
type UpdateStorageBytesArgs struct{}

func (UpdateStorageBytesArgs) Kind() string { return "update_storage_bytes" }

// StorageBytesProvider is the interface the worker calls to query real infrastructure.
// Implemented by provisioner.Client.
type StorageBytesProvider interface {
	StorageBytes(ctx context.Context, token, providerResourceID string, resType commonv1.ResourceType) (int64, error)
}

// MinIOStorageScanner queries object storage usage by summing object sizes
// under a tenant's prefix in the shared MinIO bucket. Implemented by
// minioStorageScanner (storage_minio.go) using github.com/minio/minio-go/v7.
//
// Decoupled from StorageBytesProvider so the worker can query MinIO directly
// using the same admin credentials it already loads for IAM cleanup, instead
// of paying a gRPC roundtrip through the provisioner for every storage row.
type MinIOStorageScanner interface {
	StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error)
}

// UpdateStorageBytesWorker queries actual infrastructure via the gRPC provisioner to update
// resources.storage_bytes for all active data resources.
//
// Supported resource types: postgres, redis, mongodb (via gRPC provisioner),
// storage (via direct MinIO listing — see MinIOStorageScanner).
//
// This worker runs every 6 hours so EnforceStorageQuotaWorker always has
// fresh data to check against. All provider errors are fail-open — a query
// failure logs an error and skips that resource rather than failing the job.
type UpdateStorageBytesWorker struct {
	river.WorkerDefaults[UpdateStorageBytesArgs]
	db          *sql.DB
	provClient  StorageBytesProvider // required for postgres/redis/mongodb; nil → those types skipped
	minioClient MinIOStorageScanner  // optional; nil → storage rows skipped (fail-open)
}

// NewUpdateStorageBytesWorker constructs an UpdateStorageBytesWorker.
// provClient may be nil; if nil, non-storage resources are skipped each run.
// minioClient may be nil; if nil, storage resources are skipped each run.
// When both are nil the worker is a no-op (logs a warning each run).
func NewUpdateStorageBytesWorker(db *sql.DB, provClient StorageBytesProvider, minioClient MinIOStorageScanner) *UpdateStorageBytesWorker {
	return &UpdateStorageBytesWorker{
		db:          db,
		provClient:  provClient,
		minioClient: minioClient,
	}
}

// Work iterates all active data resources and updates their storage_bytes by
// querying the gRPC provisioner. Provider errors are fail-open.
func (w *UpdateStorageBytesWorker) Work(ctx context.Context, job *river.Job[UpdateStorageBytesArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.update_storage_bytes")
	defer span.End()

	if w.provClient == nil && w.minioClient == nil {
		slog.Warn("jobs.update_storage_bytes.skipped", "reason", "no provisioner client or MinIO scanner configured")
		return nil
	}

	rows, err := w.db.QueryContext(ctx, `
		SELECT id, token, resource_type, tier, COALESCE(provider_resource_id, '')
		FROM resources
		WHERE status = 'active'
		  AND resource_type IN ('postgres', 'redis', 'mongodb', 'storage')
		ORDER BY created_at
	`)
	if err != nil {
		return fmt.Errorf("UpdateStorageBytesWorker: query failed: %w", err)
	}
	defer rows.Close()

	updated := 0
	for rows.Next() {
		var id, token, resourceType, tier, providerResourceID string
		if scanErr := rows.Scan(&id, &token, &resourceType, &tier, &providerResourceID); scanErr != nil {
			slog.Error("jobs.update_storage_bytes.scan_error", "error", scanErr)
			continue
		}

		uid, parseErr := uuid.Parse(id)
		if parseErr != nil {
			slog.Error("jobs.update_storage_bytes.invalid_uuid", "id", id, "error", parseErr)
			continue
		}

		var (
			bytes    int64
			queryErr error
		)
		switch resourceType {
		case "storage":
			if w.minioClient == nil {
				slog.Warn("jobs.update_storage_bytes.minio_scanner_unavailable",
					"resource_id", id,
					"token", token,
				)
				continue // fail-open — skip until scanner is wired up
			}
			bytes, queryErr = w.minioClient.StorageBytes(ctx, token, providerResourceID)
			if queryErr != nil {
				slog.Error("jobs.update_storage_bytes.minio_error",
					"resource_id", id,
					"token", token,
					"resource_type", resourceType,
					"error", queryErr,
				)
				continue // fail-open — skip this resource
			}
		default:
			if w.provClient == nil {
				slog.Warn("jobs.update_storage_bytes.provisioner_unavailable",
					"resource_id", id,
					"resource_type", resourceType,
				)
				continue
			}
			resType := resourceTypeEnum(resourceType)
			bytes, queryErr = w.provClient.StorageBytes(ctx, token, providerResourceID, resType)
			if queryErr != nil {
				slog.Error("jobs.update_storage_bytes.provisioner_error",
					"resource_id", id,
					"token", token,
					"resource_type", resourceType,
					"error", queryErr,
				)
				continue // fail-open — skip this resource
			}
		}

		if updateErr := updateStorageBytes(ctx, w.db, uid, bytes); updateErr != nil {
			slog.Error("jobs.update_storage_bytes.update_failed",
				"resource_id", id,
				"bytes", bytes,
				"error", updateErr,
			)
			continue
		}

		slog.Info("jobs.update_storage_bytes.updated",
			"resource_id", id,
			"bytes", bytes,
		)
		updated++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("UpdateStorageBytesWorker: rows error: %w", err)
	}

	var jobID int64
	if job.JobRow != nil {
		jobID = job.ID
	}
	slog.Info("jobs.update_storage_bytes.completed",
		"updated_count", updated,
		"job_id", jobID,
	)
	return nil
}

// resourceTypeEnum maps a resource_type string to the proto enum value.
func resourceTypeEnum(resourceType string) commonv1.ResourceType {
	switch resourceType {
	case "postgres":
		return commonv1.ResourceType_RESOURCE_TYPE_POSTGRES
	case "redis":
		return commonv1.ResourceType_RESOURCE_TYPE_REDIS
	case "mongodb":
		return commonv1.ResourceType_RESOURCE_TYPE_MONGODB
	case "storage":
		return commonv1.ResourceType_RESOURCE_TYPE_STORAGE
	default:
		return commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED
	}
}

// updateStorageBytes sets resources.storage_bytes for the given resource.
func updateStorageBytes(ctx context.Context, db *sql.DB, resourceID uuid.UUID, bytes int64) error {
	_, err := db.ExecContext(ctx,
		`UPDATE resources SET storage_bytes = $1 WHERE id = $2`,
		bytes, resourceID,
	)
	if err != nil {
		return fmt.Errorf("updateStorageBytes: %w", err)
	}
	return nil
}
