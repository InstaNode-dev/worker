package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	madmin "github.com/minio/madmin-go/v3"
	minio "github.com/minio/minio-go/v7"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
	commonv1 "instant.dev/proto/common/v1"
	"instant.dev/worker/internal/metrics"
	"instant.dev/worker/internal/provisioner"
)

// ExpireAnonymousArgs holds the arguments for the ExpireAnonymousJob.
// No fields are needed — it's a periodic maintenance job.
type ExpireAnonymousArgs struct{}

func (ExpireAnonymousArgs) Kind() string { return "expire_anonymous" }

// ExpireAnonymousWorker expires anonymous resources that have passed their expires_at time.
// It calls the provisioner to DROP the physical resource (DB/ACL user/Mongo user) before
// marking the row as deleted, so credentials stop working immediately rather than lingering
// until the next provisioner cycle.
type ExpireAnonymousWorker struct {
	river.WorkerDefaults[ExpireAnonymousArgs]
	db          *sql.DB
	provisioner *provisioner.Client // nil = deprovision skipped (fail open)
	minioClient *madmin.AdminClient // nil = MinIO IAM-user cleanup skipped (legacy self-hosted MinIO backend)
	// objectDeleter deletes a storage resource's objects under its tenant
	// prefix on the S3-compatible OBJECT_STORE_* backend (DO Spaces in prod).
	// This is the only path that actually removes a tenant's objects on
	// expiry — minioClient above only cleans up per-IAM-user metadata, which
	// does not exist on the shared-key OBJECT_STORE_* backend. nil = no
	// deleter wired (CI / docker-compose); the storage case then logs a WARN
	// so a missing cleanup path is visible rather than a silent no-op.
	objectDeleter S3BackupDeleter
	// objectBucket is the bucket the objectDeleter operates on — the shared
	// instant-shared bucket where /storage/new tenant objects live.
	objectBucket string
}

// NewExpireAnonymousWorker constructs an ExpireAnonymousWorker.
// Pass nil for provClient to skip physical deprovisioning (e.g. in tests or when the
// provisioner is unavailable — the DB row is still marked deleted).
// Pass nil for minioClient to skip MinIO IAM user cleanup.
//
// The storage-object deleter and bucket are wired separately via
// WithObjectDeleter — callers that don't set it leave storage expiry as a
// logged WARN (no silent no-op) rather than dropping the tenant's objects.
func NewExpireAnonymousWorker(db *sql.DB, provClient *provisioner.Client, minioClient *madmin.AdminClient) *ExpireAnonymousWorker {
	return &ExpireAnonymousWorker{db: db, provisioner: provClient, minioClient: minioClient}
}

// WithObjectDeleter wires the S3-compatible object deleter used to remove a
// storage resource's objects (under its tenant prefix) on expiry. deleter may
// be nil — the storage case then logs a WARN each time so the missing cleanup
// path is operator-visible. bucket defaults to "instant-shared" when empty.
// Returns the worker for chaining, matching the WithAutopsyK8s pattern.
func (w *ExpireAnonymousWorker) WithObjectDeleter(deleter S3BackupDeleter, bucket string) *ExpireAnonymousWorker {
	if bucket == "" {
		bucket = "instant-shared"
	}
	w.objectDeleter = deleter
	w.objectBucket = bucket
	return w
}

// Work executes the expiry logic.
func (w *ExpireAnonymousWorker) Work(ctx context.Context, job *river.Job[ExpireAnonymousArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.expire_anonymous")
	defer span.End()

	start := time.Now()

	// Step 1: Find all anonymous resources that have passed their TTL.
	// TTL must win over lifecycle state: a paused/suspended anonymous resource
	// whose 24h TTL has elapsed is still an expired resource. Filtering on
	// status='active' alone leaks paused/suspended rows past their TTL — the
	// physical DB/ACL/Mongo user is never dropped and the row never reaches
	// 'deleted'. Expire any non-terminal status past TTL.
	rows, err := w.db.QueryContext(ctx, `
		SELECT id::text, token::text, resource_type, COALESCE(provider_resource_id, '')
		FROM resources
		WHERE team_id IS NULL
		  AND status IN ('active', 'paused', 'suspended')
		  AND expires_at IS NOT NULL
		  AND expires_at < now()
	`)
	if err != nil {
		return fmt.Errorf("ExpireAnonymousWorker: query failed: %w", err)
	}
	defer rows.Close()

	type toExpire struct {
		id                 string
		token              string
		resourceType       string
		providerResourceID string
	}

	var candidates []toExpire
	for rows.Next() {
		var r toExpire
		if err := rows.Scan(&r.id, &r.token, &r.resourceType, &r.providerResourceID); err != nil {
			slog.Warn("jobs.expire_anonymous.scan_failed", "error", err)
			continue
		}
		candidates = append(candidates, r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("ExpireAnonymousWorker: rows error: %w", err)
	}
	rows.Close()

	if len(candidates) == 0 {
		return nil
	}

	// Step 2: For each candidate, deprovision the physical resource then mark the row deleted.
	// Errors on either step are logged but never propagate — fail open so one bad resource
	// does not block the expiry of the remaining batch.
	var expired int
	for _, r := range candidates {
		// Deprovision — credentials become invalid immediately.
		switch r.resourceType {
		case "storage":
			// Two-part cleanup for a storage resource:
			//
			//  1. Object deletion (the part that actually matters). On the
			//     prod OBJECT_STORE_* backend (DO Spaces, shared key) the
			//     tenant's objects live under its prefix in the shared
			//     bucket — they must be deleted explicitly. Relying on the
			//     24h bucket-lifecycle rule alone meant the row flipped to
			//     'deleted' while the objects lingered.
			//  2. Legacy MinIO IAM-user + policy cleanup. Only meaningful on
			//     the self-hosted MinIO backend (per-customer IAM users).
			//     With the OBJECT_STORE_* shared-key backend minioClient is
			//     nil because no per-customer IAM was ever created.
			deleteStorageObjects(ctx, w.objectDeleter, w.objectBucket, r.token, r.providerResourceID, r.id, job.ID)
			if w.minioClient != nil {
				deprovisionMinIOUser(ctx, w.minioClient, r.token, r.id, job.ID)
			}
		default:
			if w.provisioner != nil {
				resType := expireResourceTypeToProto(r.resourceType)
				if resType != commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED {
					if deprovErr := w.provisioner.DeprovisionResource(
						ctx, r.token, r.providerResourceID, resType,
					); deprovErr != nil {
						slog.Warn("jobs.expire_anonymous.deprovision_failed",
							"error", deprovErr,
							"resource_id", r.id,
							"resource_type", r.resourceType,
							"token", r.token,
							"job_id", job.ID,
						)
					}
				}
			}
		}

		// Guarded UPDATE: mark deleted only from a non-terminal status, matching
		// the SELECT above. Gating on status='active' alone would leave the
		// paused/suspended rows we just deprovisioned stuck in their old status.
		if _, err := w.db.ExecContext(ctx,
			`UPDATE resources SET status = 'deleted'
			 WHERE id = $1 AND status IN ('active', 'paused', 'suspended')`,
			r.id,
		); err != nil {
			slog.Error("jobs.expire_anonymous.mark_deleted_failed",
				"error", err,
				"resource_id", r.id,
				"resource_type", r.resourceType,
				"job_id", job.ID,
			)
			continue
		}
		metrics.ExpiredResourcesTotal.Inc()
		expired++
	}

	var activeAnon int
	if scanErr := w.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM resources
		WHERE team_id IS NULL AND status = 'active' AND expires_at IS NOT NULL
	`).Scan(&activeAnon); scanErr == nil {
		metrics.ActiveAnonymousResources.Set(float64(activeAnon))
	}

	slog.Info("jobs.expire_anonymous.completed",
		"expired_count", expired,
		"total_candidates", len(candidates),
		"duration_ms", time.Since(start).Milliseconds(),
		"job_id", job.ID,
	)
	return nil
}

// expireResourceTypeToProto maps a resource_type string to the protobuf enum.
// Queue/NATS now provisions dedicated pods (k8s backend) so it needs deprovisioning
// just like postgres/redis/mongo. Webhook + storage stay UNSPECIFIED — they don't
// have a per-resource pod (webhook is API-receiver only; storage is bucket-isolated).
//
// "vector": pgvector resources share the Postgres backend (db_<token>/usr_<token>).
// Mapping to RESOURCE_TYPE_POSTGRES lets the provisioner DROP DATABASE / DROP USER
// on TTL expiry — same cleanup path as a plain postgres resource. Without this mapping,
// expired vector resources would leave orphaned Postgres databases and users forever.
func expireResourceTypeToProto(resourceType string) commonv1.ResourceType {
	switch resourceType {
	case "postgres":
		return commonv1.ResourceType_RESOURCE_TYPE_POSTGRES
	case "redis":
		return commonv1.ResourceType_RESOURCE_TYPE_REDIS
	case "mongodb":
		return commonv1.ResourceType_RESOURCE_TYPE_MONGODB
	case "queue":
		return commonv1.ResourceType_RESOURCE_TYPE_QUEUE
	case "vector":
		// pgvector-on-Postgres: underlying DB/user cleanup is identical to postgres.
		return commonv1.ResourceType_RESOURCE_TYPE_POSTGRES
	default:
		return commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED
	}
}

// deprovisionMinIOUser removes the MinIO IAM user and prefix-scoped policy for the given token.
// The access key and policy name are derived from the first 8 chars of the token UUID,
// matching the naming convention in api/internal/providers/storage/local.go.
// Errors are logged but not propagated — fail open.
func deprovisionMinIOUser(ctx context.Context, client *madmin.AdminClient, token, resourceID string, jobID int64) {
	prefix := token
	if len(prefix) > 8 {
		prefix = prefix[:8]
	}
	accessKeyID := "key_" + prefix
	policyName := "pol_" + prefix

	if err := client.RemoveUser(ctx, accessKeyID); err != nil {
		slog.Warn("jobs.expire_anonymous.minio_remove_user_failed",
			"access_key_id", accessKeyID,
			"resource_id", resourceID,
			"error", err,
			"job_id", jobID,
		)
	}
	if err := client.RemoveCannedPolicy(ctx, policyName); err != nil {
		slog.Warn("jobs.expire_anonymous.minio_remove_policy_failed",
			"policy_name", policyName,
			"resource_id", resourceID,
			"error", err,
			"job_id", jobID,
		)
	}
	slog.Info("jobs.expire_anonymous.minio_deprovisioned",
		"access_key_id", accessKeyID,
		"resource_id", resourceID,
	)
}

// deleteStorageObjects removes every object under a storage resource's tenant
// prefix in the shared object-store bucket. This is the real cleanup on the
// prod OBJECT_STORE_* backend (DO Spaces) — the resource row flipping to
// 'deleted' does NOT remove the tenant's objects.
//
// The tenant prefix is resolved via minioObjectPrefix (storage_minio.go) — the
// SAME store-at-provision / legacy-token-fallback logic the storage-bytes
// scanner uses, so the two never disagree on which prefix belongs to a
// resource. We do NOT hand-roll a new prefix scheme here.
//
// When no deleter is wired (deleter == nil — CI / docker-compose, no
// OBJECT_STORE_* env vars) we log a WARN rather than silently no-op'ing, so a
// missing cleanup path is visible to operators. Per-object delete errors are
// logged but never propagated — fail open, matching the rest of the expiry
// batch (one bad resource must not block the others).
func deleteStorageObjects(ctx context.Context, deleter S3BackupDeleter, bucket, token, providerResourceID, resourceID string, jobID int64) {
	if deleter == nil {
		// A missing deleter is a real gap, not a benign skip — surface it.
		slog.Warn("jobs.expire_anonymous.storage_objects_not_deleted",
			"resource_id", resourceID,
			"token", token,
			"reason", "no object-store deleter wired (OBJECT_STORE_* unset) — tenant objects rely on the bucket-lifecycle rule",
			"job_id", jobID,
		)
		return
	}

	prefix := minioObjectPrefix(token, providerResourceID)
	if prefix == "" {
		slog.Warn("jobs.expire_anonymous.storage_prefix_empty",
			"resource_id", resourceID,
			"token", token,
			"job_id", jobID,
		)
		return
	}

	// Stream ListObjects → RemoveObjects via a buffered channel so a tenant
	// with thousands of objects doesn't pull the whole listing into memory —
	// the same pipe pattern as team_deletion_executor.deleteS3BackupsForToken.
	objectsCh := make(chan minio.ObjectInfo, 32)
	var objectCount int
	go func() {
		defer close(objectsCh)
		defer func() {
			if r := recover(); r != nil {
				LogRecoveredPanic("expire_anonymous.storage_list", r)
			}
		}()
		for obj := range deleter.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		}) {
			if obj.Err != nil {
				slog.Warn("jobs.expire_anonymous.storage_list_error",
					"resource_id", resourceID,
					"prefix", prefix,
					"error", obj.Err,
					"job_id", jobID,
				)
				return
			}
			objectCount++
			select {
			case objectsCh <- obj:
			case <-ctx.Done():
				return
			}
		}
	}()

	var removeErrors int
	for rmErr := range deleter.RemoveObjects(ctx, bucket, objectsCh, minio.RemoveObjectsOptions{}) {
		if rmErr.Err != nil {
			removeErrors++
			slog.Warn("jobs.expire_anonymous.storage_remove_error",
				"resource_id", resourceID,
				"object_key", rmErr.ObjectName,
				"error", rmErr.Err,
				"job_id", jobID,
			)
		}
	}

	slog.Info("jobs.expire_anonymous.storage_objects_deleted",
		"resource_id", resourceID,
		"token", token,
		"prefix", prefix,
		"objects_listed", objectCount,
		"remove_errors", removeErrors,
		"job_id", jobID,
	)
}
