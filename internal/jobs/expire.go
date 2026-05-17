package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	madmin "github.com/minio/madmin-go/v3"
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
	minioClient *madmin.AdminClient // nil = MinIO deprovision skipped (fail open)
}

// NewExpireAnonymousWorker constructs an ExpireAnonymousWorker.
// Pass nil for provClient to skip physical deprovisioning (e.g. in tests or when the
// provisioner is unavailable — the DB row is still marked deleted).
// Pass nil for minioClient to skip MinIO IAM user cleanup.
func NewExpireAnonymousWorker(db *sql.DB, provClient *provisioner.Client, minioClient *madmin.AdminClient) *ExpireAnonymousWorker {
	return &ExpireAnonymousWorker{db: db, provisioner: provClient, minioClient: minioClient}
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
			// Remove the MinIO IAM user and prefix-scoped policy.
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
