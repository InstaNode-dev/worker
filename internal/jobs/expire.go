package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	madmin "github.com/minio/madmin-go/v3"
	minio "github.com/minio/minio-go/v7"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
	"instant.dev/common/resourcestatus"
	commonv1 "instant.dev/proto/common/v1"
	"instant.dev/worker/internal/logsafe"
	"instant.dev/worker/internal/metrics"
	"instant.dev/worker/internal/provisioner"
)

// reapableStatusSQLList is the canonical SQL `IN (...)` body listing the
// statuses a TTL-expiry sweep may act on (active / paused / suspended),
// derived from resourcestatus.ReapableStatuses() so the reaper's SQL
// filter can never drift from the shared Go predicate Status.IsReapable.
// TTL must win over lifecycle state: a paused/suspended resource past its
// 24h TTL is still expired and must be deprovisioned.
//
// Built once at init from a fixed enum of literals (no caller input), so
// there is no SQL-injection surface — the values are 'active', 'paused',
// 'suspended'.
var reapableStatusSQLList = func() string {
	quoted := make([]string, 0, 3)
	for _, s := range resourcestatus.ReapableStatuses() {
		quoted = append(quoted, "'"+s+"'")
	}
	return strings.Join(quoted, ", ")
}()

// toExpire is one candidate row carried from the batch SELECT to the per-row
// reapOne tx. Package-level (rather than function-local) so reapOne can take
// it as a parameter — the per-row tx wrapper lives outside Work() so a
// regression test can drive it directly.
type toExpire struct {
	id                 string
	token              string
	resourceType       string
	providerResourceID string
}

// ExpireAnonymousArgs holds the arguments for the ExpireAnonymousJob.
// No fields are needed — it's a periodic maintenance job.
type ExpireAnonymousArgs struct{}

func (ExpireAnonymousArgs) Kind() string { return "expire_anonymous" }

// ResourceDeprovisioner is the narrow seam the reaper uses to tear down a
// resource's physical backend (DROP DATABASE / DROP USER / delete NATS pod).
// Lifted to an interface so a test can inject a fake that fails the
// deprovision and assert the reaper does NOT then mark the row deleted
// (MR-P0-1a, BugBash 2026-05-20). The concrete *provisioner.Client satisfies
// it. nil = deprovision skipped (fail open) — same posture as before.
type ResourceDeprovisioner interface {
	DeprovisionResource(ctx context.Context, token, providerResourceID string, resType commonv1.ResourceType) error
}

// compile-time assertion: the real provisioner client satisfies the seam.
var _ ResourceDeprovisioner = (*provisioner.Client)(nil)

// ExpireAnonymousWorker expires anonymous resources that have passed their expires_at time.
// It calls the provisioner to DROP the physical resource (DB/ACL user/Mongo user) before
// marking the row as deleted, so credentials stop working immediately rather than lingering
// until the next provisioner cycle.
type ExpireAnonymousWorker struct {
	river.WorkerDefaults[ExpireAnonymousArgs]
	db          *sql.DB
	provisioner ResourceDeprovisioner // nil = deprovision skipped (fail open)
	minioClient *madmin.AdminClient   // nil = MinIO IAM-user cleanup skipped (legacy self-hosted MinIO backend)
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
// Pass nil for provClient to skip physical deprovisioning (e.g. in tests or
// when the provisioner is unavailable — the deprovision step is then skipped,
// and per MR-P0-1a the row is left in its reapable status for a later retry,
// NOT marked deleted).
// Pass nil for minioClient to skip MinIO IAM user cleanup.
//
// The storage-object deleter and bucket are wired separately via
// WithObjectDeleter — callers that don't set it leave storage expiry as a
// logged WARN (no silent no-op) rather than dropping the tenant's objects.
func NewExpireAnonymousWorker(db *sql.DB, provClient *provisioner.Client, minioClient *madmin.AdminClient) *ExpireAnonymousWorker {
	w := &ExpireAnonymousWorker{db: db, minioClient: minioClient}
	// A typed-nil *provisioner.Client stored straight into the interface
	// field would make `w.provisioner != nil` true and panic on call. Only
	// assign when the pointer is genuinely non-nil so the nil-skip guard in
	// Work() behaves.
	if provClient != nil {
		w.provisioner = provClient
	}
	return w
}

// WithDeprovisioner overrides the deprovisioner seam — used by tests to
// inject a fake that fails the deprovision call so the MR-P0-1a regression
// (a failed deprovision must NOT mark the row deleted) can be exercised
// without a live provisioner. Returns the worker for chaining.
func (w *ExpireAnonymousWorker) WithDeprovisioner(d ResourceDeprovisioner) *ExpireAnonymousWorker {
	w.provisioner = d
	return w
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

	// Step 1: Find all expired anonymous AND free-tier resources past their TTL.
	//
	// TTL must win over lifecycle state: a paused/suspended resource whose 24h
	// TTL has elapsed is still an expired resource. Filtering on status='active'
	// alone leaks paused/suspended rows past their TTL — the physical
	// DB/ACL/Mongo user is never dropped and the row never reaches 'deleted'.
	// Expire any non-terminal status past TTL.
	//
	// Two expiry classes share this reaper:
	//   - tier='anonymous': never-claimed resources; team_id IS NULL.
	//   - tier='free':      claimed-but-unpaid resources; they DO carry a
	//     non-null team_id and a 24h expires_at. 'free' is the modal "claimed
	//     but didn't pay" outcome. The old predicate (team_id IS NULL only)
	//     excluded every free row, so claimed-but-unpaid infra leaked
	//     continuously — the customer DB / Redis ACL user / Mongo user was
	//     never dropped and the row never reached 'deleted'. The free path is
	//     identical to the anonymous one below: deprovision is keyed purely on
	//     resource_type + token + provider_resource_id (free rows have a real
	//     provider_resource_id), so it resolves the right backing infra
	//     regardless of whether team_id is set.
	//
	// MR-P1-7 (T5 P1-7, BugBash 2026-05-20): exclude free-tier resources whose
	// owning team is inside its 30-day restorable deletion grace window
	// (teams.status='deletion_requested'). Such resources are paused (still
	// reapable by lifecycle filter) and may carry a past expires_at — without
	// this guard the reaper would DROP the customer's DB before the 30-day
	// restore window elapsed, so a subsequent /teams/:id/restore would return
	// an "active" account whose data is gone. The team-deletion executor is
	// the authorized destructor for that data path once the grace expires;
	// the reaper must stay out of it. teams.status='active' is the only
	// state in which a free row is safe to reap; deletion_pending /
	// tombstoned teams are out-of-band (their resources have already been
	// deprovisioned by the executor, or are about to be). 'anonymous' rows
	// have team_id IS NULL by construction so the LEFT JOIN does not filter
	// them — they cannot be in a deletion grace window.
	//
	// NOTE: api-side models.ExpireAnonymousResources covers both tiers in SQL
	// but has zero non-test callers (dead code) and only flips the DB row — it
	// never calls the provisioner. It should be removed from the api repo
	// (out of scope here); this worker is the sole live reaper.
	rows, err := w.db.QueryContext(ctx, `
		SELECT r.id::text, r.token::text, r.resource_type, COALESCE(r.provider_resource_id, '')
		FROM resources r
		LEFT JOIN teams t ON t.id = r.team_id
		WHERE ((r.team_id IS NULL AND r.tier = 'anonymous') OR r.tier = 'free')
		  AND r.status IN (`+reapableStatusSQLList+`)
		  AND r.expires_at IS NOT NULL
		  AND r.expires_at < now()
		  AND (r.team_id IS NULL OR t.status = 'active')
	`)
	if err != nil {
		return fmt.Errorf("ExpireAnonymousWorker: query failed: %w", err)
	}
	defer rows.Close()

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

	// Step 2: For each candidate, deprovision the physical resource then mark
	// the row deleted. One bad resource never blocks the expiry of the rest of
	// the batch (fail open).
	//
	// MR-P0-1a (BugBash 2026-05-20, cross-confirmed by T1/T5/T20/T24): the row
	// is marked status='deleted' ONLY when the backend teardown genuinely
	// succeeded (or there was nothing to tear down). A 'deleted' row is
	// terminal and invisible to every reconciler — so marking it deleted while
	// the physical Postgres DB / Redis ACL / Mongo user / NATS pod is still
	// live orphans that backend forever (it bills real money and consumes
	// shared-cluster capacity). On a deprovision failure we instead leave the
	// row in its current reapable status: the next reaper tick (or the team-
	// deletion executor) retries the teardown.
	var expired int
	for _, r := range candidates {
		if w.reapOne(ctx, job.ID, r) {
			expired++
		}
	}

	var activeAnon int
	if scanErr := w.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM resources
		WHERE team_id IS NULL AND status = '`+resourcestatus.StatusActive.String()+`' AND expires_at IS NOT NULL
	`).Scan(&activeAnon); scanErr == nil {
		metrics.ActiveAnonymousResources.Set(float64(activeAnon))
	}

	// Wave 3 / Worker T21 P1-1 follow-up (#146): demote idle-tick INFO →
	// DEBUG. expire_anonymous runs every 1h; an idle tick (zero candidates
	// AND nothing expired) is heartbeat noise. INFO is reserved for ticks
	// that actually changed state (expired > 0, or non-zero candidates
	// that were inspected even if none could be reaped).
	if expired == 0 && len(candidates) == 0 {
		slog.Debug("jobs.expire_anonymous.completed",
			"expired_count", 0,
			"total_candidates", 0,
			"duration_ms", time.Since(start).Milliseconds(),
			"job_id", job.ID,
		)
		return nil
	}
	slog.Info("jobs.expire_anonymous.completed",
		"expired_count", expired,
		"total_candidates", len(candidates),
		"duration_ms", time.Since(start).Milliseconds(),
		"job_id", job.ID,
	)
	return nil
}

// reapOne deprovisions and marks-deleted a single candidate row under a
// SERIALIZED row lock so a concurrent `subscription.charged` webhook clearing
// `expires_at` / promoting `tier` cannot race the reaper into dropping a
// just-paid customer DB (MR-P1-5 / T5 P0-3, BugBash 2026-05-20).
//
// THE RACE (without this guard):
//  1. Batch SELECT sees `tier='free' AND expires_at < now()` for row R.
//  2. Customer's `subscription.charged` webhook fires;
//     `ElevateResourceTiersByTeam` clears `expires_at` + sets `tier='pro'`.
//  3. Reaper calls `DeprovisionResource` (DROP DATABASE / DROP USER) on R.
//  4. Webhook completes; row R is now `tier='pro', expires_at=NULL`, status
//     active — but the physical backing infra is gone.
//
// THE FIX: open a tx and `SELECT … FOR UPDATE` the row by id, re-confirming
// the still-reapable predicate (tier IN ('anonymous','free'), expires_at past,
// status reapable, team active). If the upgrade has won the race, the
// re-confirmation returns false and we abort the deprovision. Otherwise we
// run the deprovision and the mark-deleted UPDATE inside the same tx; the
// upgrade webhook either blocked on the row lock (and now finds a `deleted`
// row, its UPDATE is a no-op because `ElevateResourceTiersByTeam` filters
// non-deleted statuses) or completed before our SELECT (in which case our
// re-confirm finds the row already non-reapable and we skip).
//
// Defense-in-depth for T5 P1-7 ("don't reap a free row whose team is in
// deletion_requested grace"): the re-confirm also rechecks teams.status. The
// outer batch SELECT already filters teams.status='active', but a team
// status flip between batch-select and per-row lock must still be honored.
//
// Returns true iff the row was deprovisioned + marked deleted (so the
// caller increments the `expired` count); false if the row was skipped
// (race lost / team grace / deprovision failed / commit failed).
func (w *ExpireAnonymousWorker) reapOne(ctx context.Context, jobID int64, r toExpire) bool {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("jobs.expire_anonymous.begin_tx_failed",
			"error", err,
			"resource_id", r.id,
			"resource_type", r.resourceType,
			"job_id", jobID,
		)
		return false
	}
	committed := false
	defer func() {
		if !committed {
			// Rollback is safe (and a no-op if we already committed). On the
			// race-lost / deprovision-failed paths we want the tx aborted so
			// the row lock releases and the next tick can reacquire it.
			_ = tx.Rollback()
		}
	}()

	// Re-confirm the row still meets the reaper predicate while holding a
	// row-exclusive lock. A concurrent upgrade webhook is blocked here until
	// COMMIT/ROLLBACK; if it ran *before* we got the lock, the EXISTS returns
	// false because tier/expires_at no longer match. We hold the lock across
	// the deprovision RPC + UPDATE so the upgrade webhook either runs after
	// the row is `deleted` (and its UPDATE is a no-op — `ElevateResourceTiers
	// ByTeam` filters non-deleted statuses), or it already committed before
	// our SELECT (and we abort with race_skipped).
	//
	// We don't use SKIP LOCKED — the reaper IS the authorized deletion path
	// for this row class; if a concurrent updater holds the lock briefly we
	// want to wait (sub-second), not skip and retry next tick (we already
	// serialized SELECT→deprovision per row).
	var stillReapable bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM resources r
			LEFT JOIN teams t ON t.id = r.team_id
			WHERE r.id = $1
			  AND ((r.team_id IS NULL AND r.tier = 'anonymous') OR r.tier = 'free')
			  AND r.status IN (`+reapableStatusSQLList+`)
			  AND r.expires_at IS NOT NULL
			  AND r.expires_at < now()
			  AND (r.team_id IS NULL OR t.status = 'active')
			FOR UPDATE OF r
		)
	`, r.id).Scan(&stillReapable); err != nil {
		slog.Error("jobs.expire_anonymous.reconfirm_failed",
			"error", err,
			"resource_id", r.id,
			"resource_type", r.resourceType,
			"job_id", jobID,
		)
		return false
	}
	if !stillReapable {
		// MR-P1-5 race won by the upgrade webhook (or the team flipped to a
		// deletion grace state since batch-select). The row has been
		// promoted to a paid tier / had expires_at cleared / the team
		// entered deletion_requested. DO NOT deprovision — the customer
		// just paid for, or is restoring, this resource.
		metrics.ExpireRaceSkippedTotal.Inc()
		slog.Info("jobs.expire_anonymous.race_skipped",
			"resource_id", r.id,
			"resource_type", r.resourceType,
			"token", logsafe.Token(r.token),
			"job_id", jobID,
			"reason", "row no longer matches reaper predicate at FOR UPDATE re-confirm — "+
				"upgrade webhook or team deletion grace won the race (MR-P1-5 / P1-7)",
		)
		return false
	}

	// deprovisionFailed gates the mark-deleted UPDATE below. true = a
	// backend teardown call returned an error this tick; the row stays
	// reapable (tx rolls back) so a later tick retries instead of stranding
	// the infra.
	deprovisionFailed := false

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
		deleteStorageObjects(ctx, w.objectDeleter, w.objectBucket, r.token, r.providerResourceID, r.id, jobID)
		if w.minioClient != nil {
			deprovisionMinIOUser(ctx, w.minioClient, r.token, r.id, jobID)
		}
	default:
		// Physical teardown is keyed purely on resource_type + token +
		// provider_resource_id — it makes no distinction between an
		// 'anonymous' row (team_id IS NULL) and a claimed-but-unpaid
		// 'free' row (team_id set). A free row carries a real
		// provider_resource_id, so DeprovisionResource resolves the
		// same backing infra (DROP DATABASE / DROP USER / etc.) and
		// the credentials become invalid immediately, exactly as for
		// the anonymous case.
		if w.provisioner != nil {
			resType := expireResourceTypeToProto(r.resourceType)
			if resType != commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED {
				if deprovErr := w.provisioner.DeprovisionResource(
					ctx, r.token, r.providerResourceID, resType,
				); deprovErr != nil {
					// MR-P0-1a: a failed teardown must NOT advance the
					// row to 'deleted'. Flag it so the mark-deleted
					// UPDATE below is skipped; the tx rolls back, the
					// row stays reapable, the next tick retries the DROP.
					deprovisionFailed = true
					slog.Warn("jobs.expire_anonymous.deprovision_failed",
						"error", deprovErr,
						"resource_id", r.id,
						"resource_type", r.resourceType,
						"token", logsafe.Token(r.token),
						"job_id", jobID,
						"effect", "tx rollback; row left reapable for retry; NOT marked deleted (MR-P0-1a)",
					)
				}
			}
		}
	}

	// MR-P0-1a: skip the mark-deleted UPDATE when the backend teardown
	// failed this tick. Marking a row 'deleted' (a terminal, non-reapable
	// status) with live backend infra behind it permanently orphans that
	// infra — no reconciler ever revisits a 'deleted' row.
	if deprovisionFailed {
		metrics.ExpireDeprovisionFailedTotal.Inc()
		return false
	}

	// Guarded UPDATE: mark deleted only from a non-terminal status, matching
	// the SELECT above. Runs inside the same tx as the FOR UPDATE so the row
	// lock spans SELECT→deprovision→UPDATE.
	if _, err := tx.ExecContext(ctx,
		`UPDATE resources SET status = '`+resourcestatus.StatusDeleted.String()+`'
		 WHERE id = $1 AND status IN (`+reapableStatusSQLList+`)`,
		r.id,
	); err != nil {
		slog.Error("jobs.expire_anonymous.mark_deleted_failed",
			"error", err,
			"resource_id", r.id,
			"resource_type", r.resourceType,
			"job_id", jobID,
		)
		return false
	}
	if err := tx.Commit(); err != nil {
		slog.Error("jobs.expire_anonymous.commit_failed",
			"error", err,
			"resource_id", r.id,
			"resource_type", r.resourceType,
			"job_id", jobID,
		)
		return false
	}
	committed = true
	metrics.ExpiredResourcesTotal.Inc()
	return true
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
			"token", logsafe.Token(token),
			"reason", "no object-store deleter wired (OBJECT_STORE_* unset) — tenant objects rely on the bucket-lifecycle rule",
			"job_id", jobID,
		)
		return
	}

	prefix := minioObjectPrefix(token, providerResourceID)
	if prefix == "" {
		slog.Warn("jobs.expire_anonymous.storage_prefix_empty",
			"resource_id", resourceID,
			"token", logsafe.Token(token),
			"job_id", jobID,
		)
		return
	}

	// Stream ListObjects → RemoveObjects via a buffered channel so a tenant
	// with thousands of objects doesn't pull the whole listing into memory —
	// the same pipe pattern as team_deletion_executor.deleteS3BackupsForToken.
	objectsCh := make(chan minio.ObjectInfo, 32)
	// objectCount is incremented in the producer goroutine below and read
	// after the RemoveObjects drain loop on the consumer side — use an
	// atomic so the cross-goroutine read in the final slog is race-free
	// (the -race build flagged the plain-int read otherwise).
	var objectCount atomic.Int64
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
			objectCount.Add(1)
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
		"token", logsafe.Token(token),
		"prefix", prefix,
		"objects_listed", objectCount.Load(),
		"remove_errors", removeErrors,
		"job_id", jobID,
	)
}
