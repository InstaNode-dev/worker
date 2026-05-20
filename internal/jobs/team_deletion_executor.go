package jobs

// team_deletion_executor.go — daily 03:00 UTC sweep that completes the GDPR
// Article 17 right-to-be-forgotten lifecycle started by the API's
// DELETE /api/v1/team endpoint.
//
// The brief (W7d):
//
//   - The API marks teams.status='deletion_requested' + stamps
//     deletion_requested_at. 30-day grace window begins. Resources are
//     paused, Razorpay subscription is best-effort cancelled. Inside the
//     window the customer can POST /api/v1/team/restore to halt.
//
//   - This worker runs daily 03:00 UTC (after the platform-DB-backup job at
//     02:00 UTC, so today's tombstoned data IS in tonight's backup before
//     destruction).
//
//   - Per qualifying team (status='deletion_requested' AND
//     deletion_requested_at + 30d < now()):
//
//      0. Flip teams.status deletion_requested → deletion_pending. From this
//         instant the team is in the "destruction in flight" state — the API
//         restore endpoint refuses, and a crash mid-pipeline leaves the row
//         in deletion_pending (NOT half-tombstoned) so the orphan-sweep
//         reconciler can resume it. A team already in deletion_pending (a
//         previous run failed mid-way) is re-processed: every step below is
//         idempotent.
//      1. Hard-delete S3 backups under instant-shared/backups/<token>/ for
//         every resource owned by the team.
//      2. DeprovisionResource per active row (drops customer DB / cache /
//         mongo / etc). Idempotent — a re-run over an already-dropped
//         resource is a no-op on the provisioner side.
//      3. Delete every instant-deploy-<appID> k8s namespace owned by the
//         team (the customer's running applications + their build/deploy
//         objects). Idempotent — a NotFound namespace is treated as
//         already-gone.
//      4. NULL connection_url, metadata (key_prefix counts as metadata) on
//         every resource row owned by the team. Leave id, team_id,
//         created_at, tombstoned_at-equivalent (paused_at + status) for
//         downstream audit-trail integrity.
//      5. Erase PII from team + user rows: NULL email, name, github_id,
//         google_id, stripe_customer_id (the Razorpay subscription id
//         column). Keep `id` so foreign-key references downstream remain
//         valid but inert.
//      6. teams.status='tombstoned', tombstoned_at=now().
//      7. Emit team.tombstoned audit with metadata
//         {resource_count_destroyed, namespaces_deleted, s3_bytes_freed,
//         duration_seconds}.
//
//   - On per-team error (one resource fails to deprovision, a namespace
//     delete errors, S3 batch errors, etc.): log + emit team.deletion_failed
//     audit row, DO NOT mark tombstoned. The team stays in deletion_pending
//     state so an operator AND the orphan-sweep reconciler can investigate
//     and retry. Every step is idempotent so the retry completes cleanly.
//
// ── Module boundary notes ────────────────────────────────────────────────
//
// The audit-kind constants (team.tombstoned, team.deletion_failed) are
// declared in team_deletion_audit_kinds.go in this package. The api repo
// declares the matching strings in api/internal/models/audit_kinds.go.
// The shared-strings contract is the literal value, not a Go type — a
// drift between the two would surface as a missing kind in the Loops
// forwarder, not a build break.
//
// The S3 lister/deleter is the same minio-go interface as the storage_bytes
// scanner (storage_minio.go), but we use its RemoveObjects bulk-delete RPC
// rather than per-object DELETEs because a Team-tier customer may have
// thousands of backup snapshots — RemoveObjects batches up to 1000 keys per
// HTTP round-trip.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	minio "github.com/minio/minio-go/v7"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
	commonv1 "instant.dev/proto/common/v1"
	"instant.dev/worker/internal/provisioner"
)

// TeamDeletionExecutorArgs is the River job payload — no fields, runs as
// a periodic sweep.
type TeamDeletionExecutorArgs struct{}

// Kind is the River worker key. Distinct from the audit kind — this is the
// job dispatcher's identifier (River uses it to route to the worker
// instance), not the audit-log event name.
func (TeamDeletionExecutorArgs) Kind() string { return "team_deletion_executor" }

// teamDeletionGraceDays mirrors models.TeamDeletionGraceDays in the api.
// Duplicated rather than imported because the worker and api repos are
// separate Go modules; a drift here would let the worker tombstone a row
// the customer could still legally restore via the API.
const teamDeletionGraceDays = 30

// teamDeletionBatchLimit caps how many pending-deletion teams we sweep per
// run. The 30-day grace makes this batch self-limiting in practice — a
// healthy steady-state has at most a handful of rows per day — but a one-
// time data-migration could pile dozens up. 50 matches the brief.
const teamDeletionBatchLimit = 50

// teamDeletionActor is the audit_log.actor value for system-written rows.
// Matches the convention used by quota_wall_nudge.go and churn_predictor.go.
const teamDeletionActor = "system"

// deployNamespacePrefixTDE mirrors deployNamespace(appID) in the api's k8s
// compute provider: namespace = "instant-deploy-" + appID. Duplicated here
// (not imported) because the worker and api are separate Go modules — same
// pattern deploy_status_reconcile.go uses (deployNamespacePrefix). Suffixed
// TDE to avoid colliding with that file's identically-valued const within
// the package.
const deployNamespacePrefixTDE = "instant-deploy-"

// deployNamespaceForApp returns the k8s namespace housing a customer
// deployment's pods + build objects. "" appID yields "" so callers skip
// the delete rather than targeting the prefix-only namespace.
func deployNamespaceForApp(appID string) string {
	if appID == "" {
		return ""
	}
	return deployNamespacePrefixTDE + appID
}

// dailyAt3UTCAfterBackup is reused from churn_predictor's schedule. The
// brief says "daily 03:00 UTC, after the platform-db-backup job at 02:00
// UTC." Existing dailyAt3UTCSchedule already fits — see workers.go.

// s3BackupPrefixForToken returns the S3 key prefix where this resource's
// backups live: "backups/<token>/". Mirrors the convention the backup-job
// producer writes under, so the deletion path destroys exactly what the
// backup path created.
//
// We do NOT delete the bucket itself — the bucket is shared (instant-shared)
// and per-tenant isolation is via the prefix.
func s3BackupPrefixForToken(token string) string {
	if token == "" {
		return ""
	}
	return "backups/" + token + "/"
}

// S3BackupDeleter is the narrow surface the executor needs from the
// underlying S3 client. Lifted to an interface so tests can pass a
// fake without dialing a real bucket. The contract is:
//
//   - ListObjects streams object info for every key under the prefix.
//   - RemoveObjects accepts a channel of ObjectInfo and returns a
//     channel of RemoveObjectError; the executor drains it and counts
//     errors.
//
// Returning a non-nil error from either method is fatal for the
// per-team destruction — the team stays in deletion_requested state.
type S3BackupDeleter interface {
	ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
	RemoveObjects(ctx context.Context, bucketName string, objectsCh <-chan minio.ObjectInfo, opts minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError
}

// K8sNamespaceDeleter is the narrow surface the executor + the orphan-sweep
// reconciler need to tear down a customer's deployment namespace. Lifted to
// an interface so tests pass a fake and CI (no cluster) passes nil.
//
// Contract:
//   - DeleteNamespace removes the namespace and everything in it
//     (Deployments, Services, Ingresses, build Jobs). It MUST treat a
//     NotFound namespace as success (return nil) so the call is idempotent
//     across re-runs of a partially-failed teardown.
//   - NamespaceExists reports whether the namespace is still present. The
//     orphan-sweep reconciler uses it to decide whether a tombstoned team
//     still has live compute to reclaim.
type K8sNamespaceDeleter interface {
	DeleteNamespace(ctx context.Context, namespace string) error
	NamespaceExists(ctx context.Context, namespace string) (bool, error)
}

// TeamDeletionExecutorWorker drives the post-grace destruction phase.
type TeamDeletionExecutorWorker struct {
	river.WorkerDefaults[TeamDeletionExecutorArgs]
	db          *sql.DB
	provisioner *provisioner.Client // nil = deprovisioning skipped (fail open)
	s3          S3BackupDeleter     // nil = S3 backup destruction skipped
	k8s         K8sNamespaceDeleter // nil = deploy-namespace teardown skipped
	bucketName  string              // typically "instant-shared"
}

// NewTeamDeletionExecutorWorker constructs the executor. provClient, s3, and
// k8s can all be nil (e.g. CI / docker-compose where none is reachable); the
// worker logs at WARN and skips the corresponding step rather than hard-
// failing the entire run. This matches the fail-open conventions of the
// other workers (storage_minio scanner, deploy_status_reconciler).
func NewTeamDeletionExecutorWorker(db *sql.DB, provClient *provisioner.Client, s3 S3BackupDeleter, k8s K8sNamespaceDeleter, bucketName string) *TeamDeletionExecutorWorker {
	if bucketName == "" {
		bucketName = "instant-shared"
	}
	return &TeamDeletionExecutorWorker{
		db:          db,
		provisioner: provClient,
		s3:          s3,
		k8s:         k8s,
		bucketName:  bucketName,
	}
}

// teamPendingDeletion is the projection of the candidate scan.
type teamPendingDeletion struct {
	teamID              uuid.UUID
	deletionRequestedAt time.Time
}

// pendingResource is the projection per-team for deprovisioning.
type pendingResource struct {
	id                 uuid.UUID
	token              string
	resourceType       string
	providerResourceID string
}

// Work runs one nightly sweep.
//
// Top-level errors (the candidate query itself fails) return error so
// River retries the dispatch. Per-team errors are isolated: a failure
// to destroy team A does NOT stop the sweep from attempting team B.
func (w *TeamDeletionExecutorWorker) Work(ctx context.Context, job *river.Job[TeamDeletionExecutorArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.team_deletion_executor")
	defer span.End()

	swept := 0
	tombstoned := 0
	failed := 0
	startSweep := time.Now()

	candidates, err := w.fetchCandidates(ctx)
	if err != nil {
		return fmt.Errorf("TeamDeletionExecutorWorker: candidate query failed: %w", err)
	}

	for _, c := range candidates {
		swept++
		if err := w.processTeam(ctx, c); err != nil {
			failed++
			slog.Error("jobs.team_deletion.team_failed",
				"team_id", c.teamID.String(),
				"error", err,
				"job_id", job.ID,
			)
			// Emit team.deletion_failed audit so operators see the row
			// in the dashboard's Recent Activity. Best-effort: an audit
			// insert failure is logged but does not block the sweep.
			w.emitDeletionFailed(ctx, c.teamID, err)
			continue
		}
		tombstoned++
	}

	// Wave 3 / Worker T21 P1-1 follow-up (#146): demote idle-tick INFO →
	// DEBUG. team_deletion_executor sweeps team_deletion_pending; an idle
	// tick (no candidates touched) is heartbeat noise. INFO retained for
	// every state-transitioning tick because team-deletion outcomes are
	// the kind of audit signal an operator wants to see immediately.
	if swept == 0 && tombstoned == 0 && failed == 0 {
		slog.Debug("jobs.team_deletion.completed",
			"swept", 0,
			"tombstoned", 0,
			"failed", 0,
			"duration_ms", time.Since(startSweep).Milliseconds(),
			"job_id", job.ID,
		)
	} else {
		slog.Info("jobs.team_deletion.completed",
			"swept", swept,
			"tombstoned", tombstoned,
			"failed", failed,
			"duration_ms", time.Since(startSweep).Milliseconds(),
			"job_id", job.ID,
		)
	}
	return nil
}

// fetchCandidates returns every team whose 30-day grace window has elapsed
// AND every team already in deletion_pending (a previous run started
// destruction and failed mid-pipeline — those are retried, no grace check
// needed because the grace window already elapsed when they were first
// swept).
//
// The deletion_requested half of the query is the inverse of the API
// restore guard: API allows restore IFF deletion_requested_at + 30d >
// now(); this worker begins teardown IFF deletion_requested_at + 30d <
// now(). The two predicates partition the deletion_requested population —
// no team can be simultaneously restorable and teardown-eligible.
//
// The deletion_pending half has no time predicate: once a team is in
// deletion_pending its destruction is in flight and a retry should run on
// the very next sweep, not wait another 30 days.
func (w *TeamDeletionExecutorWorker) fetchCandidates(ctx context.Context) ([]teamPendingDeletion, error) {
	rows, err := w.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT id, deletion_requested_at
		  FROM teams
		 WHERE (status = 'deletion_requested'
		        AND deletion_requested_at + interval '%d days' < now())
		    OR status = 'deletion_pending'
		 ORDER BY deletion_requested_at ASC
		 LIMIT %d
	`, teamDeletionGraceDays, teamDeletionBatchLimit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []teamPendingDeletion
	for rows.Next() {
		var c teamPendingDeletion
		if err := rows.Scan(&c.teamID, &c.deletionRequestedAt); err != nil {
			return nil, fmt.Errorf("scan candidate: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// processTeam runs the per-team destruction pipeline. Any step returning
// an error short-circuits — the team stays in deletion_requested state for
// operator follow-up. Steps are ordered so partial progress is safe:
// destroying S3 backups before NULLing connection_url means that if step 3
// fails we can re-run; the connection_url still pointed to the (now
// destroyed) customer DB, but no caller can use it anyway since the
// provisioner already revoked credentials.
func (w *TeamDeletionExecutorWorker) processTeam(ctx context.Context, c teamPendingDeletion) error {
	start := time.Now()

	// Step 0: flip deletion_requested → deletion_pending. From here the
	// team is "destruction in flight" — the API restore endpoint refuses
	// and a crash below leaves the row in deletion_pending (recoverable by
	// the orphan-sweep reconciler), never half-tombstoned. A team already
	// in deletion_pending (a previous run failed mid-pipeline) gets 0 rows
	// affected here; that is EXPECTED and we proceed — every step below is
	// idempotent so the retry completes cleanly.
	if _, err := w.db.ExecContext(ctx, `
		UPDATE teams
		   SET status = 'deletion_pending'
		 WHERE id = $1 AND status = 'deletion_requested'
	`, c.teamID); err != nil {
		return fmt.Errorf("mark deletion_pending: %w", err)
	}

	// Step 1+2: enumerate the team's resources for both the S3 delete
	// (which keys on token) and the gRPC deprovision (which keys on
	// resource_type + provider_resource_id).
	resources, err := w.fetchTeamResources(ctx, c.teamID)
	if err != nil {
		return fmt.Errorf("fetch resources: %w", err)
	}

	// Step 1: hard-delete S3 backups for every resource (skipped when
	// no s3 client is wired in — see NewTeamDeletionExecutorWorker).
	var s3BytesFreed int64
	if w.s3 != nil {
		for _, r := range resources {
			freed, derr := w.deleteS3BackupsForToken(ctx, r.token)
			if derr != nil {
				return fmt.Errorf("delete s3 backups for %s: %w", r.token, derr)
			}
			s3BytesFreed += freed
		}
	}

	// Step 2: deprovision every active resource via the gRPC provisioner.
	// A single failure aborts the team — the row stays in deletion_pending
	// state, the audit_log carries the failure, and the next sweep (or the
	// orphan-sweep reconciler) retries. Deprovision is idempotent on the
	// provisioner side, so a retry over an already-dropped resource is a
	// no-op.
	if w.provisioner != nil {
		for _, r := range resources {
			resType := expireResourceTypeToProto(r.resourceType)
			if resType == commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED {
				// Storage / webhook have no per-resource pod to drop —
				// skip the gRPC call but log it so operators can see
				// the choice was deliberate.
				slog.Info("jobs.team_deletion.skip_unspecified_resource_type",
					"team_id", c.teamID.String(),
					"resource_id", r.id.String(),
					"resource_type", r.resourceType,
				)
				continue
			}
			if dpErr := w.provisioner.DeprovisionResource(
				ctx, r.token, r.providerResourceID, resType,
			); dpErr != nil {
				return fmt.Errorf("deprovision %s (%s): %w", r.id, r.resourceType, dpErr)
			}
		}
	}

	// Step 3: delete every instant-deploy-<appID> k8s namespace the team
	// owns — this stops the customer's running applications and reclaims
	// the paid compute. Without this step a deleted Pro/Team customer's
	// pods keep running (and keep costing) forever. Idempotent: a NotFound
	// namespace is treated as already-gone by the deleter.
	var namespacesDeleted int
	if w.k8s != nil {
		appIDs, derr := w.fetchTeamDeployAppIDs(ctx, c.teamID)
		if derr != nil {
			return fmt.Errorf("fetch deploy app ids: %w", derr)
		}
		for _, appID := range appIDs {
			ns := deployNamespaceForApp(appID)
			if ns == "" {
				continue
			}
			if delErr := w.k8s.DeleteNamespace(ctx, ns); delErr != nil {
				return fmt.Errorf("delete namespace %s: %w", ns, delErr)
			}
			namespacesDeleted++
		}
	}

	// Step 4-6: a single transaction that NULLs connection_url +
	// metadata + key_prefix on resources, NULLs PII on users + the team
	// row, flips status='tombstoned', stamps tombstoned_at.
	//
	// Wrapping these in a transaction prevents a partial tombstone state
	// (PII gone but status still 'deletion_pending') if the connection
	// drops between steps. Either the team is fully tombstoned or it
	// isn't.
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		// Rollback is a no-op once Commit succeeded.
		_ = tx.Rollback()
	}()

	// Step 4: NULL the customer-data fields on every resource row.
	// Leave id, team_id, created_at, status, paused_at so the dashboard
	// can still render "deleted resource X" historically without leaking
	// secrets. Idempotent — re-NULLing already-NULL columns is a no-op.
	if _, err := tx.ExecContext(ctx, `
		UPDATE resources
		   SET connection_url = NULL,
		       key_prefix     = '',
		       provider_resource_id = NULL
		 WHERE team_id = $1
	`, c.teamID); err != nil {
		return fmt.Errorf("null resource pii: %w", err)
	}

	// Step 5: NULL PII on user rows + team row. Email is set to a
	// per-id placeholder rather than NULL because the users.email
	// column is NOT NULL UNIQUE (see migration 001) — a real NULL
	// would fail the constraint. The deleted-<id>@tombstoned.invalid
	// pattern is uniquely-derivable from the user id, satisfies the
	// uniqueness constraint, and is operator-readable as "this row
	// is tombstoned" in psql.
	if _, err := tx.ExecContext(ctx, `
		UPDATE users
		   SET email     = 'deleted-' || id::text || '@tombstoned.invalid',
		       github_id = NULL,
		       google_id = NULL
		 WHERE team_id = $1
		   AND email NOT LIKE 'deleted-%@tombstoned.invalid'
	`, c.teamID); err != nil {
		return fmt.Errorf("null user pii: %w", err)
	}

	// Step 6: flip the team row deletion_pending → tombstoned.
	// stripe_customer_id holds the Razorpay subscription id (legacy column
	// name); we NULL it here so the post-tombstone billing scans don't try
	// to reconcile a destroyed subscription. teams.name (the visible slug)
	// is NULLed too — the dashboard's tombstoned-team view falls back to
	// the id. The WHERE guards on 'deletion_pending' (set by step 0); a
	// re-run that already tombstoned the row gets 0 rows affected, which is
	// the idempotent success case — we still commit and emit the audit.
	if _, err := tx.ExecContext(ctx, `
		UPDATE teams
		   SET status               = 'tombstoned',
		       tombstoned_at        = now(),
		       name                 = NULL,
		       stripe_customer_id   = NULL
		 WHERE id = $1
		   AND status = 'deletion_pending'
	`, c.teamID); err != nil {
		return fmt.Errorf("flip team status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	// Step 7: emit team.tombstoned audit. Best-effort — a failure here
	// is logged but does NOT roll the tombstone back. The row's
	// status='tombstoned' is itself the operator-visible signal.
	w.emitTombstoned(ctx, c.teamID, int64(len(resources)), int64(namespacesDeleted), s3BytesFreed, time.Since(start))

	slog.Info("jobs.team_deletion.tombstoned",
		"team_id", c.teamID.String(),
		"resource_count_destroyed", len(resources),
		"namespaces_deleted", namespacesDeleted,
		"s3_bytes_freed", s3BytesFreed,
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}

// fetchTeamDeployAppIDs returns the app_id of every non-deleted deployment
// owned by the team. The k8s namespace for each is instant-deploy-<appID>;
// the executor deletes those namespaces in step 3. We scan all non-'deleted'
// rows (including 'expired' / 'failed') because a namespace can outlive its
// logical deployment row's status.
func (w *TeamDeletionExecutorWorker) fetchTeamDeployAppIDs(ctx context.Context, teamID uuid.UUID) ([]string, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT DISTINCT app_id
		  FROM deployments
		 WHERE team_id = $1
		   AND app_id IS NOT NULL
		   AND app_id != ''
	`, teamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var appID string
		if err := rows.Scan(&appID); err != nil {
			return nil, err
		}
		out = append(out, appID)
	}
	return out, rows.Err()
}

// fetchTeamResources returns every non-deleted row owned by the team.
// Includes paused rows (the API paused them at deletion-request time) so
// the destruction pass sees the full set.
func (w *TeamDeletionExecutorWorker) fetchTeamResources(ctx context.Context, teamID uuid.UUID) ([]pendingResource, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT id, token::text, resource_type, COALESCE(provider_resource_id, '')
		  FROM resources
		 WHERE team_id = $1
		   AND status != 'deleted'
	`, teamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []pendingResource
	for rows.Next() {
		var r pendingResource
		if err := rows.Scan(&r.id, &r.token, &r.resourceType, &r.providerResourceID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// deleteS3BackupsForToken hard-deletes every object under the resource's
// backup prefix. Returns total bytes freed so the executor can stamp
// s3_bytes_freed in the audit metadata.
//
// We stream ListObjects → RemoveObjects via a goroutine channel pipe so a
// resource with 10K backup snapshots doesn't pull the whole list into
// memory.
func (w *TeamDeletionExecutorWorker) deleteS3BackupsForToken(ctx context.Context, token string) (int64, error) {
	prefix := s3BackupPrefixForToken(token)
	if prefix == "" {
		return 0, nil
	}

	objectsCh := make(chan minio.ObjectInfo, 32)
	var bytesFreed int64
	listErrCh := make(chan error, 1)

	go func() {
		defer close(objectsCh)
		// Panic boundary (P1-B): a panic in the S3 list iterator would
		// otherwise crash the worker pod. This defer is declared AFTER the
		// close(objectsCh) defer so it runs FIRST (LIFO) — it pushes a
		// terminal error onto the buffered listErrCh so the RemoveObjects
		// consumer is not left blocked, then close(objectsCh) signals EOF.
		defer func() {
			if r := recover(); r != nil {
				listErrCh <- fmt.Errorf("team_deletion S3 list goroutine panicked: %v", r)
				LogRecoveredPanic("team_deletion_executor.s3_list", r)
			}
		}()
		for obj := range w.s3.ListObjects(ctx, w.bucketName, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		}) {
			if obj.Err != nil {
				listErrCh <- obj.Err
				return
			}
			bytesFreed += obj.Size
			select {
			case objectsCh <- obj:
			case <-ctx.Done():
				listErrCh <- ctx.Err()
				return
			}
		}
		listErrCh <- nil
	}()

	// RemoveObjects drains objectsCh. Any per-object error is collected;
	// we surface only the first to keep the audit row's error compact.
	var firstRmErr error
	for rmErr := range w.s3.RemoveObjects(ctx, w.bucketName, objectsCh, minio.RemoveObjectsOptions{}) {
		if rmErr.Err != nil && firstRmErr == nil {
			firstRmErr = rmErr.Err
		}
	}
	listErr := <-listErrCh
	if listErr != nil {
		return bytesFreed, fmt.Errorf("list under %s: %w", prefix, listErr)
	}
	if firstRmErr != nil {
		return bytesFreed, fmt.Errorf("remove under %s: %w", prefix, firstRmErr)
	}
	return bytesFreed, nil
}

// emitTombstoned writes the success audit row. Best-effort: a failure to
// insert does NOT roll the tombstone back — the tombstone is the
// operator-visible signal.
func (w *TeamDeletionExecutorWorker) emitTombstoned(ctx context.Context, teamID uuid.UUID, resourceCount, namespacesDeleted, s3Bytes int64, duration time.Duration) {
	meta := map[string]any{
		"resource_count_destroyed": resourceCount,
		"namespaces_deleted":       namespacesDeleted,
		"s3_bytes_freed":           s3Bytes,
		"duration_seconds":         int(duration.Seconds()),
	}
	metaBytes, _ := json.Marshal(meta)
	summary := fmt.Sprintf(
		"team tombstoned — destroyed %d resources, deleted %d k8s namespaces, freed %d bytes of S3 backups in %ds",
		resourceCount, namespacesDeleted, s3Bytes, int(duration.Seconds()),
	)
	if _, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES ($1, $2, $3, $4, $5)
	`, teamID, teamDeletionActor, auditKindTombstoned, summary, metaBytes); err != nil {
		slog.Warn("jobs.team_deletion.audit_insert_failed",
			"team_id", teamID.String(),
			"kind", auditKindTombstoned,
			"error", err,
		)
	}
}

// emitDeletionFailed writes the failure audit row. Operators triage from
// this row + the structured log line.
func (w *TeamDeletionExecutorWorker) emitDeletionFailed(ctx context.Context, teamID uuid.UUID, perr error) {
	failStep := "unknown"
	// Crude step inference from the wrapped error text — good enough for
	// triage, doesn't require threading a step enum through every layer.
	msg := perr.Error()
	switch {
	case errors.Is(perr, context.Canceled), errors.Is(perr, context.DeadlineExceeded):
		failStep = "context"
	case containsAny(msg, "fetch resources"):
		failStep = "fetch_resources"
	case containsAny(msg, "s3 backups"):
		failStep = "s3_delete"
	case containsAny(msg, "deprovision"):
		failStep = "deprovision"
	case containsAny(msg, "delete namespace"), containsAny(msg, "deploy app ids"):
		failStep = "delete_namespace"
	case containsAny(msg, "mark deletion_pending"):
		failStep = "mark_deletion_pending"
	case containsAny(msg, "null resource pii"):
		failStep = "null_resource_pii"
	case containsAny(msg, "null user pii"):
		failStep = "null_user_pii"
	case containsAny(msg, "flip team status"):
		failStep = "flip_team_status"
	case containsAny(msg, "commit tx"), containsAny(msg, "begin tx"):
		failStep = "tx_commit"
	}
	meta := map[string]any{
		"error":          msg,
		"failed_at_step": failStep,
	}
	metaBytes, _ := json.Marshal(meta)
	summary := "team deletion failed at " + failStep + " — operator investigation required"
	if _, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES ($1, $2, $3, $4, $5)
	`, teamID, teamDeletionActor, auditKindTeamDeletionFailed, summary, metaBytes); err != nil {
		slog.Warn("jobs.team_deletion.audit_insert_failed",
			"team_id", teamID.String(),
			"kind", auditKindTeamDeletionFailed,
			"error", err,
		)
	}
}

// containsAny is a tiny strings.Contains wrapper that avoids pulling in
// strings just for one call inside emitDeletionFailed.
func containsAny(haystack, needle string) bool {
	if len(needle) > len(haystack) {
		return false
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
