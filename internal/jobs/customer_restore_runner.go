// customer_restore_runner.go — every 30s, claim up to 5 pending rows from
// resource_restores and run `pg_restore --clean --if-exists --no-owner --no-acl`
// streaming the gzip'd dump from S3 back into the SAME resource the backup
// came from.
//
// Why restore-into-same-resource only: backup objects in S3 are immutable;
// the schema/data they encode is keyed to the resource_id at backup time. A
// cross-resource restore (e.g. restore prod backup into staging resource)
// would need a separate flow that re-bakes the dump for the target's
// role/schema topology — out of scope for the wedge. The api side enforces
// resource_id == backup.resource_id at request time.
//
// Customer-data overwrite: --clean --if-exists drops + recreates every
// table from the dump. The runner does NOT take a "safety" snapshot first;
// the customer is opting into "rewind to this backup" and snapshotting
// before every rewind would silently chain backups forever. If a customer
// wants a pre-restore snapshot they can call POST /resources/:id/backup
// before the POST /resources/:id/restore.
//
// Lower batch (5 vs the runner's 20) because pg_restore is heavier than
// pg_dump — it holds locks against the live DB, so concurrent restores
// against the same customer-postgres pod queue up. 5 is enough to keep
// the queue moving without saturating the shared customer-postgres
// connection pool.
package jobs

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	"instant.dev/common/crypto"
)

type CustomerRestoreRunnerArgs struct{}

func (CustomerRestoreRunnerArgs) Kind() string { return "customer_restore_runner" }

const (
	restoreBatchSize     = 5
	restorePerRunTimeout = 30 * time.Minute

	// restoreReasonIntegrityFailed is the error_summary written when the
	// SHA-256 of the downloaded S3 object does not match the digest the
	// backup runner stored in resource_backups.sha256. It mirrors the
	// `backup_integrity_failed` reason documented by the api side
	// (api/internal/handlers/backup.go) and migration 043_backup_sha256.sql.
	// On this reason the runner DOES NOT invoke pg_restore — a bit-rotted
	// or truncated archive must never be applied with --clean --if-exists
	// over live customer data.
	restoreReasonIntegrityFailed = "backup_integrity_failed"
)

// pgRestoreRunner mirrors pgDumpRunner — abstraction seam for tests.
type pgRestoreRunner interface {
	// Run starts pg_restore against connURL, reading the
	// `pg_dump --format=custom` archive from r (already gunzipped by the
	// caller). Returns when the subprocess exits or ctx is cancelled.
	Run(ctx context.Context, connURL string, r io.Reader) error
}

type realPgRestoreRunner struct{}

func (realPgRestoreRunner) Run(ctx context.Context, connURL string, r io.Reader) error {
	cmd := exec.CommandContext(ctx, "pg_restore",
		"--no-owner", "--no-acl",
		"--clean", "--if-exists",
		"-d", connURL,
	)
	cmd.Stdin = r
	var stderrBuf limitedBuffer
	cmd.Stderr = &stderrBuf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("pg_restore: %w (stderr: %s)", err, stderrBuf.String())
	}
	return nil
}

type CustomerRestoreRunnerWorker struct {
	river.WorkerDefaults[CustomerRestoreRunnerArgs]
	db        *sql.DB
	store     BackupObjectStore
	pgRestore pgRestoreRunner
	bucket    string
	aesKey    string
	now       func() time.Time
	timeout   time.Duration
	batchN    int
}

func NewCustomerRestoreRunner(db *sql.DB, store BackupObjectStore, bucket, aesKey string) *CustomerRestoreRunnerWorker {
	return &CustomerRestoreRunnerWorker{
		db:        db,
		store:     store,
		pgRestore: realPgRestoreRunner{},
		bucket:    bucket,
		aesKey:    aesKey,
		now:       time.Now,
		timeout:   restorePerRunTimeout,
		batchN:    restoreBatchSize,
	}
}

func (w *CustomerRestoreRunnerWorker) Work(ctx context.Context, job *river.Job[CustomerRestoreRunnerArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.customer_restore_runner")
	defer span.End()

	if w.store == nil || w.aesKey == "" {
		slog.Warn("jobs.customer_restore_runner.skipped",
			"reason", "object store or AES key unconfigured",
			"store_set", w.store != nil,
			"aes_set", w.aesKey != "",
		)
		return nil
	}

	// D26-F5 (2026-05-21): stuck-row recovery — mirrors the equivalent
	// pass in customer_backup_runner.go (P2-W4). A restore row is
	// atomically claimed by flipping status 'pending' → 'running'. If the
	// worker pod is killed mid-pg_restore (rolling deploy, OOM, node
	// drain) AFTER the claim but BEFORE finalize/markRestoreFailed, the
	// row is orphaned at 'running' forever — the pending-row sweep below
	// only selects status='pending', so no runner ever picks it up again.
	// Customer support's "why is my restore stuck for 6h" ticket is the
	// user-visible symptom. Recovery: any 'running' row whose started_at
	// is older than restorePerRunTimeout could not still be a live
	// in-flight restore (the per-run context would have fired), so it is
	// marked 'failed' with an explicit error_summary. We deliberately do
	// NOT re-queue these (unlike the backup runner) — restore is a
	// destructive operation gated on user intent, and a silent retry of
	// a half-completed pg_restore could compound database corruption.
	// The user can re-issue the POST /resources/:id/restore manually.
	w.recoverStuckRestores(ctx)

	rows, err := w.db.QueryContext(ctx, `
		SELECT rr.id::text, rr.resource_id::text, rr.backup_id::text,
		       rb.s3_key, rb.sha256,
		       r.connection_url, r.resource_type, r.token::text, r.team_id
		FROM resource_restores rr
		JOIN resource_backups rb ON rb.id = rr.backup_id
		JOIN resources r ON r.id = rr.resource_id
		WHERE rr.status = 'pending'
		ORDER BY rr.created_at
		LIMIT $1
	`, w.batchN)
	if err != nil {
		return fmt.Errorf("CustomerRestoreRunnerWorker: select pending failed: %w", err)
	}
	defer rows.Close()

	type pending struct {
		restoreID    string
		resourceID   string
		backupID     string
		s3Key        sql.NullString
		sha256       sql.NullString
		connURL      sql.NullString
		resourceType string
		token        string
		teamID       uuid.NullUUID
	}
	var batch []pending
	for rows.Next() {
		var p pending
		if scanErr := rows.Scan(
			&p.restoreID, &p.resourceID, &p.backupID,
			&p.s3Key, &p.sha256, &p.connURL, &p.resourceType, &p.token, &p.teamID,
		); scanErr != nil {
			slog.Warn("jobs.customer_restore_runner.scan_failed", "error", scanErr)
			continue
		}
		batch = append(batch, p)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("CustomerRestoreRunnerWorker: rows error: %w", err)
	}
	rows.Close()

	processed := 0
	succeeded := 0
	for _, p := range batch {
		select {
		case <-ctx.Done():
			slog.Info("jobs.customer_restore_runner.ctx_cancelled_mid_batch",
				"processed", processed, "remaining", len(batch)-processed)
			return nil
		default:
		}

		if w.processRestore(ctx, p) {
			succeeded++
		}
		processed++
	}

	// T21 P1-1 (BugBash 2026-05-20): idle-tick demoted INFO→DEBUG.
	// Steady state is processed=0; surface INFO only when work happened
	// or a failure surfaced (otherwise this fires every River batch).
	failedCount := processed - succeeded
	if processed == 0 && failedCount == 0 {
		slog.Debug("jobs.customer_restore_runner.completed",
			"processed", processed,
			"succeeded", succeeded,
			"failed", failedCount,
			"job_id", job.ID,
		)
	} else {
		slog.Info("jobs.customer_restore_runner.completed",
			"processed", processed,
			"succeeded", succeeded,
			"failed", failedCount,
			"job_id", job.ID,
		)
	}
	return nil
}

// recoverStuckRestores marks restore rows orphaned at status='running' as
// 'failed' with a worker_killed_during_restore reason. A row qualifies only
// when started_at is older than restorePerRunTimeout — a genuinely
// in-flight restore is bounded by that per-run context, so anything older
// is a casualty of a pod kill, not a live job. Best-effort: a failure here
// is logged and the sweep proceeds.
//
// Unlike customer_backup_runner.recoverStuckRows (which re-queues to
// 'pending'), this path TERMINATES the row at 'failed'. pg_restore is
// destructive — a half-completed restore that re-queues for retry could
// compound the partial-state damage. The customer can manually re-issue
// POST /resources/:id/restore after inspecting the resource state.
func (w *CustomerRestoreRunnerWorker) recoverStuckRestores(ctx context.Context) {
	res, err := w.db.ExecContext(ctx, `
		UPDATE resource_restores
		   SET status        = 'failed',
		       finished_at   = now(),
		       error_summary = 'worker_killed_during_restore: runner pod lost before finalize — pg_restore may have partially applied; re-issue restore after inspecting resource state'
		 WHERE status = 'running'
		   AND started_at IS NOT NULL
		   AND started_at < now() - ($1::int * INTERVAL '1 second')
	`, int(w.timeout.Seconds()))
	if err != nil {
		slog.Warn("jobs.customer_restore_runner.stuck_row_recovery_failed", "error", err)
		return
	}
	if n, raErr := res.RowsAffected(); raErr == nil && n > 0 {
		slog.Warn("jobs.customer_restore_runner.recovered_stuck_rows",
			"count", n,
			"note", "rows orphaned at status='running' past the per-run timeout — marked failed (NOT re-queued; pg_restore is destructive)",
		)
	}
}

// processRestore runs a single restore row. Returns true on success.
func (w *CustomerRestoreRunnerWorker) processRestore(parentCtx context.Context, p struct {
	restoreID    string
	resourceID   string
	backupID     string
	s3Key        sql.NullString
	sha256       sql.NullString
	connURL      sql.NullString
	resourceType string
	token        string
	teamID       uuid.NullUUID
}) bool {
	start := w.now()
	ctx, cancel := context.WithTimeout(parentCtx, w.timeout)
	defer cancel()

	// Atomic claim.
	var claimed string
	claimErr := w.db.QueryRowContext(ctx, `
		UPDATE resource_restores
		   SET status = 'running', started_at = now()
		 WHERE id = $1 AND status = 'pending'
		 RETURNING id
	`, p.restoreID).Scan(&claimed)
	if errors.Is(claimErr, sql.ErrNoRows) {
		return false
	}
	if claimErr != nil {
		slog.Error("jobs.customer_restore_runner.claim_failed",
			"restore_id", p.restoreID, "error", claimErr)
		return false
	}

	if p.teamID.Valid {
		w.writeAudit(ctx, p.teamID.UUID, p.resourceID, p.resourceType, auditKindRestoreStarted,
			"Restore started", map[string]any{
				"restore_id": p.restoreID,
				"backup_id":  p.backupID,
			})
	}

	// Validate backup is still present (retention sweep may have nulled
	// s3_key out from under the api's check, in the race between the
	// /restore POST and the runner picking it up).
	if !p.s3Key.Valid || p.s3Key.String == "" {
		w.markRestoreFailed(ctx, p.restoreID, "backup s3_key is null (retention may have purged it)", start, p)
		return false
	}

	// Decrypt connection_url — same pattern as the backup runner.
	if !p.connURL.Valid || p.connURL.String == "" {
		w.markRestoreFailed(ctx, p.restoreID, "resource.connection_url is empty", start, p)
		return false
	}
	aesKey, keyErr := crypto.ParseAESKey(w.aesKey)
	if keyErr != nil {
		w.markRestoreFailed(ctx, p.restoreID, fmt.Sprintf("AES key invalid: %v", keyErr), start, p)
		return false
	}
	plainConn, decErr := crypto.Decrypt(aesKey, p.connURL.String)
	if decErr != nil {
		w.markRestoreFailed(ctx, p.restoreID, fmt.Sprintf("decrypt connection_url: %v", decErr), start, p)
		return false
	}

	// Download from S3 → temp file via streaming TeeReader. We MUST verify
	// the SHA-256 BEFORE invoking pg_restore (its `--clean --if-exists`
	// would otherwise DROP every table from an unverified — potentially
	// truncated / bit-rotted — archive before the mismatch was detected).
	//
	// D26-F4 (2026-05-21): the previous implementation `io.ReadAll`-ed the
	// whole object into RAM to satisfy the "hash before restore" ordering.
	// A multi-GB pro/team backup OOM-killed the worker pod. Fix: tee the
	// S3 read stream into BOTH a sha256.New() hasher AND a bounded temp
	// file. Memory footprint stays at the io.Copy buffer (~32 KiB) while
	// the digest is computed inline. The temp file is rewound to offset 0
	// for the gunzip + pg_restore step once the SHA gate has passed.
	//
	// Temp file lives under os.TempDir() (the pod's emptyDir / tmpfs);
	// caller MUST `defer os.Remove` to avoid leaks across worker restarts.
	obj, dlErr := w.store.Download(ctx, w.bucket, p.s3Key.String)
	if dlErr != nil {
		w.markRestoreFailed(ctx, p.restoreID, fmt.Sprintf("S3 download failed: %v", dlErr), start, p)
		return false
	}

	tmpFile, tmpErr := os.CreateTemp("", "instant-restore-*.dump.gz")
	if tmpErr != nil {
		obj.Close()
		w.markRestoreFailed(ctx, p.restoreID, fmt.Sprintf("create temp file: %v", tmpErr), start, p)
		return false
	}
	// Always clean up — the temp file is destructively rewritten on every
	// restore, and a pod restart would otherwise leak the file on /tmp.
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	hasher := sha256.New()
	tee := io.TeeReader(obj, hasher)
	if _, copyErr := io.Copy(tmpFile, tee); copyErr != nil {
		obj.Close()
		w.markRestoreFailed(ctx, p.restoreID, fmt.Sprintf("S3 read failed: %v", copyErr), start, p)
		return false
	}
	obj.Close()

	// Integrity gate (same semantics as before — only the hashing path
	// changed). The backup runner (customer_backup_runner.go, FIX-H #59)
	// hashes the COMPRESSED (gzipped) object — its SHA-256 hasher sits in
	// an io.MultiWriter fed by the gzip writer's output, i.e. the exact
	// bytes uploaded to S3. We hash the same gzipped stream here (BEFORE
	// gunzip) to compare against the stored digest.
	//
	// Fail-open on a NULL/empty stored digest: rows predating migration
	// 043_backup_sha256.sql have no sha256, and the documented contract is
	// to log a warning and proceed rather than block restores of legacy
	// backups.
	if storedDigest := strings.TrimSpace(p.sha256.String); !p.sha256.Valid || storedDigest == "" {
		slog.Warn("jobs.customer_restore_runner.integrity_check_skipped",
			"reason", "stored sha256 is null/empty (legacy backup predating migration 043)",
			"restore_id", p.restoreID,
			"backup_id", p.backupID,
		)
	} else {
		actualDigest := hex.EncodeToString(hasher.Sum(nil))
		if !strings.EqualFold(actualDigest, storedDigest) {
			w.markRestoreFailed(ctx, p.restoreID, fmt.Sprintf(
				"%s: sha256 mismatch (stored %s, downloaded %s) — backup object is corrupt or truncated; pg_restore NOT run",
				restoreReasonIntegrityFailed, storedDigest, actualDigest), start, p)
			return false
		}
	}

	// Verified (or fail-open legacy) → rewind temp file, gunzip, and
	// stream into pg_restore. The gunzip reader holds at most one block
	// of decompressed data at a time, so the pg_restore stdin pipe never
	// buffers the whole archive in memory.
	if _, seekErr := tmpFile.Seek(0, io.SeekStart); seekErr != nil {
		w.markRestoreFailed(ctx, p.restoreID, fmt.Sprintf("rewind temp file: %v", seekErr), start, p)
		return false
	}
	gzReader, gzErr := gzip.NewReader(tmpFile)
	if gzErr != nil {
		w.markRestoreFailed(ctx, p.restoreID, fmt.Sprintf("gunzip header: %v", gzErr), start, p)
		return false
	}
	defer gzReader.Close()

	if runErr := w.pgRestore.Run(ctx, plainConn, gzReader); runErr != nil {
		w.markRestoreFailed(ctx, p.restoreID, fmt.Sprintf("pg_restore failed: %v", runErr), start, p)
		return false
	}

	// Finalize.
	if _, updErr := w.db.ExecContext(parentCtx, `
		UPDATE resource_restores
		   SET status = 'ok', finished_at = now()
		 WHERE id = $1
	`, p.restoreID); updErr != nil {
		slog.Error("jobs.customer_restore_runner.finalize_failed",
			"restore_id", p.restoreID, "error", updErr)
		return false
	}

	duration := time.Since(start)
	if p.teamID.Valid {
		w.writeAudit(parentCtx, p.teamID.UUID, p.resourceID, p.resourceType,
			auditKindRestoreSucceeded, "Restore succeeded", map[string]any{
				"restore_id":       p.restoreID,
				"backup_id":        p.backupID,
				"duration_seconds": int(duration.Seconds()),
			})
	}

	slog.Info("jobs.customer_restore_runner.succeeded",
		"restore_id", p.restoreID,
		"resource_id", p.resourceID,
		"backup_id", p.backupID,
		"duration_ms", duration.Milliseconds(),
	)
	return true
}

func (w *CustomerRestoreRunnerWorker) markRestoreFailed(
	ctx context.Context, restoreID, errSummary string, start time.Time,
	p struct {
		restoreID    string
		resourceID   string
		backupID     string
		s3Key        sql.NullString
		sha256       sql.NullString
		connURL      sql.NullString
		resourceType string
		token        string
		teamID       uuid.NullUUID
	},
) {
	dbCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := w.db.ExecContext(dbCtx, `
		UPDATE resource_restores
		   SET status = 'failed', finished_at = now(), error_summary = $2
		 WHERE id = $1
	`, restoreID, errSummary); err != nil {
		slog.Error("jobs.customer_restore_runner.mark_failed_db_error",
			"restore_id", restoreID, "error", err)
	}

	duration := time.Since(start)
	if p.teamID.Valid {
		w.writeAudit(dbCtx, p.teamID.UUID, p.resourceID, p.resourceType,
			auditKindRestoreFailed, "Restore failed", map[string]any{
				"restore_id":       restoreID,
				"backup_id":        p.backupID,
				"error_summary":    errSummary,
				"duration_seconds": int(duration.Seconds()),
			})
	}

	slog.Error("jobs.customer_restore_runner.failed",
		"restore_id", restoreID,
		"error_summary", errSummary,
		"duration_ms", duration.Milliseconds(),
	)
	_ = ctx
}

func (w *CustomerRestoreRunnerWorker) writeAudit(
	ctx context.Context, teamID uuid.UUID, resourceID, resourceType, kind, summary string,
	meta map[string]any,
) {
	metaBytes, mErr := json.Marshal(meta)
	if mErr != nil {
		slog.Error("jobs.customer_restore_runner.audit_marshal_failed", "kind", kind, "error", mErr)
		return
	}
	rid, _ := uuid.Parse(resourceID)
	if _, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata, resource_type, resource_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, teamID, backupActor, kind, summary, metaBytes, resourceType, rid); err != nil {
		slog.Warn("jobs.customer_restore_runner.audit_insert_failed",
			"kind", kind, "team_id", teamID, "error", err)
	}
}
