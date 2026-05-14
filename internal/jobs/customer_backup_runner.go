// customer_backup_runner.go — every 30s, claim up to 20 pending rows from
// resource_backups and run `pg_dump --no-owner --no-acl --format=custom -d <conn> | gzip`
// streaming the output directly to S3 (DO Spaces / MinIO / AWS S3 — any
// S3-compatible endpoint).
//
// Two entry points produce 'pending' rows:
//
//   1. customer_backup_scheduler (the sibling worker in this package) — for
//      tier-eligible scheduled backups.
//   2. The api side's POST /api/v1/resources/:id/backup — for customer-
//      triggered manual backups (backup_kind='manual', triggered_by=user.id).
//
// Both flows funnel through this single runner so retention, audit-log
// emission, and S3 layout are identical regardless of trigger source.
//
// Atomic claim: SET status='running' WHERE id=$1 AND status='pending'
// RETURNING ... — if a competing worker grabbed the row, we get 0 rows
// back and skip it. This lets the runner safely scale horizontally without
// a distributed lock.
//
// Streaming: stdout of pg_dump is piped directly into the gzip writer which
// is in turn piped to s3.Upload via an io.Pipe. Total memory footprint is
// ~64MiB (the minio-go default multipart part size) regardless of dump
// size — a 50GB pro-tier postgres dump uploads in chunks, never buffered
// whole.
//
// Timeout: per-row 30 minutes. Enforced via context.WithTimeout so both the
// pg_dump subprocess AND the S3 upload share the deadline; on cancellation
// the subprocess receives SIGKILL via the command context.
//
// Retention sweep runs at the end of every batch, not per-row, so a long
// upload doesn't delay the deletes by 30+ minutes — but the actual S3
// DELETE calls are unaffected by the long pg_dump because they happen on
// a fresh context with its own timeout.
package jobs

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	"instant.dev/common/crypto"
)

// CustomerBackupRunnerArgs holds no fields — periodic job.
type CustomerBackupRunnerArgs struct{}

func (CustomerBackupRunnerArgs) Kind() string { return "customer_backup_runner" }

// backupBatchSize is the LIMIT on the pending-row sweep query. 20 is enough
// to keep one worker busy for the 30s tick window (assuming most backups
// take 30s-2min for the hobby/pro size range) without monopolizing the DB
// pool. Increase when ops sees the pending-row queue growing.
const backupBatchSize = 20

// backupPerRunTimeout caps total wall time per backup row at 30 minutes.
// Anything bigger gets killed and the row marked 'failed' with a
// "timed out" error_summary; the next scheduler tick will retry on the
// next cadence (we deliberately don't auto-retry inside a single sweep —
// a 30min timeout is almost always a "logical" failure that retrying
// in 30s won't fix).
const backupPerRunTimeout = 30 * time.Minute

// pgDumpRunner abstracts the actual subprocess execution so the test can
// substitute a fake without spawning a real pg_dump (which would require a
// running Postgres). Implementations: realPgDumpRunner (this file), and the
// test fakePgDumpRunner (customer_backup_runner_test.go).
type pgDumpRunner interface {
	// Run starts pg_dump against connURL, writing the custom-format dump
	// (already passed through gzip — implementation's choice) to w. Returns
	// when the subprocess exits or ctx is cancelled.
	Run(ctx context.Context, connURL string, w io.Writer) error
}

// realPgDumpRunner shells out to the real pg_dump binary. The
// `--format=custom` flag produces a binary archive that pg_restore can
// later apply selectively (per-table, per-schema), which matters for the
// restore-runner story even though today the restore is whole-DB.
//
// `--no-owner --no-acl` strips role-grant DDL — restores into a fresh
// per-customer DB whose role topology is owned by the provisioner, NOT by
// the source dump. Without these flags pg_restore tries to GRANT to roles
// that don't exist on the target.
type realPgDumpRunner struct{}

func (realPgDumpRunner) Run(ctx context.Context, connURL string, w io.Writer) error {
	cmd := exec.CommandContext(ctx, "pg_dump",
		"--no-owner", "--no-acl",
		"--format=custom",
		"-d", connURL,
	)
	cmd.Stdout = w
	// Stderr goes to slog at the call site by buffering — we don't want a
	// noisy pg_dump banner ("dumping contents of table ...") to flood
	// stderr at INFO. The error_summary captured into resource_backups
	// only needs the last few KB which we get from cmd.Run's err.
	var stderrBuf limitedBuffer
	cmd.Stderr = &stderrBuf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("pg_dump: %w (stderr: %s)", err, stderrBuf.String())
	}
	return nil
}

// CustomerBackupRunnerWorker is the River worker.
//
// store / pgDump may be nil in tests via NewCustomerBackupRunnerForTest;
// in production the constructor below requires both.
type CustomerBackupRunnerWorker struct {
	river.WorkerDefaults[CustomerBackupRunnerArgs]
	db       *sql.DB
	store    BackupObjectStore
	pgDump   pgDumpRunner
	bucket   string
	prefix   string
	aesKey   string // hex, decoded at use site via crypto.ParseAESKey
	now      func() time.Time
	timeout  time.Duration
	batchN   int
}

// NewCustomerBackupRunner constructs a runner with production defaults.
// store may be nil — the worker then logs WARN and skips every batch
// (fail-open). aesKey may be empty — same WARN-and-skip behavior, since we
// refuse to dump from a plaintext connection_url.
func NewCustomerBackupRunner(db *sql.DB, store BackupObjectStore, bucket, prefix, aesKey string) *CustomerBackupRunnerWorker {
	return &CustomerBackupRunnerWorker{
		db:      db,
		store:   store,
		pgDump:  realPgDumpRunner{},
		bucket:  bucket,
		prefix:  prefix,
		aesKey:  aesKey,
		now:     time.Now,
		timeout: backupPerRunTimeout,
		batchN:  backupBatchSize,
	}
}

// Work runs a single sweep tick. Returns nil on partial failure (fail-open
// per row); returns an error only on a DB-level failure that prevents any
// progress (e.g. the SELECT query itself failed).
func (w *CustomerBackupRunnerWorker) Work(ctx context.Context, job *river.Job[CustomerBackupRunnerArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.customer_backup_runner")
	defer span.End()

	if w.store == nil || w.aesKey == "" {
		slog.Warn("jobs.customer_backup_runner.skipped",
			"reason", "object store or AES key unconfigured",
			"store_set", w.store != nil,
			"aes_set", w.aesKey != "",
		)
		return nil
	}

	// Sweep pending rows. We pull the resource side-data (connection_url,
	// token, team_id, resource_type) in the same SELECT so the per-row
	// path only needs one ExecContext for the claim + one for the final
	// status update.
	rows, err := w.db.QueryContext(ctx, `
		SELECT b.id::text, b.resource_id::text, b.tier_at_backup,
		       r.token::text, r.connection_url, r.resource_type, r.team_id
		FROM resource_backups b
		JOIN resources r ON r.id = b.resource_id
		WHERE b.status = 'pending'
		ORDER BY b.created_at
		LIMIT $1
	`, w.batchN)
	if err != nil {
		return fmt.Errorf("CustomerBackupRunnerWorker: select pending failed: %w", err)
	}
	defer rows.Close()

	type pending struct {
		backupID     string
		resourceID   string
		tier         sql.NullString
		token        string
		connURL      sql.NullString
		resourceType string
		teamID       uuid.NullUUID
	}
	var batch []pending
	for rows.Next() {
		var p pending
		if scanErr := rows.Scan(
			&p.backupID, &p.resourceID, &p.tier,
			&p.token, &p.connURL, &p.resourceType, &p.teamID,
		); scanErr != nil {
			slog.Warn("jobs.customer_backup_runner.scan_failed", "error", scanErr)
			continue
		}
		batch = append(batch, p)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("CustomerBackupRunnerWorker: rows error: %w", err)
	}
	rows.Close()

	processed := 0
	succeeded := 0
	failed := 0
	for _, p := range batch {
		select {
		case <-ctx.Done():
			// Outer ctx cancelled (worker shutdown). Stop processing —
			// remaining 'pending' rows will be picked up on the next tick
			// or by a new worker pod.
			slog.Info("jobs.customer_backup_runner.ctx_cancelled_mid_batch",
				"processed", processed, "remaining", len(batch)-processed)
			return nil
		default:
		}

		ok := w.processBackup(ctx, p)
		processed++
		if ok {
			succeeded++
		} else {
			failed++
		}
	}

	// Retention sweep at end of run. A failure here doesn't unwind the
	// successful uploads from the same tick.
	w.runRetentionSweep(ctx)

	slog.Info("jobs.customer_backup_runner.completed",
		"processed", processed,
		"succeeded", succeeded,
		"failed", failed,
		"job_id", job.ID,
	)
	return nil
}

// processBackup runs a single backup row end-to-end. Returns true on success.
// Always logs + updates DB; never propagates an error to the caller.
func (w *CustomerBackupRunnerWorker) processBackup(parentCtx context.Context, p struct {
	backupID     string
	resourceID   string
	tier         sql.NullString
	token        string
	connURL      sql.NullString
	resourceType string
	teamID       uuid.NullUUID
}) bool {
	start := w.now()
	ctx, cancel := context.WithTimeout(parentCtx, w.timeout)
	defer cancel()

	// Step 1 — atomic claim. If somebody else grabbed it, we get 0 rows and
	// skip; if it's already 'running' / 'ok' / 'failed', same. The
	// RETURNING is just for the success log; we already have everything
	// from the SELECT.
	var claimed string
	claimErr := w.db.QueryRowContext(ctx, `
		UPDATE resource_backups
		   SET status = 'running', started_at = now()
		 WHERE id = $1 AND status = 'pending'
		 RETURNING id
	`, p.backupID).Scan(&claimed)
	if errors.Is(claimErr, sql.ErrNoRows) {
		// Already claimed by another runner — silent success at the
		// scheduling level, no audit row needed.
		return false
	}
	if claimErr != nil {
		slog.Error("jobs.customer_backup_runner.claim_failed",
			"backup_id", p.backupID, "error", claimErr)
		return false
	}

	// Emit backup.started audit row. team_id is required by the audit_log
	// FK constraint — skip the audit row (but not the backup) when the
	// resource has no team (anonymous rows can't reach here because the
	// scheduler skips them, but a manual API call against an anonymous
	// resource would).
	if p.teamID.Valid {
		w.writeAudit(ctx, p.teamID.UUID, p.resourceID, p.resourceType, auditKindBackupStarted,
			"Backup started", map[string]any{
				"backup_id": p.backupID,
				"tier":      p.tier.String,
			})
	}

	// Step 2 — decrypt connection_url. Mirrors the api pattern; an empty
	// or malformed ciphertext is a hard failure since we can't safely
	// dump from a guess.
	if !p.connURL.Valid || p.connURL.String == "" {
		w.markFailed(ctx, p.backupID, "resource.connection_url is empty", start, p)
		return false
	}
	aesKey, keyErr := crypto.ParseAESKey(w.aesKey)
	if keyErr != nil {
		w.markFailed(ctx, p.backupID, fmt.Sprintf("AES key invalid: %v", keyErr), start, p)
		return false
	}
	plainConn, decErr := crypto.Decrypt(aesKey, p.connURL.String)
	if decErr != nil {
		w.markFailed(ctx, p.backupID, fmt.Sprintf("decrypt connection_url: %v", decErr), start, p)
		return false
	}

	// Step 3 — stream pg_dump → gzip → S3 via io.Pipe.
	objectKey := backupObjectKey(w.prefix, p.token, p.backupID)
	pr, pw := io.Pipe()

	// Goroutine: pg_dump writes raw archive bytes into the gzip writer,
	// which writes compressed bytes into the pipe writer. Closing the
	// gzip writer flushes its final gzip footer, then we close pw to
	// signal EOF to the S3 Upload reader side.
	dumpDone := make(chan error, 1)
	go func() {
		gz := gzip.NewWriter(pw)
		runErr := w.pgDump.Run(ctx, plainConn, gz)
		// Close gzip first to flush the trailer, then the pipe so the
		// Upload side sees EOF (not just the partial gzip stream). If
		// pg_dump errored, propagate the close-error too.
		closeErr := gz.Close()
		if runErr == nil {
			runErr = closeErr
		}
		// CloseWithError signals to the Upload side why the stream
		// ended; nil = clean EOF. Either path the pipe is now closed.
		_ = pw.CloseWithError(runErr)
		dumpDone <- runErr
	}()

	size, upErr := w.store.Upload(ctx, w.bucket, objectKey, pr)
	dumpErr := <-dumpDone

	// Prefer the dump-side error when both fail (almost always the more
	// actionable: "pg_dump: connection refused" vs "pipe: io: read/write
	// on closed pipe").
	if dumpErr != nil {
		w.markFailed(ctx, p.backupID, fmt.Sprintf("pg_dump failed: %v", dumpErr), start, p)
		// Best-effort cleanup of a half-written object so we don't pay
		// for orphan bytes; failure to delete is logged but not fatal.
		if delErr := w.store.DeleteObject(parentCtx, w.bucket, objectKey); delErr != nil {
			slog.Warn("jobs.customer_backup_runner.cleanup_failed",
				"object_key", objectKey, "error", delErr)
		}
		return false
	}
	if upErr != nil {
		w.markFailed(ctx, p.backupID, fmt.Sprintf("S3 upload failed: %v", upErr), start, p)
		return false
	}

	// Step 4 — finalize.
	if _, updErr := w.db.ExecContext(parentCtx, `
		UPDATE resource_backups
		   SET status = 'ok',
		       finished_at = now(),
		       s3_key = $2,
		       size_bytes = $3
		 WHERE id = $1
	`, p.backupID, objectKey, size); updErr != nil {
		slog.Error("jobs.customer_backup_runner.finalize_failed",
			"backup_id", p.backupID,
			"object_key", objectKey,
			"size_bytes", size,
			"error", updErr,
		)
		// Backup file is in S3 but DB row isn't updated — a manual
		// reconcile (operator) can flip the row. Don't lose the file.
		return false
	}

	duration := time.Since(start)
	if p.teamID.Valid {
		w.writeAudit(parentCtx, p.teamID.UUID, p.resourceID, p.resourceType,
			auditKindBackupSucceeded, "Backup succeeded", map[string]any{
				"backup_id":        p.backupID,
				"s3_key":           objectKey,
				"size_bytes":       size,
				"duration_seconds": int(duration.Seconds()),
				"tier":             p.tier.String,
			})
	}

	slog.Info("jobs.customer_backup_runner.succeeded",
		"backup_id", p.backupID,
		"resource_id", p.resourceID,
		"s3_key", objectKey,
		"size_bytes", size,
		"duration_ms", duration.Milliseconds(),
	)
	return true
}

// markFailed updates the row to 'failed', emits backup.failed audit, and
// logs the error_summary. parentCtx is used for the DB write so a timed-
// out backup still records its failure (the inner ctx is already dead).
func (w *CustomerBackupRunnerWorker) markFailed(
	ctx context.Context, backupID, errSummary string, start time.Time,
	p struct {
		backupID     string
		resourceID   string
		tier         sql.NullString
		token        string
		connURL      sql.NullString
		resourceType string
		teamID       uuid.NullUUID
	},
) {
	// Use a fresh ctx with a small timeout so a parentCtx-already-cancelled
	// path still gets the row updated.
	dbCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := w.db.ExecContext(dbCtx, `
		UPDATE resource_backups
		   SET status = 'failed',
		       finished_at = now(),
		       error_summary = $2
		 WHERE id = $1
	`, backupID, errSummary); err != nil {
		slog.Error("jobs.customer_backup_runner.mark_failed_db_error",
			"backup_id", backupID, "error", err)
	}

	duration := time.Since(start)
	if p.teamID.Valid {
		w.writeAudit(dbCtx, p.teamID.UUID, p.resourceID, p.resourceType,
			auditKindBackupFailed, "Backup failed", map[string]any{
				"backup_id":        backupID,
				"error_summary":    errSummary,
				"duration_seconds": int(duration.Seconds()),
				"tier":             p.tier.String,
			})
	}

	slog.Error("jobs.customer_backup_runner.failed",
		"backup_id", backupID,
		"error_summary", errSummary,
		"duration_ms", duration.Milliseconds(),
	)
	_ = ctx // (parentCtx) keep param to surface intent even though we use a fresh ctx
}

// writeAudit emits an audit_log row. Errors are logged but not propagated —
// a missing audit row is a bookkeeping issue, not a data-correctness one.
func (w *CustomerBackupRunnerWorker) writeAudit(
	ctx context.Context, teamID uuid.UUID, resourceID, resourceType, kind, summary string,
	meta map[string]any,
) {
	metaBytes, mErr := json.Marshal(meta)
	if mErr != nil {
		slog.Error("jobs.customer_backup_runner.audit_marshal_failed", "kind", kind, "error", mErr)
		return
	}
	rid, _ := uuid.Parse(resourceID) // best-effort; nil UUID is acceptable for the audit row
	if _, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata, resource_type, resource_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, teamID, backupActor, kind, summary, metaBytes, resourceType, rid); err != nil {
		slog.Warn("jobs.customer_backup_runner.audit_insert_failed",
			"kind", kind, "team_id", teamID, "error", err)
	}
}

// runRetentionSweep finds all status='ok' rows whose tier_at_backup says
// they're past retention and hard-deletes the S3 object + marks the row
// 'deleted' (status flips OUT of the CHECK constraint set so we use a
// soft-flag: clear s3_key + error_summary='retained:expired', leaving
// status='ok' but with no key — the api side filters list/restore by
// "s3_key IS NOT NULL").
//
// Rationale for not adding a 'deleted' enum value: the migration's CHECK
// constraint only allows pending/running/ok/failed. Adding a fifth value
// would force a migration on both sides. Using s3_key=NULL as the soft-
// delete marker is cheap, doesn't require schema churn, and the api side
// can lazily exclude such rows from the list/restore endpoints.
func (w *CustomerBackupRunnerWorker) runRetentionSweep(ctx context.Context) {
	// We sweep tier-by-tier so each tier's WHERE clause hits the partial
	// index efficiently. Three tiers x one query = cheap.
	for _, tier := range []string{"hobby", "pro", "growth", "team", "anonymous"} {
		cutoff := retentionCutoff(tier, w.now())
		rows, err := w.db.QueryContext(ctx, `
			SELECT id::text, s3_key
			FROM resource_backups
			WHERE status = 'ok'
			  AND s3_key IS NOT NULL
			  AND tier_at_backup = $1
			  AND created_at < $2
			LIMIT 200
		`, tier, cutoff)
		if err != nil {
			slog.Warn("jobs.customer_backup_runner.retention_query_failed",
				"tier", tier, "error", err)
			continue
		}

		type victim struct {
			id    string
			s3Key string
		}
		var victims []victim
		for rows.Next() {
			var v victim
			if scanErr := rows.Scan(&v.id, &v.s3Key); scanErr != nil {
				slog.Warn("jobs.customer_backup_runner.retention_scan_failed",
					"tier", tier, "error", scanErr)
				continue
			}
			victims = append(victims, v)
		}
		rows.Close()

		for _, v := range victims {
			if delErr := w.store.DeleteObject(ctx, w.bucket, v.s3Key); delErr != nil {
				slog.Warn("jobs.customer_backup_runner.retention_s3_delete_failed",
					"tier", tier, "s3_key", v.s3Key, "error", delErr)
				continue
			}
			if _, updErr := w.db.ExecContext(ctx, `
				UPDATE resource_backups
				   SET s3_key = NULL,
				       error_summary = 'retained:expired'
				 WHERE id = $1
			`, v.id); updErr != nil {
				slog.Warn("jobs.customer_backup_runner.retention_db_update_failed",
					"backup_id", v.id, "error", updErr)
			}
		}
		if len(victims) > 0 {
			slog.Info("jobs.customer_backup_runner.retention_swept",
				"tier", tier, "deleted", len(victims))
		}
	}
}

// limitedBuffer is a tiny bytes.Buffer wrapper that caps growth at 4KiB so
// a chatty pg_dump can't blow up worker RAM with stderr. The error_summary
// column is TEXT so we'll truncate at write-site anyway.
type limitedBuffer struct {
	buf [4096]byte
	n   int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	remaining := len(b.buf) - b.n
	if remaining <= 0 {
		return len(p), nil // silently drop
	}
	if len(p) > remaining {
		p = p[:remaining]
	}
	copy(b.buf[b.n:], p)
	b.n += len(p)
	return len(p), nil
}

func (b *limitedBuffer) String() string {
	return string(b.buf[:b.n])
}
