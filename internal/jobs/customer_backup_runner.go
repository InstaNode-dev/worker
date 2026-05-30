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
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	"instant.dev/common/crypto"
	"instant.dev/worker/internal/apiclient"
	"instant.dev/worker/internal/circuit"
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
	// SEC-WORKER FINDING-2 (2026-05-29): split the customer's DB password
	// out of the URL into PGPASSWORD env so it does NOT sit in argv (and
	// therefore /proc/<pid>/cmdline + `ps aux` + kubectl describe crash
	// archive) for the entire hourly backup window. Fail-open on parse
	// error to avoid a single malformed connection_url stalling every
	// customer's backup ladder.
	dsn, pw, splitErr := splitPGPassword(connURL)
	if splitErr != nil {
		dsn = connURL
		pw = ""
	}
	cmd := exec.CommandContext(ctx, "pg_dump",
		"--no-owner", "--no-acl",
		"--format=custom",
		"-d", dsn,
	)
	if pw != "" {
		cmd.Env = append(os.Environ(), "PGPASSWORD="+pw)
	}
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
	db      *sql.DB
	store   BackupObjectStore
	pgDump  pgDumpRunner
	bucket  string
	prefix  string
	aesKey  string // hex, decoded at use site via crypto.ParseAESKey
	plans   BackupPlanRegistry
	now     func() time.Time
	timeout time.Duration
	batchN  int

	// apiBase / apiCli / jwtSecret — used by the FIX-H #65/#Q47 refund
	// path. When apiBase or jwtSecret is empty the refund call is a
	// no-op (logged) — same fail-open posture as the rest of the worker.
	apiBase   string
	apiCli    *apiclient.Client
	jwtSecret string
}

// NewCustomerBackupRunner constructs a runner with production defaults.
// store may be nil — the worker then logs WARN and skips every batch
// (fail-open). aesKey may be empty — same WARN-and-skip behavior, since we
// refuse to dump from a plaintext connection_url. plans may be nil —
// retentionDaysForTier then falls back to a hardcoded 7-day default and
// logs a WARN; the sweep still runs but with a coarse policy.
func NewCustomerBackupRunner(db *sql.DB, store BackupObjectStore, bucket, prefix, aesKey string, plans BackupPlanRegistry) *CustomerBackupRunnerWorker {
	return &CustomerBackupRunnerWorker{
		db:      db,
		store:   store,
		pgDump:  realPgDumpRunner{},
		bucket:  bucket,
		prefix:  prefix,
		aesKey:  aesKey,
		plans:   plans,
		now:     time.Now,
		timeout: backupPerRunTimeout,
		batchN:  backupBatchSize,
	}
}

// WithRefundClient wires the api endpoint + JWT secret used by the
// FIX-H #65/#Q47 refund path. cmd/ should call this after construction
// with the api base URL (e.g. http://instant-api.instant.svc.cluster.local:8080)
// and the shared WORKER_INTERNAL_JWT_SECRET. Calling with empty strings
// disables the refund (no-op + WARN); same posture as the rest of the
// fail-open guards in this worker.
func (w *CustomerBackupRunnerWorker) WithRefundClient(apiBase, jwtSecret string, httpCli *http.Client) *CustomerBackupRunnerWorker {
	w.apiBase = strings.TrimRight(apiBase, "/")
	w.jwtSecret = jwtSecret
	if httpCli == nil {
		httpCli = &http.Client{Timeout: 10 * time.Second}
	}
	w.apiCli = apiclient.New(httpCli)
	return w
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

	// P2-W4 (BugBash 2026-05-18): stuck-row recovery. A backup row is
	// atomically claimed by flipping status 'pending' → 'running'. If the
	// worker pod is killed (rolling deploy, OOM, node drain) AFTER the
	// claim but BEFORE finalize/markFailed, the row is orphaned at
	// 'running' forever — the pending-row sweep below only selects
	// status='pending', so no runner ever picks it up again. A manual
	// backup is then silently lost. Recover here: any 'running' row whose
	// started_at is older than backupPerRunTimeout could not still be a
	// live in-flight backup (the per-run context would have fired), so it
	// is reset to 'pending' to be re-claimed on this same tick. The
	// timeout floor guarantees we never reclaim a backup that's genuinely
	// still streaming.
	w.recoverStuckRows(ctx)

	// Sweep pending rows. We pull the resource side-data (connection_url,
	// token, team_id, resource_type) in the same SELECT so the per-row
	// path only needs one ExecContext for the claim + one for the final
	// status update.
	rows, err := w.db.QueryContext(ctx, `
		SELECT b.id::text, b.resource_id::text, b.tier_at_backup, b.backup_kind,
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
	defer func() { _ = rows.Close() }()

	type pending struct {
		backupID     string
		resourceID   string
		tier         sql.NullString
		kind         string // 'scheduled' | 'manual' — for refund routing
		token        string
		connURL      sql.NullString
		resourceType string
		teamID       uuid.NullUUID
	}
	var batch []pending
	for rows.Next() {
		var p pending
		if scanErr := rows.Scan(
			&p.backupID, &p.resourceID, &p.tier, &p.kind,
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
	_ = rows.Close()

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

	// T21 P1-1 (BugBash 2026-05-20): idle-tick demoted INFO→DEBUG. The
	// runner is invoked per River batch; the steady state in prod is
	// processed=0 (no backups in the batch window). When real work
	// happened (processed>0 OR a failure surfaced), stay at INFO so
	// operators see the activity.
	if processed == 0 && failed == 0 {
		slog.Debug("jobs.customer_backup_runner.completed",
			"processed", processed,
			"succeeded", succeeded,
			"failed", failed,
			"job_id", job.ID,
		)
	} else {
		slog.Info("jobs.customer_backup_runner.completed",
			"processed", processed,
			"succeeded", succeeded,
			"failed", failed,
			"job_id", job.ID,
		)
	}
	return nil
}

// recoverStuckRows resets backup rows orphaned at status='running' back
// to 'pending' so a future tick re-claims them. A row qualifies only
// when started_at is older than backupPerRunTimeout — a genuinely
// in-flight backup is bounded by that per-run context, so anything
// older is a casualty of a pod kill, not a live job. Best-effort: a
// failure here is logged and the sweep proceeds (the pending-row scan
// still drains the normal queue).
func (w *CustomerBackupRunnerWorker) recoverStuckRows(ctx context.Context) {
	res, err := w.db.ExecContext(ctx, `
		UPDATE resource_backups
		   SET status = 'pending',
		       started_at = NULL,
		       error_summary = 'recovered: runner pod lost before finalize — re-queued'
		 WHERE status = 'running'
		   AND started_at IS NOT NULL
		   AND started_at < now() - ($1::int * INTERVAL '1 second')
	`, int(w.timeout.Seconds()))
	if err != nil {
		slog.Warn("jobs.customer_backup_runner.stuck_row_recovery_failed", "error", err)
		return
	}
	if n, raErr := res.RowsAffected(); raErr == nil && n > 0 {
		slog.Warn("jobs.customer_backup_runner.recovered_stuck_rows",
			"count", n,
			"note", "rows orphaned at status='running' past the per-run timeout — reset to pending",
		)
	}
}

// processBackup runs a single backup row end-to-end. Returns true on success.
// Always logs + updates DB; never propagates an error to the caller.
func (w *CustomerBackupRunnerWorker) processBackup(parentCtx context.Context, p struct {
	backupID     string
	resourceID   string
	tier         sql.NullString
	kind         string
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

	// Step 3 — stream pg_dump → gzip → (sha256 + S3) via io.Pipe.
	//
	// FIX-H #59 — the gzip output is teed into a SHA-256 hasher so the
	// final hex digest is available at finalize time. We hash the
	// COMPRESSED bytes (not the raw pg_dump output) because the
	// compressed object is what lives in S3 and what the restore
	// handler / runner will re-read for verification. Hashing happens
	// inline on the writer side — no second pass over the bytes.
	objectKey := backupObjectKey(w.prefix, p.token, p.backupID)
	pr, pw := io.Pipe()
	hasher := sha256.New()

	// Goroutine: pg_dump writes raw archive bytes into the gzip writer,
	// which writes compressed bytes into a MultiWriter that fans out to
	// the sha256 hasher AND the pipe writer (which the S3 Upload reads
	// from). Closing the gzip writer flushes its final gzip footer,
	// then we close pw to signal EOF to the S3 Upload reader side.
	dumpDone := make(chan error, 1)
	go func() {
		// Panic boundary (P1-B): a panic in pg_dump / gzip would otherwise
		// crash the worker pod. On panic the inline pw.CloseWithError below
		// is skipped, so close the pipe with an explicit error here too so
		// the Upload reader sees EOF instead of blocking forever.
		defer func() {
			if r := recover(); r != nil {
				panicErr := fmt.Errorf("pg_dump goroutine panicked: %v", r)
				_ = pw.CloseWithError(panicErr)
				dumpDone <- panicErr
				LogRecoveredPanic("customer_backup_runner.pg_dump_pipe", r)
			}
		}()
		mw := io.MultiWriter(hasher, pw)
		gz := gzip.NewWriter(mw)
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
	digestHex := finalizeDigest(hasher, dumpErr, upErr)

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

	// Step 4 — finalize. FIX-H #59: stamp sha256 alongside s3_key and
	// size_bytes so the restore handler can verify integrity against a
	// fresh re-read of the object.
	//
	// P2-W4 (BugBash 2026-05-18): the finalize UPDATE runs on a FRESH
	// bounded context, not parentCtx. The object is already durably in
	// S3 at this point — if parentCtx were cancelled (worker shutdown)
	// the UPDATE on parentCtx would fail, leaving the row stuck at
	// 'running' while the S3 object exists: a paid-for backup the
	// customer can never see and the stuck-row recovery would later
	// re-run, orphaning the first object. A 10s fresh context (mirroring
	// markFailed) lets the row reach 'ok' even mid-shutdown so the
	// upload and the DB row stay consistent.
	finalizeCtx, finalizeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer finalizeCancel()
	if _, updErr := w.db.ExecContext(finalizeCtx, `
		UPDATE resource_backups
		   SET status = 'ok',
		       finished_at = now(),
		       s3_key = $2,
		       size_bytes = $3,
		       sha256 = NULLIF($4,'')
		 WHERE id = $1
	`, p.backupID, objectKey, size, digestHex); updErr != nil {
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
//
// FIX-H #65/#Q47 — when the failed row was a MANUAL backup, we POST to
// the api's internal refund endpoint so the team's daily counter is
// credited. Scheduled backups don't burn the manual-counter so no
// refund is needed.
func (w *CustomerBackupRunnerWorker) markFailed(
	ctx context.Context, backupID, errSummary string, start time.Time,
	p struct {
		backupID     string
		resourceID   string
		tier         sql.NullString
		kind         string
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

	// FIX-H #65/#Q47 — refund the manual-backups-today counter when a
	// MANUAL backup fails. Scheduled backups don't burn the counter so
	// they don't need a refund. Best-effort: a refund failure (api down,
	// breaker open) logs and moves on; the customer's counter stays
	// burned for the rest of the UTC day, matching pre-fix behavior.
	if p.kind == "manual" && p.teamID.Valid {
		refundErr := w.refundManualBackupQuota(p.teamID.UUID, backupID)
		if refundErr != nil {
			slog.Warn("jobs.customer_backup_runner.refund_failed",
				"backup_id", backupID,
				"team_id", p.teamID.UUID,
				"error", refundErr,
			)
		}
	}

	_ = ctx // (parentCtx) keep param to surface intent even though we use a fresh ctx
}

// finalizeDigest returns the hex-encoded SHA-256 of the gzipped pg_dump
// stream IFF the dump + upload both succeeded. On failure we deliberately
// return "" so the finalize UPDATE writes NULL into sha256 — recording a
// digest for a partial / corrupt object would lie to the restore handler.
func finalizeDigest(h hash.Hash, dumpErr, upErr error) string {
	if dumpErr != nil || upErr != nil {
		return ""
	}
	return hex.EncodeToString(h.Sum(nil))
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
	// index on tier_at_backup efficiently. The tier list comes from the
	// plans.Registry (not a hardcoded slice) so newly-added tiers in
	// plans.yaml — e.g. hobby_plus, hobby_plus_yearly, pro_yearly — get
	// retention applied the moment the worker boots with the new
	// embedded YAML. If the registry is nil (boot misconfigured) we
	// fall back to the historical five tiers so the sweep still runs.
	tiers := []string{"hobby", "pro", "growth", "team", "anonymous"}
	if w.plans != nil {
		tiers = w.plans.TierNames()
	}
	for _, tier := range tiers {
		cutoff := retentionCutoff(w.plans, tier, w.now())
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
		_ = rows.Close()

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

// refundManualBackupQuota POSTs to the api's internal refund endpoint to
// decrement the team's manual-backups-today counter after a manual
// backup failed terminally. FIX-H #65/#Q47 BugBash B36.
//
// Returns nil on a 2xx response or when the refund is disabled (no
// apiBase / no jwtSecret). The endpoint is idempotent: replays for the
// same backup_id are no-ops on the api side, so a worker restart
// mid-batch that re-processes the same row can't double-credit.
//
// Failure modes (logged but not retried):
//   - circuit.ErrOpen: api is hosed; refund skipped. The customer
//     loses one unit of daily headroom — same as pre-fix behavior.
//   - network / 5xx: same as above. The next manual backup the team
//     attempts will see the (unrefunded) counter.
//   - 4xx: refund call shape is wrong (e.g. bad JWT). Skip; an operator
//     will see the error in slog and fix the wiring.
func (w *CustomerBackupRunnerWorker) refundManualBackupQuota(teamID uuid.UUID, backupID string) error {
	if w.apiBase == "" || w.jwtSecret == "" || w.apiCli == nil {
		slog.Warn("jobs.customer_backup_runner.refund_disabled",
			"reason", "apiBase/jwtSecret/apiCli unset",
			"team_id", teamID.String(),
			"backup_id", backupID,
		)
		return nil
	}

	url := fmt.Sprintf("%s/internal/teams/%s/backup-quota/refund", w.apiBase, teamID.String())
	bodyBytes, _ := json.Marshal(map[string]string{"backup_id": backupID})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("build refund request: %w", err)
	}
	tok, tokErr := signBackupRefundJWT(w.jwtSecret, teamID.String())
	if tokErr != nil {
		return fmt.Errorf("sign refund jwt: %w", tokErr)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "instanode-worker/backup-refund")

	resp, doErr := w.apiCli.Do(req)
	if doErr != nil {
		if errors.Is(doErr, circuit.ErrOpen) {
			return fmt.Errorf("api circuit open")
		}
		return fmt.Errorf("api request: %w", doErr)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("api status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// signBackupRefundJWT mints the HS256 token the api's
// /internal/teams/:id/backup-quota/refund endpoint expects. Shape
// matches verifyInternalBackupRefundJWT on the api side:
//
//	purpose  — "internal_backup_refund"
//	team_id  — the team uuid the api will compare against the path :id
//	iat      — required, within ±60s of api-side now
func signBackupRefundJWT(secret, teamID string) (string, error) {
	if secret == "" {
		return "", errors.New("empty secret")
	}
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	now := time.Now().UTC().Unix()
	claims := map[string]any{
		"purpose": "internal_backup_refund",
		"team_id": teamID,
		"iat":     now,
		"exp":     now + 5*60,
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("marshal claims: %w", err)
	}
	body := header + "." + base64.RawURLEncoding.EncodeToString(claimsJSON)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return body + "." + sig, nil
}
