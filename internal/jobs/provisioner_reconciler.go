package jobs

// provisioner_reconciler.go — sweep that recovers stuck pending resources.
//
// Background: when the api crashes mid-provision (between INSERT INTO
// resources status='pending' and the gRPC provision call to the provisioner
// service), the row sits in 'pending' indefinitely. The customer's quota
// counter was already incremented but the resource is unusable, and there is
// no recovery path. This job sweeps every 2 minutes:
//
//   * SELECT pending rows older than 5 minutes (younger rows are likely
//     mid-flight from a healthy api, not stuck).
//   * Probe the underlying resource via the configured ResourceProber.
//   * Reachable → flip status=active, emit audit_log
//     `provisioner.reconcile_recovered`, stamp last_reconciled_at.
//   * Unreachable → flip status=failed, NULL the connection_url, emit
//     `provisioner.reconcile_abandoned`, stamp last_reconciled_at, drop
//     the customer's quota counter by 1 via Redis (fail-open).
//   * Skip (webhook / unknown type) → stamp last_reconciled_at only, so
//     we do not tight-loop the row.
//
// Idempotency: last_reconciled_at gates the SELECT — once a row has been
// touched, it's not re-swept on the next 2-minute tick.
//
// Error handling: a SELECT failure returns an error so River retries. Per-
// row failures (probe error, INSERT failure, UPDATE failure) log + continue
// — the brief explicitly demands "On any DB error mid-loop, log and continue
// to next row (don't crash the whole job)".

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	"instant.dev/worker/internal/logsafe"
	"instant.dev/worker/internal/metrics"
)

// ProvisionerReconcilerArgs is the River job payload — no fields, sweep job.
type ProvisionerReconcilerArgs struct{}

// Kind is the River worker key.
func (ProvisionerReconcilerArgs) Kind() string { return "provisioner_reconciler" }

// provisionerReconcilerInterval is the periodic dispatch cadence (2 minutes).
// The brief mandates 2 minutes. Rationale: any api crash mid-provision will
// be recovered within ~7 minutes (5-min stuck threshold + 2-min sweep cadence)
// — fast enough that a customer hitting "create db" won't notice, slow enough
// that the platform DB isn't hot-pathed for sweeps.
const provisionerReconcilerInterval = 2 * time.Minute

// provisionerReconcilerStuckThreshold is the minimum age for a pending row
// before the reconciler will touch it. Rows younger than this may be
// mid-flight from a healthy api (the provision RPC hasn't returned yet);
// touching them would race the api and produce confusing audit_log entries.
const provisionerReconcilerStuckThreshold = 5 * time.Minute

// provisionerReconcilerBatchLimit caps the per-tick fan-out. 100 matches the
// brief; a backlog larger than this drains across consecutive 2-min ticks.
// last_reconciled_at prevents the same row being re-processed before its
// peer rows get their first sweep.
const provisionerReconcilerBatchLimit = 100

// provisionerReconcilerProbeTimeout is the per-resource probe budget. 5s
// matches the heartbeat probe budget — both jobs use the same prober.
const provisionerReconcilerProbeTimeout = 5 * time.Second

// Audit kinds — duplicated from api per existing pattern. The literal strings
// here are the contract; api/internal/handlers must use the same values.
//
// reconcileRecoveredKind: pending → active.
// reconcileAbandonedKind: pending → failed.
const (
	reconcileRecoveredKind = "provisioner.reconcile_recovered"
	reconcileAbandonedKind = "provisioner.reconcile_abandoned"
)

// reconcileActor is the audit_log.actor for system-written rows. Matches
// the convention used by churn_predictor.go / expire_imminent.go.
const reconcileActor = "system"

// ProvisionerReconcilerWorker scans for stuck pending resources and decides
// each one's fate via the configured ResourceProber.
type ProvisionerReconcilerWorker struct {
	river.WorkerDefaults[ProvisionerReconcilerArgs]
	db     *sql.DB
	rdb    *redis.Client  // optional — used to refund quota counters on abandon
	prober ResourceProber // optional — nil falls back to NoopProber (see prober.go)
}

// NewProvisionerReconcilerWorker constructs the worker.
//
// rdb may be nil — abandon will skip the quota refund step (logged at WARN).
// prober may be nil — defaults to NoopProber (see prober.go for the fail-open
// rationale; the brief's "any pending > 30min" alert is the operator's
// safety net while a real prober is wired).
func NewProvisionerReconcilerWorker(db *sql.DB, rdb *redis.Client, prober ResourceProber) *ProvisionerReconcilerWorker {
	if prober == nil {
		prober = NoopProber{}
	}
	return &ProvisionerReconcilerWorker{db: db, rdb: rdb, prober: prober}
}

// reconcilerCandidate is the projection the sweep returns.
type reconcilerCandidate struct {
	id            uuid.UUID
	token         uuid.UUID
	resourceType  string
	connectionURL sql.NullString // may be NULL (api crashed before storing it)
	teamID        sql.NullString // string-form uuid; NULL for anonymous rows
}

// Work executes one sweep.
func (w *ProvisionerReconcilerWorker) Work(ctx context.Context, job *river.Job[ProvisionerReconcilerArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.provisioner_reconciler")
	defer span.End()

	start := time.Now()
	stuckCutoff := time.Now().UTC().Add(-provisionerReconcilerStuckThreshold)

	// Sweep. The brief's spec:
	//
	//   SELECT id, token, resource_type, created_at
	//   FROM resources
	//   WHERE status='pending' AND created_at < NOW() - INTERVAL '5 minutes'
	//   LIMIT 100
	//
	// We additionally project connection_url + team_id (needed for the probe
	// and the quota-refund Redis key) and filter on last_reconciled_at to
	// avoid the tight-loop re-sweep the brief flagged. The partial index
	// idx_resources_pending_sweep (see sql/030_resource_heartbeat.sql) keeps
	// this query O(stuck rows) even when the active table is huge.
	rows, err := w.db.QueryContext(ctx, `
		SELECT
			id,
			token,
			resource_type,
			COALESCE(connection_url, '') AS connection_url,
			COALESCE(team_id::text, '')   AS team_id_text
		FROM resources
		WHERE status = 'pending'
		  AND created_at < $1
		  AND (last_reconciled_at IS NULL OR last_reconciled_at < $1)
		ORDER BY created_at ASC
		LIMIT $2
	`, stuckCutoff, provisionerReconcilerBatchLimit)
	if err != nil {
		return fmt.Errorf("ProvisionerReconcilerWorker: query failed: %w", err)
	}
	defer rows.Close()

	var candidates []reconcilerCandidate
	for rows.Next() {
		var c reconcilerCandidate
		var connURL, teamID string
		if scanErr := rows.Scan(&c.id, &c.token, &c.resourceType, &connURL, &teamID); scanErr != nil {
			slog.Warn("jobs.provisioner_reconciler.scan_failed", "error", scanErr)
			continue
		}
		if connURL != "" {
			c.connectionURL = sql.NullString{String: connURL, Valid: true}
		}
		if teamID != "" {
			c.teamID = sql.NullString{String: teamID, Valid: true}
		}
		candidates = append(candidates, c)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return fmt.Errorf("ProvisionerReconcilerWorker: rows error: %w", rowsErr)
	}
	rows.Close()

	if len(candidates) == 0 {
		// T21 P1-1 (BugBash 2026-05-20): idle-tick demoted INFO→DEBUG.
		// The provisioner reconciler runs every 2 min and is observed in
		// prod logs (`candidates:0` continuously) because no row is ever
		// `status='pending'` until MR-P0-2 lands — even after, the steady
		// state is the empty case.
		slog.Debug("jobs.provisioner_reconciler.completed",
			"recovered", 0,
			"abandoned", 0,
			"candidates", 0,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil
	}

	var recovered, abandoned, skipped int
	for _, c := range candidates {
		// 5s probe budget per resource — bounded so a single hung connection
		// doesn't stall the sweep past its 2-minute cadence.
		probeCtx, cancel := context.WithTimeout(ctx, provisionerReconcilerProbeTimeout)
		outcome, probeErr := w.prober.Probe(probeCtx, c.resourceType, c.connectionURL.String)
		cancel()

		switch outcome {
		case ProbeReachable:
			if uErr := w.markRecovered(ctx, c); uErr != nil {
				slog.Error("jobs.provisioner_reconciler.mark_recovered_failed",
					"resource_id", c.id.String(),
					"resource_type", c.resourceType,
					"error", uErr,
				)
				continue
			}
			metrics.ReconcileRecoveredTotal.WithLabelValues(c.resourceType).Inc()
			recovered++
			slog.Info("jobs.provisioner_reconciler.recovered",
				"resource_id", c.id.String(),
				"resource_type", c.resourceType,
				"token", logsafe.Token(c.token.String()),
			)

		case ProbeUnreachable:
			if uErr := w.markAbandoned(ctx, c, probeErr); uErr != nil {
				slog.Error("jobs.provisioner_reconciler.mark_abandoned_failed",
					"resource_id", c.id.String(),
					"resource_type", c.resourceType,
					"error", uErr,
				)
				continue
			}
			// Refund the customer's quota counter — fail-open per the brief
			// ("just decrement Redis quota key directly"). Errors are
			// logged but never block the loop; a fresh provision will
			// re-check the counter against the actual row count anyway.
			w.refundQuota(ctx, c)
			metrics.ReconcileAbandonedTotal.WithLabelValues(c.resourceType).Inc()
			abandoned++
			slog.Info("jobs.provisioner_reconciler.abandoned",
				"resource_id", c.id.String(),
				"resource_type", c.resourceType,
				"token", logsafe.Token(c.token.String()),
				"reason", truncateReason(probeErrString(probeErr)),
			)

		case ProbeSkip:
			// Stamp last_reconciled_at so we don't reprocess the same row
			// next tick. Webhook rows in particular sit pending until they
			// get their first inbound request; touching them as "abandoned"
			// would break the existing onboarding flow.
			if _, uErr := w.db.ExecContext(ctx,
				`UPDATE resources SET last_reconciled_at = NOW() WHERE id = $1`,
				c.id,
			); uErr != nil {
				slog.Error("jobs.provisioner_reconciler.stamp_failed",
					"resource_id", c.id.String(),
					"resource_type", c.resourceType,
					"error", uErr,
				)
				continue
			}
			skipped++
		}
	}

	slog.Info("jobs.provisioner_reconciler.completed",
		"recovered", recovered,
		"abandoned", abandoned,
		"skipped", skipped,
		"candidates", len(candidates),
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}

// markRecovered flips a pending row to active, stamps last_reconciled_at,
// and writes the audit_log entry. The UPDATE + INSERT are issued as separate
// statements — wrapping in a transaction would be tidier but the brief's
// fail-open contract (one bad row doesn't stop the loop) is easier to honour
// without holding a tx open across the prober call.
func (w *ProvisionerReconcilerWorker) markRecovered(ctx context.Context, c reconcilerCandidate) error {
	if _, err := w.db.ExecContext(ctx, `
		UPDATE resources
		SET status = 'active', last_reconciled_at = NOW()
		WHERE id = $1 AND status = 'pending'
	`, c.id); err != nil {
		return fmt.Errorf("update active: %w", err)
	}

	meta := map[string]any{
		"resource_id":   c.id.String(),
		"resource_type": c.resourceType,
		"token":         logsafe.Token(c.token.String()),
	}
	metaBytes, _ := json.Marshal(meta) // map of primitives — can't fail

	summary := fmt.Sprintf("%s resource %s recovered from pending → active by reconciler", c.resourceType, c.id)

	if _, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES ($1, $2, $3, $4, $5)
	`, nullableTeamID(c.teamID), reconcileActor, reconcileRecoveredKind, summary, metaBytes); err != nil {
		return fmt.Errorf("insert audit_log: %w", err)
	}
	return nil
}

// markAbandoned flips a pending row to failed, NULLs the connection_url
// (the brief mandates this so a future agent doesn't accidentally hand out
// a credentials blob pointing at an unreachable resource), stamps
// last_reconciled_at, and writes the audit_log entry.
func (w *ProvisionerReconcilerWorker) markAbandoned(ctx context.Context, c reconcilerCandidate, probeErr error) error {
	if _, err := w.db.ExecContext(ctx, `
		UPDATE resources
		SET status = 'failed', connection_url = NULL, last_reconciled_at = NOW()
		WHERE id = $1 AND status = 'pending'
	`, c.id); err != nil {
		return fmt.Errorf("update failed: %w", err)
	}

	meta := map[string]any{
		"resource_id":   c.id.String(),
		"resource_type": c.resourceType,
		"token":         logsafe.Token(c.token.String()),
		"error":         truncateReason(probeErrString(probeErr)),
	}
	metaBytes, _ := json.Marshal(meta)

	summary := fmt.Sprintf("%s resource %s abandoned (pending > %s, probe failed)",
		c.resourceType, c.id, provisionerReconcilerStuckThreshold)

	if _, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES ($1, $2, $3, $4, $5)
	`, nullableTeamID(c.teamID), reconcileActor, reconcileAbandonedKind, summary, metaBytes); err != nil {
		return fmt.Errorf("insert audit_log: %w", err)
	}
	return nil
}

// refundQuota decrements the customer's per-resource-type quota counter in
// Redis. The key pattern matches the api's quota package (quota:{team_id}:
// {resource_type}). Errors are logged but never block — the api re-derives
// counters from row counts on quota-check anyway, so a missed DECR just
// slightly over-counts until the next clean cycle.
//
// For anonymous rows (team_id NULL) the api uses the fingerprint as the
// counter axis instead of team_id; the worker doesn't have fingerprint
// here, so we skip and log. This is a known minor gap — the brief
// explicitly allows it ("just decrement Redis quota key directly — see
// existing pattern").
func (w *ProvisionerReconcilerWorker) refundQuota(ctx context.Context, c reconcilerCandidate) {
	if w.rdb == nil {
		return
	}
	if !c.teamID.Valid {
		slog.Debug("jobs.provisioner_reconciler.quota_skip_anonymous",
			"resource_id", c.id.String(),
		)
		return
	}
	key := fmt.Sprintf("quota:%s:%s", c.teamID.String, c.resourceType)
	if _, err := w.rdb.Decr(ctx, key).Result(); err != nil {
		slog.Warn("jobs.provisioner_reconciler.quota_refund_failed",
			"resource_id", c.id.String(),
			"team_id", c.teamID.String,
			"key", key,
			"error", err,
		)
	}
}

// nullableTeamID converts a sql.NullString team_id into a value suitable for
// the audit_log.team_id column (UUID or NULL). The audit_log table accepts
// NULL team_id rows for anonymous-resource events.
func nullableTeamID(v sql.NullString) any {
	if !v.Valid || v.String == "" {
		return nil
	}
	return v.String
}

// probeErrString defends against nil-error inputs from ProbeUnreachable.
// Per prober.go's contract, ProbeUnreachable comes with a non-nil err, but
// belt-and-braces — a misbehaving prober shouldn't crash the sweep.
func probeErrString(err error) string {
	if err == nil {
		return "probe returned unreachable but no error message"
	}
	return err.Error()
}

// truncateReason caps a probe error string at 500 chars so the audit_log
// metadata stays a reasonable size even if the underlying driver dumps a
// stack-trace-shaped error.
func truncateReason(s string) string {
	const max = 500
	if len(s) <= max {
		return s
	}
	return s[:max]
}
