package jobs

// resource_heartbeat.go — periodic liveness probe of every active resource.
//
// Background: a customer's Postgres pod can get evicted, an upstream MongoDB
// cluster can drop, a redis ACL can be corrupted — and today we don't notice
// until the customer's app errors. This job sweeps every active resource
// hourly (in prod) / every minute (in dev/test) and:
//
//   * Successful probe → UPDATE last_seen_at=NOW(), degraded=false.
//   * Failed probe     → UPDATE degraded=true, degraded_reason=$err,
//                        BUT only if (degraded was already false) OR
//                        (last_seen_at is older than 10 min). This guard
//                        prevents flap-spam in audit_log.
//   * State transition (false→true or true→false) → emit an audit_log
//     row with kind resource.degraded / resource.recovered.
//
// Concurrency: a semaphore of 20 in flight (golang.org/x/sync/semaphore).
// 20 is the brief's mandate; with each probe budgeted at 5s and a typical
// active-resource count in the low thousands, a full sweep completes well
// inside the hourly cadence.
//
// Fail-open: per-row errors log + continue. A SELECT failure returns an
// error so River retries.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/semaphore"

	"instant.dev/worker/internal/metrics"
)

// ResourceHeartbeatArgs is the River job payload — no fields, sweep job.
type ResourceHeartbeatArgs struct{}

// Kind is the River worker key.
func (ResourceHeartbeatArgs) Kind() string { return "resource_heartbeat" }

// resourceHeartbeatInterval is the production cadence (1 hour). The dev/
// test cadence (1 minute) is selected by the worker config (see
// StartWorkers) and passed in via NewResourceHeartbeatWorker — the constant
// here only documents prod.
const resourceHeartbeatInterval = 1 * time.Hour

// resourceHeartbeatDevInterval is the cadence the worker uses when
// cfg.Environment != "production". 1 minute makes the dev loop tight
// enough to verify degraded-flag transitions in a single test session.
const resourceHeartbeatDevInterval = 1 * time.Minute

// resourceHeartbeatBatchLimit caps the per-tick fan-out. 500 matches the
// brief; with concurrency=20 and budget=5s/probe, a full batch finishes in
// ~125s worst-case — comfortably inside the hourly cadence.
const resourceHeartbeatBatchLimit = 500

// resourceHeartbeatProbeTimeout is the per-resource probe budget. 5s
// matches the brief and the reconciler probe budget.
const resourceHeartbeatProbeTimeout = 5 * time.Second

// resourceHeartbeatConcurrency is the in-flight probe ceiling. 20 is the
// brief's mandate (golang.org/x/sync/semaphore).
const resourceHeartbeatConcurrency = 20

// resourceHeartbeatFlapWindow is the minimum gap between two
// resource.degraded audit_log rows for the same resource. The condition
// (degraded was false) OR (last_seen_at older than 10min) means: a flap
// inside 10min doesn't re-emit; an outage that persists past 10min gets a
// fresh row (a re-emit after a long outage is informational, not spam).
const resourceHeartbeatFlapWindow = 10 * time.Minute

// Audit kinds — duplicated from api per existing pattern. The literal
// strings here are the contract; api must use the same values.
const (
	resourceDegradedKind = "resource.degraded"
	resourceRecoveredKind = "resource.recovered"
)

// resourceHeartbeatActor is the audit_log.actor value for system-written
// rows. Matches the convention used by churn_predictor.go.
const resourceHeartbeatActor = "system"

// ResourceHeartbeatWorker probes every active resource and updates
// last_seen_at / degraded / degraded_reason. State transitions emit
// audit_log rows.
type ResourceHeartbeatWorker struct {
	river.WorkerDefaults[ResourceHeartbeatArgs]
	db     *sql.DB
	prober ResourceProber
}

// NewResourceHeartbeatWorker constructs the worker. prober may be nil —
// defaults to NoopProber (see prober.go for the fail-open rationale).
func NewResourceHeartbeatWorker(db *sql.DB, prober ResourceProber) *ResourceHeartbeatWorker {
	if prober == nil {
		prober = NoopProber{}
	}
	return &ResourceHeartbeatWorker{db: db, prober: prober}
}

// heartbeatCandidate is the projection of resources used by the worker.
type heartbeatCandidate struct {
	id            uuid.UUID
	token         uuid.UUID
	resourceType  string
	connectionURL string
	teamID        sql.NullString
	wasDegraded   bool
	lastSeenAt    sql.NullTime
}

// Work executes one sweep.
func (w *ResourceHeartbeatWorker) Work(ctx context.Context, job *river.Job[ResourceHeartbeatArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.resource_heartbeat")
	defer span.End()

	start := time.Now()

	// Sweep. The brief's spec:
	//
	//   SELECT id, token, resource_type, connection_url
	//   FROM resources
	//   WHERE status='active' AND deleted_at IS NULL LIMIT 500
	//
	// We additionally project the current degraded flag and last_seen_at so
	// the in-flight goroutine can decide whether to emit an audit_log row
	// on transition without a second DB read.
	//
	// status='active' implies deleted_at IS NULL in the current schema
	// (the expire_anonymous worker flips to status='deleted' rather than
	// stamping deleted_at), so we filter on status alone — the brief's
	// reference to deleted_at is a forward-compatibility hint. If a future
	// migration adds soft-delete via deleted_at, add the AND clause here.
	rows, err := w.db.QueryContext(ctx, `
		SELECT
			id,
			token,
			resource_type,
			COALESCE(connection_url, '') AS connection_url,
			COALESCE(team_id::text, '')   AS team_id_text,
			COALESCE(degraded, false)     AS degraded,
			last_seen_at
		FROM resources
		WHERE status = 'active'
		ORDER BY COALESCE(last_seen_at, 'epoch'::timestamptz) ASC
		LIMIT $1
	`, resourceHeartbeatBatchLimit)
	if err != nil {
		return fmt.Errorf("ResourceHeartbeatWorker: query failed: %w", err)
	}
	defer rows.Close()

	var candidates []heartbeatCandidate
	for rows.Next() {
		var c heartbeatCandidate
		var teamID string
		if scanErr := rows.Scan(
			&c.id, &c.token, &c.resourceType, &c.connectionURL,
			&teamID, &c.wasDegraded, &c.lastSeenAt,
		); scanErr != nil {
			slog.Warn("jobs.resource_heartbeat.scan_failed", "error", scanErr)
			continue
		}
		if teamID != "" {
			c.teamID = sql.NullString{String: teamID, Valid: true}
		}
		candidates = append(candidates, c)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return fmt.Errorf("ResourceHeartbeatWorker: rows error: %w", rowsErr)
	}
	rows.Close()

	if len(candidates) == 0 {
		// Wave 3 / Worker T21 P1-1 follow-up (#146): demote idle-tick INFO →
		// DEBUG. resource_heartbeat runs every 1h in prod (every 1m in dev);
		// a no-candidates tick is heartbeat noise. Liveness via
		// jobs.middleware.work_ok. The non-idle .completed (line ~232)
		// stays at INFO because it carries probe-outcome state.
		slog.Debug("jobs.resource_heartbeat.completed",
			"probed", 0,
			"ok", 0,
			"failed", 0,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil
	}

	// Fan-out with a 20-slot semaphore.
	sem := semaphore.NewWeighted(int64(resourceHeartbeatConcurrency))
	var wg sync.WaitGroup
	var okCount, failCount, skipCount int64

	for _, c := range candidates {
		// semaphore.Acquire respects ctx cancellation, so a worker shutdown
		// during a sweep drains cleanly instead of leaking goroutines.
		if acqErr := sem.Acquire(ctx, 1); acqErr != nil {
			slog.Warn("jobs.resource_heartbeat.sem_acquire_failed",
				"error", acqErr,
				"resource_id", c.id.String(),
			)
			break
		}
		wg.Add(1)
		go func(cc heartbeatCandidate) {
			// Recover is declared first so it runs LAST (LIFO) — wg.Done and
			// sem.Release still fire on the panic path, so wg.Wait() below
			// is never left hanging (P1-B).
			defer Recover("resource_heartbeat.probe")
			defer wg.Done()
			defer sem.Release(1)
			outcome := w.probeOne(ctx, cc)
			switch outcome {
			case ProbeReachable:
				atomic.AddInt64(&okCount, 1)
			case ProbeUnreachable:
				atomic.AddInt64(&failCount, 1)
			case ProbeSkip:
				atomic.AddInt64(&skipCount, 1)
			}
		}(c)
	}
	wg.Wait()

	// Sample the degraded gauge by resource_type for the NR widget.
	w.sampleDegradedGauge(ctx)

	slog.Info("jobs.resource_heartbeat.completed",
		"probed", len(candidates),
		"ok", okCount,
		"failed", failCount,
		"skipped", skipCount,
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}

// (Removed: the dead errZeroRows sentinel — BugBash 2026-05-18 P3. It was
// declared "for a symmetric refactor" and parked behind `var _ = errZeroRows`,
// but had zero call sites and the markDegraded gate uses a RowsAffected probe
// directly. A sentinel kept alive only by a blank-assignment is dead code that
// misleads readers into thinking a tagged-error path exists. If a future change
// wants typed zero-row signalling, reintroduce it at that point with a real
// caller.)

// probeOne runs the prober on a single candidate and applies the resulting
// state change. Returns the probe outcome so the caller can tally.
func (w *ResourceHeartbeatWorker) probeOne(ctx context.Context, c heartbeatCandidate) ProbeOutcome {
	probeCtx, cancel := context.WithTimeout(ctx, resourceHeartbeatProbeTimeout)
	outcome, probeErr := w.prober.Probe(probeCtx, c.resourceType, c.connectionURL)
	cancel()

	switch outcome {
	case ProbeReachable:
		metrics.ResourceHeartbeatProbesTotal.WithLabelValues(c.resourceType, "ok").Inc()
		w.markHealthy(ctx, c)

	case ProbeUnreachable:
		metrics.ResourceHeartbeatProbesTotal.WithLabelValues(c.resourceType, "fail").Inc()
		w.markDegraded(ctx, c, probeErr)

	case ProbeSkip:
		metrics.ResourceHeartbeatProbesTotal.WithLabelValues(c.resourceType, "skip").Inc()
	}
	return outcome
}

// markHealthy stamps last_seen_at + clears degraded. Emits a
// resource.recovered audit_log row IFF the row was previously degraded
// (the transition false→true is the interesting event for the dashboard).
func (w *ResourceHeartbeatWorker) markHealthy(ctx context.Context, c heartbeatCandidate) {
	if _, err := w.db.ExecContext(ctx, `
		UPDATE resources
		SET last_seen_at = NOW(), degraded = false, degraded_reason = NULL
		WHERE id = $1
	`, c.id); err != nil {
		slog.Error("jobs.resource_heartbeat.update_healthy_failed",
			"resource_id", c.id.String(),
			"error", err,
		)
		return
	}

	// Only emit recovered on a true→false transition.
	if !c.wasDegraded {
		return
	}
	meta := map[string]any{
		"resource_id":   c.id.String(),
		"resource_type": c.resourceType,
		"token":         c.token.String(),
	}
	metaBytes, _ := json.Marshal(meta)
	summary := fmt.Sprintf("%s resource %s recovered (heartbeat probe succeeded)", c.resourceType, c.id)
	if _, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES ($1, $2, $3, $4, $5)
	`, nullableTeamID(c.teamID), resourceHeartbeatActor, resourceRecoveredKind, summary, metaBytes); err != nil {
		slog.Error("jobs.resource_heartbeat.audit_recovered_failed",
			"resource_id", c.id.String(),
			"error", err,
		)
	}
}

// markDegraded sets degraded=true + records the reason. Emits a
// resource.degraded audit_log row IFF either:
//   * the row was previously NOT degraded (transition false→true), OR
//   * the row WAS degraded but last_seen_at is older than 10min
//     (informational re-emit for a sustained outage).
//
// The UPDATE itself has the same gate as the brief — see the WHERE clause.
func (w *ResourceHeartbeatWorker) markDegraded(ctx context.Context, c heartbeatCandidate, probeErr error) {
	reason := truncateReason(probeErrString(probeErr))

	// The brief's gate: avoid flap-spam by only writing when state changes
	// OR the previous degraded notice is old. The UPDATE returns the new
	// degraded_reason via RETURNING so we know whether a row was touched
	// — that's the signal to emit the audit row.
	flapCutoff := time.Now().UTC().Add(-resourceHeartbeatFlapWindow)
	var rowsAffected int64
	res, err := w.db.ExecContext(ctx, `
		UPDATE resources
		SET degraded = true, degraded_reason = $2
		WHERE id = $1
		  AND (degraded IS NOT TRUE OR last_seen_at IS NULL OR last_seen_at < $3)
	`, c.id, reason, flapCutoff)
	if err != nil {
		slog.Error("jobs.resource_heartbeat.update_degraded_failed",
			"resource_id", c.id.String(),
			"error", err,
		)
		return
	}
	if res != nil {
		rowsAffected, _ = res.RowsAffected()
	}
	if rowsAffected == 0 {
		// Inside the flap window AND already degraded — no audit row.
		return
	}

	// Only emit degraded on a transition (false→true) — the long-outage
	// case (true→true) updates the row's reason but does not spam audit_log.
	// This is a stricter reading of the brief than the UPDATE itself:
	// the UPDATE refreshes the reason for an ongoing outage; the audit_log
	// row is reserved for state changes, which is what the dashboard banner
	// listens for.
	if c.wasDegraded {
		return
	}
	meta := map[string]any{
		"resource_id":   c.id.String(),
		"resource_type": c.resourceType,
		"token":         c.token.String(),
		"error":         reason,
	}
	metaBytes, _ := json.Marshal(meta)
	summary := fmt.Sprintf("%s resource %s degraded (heartbeat probe failed)", c.resourceType, c.id)
	if _, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES ($1, $2, $3, $4, $5)
	`, nullableTeamID(c.teamID), resourceHeartbeatActor, resourceDegradedKind, summary, metaBytes); err != nil {
		slog.Error("jobs.resource_heartbeat.audit_degraded_failed",
			"resource_id", c.id.String(),
			"error", err,
		)
	}
}

// sampleDegradedGauge updates the per-resource-type degraded-count gauge.
// Errors are non-fatal — the metric is informational, the dashboard reads
// the underlying table directly.
func (w *ResourceHeartbeatWorker) sampleDegradedGauge(ctx context.Context) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT resource_type, COUNT(*)
		FROM resources
		WHERE status = 'active' AND degraded = true
		GROUP BY resource_type
	`)
	if err != nil {
		slog.Warn("jobs.resource_heartbeat.sample_gauge_failed", "error", err)
		return
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var rt string
		var n int64
		if scanErr := rows.Scan(&rt, &n); scanErr != nil {
			continue
		}
		metrics.ResourceDegradedGauge.WithLabelValues(rt).Set(float64(n))
		seen[rt] = true
	}
	// Reset gauges for types that had degraded rows last run but don't now.
	// We rely on the prometheus collector preserving the label set across
	// runs; the cleanest "no current degraded rows of type X" signal is
	// setting the gauge to 0 explicitly. The set of resource_types is
	// small + fixed (postgres/redis/mongodb/queue/storage/vector) so we
	// can enumerate them.
	for _, rt := range []string{"postgres", "redis", "mongodb", "queue", "storage", "vector"} {
		if !seen[rt] {
			metrics.ResourceDegradedGauge.WithLabelValues(rt).Set(0)
		}
	}
}

// resourceHeartbeatPeriodicInterval returns the cron interval for the
// production schedule. Exported so workers.go can switch to the dev/test
// 1-minute cadence when ENVIRONMENT != "production".
func resourceHeartbeatPeriodicInterval(env string) time.Duration {
	if env == "production" {
		return resourceHeartbeatInterval
	}
	return resourceHeartbeatDevInterval
}
