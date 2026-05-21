package db

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"instant.dev/worker/internal/metrics"
)

// StartPoolStatsExporter samples *sql.DB.Stats every 5s and re-publishes
// the relevant numbers onto the `instant_pg_pool_*` Prometheus gauges
// (in metrics/metrics.go). It blocks until ctx is cancelled.
//
// Wave-3 chaos verify (2026-05-21): worker's event_email_forwarder
// failed with "remaining connection slots are reserved for
// non-replication superuser connections" during a 50-concurrent api
// /db/new burst, because the shared DO Managed Postgres pool was
// exhausted. Without this exporter the saturation is invisible in
// /metrics — operators had to infer it after the fact. The 5-second
// interval is fast enough to see a burst saturate + resolve, slow
// enough that the Stats() Mutex lock is negligible.
func StartPoolStatsExporter(ctx context.Context, pool *sql.DB, label string) {
	if pool == nil {
		slog.Warn("worker.db.pool_metrics.skip — nil pool", "label", label)
		return
	}

	const interval = 5 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Info("worker.db.pool_metrics.exporter_started",
		"label", label,
		"interval", interval.String(),
	)

	// Emit one sample immediately so the gauge has a value before the
	// first scrape window.
	publishStats(pool, label)

	for {
		select {
		case <-ctx.Done():
			slog.Info("worker.db.pool_metrics.exporter_stopped", "label", label)
			return
		case <-ticker.C:
			publishStats(pool, label)
		}
	}
}

// publishStats reads pool.Stats() and updates the metrics gauges.
// Exported as a free function so tests can call it directly.
func publishStats(pool *sql.DB, label string) {
	s := pool.Stats()
	metrics.PGPoolInUse.WithLabelValues(label).Set(float64(s.InUse))
	metrics.PGPoolIdle.WithLabelValues(label).Set(float64(s.Idle))
	metrics.PGPoolOpen.WithLabelValues(label).Set(float64(s.OpenConnections))
	metrics.PGPoolMax.WithLabelValues(label).Set(float64(s.MaxOpenConnections))
	metrics.PGPoolWaitCount.WithLabelValues(label).Set(float64(s.WaitCount))
	metrics.PGPoolWaitDurationSeconds.WithLabelValues(label).Set(s.WaitDuration.Seconds())
}
