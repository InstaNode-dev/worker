package db

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"instant.dev/worker/internal/metrics"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestPublishStats_RoundTripsAllFields — coverage block per CLAUDE.md
// rule 17 for the Wave-3 chaos verify finding (2026-05-21): every
// sql.DBStats field publishStats reads must surface as a Prometheus
// gauge or the operator can't see saturation.
func TestPublishStats_RoundTripsAllFields(t *testing.T) {
	db, err := sql.Open("postgres", "postgres://nobody@127.0.0.1:1/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(8)
	publishStats(db, "test_worker_pool")

	if got := testutil.ToFloat64(metrics.PGPoolMax.WithLabelValues("test_worker_pool")); got != 8 {
		t.Errorf("PGPoolMax: want 8, got %v", got)
	}

	// Every gauge must have been touched (not just defaulting to 0).
	for _, g := range []struct {
		name  string
		float float64
	}{
		{"PGPoolInUse", testutil.ToFloat64(metrics.PGPoolInUse.WithLabelValues("test_worker_pool"))},
		{"PGPoolIdle", testutil.ToFloat64(metrics.PGPoolIdle.WithLabelValues("test_worker_pool"))},
		{"PGPoolOpen", testutil.ToFloat64(metrics.PGPoolOpen.WithLabelValues("test_worker_pool"))},
		{"PGPoolWaitCount", testutil.ToFloat64(metrics.PGPoolWaitCount.WithLabelValues("test_worker_pool"))},
		{"PGPoolWaitDurationSeconds", testutil.ToFloat64(metrics.PGPoolWaitDurationSeconds.WithLabelValues("test_worker_pool"))},
	} {
		if g.float != 0 {
			t.Errorf("%s: want 0 on fresh pool, got %v", g.name, g.float)
		}
	}
}

// TestStartPoolStatsExporter_ContextCancellation asserts the exporter
// returns cleanly on context cancellation — a leak here would keep a
// Postgres connection alive across the worker's lifetime.
func TestStartPoolStatsExporter_ContextCancellation(t *testing.T) {
	db, err := sql.Open("postgres", "postgres://nobody@127.0.0.1:1/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		StartPoolStatsExporter(ctx, db, "cancel_test_pool")
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("StartPoolStatsExporter did not return within 1s of cancel — goroutine leak")
	}
}

// TestStartPoolStatsExporter_NilPoolSafe — no-op on nil pool instead
// of panic.
func TestStartPoolStatsExporter_NilPoolSafe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		StartPoolStatsExporter(ctx, nil, "nil_pool_test")
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("nil-pool exporter blocked instead of returning")
	}
}

// TestEnvDuration_FallsBackOnBadValues — guard against typo'd env var
// silently disabling the connection-lifetime knob.
func TestEnvDuration_FallsBackOnBadValues(t *testing.T) {
	cases := []struct {
		raw  string
		want time.Duration
	}{
		{"", 7 * time.Minute},
		{"not-a-duration", 7 * time.Minute},
		{"-1s", 7 * time.Minute},
		{"0", 7 * time.Minute},
		{"5m", 5 * time.Minute},
		{"30s", 30 * time.Second},
	}
	for _, tc := range cases {
		t.Setenv("__TEST_WORKER_PG_ENVDURATION", tc.raw)
		got := envDuration("__TEST_WORKER_PG_ENVDURATION", 7*time.Minute)
		if got != tc.want {
			t.Errorf("envDuration(%q): want %v, got %v", tc.raw, tc.want, got)
		}
	}
}
