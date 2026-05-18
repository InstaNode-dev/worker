package jobs

// heartbeat_prober_wiring_test.go — W5 (P1-W5-09) regression guard.
//
// resource_heartbeat is the worker's degraded-detection sweep. Before W5,
// StartWorkers constructed it with NewResourceHeartbeatWorker(db, nil) → the
// constructor's nil-fallback substituted NoopProber, whose every probe
// returns ProbeSkip. The whole sweep was therefore a permanent no-op in
// production: no resource was ever flagged degraded, the dashboard banner
// never lit, and a customer whose DB pod was evicted got no signal.
//
// The fix wires the real prober (NewRealProber). These tests pin that the
// real prober is genuinely non-Noop and that the heartbeat worker keeps it.

import (
	"reflect"
	"testing"

	"instant.dev/worker/internal/config"
)

// TestResourceHeartbeat_RealProberIsNotNoop asserts NewRealProber returns a
// prober that is NOT the NoopProber type. If a future change reverts the
// wiring to a Noop the heartbeat sweep silently dies again.
func TestResourceHeartbeat_RealProberIsNotNoop(t *testing.T) {
	p := NewRealProber(&config.Config{})
	if p == nil {
		t.Fatal("NewRealProber returned nil — the heartbeat worker would fall back to NoopProber")
	}
	if _, isNoop := p.(NoopProber); isNoop {
		t.Errorf("NewRealProber returned a NoopProber (%T) — degraded-detection would be a permanent no-op (P1-W5-09)", p)
	}
	if reflect.TypeOf(p).String() == reflect.TypeOf(NoopProber{}).String() {
		t.Errorf("NewRealProber's concrete type matches NoopProber — the prober must be the real per-resource-type prober")
	}
}

// TestResourceHeartbeat_WorkerKeepsRealProber pins that constructing the
// heartbeat worker the way StartWorkers does (with NewRealProber, not nil)
// leaves a non-Noop prober on the worker. The constructor's nil-fallback to
// NoopProber is correct behaviour — the bug was the *call site* passing nil.
// This test exercises the post-W5 call site.
func TestResourceHeartbeat_WorkerKeepsRealProber(t *testing.T) {
	w := NewResourceHeartbeatWorker(nil, NewRealProber(&config.Config{}))
	if w.prober == nil {
		t.Fatal("ResourceHeartbeatWorker.prober is nil")
	}
	if _, isNoop := w.prober.(NoopProber); isNoop {
		t.Errorf("ResourceHeartbeatWorker.prober is a NoopProber — W5 wiring regressed: StartWorkers must pass NewRealProber(cfg), not nil")
	}
}
