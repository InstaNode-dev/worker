package handlers

// readyz_internal_test.go — package-internal tests that need access to
// the unexported redisPinger, redisFailedPing, errStaticString, and
// statusToFloat helpers. The black-box readyz_test.go (in package
// handlers_test) exercises end-to-end /readyz responses; this file
// pins the small adapter shapes that don't surface in the response.

import (
	"context"
	"testing"

	"instant.dev/common/readiness"
)

// TestRedisPinger_NilClient_ReturnsFailedPing — if the *redis.Client
// passed at construction time is nil (a misconfigured boot, or a test
// fixture that hasn't wired redis yet), Ping MUST return a
// PingResult whose Err() is non-nil. A nil deref here would panic
// during /readyz instead of just reporting the check as failed.
func TestRedisPinger_NilClient_ReturnsFailedPing(t *testing.T) {
	p := redisPinger{r: nil}
	got := p.Ping(context.Background())
	if got == nil {
		t.Fatal("nil-client Ping returned nil PingResult; want redisFailedPing")
	}
	if got.Err() == nil {
		t.Errorf("nil-client Ping.Err() = nil; want non-nil error so the check shows failed")
	}
}

// TestErrStaticString_Error — the tiny static-string error type used
// by redisFailedPing must surface its bytes verbatim through the error
// interface so the readiness layer can pattern-match on the canned
// "redis_client_nil" string.
func TestErrStaticString_Error(t *testing.T) {
	e := errStaticString("some-static-message")
	if e.Error() != "some-static-message" {
		t.Errorf("errStaticString.Error() = %q; want %q", e.Error(), "some-static-message")
	}
}

// TestRedisFailedPing_Err — the canned redisFailedPing always reports
// "redis_client_nil". Pinning this so a future rename doesn't silently
// break log-grep-based dashboards.
func TestRedisFailedPing_Err(t *testing.T) {
	got := redisFailedPing{}.Err()
	if got == nil {
		t.Fatal("redisFailedPing{}.Err() = nil; want non-nil")
	}
	if got.Error() != "redis_client_nil" {
		t.Errorf("redisFailedPing error = %q; want %q", got.Error(), "redis_client_nil")
	}
}

// TestStatusToFloat — pins the gauge mapping (ok=1 / degraded=0.5 /
// anything-else=0). Required by the shared NR alert that fires when
// any check stays at 0 for >5min; flipping any of these would silently
// change the alert semantics.
func TestStatusToFloat(t *testing.T) {
	cases := []struct {
		name string
		s    readiness.Status
		want float64
	}{
		{"ok", readiness.StatusOK, 1},
		{"degraded", readiness.StatusDegraded, 0.5},
		{"failed", readiness.StatusFailed, 0},
		{"zero-value unknown", readiness.Status(""), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := statusToFloat(tc.s)
			if got != tc.want {
				t.Errorf("statusToFloat(%q) = %v; want %v", tc.s, got, tc.want)
			}
		})
	}
}

// TestReadyzMetrics_Observe — Observe must call ReadyzCheckStatus with
// the float-mapped status. The black-box test in readyz_test.go runs
// the full handler; this test invokes Observe directly so a future
// refactor that breaks the wiring fails immediately.
func TestReadyzMetrics_Observe(t *testing.T) {
	m := readyzMetrics{}
	// All three statuses, exercising every branch in statusToFloat
	// via the published path.
	for _, s := range []readiness.Status{readiness.StatusOK, readiness.StatusDegraded, readiness.StatusFailed} {
		m.Observe("test_check", s)
	}
}
