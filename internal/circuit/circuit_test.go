package circuit

// circuit_test.go — mechanically identical to the api side's circuit
// tests so the two copies stay in lock-step. Any new test added here
// MUST also be added in api/internal/circuit/circuit_test.go.

import (
	"errors"
	"sync"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

// TestBreaker_ClosedToOpenTransition — N consecutive failures trip the
// breaker.
func TestBreaker_ClosedToOpenTransition(t *testing.T) {
	b := NewBreaker("worker_test_closed_to_open", 3, 30*time.Second)
	if b.State() != StateClosed {
		t.Fatalf("fresh breaker should be closed, got %s", b.State())
	}
	for i := 0; i < 2; i++ {
		if !b.Allow() {
			t.Fatalf("attempt %d: Allow() should return true (still closed)", i+1)
		}
		b.Record(errBoom)
		if b.State() != StateClosed {
			t.Fatalf("attempt %d: state should still be closed, got %s", i+1, b.State())
		}
	}
	if !b.Allow() {
		t.Fatal("third attempt should still be allowed before recording")
	}
	b.Record(errBoom)
	if b.State() != StateOpen {
		t.Fatalf("after threshold breach state should be open, got %s", b.State())
	}
}

// TestBreaker_ImmediateRejectWhenOpen — open breaker rejects 100 calls
// without invoking the underlying fn.
func TestBreaker_ImmediateRejectWhenOpen(t *testing.T) {
	b := NewBreaker("worker_test_immediate_reject", 1, 30*time.Second)
	if !b.Allow() {
		t.Fatal("initial Allow() should succeed")
	}
	b.Record(errBoom)
	for i := 0; i < 100; i++ {
		if b.Allow() {
			t.Fatalf("call %d: Allow() should return false while open", i+1)
		}
	}
}

// TestBreaker_HalfOpenTrialSucceedsClosesBreaker — recovery happy path.
func TestBreaker_HalfOpenTrialSucceedsClosesBreaker(t *testing.T) {
	b := NewBreaker("worker_test_half_open_success", 1, 10*time.Millisecond)
	_ = b.Allow()
	b.Record(errBoom)
	if b.State() != StateOpen {
		t.Fatalf("expected open, got %s", b.State())
	}
	time.Sleep(15 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("first Allow() after cooldown should succeed (half-open trial)")
	}
	if b.Allow() {
		t.Fatal("second concurrent Allow() should be rejected while trial in flight")
	}
	b.Record(nil)
	if b.State() != StateClosed {
		t.Fatalf("after successful trial state should be closed, got %s", b.State())
	}
}

// TestBreaker_HalfOpenTrialFailsReopens — recovery sad path.
func TestBreaker_HalfOpenTrialFailsReopens(t *testing.T) {
	b := NewBreaker("worker_test_half_open_fail", 1, 10*time.Millisecond)
	_ = b.Allow()
	b.Record(errBoom)
	time.Sleep(15 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("trial should be allowed after cooldown")
	}
	b.Record(errBoom)
	if b.State() != StateOpen {
		t.Fatalf("failed trial should re-open the breaker, got %s", b.State())
	}
}

// TestBreaker_SuccessResetsConsecutiveCounter — flapping success
// breaks the consecutive chain.
func TestBreaker_SuccessResetsConsecutiveCounter(t *testing.T) {
	b := NewBreaker("worker_test_success_resets", 3, 30*time.Second)
	for i := 0; i < 2; i++ {
		_ = b.Allow()
		b.Record(errBoom)
	}
	_ = b.Allow()
	b.Record(nil)
	for i := 0; i < 2; i++ {
		_ = b.Allow()
		b.Record(errBoom)
	}
	if b.State() != StateClosed {
		t.Fatalf("state should still be closed after reset, got %s", b.State())
	}
}

// TestBreaker_ConcurrentCallersOnlyOneTrial — half-open admits exactly
// one caller under load.
func TestBreaker_ConcurrentCallersOnlyOneTrial(t *testing.T) {
	b := NewBreaker("worker_test_concurrent_trial", 1, 10*time.Millisecond)
	_ = b.Allow()
	b.Record(errBoom)
	time.Sleep(15 * time.Millisecond)

	const n = 50
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		admitted int
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if b.Allow() {
				mu.Lock()
				admitted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if admitted != 1 {
		t.Fatalf("exactly one goroutine should win the half-open trial, got %d", admitted)
	}
}

// TestBreaker_OnOpenCallback — optional callback fires on each open.
func TestBreaker_OnOpenCallback(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	b := NewBreaker("worker_test_on_open_cb", 1, 10*time.Millisecond).WithOnOpen(func() {
		mu.Lock()
		defer mu.Unlock()
		calls++
	})
	_ = b.Allow()
	b.Record(errBoom)
	time.Sleep(15 * time.Millisecond)
	_ = b.Allow()
	b.Record(errBoom)
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Fatalf("expected onOpen called twice, got %d", calls)
	}
}

// TestBreaker_NewBreakerClampsInvalidArgs — threshold < 1 is clamped to 1
// and a non-positive cooldown defaults to 30s. Exercises the two guard
// branches in NewBreaker (otherwise only the happy path is hit).
func TestBreaker_NewBreakerClampsInvalidArgs(t *testing.T) {
	// threshold 0 → clamped to 1: a single failure must trip the breaker.
	b := NewBreaker("worker_test_clamp_threshold", 0, 10*time.Millisecond)
	if !b.Allow() {
		t.Fatal("fresh breaker should allow")
	}
	b.Record(errBoom)
	if b.State() != StateOpen {
		t.Fatalf("threshold should clamp to 1 (single failure opens), got %s", b.State())
	}

	// cooldown <= 0 → defaults to 30s. We can't wait 30s, but we can prove
	// the breaker is still open well past a tiny sleep (a 0 cooldown would
	// have re-admitted immediately).
	b2 := NewBreaker("worker_test_clamp_cooldown", 1, 0)
	_ = b2.Allow()
	b2.Record(errBoom)
	time.Sleep(5 * time.Millisecond)
	if b2.Allow() {
		t.Fatal("cooldown should default to 30s; breaker must still reject after 5ms")
	}
	if b2.State() != StateOpen {
		t.Fatalf("expected open within default cooldown, got %s", b2.State())
	}
}

// TestBreaker_StateOpenWithinCooldown — when the breaker is open and the
// cooldown has NOT elapsed, State() takes the `now < openUntilNs` branch
// and reports open. Distinct from the half-open trial path.
func TestBreaker_StateOpenWithinCooldown(t *testing.T) {
	b := NewBreaker("worker_test_state_within_cooldown", 1, time.Hour)
	_ = b.Allow()
	b.Record(errBoom)
	// halfOpen is false, openUntil is set, now < openUntil → first return.
	if got := b.State(); got != StateOpen {
		t.Fatalf("breaker within cooldown should report open, got %s", got)
	}
}

// TestBreaker_StateOpenAfterCooldownBeforeTrial — once the cooldown has
// elapsed but no caller has claimed the half-open trial yet, State() falls
// through past the `now < openUntilNs` check and still reports open (the
// final return). This exercises the trailing branch of State().
func TestBreaker_StateOpenAfterCooldownBeforeTrial(t *testing.T) {
	b := NewBreaker("worker_test_state_after_cooldown", 1, 10*time.Millisecond)
	_ = b.Allow()
	b.Record(errBoom)
	time.Sleep(15 * time.Millisecond)
	// Cooldown elapsed, but we have NOT called Allow() — so halfOpen is
	// still false and openUntil is still in the past.
	if got := b.State(); got != StateOpen {
		t.Fatalf("breaker after cooldown but before trial should report open, got %s", got)
	}
}

// TestBreaker_StateHalfOpenReported — once a caller claims the half-open
// trial slot (Allow() after cooldown), State() takes the leading
// `halfOpen.Load()` branch and reports half_open.
func TestBreaker_StateHalfOpenReported(t *testing.T) {
	b := NewBreaker("worker_test_state_half_open", 1, 10*time.Millisecond)
	_ = b.Allow()
	b.Record(errBoom)
	time.Sleep(15 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("first Allow() after cooldown should claim the half-open trial")
	}
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("breaker mid-trial should report half_open, got %s", got)
	}
}

// TestBreaker_Name — the metric-label accessor returns the configured name.
func TestBreaker_Name(t *testing.T) {
	b := NewBreaker("worker_test_name_accessor", 1, time.Second)
	if got := b.Name(); got != "worker_test_name_accessor" {
		t.Fatalf("Name() = %q, want %q", got, "worker_test_name_accessor")
	}
}

// TestBreaker_StateStringValues — NR runbook references these exact
// strings.
func TestBreaker_StateStringValues(t *testing.T) {
	cases := []struct {
		s    State
		want string
	}{
		{StateClosed, "closed"},
		{StateOpen, "open"},
		{StateHalfOpen, "half_open"},
	}
	for _, c := range cases {
		if c.s.String() != c.want {
			t.Errorf("State(%d).String() = %q, want %q", c.s, c.s.String(), c.want)
		}
	}
}
