// Package circuit provides a small, allocation-free circuit breaker primitive
// for protecting the worker → api internal HTTP boundary.
//
// MECHANICALLY IDENTICAL to api/internal/circuit/circuit.go — kept duplicated
// because we deliberately don't vendor either module into the other. On-call
// learns one state machine; both processes emit the same metric shape with
// the same name labels. Any future change MUST be applied to both copies.
//
// State machine:
//
//	closed → (consecutive failures ≥ threshold) → open
//	open   → (cooldown elapsed)                 → half-open (one trial allowed)
//	half-open → (trial succeeds)                → closed
//	half-open → (trial fails)                   → open (cooldown restarts)
//
// All transitions are observable via the `instant_circuit_breaker_state`
// gauge (0=closed, 1=open, 2=half_open) labelled by `name`, plus counters
// for opens, attempts, and failures.
//
// Concurrency: all state is held in atomic primitives so Allow / Record
// can be called from any number of goroutines without taking a lock.
package circuit

import (
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// State enumerates the breaker's three possible states.
type State int32

const (
	StateClosed   State = 0
	StateOpen     State = 1
	StateHalfOpen State = 2
)

// String returns the lowercased label used in NR / Prometheus metrics.
func (s State) String() string {
	switch s {
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

// ErrOpen is the sentinel returned by callers when the breaker rejects.
var ErrOpen = errors.New("circuit_breaker_open")

var (
	breakerOpens = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_circuit_breaker_opens_total",
		Help: "Circuit breaker open transitions (closed→open or half_open→open)",
	}, []string{"name"})

	breakerAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_circuit_breaker_attempts_total",
		Help: "Calls that hit the circuit breaker (Allow() invocations)",
	}, []string{"name"})

	breakerFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_circuit_breaker_failures_total",
		Help: "Failures recorded against the circuit breaker",
	}, []string{"name"})

	breakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_circuit_breaker_state",
		Help: "Circuit breaker state (0=closed, 1=open, 2=half_open)",
	}, []string{"name"})
)

// Breaker is a single-instance circuit breaker. NOT safe to copy.
type Breaker struct {
	name      string
	threshold int32
	cooldown  time.Duration

	consecutive atomic.Int32
	openUntil   atomic.Int64
	halfOpen    atomic.Bool

	onOpen func()
}

// NewBreaker constructs a Breaker. threshold must be ≥ 1 (clamped);
// cooldown must be > 0 (defaults to 30s). The name MUST match the api
// side's label conventions — short snake_case, used directly as the
// only metric label.
func NewBreaker(name string, threshold int, cooldown time.Duration) *Breaker {
	if threshold < 1 {
		threshold = 1
	}
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	b := &Breaker{
		name:      name,
		threshold: int32(threshold),
		cooldown:  cooldown,
	}
	breakerState.WithLabelValues(name).Set(0)
	return b
}

// WithOnOpen installs an optional callback that fires on every
// transition into the open state. Callback runs synchronously inside
// Record(); keep it cheap.
func (b *Breaker) WithOnOpen(fn func()) *Breaker {
	b.onOpen = fn
	return b
}

// Allow reports whether a call should be attempted right now.
//
// Returns true when the breaker is closed, or when the cooldown
// elapsed and no other goroutine has grabbed the half-open trial slot.
// Returns false when open and within cooldown, or when another caller
// owns the half-open trial.
//
// MUST be paired with Record() — if Allow() returns false, the caller
// MUST NOT call Record().
func (b *Breaker) Allow() bool {
	breakerAttempts.WithLabelValues(b.name).Inc()
	openUntilNs := b.openUntil.Load()
	if openUntilNs == 0 {
		return true
	}
	now := time.Now().UnixNano()
	if now < openUntilNs {
		return false
	}
	if b.halfOpen.CompareAndSwap(false, true) {
		breakerState.WithLabelValues(b.name).Set(float64(StateHalfOpen))
		return true
	}
	return false
}

// Record feeds the call's outcome back into the breaker. nil = success
// (reset counter / close from half-open); non-nil = failure (increment
// counter / re-open from half-open).
func (b *Breaker) Record(err error) {
	if err == nil {
		b.consecutive.Store(0)
		if b.halfOpen.CompareAndSwap(true, false) {
			b.openUntil.Store(0)
			breakerState.WithLabelValues(b.name).Set(float64(StateClosed))
			slog.Info("circuit.closed",
				"name", b.name,
				"reason", "half_open_trial_succeeded",
			)
		}
		return
	}
	breakerFailures.WithLabelValues(b.name).Inc()

	if b.halfOpen.Load() {
		b.halfOpen.Store(false)
		b.consecutive.Store(0)
		b.openUntil.Store(time.Now().Add(b.cooldown).UnixNano())
		breakerOpens.WithLabelValues(b.name).Inc()
		breakerState.WithLabelValues(b.name).Set(float64(StateOpen))
		slog.Warn("circuit.reopened",
			"name", b.name,
			"reason", "half_open_trial_failed",
			"cooldown_seconds", int(b.cooldown.Seconds()),
		)
		if b.onOpen != nil {
			b.onOpen()
		}
		return
	}

	n := b.consecutive.Add(1)
	if n < b.threshold {
		return
	}
	until := time.Now().Add(b.cooldown).UnixNano()
	if b.openUntil.CompareAndSwap(0, until) {
		breakerOpens.WithLabelValues(b.name).Inc()
		breakerState.WithLabelValues(b.name).Set(float64(StateOpen))
		slog.Warn("circuit.opened",
			"name", b.name,
			"reason", "consecutive_failure_threshold_crossed",
			"threshold", b.threshold,
			"cooldown_seconds", int(b.cooldown.Seconds()),
		)
		if b.onOpen != nil {
			b.onOpen()
		}
	}
}

// State returns the breaker's current state.
func (b *Breaker) State() State {
	if b.halfOpen.Load() {
		return StateHalfOpen
	}
	openUntilNs := b.openUntil.Load()
	if openUntilNs == 0 {
		return StateClosed
	}
	if time.Now().UnixNano() < openUntilNs {
		return StateOpen
	}
	return StateOpen
}

// Name returns the metric-label name.
func (b *Breaker) Name() string { return b.name }
