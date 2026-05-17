package jobs

// safego.go — shared panic-recovery wrapper for fire-and-forget goroutines.
//
// # Why this exists (P1-B, bug-hunt 2026-05-17 round 2)
//
// A bare `go func(){ … }()` that panics takes the WHOLE worker process down:
// Go has no per-goroutine panic boundary, so an unrecovered panic in any
// goroutine calls os.Exit-equivalent runtime teardown. In the worker that
// means every in-flight River job is abandoned and k8s restarts the pod.
//
// The worker has several fire-and-forget goroutines (audit-emit, pg_dump
// pipes, S3 list fan-out, probe fan-out, heartbeat fan-out). Any of them
// dereferencing a nil, indexing out of range, or hitting a library panic
// would crash the pod. SafeGo gives every such goroutine a recover()
// boundary: the panic is logged with its stack, counted in a metric, and
// the goroutine ends cleanly while the rest of the worker keeps running.
//
// # Usage
//
//	jobs.SafeGo("deployment_expirer.audit", func() { … })
//
// The `site` string is a stable identifier (job + role) used both as the
// slog field and the metric label, so an operator can pinpoint the failing
// goroutine from the NR alert without reading a stack trace.
//
// Helpers that build their own goroutine but need to keep their existing
// closure signature (e.g. WaitGroup-tracked workers) can call `Recover`
// directly as a `defer`.

import (
	"log/slog"
	"runtime/debug"

	"instant.dev/worker/internal/metrics"
)

// Recover is the deferred panic boundary. Call it as the FIRST deferred
// statement inside any goroutine body:
//
//	go func() {
//		defer jobs.Recover("some_job.some_role")
//		…
//	}()
//
// On a panic it logs the recovered value + full stack at error level and
// increments metrics.GoroutinePanicsRecovered{site=...}. On the normal
// (no-panic) path it is a cheap no-op.
func Recover(site string) {
	if r := recover(); r != nil {
		LogRecoveredPanic(site, r)
	}
}

// LogRecoveredPanic records an already-recovered panic value: it logs the
// value + current stack and increments the recovery metric. Use this when a
// goroutine needs custom cleanup on the panic path (e.g. closing an io.Pipe
// so the reader sees EOF) and therefore calls recover() itself rather than
// deferring Recover — pass the recovered value here so the panic is still
// observed in logs + metrics.
func LogRecoveredPanic(site string, r any) {
	metrics.GoroutinePanicsRecovered.WithLabelValues(site).Inc()
	slog.Error("jobs.goroutine.panic_recovered",
		"site", site,
		"panic", r,
		"stack", string(debug.Stack()),
	)
}

// SafeGo launches fn in a new goroutine guarded by Recover(site). It is the
// fire-and-forget primitive: a panic in fn is contained, logged, and
// counted instead of crashing the worker process.
func SafeGo(site string, fn func()) {
	go func() {
		defer Recover(site)
		fn()
	}()
}
