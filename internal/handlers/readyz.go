// Package handlers implements the worker's HTTP sidecar handlers.
//
// The worker is otherwise pure-River (no inbound HTTP surface); the
// sidecar on :8091 exposes /healthz + /metrics already, and this
// package adds /readyz with deep, component-by-component checks.
//
// Why a separate handler instead of stamping every check into the
// existing /healthz: /healthz is the k8s livenessProbe — its job is
// "should this pod be restarted". Adding Brevo / River checks there
// would mean a Brevo outage SIGKILLs every worker pod, which would
// cycle the River queue and re-deliver every in-flight job. /readyz
// is the readinessProbe instead — degraded pods stay running, only
// get pulled out of the (worker has no Service endpoints today, but
// the wire shape stays consistent across services).
package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"instant.dev/common/readiness"
	"instant.dev/worker/internal/config"
	"instant.dev/worker/internal/metrics"
)

// RiverStateProvider is the minimal interface needed to surface the
// River queue's "did it start" signal on /readyz. The worker's
// jobs.Workers type already implements Started() bool — the wire is a
// 1-line adapter in main.go.
//
// Kept as an interface (not a *jobs.Workers concrete) so tests can fake
// "river is unhealthy" without spinning up a real River client + DB.
type RiverStateProvider interface {
	Started() bool
}

// ReadyzHandler bundles the worker's readiness dependencies. The
// runner is constructed eagerly so the first probe arrival is fast.
type ReadyzHandler struct {
	runner *readiness.Runner
	cfg    *config.Config
	db     *sql.DB
	rdb    *redis.Client
	river  RiverStateProvider
	http   *http.Client
}

// NewReadyzHandler wires the runner. Pass the worker's db / rdb / River
// reference. If river is nil (e.g. a test that hasn't booted the
// queue), the river check still surfaces but always reports failed —
// the operator sees the configuration gap on every probe.
func NewReadyzHandler(cfg *config.Config, db *sql.DB, rdb *redis.Client, river RiverStateProvider) *ReadyzHandler {
	h := &ReadyzHandler{
		cfg:   cfg,
		db:    db,
		rdb:   rdb,
		river: river,
		http:  &http.Client{Timeout: 5 * time.Second},
	}
	h.runner = readiness.NewRunner(readiness.Config{
		Service:        "instant-worker",
		CacheTTL:       10 * time.Second,
		OverallTimeout: 3 * time.Second,
		Metrics:        readyzMetrics{},
	}, h.buildChecks())
	return h
}

// buildChecks is the worker's canonical readiness registry.
//
//   - platform_db (CRITICAL): the worker can't process a single job
//     without it. If down, pull from rotation.
//   - redis (non-critical): used for rate-limit and dedup. Fails open.
//   - brevo (non-critical): the worker is the main email emitter; a
//     dead Brevo means lifecycle emails stall. Surface as degraded.
//   - river (CRITICAL): if the queue didn't start, the pod's main loop
//     is doing nothing. Pull from rotation.
func (h *ReadyzHandler) buildChecks() []readiness.Check {
	checks := []readiness.Check{
		{
			Name:     "platform_db",
			Critical: true,
			Fn:       readiness.PingDB(h.db, 2*time.Second),
		},
		{
			Name:     "redis",
			Critical: false,
			Fn:       readiness.PingRedis(redisPinger{h.rdb}, time.Second),
		},
		{
			Name:     "river",
			Critical: true,
			Fn:       h.riverCheck(),
		},
	}
	if h.cfg.BrevoAPIKey != "" {
		checks = append(checks, readiness.Check{
			Name:     "brevo",
			Critical: false,
			Fn: readiness.HTTPHeadCheck(h.http, "GET",
				"https://api.brevo.com/v3/account",
				map[string]string{"api-key": h.cfg.BrevoAPIKey, "accept": "application/json"},
				3*time.Second),
		})
	}
	return checks
}

// riverCheck wraps the RiverStateProvider into the readiness contract.
// Started() == false means the worker booted but River failed to start
// — the pod is alive but doing nothing. /healthz already exits the
// process when this happens at boot (main.go calls os.Exit(1)), so in
// practice this only surfaces if a future refactor changes that
// behavior. The check stays here as a contract belt.
func (h *ReadyzHandler) riverCheck() readiness.CheckFunc {
	return func(ctx context.Context) readiness.CheckResult {
		if h.river == nil {
			return readiness.CheckResult{Status: readiness.StatusFailed, LastError: "river_not_wired"}
		}
		if !h.river.Started() {
			return readiness.CheckResult{Status: readiness.StatusFailed, LastError: "river_not_started"}
		}
		return readiness.CheckResult{Status: readiness.StatusOK}
	}
}

// Get is the net/http handler the worker's main.go mounts on
// :8091/readyz. The worker doesn't use Fiber so we stay vanilla
// net/http here.
func (h *ReadyzHandler) Get(w http.ResponseWriter, r *http.Request) {
	resp, code := h.runner.Run(r.Context())
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

// redisPinger adapts *redis.Client to readiness.Pinger so the common/
// package doesn't pull in go-redis.
type redisPinger struct{ r *redis.Client }

func (p redisPinger) Ping(ctx context.Context) readiness.PingResult {
	if p.r == nil {
		return redisFailedPing{}
	}
	return p.r.Ping(ctx)
}

type redisFailedPing struct{}

func (redisFailedPing) Err() error { return errStaticString("redis_client_nil") }

type errStaticString string

func (e errStaticString) Error() string { return string(e) }

// readyzMetrics is the Prometheus hook. Worker side stamps
// service="instant-worker" via metrics.ReadyzCheckStatus.
type readyzMetrics struct{}

func (readyzMetrics) Observe(name string, status readiness.Status) {
	metrics.ReadyzCheckStatus(name, statusToFloat(status))
}

func statusToFloat(s readiness.Status) float64 {
	switch s {
	case readiness.StatusOK:
		return 1
	case readiness.StatusDegraded:
		return 0.5
	default:
		return 0
	}
}

// Compile-time interface conformance check.
var _ readiness.PingResult = redisFailedPing{}
