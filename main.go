package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"instant.dev/common/buildinfo"
	"instant.dev/common/logctx"
	commonplans "instant.dev/common/plans"
	"instant.dev/worker/internal/config"
	"instant.dev/worker/internal/db"
	"instant.dev/worker/internal/handlers"
	"instant.dev/worker/internal/jobs"
	"instant.dev/worker/internal/migrations"
	"instant.dev/worker/internal/obs"
	"instant.dev/worker/internal/provisioner"
	"instant.dev/worker/internal/telemetry"
)

// setupLogger builds the worker's default structured JSON logger wrapped in
// logctx so every line carries service + commit_id + (when present) tid /
// trace_id / team_id. Extracted from main() so the wiring is unit-testable
// without driving the full boot sequence.
func setupLogger(w io.Writer) *slog.Logger {
	base := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	})
	return slog.New(logctx.NewHandler("worker", base))
}

// resolvePlansPath applies the "plans.yaml" default when PLANS_PATH is empty.
// Extracted so the fallback branch is testable without a config.Load().
func resolvePlansPath(plansPath string) string {
	if plansPath == "" {
		return "plans.yaml"
	}
	return plansPath
}

// loadPlanRegistry loads the plan registry from path, falling back to the
// built-in defaults (with a WARN) when the file can't be read. Extracted so
// both the happy and fallback paths are unit-testable.
func loadPlanRegistry(path string) *commonplans.Registry {
	reg, err := commonplans.Load(path)
	if err != nil {
		slog.Warn("worker.plans_load_failed_using_defaults", "error", err, "path", path)
		return commonplans.Default()
	}
	return reg
}

// newHealthzHandler builds the shallow liveness handler. It reads the
// migration state through the injected reader and emits the uniform
// cross-fleet /healthz JSON shape (B14-F9). Extracted from main() so the
// JSON shape — which monitoring depends on — is pinned by a unit test
// without booting River / Postgres / Redis.
func newHealthzHandler(reader *migrations.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		mstate := reader.Get(r.Context())
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w,
			`{"ok":true,"service":"instant-worker","commit_id":%q,"build_time":%q,"version":%q,"migration_version":%q,"migration_count":%d,"migration_status":%q}`,
			buildinfo.GitSHA, buildinfo.BuildTime, buildinfo.Version,
			mstate.Filename, mstate.Count, mstate.Status,
		)
	}
}

// buildMux assembles the worker's HTTP surface: the shallow /healthz
// liveness probe, the deep /readyz readiness probe, and the Prometheus
// /metrics endpoint. Extracted from main() so the routing is unit-testable
// (each route returns the expected handler) without booting River.
func buildMux(healthz http.Handler, readyz http.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/healthz", healthz)
	mux.Handle("/readyz", readyz)
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

// serveLiveness runs srv.ListenAndServe and logs any non-ErrServerClosed
// failure. ErrServerClosed is the expected outcome on graceful shutdown and
// is not logged as an error. Extracted from the SafeGo closure so the
// error-log branch is synchronously unit-testable (a goroutine's coverage
// counter is racy to observe).
func serveLiveness(srv *http.Server) {
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("worker.liveness_server_failed", "error", err)
	}
}

// startLivenessServer launches serveLiveness under jobs.SafeGo so a panic in
// the server (or a handler) is recovered + counted instead of crashing the
// worker.
func startLivenessServer(srv *http.Server) {
	jobs.SafeGo("main.liveness_server", func() { serveLiveness(srv) })
}

// shutdownLivenessServer gracefully shuts srv down within a bounded window so
// k8s SIGTERM handling drains in-flight probes instead of cutting connections.
func shutdownLivenessServer(srv *http.Server, timeout time.Duration) {
	shutCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
}

// awaitShutdown blocks until ctx is cancelled (SIGINT / SIGTERM) and logs the
// shutdown line. Extracted so the happy-shutdown path is drivable in a test
// with a pre-cancelled context.
func awaitShutdown(ctx context.Context) {
	<-ctx.Done()
	slog.Info("worker.shutdown")
}

// workerSet is the minimal surface main() needs from jobs.StartWorkers:
// the "did River start" signal, the readiness adapter, and graceful Stop.
// Kept as an interface so run() is drivable in a test with a fake that
// reports started/not-started without spinning up a real River client + DB.
type workerSet interface {
	Started() bool
	Stop()
}

// deps bundles the infrastructure constructors run() depends on. Production
// wiring lives in productionDeps(); tests inject fakes (sqlmock DB, miniredis,
// a fake workerSet) so the full boot/shutdown path is exercised without real
// Postgres / Redis / gRPC / River.
type deps struct {
	// loadConfig returns the worker config (panics on missing required env
	// in production via config.Load).
	loadConfig func() *config.Config
	// connectPostgres / connectRedis dial the platform stores.
	connectPostgres func(url string) *sql.DB
	connectRedis    func(url string) *redis.Client
	// startPoolStats begins the pool-saturation exporter (no-op in tests).
	startPoolStats func(ctx context.Context, database *sql.DB, name string)
	// startWorkers boots the River queue and periodic jobs, returning a
	// workerSet whose Started() reports queue health.
	startWorkers func(ctx context.Context, database *sql.DB, rdb *redis.Client, cfg *config.Config) workerSet
	// newReadyzHandler builds the /readyz handler.
	newReadyzHandler func(cfg *config.Config, database *sql.DB, rdb *redis.Client, ws workerSet) http.Handler
	// newMigrationReader builds the /healthz migration-state reader.
	newMigrationReader func(database *sql.DB) *migrations.Reader
	// listenAddr is the liveness server bind address (":8091" in prod, an
	// ephemeral "127.0.0.1:0" in tests).
	listenAddr string
}

// prodStartPoolStats spawns the pool-saturation exporter goroutine.
func prodStartPoolStats(ctx context.Context, database *sql.DB, name string) {
	go db.StartPoolStatsExporter(ctx, database, name)
}

// connectProvisioner dials the provisioner gRPC service when an address is
// configured, registering a context.AfterFunc to close the connection on
// shutdown. When the address is empty (PROVISIONER_ADDR unset) it returns a
// nil client and UpdateStorageBytesWorker becomes a no-op. The empty-address
// branch is unit-testable; the dial branch needs a real gRPC target.
func connectProvisioner(ctx context.Context, cfg *config.Config) *provisioner.Client {
	if cfg.ProvisionerAddr == "" {
		slog.Info("worker.provisioner_not_configured", "note", "PROVISIONER_ADDR not set — UpdateStorageBytesWorker will be a no-op")
		return nil
	}
	pc, conn, err := provisioner.NewClient(cfg.ProvisionerAddr, cfg.ProvisionerSecret)
	if err != nil {
		slog.Error("worker.provisioner_connect_failed", "error", err)
		osExit(1)
		return nil
	}
	context.AfterFunc(ctx, func() { _ = conn.Close() })
	slog.Info("worker.provisioner_connected", "addr", cfg.ProvisionerAddr)
	return pc
}

// osExit is indirected so the connectProvisioner error path is unit-testable
// without terminating the test process. Production points it at os.Exit.
var osExit = os.Exit

// deployK8sInitOK logs the outcome of the deploy-status k8s client init and
// reports whether the clients are usable. Fails open: a non-nil err warn-logs
// and returns false so the caller nils the clients (the DeployStatusReconciler
// then warn-logs each tick while every other periodic job keeps running).
// Extracted so both the success and failure log branches are unit-testable —
// the success branch is otherwise unreachable in CI (no kubeconfig).
func deployK8sInitOK(err error) bool {
	if err != nil {
		slog.Warn("worker.deploy_status_k8s_client_init_failed",
			"error", err,
			"note", "DeployStatusReconciler will log warnings each tick; other periodic jobs unaffected")
		return false
	}
	slog.Info("worker.deploy_status_k8s_client_ready")
	return true
}

// newDeployK8sClients is indirected so prodStartWorkers' nil-out branch (the
// fail-open path when the k8s client can't be built) is unit-testable: a test
// swaps in a constructor that returns an error. Production points it at
// jobs.NewK8sDeployStatusClientWithAutopsy.
var newDeployK8sClients = jobs.NewK8sDeployStatusClientWithAutopsy

// prodStartWorkers boots the real River queue + periodic jobs. nrApp is
// captured so the worker telemetry threads through.
func prodStartWorkers(nrApp *newrelic.Application) func(ctx context.Context, database *sql.DB, rdb *redis.Client, cfg *config.Config) workerSet {
	return func(ctx context.Context, database *sql.DB, rdb *redis.Client, cfg *config.Config) workerSet {
		planRegistry := loadPlanRegistry(resolvePlansPath(cfg.PlansPath))

		// Build the k8s client used by DeployStatusReconciler and the
		// failure-autopsy capturer. Fails open: if neither in-cluster config
		// nor kubeconfig is reachable (CI, docker-compose, bare-metal dev),
		// pass nil and the reconciler warn-logs each tick while every other
		// periodic job keeps running.
		deployStatusK8s, deployAutopsyK8s, k8sErr := newDeployK8sClients()
		if !deployK8sInitOK(k8sErr) {
			deployStatusK8s = nil
			deployAutopsyK8s = nil
		}

		// BackupPlanRegistry adapter — CustomerBackupRunner reads
		// tier→retention_days from plans.yaml; nil falls back to 7 days.
		backupPlans := jobs.NewBackupPlanRegistry(planRegistry)

		provClient := connectProvisioner(ctx, cfg)

		return jobs.StartWorkers(ctx, database, rdb, cfg, provClient, planRegistry, backupPlans, deployStatusK8s, deployAutopsyK8s, nrApp)
	}
}

// prodNewReadyzHandler adapts handlers.NewReadyzHandler to the deps signature.
func prodNewReadyzHandler(cfg *config.Config, database *sql.DB, rdb *redis.Client, ws workerSet) http.Handler {
	return http.HandlerFunc(handlers.NewReadyzHandler(cfg, database, rdb, ws).Get)
}

// prodNewMigrationReader builds the /healthz migration-state reader with the
// default 60s cache TTL.
func prodNewMigrationReader(database *sql.DB) *migrations.Reader {
	return migrations.NewReader(database, 0, nil)
}

// productionDeps wires the real worker infrastructure. Each field references
// a named function above so productionDeps itself is plain assignment (fully
// covered by TestProductionDeps); the heavy infra logic lives in the named
// functions where the unit-testable branches (e.g. connectProvisioner's
// empty-address path) can be exercised directly.
func productionDeps(nrApp *newrelic.Application) deps {
	return deps{
		loadConfig:         config.Load,
		connectPostgres:    db.ConnectPostgres,
		connectRedis:       db.ConnectRedis,
		startPoolStats:     prodStartPoolStats,
		startWorkers:       prodStartWorkers(nrApp),
		newReadyzHandler:   prodNewReadyzHandler,
		newMigrationReader: prodNewMigrationReader,
		listenAddr:         ":8091",
	}
}

// run is the testable body of the worker. It boots config + stores, starts
// the River workers, serves the liveness/readiness/metrics HTTP surface, and
// blocks until ctx is cancelled. Returns a process exit code: 0 on clean
// shutdown, 1 when River failed to start (so k8s restarts the pod). main()
// is a thin os.Exit wrapper around this.
func run(ctx context.Context, d deps) int {
	cfg := d.loadConfig() // panics on missing required env vars in production

	database := d.connectPostgres(cfg.DatabaseURL)
	defer database.Close()

	// Pool-saturation observability (Wave-3 chaos verify, 2026-05-21):
	// pushes *sql.DB.Stats onto instant_pg_pool_* gauges so operators can
	// localize worker pool saturation independently from api.
	poolStatsCtx, poolStatsCancel := context.WithCancel(ctx)
	defer poolStatsCancel()
	d.startPoolStats(poolStatsCtx, database, "platform_db")

	rdb := d.connectRedis(cfg.RedisURL)
	defer rdb.Close()

	workers := d.startWorkers(ctx, database, rdb, cfg)
	defer workers.Stop()

	// Exit immediately if River failed to start so Kubernetes restarts the
	// pod. A process that is alive but has no active River client is worse
	// than a crash: k8s thinks the pod is healthy while no jobs run.
	if !workers.Started() {
		slog.Error("worker.river_start_failed — exiting so k8s restarts the pod")
		return 1
	}

	// Liveness HTTP server. /healthz is shallow (process + River up);
	// /readyz is the deep readiness probe; /metrics is the Prometheus
	// scrape. B14-F9: /healthz emits the uniform cross-fleet shape with
	// migration_version / migration_count / migration_status.
	migrationReader := d.newMigrationReader(database)
	readyzH := d.newReadyzHandler(cfg, database, rdb, workers)
	mux := buildMux(newHealthzHandler(migrationReader), readyzH)
	srv := &http.Server{Addr: d.listenAddr, Handler: mux}
	startLivenessServer(srv)
	defer shutdownLivenessServer(srv, 5*time.Second)

	slog.Info("worker.started",
		"environment", cfg.Environment,
		"liveness_addr", d.listenAddr,
		"commit_id", buildinfo.GitSHA,
		"build_time", buildinfo.BuildTime,
		"version", buildinfo.Version,
	)
	awaitShutdown(ctx)
	return 0
}

// setupTelemetry initialises the New Relic Go agent and the OTel tracer.
// Both fail open on a missing license / endpoint so local dev and CI (which
// never get a real key) still boot. It returns the NR application (may be
// nil) plus a cleanup func that shuts both down. Extracted from main() so the
// fail-open wiring is unit-testable without driving the full boot.
func setupTelemetry() (*newrelic.Application, func()) {
	nrApp, _ := obs.InitNewRelic()
	shutdownTracer := telemetry.InitTracer("instant-worker", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	return nrApp, func() { telemetryCleanup(nrApp, shutdownTracer) }
}

// telemetryCleanup shuts down the NR agent (when non-nil) and the OTel tracer,
// logging a tracer-shutdown error. Extracted from setupTelemetry's closure so
// both branches (nrApp non-nil, tracer shutdown error) are unit-testable with
// injected values.
func telemetryCleanup(nrApp *newrelic.Application, shutdownTracer func(context.Context) error) {
	if nrApp != nil {
		nrApp.Shutdown(5 * time.Second)
	}
	if err := shutdownTracer(context.Background()); err != nil {
		slog.Error("telemetry.shutdown_failed", "error", err)
	}
}

// realMain is the testable entrypoint body: it sets up logging + telemetry,
// runs the worker against the supplied context + deps, and returns the
// process exit code after telemetry cleanup. main() is a thin os.Exit
// wrapper that builds the signal context and production deps.
func realMain(ctx context.Context, w io.Writer, makeDeps func(*newrelic.Application) deps) int {
	// Structured JSON logging — wrapped in logctx so every line carries
	// service + commit_id + (when present) tid / trace_id / team_id.
	slog.SetDefault(setupLogger(w))

	nrApp, cleanup := setupTelemetry()
	defer cleanup()

	return run(ctx, makeDeps(nrApp))
}

// newSignalContext returns a context cancelled on SIGINT / SIGTERM plus its
// stop func. Extracted from main() so the signal wiring is unit-testable.
func newSignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// main's collaborators are indirected through package vars so the wrapper
// itself is unit-testable: a test swaps in a cancelled signal context, a fake
// realMain returning a known code, and a capturing exit, then calls main().
// Production points each at the real implementation.
var (
	signalCtxFn = newSignalContext
	realMainFn  = realMain
)

func main() {
	ctx, stop := signalCtxFn()
	defer stop()

	osExit(realMainFn(ctx, os.Stdout, productionDeps))
}
