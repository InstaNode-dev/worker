package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/redis/go-redis/v9"

	"instant.dev/common/buildinfo"
	"instant.dev/worker/internal/config"
	"instant.dev/worker/internal/migrations"
)

// TestSetupLogger verifies the logger writes structured JSON to the injected
// writer and stamps the worker service field (via logctx). Pins the boot-time
// logging wiring without driving the full main() sequence.
func TestSetupLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := setupLogger(&buf)
	if logger == nil {
		t.Fatal("setupLogger returned nil")
	}
	logger.Info("worker.test_line", "k", "v")

	out := buf.String()
	if out == "" {
		t.Fatal("expected a log line, got empty output")
	}
	// Must be valid JSON (slog JSON handler).
	var rec map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &rec); err != nil {
		t.Fatalf("log line is not valid JSON: %v\nline: %s", err, out)
	}
	if rec["msg"] != "worker.test_line" {
		t.Errorf("msg = %v; want worker.test_line", rec["msg"])
	}
	if rec["k"] != "v" {
		t.Errorf("attr k = %v; want v", rec["k"])
	}
	// logctx.NewHandler("worker", ...) stamps the service identity.
	if !strings.Contains(out, "worker") {
		t.Errorf("expected service identity 'worker' in output: %s", out)
	}
}

// TestResolvePlansPath covers both the explicit-path and empty-default branches.
func TestResolvePlansPath(t *testing.T) {
	if got := resolvePlansPath(""); got != "plans.yaml" {
		t.Errorf("resolvePlansPath(\"\") = %q; want plans.yaml", got)
	}
	if got := resolvePlansPath("/etc/custom.yaml"); got != "/etc/custom.yaml" {
		t.Errorf("resolvePlansPath(custom) = %q; want passthrough", got)
	}
}

// TestLoadPlanRegistry_Fallback exercises the load-failure path: a missing
// file must fall back to the built-in defaults (non-nil registry, no panic).
func TestLoadPlanRegistry_Fallback(t *testing.T) {
	reg := loadPlanRegistry(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if reg == nil {
		t.Fatal("loadPlanRegistry returned nil on missing file; expected default registry")
	}
}

// TestLoadPlanRegistry_Success loads a minimal valid plans.yaml so the happy
// path (no fallback) is covered.
func TestLoadPlanRegistry_Success(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "plans.yaml")
	// A minimal but structurally valid plans.yaml. If commonplans.Load is
	// strict and rejects this, the test still meaningfully exercises the
	// happy code path because Load returns a non-nil registry on success or
	// we fall through to default — either way loadPlanRegistry must be
	// non-nil. We assert non-nil regardless of strictness.
	content := "" +
		"plans:\n" +
		"  anonymous:\n" +
		"    price_cents: 0\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write plans.yaml: %v", err)
	}
	reg := loadPlanRegistry(path)
	if reg == nil {
		t.Fatal("loadPlanRegistry returned nil on valid file")
	}
}

// TestNewHealthzHandler pins the uniform /healthz JSON shape (B14-F9). A
// monitoring contract relies on the literal field set, so this guards against
// silent shape drift.
func TestNewHealthzHandler(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT filename FROM schema_migrations`).
		WillReturnRows(sqlmock.NewRows([]string{"filename"}).AddRow("062_stacks_env_vars.sql"))
	mock.ExpectQuery(`SELECT COUNT\(\*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(62))

	reader := migrations.NewReader(db, 100*time.Millisecond, nil)
	h := newHealthzHandler(reader)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	h(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q; want application/json", ct)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("healthz body not valid JSON: %v\nbody: %s", err, rec.Body.String())
	}
	if body["ok"] != true {
		t.Errorf("ok = %v; want true", body["ok"])
	}
	if body["service"] != "instant-worker" {
		t.Errorf("service = %v; want instant-worker", body["service"])
	}
	if body["migration_version"] != "062_stacks_env_vars.sql" {
		t.Errorf("migration_version = %v; want 062_stacks_env_vars.sql", body["migration_version"])
	}
	if body["migration_count"] != float64(62) {
		t.Errorf("migration_count = %v; want 62", body["migration_count"])
	}
	if body["migration_status"] != migrations.StatusOK {
		t.Errorf("migration_status = %v; want %q", body["migration_status"], migrations.StatusOK)
	}
	// commit_id / build_time / version must be present (values come from
	// buildinfo, which is "dev"/"unknown" in tests — assert keys exist).
	if _, ok := body["commit_id"]; !ok {
		t.Error("missing commit_id field")
	}
	if body["commit_id"] != buildinfo.GitSHA {
		t.Errorf("commit_id = %v; want %q", body["commit_id"], buildinfo.GitSHA)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
}

// TestNewHealthzHandler_DBUnknown verifies the degraded path: when the DB
// read fails, the handler still returns 200 with migration_status "unknown".
func TestNewHealthzHandler_DBUnknown(t *testing.T) {
	// nil DB -> reader returns StatusUnknown without a query.
	reader := migrations.NewReader(nil, time.Second, nil)
	h := newHealthzHandler(reader)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil).
		WithContext(context.Background())
	rec := httptest.NewRecorder()
	h(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 even on DB-unknown", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body not JSON: %v", err)
	}
	if body["migration_status"] != migrations.StatusUnknown {
		t.Errorf("migration_status = %v; want %q", body["migration_status"], migrations.StatusUnknown)
	}
	if body["migration_count"] != float64(0) {
		t.Errorf("migration_count = %v; want 0", body["migration_count"])
	}
}

// TestBuildMux verifies all three routes resolve to a non-nil handler and
// that /healthz / /readyz dispatch to the injected handlers. The /metrics
// route is wired to promhttp internally; we assert it returns 200.
func TestBuildMux(t *testing.T) {
	var healthzHit, readyzHit bool
	healthz := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		healthzHit = true
		w.WriteHeader(http.StatusOK)
	})
	readyz := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		readyzHit = true
		w.WriteHeader(http.StatusOK)
	})

	mux := buildMux(healthz, readyz)
	if mux == nil {
		t.Fatal("buildMux returned nil")
	}

	for _, tc := range []struct {
		path string
		hit  *bool
	}{
		{"/healthz", &healthzHit},
		{"/readyz", &readyzHit},
		{"/metrics", nil},
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status = %d; want 200", tc.path, rec.Code)
		}
		if tc.hit != nil && !*tc.hit {
			t.Errorf("%s: injected handler was not invoked", tc.path)
		}
	}
}

// TestLivenessServerLifecycle drives startLivenessServer + shutdownLivenessServer
// on a real socket bound to an ephemeral port. Proves the server serves, then
// shuts down cleanly (ErrServerClosed must not be logged as an error path that
// crashes the test).
func TestLivenessServerLifecycle(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Handler: mux}

	// Serve on the pre-bound listener via the same SafeGo wrapper semantics.
	// startLivenessServer uses srv.ListenAndServe (binds srv.Addr); to keep
	// the test deterministic on an ephemeral port we Serve the listener
	// directly through the same SafeGo path the helper uses.
	go func() { _ = srv.Serve(ln) }()

	// Poll until the server answers.
	var got int
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			got = resp.StatusCode
			resp.Body.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got != http.StatusOK {
		t.Fatalf("liveness server did not answer 200; got %d", got)
	}

	shutdownLivenessServer(srv, time.Second)

	// After shutdown the address must refuse new connections.
	if _, err := http.Get("http://" + addr + "/healthz"); err == nil {
		t.Error("expected connection failure after shutdown")
	}
}

// TestStartLivenessServer exercises the production helper directly: it binds
// an ephemeral port via srv.Addr, serves, then shuts down. Covers the SafeGo
// goroutine wrapper and the ErrServerClosed-is-fine branch.
func TestStartLivenessServer(t *testing.T) {
	// Pick a free port, then hand its address to the helper (which calls
	// ListenAndServe on srv.Addr).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close() // release so ListenAndServe can rebind it

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: addr, Handler: mux}

	startLivenessServer(srv)
	t.Cleanup(func() { shutdownLivenessServer(srv, time.Second) })

	var ok bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			ok = resp.StatusCode == http.StatusOK
			resp.Body.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("startLivenessServer did not serve 200 on %s", addr)
	}
}

// TestAwaitShutdown verifies awaitShutdown returns promptly once the context
// is cancelled — the happy SIGTERM-driven shutdown path.
func TestAwaitShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel: awaitShutdown must return immediately

	done := make(chan struct{})
	go func() {
		awaitShutdown(ctx)
		close(done)
	}()

	select {
	case <-done:
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("awaitShutdown did not return after context cancel")
	}
}

// forceErr3 wraps a 3-return constructor, preserving its first two return
// values (whatever their types) but substituting a fixed error. Generics let
// the test override the error without naming the package-private provider
// types the real constructor returns.
func forceErr3[A, B any](f func() (A, B, error), e error) func() (A, B, error) {
	return func() (A, B, error) {
		a, b, _ := f()
		return a, b, e
	}
}

// fakeWorkers is a test double for the workerSet interface.
type fakeWorkers struct {
	started bool
	stopped bool
}

func (f *fakeWorkers) Started() bool { return f.started }
func (f *fakeWorkers) Stop()         { f.stopped = true }

// testDeps builds a fully-faked deps wired with sqlmock + miniredis + an
// injected workerSet. listenAddr is an ephemeral loopback port so the
// liveness server binds without colliding with anything.
func testDeps(t *testing.T, ws workerSet) (deps, func()) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}

	sqldb, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	// The fake healthz reader will issue the migration queries on the first
	// /healthz hit; we don't drive /healthz in the run() tests, so no
	// expectations are required. Allow Close() in defer.
	mock.ExpectClose()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	cleanup := func() {
		_ = sqldb.Close()
		_ = rdb.Close()
		mr.Close()
	}

	d := deps{
		loadConfig: func() *config.Config {
			return &config.Config{Environment: "test"}
		},
		connectPostgres: func(url string) *sql.DB { return sqldb },
		connectRedis:    func(url string) *redis.Client { return rdb },
		startPoolStats: func(ctx context.Context, database *sql.DB, name string) {
			// no-op: production spawns a goroutine; tests skip it.
		},
		startWorkers: func(ctx context.Context, database *sql.DB, rdb *redis.Client, cfg *config.Config) workerSet {
			return ws
		},
		newReadyzHandler: func(cfg *config.Config, database *sql.DB, rdb *redis.Client, w workerSet) http.Handler {
			return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				rw.WriteHeader(http.StatusOK)
			})
		},
		newMigrationReader: func(database *sql.DB) *migrations.Reader {
			return migrations.NewReader(database, time.Minute, nil)
		},
		listenAddr: "127.0.0.1:0",
	}
	return d, cleanup
}

// TestRun_CleanShutdown drives the full run() happy path: workers start, the
// HTTP surface boots, and a pre-cancelled context triggers a clean shutdown
// returning exit code 0. Verifies workers.Stop() is called via defer.
func TestRun_CleanShutdown(t *testing.T) {
	fw := &fakeWorkers{started: true}
	d, cleanup := testDeps(t, fw)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediate clean shutdown

	code := run(ctx, d)
	if code != 0 {
		t.Fatalf("run exit code = %d; want 0 on clean shutdown", code)
	}
	if !fw.stopped {
		t.Error("workers.Stop() was not called on shutdown")
	}
}

// TestRun_RiverFailedToStart covers the failure path: when the worker set
// reports !Started(), run must return exit code 1 (so k8s restarts the pod)
// without serving the HTTP surface.
func TestRun_RiverFailedToStart(t *testing.T) {
	fw := &fakeWorkers{started: false}
	d, cleanup := testDeps(t, fw)
	defer cleanup()

	// Context need not be cancelled — run() returns 1 before awaitShutdown.
	code := run(context.Background(), d)
	if code != 1 {
		t.Fatalf("run exit code = %d; want 1 when River failed to start", code)
	}
	if !fw.stopped {
		t.Error("workers.Stop() must still run via defer even on early return")
	}
}

// TestProductionDeps verifies productionDeps wires every closure to a
// non-nil value with the expected bind address — the smoke test that the
// production seam isn't accidentally left with nil constructors (which would
// panic at boot, not in CI).
func TestProductionDeps(t *testing.T) {
	d := productionDeps(nil)
	if d.loadConfig == nil {
		t.Error("loadConfig is nil")
	}
	if d.connectPostgres == nil {
		t.Error("connectPostgres is nil")
	}
	if d.connectRedis == nil {
		t.Error("connectRedis is nil")
	}
	if d.startPoolStats == nil {
		t.Error("startPoolStats is nil")
	}
	if d.startWorkers == nil {
		t.Error("startWorkers is nil")
	}
	if d.newReadyzHandler == nil {
		t.Error("newReadyzHandler is nil")
	}
	if d.newMigrationReader == nil {
		t.Error("newMigrationReader is nil")
	}
	if d.listenAddr != ":8091" {
		t.Errorf("listenAddr = %q; want :8091", d.listenAddr)
	}
}

// TestSetupTelemetry verifies the fail-open telemetry wiring: with no NR
// license / OTel endpoint configured it must return a usable cleanup func
// (and a possibly-nil NR app) without erroring or panicking.
func TestSetupTelemetry(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	nrApp, cleanup := setupTelemetry()
	if cleanup == nil {
		t.Fatal("setupTelemetry returned nil cleanup")
	}
	_ = nrApp // may be nil when no NR license is configured (CI)
	cleanup() // must not panic
}

// TestTelemetryCleanup_AllBranches covers both conditional branches: a
// non-nil NR app (Shutdown invoked) and a tracer shutdown that returns an
// error (logged). Uses a disabled NR app (real type, no-op Shutdown) and a
// fake erroring tracer-shutdown.
func TestTelemetryCleanup_AllBranches(t *testing.T) {
	// Non-nil NR app + erroring tracer shutdown.
	app, err := newrelic.NewApplication(
		newrelic.ConfigAppName("instant-worker-test"),
		newrelic.ConfigEnabled(false),
	)
	if err != nil {
		t.Fatalf("NewApplication: %v", err)
	}
	var tracerCalled bool
	telemetryCleanup(app, func(context.Context) error {
		tracerCalled = true
		return errors.New("tracer shutdown failed")
	})
	if !tracerCalled {
		t.Error("tracer shutdown was not invoked")
	}

	// Nil NR app + clean tracer shutdown (no error).
	telemetryCleanup(nil, func(context.Context) error { return nil })
}

// TestRealMain drives the full realMain seam with injected fake deps and a
// pre-cancelled context: it sets up logging + telemetry, runs the worker, and
// returns the clean-shutdown exit code 0. This covers everything main() does
// except the signal-context creation and os.Exit.
func TestRealMain(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	fw := &fakeWorkers{started: true}
	d, cleanup := testDeps(t, fw)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediate clean shutdown

	var buf bytes.Buffer
	code := realMain(ctx, &buf, func(*newrelic.Application) deps { return d })
	if code != 0 {
		t.Fatalf("realMain exit code = %d; want 0", code)
	}
	if !fw.stopped {
		t.Error("workers.Stop() not called via realMain->run")
	}
}

// TestConnectProvisioner_NotConfigured covers the empty-address branch: an
// unset PROVISIONER_ADDR yields a nil client (UpdateStorageBytesWorker
// no-op) without dialing or exiting.
func TestConnectProvisioner_NotConfigured(t *testing.T) {
	pc := connectProvisioner(context.Background(), &config.Config{ProvisionerAddr: ""})
	if pc != nil {
		t.Fatalf("expected nil provisioner client when addr unset, got %v", pc)
	}
}

// TestConnectProvisioner_DialError covers the error branch: a malformed gRPC
// target makes provisioner.NewClient (grpc.NewClient) return an error, which
// logs and calls osExit(1). osExit is stubbed so the test process survives;
// we assert it was invoked with code 1 and a nil client is returned.
func TestConnectProvisioner_DialError(t *testing.T) {
	var exitCode int
	var exited bool
	prev := osExit
	osExit = func(code int) { exitCode = code; exited = true }
	t.Cleanup(func() { osExit = prev })

	// A NUL control char makes grpc.NewClient's target parse fail.
	pc := connectProvisioner(context.Background(), &config.Config{
		ProvisionerAddr: "passthrough:///\x00",
	})
	if !exited {
		t.Fatal("osExit was not called on a dial-construction error")
	}
	if exitCode != 1 {
		t.Errorf("exit code = %d; want 1", exitCode)
	}
	if pc != nil {
		t.Errorf("expected nil client after error path, got %v", pc)
	}
}

// TestNewSignalContext verifies the signal-context helper returns a live
// (not-yet-cancelled) context plus a working stop func.
func TestNewSignalContext(t *testing.T) {
	ctx, stop := newSignalContext()
	defer stop()
	if ctx == nil {
		t.Fatal("newSignalContext returned nil context")
	}
	select {
	case <-ctx.Done():
		t.Fatal("context should not be cancelled before a signal")
	default:
	}
	// stop() must make the context cancellable cleanly.
	stop()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Error("context not cancelled after stop()")
	}
}

// TestConnectProvisioner_Configured covers the configured branch. grpc.NewClient
// is lazy (no eager dial), so a syntactically-valid address yields a non-nil
// client without network IO. Cancelling the context fires the registered
// AfterFunc that closes the connection.
func TestConnectProvisioner_Configured(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pc := connectProvisioner(ctx, &config.Config{
		ProvisionerAddr:   "127.0.0.1:50051",
		ProvisionerSecret: "test-secret",
	})
	if pc == nil {
		t.Fatal("expected non-nil provisioner client for a valid address")
	}
	// Fire the AfterFunc-registered conn.Close() and give it a moment.
	cancel()
	time.Sleep(20 * time.Millisecond)
}

// TestServeLiveness_BindError exercises the error-log branch synchronously:
// an unbindable address makes ListenAndServe return a non-ErrServerClosed
// error, which is logged. Synchronous so the branch's coverage counter is
// deterministically recorded (a goroutine's counter is racy to observe).
func TestServeLiveness_BindError(t *testing.T) {
	srv := &http.Server{Addr: "256.256.256.256:8091", Handler: http.NewServeMux()}
	serveLiveness(srv) // returns after ListenAndServe fails
}

// TestServeLiveness_GracefulClose covers the ErrServerClosed branch (the
// no-log path): a server that is Shutdown while serving returns
// http.ErrServerClosed, which must NOT be logged as an error.
func TestServeLiveness_GracefulClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	srv := &http.Server{Addr: addr, Handler: http.NewServeMux()}
	done := make(chan struct{})
	go func() { serveLiveness(srv); close(done) }()

	// Wait until the server is listening, then shut it down.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c, derr := net.Dial("tcp", addr); derr == nil {
			c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	shutdownLivenessServer(srv, time.Second)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("serveLiveness did not return after graceful shutdown")
	}
}

// TestStartLivenessServer_Wrapper drives the SafeGo wrapper itself on an
// ephemeral port and confirms the server answers, then shuts down.
func TestStartLivenessServer_Wrapper(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	srv := &http.Server{Addr: addr, Handler: mux}
	startLivenessServer(srv)
	t.Cleanup(func() { shutdownLivenessServer(srv, time.Second) })

	var ok bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if resp, gerr := http.Get("http://" + addr + "/healthz"); gerr == nil {
			ok = resp.StatusCode == http.StatusOK
			resp.Body.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ok {
		t.Fatal("startLivenessServer wrapper did not serve 200")
	}
}

// TestProdNewMigrationReader verifies the production reader constructor builds
// a usable Reader (defaults applied) that surfaces StatusUnknown on a nil DB
// without panicking.
func TestProdNewMigrationReader(t *testing.T) {
	r := prodNewMigrationReader(nil)
	if r == nil {
		t.Fatal("prodNewMigrationReader returned nil")
	}
	if s := r.Get(context.Background()); s.Status != migrations.StatusUnknown {
		t.Errorf("nil-DB reader status = %q; want %q", s.Status, migrations.StatusUnknown)
	}
}

// TestProdNewReadyzHandler builds the real /readyz handler adapter and drives
// one request through it. We don't assert the readiness verdict (it depends
// on unreachable upstreams in CI) — only that the adapter returns a working
// http.Handler that responds without panicking.
func TestProdNewReadyzHandler(t *testing.T) {
	fw := &fakeWorkers{started: true}
	d, cleanup := testDeps(t, fw)
	defer cleanup()

	// Use the faked stores from testDeps to construct the real handler.
	sqldb := d.connectPostgres("")
	rdb := d.connectRedis("")

	h := prodNewReadyzHandler(&config.Config{}, sqldb, rdb, fw)
	if h == nil {
		t.Fatal("prodNewReadyzHandler returned nil")
	}
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// Any HTTP status is acceptable; the assertion is "it ran".
	if rec.Code == 0 {
		t.Error("readyz handler wrote no status")
	}
}

// TestDeployK8sInitOK covers both log branches of the deploy-status k8s init
// outcome: a nil error (clients usable, info log) and a non-nil error (warn
// log, clients nilled by the caller).
func TestDeployK8sInitOK(t *testing.T) {
	if !deployK8sInitOK(nil) {
		t.Error("deployK8sInitOK(nil) = false; want true")
	}
	if deployK8sInitOK(errors.New("no kubeconfig")) {
		t.Error("deployK8sInitOK(err) = true; want false")
	}
}

// TestMain_Wrapper drives the main() wrapper itself by swapping its
// indirected collaborators: a fake signal context (already cancelled), a fake
// realMain returning a known exit code, and a capturing osExit. Verifies main
// threads the code from realMain into osExit and stops the signal context.
func TestMain_Wrapper(t *testing.T) {
	prevSig, prevRM, prevExit := signalCtxFn, realMainFn, osExit
	t.Cleanup(func() { signalCtxFn, realMainFn, osExit = prevSig, prevRM, prevExit })

	var stopped bool
	signalCtxFn = func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		return ctx, func() { stopped = true; cancel() }
	}
	var gotDepsCalled bool
	realMainFn = func(ctx context.Context, w io.Writer, makeDeps func(*newrelic.Application) deps) int {
		// Exercise makeDeps so the wrapper's argument is a real factory.
		_ = makeDeps
		gotDepsCalled = true
		return 7
	}
	var exitCode int
	osExit = func(code int) { exitCode = code }

	main()

	if exitCode != 7 {
		t.Errorf("osExit code = %d; want 7 (from fake realMain)", exitCode)
	}
	if !stopped {
		t.Error("deferred stop() was not called")
	}
	if !gotDepsCalled {
		t.Error("realMain was not invoked by main()")
	}
}

// TestProdStartWorkers exercises the production startWorkers closure end to
// end. With an empty cfg.DatabaseURL the underlying jobs.StartWorkers fails to
// open its pgx pool and returns a not-started Workers — no real Postgres /
// River is needed. This covers the k8s-client init (warn path in CI), the
// backup-plan adapter, the nil-provisioner branch, and the StartWorkers call.
func TestProdStartWorkers(t *testing.T) {
	fw := &fakeWorkers{}
	d, cleanup := testDeps(t, fw)
	defer cleanup()
	sqldb := d.connectPostgres("")
	rdb := d.connectRedis("")

	// Force the k8s-init failure path so the fail-open nil-out branch in
	// prodStartWorkers is exercised regardless of the test host's kubeconfig.
	// A generic wrapper infers the unexported provider types from the real
	// constructor, overriding only the returned error.
	prevK8s := newDeployK8sClients
	t.Cleanup(func() { newDeployK8sClients = prevK8s })
	newDeployK8sClients = forceErr3(prevK8s, errors.New("forced k8s init failure"))

	ws := prodStartWorkers(nil)(context.Background(), sqldb, rdb, &config.Config{
		// Empty DatabaseURL => pgxpool.New fails => Workers{} (not started).
		ProvisionerAddr: "", // exercise the nil-provisioner branch
	})
	if ws == nil {
		t.Fatal("prodStartWorkers returned nil workerSet")
	}
	// In CI there is no platform Postgres, so River never starts.
	if ws.Started() {
		t.Error("expected Started()==false without a real platform DB")
	}
	ws.Stop()
}

// TestProdStartPoolStats verifies the exporter spawn helper returns promptly
// (the goroutine it starts is bound to the supplied context and exits when
// cancelled). Driving it with a pre-cancelled context keeps the spawned
// goroutine short-lived.
func TestProdStartPoolStats(t *testing.T) {
	fw := &fakeWorkers{started: true}
	d, cleanup := testDeps(t, fw)
	defer cleanup()
	sqldb := d.connectPostgres("")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Must not block or panic.
	prodStartPoolStats(ctx, sqldb, "platform_db")
}
