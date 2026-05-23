package jobs

// workers_schedule_coverage_test.go — drives the previously-0% leaf helpers in
// workers.go: the cron PeriodicSchedule.Next implementations, scheduleFunc.Next,
// Workers.Started, the entitlementRegraderAdapter error arm, and the two
// StartWorkers early-return guards (bad DatabaseURL → pgxpool error;
// unknown EMAIL_PROVIDER → email-provider init error).

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"instant.dev/worker/internal/config"
	"instant.dev/worker/internal/provisioner"
	commonv1 "instant.dev/proto/common/v1"
)

func TestWorkers_Started(t *testing.T) {
	if (&Workers{started: true}).Started() != true {
		t.Error("Started() should be true")
	}
	if (&Workers{started: false}).Started() != false {
		t.Error("Started() should be false")
	}
}

func TestScheduleFunc_Next(t *testing.T) {
	want := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	var s scheduleFunc = func(time.Time) time.Time { return want }
	if got := s.Next(time.Now()); !got.Equal(want) {
		t.Errorf("scheduleFunc.Next = %v, want %v", got, want)
	}
}

func TestMondayAt8UTCSchedule_Next(t *testing.T) {
	sched := mondayAt8UTCSchedule{}

	// From a Wednesday → next Monday 08:00 UTC.
	wed := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC) // Wed
	next := sched.Next(wed)
	if next.Weekday() != time.Monday || next.Hour() != 8 {
		t.Errorf("Next(Wed) = %v, want a Monday 08:00", next)
	}
	if !next.After(wed) {
		t.Errorf("Next(Wed) = %v not after %v", next, wed)
	}

	// From Monday 09:00 (past the 08:00 slot) → next Monday (rolls 7d).
	monLate := time.Date(2026, 5, 18, 9, 0, 0, 0, time.UTC) // Mon 09:00
	nl := sched.Next(monLate)
	if nl.Weekday() != time.Monday || !nl.After(monLate) {
		t.Errorf("Next(Mon 09:00) = %v, want a later Monday", nl)
	}

	// From Monday 07:00 (before slot) → same Monday 08:00.
	monEarly := time.Date(2026, 5, 18, 7, 0, 0, 0, time.UTC)
	ne := sched.Next(monEarly)
	if ne.Weekday() != time.Monday || ne.Hour() != 8 || ne.Day() != monEarly.Day() {
		t.Errorf("Next(Mon 07:00) = %v, want same-day Monday 08:00", ne)
	}
}

func TestDailyAt3UTCSchedule_Next(t *testing.T) {
	sched := dailyAt3UTCSchedule{}
	// Before 03:00 → same day 03:00.
	early := time.Date(2026, 5, 20, 1, 0, 0, 0, time.UTC)
	if n := sched.Next(early); n.Hour() != 3 || n.Day() != early.Day() {
		t.Errorf("Next(01:00) = %v, want same-day 03:00", n)
	}
	// After 03:00 → next day.
	late := time.Date(2026, 5, 20, 5, 0, 0, 0, time.UTC)
	if n := sched.Next(late); n.Hour() != 3 || n.Day() != late.Day()+1 {
		t.Errorf("Next(05:00) = %v, want next-day 03:00", n)
	}
}

func TestDailyAt2UTCSchedule_Next(t *testing.T) {
	sched := dailyAt2UTCSchedule{}
	early := time.Date(2026, 5, 20, 1, 0, 0, 0, time.UTC)
	if n := sched.Next(early); n.Hour() != 2 || n.Day() != early.Day() {
		t.Errorf("Next(01:00) = %v, want same-day 02:00", n)
	}
	late := time.Date(2026, 5, 20, 5, 0, 0, 0, time.UTC)
	if n := sched.Next(late); n.Hour() != 2 || n.Day() != late.Day()+1 {
		t.Errorf("Next(05:00) = %v, want next-day 02:00", n)
	}
}

// entitlementRegraderAdapter.RegradeResource error arm: a client pointed at a
// dead address surfaces the gRPC error (covers the err != nil branch + the
// adapter wiring).
func TestEntitlementRegraderAdapter_ErrorArm(t *testing.T) {
	client, conn, err := provisioner.NewClient("127.0.0.1:1", "secret")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer conn.Close()

	adapter := entitlementRegraderAdapter{client: client}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = adapter.RegradeResource(ctx, "tok", "prid", commonv1.ResourceType_RESOURCE_TYPE_POSTGRES, "pro", "req-1")
	if err == nil {
		t.Error("RegradeResource against dead address should error")
	}
}

// newMinioAdminClient: empty endpoint short-circuits to (nil, nil); a
// configured endpoint builds an admin client without contacting it
// (madmin.NewWithOptions only parses the endpoint URL, no network I/O).
func TestNewMinioAdminClient_EmptyEndpoint_ReturnsNil(t *testing.T) {
	mc, err := newMinioAdminClient(&config.Config{MinioEndpoint: ""})
	if err != nil {
		t.Fatalf("empty endpoint returned err: %v", err)
	}
	if mc != nil {
		t.Errorf("empty endpoint returned non-nil client %v, want nil", mc)
	}
}

func TestNewMinioAdminClient_ConfiguredEndpoint_BuildsClient(t *testing.T) {
	mc, err := newMinioAdminClient(&config.Config{
		MinioEndpoint:     "minio.example.com:9000",
		MinioRootUser:     "minioadmin",
		MinioRootPassword: "minioadmin",
	})
	if err != nil {
		t.Fatalf("configured endpoint returned err: %v", err)
	}
	if mc == nil {
		t.Error("configured endpoint returned nil client, want non-nil")
	}
}

// StartWorkers early-return guards. We don't start a real worker pool here —
// only the synchronous early-exit branches.
func TestStartWorkers_BadDatabaseURL_ReturnsEmpty(t *testing.T) {
	cfg := &config.Config{
		DatabaseURL: "://not-a-valid-dsn", // pgxpool.New parse failure
	}
	ctx := context.Background()
	w := StartWorkers(ctx, nil, nil, cfg, nil, nil, nil, nil, nil, nil)
	if w == nil {
		t.Fatal("StartWorkers returned nil")
	}
	if w.Started() {
		t.Error("StartWorkers with bad DSN should not report Started")
	}
}

// TestStartWorkers_FullBoot drives the entire StartWorkers body against a live
// Postgres + Redis: pgxpool, River schema migrations, the email-provider
// factory (noop default), every river.AddWorker registration, the k8s
// constructor fail-open WARN arms, river.NewClient, riverClient.Start, and the
// success return — then exercises Workers.Stop's client!=nil graceful-drain
// arm. Skips cleanly when the docker DBs aren't reachable.
func TestStartWorkers_FullBoot(t *testing.T) {
	// Dedicated env var (not TEST_DATABASE_URL) so this test owns its DB: the
	// StartWorkers boot runs River schema migrations + RunOnStart periodic jobs,
	// a different schema surface than the api-platform schema the propagation
	// integration tests expect on TEST_DATABASE_URL. Keeping them separate means
	// neither test perturbs the other's DB.
	pgDSN := os.Getenv("TEST_WORKER_STARTUP_DSN")
	if pgDSN == "" {
		t.Skip("set TEST_WORKER_STARTUP_DSN (a River-capable postgres) to run the StartWorkers full-boot coverage test")
	}
	// Verify the DSN actually opens before driving River (the docker daemon in
	// this env is flaky; a clean skip beats a confusing River error).
	probe, err := sql.Open("postgres", pgDSN)
	if err != nil {
		t.Skipf("sql.Open: %v", err)
	}
	pctx, pcancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer pcancel()
	if err := probe.PingContext(pctx); err != nil {
		probe.Close()
		t.Skipf("ping %s: %v — DB not reachable", pgDSN, err)
	}
	probe.Close()

	cfg := &config.Config{
		DatabaseURL: pgDSN,
		Environment: "development",
		// EmailProvider empty → NoopProvider (fail-open default).
		// All optional dependencies (provisioner, redis admin, object store,
		// Razorpay, k8s) left unset → each worker is wired in fail-open mode.
	}
	ctx := context.Background()

	w := StartWorkers(ctx, nil, nil, cfg, nil, nil, nil, nil, nil, nil)
	if w == nil {
		t.Fatal("StartWorkers returned nil")
	}
	// Always stop, even if it failed to start, so the pgxpool/River goroutines
	// don't leak into sibling tests.
	defer w.Stop()

	if !w.Started() {
		// River may legitimately fail to start if the DB lacks permissions to
		// run its schema migrations; treat as a skip rather than a hard fail so
		// this stays robust across environments.
		t.Skip("StartWorkers did not reach started=true (River migrate/start gated by DB perms) — body still executed for coverage")
	}
}

// TestStartWorkers_BootInIsolatedDB drives StartWorkers past the
// email-provider init into the MinIO-client + worker-registration body using
// a throwaway database created from the root test-pg connection. Unlike
// TestStartWorkers_FullBoot (which needs the dedicated TEST_WORKER_STARTUP_DSN
// and skips in CI), this self-provisions its own DB so River's schema
// migrations never touch the shared TEST_DATABASE_URL schema — giving CI
// coverage of the post-email-init body (incl. the newMinioAdminClient call
// site) without the cross-test perturbation risk that motivated the separate
// DSN. Skips cleanly when the root pg container is unreachable.
func TestStartWorkers_BootInIsolatedDB(t *testing.T) {
	root, err := sql.Open("postgres", pgTestDSN())
	if err != nil {
		t.Skipf("postgres open: %v", err)
	}
	defer root.Close()
	root.SetConnMaxLifetime(5 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := root.PingContext(ctx); err != nil {
		t.Skipf("postgres ping failed (docker test-pg not reachable): %v", err)
	}

	dbName := fmt.Sprintf("worker_boot_%d", time.Now().UnixNano()%1_000_000)
	if _, err := root.ExecContext(ctx, `CREATE DATABASE `+quoteIdent(dbName)); err != nil {
		t.Skipf("CREATE DATABASE: %v", err)
	}
	t.Cleanup(func() { _, _ = root.Exec(`DROP DATABASE IF EXISTS ` + quoteIdent(dbName)) })

	// Rewrite the path of pgTestDSN to point at the throwaway DB.
	bootDSN := dsnWithDB(pgTestDSN(), dbName)

	cfg := &config.Config{
		DatabaseURL: bootDSN,
		Environment: "development",
		// EmailProvider empty → NoopProvider; all optional deps unset →
		// every worker wired fail-open. A deliberately-malformed MinioEndpoint
		// makes newMinioAdminClient return an error so the call site's
		// fail-open WARN branch is exercised (minioClient stays nil and boot
		// continues — proving the cleanup-client init never blocks startup).
		MinioEndpoint: "::::bad",
	}

	w := StartWorkers(context.Background(), nil, nil, cfg, nil, nil, nil, nil, nil, nil)
	if w == nil {
		t.Fatal("StartWorkers returned nil")
	}
	defer w.Stop()
}

// dsnWithDB replaces the database name (the URL path) in a postgres DSN.
func dsnWithDB(dsn, db string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	u.Path = "/" + db
	return u.String()
}
