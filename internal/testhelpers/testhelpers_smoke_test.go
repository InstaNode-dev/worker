package testhelpers

// testhelpers_smoke_test.go — in-package coverage for the worker integration
// harness itself.
//
// WHY THIS EXISTS
// ---------------
// The Seed*/read helpers + SetupTestDB live in this (non-_test.go) file so the
// jobs-package integration tests can import them. Go's per-package coverage
// attribution only credits the `testhelpers` package when a test in THIS
// package runs — the jobs-package integration tests that call these helpers
// credit `internal/jobs`, not `internal/testhelpers`. Without an in-package
// test, every line of testhelpers.go reads as 0% in diff-cover and reds the
// 100%-patch-coverage gate (the exact failure on PR #87). This mirrors
// api/internal/testhelpers/testapp_smoke_test.go, the established platform
// convention for giving a test-harness package its own coverage.
//
// The harness's own fail/skip arms (DB fails to open/ping, an INSERT errors, a
// scan fails) are exercised by swapping the package's tFatalf/tSkipf seams for
// recording stubs and driving the arm with a deliberately-closed DB. This is a
// test seam (the platform's "use test seams, not waivers" coverage rule), not a
// behavioural change — real callers get genuine t.Fatalf / t.Skipf.
//
// GATING: the DB-backed tests route through SetupTestDB, which skips when no
// Postgres is reachable — so they skip cleanly on the no-DB workflows
// (deploy.yml `-short`, ci.yml `-race`) and run against coverage.yml's postgres
// service + any developer DB. isUndefinedColumn is pure logic and is
// unit-tested unconditionally below.

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
)

// swapFatalSeams replaces tFatalf/tSkipf with stubs that record the message and
// abort the *current goroutine path* via panic-free early return semantics. The
// helpers under test all `return` immediately after calling the seam, so a
// recording stub that does nothing lets the helper return its zero value while
// the test asserts the arm fired. Restored via t.Cleanup.
func swapFatalSeams(t *testing.T) (fatal, skip *[]string) {
	t.Helper()
	var fatals, skips []string
	origF, origS := tFatalf, tSkipf
	tFatalf = func(_ *testing.T, format string, args ...any) { fatals = append(fatals, format) }
	tSkipf = func(_ *testing.T, format string, args ...any) { skips = append(skips, format) }
	t.Cleanup(func() { tFatalf, tSkipf = origF, origS })
	return &fatals, &skips
}

// TestSeamDefaults covers the default tFatalf/tSkipf closures (the real
// (*testing.T).Fatalf / .Skipf forwarders). Both abort via runtime.Goexit, so
// each is invoked on a throwaway *testing.T inside its own goroutine: the Goexit
// terminates only that goroutine, never the parent test, and a sentinel set
// AFTER the call proves the call returned only via the seam (Goexit), i.e. the
// forwarder body ran.
func TestSeamDefaults(t *testing.T) {
	run := func(name string, call func(*testing.T)) {
		reached := false
		done := make(chan struct{})
		// A fresh, isolated *testing.T whose pass/fail is intentionally
		// discarded (we never call t.Run on it) — we only need a valid receiver
		// for the forwarder. The goroutine ends at the seam's runtime.Goexit.
		go func() {
			defer close(done)
			st := &testing.T{}
			call(st)
			reached = true // unreachable when call() Goexits, as it must.
		}()
		<-done
		if reached {
			t.Fatalf("%s: default seam did not abort the goroutine (forwarder body not exercised)", name)
		}
	}
	run("fatal", func(st *testing.T) { tFatalf(st, "default fatal forwarder: %s", "ok") })
	run("skip", func(st *testing.T) { tSkipf(st, "default skip forwarder: %s", "ok") })
}

// TestIsUndefinedColumn exercises every branch of the SQLSTATE-42703 detector
// used by SeedDeployment's prod-vs-bare schema fallback. Pure logic, no DB —
// runs on every workflow (unit test, not an Integration test).
func TestIsUndefinedColumn(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"sqlstate 42703", errors.New(`pq: column "app_id" of relation "deployments" (SQLSTATE 42703)`), true},
		{"column does not exist phrasing", errors.New(`ERROR: column "app_id" does not exist`), true},
		{"column word only, no does-not-exist", errors.New(`ERROR: column "x" is ambiguous`), false},
		{"does-not-exist without column word", errors.New(`relation "foo" does not exist`), false},
		{"unrelated error", errors.New("connection refused"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isUndefinedColumn(tc.err); got != tc.want {
				t.Fatalf("isUndefinedColumn(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestIntegration_SetupTestDB_DefaultDSNFallback covers the
// `TEST_DATABASE_URL unset -> DefaultTestDBURL` fallback branch. CI always
// exports TEST_DATABASE_URL, so only an explicit unset exercises it. The
// default DSN points at the same local/CI Postgres (localhost:5432), so when a
// DB is reachable SetupTestDB succeeds; otherwise it skips via the ping arm —
// either way the fallback assignment line runs.
func TestIntegration_SetupTestDB_DefaultDSNFallback(t *testing.T) {
	t.Setenv("TEST_DATABASE_URL", "") // empty -> SetupTestDB falls back to DefaultTestDBURL
	db, cleanup := SetupTestDB(t)     // skips here if the default DSN is unreachable
	defer cleanup()
	if db == nil {
		t.Fatal("SetupTestDB returned nil db after default-DSN fallback")
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("default-DSN db not usable: %v", err)
	}
}

// TestSetupTestDB_BadDSN covers SetupTestDB's DSN-parse skip arm by pointing
// TEST_DATABASE_URL at a string pq.NewConnector rejects (invalid URL escape).
func TestSetupTestDB_BadDSN(t *testing.T) {
	t.Setenv("TEST_DATABASE_URL", "postgres://%zz")
	_, skips := swapFatalSeams(t)
	db, cleanup := SetupTestDB(t)
	if cleanup != nil {
		cleanup()
	}
	if db != nil {
		t.Fatal("SetupTestDB returned a non-nil db on DSN parse failure")
	}
	if len(*skips) == 0 {
		t.Fatal("expected a Skipf on DSN parse failure, got none")
	}
}

// TestSetupTestDB_PingError covers the ping-failure skip arm by pointing at a
// valid DSN whose host/port refuses connections.
func TestSetupTestDB_PingError(t *testing.T) {
	// Point at a closed port so PingContext fails fast → the ping skip arm.
	t.Setenv("TEST_DATABASE_URL", "postgres://postgres@127.0.0.1:1/doesnotexist?sslmode=disable&connect_timeout=1")
	_, skips := swapFatalSeams(t)
	db, cleanup := SetupTestDB(t)
	if cleanup != nil {
		cleanup()
	}
	if db != nil {
		t.Fatal("SetupTestDB returned a non-nil db on ping failure")
	}
	if len(*skips) == 0 {
		t.Fatal("expected a Skipf on ping failure, got none")
	}
}

// TestHarnessErrorArms drives every fallible helper against a CLOSED *sql.DB so
// each Exec/QueryRow fails — covering the error arm (via the recording seam) of
// SeedTeam, SeedDeployment, SeedResource, DeploymentStatus, AppliedConnLimit,
// CountAuditLog, AutopsyRow, ensureSchema and ensureAutopsyUniqueIndex.
func TestHarnessErrorArms(t *testing.T) {
	closed, err := sql.Open("postgres", "postgres://postgres@127.0.0.1:1/x?sslmode=disable")
	if err != nil {
		t.Fatalf("open placeholder db: %v", err)
	}
	_ = closed.Close() // every subsequent Exec/QueryRow now errors.

	fatals, _ := swapFatalSeams(t)
	id := uuid.New()

	ensureSchema(t, closed)
	ensureAutopsyUniqueIndex(t, closed)
	SeedTeam(t, closed, "pro")
	SeedDeployment(t, closed, id, "building", "prov")
	SeedResource(t, closed, id, "postgres", "pro", sql.NullInt64{Int64: 5, Valid: true})
	DeploymentStatus(t, closed, id)
	AppliedConnLimit(t, closed, id)
	CountAuditLog(t, closed, "deploy.failed", id.String())
	AutopsyRow(t, closed, id)

	// 9 distinct fallible call paths each recorded at least one Fatalf.
	if len(*fatals) < 9 {
		t.Fatalf("expected >=9 recorded Fatalf arms against the closed DB, got %d: %v", len(*fatals), *fatals)
	}
}

// TestIntegration_HarnessRoundTrip drives every DB-backed helper against a real
// Postgres so the harness's happy paths + both reachable conditional branches
// (the autopsy-index create branch and the bare-schema SeedDeployment fallback)
// carry real line coverage.
func TestIntegration_HarnessRoundTrip(t *testing.T) {
	db, cleanup := SetupTestDB(t)
	defer cleanup()

	// ensureSchema is idempotent — re-run drives the IF-NOT-EXISTS no-op path
	// and the already-present arm of ensureAutopsyUniqueIndex.
	ensureSchema(t, db)

	// --- create-index branch of ensureAutopsyUniqueIndex (250-256) ---------
	// Drop any failure_autopsy unique index so the "not present" arm runs.
	dropAutopsyIndexes(t, db)
	ensureAutopsyUniqueIndex(t, db)
	if !autopsyIndexPresent(t, db) {
		t.Fatal("ensureAutopsyUniqueIndex did not create the autopsy index when absent")
	}

	// SeedTeam: explicit tier + the "" -> default-hobby branch.
	teamPro := SeedTeam(t, db, "pro")
	teamDefault := SeedTeam(t, db, "")
	if teamPro == uuid.Nil || teamDefault == uuid.Nil {
		t.Fatal("SeedTeam returned nil uuid")
	}

	// SeedDeployment with a provider_id, then read it back.
	depID := SeedDeployment(t, db, teamPro, "building", "prov-123")
	if status, _ := DeploymentStatus(t, db, depID); status != "building" {
		t.Fatalf("DeploymentStatus = %q, want building", status)
	}

	// SeedDeployment with an empty provider_id (the stuck-building model).
	stuckID := SeedDeployment(t, db, teamPro, "building", "")
	if status, _ := DeploymentStatus(t, db, stuckID); status != "building" {
		t.Fatalf("stuck DeploymentStatus = %q, want building", status)
	}

	// SeedResource: a valid applied_conn_limit and a NULL one.
	resGraded, _ := SeedResource(t, db, teamPro, "postgres", "pro",
		sql.NullInt64{Int64: 20, Valid: true})
	resUngraded, tok := SeedResource(t, db, teamPro, "redis", "pro", sql.NullInt64{})
	if tok == "" {
		t.Fatal("SeedResource returned empty token")
	}
	if v := AppliedConnLimit(t, db, resGraded); !v.Valid || v.Int64 != 20 {
		t.Fatalf("AppliedConnLimit(graded) = %+v, want {20,true}", v)
	}
	if v := AppliedConnLimit(t, db, resUngraded); v.Valid {
		t.Fatalf("AppliedConnLimit(ungraded) = %+v, want NULL", v)
	}

	// AutopsyRow not-found arm before any autopsy row exists.
	if _, ok := AutopsyRow(t, db, depID); ok {
		t.Fatal("AutopsyRow returned ok=true before any autopsy row was written")
	}

	// Write a failure_autopsy deployment_events row + a deploy.failed audit_log
	// row, then read both back (CountAuditLog + AutopsyRow found arm).
	if _, err := db.Exec(
		`INSERT INTO deployment_events (deployment_id, kind, reason)
		 VALUES ($1, 'failure_autopsy', 'BackoffLimitExceeded')`, depID,
	); err != nil {
		t.Fatalf("insert autopsy row: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		 VALUES ($1, 'worker', 'deploy.failed', 'deploy failed',
		         jsonb_build_object('deploy_id', $2::text))`,
		teamPro, depID.String(),
	); err != nil {
		t.Fatalf("insert audit_log row: %v", err)
	}

	if reason, ok := AutopsyRow(t, db, depID); !ok || reason != "BackoffLimitExceeded" {
		t.Fatalf("AutopsyRow = (%q, %v), want (BackoffLimitExceeded, true)", reason, ok)
	}
	if n := CountAuditLog(t, db, "deploy.failed", depID.String()); n != 1 {
		t.Fatalf("CountAuditLog = %d, want 1", n)
	}

	// --- create-error arm of ensureAutopsyUniqueIndex (272) ----------------
	// Drop the autopsy index, then insert two failure_autopsy rows for one
	// deployment so the CREATE UNIQUE INDEX fails on the duplicate. The probe
	// returns not-present, so the create branch runs and its error arm fires.
	dep2 := SeedDeployment(t, db, teamPro, "building", "p2")
	dropAutopsyIndexes(t, db)
	for i := 0; i < 2; i++ {
		if _, err := db.Exec(
			`INSERT INTO deployment_events (deployment_id, kind, reason)
			 VALUES ($1, 'failure_autopsy', 'dup')`, dep2,
		); err != nil {
			t.Fatalf("seed duplicate autopsy row: %v", err)
		}
	}
	func() {
		fatals, _ := swapFatalSeams(t)
		ensureAutopsyUniqueIndex(t, db)
		if len(*fatals) == 0 {
			t.Fatal("expected ensureAutopsyUniqueIndex create arm to fail on duplicate rows")
		}
	}()
	// Clean up the duplicate rows so the index can be recreated for other tests.
	if _, err := db.Exec(`DELETE FROM deployment_events WHERE deployment_id = $1`, dep2); err != nil {
		t.Fatalf("cleanup duplicate autopsy rows: %v", err)
	}
	ensureAutopsyUniqueIndex(t, db) // restore the index

	// --- bare-schema fallback of SeedDeployment (the isUndefinedColumn arm) -
	// Temporarily drop the prod app_id column so the first INSERT 42703s and the
	// bare-schema fallback INSERT runs. Restored via cleanup.
	withoutAppIDColumn(t, db, func() {
		bareID := SeedDeployment(t, db, teamPro, "building", "prov-bare")
		if bareID == uuid.Nil {
			t.Fatal("SeedDeployment bare-schema fallback returned nil id")
		}
		if status, _ := DeploymentStatus(t, db, bareID); status != "building" {
			t.Fatalf("bare-schema DeploymentStatus = %q, want building", status)
		}
	})
}

// dropAutopsyIndexes drops every unique index on deployment_events whose
// definition mentions failure_autopsy, so the create-index arm of
// ensureAutopsyUniqueIndex runs on the next call.
func dropAutopsyIndexes(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query(`
		SELECT idx.relname
		  FROM pg_index i
		  JOIN pg_class idx ON idx.oid = i.indexrelid
		  JOIN pg_class tbl ON tbl.oid = i.indrelid
		 WHERE tbl.relname = 'deployment_events'
		   AND i.indisunique
		   AND pg_get_indexdef(i.indexrelid) ILIKE '%failure_autopsy%'`)
	if err != nil {
		t.Fatalf("dropAutopsyIndexes query: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan index name: %v", err)
		}
		names = append(names, n)
	}
	for _, n := range names {
		if _, err := db.Exec(`DROP INDEX IF EXISTS ` + n); err != nil {
			t.Fatalf("drop index %s: %v", n, err)
		}
	}
}

func autopsyIndexPresent(t *testing.T, db *sql.DB) bool {
	t.Helper()
	var present bool
	if err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM pg_index i
			  JOIN pg_class idx ON idx.oid = i.indexrelid
			  JOIN pg_class tbl ON tbl.oid = i.indrelid
			 WHERE tbl.relname = 'deployment_events'
			   AND i.indisunique
			   AND pg_get_indexdef(i.indexrelid) ILIKE '%failure_autopsy%')`).Scan(&present); err != nil {
		t.Fatalf("autopsyIndexPresent: %v", err)
	}
	return present
}

// withoutAppIDColumn drops deployments.app_id for the duration of fn, then
// restores it (nullable — the harness only needs the column to exist). If the
// column was already absent (bare harness DB), fn runs unchanged.
func withoutAppIDColumn(t *testing.T, db *sql.DB, fn func()) {
	t.Helper()
	var had bool
	if err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			 WHERE table_name = 'deployments' AND column_name = 'app_id')`).Scan(&had); err != nil {
		t.Fatalf("probe app_id column: %v", err)
	}
	if had {
		if _, err := db.Exec(`ALTER TABLE deployments DROP COLUMN app_id`); err != nil {
			t.Fatalf("drop app_id: %v", err)
		}
		defer func() {
			if _, err := db.Exec(`ALTER TABLE deployments ADD COLUMN IF NOT EXISTS app_id TEXT`); err != nil {
				t.Fatalf("restore app_id: %v", err)
			}
		}()
	}
	fn()
}
