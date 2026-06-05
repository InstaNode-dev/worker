// Package testhelpers provides a real-Postgres integration harness for the
// worker's periodic-job tests.
//
// # WHY THIS EXISTS
//
// Before this package, only 3 of the worker's ~59 job files exercised a real
// database — the rest drove their logic through sqlmock / fake River / fake
// k8s clients. That is *unit* coverage of job logic, not *integration*
// coverage of the trigger→DB-effect round-trip that the job actually performs
// in production. The integration-coverage plan (INTEGRATION-COVERAGE-PLAN-
// 2026-06-04.md §5 #1) flagged the worker as the single biggest integration
// gap on the platform and called for a harness mirroring api's
// testhelpers.SetupTestDB.
//
// # DESIGN
//
// The worker module does NOT import the api module (the platform-DB schema is
// owned by api/internal/db/migrations). So — like the job files themselves
// (deploy_status_reconcile.go, deploy_failure_autopsy.go) which duplicate the
// schema strings rather than importing api — this harness ensures the *subset*
// of the platform schema the integration-tested jobs touch, idempotently, via
// CREATE TABLE / CREATE INDEX IF NOT EXISTS. It is deliberately a subset (not
// the full 66-migration mirror api maintains): only the tables the worker jobs
// under test round-trip against. New jobs that touch new tables extend
// ensureSchema here.
//
// # GATING
//
// SetupTestDB calls t.Skip (NOT t.Fatal) when the DB is unreachable or
// TEST_DATABASE_URL is unset, AND when running under `-short`. This matches the
// worker's two CI workflows: deploy.yml runs `go test ./... -short` (no DB
// service) and ci.yml runs `go test ./... -race` (also no DB service). In both,
// these integration tests SKIP cleanly. They run only where a real Postgres is
// supplied via TEST_DATABASE_URL (developer machine / a future CI DB service).
// This mirrors the existing propagation_runner_integration_test.go gating.
package testhelpers

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// tFatalf / tSkipf are indirection seams over (*testing.T).Fatalf / .Skipf.
// They exist solely so the harness's own error/skip arms (a DB that fails to
// open, an INSERT that errors, a scan that fails) are reachable from this
// package's in-package coverage tests — which swap them for recording stubs and
// drive the arms with a deliberately-broken DB. In every real test run they are
// the genuine t.Fatalf / t.Skipf. This is a test seam (per the platform's
// "use test seams, not waivers" coverage rule), NOT a behavioural change:
// production callers see identical fail/skip semantics. The default values are
// reassigned only inside testhelpers_smoke_test.go and restored via t.Cleanup.
var (
	tFatalf = func(t *testing.T, format string, args ...any) { t.Helper(); t.Fatalf(format, args...) }
	tSkipf  = func(t *testing.T, format string, args ...any) { t.Helper(); t.Skipf(format, args...) }
)

// isUndefinedColumn reports whether err is a Postgres "column does not exist"
// error (SQLSTATE 42703). Used so SeedDeployment can target the richer prod
// schema (NOT NULL app_id) and fall back to the bare-harness schema when the
// column is absent — without importing the pq error type at the call site.
func isUndefinedColumn(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "42703") ||
		(strings.Contains(msg, "column") && strings.Contains(msg, "does not exist"))
}

// DefaultTestDBURL is the local platform-DB DSN used when TEST_DATABASE_URL is
// unset. Matches the integration-coverage plan §1.4 (the api harness uses the
// same host/db). The worker's local dev DB and the test DB share the
// instant_dev_test database.
const DefaultTestDBURL = "postgres://postgres@localhost:5432/instant_dev_test?sslmode=disable"

// SetupTestDB opens a connection to the platform test database, ensures the
// schema subset the worker integration tests need, and returns the *sql.DB
// plus a cleanup function.
//
// It SKIPS (does not fail) the test when TEST_DATABASE_URL is unset AND the
// default local DB is unreachable. This keeps `make gate` / deploy.yml / ci.yml
// green without a DB (those workflows ship no Postgres service container, so the
// ping below misses and the test skips) while still running the real round-trip
// — and crediting this package's own coverage — wherever a Postgres is provided
// (developer machine, coverage.yml's postgres service).
//
// NOTE: this deliberately does NOT short-circuit on `testing.Short()`. The
// coverage.yml job runs `go test ./... -short` against a real Postgres service;
// a `-short` guard here would skip the harness in that job and leave every line
// of this file uncovered, reding the 100%-patch-coverage gate. Gating purely on
// DB reachability matches api/internal/testhelpers.SetupTestDB and keeps the
// `-short`, no-DB workflows green via the ping skip below.
func SetupTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = DefaultTestDBURL
	}

	// Build the connector explicitly via pq.NewConnector rather than sql.Open:
	// sql.Open only validates the (always-"postgres") driver string and never
	// returns an error here, so its error arm would be an untestable dead branch
	// under the patch-coverage gate. pq.NewConnector parses the DSN eagerly and
	// DOES return an error for a malformed DSN — a reachable, tested skip arm.
	connector, err := pq.NewConnector(dsn)
	if err != nil {
		tSkipf(t, "testhelpers.SetupTestDB: parse DSN %q: %v — set TEST_DATABASE_URL to a valid platform DB", dsn, err)
		return nil, func() {}
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		tSkipf(t, "testhelpers.SetupTestDB: ping %q failed: %v — DB not reachable (set TEST_DATABASE_URL or start postgres)", dsn, err)
		return nil, func() {}
	}

	ensureSchema(t, db)

	return db, func() { _ = db.Close() }
}

// ensureSchema applies the subset of the platform schema the worker
// integration tests round-trip against. Every statement is idempotent
// (IF NOT EXISTS / ADD COLUMN IF NOT EXISTS) so it is safe against a DB that
// already has the full api migration set applied (the modal local case) AND
// against a bare DB.
//
// The one thing a bare api-migrated DB can lack (it was a real gap on the dev
// box that authored this) is the deployment_events autopsy partial-unique
// index that deploy_failure_autopsy.go's ON CONFLICT clause depends on — so we
// (re)create it here explicitly.
func ensureSchema(t *testing.T, db *sql.DB) {
	t.Helper()

	stmts := []string{
		`CREATE EXTENSION IF NOT EXISTS pgcrypto`,

		// teams — minimal shape the deploy + entitlement jobs join against.
		`CREATE TABLE IF NOT EXISTS teams (
			id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			name       TEXT,
			plan_tier  TEXT NOT NULL DEFAULT 'hobby',
			status     TEXT NOT NULL DEFAULT 'active',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`ALTER TABLE teams ADD COLUMN IF NOT EXISTS plan_tier TEXT NOT NULL DEFAULT 'hobby'`,
		`ALTER TABLE teams ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'`,
		// is_test_cohort (api migration 067) — the synthetic-cohort skip-guard
		// flag every team-iterating job filters on. Idempotent add so the harness
		// works against a bare DB AND a fully api-migrated one.
		`ALTER TABLE teams ADD COLUMN IF NOT EXISTS is_test_cohort BOOLEAN NOT NULL DEFAULT false`,
		`ALTER TABLE teams ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT`,
		// deletion-lifecycle columns the team-deletion executor + the
		// e2e_cohort_sweep reaper read/write (status flip, tombstone stamp,
		// grace-window scan). Idempotent adds so the harness works on a bare DB
		// AND a fully api-migrated one (where these come from the deletion migs).
		`ALTER TABLE teams ADD COLUMN IF NOT EXISTS deletion_requested_at TIMESTAMPTZ`,
		`ALTER TABLE teams ADD COLUMN IF NOT EXISTS tombstoned_at TIMESTAMPTZ`,

		// resources — the entitlement reconciler reads tier / applied_conn_limit.
		`CREATE TABLE IF NOT EXISTS resources (
			id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			team_id              UUID REFERENCES teams(id) ON DELETE SET NULL,
			token                UUID UNIQUE NOT NULL DEFAULT gen_random_uuid(),
			resource_type        TEXT NOT NULL,
			tier                 TEXT NOT NULL DEFAULT 'anonymous',
			status               TEXT NOT NULL DEFAULT 'active',
			provider_resource_id TEXT,
			applied_conn_limit   INT,
			expires_at           TIMESTAMPTZ,
			created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`ALTER TABLE resources ADD COLUMN IF NOT EXISTS applied_conn_limit INT`,
		`ALTER TABLE resources ADD COLUMN IF NOT EXISTS provider_resource_id TEXT`,
		// Columns the cohort-guarded quota / expiry-warning scans project.
		`ALTER TABLE resources ADD COLUMN IF NOT EXISTS storage_bytes BIGINT NOT NULL DEFAULT 0`,
		`ALTER TABLE resources ADD COLUMN IF NOT EXISTS name TEXT`,
		`ALTER TABLE resources ADD COLUMN IF NOT EXISTS reminders_sent INT NOT NULL DEFAULT 0`,
		`ALTER TABLE resources ADD COLUMN IF NOT EXISTS last_reminder_at TIMESTAMPTZ`,
		`ALTER TABLE resources ADD COLUMN IF NOT EXISTS expiry_reminded_at TIMESTAMPTZ`,
		`ALTER TABLE resources ADD COLUMN IF NOT EXISTS key_prefix TEXT`,

		// users — the expiry-warning + weekly-digest scans join the team's
		// primary user for the recipient address.
		`CREATE TABLE IF NOT EXISTS users (
			id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			team_id    UUID REFERENCES teams(id) ON DELETE CASCADE,
			email      TEXT NOT NULL,
			is_primary BOOLEAN NOT NULL DEFAULT false,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS is_primary BOOLEAN NOT NULL DEFAULT false`,

		// pending_checkouts — the checkout reconciler + billing orphan sweep scan
		// this; both carry team_id for the cohort guard.
		`CREATE TABLE IF NOT EXISTS pending_checkouts (
			subscription_id     TEXT PRIMARY KEY,
			team_id             UUID REFERENCES teams(id) ON DELETE CASCADE,
			customer_email      TEXT,
			plan_tier           TEXT,
			created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
			resolved_at         TIMESTAMPTZ,
			failure_notified_at TIMESTAMPTZ
		)`,

		// payment_grace_periods — the dunning reminder + terminator scan this;
		// both carry team_id for the cohort guard.
		`CREATE TABLE IF NOT EXISTS payment_grace_periods (
			id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			team_id          UUID REFERENCES teams(id) ON DELETE CASCADE,
			subscription_id  TEXT,
			status           TEXT NOT NULL DEFAULT 'active',
			started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
			expires_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
			last_reminder_at TIMESTAMPTZ,
			reminders_sent   INT NOT NULL DEFAULT 0,
			terminated_at    TIMESTAMPTZ
		)`,

		// deployments — the status reconciler + failure autopsy round-trip here.
		`CREATE TABLE IF NOT EXISTS deployments (
			id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			team_id       UUID REFERENCES teams(id) ON DELETE SET NULL,
			provider_id   TEXT,
			status        TEXT NOT NULL DEFAULT 'building',
			error_message TEXT,
			created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`ALTER TABLE deployments ADD COLUMN IF NOT EXISTS provider_id TEXT`,
		`ALTER TABLE deployments ADD COLUMN IF NOT EXISTS error_message TEXT`,
		`ALTER TABLE deployments ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`,
		`CREATE INDEX IF NOT EXISTS idx_deployments_status ON deployments(status)`,

		// deployment_events — the autopsy upsert target. The partial-unique
		// index is what ON CONFLICT (deployment_id, kind) WHERE
		// kind = 'failure_autopsy' resolves against; without it the upsert
		// errors "no unique or exclusion constraint matching the ON CONFLICT".
		`CREATE TABLE IF NOT EXISTS deployment_events (
			id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			deployment_id UUID NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
			kind          TEXT NOT NULL,
			reason        TEXT NOT NULL,
			exit_code     INT,
			event         TEXT NOT NULL DEFAULT '',
			last_lines    JSONB NOT NULL DEFAULT '[]'::jsonb,
			hint          TEXT NOT NULL DEFAULT '',
			created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE INDEX IF NOT EXISTS deployment_events_deployment_id_idx
			ON deployment_events (deployment_id, created_at DESC)`,

		// audit_log — the failure-autopsy backstop emits deploy.failed here so
		// the email forwarder dispatches the failure email.
		`CREATE TABLE IF NOT EXISTS audit_log (
			id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			team_id       UUID REFERENCES teams(id) ON DELETE CASCADE,
			user_id       UUID,
			actor         TEXT NOT NULL DEFAULT 'agent',
			kind          TEXT NOT NULL,
			resource_type TEXT,
			resource_id   UUID,
			summary       TEXT NOT NULL,
			metadata      JSONB,
			created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`ALTER TABLE audit_log ALTER COLUMN team_id DROP NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_audit_team_at ON audit_log (team_id, created_at DESC)`,
	}

	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			tFatalf(t, "testhelpers.ensureSchema: %v\n  SQL: %.140s", err, s)
			return
		}
	}

	ensureAutopsyUniqueIndex(t, db)
}

// ensureAutopsyUniqueIndex guarantees the public.deployment_events table
// carries a partial-unique index on (deployment_id, kind) WHERE
// kind = 'failure_autopsy' — the constraint deploy_failure_autopsy.go's
// ON CONFLICT clause resolves against. A plain
// `CREATE UNIQUE INDEX IF NOT EXISTS <name>` is NOT sufficient here: index
// names are schema-scoped, so if the canonical name is already taken by an
// index on a *different* table (observed on a dev box that had a stray
// deployment_events_hidden table owning deployment_events_autopsy_uniq), the
// IF NOT EXISTS turns into a silent no-op and the real table never gets the
// constraint. So we check for a matching index on the actual table first and
// only create one (under a collision-proof name) when none exists.
func ensureAutopsyUniqueIndex(t *testing.T, db *sql.DB) {
	t.Helper()
	var present bool
	if err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			  FROM pg_index i
			  JOIN pg_class idx  ON idx.oid = i.indexrelid
			  JOIN pg_class tbl  ON tbl.oid = i.indrelid
			  JOIN pg_namespace n ON n.oid = tbl.relnamespace
			 WHERE tbl.relname = 'deployment_events'
			   AND n.nspname  = 'public'
			   AND i.indisunique
			   AND pg_get_indexdef(i.indexrelid) ILIKE '%failure_autopsy%'
		)
	`).Scan(&present); err != nil {
		tFatalf(t, "ensureAutopsyUniqueIndex: probe: %v", err)
		return
	}
	if present {
		return
	}
	// Use a harness-specific name that cannot collide with the canonical
	// production index name on any other table.
	if _, err := db.Exec(`
		CREATE UNIQUE INDEX IF NOT EXISTS deployment_events_autopsy_uniq_itest
			ON public.deployment_events (deployment_id, kind)
			WHERE kind = 'failure_autopsy'
	`); err != nil {
		tFatalf(t, "ensureAutopsyUniqueIndex: create: %v", err)
		return
	}
}

// SeedTeam inserts a team row (plan_tier defaults to "hobby") and returns its
// id. The row is removed by the returned test via t.Cleanup so a re-run of the
// same test against a long-lived local DB does not accumulate rows; the
// ON DELETE CASCADE / SET NULL on dependent tables tidies children.
func SeedTeam(t *testing.T, db *sql.DB, planTier string) uuid.UUID {
	t.Helper()
	if planTier == "" {
		planTier = "hobby"
	}
	id := uuid.New()
	if _, err := db.Exec(
		`INSERT INTO teams (id, name, plan_tier, status) VALUES ($1, $2, $3, 'active')`,
		id, "itest-"+id.String()[:8], planTier,
	); err != nil {
		tFatalf(t, "SeedTeam: %v", err)
		return uuid.Nil
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM teams WHERE id = $1`, id)
	})
	return id
}

// SeedDeployment inserts a deployments row with the given status / provider_id
// and returns its id. providerID may be "" to model the stuck-building case
// (api goroutine died before stamping provider_id). The row (and any
// deployment_events / audit_log children via FK) is cleaned up after the test.
//
// app_id is populated with a unique value: the production deployments table
// (full api migration applied locally) carries a NOT NULL app_id column with no
// default; the bare harness CREATE TABLE above omits it, so SeedDeployment must
// supply it to satisfy both schema shapes.
func SeedDeployment(t *testing.T, db *sql.DB, teamID uuid.UUID, status, providerID string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	var providerArg interface{}
	if providerID != "" {
		providerArg = providerID
	}
	// app_id is NOT NULL (no default) in the prod schema; the bare-harness
	// table lacks the column. Try the prod shape first, fall back to the bare
	// shape so the harness works against either schema.
	_, err := db.Exec(
		`INSERT INTO deployments (id, team_id, app_id, provider_id, status, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, now(), now())`,
		id, teamID, id.String(), providerArg, status,
	)
	if err != nil && isUndefinedColumn(err) {
		_, err = db.Exec(
			`INSERT INTO deployments (id, team_id, provider_id, status, created_at, updated_at)
			 VALUES ($1, $2, $3, $4, now(), now())`,
			id, teamID, providerArg, status,
		)
	}
	if err != nil {
		tFatalf(t, "SeedDeployment: %v", err)
		return uuid.Nil
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM deployments WHERE id = $1`, id)
	})
	return id
}

// SeedResource inserts a resources row (used by the entitlement reconciler
// round-trip) and returns its id + token. appliedConnLimit may be an invalid
// sql.NullInt64 to model the never-re-graded (NULL) case. The row is cleaned
// up after the test.
func SeedResource(
	t *testing.T,
	db *sql.DB,
	teamID uuid.UUID,
	resourceType, tier string,
	appliedConnLimit sql.NullInt64,
) (uuid.UUID, string) {
	t.Helper()
	id := uuid.New()
	token := uuid.New()
	var limitArg interface{}
	if appliedConnLimit.Valid {
		limitArg = appliedConnLimit.Int64
	}
	if _, err := db.Exec(
		`INSERT INTO resources
			(id, team_id, token, resource_type, tier, status, applied_conn_limit, created_at)
		 VALUES ($1, $2, $3, $4, $5, 'active', $6, now())`,
		id, teamID, token, resourceType, tier, limitArg,
	); err != nil {
		tFatalf(t, "SeedResource: %v", err)
		return uuid.Nil, ""
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM resources WHERE id = $1`, id)
	})
	return id, token.String()
}

// DeploymentStatus reads back the status + error_message of a deployments row.
func DeploymentStatus(t *testing.T, db *sql.DB, id uuid.UUID) (status string, errorMessage sql.NullString) {
	t.Helper()
	if err := db.QueryRow(
		`SELECT status, error_message FROM deployments WHERE id = $1`, id,
	).Scan(&status, &errorMessage); err != nil {
		tFatalf(t, "DeploymentStatus: %v", err)
		return "", sql.NullString{}
	}
	return status, errorMessage
}

// AppliedConnLimit reads back resources.applied_conn_limit for a row.
func AppliedConnLimit(t *testing.T, db *sql.DB, id uuid.UUID) sql.NullInt64 {
	t.Helper()
	var v sql.NullInt64
	if err := db.QueryRow(
		`SELECT applied_conn_limit FROM resources WHERE id = $1`, id,
	).Scan(&v); err != nil {
		tFatalf(t, "AppliedConnLimit: %v", err)
		return sql.NullInt64{}
	}
	return v
}

// CountAuditLog returns how many audit_log rows of the given kind reference the
// given deployment id in metadata->>'deploy_id'. Used to assert the
// failure-autopsy deploy.failed emit (and its idempotency).
func CountAuditLog(t *testing.T, db *sql.DB, kind, deployID string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(
		`SELECT count(*) FROM audit_log WHERE kind = $1 AND metadata->>'deploy_id' = $2`,
		kind, deployID,
	).Scan(&n); err != nil {
		tFatalf(t, "CountAuditLog: %v", err)
		return 0
	}
	return n
}

// AutopsyRow reads back the failure_autopsy deployment_events row for a
// deployment. Returns ok=false when no such row exists.
func AutopsyRow(t *testing.T, db *sql.DB, deploymentID uuid.UUID) (reason string, ok bool) {
	t.Helper()
	err := db.QueryRow(
		`SELECT reason FROM deployment_events
		  WHERE deployment_id = $1 AND kind = 'failure_autopsy'`,
		deploymentID,
	).Scan(&reason)
	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		tFatalf(t, "AutopsyRow: %v", err)
		return "", false
	}
	return reason, true
}
