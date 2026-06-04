package testhelpers

// billing_deletion_smoke_test.go — in-package coverage for the Tier-1
// (billing-reconciler / team-deletion) harness extensions in billing_deletion.go.
//
// WHY THIS EXISTS
// ---------------
// Same per-package coverage-attribution reason as testhelpers_smoke_test.go: the
// Seed*/read helpers in billing_deletion.go live in a non-_test.go file so the
// jobs-package integration tests can import them, but Go credits their line
// coverage to `internal/jobs` (the caller's package), not `internal/testhelpers`.
// Without an in-package test, every line of billing_deletion.go reads as 0% in
// diff-cover and reds the 100%-patch-coverage gate (the exact failure on PR #89,
// identical to the PR #87 testhelpers.go failure). This file mirrors
// testhelpers_smoke_test.go, the established convention for giving a test-harness
// package its own coverage.
//
// Each helper's t.Fatalf-equivalent failure arm routes through the package's
// tFatalf seam (the same seam testhelpers.go uses); the error arms are exercised
// by swapping that seam for a recording stub and driving the arm with a
// deliberately-closed DB (a test seam, not a behavioural change — real callers
// still get a genuine t.Fatalf via the default seam).
//
// GATING: the DB-backed test routes through SetupTestDB, which skips when no
// Postgres is reachable — so it skips cleanly on the no-DB workflows
// (deploy.yml `-short`, ci.yml `-race`) and runs against coverage.yml's postgres
// service (which exports TEST_DATABASE_URL and applies the api migrations these
// helpers' columns require) + any developer DB with the migrated schema.

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
)

// TestLowSortingTeamID covers lowSortingTeamID: the result must (a) have its
// first 10 bytes zeroed (so it sorts at the front of an ORDER BY id scan) and
// (b) be unique across calls (random low 6 bytes). Pure logic, no DB — runs on
// every workflow.
func TestLowSortingTeamID(t *testing.T) {
	t.Parallel()
	a := lowSortingTeamID()
	b := lowSortingTeamID()
	for i := 0; i < 10; i++ {
		if a[i] != 0 {
			t.Fatalf("lowSortingTeamID: byte %d = %d, want 0 (must sort first)", i, a[i])
		}
	}
	if a == b {
		t.Fatal("lowSortingTeamID returned identical ids on two calls — not unique")
	}
	if a == uuid.Nil {
		t.Fatal("lowSortingTeamID returned the nil UUID (low bytes not randomized)")
	}
}

// TestBillingDeletionErrorArms drives every fallible helper against a CLOSED
// *sql.DB so each Exec/QueryRow fails — covering the tFatalf error arm (via the
// recording seam) of SeedTeamWithSubscription, SetTeamSubscription, TeamPlanTier,
// TeamStatus, CountAuditLogByTeam, SeedTeamPendingDeletion, SeedResourceWithSecret,
// ResourceSecretFields, SeedUser and UserEmail. SeedUser additionally exercises
// its fallback INSERT (the first Exec errors → the second Exec is attempted →
// also errors → tFatalf), covering the fallback branch's failure path.
func TestBillingDeletionErrorArms(t *testing.T) {
	closed, err := sql.Open("postgres", "postgres://postgres@127.0.0.1:1/x?sslmode=disable")
	if err != nil {
		t.Fatalf("open placeholder db: %v", err)
	}
	_ = closed.Close() // every subsequent Exec/QueryRow now errors.

	fatals, _ := swapFatalSeams(t)
	id := uuid.New()

	if got := SeedTeamWithSubscription(t, closed, "pro", "sub_x"); got != uuid.Nil {
		t.Fatalf("SeedTeamWithSubscription on closed db = %v, want Nil", got)
	}
	SetTeamSubscription(t, closed, id, "sub_y")
	if got := TeamPlanTier(t, closed, id); got != "" {
		t.Fatalf("TeamPlanTier on closed db = %q, want \"\"", got)
	}
	if got := TeamStatus(t, closed, id); got != "" {
		t.Fatalf("TeamStatus on closed db = %q, want \"\"", got)
	}
	if got := CountAuditLogByTeam(t, closed, id, "team.tombstoned"); got != 0 {
		t.Fatalf("CountAuditLogByTeam on closed db = %d, want 0", got)
	}
	if got := SeedTeamPendingDeletion(t, closed, "pro", 40); got != uuid.Nil {
		t.Fatalf("SeedTeamPendingDeletion on closed db = %v, want Nil", got)
	}
	if got := SeedResourceWithSecret(t, closed, id, "postgres"); got != uuid.Nil {
		t.Fatalf("SeedResourceWithSecret on closed db = %v, want Nil", got)
	}
	if connURL, keyPrefix := ResourceSecretFields(t, closed, id); connURL.Valid || keyPrefix != "" {
		t.Fatalf("ResourceSecretFields on closed db = (%+v, %q), want (NULL, \"\")", connURL, keyPrefix)
	}
	if got := SeedUser(t, closed, id, "x@example.com"); got != uuid.Nil {
		t.Fatalf("SeedUser on closed db = %v, want Nil", got)
	}
	if got := UserEmail(t, closed, id); got != "" {
		t.Fatalf("UserEmail on closed db = %q, want \"\"", got)
	}

	// 10 distinct fallible call paths each recorded at least one Fatalf.
	if len(*fatals) < 10 {
		t.Fatalf("expected >=10 recorded Fatalf arms against the closed DB, got %d: %v", len(*fatals), *fatals)
	}
}

// TestIntegration_BillingDeletionRoundTrip drives every DB-backed helper against
// a real Postgres so the happy paths + both default-tier branches (planTier=="")
// and the SeedUser fallback-INSERT branch carry real line coverage. Mirrors the
// jobs-package integration tests (billing_reconciler / team_deletion_executor)
// but exercises the harness in its OWN package so the coverage is attributed
// here.
func TestIntegration_BillingDeletionRoundTrip(t *testing.T) {
	db, cleanup := SetupTestDB(t)
	defer cleanup()
	ensureSchema(t, db) // ensure the shared deployment_events/etc tables exist (idempotent).

	// Unique per-run subscription suffix so a prior crash that skipped t.Cleanup
	// can't collide with the teams.stripe_customer_id UNIQUE constraint on rerun.
	sfx := uuid.NewString()

	// SeedTeamWithSubscription: explicit tier; then SetTeamSubscription rewrites
	// the subscription id, then TeamPlanTier reads the tier back.
	teamA := SeedTeamWithSubscription(t, db, "pro", "sub_round_a_"+sfx)
	if teamA == uuid.Nil {
		t.Fatal("SeedTeamWithSubscription returned nil uuid")
	}
	SetTeamSubscription(t, db, teamA, "sub_round_a2_"+sfx)
	if got := TeamPlanTier(t, db, teamA); got != "pro" {
		t.Fatalf("TeamPlanTier(teamA) = %q, want pro", got)
	}
	if got := TeamStatus(t, db, teamA); got != "active" {
		t.Fatalf("TeamStatus(teamA) = %q, want active", got)
	}

	// "" -> default-pro branch of SeedTeamWithSubscription.
	teamDefault := SeedTeamWithSubscription(t, db, "", "sub_round_default_"+sfx)
	if got := TeamPlanTier(t, db, teamDefault); got != "pro" {
		t.Fatalf("TeamPlanTier(teamDefault, blank-tier) = %q, want pro (default)", got)
	}

	// SeedTeamPendingDeletion: explicit tier + the "" -> default-pro branch.
	teamDel := SeedTeamPendingDeletion(t, db, "hobby", 40)
	if teamDel == uuid.Nil {
		t.Fatal("SeedTeamPendingDeletion returned nil uuid")
	}
	if got := TeamStatus(t, db, teamDel); got != "deletion_requested" {
		t.Fatalf("TeamStatus(teamDel) = %q, want deletion_requested", got)
	}
	if got := TeamPlanTier(t, db, teamDel); got != "hobby" {
		t.Fatalf("TeamPlanTier(teamDel) = %q, want hobby", got)
	}
	teamDelDefault := SeedTeamPendingDeletion(t, db, "", 40)
	if got := TeamPlanTier(t, db, teamDelDefault); got != "pro" {
		t.Fatalf("TeamPlanTier(teamDelDefault, blank-tier) = %q, want pro (default)", got)
	}

	// SeedResourceWithSecret + ResourceSecretFields: the secret fields read back
	// non-empty before any tombstone scrub.
	resID := SeedResourceWithSecret(t, db, teamDel, "postgres")
	if resID == uuid.Nil {
		t.Fatal("SeedResourceWithSecret returned nil uuid")
	}
	connURL, keyPrefix := ResourceSecretFields(t, db, resID)
	if !connURL.Valid || connURL.String == "" {
		t.Fatalf("ResourceSecretFields connURL = %+v, want a non-empty value", connURL)
	}
	if keyPrefix == "" {
		t.Fatal("ResourceSecretFields keyPrefix is empty, want a seeded prefix")
	}

	// SeedUser (happy path: first INSERT with github_id succeeds) + UserEmail.
	userID := SeedUser(t, db, teamDel, "smoke-user-"+teamDel.String()[:8]+"@example.com")
	if userID == uuid.Nil {
		t.Fatal("SeedUser returned nil uuid")
	}
	if email := UserEmail(t, db, userID); email == "" {
		t.Fatal("UserEmail returned empty for a seeded user")
	}

	// SeedUser fallback-INSERT branch: drop github_id so the first INSERT 42703s
	// and the fallback INSERT (without github_id) runs and succeeds. Restored
	// after fn.
	withoutGithubIDColumn(t, db, func() {
		bareUser := SeedUser(t, db, teamDel, "smoke-user-bare-"+teamDel.String()[:8]+"@example.com")
		if bareUser == uuid.Nil {
			t.Fatal("SeedUser fallback-INSERT branch returned nil id")
		}
		if email := UserEmail(t, db, bareUser); email == "" {
			t.Fatal("UserEmail returned empty for the fallback-INSERT user")
		}
	})

	// CountAuditLogByTeam: write a team-scoped audit row then count it (found
	// arm), and assert a zero count for an unrelated kind (not-found arm).
	if _, err := db.Exec(
		`INSERT INTO audit_log (team_id, actor, kind, summary)
		 VALUES ($1, 'worker', 'team.tombstoned', 'tombstoned')`, teamDel,
	); err != nil {
		t.Fatalf("insert audit_log row: %v", err)
	}
	if n := CountAuditLogByTeam(t, db, teamDel, "team.tombstoned"); n != 1 {
		t.Fatalf("CountAuditLogByTeam(found) = %d, want 1", n)
	}
	if n := CountAuditLogByTeam(t, db, teamDel, "subscription.canceled"); n != 0 {
		t.Fatalf("CountAuditLogByTeam(absent kind) = %d, want 0", n)
	}
}

// withoutGithubIDColumn drops users.github_id for the duration of fn, then
// restores it (nullable — the harness only needs the column to exist for the
// happy path). If the column was already absent (bare harness DB), fn runs
// unchanged. Mirrors withoutAppIDColumn in testhelpers_smoke_test.go.
func withoutGithubIDColumn(t *testing.T, db *sql.DB, fn func()) {
	t.Helper()
	var had bool
	if err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			 WHERE table_name = 'users' AND column_name = 'github_id')`).Scan(&had); err != nil {
		t.Fatalf("probe github_id column: %v", err)
	}
	if had {
		if _, err := db.Exec(`ALTER TABLE users DROP COLUMN github_id`); err != nil {
			t.Fatalf("drop github_id: %v", err)
		}
		defer func() {
			if _, err := db.Exec(`ALTER TABLE users ADD COLUMN IF NOT EXISTS github_id TEXT`); err != nil {
				t.Fatalf("restore github_id: %v", err)
			}
		}()
	}
	fn()
}
