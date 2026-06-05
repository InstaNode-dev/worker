package testhelpers

// cohort_smoke_test.go — in-package coverage for the Tier-2 (synthetic
// test-cohort skip-guard) harness extensions in cohort.go.
//
// WHY THIS EXISTS
// ---------------
// Same per-package coverage-attribution reason as testhelpers_smoke_test.go and
// billing_deletion_smoke_test.go: the Set*/Seed*/read helpers in cohort.go live
// in a non-_test.go file so the jobs-package cohort skip-guard integration tests
// can import them, but Go credits their line coverage to `internal/jobs` (the
// caller's package), not `internal/testhelpers`. Without an in-package test,
// every line of cohort.go reads as 0% in diff-cover and reds the
// 100%-patch-coverage gate (the exact failure on PR #90, identical to the PR #87
// testhelpers.go and PR #89 billing_deletion.go failures). This file mirrors
// billing_deletion_smoke_test.go, the established convention for giving a
// test-harness package its own coverage.
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
// helpers' columns require, incl. mig 067 teams.is_test_cohort) + any developer
// DB with the migrated schema.

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestCohortErrorArms drives every fallible cohort helper against a CLOSED
// *sql.DB so each Exec/QueryRow fails — covering the tFatalf error arm (via the
// recording seam) of SetTeamTestCohort, SeedPrimaryUser, SeedExpiringFreeResource,
// SeedOverQuotaResource, SeedAbandonedCheckout, SeedActiveGracePeriod,
// GraceRemindersSent and CheckoutNotified.
func TestCohortErrorArms(t *testing.T) {
	closed, err := sql.Open("postgres", "postgres://postgres@127.0.0.1:1/x?sslmode=disable")
	if err != nil {
		t.Fatalf("open placeholder db: %v", err)
	}
	_ = closed.Close() // every subsequent Exec/QueryRow now errors.

	fatals, _ := swapFatalSeams(t)
	id := uuid.New()

	// SetTeamTestCohort returns nothing; the closed-DB Exec error fires tFatalf.
	SetTeamTestCohort(t, closed, id, true)

	if got := SeedCohortTeamWithAge(t, closed, 3*time.Hour); got != uuid.Nil {
		t.Fatalf("SeedCohortTeamWithAge on closed db = %v, want Nil", got)
	}

	// ClearPreexistingTestCohortTeams returns nothing; the closed-DB Exec
	// error fires tFatalf.
	ClearPreexistingTestCohortTeams(t, closed)

	if got := SeedPrimaryUser(t, closed, id, "smoke"); got != uuid.Nil {
		t.Fatalf("SeedPrimaryUser on closed db = %v, want Nil", got)
	}
	if got := SeedExpiringFreeResource(t, closed, id, "postgres", time.Hour); got != uuid.Nil {
		t.Fatalf("SeedExpiringFreeResource on closed db = %v, want Nil", got)
	}
	if got := SeedOverQuotaResource(t, closed, id, "postgres", "free", 1<<20); got != uuid.Nil {
		t.Fatalf("SeedOverQuotaResource on closed db = %v, want Nil", got)
	}
	if got := SeedAbandonedCheckout(t, closed, id, "x@example.com"); got != "" {
		t.Fatalf("SeedAbandonedCheckout on closed db = %q, want \"\"", got)
	}
	if got := SeedActiveGracePeriod(t, closed, id, time.Hour); got != uuid.Nil {
		t.Fatalf("SeedActiveGracePeriod on closed db = %v, want Nil", got)
	}
	if got := GraceRemindersSent(t, closed, id); got != -1 {
		t.Fatalf("GraceRemindersSent on closed db = %d, want -1", got)
	}
	if got := CheckoutNotified(t, closed, "sub_x"); got {
		t.Fatal("CheckoutNotified on closed db = true, want false")
	}

	// 10 distinct fallible call paths each recorded at least one Fatalf.
	if len(*fatals) < 10 {
		t.Fatalf("expected >=10 recorded Fatalf arms against the closed DB, got %d: %v", len(*fatals), *fatals)
	}
}

// TestIntegration_CohortRoundTrip drives every DB-backed cohort helper against a
// real Postgres so the happy paths + every read-back branch carry real line
// coverage. Mirrors the jobs-package cohort skip-guard integration tests but
// exercises the harness in its OWN package so the coverage is attributed here.
func TestIntegration_CohortRoundTrip(t *testing.T) {
	db, cleanup := SetupTestDB(t)
	defer cleanup()
	ensureSchema(t, db) // ensure teams/resources/users/pending_checkouts/grace tables exist (idempotent).

	team := SeedTeam(t, db, "free")
	if team == uuid.Nil {
		t.Fatal("SeedTeam returned nil uuid")
	}

	// SetTeamTestCohort: flip on, then off — both arms run the Exec happy path.
	SetTeamTestCohort(t, db, team, true)
	if got := isTestCohort(t, db, team); !got {
		t.Fatal("SetTeamTestCohort(true) did not set is_test_cohort")
	}
	SetTeamTestCohort(t, db, team, false)
	if got := isTestCohort(t, db, team); got {
		t.Fatal("SetTeamTestCohort(false) did not clear is_test_cohort")
	}

	// SeedCohortTeamWithAge: a stale (3h-old) is_test_cohort team — the
	// e2e_cohort_sweep candidate shape. Read back the flag + age to exercise
	// the happy path.
	staleCohort := SeedCohortTeamWithAge(t, db, 3*time.Hour)
	if staleCohort == uuid.Nil {
		t.Fatal("SeedCohortTeamWithAge returned nil uuid")
	}
	if got := isTestCohort(t, db, staleCohort); !got {
		t.Fatal("SeedCohortTeamWithAge did not set is_test_cohort=true")
	}

	// ClearPreexistingTestCohortTeams flips the flag off across the DB — the
	// staleCohort team above must read back is_test_cohort=false afterwards.
	ClearPreexistingTestCohortTeams(t, db)
	if got := isTestCohort(t, db, staleCohort); got {
		t.Fatal("ClearPreexistingTestCohortTeams did not clear is_test_cohort")
	}

	// SeedPrimaryUser: inserts an is_primary=true user with a globally-unique addr.
	primary := SeedPrimaryUser(t, db, team, "cohort-primary")
	if primary == uuid.Nil {
		t.Fatal("SeedPrimaryUser returned nil uuid")
	}

	// SeedExpiringFreeResource: an active free resource expiring soon.
	expRes := SeedExpiringFreeResource(t, db, team, "postgres", 2*time.Hour)
	if expRes == uuid.Nil {
		t.Fatal("SeedExpiringFreeResource returned nil uuid")
	}

	// SeedOverQuotaResource: an active resource over its tier limit.
	quotaRes := SeedOverQuotaResource(t, db, team, "postgres", "free", 100<<20)
	if quotaRes == uuid.Nil {
		t.Fatal("SeedOverQuotaResource returned nil uuid")
	}

	// SeedAbandonedCheckout + CheckoutNotified: a far-past unresolved checkout
	// whose failure_notified_at starts NULL (the un-notified read-back arm).
	subID := SeedAbandonedCheckout(t, db, team, "cohort-checkout@example.com")
	if subID == "" {
		t.Fatal("SeedAbandonedCheckout returned empty subscription id")
	}
	if CheckoutNotified(t, db, subID) {
		t.Fatal("CheckoutNotified on a fresh abandoned checkout = true, want false")
	}
	// Stamp failure_notified_at then re-read — the notified read-back arm.
	if _, err := db.Exec(
		`UPDATE pending_checkouts SET failure_notified_at = now() WHERE subscription_id = $1`, subID,
	); err != nil {
		t.Fatalf("stamp failure_notified_at: %v", err)
	}
	if !CheckoutNotified(t, db, subID) {
		t.Fatal("CheckoutNotified after stamping failure_notified_at = false, want true")
	}

	// SeedActiveGracePeriod (reminder candidate: expiresIn > 0) + GraceRemindersSent
	// reads back the initial 0 counter (the cohort-skip assertion target).
	grace := SeedActiveGracePeriod(t, db, team, 24*time.Hour)
	if grace == uuid.Nil {
		t.Fatal("SeedActiveGracePeriod returned nil uuid")
	}
	if n := GraceRemindersSent(t, db, grace); n != 0 {
		t.Fatalf("GraceRemindersSent on a fresh grace period = %d, want 0", n)
	}
	// Terminator candidate: expiresIn < 0 (clock elapsed) — the other call shape.
	// Seeded onto a SECOND team because the prod schema (full api migrations
	// applied in coverage.yml) carries the uq_payment_grace_team_active partial
	// unique index: at most one active grace period per team. A second active
	// row on `team` would 23505 there even though the bare harness CREATE TABLE
	// has no such constraint.
	team2 := SeedTeam(t, db, "free")
	if team2 == uuid.Nil {
		t.Fatal("SeedTeam(team2) returned nil uuid")
	}
	graceElapsed := SeedActiveGracePeriod(t, db, team2, -24*time.Hour)
	if graceElapsed == uuid.Nil {
		t.Fatal("SeedActiveGracePeriod(elapsed) returned nil uuid")
	}
	if n := GraceRemindersSent(t, db, graceElapsed); n != 0 {
		t.Fatalf("GraceRemindersSent on elapsed grace period = %d, want 0", n)
	}
}

// isTestCohort reads back teams.is_test_cohort for an assertion in the round-trip
// test. Local to the smoke test; not part of the shared harness surface.
func isTestCohort(t *testing.T, db *sql.DB, teamID uuid.UUID) bool {
	t.Helper()
	var flagged bool
	if err := db.QueryRow(
		`SELECT is_test_cohort FROM teams WHERE id = $1`, teamID,
	).Scan(&flagged); err != nil {
		t.Fatalf("isTestCohort: %v", err)
	}
	return flagged
}
