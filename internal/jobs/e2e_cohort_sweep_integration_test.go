package jobs

// e2e_cohort_sweep_integration_test.go — REAL-Postgres proof that the
// e2e_cohort_sweep reaper purges ONLY stale synthetic test-cohort teams and
// leaves a fresh cohort team AND a normal (non-cohort) team completely
// untouched.
//
// This is the safety-critical assertion: the job runs a DESTRUCTIVE per-team
// teardown (deprovision + scrub PII + tombstone). A regression that widened
// the candidate query — dropping the is_test_cohort filter, the TTL, or the
// status guard — would tombstone real customer teams. So the test seeds three
// teams and asserts the surgical outcome:
//
//   - STALE cohort team (is_test_cohort=true, created 3h ago)  → tombstoned,
//     resource secrets scrubbed, user PII scrubbed, audit row emitted.
//   - FRESH cohort team (is_test_cohort=true, created 5m ago)  → UNTOUCHED
//     (younger than the 2h TTL — a CI run may still be using it).
//   - NORMAL team (is_test_cohort=false, created 3h ago)       → UNTOUCHED
//     (the is_test_cohort filter is the only thing protecting real customers).
//
// The executor's S3 / k8s / gRPC legs are nil-skipped (fail-open per
// NewTeamDeletionExecutorWorker — CI has no bucket/cluster/provisioner). The
// DB cascade (candidate scan, deletion_pending flip, scrub tx, audit emit) is
// REAL and is the integration target.
//
// GATING: testhelpers.SetupTestDB skips under -short / no-DB, so this runs
// only where a real platform Postgres (with api mig 067 teams.is_test_cohort
// applied) is supplied via TEST_DATABASE_URL — same posture as the sibling
// *_integration_test.go files.

import (
	"context"
	"testing"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"instant.dev/worker/internal/testhelpers"
)

func fakeCohortSweepJob() *river.Job[E2ECohortSweepArgs] {
	return &river.Job[E2ECohortSweepArgs]{JobRow: &rivertype.JobRow{ID: 1}}
}

// TestIntegration_E2ECohortSweep_PurgesOnlyStaleCohort is the load-bearing
// safety test: only the stale cohort team is swept; the fresh cohort team and
// the normal team are left fully intact.
func TestIntegration_E2ECohortSweep_PurgesOnlyStaleCohort(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	// Neutralize cohort-flag debris from prior runs on the shared local DB —
	// the sweep scans ALL cohort teams under a batch limit, so leftover debris
	// would crowd out the seeded team. Safe: no real team is ever is_test_cohort.
	testhelpers.ClearPreexistingTestCohortTeams(t, db)

	// 1. STALE cohort team — 3h old (> 2h TTL), is_test_cohort=true.
	stale := testhelpers.SeedCohortTeamWithAge(t, db, 3*time.Hour)
	staleRes := testhelpers.SeedResourceWithSecret(t, db, stale, "postgres")
	staleUser := testhelpers.SeedUser(t, db, stale, "cohort-stale-"+stale.String()[:8]+"@example.com")

	// 2. FRESH cohort team — 5m old (< 2h TTL), is_test_cohort=true. A CI run
	//    may still be using it; the sweep must NOT reap it.
	fresh := testhelpers.SeedCohortTeamWithAge(t, db, 5*time.Minute)
	freshRes := testhelpers.SeedResourceWithSecret(t, db, fresh, "postgres")

	// 3. NORMAL team — 3h old but is_test_cohort=false. A real customer; the
	//    cohort filter is the ONLY thing keeping the destroy path off it.
	normal := testhelpers.SeedTeam(t, db, "pro")
	normalRes := testhelpers.SeedResourceWithSecret(t, db, normal, "postgres")
	normalUser := testhelpers.SeedUser(t, db, normal, "cohort-normal-"+normal.String()[:8]+"@example.com")

	// provisioner / s3 / k8s all nil → fail-open; pure-DB cascade only.
	executor := NewTeamDeletionExecutorWorker(db, nil, nil, nil, "")
	w := NewE2ECohortSweepWorker(db, executor)

	if err := w.Work(context.Background(), fakeCohortSweepJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	// ── STALE cohort team: fully purged ──────────────────────────────────────
	if got := testhelpers.TeamStatus(t, db, stale); got != "tombstoned" {
		t.Errorf("stale cohort team status = %q, want \"tombstoned\" (must be swept)", got)
	}
	connURL, keyPrefix := testhelpers.ResourceSecretFields(t, db, staleRes)
	if connURL.Valid {
		t.Errorf("stale cohort resource connection_url = %q, want NULL (scrubbed)", connURL.String)
	}
	if keyPrefix != "" {
		t.Errorf("stale cohort resource key_prefix = %q, want empty (scrubbed)", keyPrefix)
	}
	email := testhelpers.UserEmail(t, db, staleUser)
	wantPrefix := "deleted-" + staleUser.String()
	if email != wantPrefix+"@tombstoned.invalid" {
		t.Errorf("stale cohort user email = %q, want %q@tombstoned.invalid (scrubbed)", email, wantPrefix)
	}
	if n := testhelpers.CountAuditLogByTeam(t, db, stale, auditKindTombstoned); n < 1 {
		t.Errorf("stale cohort %s audit rows = %d, want >= 1", auditKindTombstoned, n)
	}

	// ── FRESH cohort team: UNTOUCHED (within TTL) ────────────────────────────
	if got := testhelpers.TeamStatus(t, db, fresh); got != "active" {
		t.Errorf("fresh cohort team status = %q, want \"active\" (within TTL — must NOT be swept)", got)
	}
	freshConn, _ := testhelpers.ResourceSecretFields(t, db, freshRes)
	if !freshConn.Valid || freshConn.String == "" {
		t.Error("fresh cohort resource connection_url was scrubbed — the TTL guard failed (would reap an in-flight CI account)")
	}
	if n := testhelpers.CountAuditLogByTeam(t, db, fresh, auditKindTombstoned); n != 0 {
		t.Errorf("fresh cohort %s audit rows = %d, want 0 (within TTL)", auditKindTombstoned, n)
	}

	// ── NORMAL team: UNTOUCHED (not cohort) ──────────────────────────────────
	if got := testhelpers.TeamStatus(t, db, normal); got != "active" {
		t.Errorf("normal team status = %q, want \"active\" (NOT a cohort team — must NEVER be swept)", got)
	}
	normalConn, _ := testhelpers.ResourceSecretFields(t, db, normalRes)
	if !normalConn.Valid || normalConn.String == "" {
		t.Error("normal team resource connection_url was scrubbed — the is_test_cohort filter failed (real-customer data-loss regression)")
	}
	normalEmail := testhelpers.UserEmail(t, db, normalUser)
	if normalEmail != "cohort-normal-"+normal.String()[:8]+"@example.com" {
		t.Errorf("normal team user email = %q, want the original (NOT scrubbed)", normalEmail)
	}
	if n := testhelpers.CountAuditLogByTeam(t, db, normal, auditKindTombstoned); n != 0 {
		t.Errorf("normal team %s audit rows = %d, want 0 (must never be tombstoned)", auditKindTombstoned, n)
	}
}

// TestIntegration_E2ECohortSweep_NilExecutorNoOps proves the fail-open arm: a
// nil executor disables the job (no candidate is touched), matching every
// other worker's posture when an external dependency is unavailable.
func TestIntegration_E2ECohortSweep_NilExecutorNoOps(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	testhelpers.ClearPreexistingTestCohortTeams(t, db)
	stale := testhelpers.SeedCohortTeamWithAge(t, db, 3*time.Hour)

	w := NewE2ECohortSweepWorker(db, nil) // nil executor → fail-open skip
	if err := w.Work(context.Background(), fakeCohortSweepJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := testhelpers.TeamStatus(t, db, stale); got != "active" {
		t.Errorf("stale cohort team status = %q with nil executor, want \"active\" (job must no-op)", got)
	}
}
