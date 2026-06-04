package jobs

// team_deletion_executor_integration_test.go — REAL-Postgres integration test
// for the team-deletion purge cascade. This is the §5 #1 single highest-value
// data-loss gap: GDPR Article 17 right-to-be-forgotten teardown that, if it
// silently fails or scrubs the wrong rows, is both a compliance breach and a
// data-loss incident (cf. the 2026-06-03 truehomie-db DROP incident class).
//
// The sibling team_deletion_executor_test.go drives Work() with fake S3 / k8s
// clients and asserts the destruction *attempts* against those fakes. THIS test
// seeds a real team whose 30-day grace window has elapsed, with real resources
// (carrying connection_url + key_prefix secrets), real users (carrying PII),
// and a real deployment, then runs Work() against a live platform Postgres with
// provisioner/S3/k8s all nil (fail-open per NewTeamDeletionExecutorWorker) and
// asserts the PERSISTED purge cascade:
//
//   - teams.status flipped deletion_requested → tombstoned, tombstoned_at set,
//   - resources.connection_url scrubbed to NULL, key_prefix blanked to '',
//   - users.email scrubbed to the deleted-<id>@tombstoned.invalid placeholder
//     (NULL would violate the NOT NULL UNIQUE constraint),
//   - a team.tombstoned audit_log row emitted.
//
// The S3 / k8s / gRPC legs are nil-skipped (CI has no bucket / cluster /
// provisioner; the job is fail-open by design). The DB leg — the candidate
// scan (deletion_requested_at + 30d < now()), the status flip, the
// scrub transaction, the audit emit — is REAL and is the integration target.
// A sqlmock test passes whether or not the scrub UPDATE's WHERE team_id = $1
// matched the seeded rows; this proves it did, against the real schema's
// constraints (the users.email NOT NULL UNIQUE that forces the placeholder
// instead of a NULL).
//
// GATING: testhelpers.SetupTestDB skips under -short / no-DB.

import (
	"context"
	"testing"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"instant.dev/worker/internal/testhelpers"
)

func fakeTeamDeletionJob() *river.Job[TeamDeletionExecutorArgs] {
	return &river.Job[TeamDeletionExecutorArgs]{JobRow: &rivertype.JobRow{ID: 1}}
}

// TestIntegration_TeamDeletionExecutor_PurgeCascadeTombstones seeds a team
// past its 30-day grace window with secret-bearing resources, PII-bearing
// users, and a deployment, runs the executor with no external clients, and
// asserts the full persisted tombstone cascade.
func TestIntegration_TeamDeletionExecutor_PurgeCascadeTombstones(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	// Grace window elapsed (40 > 30 days) → the candidate scan selects it.
	teamID := testhelpers.SeedTeamPendingDeletion(t, db, "pro", 40)
	resID := testhelpers.SeedResourceWithSecret(t, db, teamID, "postgres")
	userID := testhelpers.SeedUser(t, db, teamID, "purge-itest-"+teamID.String()[:8]+"@example.com")
	// A deployment so fetchTeamDeployAppIDs has a row to enumerate (the k8s
	// leg is nil-skipped, but the row must not break the pure-DB cascade).
	_ = testhelpers.SeedDeployment(t, db, teamID, "healthy", "app-purge-itest")

	// provisioner / s3 / k8s all nil → fail-open; pure-DB cascade only.
	w := NewTeamDeletionExecutorWorker(db, nil, nil, nil, "")

	if err := w.Work(context.Background(), fakeTeamDeletionJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	// 1. Team tombstoned.
	if got := testhelpers.TeamStatus(t, db, teamID); got != "tombstoned" {
		t.Errorf("team status = %q after purge, want \"tombstoned\"", got)
	}

	// 2. Resource secrets scrubbed.
	connURL, keyPrefix := testhelpers.ResourceSecretFields(t, db, resID)
	if connURL.Valid {
		t.Errorf("resource connection_url = %q after purge, want NULL", connURL.String)
	}
	if keyPrefix != "" {
		t.Errorf("resource key_prefix = %q after purge, want empty", keyPrefix)
	}

	// 3. User PII scrubbed to the tombstone placeholder (NOT the original email,
	//    NOT NULL — the NOT NULL UNIQUE constraint forces the placeholder).
	email := testhelpers.UserEmail(t, db, userID)
	wantPrefix := "deleted-" + userID.String()
	if email != wantPrefix+"@tombstoned.invalid" {
		t.Errorf("user email = %q after purge, want %q@tombstoned.invalid", email, wantPrefix)
	}

	// 4. team.tombstoned audit row emitted.
	if n := testhelpers.CountAuditLogByTeam(t, db, teamID, auditKindTombstoned); n < 1 {
		t.Errorf("%s audit rows = %d, want >= 1", auditKindTombstoned, n)
	}
}

// TestIntegration_TeamDeletionExecutor_WithinGraceNotPurged is the safety
// complement: a team still INSIDE its 30-day grace window (the customer can
// still restore) MUST NOT be tombstoned. This pins the candidate scan's time
// predicate against real data — a regression that ignores the grace window
// would destroy data a customer is legally entitled to recover. Asserting the
// no-op is as important as asserting the purge: the truehomie-class incident is
// "the destroy path ran when it should not have."
func TestIntegration_TeamDeletionExecutor_WithinGraceNotPurged(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	// Only 5 days elapsed (< 30) → still restorable → must NOT be swept.
	teamID := testhelpers.SeedTeamPendingDeletion(t, db, "pro", 5)
	resID := testhelpers.SeedResourceWithSecret(t, db, teamID, "postgres")

	w := NewTeamDeletionExecutorWorker(db, nil, nil, nil, "")

	if err := w.Work(context.Background(), fakeTeamDeletionJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	// Team still in deletion_requested (untouched), NOT tombstoned.
	if got := testhelpers.TeamStatus(t, db, teamID); got != "deletion_requested" {
		t.Errorf("team status = %q for an in-grace team, want \"deletion_requested\" (must not be swept)", got)
	}

	// Resource secret still present (NOT scrubbed).
	connURL, _ := testhelpers.ResourceSecretFields(t, db, resID)
	if !connURL.Valid || connURL.String == "" {
		t.Error("resource connection_url was scrubbed for an in-grace team — the grace window predicate failed (data-loss regression)")
	}

	// No tombstone audit.
	if n := testhelpers.CountAuditLogByTeam(t, db, teamID, auditKindTombstoned); n != 0 {
		t.Errorf("%s audit rows = %d for an in-grace team, want 0", auditKindTombstoned, n)
	}
}
