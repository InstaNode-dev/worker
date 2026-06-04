package jobs

// deploy_reconcile_integration_test.go — REAL-Postgres integration tests for
// the silent-deploy-failure triad (2026-05-30 incident class). These are the
// highest-value worker integration tests per INTEGRATION-COVERAGE-PLAN-
// 2026-06-04.md §5 #2: deploy_status_reconcile + deploy_failure_autopsy were
// fake-tested only, which is exactly the regression class that hides — a sqlmock
// expectation passes whether or not the real SQL is valid against the live
// schema.
//
// Unlike the sibling *_test.go files in this package (which drive the SQL with
// go-sqlmock), these tests seed real rows in a live platform Postgres, run the
// job's Work()/captureDeploymentAutopsy against that DB, and assert the actual
// row state transition + side effects (status flip, deployment_events upsert,
// audit_log deploy.failed emit + its idempotency, error_message stamp).
//
// The k8s leg stays faked (no live cluster) — the fakeDeployStatusK8s /
// fakeAutopsyK8sCov helpers from deploy_lifecycle_coverage_test.go +
// deploy_status_reconcile_job_failed_test.go supply the cluster state. The DB
// leg is REAL. That split is the design the plan prescribes (§3 wave 3): "the
// deploy/k8s jobs use a fake clientset for the k8s leg but a real DB for the
// row mutation."
//
// GATING: testhelpers.SetupTestDB skips when no DB is reachable, so `make gate`
// (deploy.yml) and ci.yml (no DB service) skip these cleanly. They run locally
// against postgres://postgres@localhost:5432/instant_dev_test and wherever a
// TEST_DATABASE_URL is supplied (developer DB, coverage.yml's postgres service).

import (
	"context"
	"testing"
	"time"

	"instant.dev/worker/internal/testhelpers"
)

// TestIntegration_DeployStatusReconcile_JobFailedFlipsToFailed is the PRIMARY
// real-DB guard for the silent-deploy-failure bug class (Bug A of the
// 2026-05-30 triad). Setup mirrors the user's incident:
//
//   - a deployments row at status='building' (api goroutine crashed mid-build
//     or never stamped the terminal status)
//   - the runtime Deployment was never created → GetDeployment returns NotFound
//   - the kaniko build Job is Failed (BackoffLimitExceeded) but survives within
//     its TTLSecondsAfterFinished window
//
// The fixed reconciler MUST: (1) flip the REAL row to 'failed', and (2) write a
// REAL deployment_events failure_autopsy row (the in-sweep capture). The
// sqlmock sibling pins the SQL string; THIS pins that the SQL is valid against
// the live schema and the row actually transitions — a sqlmock expectation
// would pass even if the UPDATE's WHERE clause silently matched zero rows.
func TestIntegration_DeployStatusReconcile_JobFailedFlipsToFailed(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	teamID := testhelpers.SeedTeam(t, db, "pro")
	// provider_id "app-itest1" → namespace "instant-deploy-itest1".
	deployID := testhelpers.SeedDeployment(t, db, teamID, deployStatusBuilding, "app-itest1")

	k8s := newFakeDeployStatusK8s()
	// Runtime Deployment missing (build never reached apply). Build Job Failed.
	k8s.jobs["instant-deploy-itest1|build-itest1"] = jobBackoffLimitExceeded()

	w := NewDeployStatusReconciler(db, k8s).WithAutopsyK8s(&fakeAutopsyK8sCov{})
	if err := w.Work(context.Background(), fakeRiverJob[DeployStatusReconcileArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	gotStatus, _ := testhelpers.DeploymentStatus(t, db, deployID)
	if gotStatus != deployStatusFailed {
		t.Errorf("deployment status = %q, want %q (build Job Failed must flip the real row)", gotStatus, deployStatusFailed)
	}

	// The in-sweep autopsy must have written a REAL deployment_events row.
	if _, ok := testhelpers.AutopsyRow(t, db, deployID); !ok {
		t.Error("no failure_autopsy deployment_events row written — the in-sweep capture did not round-trip to the DB")
	}
}

// TestIntegration_DeployStatusReconcile_HealthyTransition asserts the happy
// path round-trips: a 'building' row whose runtime Deployment is now healthy
// (AvailableReplicas>=1) is flipped to 'healthy' in the REAL DB. This pins that
// the updateStatus UPDATE's WHERE status IN (...) guard actually matches the
// row (a sqlmock test cannot catch a guard that excludes the live row).
func TestIntegration_DeployStatusReconcile_HealthyTransition(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	teamID := testhelpers.SeedTeam(t, db, "hobby")
	deployID := testhelpers.SeedDeployment(t, db, teamID, deployStatusBuilding, "app-itest2")

	k8s := newFakeDeployStatusK8s()
	k8s.objs["instant-deploy-itest2|app-itest2"] = newHealthyDeployment()

	w := NewDeployStatusReconciler(db, k8s)
	if err := w.Work(context.Background(), fakeRiverJob[DeployStatusReconcileArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	gotStatus, _ := testhelpers.DeploymentStatus(t, db, deployID)
	if gotStatus != deployStatusHealthy {
		t.Errorf("deployment status = %q, want %q", gotStatus, deployStatusHealthy)
	}
}

// TestIntegration_DeployStatusReconcile_StuckBuildingReaped covers sweep
// finding #5 (Bug A's tier-cap leak): a 'building' row with an EMPTY
// provider_id whose age exceeds stuckBuildingGrace is reaped to 'failed' with
// the stuck-building error_message stamped. This exercises reapStuckBuilding's
// double-guarded UPDATE against the live schema — the WHERE provider_id IS NULL
// OR provider_id = ” guard must match the real NULL row.
func TestIntegration_DeployStatusReconcile_StuckBuildingReaped(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	teamID := testhelpers.SeedTeam(t, db, "hobby")
	// Empty provider_id → SeedDeployment writes NULL.
	deployID := testhelpers.SeedDeployment(t, db, teamID, deployStatusBuilding, "")

	// Age the row past the 15m grace window so the reaper fires.
	if _, err := db.Exec(
		`UPDATE deployments SET created_at = $1 WHERE id = $2`,
		time.Now().Add(-stuckBuildingGrace-time.Minute), deployID,
	); err != nil {
		t.Fatalf("age row: %v", err)
	}

	// k8s is present but the row has no provider_id, so no namespace is
	// derivable — the reaper path runs without any k8s Get.
	w := NewDeployStatusReconciler(db, newFakeDeployStatusK8s())
	if err := w.Work(context.Background(), fakeRiverJob[DeployStatusReconcileArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	gotStatus, gotErr := testhelpers.DeploymentStatus(t, db, deployID)
	if gotStatus != deployStatusFailed {
		t.Errorf("stuck-building row status = %q, want %q (reaper must free the tier cap)", gotStatus, deployStatusFailed)
	}
	if !gotErr.Valid || gotErr.String != stuckBuildingReapMessage {
		t.Errorf("error_message = %q (valid=%v), want %q", gotErr.String, gotErr.Valid, stuckBuildingReapMessage)
	}
}

// TestIntegration_DeployFailureAutopsy_UpsertAndAuditEmit is the real-DB guard
// for Bug B of the triad. captureDeploymentAutopsy against a live DB must:
//
//  1. UPSERT a deployment_events failure_autopsy row (idempotent on re-run via
//     the partial-unique index — a second call must NOT create a duplicate),
//  2. stamp deployments.error_message with "<reason>: <hint snippet>", and
//  3. emit an audit_log kind='deploy.failed' row exactly ONCE even across
//     repeated ticks (the idempotency guard added in bug bash 2026-06-02 #15).
//
// The ON CONFLICT ... WHERE kind='failure_autopsy' clause + the audit dedup
// probe are SQL that sqlmock cannot validate against the real schema — this is
// where the partial-unique-index gap (the dev-box schema lacked the index)
// would surface as a hard error rather than a green mock.
func TestIntegration_DeployFailureAutopsy_UpsertAndAuditEmit(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	teamID := testhelpers.SeedTeam(t, db, "pro")
	deployID := testhelpers.SeedDeployment(t, db, teamID, deployStatusFailed, "app-itest3")

	// Autopsy k8s returns OOMKilled pod logs so the captured reason is concrete
	// (not Unknown) — proves the k8s→DB plumbing end-to-end.
	autopsy := &fakeAutopsyK8sCov{
		logs: []string{"panic: out of memory", "exit status 137"},
	}

	ctx := context.Background()

	// First capture.
	captureDeploymentAutopsy(ctx, db, deployID, "app-itest3", autopsy)

	reason, ok := testhelpers.AutopsyRow(t, db, deployID)
	if !ok {
		t.Fatal("no failure_autopsy row after first capture")
	}
	if reason == "" {
		t.Error("autopsy reason is empty — capture did not populate the row")
	}

	// error_message must be stamped (it was NULL on seed).
	_, gotErr := testhelpers.DeploymentStatus(t, db, deployID)
	if !gotErr.Valid || gotErr.String == "" {
		t.Error("deployments.error_message not stamped by autopsy")
	}

	// audit_log deploy.failed emitted exactly once.
	if n := testhelpers.CountAuditLog(t, db, auditKindDeployFailed, deployID.String()); n != 1 {
		t.Errorf("deploy.failed audit rows after first capture = %d, want 1", n)
	}

	// Second capture (idempotent re-tick): MUST NOT duplicate either the
	// autopsy row or the audit_log row.
	captureDeploymentAutopsy(ctx, db, deployID, "app-itest3", autopsy)

	if n := testhelpers.CountAuditLog(t, db, auditKindDeployFailed, deployID.String()); n != 1 {
		t.Errorf("deploy.failed audit rows after SECOND capture = %d, want 1 — idempotency guard regressed (duplicate failure emails)", n)
	}

	// Still exactly one autopsy row (partial-unique index + ON CONFLICT).
	var autopsyRows int
	if err := db.QueryRow(
		`SELECT count(*) FROM deployment_events WHERE deployment_id = $1 AND kind = 'failure_autopsy'`,
		deployID,
	).Scan(&autopsyRows); err != nil {
		t.Fatalf("count autopsy rows: %v", err)
	}
	if autopsyRows != 1 {
		t.Errorf("failure_autopsy rows after two captures = %d, want 1 (ON CONFLICT upsert regressed)", autopsyRows)
	}
}
