package jobs

// entitlement_reconciler_integration_test.go — REAL-Postgres integration test
// for the entitlement reconciler's Postgres connection-cap regrade round-trip.
//
// Per INTEGRATION-COVERAGE-PLAN-2026-06-04.md §5 #1, entitlement_reconciler is
// a Tier-1 (data-adjacent) job whose DB round-trip was fake-tested only. The
// sibling entitlement_reconciler_test.go drives the SELECT/UPDATE through
// go-sqlmock; THIS test seeds a real drifted resources row, runs Work() against
// a live platform Postgres with a stub provisioner regrader, and asserts the
// row's applied_conn_limit was actually persisted by the worker's UPDATE.
//
// The provisioner gRPC leg stays stubbed (stubRegrader returns
// {Applied:true, AppliedConnLimit:5}) — the DB leg (the drift SELECT + the
// UPDATE resources SET applied_conn_limit) is REAL. This is the convergence
// signal the job depends on in production: a sqlmock test passes whether or not
// the UPDATE's WHERE id = $2 matches the live row; this proves it does.
//
// GATING: testhelpers.SetupTestDB skips when no DB is reachable, so the regular
// gate (deploy.yml / ci.yml, no Postgres service) stays green. It runs against a
// real Postgres wherever one is supplied (developer DB, coverage.yml service).

import (
	"context"
	"database/sql"
	"testing"

	"instant.dev/worker/internal/testhelpers"
)

// TestIntegration_EntitlementReconciler_PersistsRegradedConnLimit seeds a
// drifted Postgres resource (applied_conn_limit = NULL, never re-graded) on a
// pro-tier team, runs the reconciler with a stub regrader that reports
// {Applied:true, AppliedConnLimit:5}, and asserts the worker persisted that
// value to the REAL row.
//
// Drift detection: shouldRegrade returns drift=true for a NULL applied limit on
// any non-ephemeral tier. The stub stands in for the provisioner's
// RegradeResource; the worker's `UPDATE resources SET applied_conn_limit = $1
// WHERE id = $2` is the integration assertion target.
func TestIntegration_EntitlementReconciler_PersistsRegradedConnLimit(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	teamID := testhelpers.SeedTeam(t, db, "pro")
	// resource.tier = pro (the per-row snapshot the reconciler resolves caps
	// from), applied_conn_limit = NULL → drifts.
	resID, _ := testhelpers.SeedResource(t, db, teamID, "postgres", "pro", sql.NullInt64{})

	// Scope the sweep to ONLY this team so it cannot touch any other rows that
	// may exist in a shared local DB (and so the assertion is deterministic).
	t.Setenv("ENTITLEMENT_RECONCILE_TEAM", teamID.String())

	stub := &stubRegrader{} // returns Applied:true, AppliedConnLimit:5
	reg := liveRegistry(t)
	w := NewEntitlementReconcilerWorker(db, reg, stub)

	if err := w.Work(context.Background(), fakeEntitlementJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	// The stub must have been asked to regrade our drifted row at least once.
	if got := int(stub.calls.Load()); got < 1 {
		t.Fatalf("RegradeResource called %d times, want >= 1 (drifted row should have been regraded)", got)
	}

	// The REAL row's applied_conn_limit must now equal the stub's reported value.
	got := testhelpers.AppliedConnLimit(t, db, resID)
	if !got.Valid {
		t.Fatal("applied_conn_limit is still NULL — the worker's UPDATE did not persist against the live row")
	}
	if got.Int64 != 5 {
		t.Errorf("applied_conn_limit = %d, want 5 (the value the stub regrader reported)", got.Int64)
	}
}

// TestIntegration_EntitlementReconciler_NoDriftLeavesRowUntouched is the
// complement: a resource whose applied_conn_limit already equals the entitled
// cap for its tier must NOT be re-graded (no drift) and the row is left
// untouched. This pins that the live drift SELECT + shouldRegrade decision
// correctly identify the no-op case against real data — a regression that
// always-regrades would burn provisioner RPCs every 5-minute tick.
func TestIntegration_EntitlementReconciler_NoDriftLeavesRowUntouched(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	reg := liveRegistry(t)
	entitled := reg.ConnectionsLimit("pro", "postgres")

	teamID := testhelpers.SeedTeam(t, db, "pro")
	// applied_conn_limit already == entitled → no drift.
	resID, _ := testhelpers.SeedResource(t, db, teamID, "postgres", "pro",
		sql.NullInt64{Int64: int64(entitled), Valid: true})

	t.Setenv("ENTITLEMENT_RECONCILE_TEAM", teamID.String())

	stub := &stubRegrader{}
	w := NewEntitlementReconcilerWorker(db, reg, stub)

	if err := w.Work(context.Background(), fakeEntitlementJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := int(stub.calls.Load()); got != 0 {
		t.Errorf("RegradeResource called %d times, want 0 — a row at the entitled cap must NOT drift", got)
	}

	// Row value unchanged.
	got := testhelpers.AppliedConnLimit(t, db, resID)
	if !got.Valid || got.Int64 != int64(entitled) {
		t.Errorf("applied_conn_limit = %v, want %d (untouched)", got, entitled)
	}
}
