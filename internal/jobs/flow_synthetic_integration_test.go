package jobs

// flow_synthetic_integration_test.go — REAL-Postgres round-trip proof for the
// synthetic flow runner's DB side-effects: the idempotent cohort-team seed and
// the reaper backstop's soft-delete of stale synthetic resources. A sqlmock
// test proves the SQL is issued; this proves it actually writes the expected
// rows against a live platform Postgres (the convergence sqlmock can't show).
//
// GATING: testhelpers.SetupTestDB skips under no-DB (the -short `make gate` /
// deploy.yml / ci.yml lanes ship no Postgres), so this runs only where a real
// platform Postgres is supplied via TEST_DATABASE_URL — same posture as the
// other *_integration_test.go files.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"instant.dev/worker/internal/metrics"
	"instant.dev/worker/internal/testhelpers"
)

// failAllAPIServer returns an httptest server that 500s every request, so the
// HTTP flows fail/degrade FAST (no real api needed) while the seed + reaper DB
// paths run to completion — the surface this integration test targets.
func failAllAPIServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestIntegration_FlowSynthetic_SeedsCohortTeam proves one Work() tick upserts
// the deterministic synthetic team (is_test_cohort=true) + its primary user, so
// every team-iterating background job's cohort guard will skip it.
func TestIntegration_FlowSynthetic_SeedsCohortTeam(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	// Clean any prior synthetic rows so the assertion is deterministic.
	_, _ = db.Exec(`DELETE FROM users WHERE id = $1`, flowSyntheticUserID)
	_, _ = db.Exec(`DELETE FROM resources WHERE team_id = $1`, flowSyntheticTeamID)
	_, _ = db.Exec(`DELETE FROM audit_log WHERE team_id = $1`, flowSyntheticTeamID)
	_, _ = db.Exec(`DELETE FROM teams WHERE id = $1`, flowSyntheticTeamID)
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM users WHERE id = $1`, flowSyntheticUserID)
		_, _ = db.Exec(`DELETE FROM resources WHERE team_id = $1`, flowSyntheticTeamID)
		_, _ = db.Exec(`DELETE FROM audit_log WHERE team_id = $1`, flowSyntheticTeamID)
		_, _ = db.Exec(`DELETE FROM teams WHERE id = $1`, flowSyntheticTeamID)
	})

	srv := failAllAPIServer(t)
	w := NewFlowSyntheticWorker(db, srv.Client(), FlowSyntheticPromMetrics{}, nil, FlowSyntheticConfig{
		Enabled:   true,
		BaseURL:   srv.URL,
		JWTSecret: "itest-secret",
		Tier:      "pro",
	})
	if err := w.Work(context.Background(), fakeJobID(FlowSyntheticArgs{})); err != nil {
		t.Fatalf("Work: %v", err)
	}

	// Team exists, cohort-tagged, at the seeded tier.
	var cohort bool
	var tier string
	if err := db.QueryRow(
		`SELECT is_test_cohort, plan_tier FROM teams WHERE id = $1`, flowSyntheticTeamID,
	).Scan(&cohort, &tier); err != nil {
		t.Fatalf("synthetic team not seeded: %v", err)
	}
	if !cohort {
		t.Error("synthetic team must be is_test_cohort=true")
	}
	if tier != "pro" {
		t.Errorf("synthetic team plan_tier = %q, want pro", tier)
	}

	// Primary user exists for the /auth/me lookup.
	var email string
	if err := db.QueryRow(
		`SELECT email FROM users WHERE id = $1 AND team_id = $2 AND is_primary`,
		flowSyntheticUserID, flowSyntheticTeamID,
	).Scan(&email); err != nil {
		t.Fatalf("synthetic primary user not seeded: %v", err)
	}

	// Re-run: seed is idempotent (no duplicate-key error, one team).
	if err := w.Work(context.Background(), fakeJobID(FlowSyntheticArgs{})); err != nil {
		t.Fatalf("second Work (idempotency): %v", err)
	}
	var teamCount int
	if err := db.QueryRow(`SELECT count(*) FROM teams WHERE id = $1`, flowSyntheticTeamID).Scan(&teamCount); err != nil {
		t.Fatalf("count teams: %v", err)
	}
	if teamCount != 1 {
		t.Errorf("idempotent seed: want 1 synthetic team, got %d", teamCount)
	}
}

// TestIntegration_FlowSynthetic_ReaperSoftDeletesStaleResource proves the
// reaper backstop flips a stale synthetic resource to status='deleted' and
// writes the cleanup-ledger audit row (rule 24).
func TestIntegration_FlowSynthetic_ReaperSoftDeletesStaleResource(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	_, _ = db.Exec(`DELETE FROM resources WHERE team_id = $1`, flowSyntheticTeamID)
	_, _ = db.Exec(`DELETE FROM audit_log WHERE team_id = $1`, flowSyntheticTeamID)
	_, _ = db.Exec(`DELETE FROM users WHERE id = $1`, flowSyntheticUserID)
	_, _ = db.Exec(`DELETE FROM teams WHERE id = $1`, flowSyntheticTeamID)
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM resources WHERE team_id = $1`, flowSyntheticTeamID)
		_, _ = db.Exec(`DELETE FROM audit_log WHERE team_id = $1`, flowSyntheticTeamID)
		_, _ = db.Exec(`DELETE FROM users WHERE id = $1`, flowSyntheticUserID)
		_, _ = db.Exec(`DELETE FROM teams WHERE id = $1`, flowSyntheticTeamID)
	})

	// Seed the synthetic team + a STALE (older than max-age) active resource the
	// inline reap "missed" (simulating a runner crash mid-leg).
	if _, err := db.Exec(`
		INSERT INTO teams (id, name, plan_tier, status, is_test_cohort)
		VALUES ($1, 'synthetic-flowtest', 'free', 'active', true)
		ON CONFLICT (id) DO NOTHING
	`, flowSyntheticTeamID); err != nil {
		t.Fatalf("seed team: %v", err)
	}
	staleID := "44444444-4444-4444-8444-444444444444"
	if _, err := db.Exec(`
		INSERT INTO resources (id, team_id, token, resource_type, tier, status, name, created_at)
		VALUES ($1, $2, gen_random_uuid(), 'postgres', 'free', 'active', 'synthetic-flow-stale', now() - interval '1 hour')
	`, staleID, flowSyntheticTeamID); err != nil {
		t.Fatalf("seed stale resource: %v", err)
	}

	// Run only the reaper backstop via a full tick (flag on, fail-all api so the
	// flows don't create new resources).
	srv := failAllAPIServer(t)
	w := NewFlowSyntheticWorker(db, srv.Client(), FlowSyntheticPromMetrics{}, nil, FlowSyntheticConfig{
		Enabled: true,
		BaseURL: srv.URL,
		// No JWTSecret → authed flows degrade (no provision), so the only
		// resource the reaper sees is the stale one we seeded.
		Tier: "free",
	})
	if err := w.Work(context.Background(), fakeJobID(FlowSyntheticArgs{})); err != nil {
		t.Fatalf("Work: %v", err)
	}

	var status string
	if err := db.QueryRow(`SELECT status FROM resources WHERE id = $1`, staleID).Scan(&status); err != nil {
		t.Fatalf("read stale resource: %v", err)
	}
	if status != "deleted" {
		t.Errorf("stale synthetic resource status = %q, want deleted (reaper backstop)", status)
	}

	// Cleanup ledger row written (rule 24).
	if n := testhelpers.CountAuditLogByTeam(t, db, flowSyntheticTeamID, auditKindFlowReaped); n < 1 {
		t.Errorf("want ≥1 synthetic.reaped ledger row, got %d", n)
	}
}

// ensure the metrics package is linked (the Prom impl referenced above lives in
// internal/metrics) — keeps the import non-blank if a future refactor drops the
// direct reference.
var _ = metrics.FlowTestTotal
