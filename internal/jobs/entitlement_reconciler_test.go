package jobs

// entitlement_reconciler_test.go — unit tests for the drift-detection
// decision (shouldRegrade) and the env-var cadence resolver.
//
// Per this repo's reliability rule 18 ("registry-iterating regression tests,
// not hand-typed lists"), the tier→connection-limit expectations are NOT a
// hand-typed slice. The test iterates the LIVE plans registry
// (commonplans.Default().All()) and derives the entitled cap from the same
// ConnectionsLimit() the production worker calls. Adding a tier in plans.yaml
// is automatically covered — no test edit needed.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	commonv1 "instant.dev/proto/common/v1"

	commonplans "instant.dev/common/plans"
)

// liveRegistry is the production plans source — the same *commonplans.Registry
// the worker is handed by StartWorkers. *Registry satisfies PlanRegistry.
func liveRegistry(t *testing.T) *commonplans.Registry {
	t.Helper()
	r := commonplans.Default()
	if r == nil {
		t.Fatal("commonplans.Default() returned nil")
	}
	return r
}

// nullInt is a tiny helper to build a non-NULL sql.NullInt64.
func nullInt(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }

// TestShouldRegrade_NullAppliedLimit_AlwaysDrifts: a row whose
// applied_conn_limit is NULL (never re-graded since migration 047) has
// drifted for EVERY non-ephemeral tier in the live registry.
func TestShouldRegrade_NullAppliedLimit_AlwaysDrifts(t *testing.T) {
	reg := liveRegistry(t)
	for tier := range reg.All() {
		if entitlementEphemeralTiers[tier] {
			continue // covered by the ephemeral-tier test below
		}
		drift, entitled := shouldRegrade(reg, tier, sql.NullInt64{}) // NULL
		if !drift {
			t.Errorf("tier %q: NULL applied_conn_limit must drift, got drift=false", tier)
		}
		want := reg.ConnectionsLimit(tier, "postgres")
		if entitled != want {
			t.Errorf("tier %q: entitled=%d, want %d (from live registry)", tier, entitled, want)
		}
	}
}

// TestShouldRegrade_AppliedMatchesEntitled_NoDrift: when applied_conn_limit
// already equals the live registry's entitled cap, there is no drift — for
// every non-ephemeral tier.
func TestShouldRegrade_AppliedMatchesEntitled_NoDrift(t *testing.T) {
	reg := liveRegistry(t)
	for tier := range reg.All() {
		if entitlementEphemeralTiers[tier] {
			continue
		}
		entitled := reg.ConnectionsLimit(tier, "postgres")
		drift, _ := shouldRegrade(reg, tier, nullInt(int64(entitled)))
		if drift {
			t.Errorf("tier %q: applied_conn_limit==entitled(%d) must NOT drift, got drift=true",
				tier, entitled)
		}
	}
}

// TestShouldRegrade_AppliedDiffersFromEntitled_Drifts: a stale connection cap
// (off by one from the entitled value) drifts for every non-ephemeral tier.
func TestShouldRegrade_AppliedDiffersFromEntitled_Drifts(t *testing.T) {
	reg := liveRegistry(t)
	for tier := range reg.All() {
		if entitlementEphemeralTiers[tier] {
			continue
		}
		entitled := reg.ConnectionsLimit(tier, "postgres")
		stale := int64(entitled) - 1 // any value != entitled
		drift, got := shouldRegrade(reg, tier, nullInt(stale))
		if !drift {
			t.Errorf("tier %q: applied=%d != entitled=%d must drift, got drift=false",
				tier, stale, entitled)
		}
		if got != entitled {
			t.Errorf("tier %q: entitled=%d, want %d", tier, got, entitled)
		}
	}
}

// TestShouldRegrade_EphemeralTiers_NeverDrift: anonymous/free tiers are never
// re-graded up — drift is always false regardless of applied_conn_limit.
func TestShouldRegrade_EphemeralTiers_NeverDrift(t *testing.T) {
	reg := liveRegistry(t)
	for tier := range entitlementEphemeralTiers {
		// NULL applied limit.
		if drift, _ := shouldRegrade(reg, tier, sql.NullInt64{}); drift {
			t.Errorf("ephemeral tier %q: NULL applied limit must NOT drift", tier)
		}
		// A wildly mismatched applied limit.
		if drift, _ := shouldRegrade(reg, tier, nullInt(999999)); drift {
			t.Errorf("ephemeral tier %q: mismatched applied limit must NOT drift", tier)
		}
	}
}

// TestEntitlementReconcileInterval_Default: unset env var → 5m default.
func TestEntitlementReconcileInterval_Default(t *testing.T) {
	t.Setenv("ENTITLEMENT_RECONCILE_INTERVAL", "")
	os.Unsetenv("ENTITLEMENT_RECONCILE_INTERVAL")
	if got := EntitlementReconcileInterval(); got != defaultEntitlementReconcileInterval {
		t.Errorf("unset env: got %v, want %v", got, defaultEntitlementReconcileInterval)
	}
}

// TestEntitlementReconcileInterval_Override: a valid Go duration string is
// honoured — this is the knob tests use to run the sweep fast.
func TestEntitlementReconcileInterval_Override(t *testing.T) {
	t.Setenv("ENTITLEMENT_RECONCILE_INTERVAL", "15s")
	if got := EntitlementReconcileInterval(); got != 15*time.Second {
		t.Errorf("override: got %v, want 15s", got)
	}
}

// TestEntitlementReconcileInterval_BadValue: an unparseable / non-positive
// value falls back to the default rather than panicking or returning 0.
func TestEntitlementReconcileInterval_BadValue(t *testing.T) {
	for _, bad := range []string{"not-a-duration", "0s", "-5m"} {
		t.Setenv("ENTITLEMENT_RECONCILE_INTERVAL", bad)
		if got := EntitlementReconcileInterval(); got != defaultEntitlementReconcileInterval {
			t.Errorf("bad value %q: got %v, want fallback %v",
				bad, got, defaultEntitlementReconcileInterval)
		}
	}
}

// --- Regression test: NULL provider_resource_id must not abort the scan ---
//
// P0-2 / bughunt/U03: before the fix, entitlementCandidate.providerResourceID
// was typed as `string`. sql.Rows.Scan into a plain string panics/errors on a
// NULL column value, which caused the entire row to be dropped (the `continue`
// in the scan loop silently skipped it). In production every pool-claimed and
// legacy resource has provider_resource_id = NULL, so the sweep always logged
// `scanned:0` and no resource was ever re-graded.
//
// This test seeds a sqlmock result set with three rows:
//   row 1: provider_resource_id = NULL  (modal prod case)
//   row 2: provider_resource_id = ""    (empty string — also safe, fallback in K8sBackend)
//   row 3: provider_resource_id = "ns/abc" (non-NULL non-empty — normal case)
//
// All three rows have applied_conn_limit = NULL (never re-graded) and
// plan_tier = "hobby" (non-ephemeral), so all three are drift candidates.
// The stub regrader counts calls; the test asserts count == 3 (all rows
// produced candidates). Pre-fix behaviour yields count == 0 or 1, so the
// assertion fails exactly when the NULL-scan bug is reintroduced.

// stubRegrader satisfies entitlementRegrader and counts RegradeResource calls.
type stubRegrader struct{ calls atomic.Int32 }

func (s *stubRegrader) RegradeResource(
	_ context.Context, _, _ string, _ commonv1.ResourceType, _, _ string,
) (regradeOutcome, error) {
	s.calls.Add(1)
	return regradeOutcome{Applied: true, AppliedConnLimit: 5}, nil
}

// entitlementSweepCols are the column names the Postgres entitlement reconciler
// SELECT projects. They must match the order in rows.Scan exactly.
var entitlementSweepCols = []string{
	"id", "token", "provider_resource_id",
	"tier", "applied_conn_limit", "plan_tier",
}

// redisSweepCols are the column names the Redis A4 backfill SELECT projects.
// No applied_maxmemory_mb column — the reconciler is stateless for Redis.
var redisSweepCols = []string{
	"id", "token", "provider_resource_id",
	"tier", "plan_tier",
}

// emptyRedisRows returns an empty sqlmock result set for the Redis sweep query.
// Use in tests that focus on Postgres behaviour and want the Redis sweep to be
// a no-op.
func emptyRedisRows() *sqlmock.Rows {
	return sqlmock.NewRows(redisSweepCols)
}

// fakeEntitlementJob returns a minimal *river.Job for EntitlementReconcilerArgs.
func fakeEntitlementJob() *river.Job[EntitlementReconcilerArgs] {
	return &river.Job[EntitlementReconcilerArgs]{JobRow: &rivertype.JobRow{ID: 99}}
}

func TestEntitlementReconciler_NullProviderResourceID_AllRowsScanned(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()

	// Three drifted rows: applied_conn_limit=NULL (never re-graded), plan_tier="hobby".
	// provider_resource_id varies: NULL, empty string, non-empty string.
	rows := sqlmock.NewRows(entitlementSweepCols).
		AddRow(id1, "tok-null-prid", nil, "hobby", nil, "hobby").         // NULL prid — the bug case
		AddRow(id2, "tok-empty-prid", "", "hobby", nil, "hobby").         // empty-string prid
		AddRow(id3, "tok-nonempty-prid", "ns/abc", "hobby", nil, "hobby") // non-NULL prid

	// The worker does UPDATE resources SET applied_conn_limit = $1 WHERE id = $2
	// once per successfully re-graded row.
	mock.ExpectQuery(`SELECT`).WillReturnRows(rows) // Postgres query
	for i := 0; i < 3; i++ {
		mock.ExpectExec(`UPDATE resources SET applied_conn_limit`).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectQuery(`SELECT`).WillReturnRows(emptyRedisRows()) // Redis A4 query (no-op)

	stub := &stubRegrader{}
	reg := liveRegistry(t)
	w := NewEntitlementReconcilerWorker(db, reg, stub)

	if err := w.Work(context.Background(), fakeEntitlementJob()); err != nil {
		t.Fatalf("Work() returned unexpected error: %v", err)
	}

	// Assert all three rows were scanned and passed to the regrader.
	// Pre-fix: rows with NULL prid abort Scan → candidates=[] → calls==0.
	// Post-fix: all rows scan successfully           → calls==3.
	if got := int(stub.calls.Load()); got != 3 {
		t.Errorf("RegradeResource called %d times, want 3 — NULL provider_resource_id may still abort the scan", got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestEntitlementReconciler_NullProviderResourceID_PassesEmptyStringToRegrader
// verifies that a NULL prid row passes "" (not "<nil>" or a garbage value) to
// RegradeResource — the K8sBackend falls back to k8sNsPrefix+token when prid=="".
func TestEntitlementReconciler_NullProviderResourceID_PassesEmptyStringToRegrader(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id1 := uuid.New()

	rows := sqlmock.NewRows(entitlementSweepCols).
		AddRow(id1, "tok-null-prid", nil, "hobby", nil, "hobby") // NULL prid, drifted

	mock.ExpectQuery(`SELECT`).WillReturnRows(rows)                          // Postgres query
	mock.ExpectExec(`UPDATE resources SET applied_conn_limit`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(`SELECT`).WillReturnRows(emptyRedisRows())               // Redis A4 query (no-op)

	var capturedPRID string
	capturingRegrader := &capturePRIDRegrader{captured: &capturedPRID}
	reg := liveRegistry(t)
	w := NewEntitlementReconcilerWorker(db, reg, capturingRegrader)

	if err := w.Work(context.Background(), fakeEntitlementJob()); err != nil {
		t.Fatalf("Work() returned unexpected error: %v", err)
	}

	if capturedPRID != "" {
		t.Errorf("NULL prid should produce empty string to RegradeResource, got %q", capturedPRID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// capturePRIDRegrader captures the providerResourceID passed to RegradeResource.
// For the Postgres NULL-prid test it captures the first call only (the Postgres
// regrade). Redis calls in the same sweep use the k8s namespace IDs which are
// always non-empty.
type capturePRIDRegrader struct{ captured *string }

func (c *capturePRIDRegrader) RegradeResource(
	_ context.Context, _, prid string, _ commonv1.ResourceType, _, _ string,
) (regradeOutcome, error) {
	*c.captured = prid
	return regradeOutcome{Applied: true, AppliedConnLimit: 5}, nil
}

// ─── Redis A4 backfill tests ──────────────────────────────────────────────────
//
// The following tests exercise the Redis maxmemory sweep added in A4.
// They use sqlmock in two-query mode: first SELECT (Postgres) returns empty,
// second SELECT (Redis) returns the test data.
//
// The stubRegrader is reused; it counts calls regardless of resource type.

// captureResourceTypeRegrader records each (resType, prid, tier) triple so
// tests can verify the reconciler passes RESOURCE_TYPE_REDIS to the provisioner.
type captureResourceTypeRegrader struct {
	calls []struct {
		resType commonv1.ResourceType
		prid    string
		tier    string
	}
	outcome regradeOutcome
}

func (c *captureResourceTypeRegrader) RegradeResource(
	_ context.Context, _, prid string, resType commonv1.ResourceType, tier, _ string,
) (regradeOutcome, error) {
	c.calls = append(c.calls, struct {
		resType commonv1.ResourceType
		prid    string
		tier    string
	}{resType, prid, tier})
	return c.outcome, nil
}

// TestRedisA4_SweepCallsRegradeWithRedisType verifies that the Redis sweep
// emits RegradeResource calls with RESOURCE_TYPE_REDIS and the correct
// provider_resource_id (the k8s namespace name).
func TestRedisA4_SweepCallsRegradeWithRedisType(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id1 := uuid.New()
	id2 := uuid.New()

	// Two k8s-backed Redis resources: pro and team tier.
	redisRows := sqlmock.NewRows(redisSweepCols).
		AddRow(id1, "tok-pro", "instant-customer-tok-pro", "pro", "pro").
		AddRow(id2, "tok-team", "instant-customer-tok-team", "team", "team")

	// Postgres query returns empty (no postgres drift in this test).
	mock.ExpectQuery(`SELECT`).WillReturnRows(sqlmock.NewRows(entitlementSweepCols))
	// Redis query returns two rows.
	mock.ExpectQuery(`SELECT`).WillReturnRows(redisRows)

	regrader := &captureResourceTypeRegrader{
		outcome: regradeOutcome{Applied: true, AppliedConnLimit: 512},
	}
	reg := liveRegistry(t)
	w := NewEntitlementReconcilerWorker(db, reg, regrader)

	if err := w.Work(context.Background(), fakeEntitlementJob()); err != nil {
		t.Fatalf("Work() returned unexpected error: %v", err)
	}

	if len(regrader.calls) != 2 {
		t.Fatalf("expected 2 RegradeResource calls, got %d", len(regrader.calls))
	}
	for i, call := range regrader.calls {
		if call.resType != commonv1.ResourceType_RESOURCE_TYPE_REDIS {
			t.Errorf("call[%d]: resType=%v, want RESOURCE_TYPE_REDIS", i, call.resType)
		}
		if call.prid == "" {
			t.Errorf("call[%d]: empty providerResourceID — k8s namespace should be non-empty", i)
		}
	}
	// Pro tier call.
	if regrader.calls[0].prid != "instant-customer-tok-pro" {
		t.Errorf("pro pod prid=%q, want instant-customer-tok-pro", regrader.calls[0].prid)
	}
	if regrader.calls[0].tier != "pro" {
		t.Errorf("pro pod tier=%q, want pro", regrader.calls[0].tier)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestRedisA4_SharedBackendResource_NotInSweep verifies that the SQL query
// filters out resources without the 'instant-customer-' prefix. This is
// enforced at the DB query level (LIKE 'instant-customer-%') so this test
// confirms that the sqlmock only returns k8s-prefixed rows.
//
// A shared-backend Redis resource (provider_resource_id NULL or unrelated)
// cannot appear in the sweep because the WHERE clause excludes it. This test
// seeds ONLY a k8s row and asserts the regrader is called exactly once.
func TestRedisA4_OnlyK8sResourcesSwept(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id1 := uuid.New()

	// Only a k8s-backed Redis resource in the result set (the WHERE clause
	// in the real DB filters shared resources; sqlmock simulates that outcome).
	redisRows := sqlmock.NewRows(redisSweepCols).
		AddRow(id1, "tok-pro", "instant-customer-tok-pro", "pro", "pro")

	mock.ExpectQuery(`SELECT`).WillReturnRows(sqlmock.NewRows(entitlementSweepCols))
	mock.ExpectQuery(`SELECT`).WillReturnRows(redisRows)

	stub := &stubRegrader{}
	reg := liveRegistry(t)
	w := NewEntitlementReconcilerWorker(db, reg, stub)

	if err := w.Work(context.Background(), fakeEntitlementJob()); err != nil {
		t.Fatalf("Work() returned unexpected error: %v", err)
	}
	if got := int(stub.calls.Load()); got != 1 {
		t.Errorf("RegradeResource called %d times, want 1 (only k8s pod)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestRedisA4_TeamTier_SentToProvisioner verifies that team-tier Redis pods
// are included in the sweep — the provisioner is responsible for setting
// maxmemory=0 (unlimited) for team/growth. The reconciler must NOT skip them.
func TestRedisA4_TeamTier_SentToProvisioner(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id1 := uuid.New()

	redisRows := sqlmock.NewRows(redisSweepCols).
		AddRow(id1, "tok-team", "instant-customer-tok-team", "team", "team")

	mock.ExpectQuery(`SELECT`).WillReturnRows(sqlmock.NewRows(entitlementSweepCols))
	mock.ExpectQuery(`SELECT`).WillReturnRows(redisRows)

	regrader := &captureResourceTypeRegrader{
		outcome: regradeOutcome{Applied: true, AppliedConnLimit: 0}, // 0 = unlimited
	}
	reg := liveRegistry(t)
	w := NewEntitlementReconcilerWorker(db, reg, regrader)

	if err := w.Work(context.Background(), fakeEntitlementJob()); err != nil {
		t.Fatalf("Work() returned unexpected error: %v", err)
	}
	if len(regrader.calls) != 1 {
		t.Fatalf("expected 1 RegradeResource call for team-tier pod, got %d", len(regrader.calls))
	}
	if regrader.calls[0].tier != "team" {
		t.Errorf("tier=%q, want team", regrader.calls[0].tier)
	}
	// Team tier is NOT ephemeral — it must NOT be skipped.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestRedisA4_FailSoftPerPod verifies that a gRPC error on one Redis pod
// does not abort the sweep — subsequent pods are still processed.
func TestRedisA4_FailSoftPerPod(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id1 := uuid.New()
	id2 := uuid.New()

	redisRows := sqlmock.NewRows(redisSweepCols).
		AddRow(id1, "tok-bad", "instant-customer-tok-bad", "pro", "pro").   // will error
		AddRow(id2, "tok-ok", "instant-customer-tok-ok", "pro", "pro")      // must still be called

	mock.ExpectQuery(`SELECT`).WillReturnRows(sqlmock.NewRows(entitlementSweepCols))
	mock.ExpectQuery(`SELECT`).WillReturnRows(redisRows)

	callCount := 0
	failSoftRegrader := &funcRegrader{fn: func(_ context.Context, _, prid string, resType commonv1.ResourceType, _, _ string) (regradeOutcome, error) {
		callCount++
		if prid == "instant-customer-tok-bad" {
			return regradeOutcome{}, fmt.Errorf("connection refused: pod not ready")
		}
		return regradeOutcome{Applied: true, AppliedConnLimit: 512}, nil
	}}

	reg := liveRegistry(t)
	w := NewEntitlementReconcilerWorker(db, reg, failSoftRegrader)

	if err := w.Work(context.Background(), fakeEntitlementJob()); err != nil {
		t.Fatalf("Work() must not return error even when one pod fails: %v", err)
	}
	if callCount != 2 {
		t.Errorf("expected 2 RegradeResource calls (one fail-soft, one success), got %d", callCount)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// funcRegrader implements entitlementRegrader with a function for flexible test setup.
type funcRegrader struct {
	fn func(ctx context.Context, token, prid string, resType commonv1.ResourceType, tier, reqID string) (regradeOutcome, error)
}

func (f *funcRegrader) RegradeResource(ctx context.Context, token, prid string, resType commonv1.ResourceType, tier, reqID string) (regradeOutcome, error) {
	return f.fn(ctx, token, prid, resType, tier, reqID)
}
