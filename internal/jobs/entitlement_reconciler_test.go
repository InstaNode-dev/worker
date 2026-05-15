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
	"database/sql"
	"os"
	"testing"
	"time"

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
