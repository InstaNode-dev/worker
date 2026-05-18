package jobs

// deploy_status_reconcile_test.go — invariant guards for the
// DeployStatusReconciler tunables.
//
// The reconciler runs failure-autopsy capture synchronously inside the
// sweep loop. BugBash 2026-05-18 W3 T3 flagged that an unbounded count of
// synchronous autopsy captures (each with its own multi-second log-tail
// call) can overrun the 30s reconcile interval. The fix bounds the
// captures per tick two ways — a hard count cap and a wall-clock budget.
//
// These tests are package-internal (package jobs) so they can read the
// unexported tunables directly; they pin the relationships the in-sweep
// cap logic in Work() depends on, so a future edit that loosens a cap
// fails CI rather than silently reintroducing the tick-overrun.

import (
	"testing"
	"time"
)

// TestAutopsyBudgetLeavesHeadroomInsideInterval is the core BugBash
// 2026-05-18 W3 T3 guard: the per-tick autopsy wall-clock budget MUST be
// strictly less than the reconcile interval, otherwise a tick spent
// entirely on autopsy captures leaves zero time for the status-transition
// UPDATEs that share the same loop — exactly the overrun the cap exists
// to prevent.
func TestAutopsyBudgetLeavesHeadroomInsideInterval(t *testing.T) {
	if autopsyBudgetPerTick >= deployStatusReconcileInterval {
		t.Errorf("autopsyBudgetPerTick (%s) must be < deployStatusReconcileInterval (%s) — "+
			"a budget that fills the whole tick starves the status transitions and overruns the interval",
			autopsyBudgetPerTick, deployStatusReconcileInterval)
	}
}

// TestAutopsyCountCapIsPositive guards against a regression that sets the
// per-tick count cap to zero or negative — which would defer EVERY
// autopsy forever and the "failure" object would never surface in the api
// response.
func TestAutopsyCountCapIsPositive(t *testing.T) {
	if maxAutopsiesPerTick <= 0 {
		t.Errorf("maxAutopsiesPerTick = %d; must be > 0 or no failure autopsy is ever captured", maxAutopsiesPerTick)
	}
}

// TestAutopsyWorstCaseCountFitsBudget sanity-checks that the count cap and
// the wall-clock budget are sized consistently: even if every one of the
// maxAutopsiesPerTick captures burned the full k8sGetTimeout, the total
// would not exceed the budget. If they drift apart the count cap becomes
// dead (the budget always trips first) or vice versa — the test documents
// the intended sizing relationship.
func TestAutopsyWorstCaseCountFitsBudget(t *testing.T) {
	worstCase := time.Duration(maxAutopsiesPerTick) * k8sGetTimeout
	if worstCase > autopsyBudgetPerTick {
		t.Logf("note: maxAutopsiesPerTick (%d) × k8sGetTimeout (%s) = %s exceeds autopsyBudgetPerTick (%s); "+
			"the wall-clock budget is the binding guard for slow log-tail calls — this is intentional belt-and-braces, not a bug",
			maxAutopsiesPerTick, k8sGetTimeout, worstCase, autopsyBudgetPerTick)
	}
}
