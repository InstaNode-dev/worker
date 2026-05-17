package jobs

import "testing"

// TestQueueReconcileConst guards the queue name so a typo in the
// periodic-job closures doesn't silently route reconcilers back to the
// default queue, reintroducing the starvation bug.
//
// Background: prior to fix/reconcile-queue, all periodic jobs landed on
// river.QueueDefault. A weekly_digest fan-out (1 row per team) accumulated
// 232K available jobs and pinned all 5 worker slots, so the
// deploy_status_reconcile job (queued every 30s) never ran. Customers saw
// status="building" indefinitely while their pods were already Ready.
func TestQueueReconcileConst(t *testing.T) {
	if queueReconcile != "reconcile" {
		t.Errorf("queueReconcile = %q; want %q. River queue names are referenced as plain strings from periodic-job InsertOpts; renaming this without updating the matching QueueConfig entry in workers.go reintroduces the starvation bug.", queueReconcile, "reconcile")
	}
}

// TestReconcileInsertOpts_BindsToReconcileQueue verifies the helper that
// every reconciler periodic-job closure calls returns an InsertOpts whose
// Queue matches the dedicated queue name. This is the behavior guard —
// TestQueueReconcileConst above checks the const alone, but a typo could
// still route closures to a different queue if the InsertOpts struct
// literal was edited in isolation. With the helper, periodic-job builders
// share a single call site that this test exercises directly.
func TestReconcileInsertOpts_BindsToReconcileQueue(t *testing.T) {
	opts := reconcileInsertOpts()
	if opts == nil {
		t.Fatal("reconcileInsertOpts() returned nil — closures would default to QueueDefault, reintroducing starvation")
	}
	if opts.Queue != queueReconcile {
		t.Errorf("reconcileInsertOpts().Queue = %q; want %q. The helper is meant to keep reconcilers off the default queue — if it drifts, the deploy_status_reconcile starvation bug returns.", opts.Queue, queueReconcile)
	}
	if opts.Queue != "reconcile" {
		t.Errorf("reconcileInsertOpts().Queue resolved to %q; the dedicated queue config in workers.go is keyed by the literal \"reconcile\", so the helper must produce that string verbatim.", opts.Queue)
	}
}

// TestQueueBillingConst guards the dedicated billing queue name (P1-L).
//
// Background: the billing reconciler sweep runs ~17 min (≈100 teams × 2
// Razorpay calls × 100ms stagger). It used to run on queueReconcile, where
// it occupied both worker slots for the whole sweep and starved the fast
// reconcilers (deploy-status every 30s, custom-domain every 5min). The fix
// gave it its own queue. A typo here would silently route the billing sweep
// back onto a shared queue and reintroduce the starvation.
func TestQueueBillingConst(t *testing.T) {
	if queueBilling != "billing" {
		t.Errorf("queueBilling = %q; want %q. The QueueConfig entry in workers.go is keyed by this literal; renaming without updating it strands the billing reconciler.", queueBilling, "billing")
	}
	if queueBilling == queueReconcile {
		t.Fatalf("queueBilling (%q) must NOT equal queueReconcile (%q) — the whole point of P1-L is that the long billing sweep runs on an ISOLATED queue so it cannot starve the fast reconcilers.", queueBilling, queueReconcile)
	}
}

// TestBillingInsertOpts_BindsToBillingQueue verifies the billing-reconciler
// periodic-job closure routes to the dedicated billing queue, not the
// reconcile queue (P1-L). If billingInsertOpts ever drifts back to
// queueReconcile, the ~17-min billing sweep starves deploy-status again.
func TestBillingInsertOpts_BindsToBillingQueue(t *testing.T) {
	opts := billingInsertOpts()
	if opts == nil {
		t.Fatal("billingInsertOpts() returned nil — the billing sweep would default to QueueDefault")
	}
	if opts.Queue != queueBilling {
		t.Errorf("billingInsertOpts().Queue = %q; want %q (the dedicated billing queue).", opts.Queue, queueBilling)
	}
	if opts.Queue == queueReconcile {
		t.Errorf("billingInsertOpts().Queue = %q — that is the FAST-reconciler queue. The billing sweep must be isolated from it (P1-L), otherwise the ~17-min sweep starves deploy-status/custom-domain reconcilers.", opts.Queue)
	}
}
