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
