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
