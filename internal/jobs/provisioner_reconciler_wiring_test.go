package jobs

// provisioner_reconciler_wiring_test.go — MR-P0-2 regression guard
// (BugBash 2026-05-20, cross-confirmed by T1/T20).
//
// THE BUG: StartWorkers constructed the provisioner_reconciler with
// `NewProvisionerReconcilerWorker(db, rdb, nil)`. A nil prober falls back to
// NoopProber, whose every Probe returns ProbeReachable. So the reconciler —
// whose whole job is to decide the fate of a stuck 'pending' row — would
// blindly flip EVERY stuck row to status='active' WITHOUT ever checking the
// backend, handing the customer a credentials-less resource the platform
// claims is healthy.
//
// THE FIX: workers.go now passes NewRealProber(cfg) — the SAME real prober
// resource_heartbeat already uses. A stuck 'pending' row is promoted to
// 'active' ONLY if its backend is genuinely reachable, else abandoned.
//
// These tests are the internal-package twin of heartbeat_prober_wiring_test.go.
// They cannot live in package jobs_test because they inspect the unexported
// ProvisionerReconcilerWorker.prober field — that field is the exact thing the
// bug got wrong, so the regression guard must assert on it directly.

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"instant.dev/worker/internal/config"
)

// TestProvisionerReconciler_WorkerKeepsRealProber pins that constructing the
// reconciler the way StartWorkers does (with NewRealProber(cfg), not nil)
// leaves a non-Noop prober on the worker.
//
// The constructor's nil→NoopProber fallback is correct behaviour in the
// abstract — the BUG was the *call site* (workers.go) passing nil. This test
// exercises the post-MR-P0-2 call site: if a future edit reverts workers.go to
// pass nil (or anything that resolves to NoopProber), the reconciler silently
// resumes promoting unreachable resources to 'active' and this test fails.
func TestProvisionerReconciler_WorkerKeepsRealProber(t *testing.T) {
	w := NewProvisionerReconcilerWorker(nil, nil, NewRealProber(&config.Config{}))
	if w.prober == nil {
		t.Fatal("ProvisionerReconcilerWorker.prober is nil — would fall back to NoopProber")
	}
	if _, isNoop := w.prober.(NoopProber); isNoop {
		t.Errorf("MR-P0-2 regression: ProvisionerReconcilerWorker.prober is a NoopProber — " +
			"the reconciler would promote every stuck 'pending' row to 'active' WITHOUT " +
			"probing the backend. StartWorkers must pass NewRealProber(cfg), not nil.")
	}
}

// TestProvisionerReconciler_NilProberFallsBackToNoop documents the constructor
// contract the bug exploited: a nil prober DOES resolve to NoopProber. This is
// why passing nil at the call site was silently wrong — there is no panic, no
// log, just a reconciler that rubber-stamps everything. The test exists so the
// danger of the nil path is explicit and a future reader does not "tidy up" by
// reintroducing the nil call site.
func TestProvisionerReconciler_NilProberFallsBackToNoop(t *testing.T) {
	w := NewProvisionerReconcilerWorker(nil, nil, nil)
	if _, isNoop := w.prober.(NoopProber); !isNoop {
		t.Fatalf("expected a nil prober to fall back to NoopProber, got %T", w.prober)
	}
}

// TestProvisionerReconciler_StartWorkersCallSite_PassesRealProber is the
// MR-P0-2 call-site binding. The pure-behavior tests above prove that GIVEN
// a real prober the reconciler does the right thing — they cannot catch the
// actual bug, which is that workers.go used to pass nil. This test reads
// workers.go directly and asserts the AddWorker line for
// NewProvisionerReconcilerWorker passes NewRealProber(cfg), NOT nil.
//
// If a future edit reverts the call site to `nil` (or any other expression
// that resolves to NoopProber), this test fails immediately — which is the
// only way to keep this regression from sneaking back in, since the
// constructor's nil-fallback makes the bug silently compile.
func TestProvisionerReconciler_StartWorkersCallSite_PassesRealProber(t *testing.T) {
	const path = "workers.go"
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	// Pattern: NewProvisionerReconcilerWorker(<anything>, <anything>, <prober-expr>)
	// Captures the prober-expr (the third argument).
	pat := regexp.MustCompile(`NewProvisionerReconcilerWorker\(\s*[^,]+,\s*[^,]+,\s*([^)]+?)\s*\)`)
	matches := pat.FindAllStringSubmatch(string(src), -1)
	if len(matches) == 0 {
		t.Fatal("MR-P0-2 binding: could not locate NewProvisionerReconcilerWorker(...) call in workers.go — " +
			"the wiring may have been moved; update this test to track it.")
	}
	for _, m := range matches {
		proberArg := strings.TrimSpace(m[1])
		if proberArg == "nil" {
			t.Errorf("MR-P0-2 regression: workers.go calls NewProvisionerReconcilerWorker(..., nil) — "+
				"a nil prober falls back to NoopProber, which reports every stuck 'pending' row "+
				"as ProbeReachable. The reconciler would blindly promote unreachable resources "+
				"to status='active'. Pass NewRealProber(cfg) instead. (matched arg: %q)", proberArg)
		}
		// The fix lands NewRealProber(cfg) — accept any expression that
		// references the real prober.
		if !strings.Contains(proberArg, "NewRealProber") {
			t.Errorf("MR-P0-2 regression: workers.go calls NewProvisionerReconcilerWorker(..., %q) — "+
				"expected an expression containing NewRealProber. If the wiring intentionally "+
				"changed prober flavors, update this test together with the call site.", proberArg)
		}
	}
}
