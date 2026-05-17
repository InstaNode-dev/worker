package jobs

// safego_test.go — coverage for the P1-B fire-and-forget panic boundary.

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"instant.dev/worker/internal/metrics"
)

// metricCount reads the current value of the GoroutinePanicsRecovered
// counter for a given site label. Returns 0 if the label is unseen.
func metricCount(t *testing.T, site string) float64 {
	t.Helper()
	c, err := metrics.GoroutinePanicsRecovered.GetMetricWithLabelValues(site)
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues(%q): %v", site, err)
	}
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("metric Write: %v", err)
	}
	return m.GetCounter().GetValue()
}

// waitForMetric polls until GoroutinePanicsRecovered{site} reaches want, or
// fails after a short deadline. SafeGo's Recover runs in its own goroutine
// AFTER fn returns, so the test cannot synchronise on fn alone — it must
// observe the metric the recovery boundary itself bumps.
func waitForMetric(t *testing.T, site string, want float64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if metricCount(t, site) >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("GoroutinePanicsRecovered{site=%q} never reached %v (got %v). "+
		"SafeGo must count every recovered panic so the NR alert fires.",
		site, want, metricCount(t, site))
}

// TestSafeGo_RecoversPanic_DoesNotCrash verifies that a panic inside a
// SafeGo goroutine is contained: the test process survives, and the
// recovery metric for the site is incremented. (If the panic were NOT
// recovered, the whole test binary would crash — so reaching the assertion
// at all is itself part of the proof.)
func TestSafeGo_RecoversPanic_DoesNotCrash(t *testing.T) {
	const site = "test.safego_panic"
	before := metricCount(t, site)

	SafeGo(site, func() {
		panic("boom — a fire-and-forget goroutine panicked")
	})

	waitForMetric(t, site, before+1)
}

// TestSafeGo_NoPanic_RunsToCompletion verifies the happy path: SafeGo runs
// fn normally and does NOT touch the recovery metric when nothing panics.
func TestSafeGo_NoPanic_RunsToCompletion(t *testing.T) {
	const site = "test.safego_clean"
	before := metricCount(t, site)

	var ran bool
	var wg sync.WaitGroup
	wg.Add(1)
	SafeGo(site, func() {
		defer wg.Done()
		ran = true
	})
	wg.Wait()

	if !ran {
		t.Error("SafeGo did not execute fn")
	}
	if got := metricCount(t, site); got != before {
		t.Errorf("GoroutinePanicsRecovered{site=%q} = %v on the no-panic path; want %v (unchanged).", site, got, before)
	}
}

// TestRecover_AsDefer_ContainsPanic verifies the Recover deferred form (used
// by WaitGroup-tracked / pipe goroutines) also contains a panic and counts it.
func TestRecover_AsDefer_ContainsPanic(t *testing.T) {
	const site = "test.recover_defer"
	before := metricCount(t, site)

	func() {
		defer Recover(site)
		panic("boom via defer Recover")
	}()

	if got := metricCount(t, site); got != before+1 {
		t.Errorf("GoroutinePanicsRecovered{site=%q} = %v; want %v.", site, got, before+1)
	}
}

// TestNoBareGoFunc_InWorkerSource is the rule-16/18 coverage guard: it scans
// every non-test .go file in the worker module and fails if a bare
// `go func` appears anywhere OTHER than the SafeGo helper itself. Every
// fire-and-forget goroutine must route through SafeGo or carry an inline
// `defer Recover(...)` / `recover()` boundary. A bare `go func` without a
// recovery boundary crashes the whole worker pod on panic (P1-B).
//
// The scan tolerates a `go func` only when the same file also references
// Recover or LogRecoveredPanic (the inline-recover pattern used by the
// pipe / fan-out goroutines that need custom cleanup on the panic path).
func TestNoBareGoFunc_InWorkerSource(t *testing.T) {
	root := workerModuleRoot(t)

	// Files allowed to contain a bare `go func`: safego.go defines the
	// helper itself; test files are out of scope.
	allowed := map[string]bool{
		filepath.Join("internal", "jobs", "safego.go"): true,
	}

	out, err := exec.Command("grep", "-rln", "go func", root).Output()
	if err != nil {
		// grep exits 1 when there are zero matches — that is a pass.
		if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() == 1 {
			return
		}
		t.Fatalf("grep failed: %v", err)
	}

	for _, f := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if f == "" {
			continue
		}
		rel, relErr := filepath.Rel(root, f)
		if relErr != nil {
			rel = f
		}
		if strings.HasSuffix(f, "_test.go") {
			continue // test files are out of scope
		}
		if allowed[rel] {
			continue
		}
		// The file has a `go func`. It is acceptable ONLY if it also
		// references a recovery boundary helper.
		body, readErr := exec.Command("grep", "-lE", "Recover\\(|LogRecoveredPanic\\(|SafeGo\\(", f).Output()
		if readErr != nil || strings.TrimSpace(string(body)) == "" {
			t.Errorf("%s contains a bare `go func` with no panic-recovery boundary "+
				"(SafeGo / Recover / LogRecoveredPanic). A panic in that goroutine "+
				"crashes the whole worker pod — route it through jobs.SafeGo or add "+
				"`defer Recover(\"<site>\")` (P1-B).", rel)
		}
	}
}

// workerModuleRoot returns the absolute path to the worker module root by
// walking up from this test file's directory until go.mod is found.
func workerModuleRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(thisFile)
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatal("could not locate worker module root (go.mod)")
	return ""
}
