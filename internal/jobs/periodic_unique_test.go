package jobs

// periodic_unique_test.go — W1 (P1-W3-07 / P1-W4-06 / P2-W5-05) regression
// guard. The worker runs at replicas:2; River's periodic-job scheduler runs
// independently in each pod, so a periodic job with no UniqueOpts is
// enqueued — and therefore RUN — twice every tick (double lifecycle emails,
// double audit_log rows, double Razorpay API spend on the billing sweep).
//
// This test iterates the LIVE periodic-job registry (buildPeriodicJobs, the
// exact slice StartWorkers hands River) rather than a hand-typed list, per
// CLAUDE rule 18: a 34th periodic job added without UniqueOpts fails CI here
// instead of silently double-running in prod.
//
// River's PeriodicJob struct keeps its constructorFunc unexported, so the
// test reaches it via reflect + unsafe. That's deliberate: the alternative
// (a parallel hand-maintained list) is itself a single-site fallacy — it
// would drift from the real registry the moment someone adds a job.

import (
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/riverqueue/river"
	"instant.dev/worker/internal/config"
)

// periodicConstructorOf extracts the (args, *InsertOpts) constructor closure
// from a *river.PeriodicJob via its unexported constructorFunc field.
func periodicConstructorOf(t *testing.T, pj *river.PeriodicJob) func() (river.JobArgs, *river.InsertOpts) {
	t.Helper()
	v := reflect.ValueOf(pj).Elem()
	f := v.FieldByName("constructorFunc")
	if !f.IsValid() {
		t.Fatalf("river.PeriodicJob has no constructorFunc field — River's struct shape changed; update this test")
	}
	// The field is unexported; make it readable via unsafe.
	f = reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()
	ctor, ok := f.Interface().(river.PeriodicJobConstructor)
	if !ok {
		t.Fatalf("constructorFunc is not a river.PeriodicJobConstructor")
	}
	return ctor
}

// TestPeriodicJobs_AllCarryUniqueOpts is the W1 registry-iterating guard:
// EVERY periodic job in buildPeriodicJobs must return a non-nil *InsertOpts
// whose UniqueOpts carries ByArgs=true and a non-zero ByPeriod. A job that
// returns nil opts (or opts with an empty UniqueOpts) double-runs under
// replicas:2.
func TestPeriodicJobs_AllCarryUniqueOpts(t *testing.T) {
	cfg := &config.Config{Environment: "production"}
	jobs := buildPeriodicJobs(cfg)
	if len(jobs) == 0 {
		t.Fatal("buildPeriodicJobs returned no jobs — the registry is empty, the test would vacuously pass")
	}

	for i, pj := range jobs {
		ctor := periodicConstructorOf(t, pj)
		args, opts := ctor()
		kind := args.Kind()

		if opts == nil {
			t.Errorf("periodic job #%d (kind=%q): constructor returned nil *InsertOpts — under replicas:2 River enqueues this sweep on BOTH pods every tick. Route it through periodicInsertOpts / reconcileInsertOpts / billingInsertOpts.", i, kind)
			continue
		}
		if !opts.UniqueOpts.ByArgs {
			t.Errorf("periodic job #%d (kind=%q): UniqueOpts.ByArgs is false — set it via the periodicInsertOpts family so a sibling replica's identical tick is deduped.", i, kind)
		}
		if opts.UniqueOpts.ByPeriod <= 0 {
			t.Errorf("periodic job #%d (kind=%q): UniqueOpts.ByPeriod = %v; want the job's scheduling interval. Without a period window the dedup is across ALL history, which would block the job after its first-ever run.", i, kind, opts.UniqueOpts.ByPeriod)
		}
	}
}

// TestPeriodicInsertOpts_HelpersCarryUniqueOpts pins the three helpers
// directly — buildPeriodicJobs routes every job through one of them, so a
// regression in the helper is the cheapest place to catch the W1 bug.
func TestPeriodicInsertOpts_HelpersCarryUniqueOpts(t *testing.T) {
	period := 7 * time.Minute

	def := periodicInsertOpts(period)
	if def == nil || !def.UniqueOpts.ByArgs || def.UniqueOpts.ByPeriod != period {
		t.Errorf("periodicInsertOpts(%v) = %+v; want ByArgs=true ByPeriod=%v", period, def, period)
	}

	rec := reconcileInsertOpts(period)
	if rec == nil || !rec.UniqueOpts.ByArgs || rec.UniqueOpts.ByPeriod != period || rec.Queue != queueReconcile {
		t.Errorf("reconcileInsertOpts(%v) = %+v; want ByArgs=true ByPeriod=%v Queue=%q", period, rec, period, queueReconcile)
	}

	bil := billingInsertOpts(period)
	if bil == nil || !bil.UniqueOpts.ByArgs || bil.UniqueOpts.ByPeriod != period || bil.Queue != queueBilling {
		t.Errorf("billingInsertOpts(%v) = %+v; want ByArgs=true ByPeriod=%v Queue=%q", period, bil, period, queueBilling)
	}
}
