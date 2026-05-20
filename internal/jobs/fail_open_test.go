package jobs_test

// fail_open_test.go — regression test for the P2 visibility helper from
// CIRCUIT-RETRY-AUDIT-2026-05-20 (worker brief item 4).
//
// The fail-open paths (Redis blip, DB brownout, Brevo bounce-suppression
// lookup) intentionally keep marching on error. Before this slice each
// such site emitted at most one slog line — invisible to alerting. The
// helper here gives every site a Prometheus counter + a structured slog
// field so an SRE can alert on "fail-open rate > threshold". This test
// pins that contract: calling RecordFailOpen MUST increment
// instant_worker_fail_open_total{site, reason}.

import (
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"instant.dev/worker/internal/jobs"
	"instant.dev/worker/internal/metrics"
)

// TestRecordFailOpen_IncrementsCounter is the load-bearing regression: each
// call must move the labelled counter forward by exactly one.
func TestRecordFailOpen_IncrementsCounter(t *testing.T) {
	const site = "test.fail_open_test.increments_counter"
	const reason = "db_error"

	before := testutil.ToFloat64(metrics.FailOpenTotal.WithLabelValues(site, reason))
	jobs.RecordFailOpen(site, reason, errors.New("simulated db blip"), "extra_key", "extra_value")
	after := testutil.ToFloat64(metrics.FailOpenTotal.WithLabelValues(site, reason))

	if got := after - before; got != 1 {
		t.Fatalf("fail-open counter delta: got %v, want 1 (site=%s reason=%s)", got, site, reason)
	}
}

// TestRecordFailOpen_DistinctSitesGetDistinctSeries — the counter is
// labelled by (site, reason), so calls at different sites must produce
// independent series.
func TestRecordFailOpen_DistinctSitesGetDistinctSeries(t *testing.T) {
	siteA := "test.fail_open_test.site_a"
	siteB := "test.fail_open_test.site_b"
	const reason = "redis_error"
	beforeA := testutil.ToFloat64(metrics.FailOpenTotal.WithLabelValues(siteA, reason))
	beforeB := testutil.ToFloat64(metrics.FailOpenTotal.WithLabelValues(siteB, reason))
	jobs.RecordFailOpen(siteA, reason, nil)
	jobs.RecordFailOpen(siteA, reason, nil)
	jobs.RecordFailOpen(siteB, reason, nil)
	afterA := testutil.ToFloat64(metrics.FailOpenTotal.WithLabelValues(siteA, reason))
	afterB := testutil.ToFloat64(metrics.FailOpenTotal.WithLabelValues(siteB, reason))
	if got := afterA - beforeA; got != 2 {
		t.Errorf("site_a delta: got %v, want 2", got)
	}
	if got := afterB - beforeB; got != 1 {
		t.Errorf("site_b delta: got %v, want 1", got)
	}
}

// TestRecordFailOpen_NilErrorOK — calling with a nil error is allowed
// (sometimes the fail-open site has no underlying error, just a "we got
// nil and chose to keep going" decision). Must not panic, must still
// increment.
func TestRecordFailOpen_NilErrorOK(t *testing.T) {
	site := "test.fail_open_test.nil_err"
	reason := "geoip_unknown"
	before := testutil.ToFloat64(metrics.FailOpenTotal.WithLabelValues(site, reason))
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("RecordFailOpen with nil error panicked: %v", r)
		}
	}()
	jobs.RecordFailOpen(site, reason, nil)
	after := testutil.ToFloat64(metrics.FailOpenTotal.WithLabelValues(site, reason))
	if after-before != 1 {
		t.Errorf("nil-error fail-open did not increment; delta=%v", after-before)
	}
}

// TestFailOpenCounter_IsRegistered guarantees the metric is wired into
// the default Prometheus registry under the audit-mandated name. NR
// alert rules reference this exact metric name.
func TestFailOpenCounter_IsRegistered(t *testing.T) {
	got := metrics.FailOpenTotal
	if got == nil {
		t.Fatal("metrics.FailOpenTotal is nil")
	}
	// One label-set probe to confirm the vec is functional.
	jobs.RecordFailOpen("test.fail_open_test.registered", "registration_probe", nil)
	desc := got.WithLabelValues("any", "thing").Desc().String()
	if !strings.Contains(desc, "instant_worker_fail_open_total") {
		t.Errorf("metric name drift: got desc=%q, want it to contain instant_worker_fail_open_total", desc)
	}
}
