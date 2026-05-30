package metrics

// metrics_test.go — coverage for the worker metrics surface.
//
// The package is mostly a registry of promauto-declared counters and
// gauges. The one declared function is ReadyzCheckStatus and we
// exercise it directly here. Touching the other promauto vars also
// proves they registered cleanly at package init time.

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestReadyzCheckStatus_UpdatesLabelledGauge — the worker-side helper
// stamps service="instant-worker" inside the helper so callers can't
// accidentally publish under the wrong label. This pins both the gauge
// shape and the label-injection contract.
func TestReadyzCheckStatus_UpdatesLabelledGauge(t *testing.T) {
	for _, tc := range []struct {
		name  string
		check string
		value float64
	}{
		{"platform_db ok", "platform_db", 1},
		{"redis degraded", "redis", 0.5},
		{"brevo failed", "brevo", 0},
		{"river ok", "river", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ReadyzCheckStatus(tc.check, tc.value)
			got := testutil.ToFloat64(readyzCheckStatusGauge.WithLabelValues("instant-worker", tc.check))
			if got != tc.value {
				t.Errorf("readyz_check_status{service=instant-worker, check=%s} = %v; want %v",
					tc.check, got, tc.value)
			}
		})
	}
}

// TestAllMetrics_AreRegistered touches each exported promauto metric so
// a nil pointer (e.g. duplicate registration panic at init time) would
// fail the test immediately. The Add(0) / Set(0) calls are no-ops on a
// healthy counter/gauge.
func TestAllMetrics_AreRegistered(t *testing.T) {
	// Plain counters
	for _, c := range []interface{ Add(float64) }{
		ExpiredResourcesTotal,
		ExpireDeprovisionFailedTotal,
		ExpireRaceSkippedTotal,
		DeployExpiringSoonTotal,
		DeployExpiredTotal,
		DeployRemindersSentTotal,
		EntitlementDriftDetectedTotal,
		EntitlementRegradedTotal,
		EntitlementRegradeFailedTotal,
		RedisMaxmemoryCheckedTotal,
		RedisMaxmemoryAppliedTotal,
		RedisMaxmemorySkippedTotal,
		RedisMaxmemoryFailedTotal,
		RedisEvictedKeysTotal,
		RedisEvictedBytesTotal,
		RedisEvictedTenantsTotal,
		RedisEvictionFailedTotal,
		BillingReconcilerTeamsScanned,
		BillingReconcilerGraceMissed,
		BillingReconcilerRazorpayErrors,
		BillingReconcilerOrphanScanned,
		BillingReconcilerOrphanCorrected,
		BillingChargeUndeliverableTotal,
	} {
		c.Add(0)
	}

	// Plain gauges
	ActiveAnonymousResources.Set(0)

	// Counter vecs — observe one label combination to prove they
	// register cleanly. The label values are throwaway but the
	// cardinality must match the declaration in metrics.go.
	ReconcileRecoveredTotal.WithLabelValues("postgres").Add(0)
	ReconcileAbandonedTotal.WithLabelValues("postgres").Add(0)
	ResourceHeartbeatProbesTotal.WithLabelValues("postgres", "ok").Add(0)
	EntitlementDriftCorrectedTotal.WithLabelValues("postgres").Add(0)
	BillingReconcilerGapDetected.WithLabelValues("missing_subscription").Add(0)
	BillingReconcilerGapCorrected.WithLabelValues("missing_subscription").Add(0)
	GoroutinePanicsRecovered.WithLabelValues("test_job").Add(0)
	FailOpenTotal.WithLabelValues("test_site", "test_reason").Add(0)
	BrevoSendErrorsTotal.WithLabelValues("transient", "500").Add(0)
	EmailMissingRendererTotal.WithLabelValues("noop").Add(0)
	PropagationUnexpectedSkipTotal.WithLabelValues("kind_x", "postgres", "reason_y").Add(0)
	PropagationDeadLetteredTotal.WithLabelValues("test_reason", "test_kind").Add(0)
	PropagationUnknownKindTotal.WithLabelValues("unknown").Add(0)
	OrphanSweepReapedTotal.WithLabelValues("team_tombstoned").Add(0)
	OrphanSweepReapFailedTotal.WithLabelValues("team_tombstoned").Add(0)
	DeployJobFailedDetectedTotal.WithLabelValues("BackoffLimitExceeded").Add(0)

	// Gauge vecs
	ResourceDegradedGauge.WithLabelValues("postgres").Set(0)
	DeployTTLStateGauge.WithLabelValues("auto_24h").Set(0)
	PGPoolInUse.WithLabelValues("platform_db").Set(0)
	PGPoolIdle.WithLabelValues("platform_db").Set(0)
	PGPoolOpen.WithLabelValues("platform_db").Set(0)
	PGPoolMax.WithLabelValues("platform_db").Set(0)
	PGPoolWaitCount.WithLabelValues("platform_db").Set(0)
	PGPoolWaitDurationSeconds.WithLabelValues("platform_db").Set(0)
}
