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
	// Prime all three e2e_cohort_sweep outcome label values so /metrics
	// exposes them from process start (lazy emit otherwise leaves the panel
	// empty until the first real cohort sweep fires).
	E2ECohortSweptTotal.WithLabelValues("swept").Add(0)
	E2ECohortSweptTotal.WithLabelValues("failed").Add(0)
	E2ECohortSweptTotal.WithLabelValues("skipped_not_cohort").Add(0)
	DeployJobFailedDetectedTotal.WithLabelValues("BackoffLimitExceeded").Add(0)
	// Prime the runtime-failure detector label so /metrics exposes it from
	// process start (lazy *Vec; first real observation is a ProgressDeadlineExceeded
	// detection in deploy_status_reconcile).
	DeployRuntimeFailedDetectedTotal.WithLabelValues("progress_deadline_exceeded").Add(0)
	// Prime all four DeployAutopsyCapturedTotal outcome label values so
	// /metrics exposes them from process start (lazy emit otherwise leaves
	// the panel empty until the first real autopsy fires).
	DeployAutopsyCapturedTotal.WithLabelValues("logs_captured").Add(0)
	DeployAutopsyCapturedTotal.WithLabelValues("logs_unavailable").Add(0)
	DeployAutopsyCapturedTotal.WithLabelValues("already_present").Add(0)
	DeployAutopsyCapturedTotal.WithLabelValues("audit_emit_failed").Add(0)
	// Prime all four scale-to-zero outcome label values so /metrics exposes the
	// series from process start (lazy *Vec otherwise leaves the dashboard tile
	// empty until the first real scale action).
	DeployScaledToZeroTotal.WithLabelValues("scaled_down").Add(0)
	DeployScaledToZeroTotal.WithLabelValues("woke_up").Add(0)
	DeployScaledToZeroTotal.WithLabelValues("wake_failed").Add(0)
	DeployScaledToZeroTotal.WithLabelValues("scale_failed").Add(0)

	// Prime the Layer-3 payment-prober label families so /metrics exposes the
	// series from process start (lazy *Vec otherwise leaves the dashboard tile
	// empty until the operator lights PAYMENT_PROBE_ENABLED and the first tick
	// fires). One representative (leg, result) plus a latency observation.
	PaymentProbeOutcomeTotal.WithLabelValues("checkout_reachable", "pass").Add(0)
	PaymentProbeOutcomeTotal.WithLabelValues("checkout_reachable", "fail").Add(0)
	PaymentProbeOutcomeTotal.WithLabelValues("webhook_security", "pass").Add(0)
	PaymentProbeOutcomeTotal.WithLabelValues("upgrade_webhook_e2e", "degraded").Add(0)
	PaymentProbeLatencySeconds.WithLabelValues("checkout_reachable").Observe(0)

	// Plain gauge
	DeployIdleApps.Set(0)

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
