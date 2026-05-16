package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ExpiredResourcesTotal counts anonymous resources successfully marked deleted by the expiry job.
	ExpiredResourcesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_expired_resources_total",
		Help: "Anonymous resources expired (DB row marked deleted) by the worker",
	})

	// ActiveAnonymousResources is the count of active anonymous resources with a TTL (updated each expiry job run).
	ActiveAnonymousResources = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "instant_active_anonymous_resources",
		Help: "Active anonymous resources that have expires_at set (sampled when expire job runs)",
	})

	// ReconcileRecoveredTotal counts pending-but-reachable rows the
	// provisioner_reconciler flipped to status='active'. Cardinality is
	// labelled by resource_type for the NR widget.
	ReconcileRecoveredTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_reconcile_recovered_total",
		Help: "Pending resources recovered to status=active by the reconciler",
	}, []string{"resource_type"})

	// ReconcileAbandonedTotal counts pending-and-unreachable rows the
	// reconciler flipped to status='failed'. Drives the >5/hour NR alert.
	ReconcileAbandonedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_reconcile_abandoned_total",
		Help: "Pending resources abandoned (status=failed) by the reconciler",
	}, []string{"resource_type"})

	// ResourceHeartbeatProbesTotal counts probe attempts. Labelled by
	// resource_type and outcome ("ok" / "fail" / "skip") — the NR
	// success-rate widget computes sum(ok)/sum(ok+fail).
	ResourceHeartbeatProbesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_resource_heartbeat_probes_total",
		Help: "Resource heartbeat probe attempts by type and outcome",
	}, []string{"resource_type", "outcome"})

	// ResourceDegradedGauge is sampled at the end of each heartbeat run.
	// Labelled by resource_type so the dashboard can break down "how many
	// of my Postgres instances are unreachable right now".
	ResourceDegradedGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_resource_degraded_count",
		Help: "Active resources currently flagged degraded=true (sampled per heartbeat run)",
	}, []string{"resource_type"})

	// Deploy TTL metrics (Wave FIX-J). Labelled by ttl_policy so the NR
	// dashboard can compare auto_24h vs permanent populations.
	DeployTTLStateGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_deploy_ttl_state",
		Help: "Active deployments by ttl_policy (auto_24h | permanent | custom). Sampled per reminder tick.",
	}, []string{"policy"})

	// DeployExpiringSoonTotal counts deployments observed in the reminder
	// window during each tick — sum across ticks so an operator can
	// confirm "yes, the reminder loop is alive."
	DeployExpiringSoonTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_deploy_expiring_soon_total",
		Help: "Total deployments seen in the 12h reminder window across reminder ticks (cumulative).",
	})

	// DeployExpiredTotal counts rows soft-deleted by the deployment_expirer
	// job (one increment per row that crosses to status='expired').
	DeployExpiredTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_deploy_expired_total",
		Help: "Deployments soft-deleted (status='expired') by the expirer worker.",
	})

	// DeployRemindersSentTotal counts reminder emails actually dispatched
	// to a real owner email (post-CAS, post-email-send).
	DeployRemindersSentTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_deploy_reminders_sent_total",
		Help: "Deploy expiry reminder emails dispatched successfully.",
	})

	// EntitlementDriftDetectedTotal counts postgres resources the
	// entitlement reconciler found with a connection cap that no longer
	// matches their team's plan tier (applied_conn_limit IS NULL or != entitled).
	EntitlementDriftDetectedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_entitlement_drift_detected_total",
		Help: "Postgres resources found drifted from their plan's entitled connection cap.",
	})

	// EntitlementRegradedTotal counts resources successfully re-graded —
	// the provisioner returned applied=true and the row's applied_conn_limit
	// was updated to the entitled value.
	EntitlementRegradedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_entitlement_regraded_total",
		Help: "Drifted resources successfully re-graded by the entitlement reconciler.",
	})

	// EntitlementRegradeFailedTotal counts resources the reconciler tried to
	// re-grade but could not — a gRPC error or a provisioner-side skip
	// (applied=false). The row is left for the next sweep.
	EntitlementRegradeFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_entitlement_regrade_failed_total",
		Help: "Drifted resources the reconciler failed to re-grade (gRPC error or provisioner skip).",
	})

	// ── billing_reconciler metrics ────────────────────────────────────────────
	//
	// NR alert: billing.reconciler.gap_detected > 3 in 15m → PagerDuty P2.
	// This is the first observable signal for a dropped Razorpay webhook.

	// BillingReconcilerTeamsScanned is a running total of teams inspected each
	// reconciler tick. Labelled per-tick so an operator can confirm the sweep
	// is alive and reaching all subscriber rows.
	BillingReconcilerTeamsScanned = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_billing_reconciler_teams_scanned_total",
		Help: "Teams with a Razorpay subscription_id inspected by the billing reconciler.",
	})

	// BillingReconcilerGapDetected counts mismatches between Razorpay's live
	// subscription state and teams.plan_tier. Labelled by direction (upgrade /
	// downgrade) so the NR alert can fire on either axis independently.
	// This is the primary signal for a dropped Razorpay webhook.
	BillingReconcilerGapDetected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_billing_reconciler_gap_detected_total",
		Help: "Mismatches found between Razorpay subscription state and DB plan_tier (labelled by direction).",
	}, []string{"direction"})

	// BillingReconcilerGapCorrected counts mismatches that were successfully
	// corrected by the reconciler. Labelled by direction to match GapDetected.
	// (gap_detected - gap_corrected) per tick = uncorrected gaps (alert if > 0).
	BillingReconcilerGapCorrected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_billing_reconciler_gap_corrected_total",
		Help: "Mismatches successfully corrected by the billing reconciler (labelled by direction).",
	}, []string{"direction"})

	// BillingReconcilerGraceMissed counts teams for which the reconciler opened
	// a grace period that the webhook had not created (halted/paused status with
	// no active payment_grace_periods row). A non-zero value means at least one
	// subscription.charged_failed webhook was dropped.
	BillingReconcilerGraceMissed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_billing_reconciler_grace_missed_total",
		Help: "Teams for which the reconciler opened a missed grace period (halted/paused with no active grace row).",
	})

	// BillingReconcilerRazorpayErrors counts Razorpay API call failures during
	// a reconciler tick, including circuit-open events. Used to distinguish
	// "correctable mismatches" from "ticks that couldn't check because Razorpay
	// was down".
	BillingReconcilerRazorpayErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_billing_reconciler_razorpay_errors_total",
		Help: "Razorpay API errors (including circuit-open) encountered during a reconciler tick.",
	})
)
