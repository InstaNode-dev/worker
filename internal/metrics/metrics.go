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

	// ── entitlement_reconciler — Redis maxmemory metrics ─────────────────────
	//
	// A4 backfill: the reconciler now also sweeps dedicated k8s Redis pods to
	// ensure their maxmemory matches the tier cap from plans.yaml. Separate
	// counters from the Postgres metrics so NR dashboards can track the two
	// paths independently.
	//
	// NR alert: redis_regrade_failed > 0 for any 5-minute window → investigate.

	// RedisMaxmemoryCheckedTotal counts dedicated Redis pods inspected by the
	// A4 backfill reconciler each sweep. Includes both already-correct pods
	// (Applied=false, SkipReason="already correct") and pods that were updated.
	RedisMaxmemoryCheckedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_redis_maxmemory_checked_total",
		Help: "Dedicated Redis pods inspected by the A4 maxmemory reconciler.",
	})

	// RedisMaxmemoryAppliedTotal counts pods where CONFIG SET maxmemory was
	// applied (Applied=true from the provisioner). These are the ~9 pre-existing
	// pods that had no cap before the A4 fix; the count should converge to 0
	// within a few sweeps once all pods are corrected.
	RedisMaxmemoryAppliedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_redis_maxmemory_applied_total",
		Help: "Dedicated Redis pods with maxmemory successfully updated by the A4 reconciler.",
	})

	// RedisMaxmemorySkippedTotal counts pods where the provisioner reported
	// Applied=false. Covers both already-correct pods and soft-skips (legacy
	// pods without the redis-auth Secret). Does NOT include gRPC errors
	// (those go to RedisMaxmemoryFailedTotal).
	RedisMaxmemorySkippedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_redis_maxmemory_skipped_total",
		Help: "Dedicated Redis pods skipped by the A4 reconciler (already correct or legacy pod).",
	})

	// RedisMaxmemoryFailedTotal counts sweep iterations where the provisioner
	// RegradeResource call returned an error (gRPC transport failure, not a
	// soft skip). The row is left for the next sweep (fail-soft).
	RedisMaxmemoryFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_redis_maxmemory_failed_total",
		Help: "Dedicated Redis pods the A4 reconciler failed to regrade (gRPC error, retried next sweep).",
	})

	// ── EnforceStorageQuotaWorker — shared-Redis per-tenant eviction (A4) ─────
	//
	// Shared-backend Redis tenants (anonymous/free, key-scoped ACL users on the
	// `redis-provision` pod) have no per-user maxmemory — the quota worker
	// SCAN+DELETEs an over-cap tenant's `{token}:*` keyspace oldest-first. These
	// counters track that eviction path; they are distinct from the
	// instant_redis_maxmemory_* counters (which track DEDICATED k8s pods).
	//
	// NR alert (per-tenant — leading indicator):
	//   instant_redis_evicted_tenants_total rising steadily → free tenants are
	//   routinely hitting cap; expected, but a sharp spike warrants a look.
	//
	// NR alert (pod-wide — defense-in-depth backstop): configure an alert on the
	// shared `redis-provision` pod's used_memory / maxmemory ratio —
	//   WHEN used_memory / maxmemory > 0.85 for 5m THEN page.
	// The per-tenant eviction is the first line of defence; the pod-wide ratio
	// alert catches the case where eviction is falling behind the noisy neighbour.

	// RedisEvictedKeysTotal counts keys deleted from over-quota shared-backend
	// Redis tenants by the quota worker's eviction loop.
	RedisEvictedKeysTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_redis_evicted_keys_total",
		Help: "Keys deleted from over-quota shared-backend Redis tenants by the quota worker.",
	})

	// RedisEvictedBytesTotal counts bytes reclaimed (best-effort, summed from
	// MEMORY USAGE) by shared-backend Redis per-tenant eviction.
	RedisEvictedBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_redis_evicted_bytes_total",
		Help: "Bytes reclaimed from over-quota shared-backend Redis tenants by the quota worker.",
	})

	// RedisEvictedTenantsTotal counts tenants that had at least one key evicted
	// in a sweep — i.e. distinct over-cap shared-backend Redis tenants enforced.
	RedisEvictedTenantsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_redis_evicted_tenants_total",
		Help: "Distinct over-quota shared-backend Redis tenants enforced (>=1 key evicted) by the quota worker.",
	})

	// RedisEvictionFailedTotal counts tenants whose eviction pass returned an
	// error (Redis connectivity, parse failure, or a prefix-safety violation).
	// Fail-soft: the row is left for the next sweep.
	RedisEvictionFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_redis_eviction_failed_total",
		Help: "Shared-backend Redis tenants the quota worker failed to evict (retried next sweep).",
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
