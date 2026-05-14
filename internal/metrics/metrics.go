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
)
