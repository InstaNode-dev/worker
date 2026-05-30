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

	// ExpireDeprovisionFailedTotal counts resources whose physical backend
	// teardown (DeprovisionResource) returned an error during an expiry tick.
	// Per MR-P0-1a (BugBash 2026-05-20) the reaper now LEAVES such a row in
	// its reapable status (it is NOT marked deleted) so the next tick retries
	// — preventing the namespace/DB leak. A sustained non-zero rate means the
	// provisioner is failing teardowns and customer infra is accumulating.
	// NR alert: rate(instant_expire_deprovision_failed_total[15m]) > 0 → P2.
	ExpireDeprovisionFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_expire_deprovision_failed_total",
		Help: "Resources whose backend deprovision failed during expiry (row left reapable for retry, MR-P0-1a).",
	})

	// ExpireRaceSkippedTotal counts rows the reaper skipped at the per-row
	// FOR UPDATE re-confirm because a concurrent state change (the
	// `subscription.charged` upgrade webhook, or a team flip to
	// deletion_requested) had cleared the reapable predicate after batch
	// SELECT. Per MR-P1-5 / T5 P0-3 (BugBash 2026-05-20) the per-row tx
	// re-confirms tier+expires_at+team-status under a row lock; a non-zero
	// rate is *expected* (and a positive signal — the race guard fired and
	// saved a paying customer's DB from a wrongful DROP). Sustained 0 is
	// also fine. NR alert: this is not by itself an error metric; pair with
	// `instant_expire_deprovision_failed_total` to spot patterns.
	ExpireRaceSkippedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_expire_race_skipped_total",
		Help: "Reaper rows skipped at FOR UPDATE re-confirm because an upgrade webhook or deletion-grace transition won the race (MR-P1-5 / T5 P1-7).",
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

	// EntitlementDriftCorrectedTotal counts resources whose infrastructure
	// entitlement was actually corrected by the reconciler — a Postgres
	// connection-cap regrade that the provisioner applied, or a Redis maxmemory
	// CONFIG SET that landed (F6). Distinct from EntitlementRegradedTotal so
	// the per-event structured `jobs.entitlement_reconciler.drift_corrected`
	// log line has a 1:1 counter for NR alerting. Labelled by resource_type so
	// monitoring can alert on a rising correction rate per resource class —
	// a sustained non-zero rate means upgrades are routinely landing the team
	// row before the infra cap, which is the F6 partial-upgrade symptom.
	// NR alert: rate(instant_entitlement_drift_corrected_total[1h]) climbing → investigate.
	EntitlementDriftCorrectedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_entitlement_drift_corrected_total",
		Help: "Resources whose infra entitlement drift the reconciler corrected (labelled by resource_type).",
	}, []string{"resource_type"})

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

	// BillingReconcilerOrphanScanned counts pending_checkouts rows inspected by
	// the Razorpay-authoritative orphan sweep (F1). These are checkouts whose
	// subscription_id was never persisted onto teams.stripe_customer_id — the
	// team is structurally invisible to the primary teams-table sweep, so this
	// second pass starts from pending_checkouts instead. NR can confirm the
	// orphan sweep is alive by watching this counter advance per tick.
	BillingReconcilerOrphanScanned = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_billing_reconciler_orphan_scanned_total",
		Help: "pending_checkouts rows inspected by the billing reconciler's orphan (un-persisted subscription_id) sweep.",
	})

	// BillingReconcilerOrphanCorrected counts teams the orphan sweep elevated:
	// Razorpay reports the subscription active/paid but teams.plan_tier was
	// stuck below the entitled tier because the checkout-time subscription_id
	// UPDATE was lost. A non-zero value means at least one paying customer was
	// charged-but-not-upgraded and the orphan sweep recovered them.
	// NR alert: instant_billing_reconciler_orphan_corrected_total > 0 in 15m → P1.
	BillingReconcilerOrphanCorrected = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_billing_reconciler_orphan_corrected_total",
		Help: "Teams elevated by the billing reconciler's orphan sweep (paid at Razorpay, no persisted subscription_id).",
	})

	// BillingChargeUndeliverableTotal counts audit_log rows the worker
	// observes with kind='billing.charge_undeliverable' on each reconciler
	// tick. B11-F3 (BugBash 2026-05-20): the api emits this audit row
	// when a Razorpay webhook arrives carrying a payload the trust pass
	// cannot resolve to a real team (synthetic team_id, junk plan_id, or
	// signed payload from an attacker). The api side writes the row
	// LOUDLY (not a silent 200) but no observability surface fired on it
	// — no NR/Prom alert was wired, so a steady drip of phishing webhook
	// payloads or webhook-routing drift was invisible. This counter
	// surfaces those rows via a worker-side scan so the standard
	// counter-rate NR alert keys on a single metric independent of which
	// service emitted the audit.
	//
	// The reconciler ticks every 15 min; on each tick it counts the
	// number of charge_undeliverable rows whose created_at falls inside
	// the tick window and increments by that delta. Per-tick window keeps
	// the metric monotonic and lossy-safe — a row created during a worker
	// downtime is missed (the metric is observability, not authority),
	// which matches the rest of the worker's fail-open posture.
	//
	// NR alert: increase(instant_billing_charge_undeliverable_total[1h]) > 0 → P2.
	BillingChargeUndeliverableTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "instant_billing_charge_undeliverable_total",
		Help: "audit_log rows with kind='billing.charge_undeliverable' observed by the worker since pod start. The api emits these when a webhook payload cannot be resolved to a real team (B11-F3 BugBash 2026-05-20).",
	})

	// GoroutinePanicsRecovered counts panics caught by the shared
	// fire-and-forget goroutine recovery helper (jobs.SafeGo). A non-zero
	// value means a background goroutine panicked but the worker pod
	// survived (without the helper the panic crashes the whole process).
	// Labelled by `site` — a stable string identifying the goroutine — so
	// the NR alert can point straight at the failing job.
	// NR alert: instant_worker_goroutine_panics_recovered_total > 0 in 15m → P2.
	GoroutinePanicsRecovered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_worker_goroutine_panics_recovered_total",
		Help: "Panics recovered by the worker's fire-and-forget goroutine recovery helper, labelled by site.",
	}, []string{"site"})

	// ── fail-open visibility (CIRCUIT-RETRY-AUDIT-2026-05-20, P2 worker) ──────
	//
	// Every fail-open site in the worker — Redis errors, DB brownouts, Brevo
	// suppression-lookup failures, GeoIP misses, k8s lookups — increments this
	// counter so an SRE can alert on "fail-open rate above a baseline" without
	// changing the fail-mode semantics of any particular site.
	//
	// Labels:
	//
	//   site   — stable, low-cardinality string identifying the call site
	//            (e.g. "event_email_forwarder.bounce_suppression",
	//            "billing_reconciler.upgrade_audit_insert", ...). NEVER
	//            include team_id / resource_id / email — keep cardinality
	//            bounded.
	//
	//   reason — short classification of WHAT the underlying failure was
	//            ("redis_error", "db_error", "brevo_classify_failed",
	//            "geoip_unknown"). Distinct from `site` so the operator can
	//            slice by infrastructure (all redis_error sites) OR by job
	//            (all billing_reconciler sites). Bounded by code path.
	//
	// NR alert (suggested): rate(instant_worker_fail_open_total[15m]) by site,
	//   alert when a single site exceeds N events per minute sustained for
	//   5+ minutes — that points straight at the brownout backend.
	//
	// IMPORTANT: incrementing this counter MUST be paired with a structured
	// slog line that includes the key/value pair `fail_open=true` (or a
	// `fail_open` slog.Bool attr) — see the helper in fail_open.go. The
	// log line carries the high-cardinality context (team_id, error) that
	// the metric must not.
	FailOpenTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_worker_fail_open_total",
		Help: "Worker fail-open events (request proceeded despite a check failing). Labelled by call site and failure reason. Pair with the slog 'fail_open=true' field for context.",
	}, []string{"site", "reason"})

	// ── Brevo send-classification (BugBash 2026-05-20 P0-1) ──────────────────
	//
	// Every Brevo POST that does NOT return a 2xx increments this counter,
	// labelled by:
	//
	//   classification — one of "transient" / "permanent" / "skipped_no_template"
	//                    (same vocabulary as email.SendClass.String()). Drives the
	//                    NR alert that distinguishes "we're losing email" from
	//                    "operator hasn't wired a template yet".
	//
	//   status_code    — the Brevo HTTP status as a string ("401", "429", "503",
	//                    or "0" for a transport-level network/timeout error
	//                    where there was no response). Bounded by HTTP — no
	//                    cardinality explosion.
	//
	// NR alert (suggested):
	//   rate(brevo_send_errors_total{classification="transient"}[5m]) > 0
	//     for >10 min → operator-page (Brevo / network outage; cursor held
	//     so email is recoverable but the backlog grows).
	//   rate(brevo_send_errors_total{classification="permanent"}[15m]) > 0
	//     → poisoned audit row(s); inspect the audit_log + log stream.
	//
	// IMPORTANT: this counter is provider-specific. SES has its own error
	// model (smithy fault classifications) and will need its own
	// ses_send_errors_total counter when the SES path is fully load-bearing —
	// keeping them split avoids muddying "Brevo dropped 100 emails" alerts
	// with SES throttles or vice versa.
	BrevoSendErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "brevo_send_errors_total",
		Help: "Brevo send classifications for non-2xx outcomes. Labelled by classification (transient|permanent|skipped_no_template) and status_code ('0' for network errors).",
	}, []string{"classification", "status_code"})

	// ── F4 missing-renderer counter (BugBash 2026-05-20) ──────────────────────
	//
	// Increments every time the event_email_forwarder sees an audit_log row
	// whose kind has a builder but NO entry in eventEmailBodyRenderers. This
	// is the exact failure mode of the 2026-05-15 expiry-email regression
	// (resource.expiry_imminent shipped without a renderer; rows were
	// silently consumed). The runtime counter is the backstop — the
	// TestEventEmail_EverySupportedKindFullyWired registry test catches the
	// half-registration at CI time, and this metric catches it in prod
	// if anything ever slips past CI.
	//
	// Labelled by `kind` so the NR alert can name the offending registry
	// entry. Kind cardinality is bounded by supportedAuditKinds (~30
	// entries) — safe for Prometheus.
	//
	// NR alert (suggested):
	//   sum(rate(email_missing_renderer_total[5m])) by (kind) > 0
	//     for any 5m window → P1 page. A non-zero value means at least
	//     one customer email kind is silently being dropped because a
	//     deploy half-registered a new audit kind.
	EmailMissingRendererTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "email_missing_renderer_total",
		Help: "Event-email forwarder hits on an audit_log row whose kind has a builder but no Go renderer. A non-zero rate means a kind is being silently dropped — fix the eventEmailBodyRenderers map.",
	}, []string{"kind"})

	// ── propagation_runner — unexpected_skip counter (CHAOS-DRILL-2026-05-20 F1) ─
	//
	// Every time the propagation_runner's per-resource RegradeResource call
	// returns (Applied=false, SkipReason=<not in the allowed-skip whitelist>),
	// this counter increments. Pre-fix the runner WARN-and-applied that case,
	// silently stamping the propagation row as applied without the entitlement
	// landing — a paying customer ended up with "Pro on paper, hobby-grade infra"
	// and no alert (CHAOS-DRILL-2026-05-20 finding #1). Now the case is treated
	// as a retryable error: the row retries via the backoff schedule and
	// dead-letters at propagationMaxAttempts. This counter is the leading
	// indicator; the dead-letter audit row is the alert-able lagging signal.
	//
	// Labels:
	//
	//   kind           — pending_propagations.kind ("tier_elevation", etc.).
	//                    Bounded by propagationKnownKinds (~1-3 entries).
	//
	//   resource_type  — "postgres" | "redis" | "mongodb" (the offending
	//                    resource class). Bounded by ResourceType enum.
	//
	//   skip_reason    — a SHORT canonical bucket derived from the raw
	//                    skip_reason string ("postgres_admin_secret_missing",
	//                    "namespace_not_found", "other"). The runner does
	//                    the bucketing (jobs.bucketSkipReason) so cardinality
	//                    stays bounded — never pass the raw SkipReason here.
	//
	// NR alert (suggested):
	//   sum(rate(instant_propagation_unexpected_skip_total[15m])) > 0
	//     for 30+ minutes → P2 page. A single isolated event is the
	//     mid-deprovisioning-race signal; a sustained rate is a real
	//     downstream regression and an operator must investigate before
	//     the row dead-letters ~24h later.
	PropagationUnexpectedSkipTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_propagation_unexpected_skip_total",
		Help: "propagation_runner per-resource RegradeResource returned (Applied=false, SkipReason=<not in allowed-skip whitelist>). Leading indicator for the dead-letter alert.",
	}, []string{"kind", "resource_type", "skip_reason"})

	// ── propagation_runner — dead-letter + unknown-kind counters (CHAOS F2/F3) ─
	//
	// PropagationDeadLetteredTotal increments every time the propagation_runner
	// transitions a row to failed_at + emits a propagation.*dead_lettered audit
	// row. Two triggers feed this single metric, distinguished by `reason`:
	//
	//   reason="max_attempts" — the modal path. Per-resource RegradeResource
	//                           failures, F1's unexpected_skip-as-failure, and
	//                           markApplied DB failures all converge here once
	//                           they reach propagationMaxAttempts.
	//   reason="unknown_kind" — CHAOS F2: a worker pod that doesn't recognise
	//                           a `kind` enqueued by a newer api image. Without
	//                           the F2 fix these escape the maxAttempts ceiling.
	//
	// `kind` carries the row's pending_propagations.kind value for the
	// max_attempts path (bounded by propagationKnownKinds — ~1-3 entries).
	// The unknown_kind path passes kind="unknown_kind" as a bounded bucket,
	// so an attacker-controlled api-side enqueue can't blow up worker
	// label cardinality.
	//
	// NR alert (suggested):
	//   rate(instant_propagation_dead_lettered_total[5m]) > 0 for 5m → P1 page.
	//   propagation_runner is the last line of defence between Razorpay webhook
	//   delivery and customer infra; any dead-letter means a paying customer's
	//   regrade fell through (or, on the unknown_kind path, that a worker pod
	//   is running an old image vs the api).
	PropagationDeadLetteredTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_propagation_dead_lettered_total",
		Help: "propagation_runner rows transitioned to failed_at. Labelled by reason (max_attempts|unknown_kind) and kind (pending_propagations.kind, or 'unknown_kind' for F2's bounded bucket).",
	}, []string{"reason", "kind"})

	// PropagationUnknownKindTotal counts every TICK that picked up at least
	// one row whose kind had no handler in propagationHandlers. Distinct
	// from PropagationDeadLetteredTotal{reason="unknown_kind"} — that fires
	// once at the END of the row's life (after maxAttempts), this fires on
	// EVERY tick while the row is retrying. Lets the operator see "the
	// worker is older than the api" within seconds rather than waiting the
	// ~24h backoff for the dead-letter to land.
	//
	// `kind` is the raw pending_propagations.kind value. Bounded by the
	// api-side enqueue surface (NOT by attacker input — only the api can
	// INSERT into pending_propagations); the cardinality risk is accepted
	// because in the rollback-drift scenario the operator wants to know
	// EXACTLY which new kind their old worker is rejecting.
	//
	// NR alert (suggested):
	//   sum(rate(instant_propagation_unknown_kind_total[5m])) by (kind) > 0
	//     for 5m → P2 page. Action: finish the rollout.
	PropagationUnknownKindTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_propagation_unknown_kind_total",
		Help: "propagation_runner ticks that saw at least one row whose kind has no handler (image-skew indicator). Labelled by kind.",
	}, []string{"kind"})

	// readyzCheckStatusGauge — per-component readiness status surfaced by
	// /readyz on this service's HTTP sidecar (:8091). See the matching
	// gauge in the api repo at api/internal/metrics/metrics.go for the
	// full contract. The shared NR alert fires when any check on any
	// service stays at 0 (failed) for >5 minutes.
	readyzCheckStatusGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "readyz_check_status",
		Help: "Per-component readiness status (1=ok, 0.5=degraded, 0=failed). Set by /readyz on every probe.",
	}, []string{"service", "check"})

	// ── DeployStatusReconciler — Job-failed override (silent-deploy-failure fix) ─
	//
	// Increments every time the reconciler detects a kaniko build Job in the
	// `Failed` state (BackoffLimit exhausted, ActiveDeadlineSeconds exceeded,
	// or any JobCondition of type Failed). The pre-fix reconciler only
	// queried the runtime appsv1.Deployment and missed this whole class of
	// failure (2026-05-30 incident: a user's deploy sat at `building` forever
	// because the build pod was GC'd and there was no Deployment object to
	// query). This counter is the leading indicator that the Job-query
	// override is doing its job; pair with `instant_deploy_autopsy_captured_total`
	// to see the autopsy follow-through.
	//
	// Labels:
	//   reason — the Job's `Failed` condition reason verbatim. k8s uses a
	//            small, stable set: "BackoffLimitExceeded", "DeadlineExceeded",
	//            "PodFailurePolicy". Plus two bounded fallbacks set in
	//            jobFailureReason: "failed_no_reason" (condition present but
	//            no reason string) and "backoff_limit_exceeded" (cluster-
	//            version backstop — JobFailed condition not stamped but
	//            Status.Failed > BackoffLimit).
	//
	// NR alert (suggested):
	//   sum(rate(instant_deploy_job_failed_detected_total[15m])) by (reason) > 0.5
	//     for 30+ minutes → P2 page. A sustained rate of
	//     reason="DeadlineExceeded" means the platform's kaniko build slot is
	//     timing out for many tenants (image bloat or a degraded GHCR push
	//     path); reason="BackoffLimitExceeded" is the modal Dockerfile-error
	//     bucket — alert at a higher threshold or visualize on the dashboard
	//     only.
	//
	// Catalog row (infra/observability/METRICS-CATALOG.md):
	//   instant_deploy_job_failed_detected_total | counter | reason | lazy
	//   (first observation is a real Job-failed detection — does not appear at
	//   /metrics until then; the test in metrics_test.go forces a label so the
	//   metric is registered at process start).
	DeployJobFailedDetectedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_deploy_job_failed_detected_total",
		Help: "Kaniko build Jobs detected in Failed state by deploy_status_reconcile (silent-deploy-failure fix, 2026-05-30). Labelled by Job Failed-condition reason.",
	}, []string{"reason"})

	// ── deploy_failure_autopsy — capture outcome counter (PR 2, 2026-05-30) ──
	//
	// Increments once per captureDeploymentAutopsy call, labelled by outcome.
	// Outcomes:
	//
	//   logs_captured       — at least one log line was captured from the app
	//                         pod OR the build pod fallback (the modal success
	//                         path for the silent-deploy-failure fix).
	//   logs_unavailable    — autopsy ran but no log lines could be captured
	//                         (pod already GC'd, image-pull failure, or DB
	//                         write failed). Reason + event fields are still
	//                         populated from k8s state + Job event fallback.
	//   already_present     — pure idempotent re-capture: the deployment_events
	//                         row already had a real (non-Unknown) reason and
	//                         this tick added nothing new. Distinguishes
	//                         "doing useful work" from "looping over old state".
	//   audit_emit_failed   — autopsy row upsert succeeded but the audit_log
	//                         emit (kind=deploy.failed → email forwarder)
	//                         failed. A non-zero rate means failure emails
	//                         are silently dropped.
	//
	// NR alert (suggested):
	//   sum(rate(instant_deploy_autopsy_captured_total{outcome="logs_unavailable"}[15m])) > 1
	//     for 30+ minutes → P2 page. A sustained rate means autopsies are
	//     consistently running too late (pods GC'd before capture). Action:
	//     check if the Job's TTLSecondsAfterFinished was reduced or if the
	//     reconciler tick interval drifted up.
	//   sum(rate(instant_deploy_autopsy_captured_total{outcome="audit_emit_failed"}[5m])) > 0
	//     → P1 page. Customers are not getting deploy.failed emails for at
	//     least one tenant; check platform-DB pool saturation.
	//
	// Catalog row (infra/observability/METRICS-CATALOG.md):
	//   instant_deploy_autopsy_captured_total | counter | outcome | lazy
	//   (label families primed in metrics_test.go so /metrics exposes the
	//   four outcomes from process start).
	DeployAutopsyCapturedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_deploy_autopsy_captured_total",
		Help: "deploy_failure_autopsy capture outcomes (PR 2, silent-deploy-failure fix). Labelled by outcome (logs_captured | logs_unavailable | already_present | audit_emit_failed).",
	}, []string{"outcome"})

	// ── orphan_sweep_reconciler — reap counters (2026-05-20) ──────────────────
	//
	// Every namespace / DB row the orphan-sweep reconciler reaps (or flips to
	// failed) increments this counter, labelled by `reason`. Distinct reasons
	// reflect distinct failure modes — operators alert on each independently:
	//
	//   reason="team_tombstoned"      — PASS 3: instant-deploy-* namespace whose
	//                                   team is tombstoned/deletion_pending or
	//                                   whose DB row has status='deleted'.
	//   reason="no_db_row"            — PASS 3: instant-deploy-* namespace with
	//                                   NO matching DB row (the P0-3 atomic-
	//                                   provision symptom — and the leading
	//                                   indicator alert for it).
	//   reason="failed_old_deployment" — PASS 3: instant-deploy-* namespace
	//                                   whose row has status='failed' AND
	//                                   created_at < 6h ago. The autopsy stays
	//                                   in deployment_events; the namespace
	//                                   doesn't need to linger paying compute.
	//   reason="failed_build"          — PASS 6: deployments row status IN
	//                                   ('building','deploying') for >30min
	//                                   whose pod is in
	//                                   ImagePullBackOff/ErrImagePull/
	//                                   CrashLoopBackOff. The reconciler flips
	//                                   the row to 'failed' so PASS 3 reaps
	//                                   the namespace 6h later.
	//   reason="customer_no_row"      — PASS 4: instant-customer-<token>
	//                                   namespace whose token has no
	//                                   active/paused/suspended resources row
	//                                   (the MR-P0-1b backstop).
	//   reason="stack_no_row"         — PASS 5: instant-stack-<id> namespace
	//                                   whose id has no stacks row (the
	//                                   T6 P0-1 prefix-mismatch backstop).
	//
	// NR alert (mandatory):
	//   sum(rate(instant_orphan_sweep_reaped_total{reason="no_db_row"}[1h])) > 0
	//     → P0 page. A no_db_row event means a deploy was provisioned (k8s
	//     namespace created) but the deployments INSERT never landed — the
	//     P0-3 atomic-provision bug surfacing in prod. Investigate same hour.
	//
	// NR alert (suggested):
	//   sum(rate(instant_orphan_sweep_reaped_total{reason="failed_build"}[15m])) > 5
	//     → P2 page. A burst of failed-build reaps means the kaniko/GHCR path
	//     is degraded for many customers at once.
	OrphanSweepReapedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_orphan_sweep_reaped_total",
		Help: "Orphan-sweep reconciler reaps, labelled by reason. no_db_row is the P0-3 atomic-provision leading indicator.",
	}, []string{"reason"})

	// OrphanSweepReapFailedTotal counts reaps the reconciler tried but could
	// not complete (k8s API error, DB write failure). Paired with
	// OrphanSweepReapedTotal so the per-reason ratio reveals "reconciler
	// detected the orphan but couldn't clean it" sustained failure modes.
	//
	// NR alert (suggested):
	//   sum(rate(instant_orphan_sweep_reap_failed_total[15m])) by (reason) > 0
	//     for 30+ minutes → P2 page. A single transient failure is fine; a
	//     sustained rate means the reap path itself is broken.
	OrphanSweepReapFailedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_orphan_sweep_reap_failed_total",
		Help: "Orphan-sweep reconciler reap attempts that failed (k8s API error or DB write failure), labelled by reason.",
	}, []string{"reason"})

	// PGPool* gauges expose worker's *sql.DB pool state. Sampled every
	// 5s by the exporter started from main.go. See api/internal/metrics
	// for the matching counterparts in the api process.
	//
	// Wave-3 chaos verify (2026-05-21): a 50-concurrent /db/new burst
	// against the api process exhausted the shared DigitalOcean Managed
	// Postgres user-connection ceiling — and worker's
	// event_email_forwarder failed with "remaining connection slots are
	// reserved for non-replication superuser connections" because there
	// were no slots left for the worker process to grab. These gauges
	// let an operator see worker's own saturation independently from
	// api's, so the next time the symptom appears the cause is
	// immediately localized.
	//
	// Label: pool — "platform_db" (worker's main pool against the
	// platform DB). Future arms (customer_db, redis_provision) would
	// each get their own label value, keeping cardinality bounded.
	PGPoolInUse = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_pg_pool_in_use",
		Help: "Postgres connections currently in use by the worker process pool. Sampled every 5s. Wave-3 chaos verify 2026-05-21.",
	}, []string{"pool"})

	PGPoolIdle = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_pg_pool_idle",
		Help: "Postgres connections currently idle in the worker process pool. Sampled every 5s.",
	}, []string{"pool"})

	PGPoolOpen = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_pg_pool_open",
		Help: "Postgres connections currently open (in-use + idle) in the worker process pool. Sampled every 5s.",
	}, []string{"pool"})

	PGPoolMax = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_pg_pool_max",
		Help: "Postgres connections ceiling (SetMaxOpenConns) on the worker pool. Constant for the process lifetime.",
	}, []string{"pool"})

	PGPoolWaitCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_pg_pool_wait_count",
		Help: "Cumulative count of connection-acquisition waits since process start (sql.DBStats.WaitCount). A flat line == no saturation; a steepening slope == pool saturated.",
	}, []string{"pool"})

	PGPoolWaitDurationSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "instant_pg_pool_wait_duration_seconds",
		Help: "Cumulative time spent waiting for a connection since process start, in seconds (sql.DBStats.WaitDuration). Pairs with instant_pg_pool_wait_count.",
	}, []string{"pool"})

	// AuthProbeOutcomeTotal — AUTH-004 synthetic prober counters. Labelled by
	// `leg` (email_start | exchange_headers | me) and `result` (pass | fail
	// | degraded). result="fail" is the alert-able signal — the AUTH-004
	// chain (broken /auth/exchange + missing ACAC header) was undetectable
	// for ~24h because nothing drove a real browser-shaped probe against
	// prod. NR alert: any fail in 10m → P0 (auth-probe-fail.json).
	// Prom rule: AuthProbeFail in prometheus-rules.yaml.
	// Emit site: worker/internal/jobs/auth_probe.go (AuthProbePromMetrics).
	AuthProbeOutcomeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_auth_probe_outcome_total",
		Help: "AUTH-004 synthetic prober outcomes per leg (email_start|exchange_headers|me) and result (pass|fail|degraded).",
	}, []string{"leg", "result"})

	// AuthProbeLatencySeconds — per-leg HTTP latency histogram. Only
	// observed on a real response (DNS / TCP errors omit the observation
	// so a sustained outage doesn't pile zeros into the bucket).
	AuthProbeLatencySeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "instant_auth_probe_latency_seconds",
		Help:    "AUTH-004 synthetic prober per-leg latency. Buckets centred on the per-leg latency budgets (50ms…5s).",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	}, []string{"leg"})

	// DeployProbeOutcomeTotal — hourly synthetic deploy prober counters.
	// Labelled by `leg` (submit | status | serve) and `result` (pass |
	// fail | degraded). result="fail" is the alert-able signal — the
	// 2026-05-30 morning truehomie-api stuck-build incident hid the
	// /deploy/new pipeline being broken for ~30 minutes until the user
	// reported it. NR alert: any fail in 30m → P0 (deploy-probe-fail.json).
	// Prom rule: DeployProbeFail in prometheus-rules.yaml.
	// Emit site: worker/internal/jobs/deploy_probe.go (DeployProbePromMetrics).
	DeployProbeOutcomeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "instant_deploy_probe_outcome_total",
		Help: "Hourly synthetic deploy prober outcomes per leg (submit|status|serve) and result (pass|fail|degraded).",
	}, []string{"leg", "result"})

	// DeployProbeLatencySeconds — per-leg HTTP/poll latency histogram.
	// Only observed on a real response (DNS / TCP errors omit the
	// observation). Buckets span the per-leg budgets (submit 30s,
	// status poll up to 90s wall-clock, serve 30s) — the wider 120s
	// upper bucket captures the cold-cluster Kaniko build edge case.
	DeployProbeLatencySeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "instant_deploy_probe_latency_seconds",
		Help:    "Hourly deploy prober per-leg wall-clock latency. Buckets cover the per-leg budgets up to the 120s cold-cluster Kaniko ceiling.",
		Buckets: []float64{0.5, 1, 5, 10, 30, 60, 90, 120},
	}, []string{"leg"})
)

// ReadyzCheckStatus updates the gauge for one check on this service.
// Stamped with service="instant-worker" inside this helper so a caller
// can't accidentally publish under the wrong label.
func ReadyzCheckStatus(check string, value float64) {
	readyzCheckStatusGauge.WithLabelValues("instant-worker", check).Set(value)
}
