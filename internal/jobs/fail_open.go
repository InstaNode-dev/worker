package jobs

// fail_open.go — observability helper for the worker's fail-open sites.
//
// # Why this exists (CIRCUIT-RETRY-AUDIT-2026-05-20, P2 worker)
//
// The worker has many call sites that intentionally fail-OPEN — Redis errors,
// DB brownouts, Brevo suppression-lookup failures, GeoIP misses, k8s lookups
// for soft cleanup. Each of these is a defensible local decision ("a
// transient Redis blip should not pin the queue" / "a missing GeoIP file
// must not block provisioning" / "a bounce lookup error should not silently
// halt sender reputation").
//
// But each was silent in aggregate: a single warning slog line per event,
// nothing the SRE could ALERT on. A widespread brownout — Redis stalls for
// 90 seconds, every fail-open site fires, the worker keeps marching, and
// the operator only finds out when the downstream effect (mis-emailed
// suppressed users, mis-paced rate limiting, mis-rendered geoip-gated
// pricing) is reported by a customer.
//
// This helper gives every fail-open site:
//
//  1. A Prometheus counter (`instant_worker_fail_open_total`) labelled by
//     `site` + `reason` so the SRE can alert on a per-site rate.
//
//  2. A structured slog line at WARN level with `fail_open=true` so log
//     pipelines can filter the population of fail-open events directly.
//
// IMPORTANT: this helper changes ONLY observability. It does NOT change
// fail-mode semantics. Call sites stay fail-open; the caller still
// proceeds. The audit explicitly flagged "don't change fail-mode semantics
// unless trivially safe" — we honor that.
//
// # Usage
//
//	jobs.RecordFailOpen(
//	    "event_email_forwarder.bounce_suppression",
//	    "db_error",
//	    err,
//	    "audit_id", row.ID,
//	    "kind", row.Kind,
//	)
//
// The first two arguments are bounded-cardinality labels (the call site
// and the failure-reason classification). They MUST NOT contain team_id,
// email, or any other unbounded value — the metric explodes if so. The
// `err` is logged but not labelled. `extra...` carries the per-row context
// (team_id, audit_id, error) that the metric does not see.
//
// # Naming the site
//
// Convention: `<job_name>.<call_path>` — e.g. `billing_reconciler.upgrade_audit_insert`,
// `entitlement_reconciler.heartbeat_lookup`, `event_email_forwarder.bounce_suppression`.
// Stable names let the alert rule reference a specific code path without
// scanning the log stream.

import (
	"log/slog"

	"instant.dev/worker/internal/metrics"
)

// RecordFailOpen logs and counts a single fail-open event.
//
// `site` and `reason` MUST be low-cardinality (bounded by code paths, not
// by row data). `err` is the underlying error that the caller chose to
// fail-open on. `extra` carries optional structured-log key/value pairs
// (alternating string keys + values, matching slog.Warn's variadic shape)
// for high-cardinality context like team_id, audit_id, etc.
//
// The slog line always emits `fail_open=true` and `site` / `reason`
// fields so log queries can filter the population without parsing the
// message body. Level is WARN — fail-open is not an error (the call
// succeeded in the soft sense) but is also not normal flow.
func RecordFailOpen(site, reason string, err error, extra ...any) {
	metrics.FailOpenTotal.WithLabelValues(site, reason).Inc()
	args := make([]any, 0, 8+len(extra))
	args = append(args,
		"site", site,
		"reason", reason,
		"fail_open", true,
	)
	if err != nil {
		args = append(args, "error", err)
	}
	args = append(args, extra...)
	slog.Warn("jobs.fail_open", args...)
}
