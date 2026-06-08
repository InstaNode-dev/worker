package jobs

// orphan_db_sweep.go — AUDIT-ONLY (dry-run) orphan-customer-DB /
// orphan-redis-namespace sweep.
//
// WHY THIS EXISTS (and how it differs from orphan_sweep_reconciler.go PASS 4)
//
// The orphan_sweep_reconciler's PASS 4 already lists instant-customer-*
// namespaces and *deletes* the ones with no live resources row. That reclaim
// path is correct, but it is a side-effect of a much larger reconciler and it
// acts immediately. After the truehomie-2026-06-03 incident — an active Pro
// customer's database + role were DROPPED by an unidentified, unaudited path —
// we want a SEPARATE, conservative, OBSERVABILITY-FIRST surface for the ~25
// orphaned customer DB / redis namespace drain-backlog:
//
//   - It runs in AUDIT-ONLY (detection / dry-run) mode by DEFAULT. It
//     IDENTIFIES orphan candidates and LOGS them (masked) + emits a
//     candidate-count metric. It DROPS NOTHING.
//   - It is gated behind a MASTER feature flag (ORPHAN_DB_SWEEP_ENABLED),
//     default OFF / fail-closed. When the flag is off the Work method no-ops
//     after a single DEBUG line — no namespace List, no DB read, no metric.
//   - The actual deprovision sits behind a SECOND, destructive flag
//     (ORPHAN_DB_SWEEP_DESTRUCTIVE_ENABLED), also default OFF, which is
//     meaningless unless the master flag is also on. When (and only when) BOTH
//     are on does a confirmed orphan route through the AUDITED provisioner
//     DeprovisionResource chokepoint — the SAME path the TTL reaper
//     (expire.go) uses. There is NO manual / raw DROP anywhere in this file.
//     For THIS PR the destructive path is intentionally UNREACHABLE-BY-DEFAULT:
//     we ship audit-only, review the dry-run candidate list, and only later (a
//     deliberate operator action) consider lighting the destructive flag.
//
// WHAT IS AN ORPHAN CANDIDATE
//
// Every db/redis/mongo/queue resource gets a dedicated instant-customer-<token>
// namespace. A namespace is a candidate iff:
//
//   1. its <token> has NO non-terminal (pending/active/paused/suspended)
//      resources row — nothing the platform still considers a live resource;
//      AND
//   2. it is older than the provisioning grace window (so a namespace created
//      mid-provision, before its 'pending' INSERT is visible, is NEVER a
//      candidate — fail-safe: when in doubt, skip + log).
//
// FAIL-SAFE / FAIL-OPEN POSTURE
//
//   - A nil k8s lister disables the sweep with a WARN (no candidates).
//   - A namespace List failure or a live-token DB-read failure degrades to one
//     WARN and a zero-candidate result — NEVER an empty-set delete decision
//     (that bug would "orphan" every live namespace). The job never returns an
//     error from the detection path.
//   - When the destructive flag is on but a candidate looks even slightly live
//     (its token reappears in the live set at re-check time), we SKIP + log
//     rather than deprovision. When in doubt, never drop.
//
// The `kind` metric label distinguishes a generic customer_namespace orphan
// from a redis_namespace orphan: if the orphan token's most recent terminal
// resources row was resource_type='redis', we label it redis_namespace so the
// redis-pod drain backlog is independently visible.

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	commonv1 "instant.dev/proto/common/v1"
	"instant.dev/worker/internal/logsafe"
	"instant.dev/worker/internal/metrics"
)

// orphanDBSweepInterval is how often the audit-only sweep runs. 1h is
// deliberately slower than orphan_sweep_reconciler's 15min: this is a
// drain-backlog observability surface, not a money-stopping reconciler, so an
// hourly snapshot of "how big is the orphan backlog" is plenty and keeps the
// cluster-wide namespace List off the hot path.
const orphanDBSweepInterval = 1 * time.Hour

// orphanDBSweepKindCustomerNS / orphanDBSweepKindRedisNS are the bounded
// `kind` label values for the candidate metrics. customer_namespace is the
// generic orphan (no row, or a non-redis terminal row); redis_namespace is an
// orphan whose token's most recent terminal resources row was a redis pod —
// surfaced separately so the redis-pod drain backlog is independently visible.
const (
	orphanDBSweepKindCustomerNS = "customer_namespace"
	orphanDBSweepKindRedisNS    = "redis_namespace"
)

// orphanDBSweepResourceTypeRedis is the resources.resource_type literal that
// marks a token's backing infra as a dedicated Redis pod. Named so the kind
// classification can never drift from an inline string (feedback: named
// constants, not hardcoded strings).
const orphanDBSweepResourceTypeRedis = "redis"

// OrphanDBSweepArgs is the periodic-job payload — empty, every run is a full
// audit-only sweep.
type OrphanDBSweepArgs struct{}

// Kind implements river.JobArgs.
func (OrphanDBSweepArgs) Kind() string { return "orphan_db_sweep" }

// OrphanDBSweepConfig carries the two flag gates + the grace window. Both flags
// default to the zero value (false) so a caller that forgets to set them gets
// the safe, fail-closed posture: the sweep is OFF.
type OrphanDBSweepConfig struct {
	// Enabled is the MASTER flag (ORPHAN_DB_SWEEP_ENABLED). false (default) →
	// Work no-ops immediately.
	Enabled bool
	// DestructiveEnabled is the SECOND, destructive flag
	// (ORPHAN_DB_SWEEP_DESTRUCTIVE_ENABLED). Meaningless unless Enabled is also
	// true. When both are true a confirmed orphan is routed through the AUDITED
	// provisioner DeprovisionResource chokepoint (never a raw DROP). Default
	// false → audit-only (detection + log + metric, no drop).
	DestructiveEnabled bool
	// Grace is the minimum namespace age before a no-live-row namespace is even
	// considered a candidate — protects mid-provision namespaces. Zero falls
	// back to orphanNoDBRowGrace (shared with orphan_sweep_reconciler).
	Grace time.Duration
}

// effectiveGrace returns the configured grace window, defaulting to the shared
// orphanNoDBRowGrace when unset.
func (c OrphanDBSweepConfig) effectiveGrace() time.Duration {
	if c.Grace <= 0 {
		return orphanNoDBRowGrace
	}
	return c.Grace
}

// OrphanDBSweepWorker is the River worker. It reuses the SAME seams the rest of
// the worker already wires:
//   - K8sNamespaceLister: the cluster-wide customer-namespace List + age check
//     (orphan_sweep_reconciler.go). nil → sweep disabled with a WARN.
//   - ResourceDeprovisioner: the AUDITED teardown chokepoint (expire.go), used
//     ONLY when BOTH flags are on. nil → destructive path can never run (it
//     also requires both flags, so this is belt-and-suspenders).
type OrphanDBSweepWorker struct {
	river.WorkerDefaults[OrphanDBSweepArgs]
	db          *sql.DB
	k8s         K8sNamespaceLister    // nil = sweep disabled (WARN)
	provisioner ResourceDeprovisioner // nil = destructive path unreachable
	cfg         OrphanDBSweepConfig
}

// NewOrphanDBSweepWorker constructs the audit-only sweep. k8s may be nil (the
// sweep then WARN-skips each tick). provisioner may be nil — the destructive
// path is then permanently unreachable regardless of the flags (which is the
// shipped default anyway). cfg carries the two flag gates.
func NewOrphanDBSweepWorker(db *sql.DB, k8s K8sNamespaceLister, provisioner ResourceDeprovisioner, cfg OrphanDBSweepConfig) *OrphanDBSweepWorker {
	return &OrphanDBSweepWorker{
		db:          db,
		k8s:         k8s,
		provisioner: provisioner,
		cfg:         cfg,
	}
}

// orphanDBCandidate is one detected orphan namespace carried from detection to
// the (audit-only) log + (flag-gated, audited) deprovision.
type orphanDBCandidate struct {
	namespace string
	token     string
	kind      string // orphanDBSweepKindCustomerNS | orphanDBSweepKindRedisNS
}

// Work runs one full audit-only sweep.
//
// Returns nil unconditionally on the detection path: a River retry would just
// re-run the (idempotent, read-only) scan faster than the cadence with no
// benefit — the metric + structured log already capture the candidate list for
// the operator.
func (w *OrphanDBSweepWorker) Work(ctx context.Context, job *river.Job[OrphanDBSweepArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.orphan_db_sweep")
	defer span.End()

	// MASTER flag — fail-closed. When off the whole layer is inert: no List, no
	// DB read, no metric, just one DEBUG line. A single env flip turns it on.
	if !w.cfg.Enabled {
		slog.Debug("jobs.orphan_db_sweep.disabled",
			"reason", "ORPHAN_DB_SWEEP_ENABLED unset", "job_id", job.ID)
		return nil
	}

	if w.k8s == nil {
		slog.Warn("jobs.orphan_db_sweep.skipped_no_k8s",
			"detail", "no K8sNamespaceLister wired (CI / docker-compose); audit-only sweep cannot list customer namespaces")
		return nil
	}

	start := time.Now()

	candidates := w.detectCandidates(ctx)

	// Per-kind counts for the gauge (current backlog) — Set even when zero so the
	// tile falls back to 0 once the backlog drains. Always publish both kinds.
	perKind := map[string]int{
		orphanDBSweepKindCustomerNS: 0,
		orphanDBSweepKindRedisNS:    0,
	}

	for _, c := range candidates {
		perKind[c.kind]++
		// AUDIT: count + log every candidate, masked. This is the dry-run
		// surface — what WOULD be reclaimed if the destructive flag were on.
		metrics.OrphanDBSweepCandidatesTotal.WithLabelValues(c.kind).Inc()
		slog.Info("jobs.orphan_db_sweep.candidate",
			"namespace", c.namespace,
			"token", logsafe.Token(c.token),
			"kind", c.kind,
			"mode", w.modeLabel(),
			"action", "AUDIT-ONLY: candidate logged + counted; NOT dropped",
		)

		// DESTRUCTIVE path — UNREACHABLE BY DEFAULT. Requires BOTH flags AND a
		// wired audited provisioner. Even here we re-confirm + route through the
		// audited chokepoint only; never a raw DROP. Shipped default: both flags
		// off → this branch never executes.
		if w.destructiveArmed() {
			w.maybeDeprovision(ctx, c, job.ID)
		}
	}

	for kind, n := range perKind {
		metrics.OrphanDBSweepCandidatesCurrent.WithLabelValues(kind).Set(float64(n))
	}

	// Idle-tick discipline (worker CLAUDE.md #1): an all-clear sweep is DEBUG;
	// any candidate is operational signal → INFO.
	level := slog.LevelInfo
	if len(candidates) == 0 {
		level = slog.LevelDebug
	}
	slog.Log(ctx, level, "jobs.orphan_db_sweep.completed",
		"candidates_total", len(candidates),
		"candidates_customer_namespace", perKind[orphanDBSweepKindCustomerNS],
		"candidates_redis_namespace", perKind[orphanDBSweepKindRedisNS],
		"mode", w.modeLabel(),
		"duration_ms", time.Since(start).Milliseconds(),
		"job_id", job.ID,
	)
	return nil
}

// destructiveArmed reports whether the audited deprovision path is allowed to
// run: BOTH flags on AND a provisioner wired. Defense-in-depth — any one being
// false keeps the sweep audit-only.
func (w *OrphanDBSweepWorker) destructiveArmed() bool {
	return w.cfg.Enabled && w.cfg.DestructiveEnabled && w.provisioner != nil
}

// modeLabel is the structured-log mode string: "audit-only" (the shipped
// default) or "destructive-audited". Never "raw-drop" — that mode does not
// exist by design (truehomie-2026-06-03 safety).
func (w *OrphanDBSweepWorker) modeLabel() string {
	if w.destructiveArmed() {
		return "destructive-audited"
	}
	return "audit-only"
}

// detectCandidates is the read-only core: list every instant-customer-*
// namespace, drop the ones still backed by a live resource row, drop the ones
// inside the provisioning grace window, and classify the rest. Fail-open: any
// List / DB-read failure returns an empty slice (NEVER an empty-set that a
// destructive caller could misread as "delete everything").
func (w *OrphanDBSweepWorker) detectCandidates(ctx context.Context) []orphanDBCandidate {
	namespaces, err := w.k8s.ListCustomerNamespaces(ctx)
	if err != nil {
		slog.Warn("jobs.orphan_db_sweep.namespace_list_failed",
			"error", err.Error(),
			"detail", "audit-only sweep skipped this tick; check instant-worker RBAC for cluster-scoped namespaces list")
		return nil
	}
	if len(namespaces) == 0 {
		return nil
	}

	liveTokens, err := w.fetchLiveResourceTokens(ctx)
	if err != nil {
		// A DB blip MUST NOT yield an empty live-set that a destructive caller
		// could read as "every namespace is an orphan". Skip the whole tick.
		slog.Warn("jobs.orphan_db_sweep.live_tokens_failed",
			"error", err.Error(),
			"detail", "audit-only sweep skipped this tick (live-token query failed); refusing to treat all namespaces as orphans")
		return nil
	}

	// Most-recent terminal resource_type per token — drives the redis-vs-generic
	// kind classification. A DB failure here is non-fatal: we fall back to the
	// generic kind so detection still runs (the classification is a nice-to-have
	// label, not a safety gate).
	terminalTypes, ttErr := w.fetchTerminalResourceTypes(ctx)
	if ttErr != nil {
		slog.Warn("jobs.orphan_db_sweep.terminal_types_failed",
			"error", ttErr.Error(),
			"detail", "kind classification degraded to customer_namespace for all candidates this tick")
		terminalTypes = map[string]string{}
	}

	grace := w.cfg.effectiveGrace()
	var candidates []orphanDBCandidate
	for _, ns := range namespaces {
		token := ns[len(customerNamespacePrefix):]
		if token == "" {
			continue
		}
		if liveTokens[token] {
			continue // a live (pending/active/paused/suspended) resource backs it — leave it
		}
		// Provisioning grace — never flag a namespace younger than the grace
		// window (it may be mid-provision before its 'pending' INSERT is
		// visible). Fail-safe: on an age-lookup error, skip this namespace.
		age, ageErr := w.k8s.GetNamespaceAge(ctx, ns)
		if ageErr != nil {
			slog.Warn("jobs.orphan_db_sweep.namespace_age_lookup_failed",
				"namespace", ns, "error", ageErr.Error(),
				"detail", "skipping candidate evaluation this tick; will retry next interval")
			continue
		}
		if age < grace {
			slog.Debug("jobs.orphan_db_sweep.within_grace",
				"namespace", ns, "age", age.String(), "grace", grace.String(),
				"detail", "namespace younger than provisioning grace — not a candidate (may be mid-provision)")
			continue
		}
		candidates = append(candidates, orphanDBCandidate{
			namespace: ns,
			token:     token,
			kind:      classifyOrphanDBKind(terminalTypes[token]),
		})
	}
	return candidates
}

// classifyOrphanDBKind maps a token's most-recent terminal resource_type to the
// metric kind label. A redis terminal row → redis_namespace (the redis-pod
// drain backlog), anything else (incl. "" — no terminal row found) →
// customer_namespace (the generic orphan).
func classifyOrphanDBKind(terminalResourceType string) string {
	if terminalResourceType == orphanDBSweepResourceTypeRedis {
		return orphanDBSweepKindRedisNS
	}
	return orphanDBSweepKindCustomerNS
}

// fetchLiveResourceTokens returns the set of tokens that still have a
// non-terminal (pending/active/paused/suspended) resources row — exactly the
// tokens the sweep must NOT treat as orphans. Mirrors orphan_sweep_reconciler's
// fetchLiveResourceTokens (including 'pending' for the two-phase-provision
// window) so the two surfaces never disagree on what "live" means.
func (w *OrphanDBSweepWorker) fetchLiveResourceTokens(ctx context.Context) (map[string]bool, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT DISTINCT token::text
		  FROM resources
		 WHERE status IN ('pending', 'active', 'paused', 'suspended')
		   AND token IS NOT NULL
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]bool)
	for rows.Next() {
		var token string
		if scanErr := rows.Scan(&token); scanErr != nil {
			return nil, scanErr
		}
		if token != "" {
			out[token] = true
		}
	}
	return out, rows.Err()
}

// fetchTerminalResourceTypes returns, per token, the resource_type of its most
// recent terminal (deleted/expired) resources row. Used ONLY to classify the
// metric kind (redis vs generic) for an orphan namespace — it is never a safety
// gate. A token with no terminal row at all is simply absent from the map.
func (w *OrphanDBSweepWorker) fetchTerminalResourceTypes(ctx context.Context) (map[string]string, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT DISTINCT ON (token::text) token::text, resource_type
		  FROM resources
		 WHERE status IN ('deleted', 'expired')
		   AND token IS NOT NULL
		 ORDER BY token::text, created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]string)
	for rows.Next() {
		var token, resType string
		if scanErr := rows.Scan(&token, &resType); scanErr != nil {
			return nil, scanErr
		}
		if token != "" {
			out[token] = resType
		}
	}
	return out, rows.Err()
}

// maybeDeprovision is the DESTRUCTIVE branch — reachable ONLY when destructiveArmed()
// is true (BOTH flags on AND a wired audited provisioner). It NEVER issues a raw
// DROP: it re-confirms the token is still not live, then routes the teardown
// through the AUDITED provisioner DeprovisionResource chokepoint — the SAME path
// the TTL reaper (expire.go) uses. On ANY doubt (token reappeared as live, type
// unmappable, deprovision error) it SKIPS + logs rather than dropping.
//
// Shipped default: both flags off → this method is never called.
func (w *OrphanDBSweepWorker) maybeDeprovision(ctx context.Context, c orphanDBCandidate, jobID int64) {
	// Re-confirm under a fresh live-token read: if the token reappeared as a
	// live resource between detection and now (a just-paid upgrade / a restore),
	// DO NOT touch it. When in doubt, skip + log.
	liveTokens, err := w.fetchLiveResourceTokens(ctx)
	if err != nil {
		slog.Warn("jobs.orphan_db_sweep.destructive_reconfirm_failed",
			"namespace", c.namespace, "token", logsafe.Token(c.token),
			"error", err.Error(),
			"detail", "skipping deprovision this tick — refusing to drop without a clean re-confirm")
		return
	}
	if liveTokens[c.token] {
		slog.Info("jobs.orphan_db_sweep.destructive_skipped_now_live",
			"namespace", c.namespace, "token", logsafe.Token(c.token),
			"detail", "token reappeared as a live resource at re-confirm — NOT deprovisioning (fail-safe)")
		return
	}

	// Map the kind back to the proto resource type the audited chokepoint
	// expects. Only redis is positively mapped here (the only kind this sweep
	// classifies); a generic customer_namespace orphan has no proven backing
	// type, so we do NOT guess a DROP — skip + log (fail-safe). The TTL reaper
	// remains the per-typed-row authority for those.
	resType := orphanDBSweepKindToProto(c.kind)
	if resType == commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED {
		slog.Info("jobs.orphan_db_sweep.destructive_skipped_unmapped_kind",
			"namespace", c.namespace, "token", logsafe.Token(c.token), "kind", c.kind,
			"detail", "no proven backing resource type for this orphan kind — NOT issuing a deprovision (fail-safe); operator review")
		return
	}

	slog.Info("jobs.orphan_db_sweep.destructive_proposed",
		"namespace", c.namespace, "token", logsafe.Token(c.token), "kind", c.kind,
		"action", "routing through AUDITED provisioner DeprovisionResource (no raw DROP)",
		"job_id", jobID,
	)
	if derr := w.provisioner.DeprovisionResource(ctx, c.token, "", resType); derr != nil {
		slog.Error("jobs.orphan_db_sweep.destructive_deprovision_failed",
			"namespace", c.namespace, "token", logsafe.Token(c.token), "kind", c.kind,
			"error", derr,
			"detail", "audited deprovision failed; namespace left for the next tick / TTL reaper",
			"job_id", jobID,
		)
		return
	}
	slog.Info("jobs.orphan_db_sweep.destructive_deprovisioned",
		"namespace", c.namespace, "token", logsafe.Token(c.token), "kind", c.kind,
		"detail", "orphan reclaimed via the audited provisioner chokepoint",
		"job_id", jobID,
	)
}

// orphanDBSweepKindToProto maps a candidate kind to the proto resource type the
// audited DeprovisionResource chokepoint accepts. ONLY redis_namespace is
// positively mapped — a generic customer_namespace orphan has no proven backing
// type, so it maps to UNSPECIFIED and the destructive path skips it (fail-safe).
func orphanDBSweepKindToProto(kind string) commonv1.ResourceType {
	if kind == orphanDBSweepKindRedisNS {
		return commonv1.ResourceType_RESOURCE_TYPE_REDIS
	}
	return commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED
}
