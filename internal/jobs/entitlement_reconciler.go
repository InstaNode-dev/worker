package jobs

// entitlement_reconciler.go — periodic sweep that fixes "upgrade drift".
//
// Background: when a customer upgrades their plan, the api updates
// resources.tier (and teams.plan_tier), but the resource's *actual* Postgres
// connection cap or Redis maxmemory is NOT re-applied to the infrastructure —
// the higher entitlement is recorded but never enforced. This job detects that
// drift and fixes it.
//
// Postgres sweep:
//
//   * SELECT active, non-expired postgres resources joined to their team's
//     plan_tier, projecting the row's last-applied connection cap
//     (resources.applied_conn_limit; migration 047 — NULL = never re-graded,
//     -1 = unlimited).
//   * Skip ephemeral tiers (anonymous / free) — those are never re-graded up.
//   * Compute the entitled cap for the team's plan_tier from the shared plans
//     registry (PlanRegistry.ConnectionsLimit(tier, "postgres")) — the same
//     source the api uses.
//   * A row has DRIFTED when applied_conn_limit IS NULL, OR
//     applied_conn_limit != entitled.
//   * For each drifted row, call the provisioner gRPC RegradeResource RPC.
//       - applied=true  → UPDATE resources SET applied_conn_limit = <resp value>.
//       - applied=false (provisioner skip) or a gRPC error → log + leave the
//         row for the next sweep.
//
// Redis maxmemory sweep (A4 backfill):
//
//   * SELECT all active, non-expired redis resources joined to their team's
//     plan_tier. Unlike the original implementation, we do NOT filter on
//     provider_resource_id — 100% of prod Redis rows have provider_resource_id=NULL,
//     so the old LIKE 'instant-customer-%' predicate matched 0 rows every tick.
//   * The k8s namespace is derived from the token: instant-customer-<token>.
//     This is deterministic and does not require a non-NULL provider_resource_id.
//     If prid already has the "instant-customer-" prefix (legacy rows with prid set),
//     it is used as-is; otherwise the bare token is passed to the provisioner which
//     constructs the namespace itself.
//   * Skip ephemeral tiers (anonymous / free) — unchanged from Postgres.
//   * Resolve the entitled maxmemory cap from plans.Registry.StorageLimitMB(tier, "redis").
//     Values of -1 (unlimited) are passed as 0 to the provisioner, which sets
//     maxmemory=0 (Redis "no cap") — safe for team tiers with dedicated infra.
//   * Call provisioner gRPC RegradeResource with RESOURCE_TYPE_REDIS.
//     The provisioner does CONFIG GET maxmemory first; if already correct it
//     returns {applied:false, skip_reason:"already correct"} so this is idempotent
//     and safe to re-run every sweep without customer-visible side effects.
//     Shared/local-backend resources (no k8s pod) return {applied:false,
//     skip_reason:"backend does not support redis regrade"} — also safe.
//   * Fail-soft: one bad pod must not abort the sweep. Errors are logged and
//     the row is left for the next tick.
//   * No DB column is used to track applied maxmemory — the provisioner's
//     idempotent CONFIG GET / CONFIG SET is the convergence signal. This avoids
//     a migration on the api/ repo while keeping the worker stateless for Redis.
//
// Fail-open / resilience: one bad resource must NOT abort the sweep — every
// per-resource step is wrapped. A SELECT failure returns an error so River
// retries the whole tick.

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	commonv1 "instant.dev/proto/common/v1"
	"instant.dev/worker/internal/logsafe"
	"instant.dev/worker/internal/metrics"
)

// EntitlementReconcilerArgs is the River job payload — no fields, sweep job.
type EntitlementReconcilerArgs struct{}

// Kind is the River worker key.
func (EntitlementReconcilerArgs) Kind() string { return "entitlement_reconciler" }

// defaultEntitlementReconcileInterval is the fallback cadence when the
// ENTITLEMENT_RECONCILE_INTERVAL env var is unset or unparseable. 5 minutes
// matches the custom-domain reconciler: drift only ever appears at plan-change
// time, so a 5-minute lag between upgrade and cap-applied is invisible to a
// customer while keeping the platform DB off the hot path.
const defaultEntitlementReconcileInterval = 5 * time.Minute

// entitlementReconcilerBatchLimit caps the per-tick fan-out. A backlog larger
// than this drains across consecutive ticks — once a row's applied_conn_limit
// is corrected it no longer matches the drift predicate, so the next sweep
// naturally moves on to the still-drifted rows.
const entitlementReconcilerBatchLimit = 200

// entitlementReconcilerRegradeTimeout is the per-resource gRPC budget.
const entitlementReconcilerRegradeTimeout = 30 * time.Second

// entitlementDriftCorrectedSignal is the stable structured-log event name
// emitted once per infra-entitlement drift correction (F6). It is distinct
// from the per-resource `*.regraded` / `*.maxmemory_applied` INFO lines: those
// are operational detail, this WARN-level line is the alertable signal a
// monitor watches for a rising correction rate. Held as a const so the F6
// regression test pins the exact string the NR alert query depends on.
const entitlementDriftCorrectedSignal = "jobs.entitlement_reconciler.drift_corrected"

// entitlementResourceType* are the resource_type label values carried by the
// drift-corrected signal and its Prometheus counter. Named constants (not
// scattered string literals) per CLAUDE.md conventions.
const (
	entitlementResourceTypePostgres = "postgres"
	entitlementResourceTypeRedis    = "redis"
)

// entitlementEphemeralTiers are the tiers that are never re-graded up — the
// anonymous (24h TTL) and legacy free tiers are ephemeral, so applying a paid
// connection cap to them is meaningless. Drift detection skips these rows.
var entitlementEphemeralTiers = map[string]bool{
	"anonymous": true,
	"free":      true,
}

// EntitlementReconcileInterval resolves the periodic dispatch cadence from the
// ENTITLEMENT_RECONCILE_INTERVAL env var (a Go duration string, e.g. "5m" or
// "15s"). It falls back to defaultEntitlementReconcileInterval when the var is
// unset, empty, unparseable, or non-positive — and logs a WARN in the bad-value
// case so a typo in the k8s ConfigMap is visible. Exposing the env var lets
// tests run the sweep fast.
func EntitlementReconcileInterval() time.Duration {
	raw := os.Getenv("ENTITLEMENT_RECONCILE_INTERVAL")
	if raw == "" {
		return defaultEntitlementReconcileInterval
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		slog.Warn("jobs.entitlement_reconciler.bad_interval",
			"value", raw,
			"error", err,
			"fallback", defaultEntitlementReconcileInterval.String(),
		)
		return defaultEntitlementReconcileInterval
	}
	return d
}

// EntitlementReconcileTeamFilter resolves the optional team-scoping allowlist
// from the ENTITLEMENT_RECONCILE_TEAM env var — a comma-separated list of team
// UUIDs. When set, the sweep is restricted to those teams; when empty (the
// prod default) the sweep covers every team.
//
// Two uses: (1) contained integration testing — scope the sweep to a single
// test team so it cannot touch real customers; (2) prod canary — roll the
// feature out to a handful of teams before fleet-wide enablement.
func EntitlementReconcileTeamFilter() string {
	return strings.TrimSpace(os.Getenv("ENTITLEMENT_RECONCILE_TEAM"))
}

// entitlementRegrader is the narrow provisioner surface the reconciler needs.
// *provisioner.Client satisfies it; a test supplies a stub. Keeping the seam
// narrow means the job's unit tests don't dial a real gRPC server.
type entitlementRegrader interface {
	RegradeResource(ctx context.Context, token, providerResourceID string, resType commonv1.ResourceType, tier, requestID string) (regradeOutcome, error)
}

// regradeOutcome mirrors provisioner.RegradeResult — re-declared here so the
// jobs package's interface doesn't import a concrete struct from the
// provisioner package (the adapter below bridges the two).
type regradeOutcome struct {
	Applied          bool
	AppliedConnLimit int32
	SkipReason       string
}

// EntitlementReconcilerWorker scans for postgres resources whose applied
// connection cap has drifted from their team's plan entitlement and re-grades
// each one via the provisioner.
type EntitlementReconcilerWorker struct {
	river.WorkerDefaults[EntitlementReconcilerArgs]
	db       *sql.DB
	plans    PlanRegistry         // ConnectionsLimit(tier, "postgres") — entitled cap source
	regrader entitlementRegrader  // optional — nil disables re-grading (logs WARN each tick)
}

// NewEntitlementReconcilerWorker constructs the worker.
//
// regrader may be nil — when the provisioner isn't configured (PROVISIONER_ADDR
// unset) the worker logs a WARN each tick and short-circuits, matching the
// fail-open posture of the storage-bytes / team-deletion workers.
func NewEntitlementReconcilerWorker(db *sql.DB, plans PlanRegistry, regrader entitlementRegrader) *EntitlementReconcilerWorker {
	return &EntitlementReconcilerWorker{db: db, plans: plans, regrader: regrader}
}

// entitlementRegraderAdapter bridges *provisioner.Client (which returns
// provisioner.RegradeResult) onto the jobs package's entitlementRegrader
// interface (which returns the local regradeOutcome). Declared in workers.go
// alongside the rest of the StartWorkers wiring.

// entitlementCandidate is the projection one sweep row yields.
//
// providerResourceID is sql.NullString because resources.provider_resource_id
// (migration 002) is a nullable TEXT column with no default — many legacy and
// pool-claimed rows have it NULL. Scanning a NULL into a plain string aborts
// the row's Scan, which previously dropped every NULL-prid row from the sweep
// (the modal case in prod). An empty providerResourceID is safe to pass to the
// provisioner: K8sBackend.Regrade falls back to k8sNsPrefix+token when it is "".
type entitlementCandidate struct {
	id                 uuid.UUID
	token              string
	providerResourceID sql.NullString
	resourceTier       string         // resources.tier — informational only
	appliedConnLimit   sql.NullInt64  // NULL = never re-graded (migration 047)
	planTier           string         // teams.plan_tier — the entitled tier
}

// shouldRegrade is the pure drift-detection decision. It is intentionally
// side-effect-free and exported-for-test-adjacent so the unit test can drive
// it against the live plans registry. Returns:
//
//	drift    — true when the resource needs a re-grade.
//	entitled — the entitled connection cap for resourceTier (the value the
//	           provisioner should apply). Only meaningful when drift is true.
//
// MR-P1-21 / T8 P1-6 (BugBash 2026-05-20): caps are resolved from
// resourceTier (the per-row SNAPSHOT), not team.plan_tier. The pre-fix
// code joined teams.plan_tier and resolved caps from THAT — which silently
// degraded a downgraded customer's existing paid resources, contradicting
// the documented "keep their tier" courtesy:
//
//	team upgrades free → pro → resource.tier=pro, applied_conn_limit=20
//	team downgrades pro → hobby → team.plan_tier=hobby BUT resource.tier
//	  stays pro (the documented snapshot courtesy)
//	pre-fix reconciler: entitled = ConnectionsLimit("hobby") = 8
//	→ drift detected (20 != 8) → ALTER ROLE … CONNECTION LIMIT 8
//	→ paid customer's pro infra cap silently cut to hobby.
//
// The snapshot IS the entitlement-of-record for an individual resource —
// that is the whole point of the snapshot/downgrade-asymmetry design.
//
// Ephemeral tiers (anonymous/free) always return drift=false: they are
// never re-graded up regardless of their applied_conn_limit.
func shouldRegrade(plans PlanRegistry, resourceTier string, appliedConnLimit sql.NullInt64) (drift bool, entitled int) {
	if entitlementEphemeralTiers[resourceTier] {
		return false, 0
	}
	entitled = plans.ConnectionsLimit(resourceTier, "postgres")
	if !appliedConnLimit.Valid {
		// NULL — never re-graded since migration 047 added the column.
		return true, entitled
	}
	if appliedConnLimit.Int64 != int64(entitled) {
		return true, entitled
	}
	return false, entitled
}

// redisK8sNsPrefix is the namespace prefix for k8s-backed dedicated Redis pods.
// Mirrors the const in provisioner/internal/backend/redis/k8s.go — both must stay
// in sync. Declared here so the worker can derive the namespace from a bare token
// without importing the provisioner package.
const redisK8sNsPrefix = "instant-customer-"

// redisEntitlementCandidate is the projection for one Redis sweep row.
// Unlike the Postgres candidate, there is no applied_maxmemory_mb DB column
// (no migration needed — the reconciler is stateless for Redis; the
// provisioner's idempotent CONFIG GET / CONFIG SET is the convergence signal).
//
// providerResourceID is sql.NullString because resources.provider_resource_id
// is a nullable TEXT column — prod shows 100% NULL for Redis resources. The
// sweep identifies pods by token (not by prid) and derives the k8s namespace
// as instant-customer-<token>. The prid is kept for logging / future use.
type redisEntitlementCandidate struct {
	id                 uuid.UUID
	token              string
	providerResourceID sql.NullString // nullable — prod rows are NULL; namespace derived from token
	resourceTier       string         // resources.tier — informational
	planTier           string         // teams.plan_tier — the entitled tier
}

// mongoEntitlementCandidate is the projection for one MongoDB sweep row.
//
// MR-P1-22 / T8 P1-9 (BugBash 2026-05-20): the reconciler previously only
// re-graded Postgres + Redis; a hobby → pro upgrade silently under-delivered
// Mongo (the Mongo user's storage quota / connection cap stayed at hobby
// values until the resource was re-provisioned). This sweep is the third
// arm. It iterates every active mongo resource and calls RegradeResource —
// today the provisioner's RegradeResource returns `applied=false, skip_reason
// ="unsupported resource type for regrade"` for MONGODB, so the sweep is a
// loud no-op (one skipped count per tick) and a clear hook for when the
// Mongo arm of provisioner.regradeMongo is implemented. With the hook in
// place the surface checklist (CLAUDE.md rule 22 + 18) is satisfied — a
// future provisioner.regradeMongo implementation does not also have to add
// the worker sweep.
type mongoEntitlementCandidate struct {
	id                 uuid.UUID
	token              string
	providerResourceID sql.NullString
	resourceTier       string
	planTier           string
}

// Work executes one sweep: Postgres connection-cap regrade + Redis maxmemory
// backfill (A4). Both paths are fail-soft per resource; a SELECT failure
// returns an error so River retries the full tick.
func (w *EntitlementReconcilerWorker) Work(ctx context.Context, job *river.Job[EntitlementReconcilerArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.entitlement_reconciler")
	defer span.End()

	start := time.Now()

	if w.regrader == nil {
		slog.Warn("jobs.entitlement_reconciler.skipped",
			"reason", "no provisioner client configured (PROVISIONER_ADDR unset)",
		)
		return nil
	}

	// teamFilter ($2) scopes the sweep: empty = every team (prod default);
	// a comma-separated UUID list = only those teams. The `$2 = '' OR …`
	// predicate is a parameterised no-op when the filter is empty.
	teamFilter := EntitlementReconcileTeamFilter()
	if teamFilter != "" {
		slog.Info("jobs.entitlement_reconciler.scoped", "team_filter", teamFilter)
	}

	// ── Postgres: connection-cap drift detection and remediation ────────────
	//
	// The ORDER BY id keeps the per-tick window stable across consecutive ticks
	// while a backlog drains. applied_conn_limit IS NULL = never re-graded since
	// migration 047 added the column.
	pgRows, err := w.db.QueryContext(ctx, `
		SELECT r.id, r.token, r.provider_resource_id, r.tier, r.applied_conn_limit, t.plan_tier
		  FROM resources r
		  JOIN teams t ON t.id = r.team_id
		 WHERE r.resource_type = 'postgres'
		   AND r.status = 'active'
		   AND (r.expires_at IS NULL OR r.expires_at > now())
		   AND ($2 = '' OR t.id::text = ANY(string_to_array($2, ',')))
		 ORDER BY r.id
		 LIMIT $1
	`, entitlementReconcilerBatchLimit, teamFilter)
	if err != nil {
		return fmt.Errorf("EntitlementReconcilerWorker: postgres query failed: %w", err)
	}
	defer func() { _ = pgRows.Close() }()

	var pgCandidates []entitlementCandidate
	for pgRows.Next() {
		var c entitlementCandidate
		if scanErr := pgRows.Scan(
			&c.id, &c.token, &c.providerResourceID,
			&c.resourceTier, &c.appliedConnLimit, &c.planTier,
		); scanErr != nil {
			slog.Warn("jobs.entitlement_reconciler.postgres.scan_failed", "error", scanErr)
			continue
		}
		pgCandidates = append(pgCandidates, c)
	}
	if rowsErr := pgRows.Err(); rowsErr != nil {
		return fmt.Errorf("EntitlementReconcilerWorker: postgres rows error: %w", rowsErr)
	}
	_ = pgRows.Close()

	var pgScanned, pgDrifted, pgRegraded, pgFailed, pgSkippedTier int
	for _, c := range pgCandidates {
		pgScanned++

		// Skip when EITHER the resource snapshot OR the team plan is
		// ephemeral: the snapshot is the source of truth (MR-P1-21), but
		// a deleted/downgraded-to-free team's resources should not be
		// regraded even if their snapshot is paid (defensive).
		if entitlementEphemeralTiers[c.resourceTier] || entitlementEphemeralTiers[c.planTier] {
			pgSkippedTier++
			continue
		}

		// MR-P1-21: resolve caps from resource.tier (the per-row snapshot),
		// NOT team.plan_tier. A downgraded paying customer keeps their
		// previously-applied cap because the snapshot is preserved.
		needsRegrade, entitled := shouldRegrade(w.plans, c.resourceTier, c.appliedConnLimit)
		if !needsRegrade {
			continue
		}
		pgDrifted++
		metrics.EntitlementDriftDetectedTotal.Inc()

		oldLimit := "null"
		if c.appliedConnLimit.Valid {
			oldLimit = fmt.Sprintf("%d", c.appliedConnLimit.Int64)
		}

		// Per-resource re-grade. requestID is the resource id — gives the
		// provisioner a stable idempotency key per resource. The TIER passed
		// to the provisioner is the resource.tier snapshot (MR-P1-21), so
		// the infra cap matches the resource's recorded entitlement.
		regradeCtx, cancel := context.WithTimeout(ctx, entitlementReconcilerRegradeTimeout)
		out, regErr := w.regrader.RegradeResource(
			regradeCtx, c.token, c.providerResourceID.String,
			commonv1.ResourceType_RESOURCE_TYPE_POSTGRES, c.resourceTier, c.id.String(),
		)
		cancel()

		if regErr != nil {
			pgFailed++
			metrics.EntitlementRegradeFailedTotal.Inc()
			slog.Error("jobs.entitlement_reconciler.postgres.regrade_failed",
				"resource_id", c.id.String(),
				"resource_tier", c.resourceTier,
				"plan_tier", c.planTier,
				"old_limit", oldLimit,
				"entitled_limit", entitled,
				"error", regErr,
			)
			continue // leave the row for the next sweep
		}

		if !out.Applied {
			pgFailed++
			metrics.EntitlementRegradeFailedTotal.Inc()
			slog.Warn("jobs.entitlement_reconciler.postgres.regrade_skipped",
				"resource_id", c.id.String(),
				"resource_tier", c.resourceTier,
				"plan_tier", c.planTier,
				"old_limit", oldLimit,
				"entitled_limit", entitled,
				"skip_reason", out.SkipReason,
			)
			continue // leave the row for the next sweep
		}

		// applied=true — persist the connection cap the provisioner actually
		// applied. We trust resp.AppliedConnLimit over our locally-computed
		// `entitled` so the row reflects DB reality (the provisioner is the
		// authority on what it managed to apply).
		if _, uErr := w.db.ExecContext(ctx,
			`UPDATE resources SET applied_conn_limit = $1 WHERE id = $2`,
			out.AppliedConnLimit, c.id,
		); uErr != nil {
			pgFailed++
			metrics.EntitlementRegradeFailedTotal.Inc()
			slog.Error("jobs.entitlement_reconciler.postgres.persist_failed",
				"resource_id", c.id.String(),
				"resource_tier", c.resourceTier,
				"plan_tier", c.planTier,
				"applied_conn_limit", out.AppliedConnLimit,
				"error", uErr,
			)
			continue
		}

		pgRegraded++
		metrics.EntitlementRegradedTotal.Inc()
		slog.Info("jobs.entitlement_reconciler.postgres.regraded",
			"resource_id", c.id.String(),
			"resource_tier", c.resourceTier,
			"plan_tier", c.planTier,
			"old_limit", oldLimit,
			"new_limit", out.AppliedConnLimit,
			"skip_reason", out.SkipReason,
		)
		// F6: emit a dedicated, alertable drift-correction signal. The
		// per-resource INFO above is operational detail; this WARN-level line
		// (with a stable event name + a 1:1 Prometheus counter) is what
		// monitoring watches — a rising rate means plan upgrades are routinely
		// landing the team row before the infra cap, the F6 partial-upgrade
		// symptom. Without it the correction was silent and an operator could
		// not see the drift rate climbing.
		metrics.EntitlementDriftCorrectedTotal.WithLabelValues(entitlementResourceTypePostgres).Inc()
		slog.Warn(entitlementDriftCorrectedSignal,
			"resource_type", entitlementResourceTypePostgres,
			"resource_id", c.id.String(),
			"resource_tier", c.resourceTier,
			"plan_tier", c.planTier,
			"old_limit", oldLimit,
			"new_limit", out.AppliedConnLimit,
		)
	}

	// ── Redis: maxmemory backfill (A4) ───────────────────────────────────────
	//
	// Bug fixed (fix/a4-redis-rekey-on-token): the original sweep filtered on
	// provider_resource_id LIKE 'instant-customer-%', but 100% of prod Redis
	// resources have provider_resource_id = NULL — so the sweep matched 0 rows
	// every tick (redis_checked:0 in prod logs).
	//
	// The k8s namespace is deterministic: instant-customer-<token>. We no longer
	// rely on provider_resource_id to identify k8s pods. Instead we select ALL
	// active Redis resources and pass the token to RegradeResource; the provisioner
	// derives the namespace from the token. The provisioner's regradeRedis already
	// guards against non-k8s (shared) backends by checking the identifier prefix or
	// the backend type — a bare token produces namespace "instant-customer-<token>"
	// which is always the k8s path; shared-backend resources with no k8s pod will
	// return {applied:false, skip_reason:"backend does not support redis regrade"}.
	//
	// There is no applied_maxmemory_mb column (no migration); the provisioner's
	// Regrade is idempotent (CONFIG GET → compare → CONFIG SET only if different)
	// so calling it every sweep is safe and correct. The ~9 pre-existing uncapped
	// pods converge on the first sweep; subsequent sweeps are all CONFIG GET
	// no-ops ("already correct").
	redisRows, redisErr := w.db.QueryContext(ctx, `
		SELECT r.id, r.token, r.provider_resource_id, r.tier, t.plan_tier
		  FROM resources r
		  JOIN teams t ON t.id = r.team_id
		 WHERE r.resource_type = 'redis'
		   AND r.status = 'active'
		   AND (r.expires_at IS NULL OR r.expires_at > now())
		   AND ($2 = '' OR t.id::text = ANY(string_to_array($2, ',')))
		 ORDER BY r.id
		 LIMIT $1
	`, entitlementReconcilerBatchLimit, teamFilter)
	if redisErr != nil {
		// Redis sweep failure is non-fatal for the Postgres path — log and continue.
		// The Postgres results are already accumulated above. Return an error so
		// River retries the whole tick, which will re-run both sweeps.
		return fmt.Errorf("EntitlementReconcilerWorker: redis query failed: %w", redisErr)
	}
	defer func() { _ = redisRows.Close() }()

	var redisCandidates []redisEntitlementCandidate
	for redisRows.Next() {
		var c redisEntitlementCandidate
		if scanErr := redisRows.Scan(
			&c.id, &c.token, &c.providerResourceID,
			&c.resourceTier, &c.planTier,
		); scanErr != nil {
			slog.Warn("jobs.entitlement_reconciler.redis.scan_failed", "error", scanErr)
			continue
		}
		// Skip resources that have no token — without a token we cannot derive
		// the k8s namespace. This should never happen in practice (token is a
		// NOT NULL column), but guard defensively.
		if c.token == "" {
			slog.Warn("jobs.entitlement_reconciler.redis.no_token_skip",
				"resource_id", c.id.String())
			continue
		}
		redisCandidates = append(redisCandidates, c)
	}
	if rowsErr := redisRows.Err(); rowsErr != nil {
		return fmt.Errorf("EntitlementReconcilerWorker: redis rows error: %w", rowsErr)
	}
	_ = redisRows.Close()

	var redisChecked, redisApplied, redisSkipped, redisFailed, redisSkippedTier int
	for _, c := range redisCandidates {
		redisChecked++
		metrics.RedisMaxmemoryCheckedTotal.Inc()

		// MR-P1-21 (Redis twin): skip on EITHER ephemeral resource.tier
		// OR ephemeral team.plan_tier. Caps come from resource.tier — the
		// snapshot — so a downgraded paying customer's Redis pod keeps
		// its pro maxmemory cap (T8 P1-10 was the noeviction-shrink bug).
		if entitlementEphemeralTiers[c.resourceTier] || entitlementEphemeralTiers[c.planTier] {
			// Anonymous and free tier dedicated Redis would be unusual, but skip
			// gracefully for correctness — they are not re-graded.
			redisSkippedTier++
			continue
		}

		// Per-resource regrade — call the provisioner which does an idempotent
		// CONFIG GET / CONFIG SET. requestID is the resource UUID for stable
		// idempotency.
		//
		// Key change (fix/a4-redis-rekey-on-token): we pass the bare token as
		// providerResourceID instead of the DB prid (which is NULL in prod). The
		// provisioner's regradeRedis constructs "instant-customer-<token>" from a
		// bare token, so the k8s pod is found correctly. If prid already carries
		// the "instant-customer-" prefix (legacy rows that do have it set) we pass
		// it as-is — the provisioner accepts both forms.
		nsIdentifier := c.token
		if strings.HasPrefix(c.providerResourceID.String, redisK8sNsPrefix) {
			// prid already encodes the k8s namespace — use it directly.
			nsIdentifier = c.providerResourceID.String
		}

		// MR-P1-21: tier passed to the provisioner is the resource snapshot
		// — NOT team.plan_tier. A downgraded paying customer keeps their
		// previously-applied maxmemory because the snapshot is preserved.
		regradeCtx, cancel := context.WithTimeout(ctx, entitlementReconcilerRegradeTimeout)
		out, regErr := w.regrader.RegradeResource(
			regradeCtx, c.token, nsIdentifier,
			commonv1.ResourceType_RESOURCE_TYPE_REDIS, c.resourceTier, c.id.String(),
		)
		cancel()

		if regErr != nil {
			redisFailed++
			metrics.RedisMaxmemoryFailedTotal.Inc()
			slog.Error("jobs.entitlement_reconciler.redis.regrade_failed",
				"resource_id", c.id.String(),
				"token", logsafe.Token(c.token),
				"resource_tier", c.resourceTier,
				"plan_tier", c.planTier,
				"ns_identifier", nsIdentifier,
				"provider_resource_id", c.providerResourceID.String,
				"error", regErr,
			)
			continue // leave for next sweep
		}

		if out.Applied {
			redisApplied++
			metrics.RedisMaxmemoryAppliedTotal.Inc()
			slog.Info("jobs.entitlement_reconciler.redis.maxmemory_applied",
				"resource_id", c.id.String(),
				"token", logsafe.Token(c.token),
				"resource_tier", c.resourceTier,
				"plan_tier", c.planTier,
				"applied_maxmemory_mb", out.AppliedConnLimit, // AppliedConnLimit repurposed for MB
				"ns_identifier", nsIdentifier,
			)
			// F6: a CONFIG SET that landed is an infra-entitlement drift
			// correction — emit the same dedicated, alertable signal the
			// Postgres path emits so monitoring sees Redis drift corrections
			// in the one `entitlement.drift_corrected` stream.
			metrics.EntitlementDriftCorrectedTotal.WithLabelValues(entitlementResourceTypeRedis).Inc()
			slog.Warn(entitlementDriftCorrectedSignal,
				"resource_type", entitlementResourceTypeRedis,
				"resource_id", c.id.String(),
				"resource_tier", c.resourceTier,
				"plan_tier", c.planTier,
				"new_limit", out.AppliedConnLimit,
			)
		} else {
			redisSkipped++
			metrics.RedisMaxmemorySkippedTotal.Inc()
			slog.Debug("jobs.entitlement_reconciler.redis.maxmemory_skipped",
				"resource_id", c.id.String(),
				"token", logsafe.Token(c.token),
				"resource_tier", c.resourceTier,
				"plan_tier", c.planTier,
				"skip_reason", out.SkipReason,
			)
		}
	}

	// ── MongoDB: regrade fan-out (MR-P1-22) ──────────────────────────────────
	//
	// Iterates every active, non-expired Mongo resource and calls
	// RegradeResource. The provisioner today returns
	// {applied:false, skip_reason:"unsupported resource type for regrade"}
	// for MONGODB — that's expected, the sweep is the loud no-op that gives a
	// future Mongo-regrade implementation a CI-protected attach point. When
	// provisioner.regradeMongo lands, this sweep starts delivering Mongo cap
	// updates with zero changes to the worker. Until then, every sweep emits
	// one skipped count per Mongo resource and the operator sees the sweep is
	// covering Mongo (rather than silently omitting it, which was the bug).
	mongoChecked, mongoApplied, mongoSkipped, mongoFailed, mongoSkippedTier := w.sweepMongoEntitlements(ctx, teamFilter)

	// #146 (BugBash 2026-05-20 idle-tick noise pass): the reconciler runs
	// every 5 minutes (288 times/day). A true idle tick — no drift, no
	// regrade, no failure across all three resource types — is operational
	// silence and goes to DEBUG. INFO any time we moved bits (regraded,
	// applied), saw drift, or hit a failure.
	level := slog.LevelInfo
	if pgDrifted == 0 && pgRegraded == 0 && pgFailed == 0 &&
		redisApplied == 0 && redisFailed == 0 &&
		mongoApplied == 0 && mongoFailed == 0 {
		level = slog.LevelDebug
	}
	slog.Log(ctx, level, "jobs.entitlement_reconciler.completed",
		// Postgres metrics (backward-compatible log keys)
		"postgres_scanned", pgScanned,
		"postgres_drifted", pgDrifted,
		"postgres_regraded", pgRegraded,
		"postgres_failed", pgFailed,
		"postgres_skipped_ephemeral", pgSkippedTier,
		// Redis A4 metrics
		"redis_checked", redisChecked,
		"redis_applied", redisApplied,
		"redis_skipped", redisSkipped,
		"redis_failed", redisFailed,
		"redis_skipped_ephemeral", redisSkippedTier,
		// Mongo MR-P1-22 metrics (sweep coverage; provisioner-side
		// implementation lands separately — see sweepMongoEntitlements).
		"mongo_checked", mongoChecked,
		"mongo_applied", mongoApplied,
		"mongo_skipped", mongoSkipped,
		"mongo_failed", mongoFailed,
		"mongo_skipped_ephemeral", mongoSkippedTier,
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}

// sweepMongoEntitlements is the MR-P1-22 / T8 P1-9 arm: every sweep iterates
// active, non-expired Mongo resources and asks the provisioner to RegradeResource.
// The tier sent is `resource.tier` — the per-row snapshot — matching the
// downgrade-asymmetry contract that the snapshot is the entitlement-of-record
// (MR-P1-21).
//
// Returns: (checked, applied, skipped, failed, skippedEphemeral). A query
// failure is logged at WARN and treated as a zero-row tick — Mongo coverage
// is best-effort right now (until provisioner.regradeMongo ships, every
// tick is a skip, so a transient query failure has zero customer impact).
func (w *EntitlementReconcilerWorker) sweepMongoEntitlements(ctx context.Context, teamFilter string) (checked, applied, skipped, failed, skippedTier int) {
	mongoRows, err := w.db.QueryContext(ctx, `
		SELECT r.id, r.token, r.provider_resource_id, r.tier, t.plan_tier
		  FROM resources r
		  JOIN teams t ON t.id = r.team_id
		 WHERE r.resource_type = 'mongodb'
		   AND r.status = 'active'
		   AND (r.expires_at IS NULL OR r.expires_at > now())
		   AND ($2 = '' OR t.id::text = ANY(string_to_array($2, ',')))
		 ORDER BY r.id
		 LIMIT $1
	`, entitlementReconcilerBatchLimit, teamFilter)
	if err != nil {
		slog.Warn("jobs.entitlement_reconciler.mongo.query_failed",
			"error", err.Error(),
			"note", "Mongo sweep skipped this tick — Mongo regrade is provisioner-side TODO; query failure is non-fatal for the Postgres/Redis arms",
		)
		return 0, 0, 0, 0, 0
	}
	defer func() { _ = mongoRows.Close() }()

	var mongoCandidates []mongoEntitlementCandidate
	for mongoRows.Next() {
		var c mongoEntitlementCandidate
		if scanErr := mongoRows.Scan(
			&c.id, &c.token, &c.providerResourceID,
			&c.resourceTier, &c.planTier,
		); scanErr != nil {
			slog.Warn("jobs.entitlement_reconciler.mongo.scan_failed", "error", scanErr)
			continue
		}
		if c.token == "" {
			continue
		}
		mongoCandidates = append(mongoCandidates, c)
	}
	if rowsErr := mongoRows.Err(); rowsErr != nil {
		slog.Warn("jobs.entitlement_reconciler.mongo.rows_failed", "error", rowsErr)
		return checked, applied, skipped, failed, skippedTier
	}
	_ = mongoRows.Close()

	for _, c := range mongoCandidates {
		checked++

		// MR-P1-21: skip ephemeral resource.tier OR plan_tier — the
		// snapshot is the source of truth (a paid-tier resource on a
		// downgraded team keeps its cap), but a deleted/free team's
		// resources still skip even with a paid snapshot.
		if entitlementEphemeralTiers[c.resourceTier] || entitlementEphemeralTiers[c.planTier] {
			skippedTier++
			continue
		}

		// Tier sent to provisioner is the resource snapshot (MR-P1-21).
		regradeCtx, cancel := context.WithTimeout(ctx, entitlementReconcilerRegradeTimeout)
		out, regErr := w.regrader.RegradeResource(
			regradeCtx, c.token, c.providerResourceID.String,
			commonv1.ResourceType_RESOURCE_TYPE_MONGODB, c.resourceTier, c.id.String(),
		)
		cancel()

		if regErr != nil {
			failed++
			slog.Warn("jobs.entitlement_reconciler.mongo.regrade_failed",
				"resource_id", c.id.String(),
				"token", logsafe.Token(c.token),
				"resource_tier", c.resourceTier,
				"plan_tier", c.planTier,
				"error", regErr,
			)
			continue
		}

		if out.Applied {
			applied++
			slog.Info("jobs.entitlement_reconciler.mongo.regraded",
				"resource_id", c.id.String(),
				"token", logsafe.Token(c.token),
				"resource_tier", c.resourceTier,
				"plan_tier", c.planTier,
				"applied_value", out.AppliedConnLimit,
				"skip_reason", out.SkipReason,
			)
		} else {
			skipped++
			// DEBUG, not INFO — until provisioner.regradeMongo lands, this
			// fires once per Mongo resource per 5-minute tick (the expected
			// steady state). Logged at DEBUG so it does not spam INFO; the
			// rollup `mongo_skipped` counter on the .completed line is the
			// visible signal.
			slog.Debug("jobs.entitlement_reconciler.mongo.regrade_skipped",
				"resource_id", c.id.String(),
				"token", logsafe.Token(c.token),
				"resource_tier", c.resourceTier,
				"plan_tier", c.planTier,
				"skip_reason", out.SkipReason,
			)
		}
	}
	return checked, applied, skipped, failed, skippedTier
}
