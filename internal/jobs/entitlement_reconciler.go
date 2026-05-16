package jobs

// entitlement_reconciler.go — periodic sweep that fixes "upgrade drift".
//
// Background: when a customer upgrades their plan, the api updates
// resources.tier (and teams.plan_tier), but the resource's *actual* Postgres
// connection cap is NOT re-applied to the database — the higher entitlement
// is recorded but never enforced. This job detects that drift and fixes it.
//
// Each sweep:
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
//	entitled — the entitled connection cap for planTier (the value the
//	           provisioner should apply). Only meaningful when drift is true.
//
// ephemeral tiers (anonymous/free) always return drift=false: they are never
// re-graded up regardless of their applied_conn_limit.
func shouldRegrade(plans PlanRegistry, planTier string, appliedConnLimit sql.NullInt64) (drift bool, entitled int) {
	if entitlementEphemeralTiers[planTier] {
		return false, 0
	}
	entitled = plans.ConnectionsLimit(planTier, "postgres")
	if !appliedConnLimit.Valid {
		// NULL — never re-graded since migration 047 added the column.
		return true, entitled
	}
	if appliedConnLimit.Int64 != int64(entitled) {
		return true, entitled
	}
	return false, entitled
}

// Work executes one sweep.
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

	// Sweep: active, non-expired postgres resources joined to their team's
	// plan tier. The ORDER BY id keeps the per-tick window stable across
	// consecutive ticks while a backlog drains.
	//
	// teamFilter ($2) scopes the sweep: empty = every team (prod default);
	// a comma-separated UUID list = only those teams. The `$2 = '' OR …`
	// predicate is a parameterised no-op when the filter is empty.
	teamFilter := EntitlementReconcileTeamFilter()
	if teamFilter != "" {
		slog.Info("jobs.entitlement_reconciler.scoped", "team_filter", teamFilter)
	}
	rows, err := w.db.QueryContext(ctx, `
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
		return fmt.Errorf("EntitlementReconcilerWorker: query failed: %w", err)
	}
	defer rows.Close()

	var candidates []entitlementCandidate
	for rows.Next() {
		var c entitlementCandidate
		if scanErr := rows.Scan(
			&c.id, &c.token, &c.providerResourceID,
			&c.resourceTier, &c.appliedConnLimit, &c.planTier,
		); scanErr != nil {
			slog.Warn("jobs.entitlement_reconciler.scan_failed", "error", scanErr)
			continue
		}
		candidates = append(candidates, c)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return fmt.Errorf("EntitlementReconcilerWorker: rows error: %w", rowsErr)
	}
	rows.Close()

	var scanned, drifted, regraded, failed, skippedTier int
	for _, c := range candidates {
		scanned++

		if entitlementEphemeralTiers[c.planTier] {
			skippedTier++
			continue
		}

		needsRegrade, entitled := shouldRegrade(w.plans, c.planTier, c.appliedConnLimit)
		if !needsRegrade {
			continue
		}
		drifted++
		metrics.EntitlementDriftDetectedTotal.Inc()

		oldLimit := "null"
		if c.appliedConnLimit.Valid {
			oldLimit = fmt.Sprintf("%d", c.appliedConnLimit.Int64)
		}

		// Per-resource re-grade. requestID is the resource id — gives the
		// provisioner a stable idempotency key per resource.
		regradeCtx, cancel := context.WithTimeout(ctx, entitlementReconcilerRegradeTimeout)
		out, regErr := w.regrader.RegradeResource(
			regradeCtx, c.token, c.providerResourceID.String,
			commonv1.ResourceType_RESOURCE_TYPE_POSTGRES, c.planTier, c.id.String(),
		)
		cancel()

		if regErr != nil {
			failed++
			metrics.EntitlementRegradeFailedTotal.Inc()
			slog.Error("jobs.entitlement_reconciler.regrade_failed",
				"resource_id", c.id.String(),
				"plan_tier", c.planTier,
				"old_limit", oldLimit,
				"entitled_limit", entitled,
				"error", regErr,
			)
			continue // leave the row for the next sweep
		}

		if !out.Applied {
			failed++
			metrics.EntitlementRegradeFailedTotal.Inc()
			slog.Warn("jobs.entitlement_reconciler.regrade_skipped",
				"resource_id", c.id.String(),
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
			failed++
			metrics.EntitlementRegradeFailedTotal.Inc()
			slog.Error("jobs.entitlement_reconciler.persist_failed",
				"resource_id", c.id.String(),
				"plan_tier", c.planTier,
				"applied_conn_limit", out.AppliedConnLimit,
				"error", uErr,
			)
			continue
		}

		regraded++
		metrics.EntitlementRegradedTotal.Inc()
		slog.Info("jobs.entitlement_reconciler.regraded",
			"resource_id", c.id.String(),
			"plan_tier", c.planTier,
			"old_limit", oldLimit,
			"new_limit", out.AppliedConnLimit,
			"skip_reason", out.SkipReason,
		)
	}

	slog.Info("jobs.entitlement_reconciler.completed",
		"scanned", scanned,
		"drifted", drifted,
		"regraded", regraded,
		"failed", failed,
		"skipped_ephemeral_tier", skippedTier,
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}
