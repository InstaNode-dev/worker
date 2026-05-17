package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"

	"instant.dev/worker/internal/metrics"
)

// redisServiceName is the plans.Registry service key for Redis storage limits.
// StorageLimitMB(tier, redisServiceName) resolves the tier's redis_memory_mb
// from plans.yaml — the single source of truth for the per-tenant cap. Named
// constant per CLAUDE.md ("Use named constants, not inline strings").
const redisServiceName = "redis"

// ── EnforceStorageQuotaWorker ─────────────────────────────────────────────────

// EnforceStorageQuotaArgs holds arguments for the storage quota enforcement job.
type EnforceStorageQuotaArgs struct{}

func (EnforceStorageQuotaArgs) Kind() string { return "enforce_storage_quota" }

// PlanRegistry is the minimal interface needed from plans.Registry across
// all worker jobs. Methods are added here as new jobs need them; the live
// *commonplans.Registry satisfies every method, so a superset is always
// safe — test mocks just need the new method stubbed.
type PlanRegistry interface {
	StorageLimitMB(tier, service string) int
	// ConnectionsLimit is consumed by QuotaWallNudgeWorker.
	ConnectionsLimit(tier, service string) int
	// ProvisionLimit is consumed by QuotaWallNudgeWorker.
	ProvisionLimit(tier string) int
}

// StorageQuotaChecker is the minimal interface needed for storage quota checks.
type StorageQuotaChecker interface {
	CheckStorageQuota(ctx context.Context, db *sql.DB, resourceID uuid.UUID, limitMB int) (int64, bool, error)
}

// resourceStatusSuspended / resourceStatusActive are named constants for the
// two status values touched by the suspend/unsuspend lifecycle. Using constants
// avoids scattered string literals and makes the "suspended → active" transition
// greppable. (CLAUDE.md: "Use named constants, not inline strings".)
const (
	resourceStatusSuspended = "suspended"
	resourceStatusActive    = "active"
)

// quotaUnsuspendHysteresisFactor is the hysteresis band on the unsuspend
// threshold. A resource is SUSPENDED at bytesUsed >= limitBytes but only
// UNSUSPENDED once bytesUsed drops below limitBytes * factor (i.e. below 90%
// of the limit). Without this band a resource sitting exactly at the limit
// flip-flops suspend→active→suspend every tick — and each flip does a real
// provider-side REVOKE/GRANT round-trip. The 10% gap means usage must
// meaningfully recede before access is restored. Each flip is also costly
// (a customer-facing infra mutation), so the dead-band is deliberately wide.
const quotaUnsuspendHysteresisFactor = 0.9

// EnforceStorageQuotaWorker checks all active resources against their plan's
// storage limit and suspends those that have exceeded it. It also scans
// currently-suspended resources and unsuspends those whose usage has dropped
// back below the limit.
//
// Two loops per Work() run:
//  1. Suspend loop — scans status='active' postgres/redis/mongodb resources,
//     calls the provisioner-side infra revoke (REVOKE CONNECT / ACL SETUSER off
//     / revokeRolesFromUser), then flips status to 'suspended'.
//  2. Unsuspend loop — scans status='suspended' resources, re-checks storage,
//     re-grants infra access, and flips status back to 'active' for those now
//     under limit.
//
// Both loops fail-open on infra errors (connectivity issues with the customer
// DB / Redis / Mongo only affect the row flip as a fallback — the status flip
// still lands). Fail-open matches CLAUDE.md convention #1.
type EnforceStorageQuotaWorker struct {
	river.WorkerDefaults[EnforceStorageQuotaArgs]
	db      *sql.DB
	plans   PlanRegistry
	revoker ResourceInfraRevoker // nil = no infra revoke (status-row only)
	// evictor performs per-tenant key eviction for SHARED-backend Redis
	// tenants (anonymous/free) — see quota_redis_eviction.go. nil = eviction
	// loop is a logged no-op (suspend/unsuspend loops still run). Redis ACL has
	// no per-user maxmemory, so this is the only per-tenant memory enforcement
	// on the shared `redis-provision` pod (closes the A4 caveat).
	evictor RedisKeyEvictor
}

// NewEnforceStorageQuotaWorker constructs an EnforceStorageQuotaWorker.
// Pass nil for revoker to skip infra-level revoke/grant (status flip still
// occurs). In production, pass NewDirectResourceRevoker(...) built from cfg.
//
// Backward-compatible: the evictor defaults to nil. Callers that want
// shared-Redis per-tenant key eviction must use NewEnforceStorageQuotaWorkerWithEvictor.
func NewEnforceStorageQuotaWorker(db *sql.DB, plans PlanRegistry, revoker ResourceInfraRevoker) *EnforceStorageQuotaWorker {
	return &EnforceStorageQuotaWorker{db: db, plans: plans, revoker: revoker}
}

// NewEnforceStorageQuotaWorkerWithEvictor constructs an EnforceStorageQuotaWorker
// that also performs per-tenant key eviction for over-quota SHARED-backend Redis
// tenants. Pass nil for evictor to disable eviction (equivalent to
// NewEnforceStorageQuotaWorker). In production, pass NewDirectRedisEvictor(cfg.CustomerRedisURL).
func NewEnforceStorageQuotaWorkerWithEvictor(db *sql.DB, plans PlanRegistry, revoker ResourceInfraRevoker, evictor RedisKeyEvictor) *EnforceStorageQuotaWorker {
	return &EnforceStorageQuotaWorker{db: db, plans: plans, revoker: revoker, evictor: evictor}
}

// Work scans all active postgres/redis/mongodb resources and suspends those
// over their quota, then unsuspends any suspended resources that are now back
// under their quota.
func (w *EnforceStorageQuotaWorker) Work(ctx context.Context, job *river.Job[EnforceStorageQuotaArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.enforce_storage_quota")
	defer span.End()

	suspendedIDs, err := w.runSuspendLoop(ctx)
	if err != nil {
		return err
	}
	suspended := len(suspendedIDs)

	// The unsuspend loop must NOT act on a resource the suspend loop just
	// flipped in this same Work() — the suspend loop's UPDATE is already
	// committed, so a fresh status='suspended' SELECT would re-include it and
	// (on a stale storage_bytes snapshot) immediately unsuspend it: a
	// suspend→active flap inside one tick. Pass the just-suspended IDs as a
	// skip-set so this tick can never undo its own work.
	unsuspended, err := w.runUnsuspendLoop(ctx, suspendedIDs)
	if err != nil {
		// Don't fail the job for unsuspend errors — suspends already landed.
		slog.Error("jobs.enforce_storage_quota.unsuspend_loop_error", "error", err)
	}

	// Shared-backend Redis per-tenant key eviction (A4). Redis ACL has no
	// per-user maxmemory, so an over-quota free/anonymous tenant can starve
	// every other tenant on the shared pod. This loop SCAN+DELETEs the
	// over-cap tenant's OWN `{token}:*` keyspace oldest-first. Fail-soft per
	// tenant; an error here never fails the job — the suspend loop already
	// landed.
	evictedTenants, err := w.runRedisEvictionLoop(ctx)
	if err != nil {
		slog.Error("jobs.enforce_storage_quota.redis_eviction_loop_error", "error", err)
	}

	var jobID int64
	if job.JobRow != nil {
		jobID = job.ID
	}
	slog.Info("jobs.enforce_storage_quota.completed",
		"suspended_count", suspended,
		"unsuspended_count", unsuspended,
		"redis_evicted_tenants", evictedTenants,
		"job_id", jobID,
	)
	return nil
}

// runRedisEvictionLoop scans status='active' SHARED-backend Redis resources
// (anonymous/free tier — see isSharedRedisTier) and, for each tenant whose
// measured usage exceeds its tier's redis_memory_mb cap, deletes keys from that
// ONE tenant's `{token}:*` keyspace oldest-first until it is back under cap.
//
// This is the per-tenant memory enforcement that Redis ACL cannot provide on a
// shared pod. Dedicated (k8s-backed, paid-tier) Redis pods have a real
// maxmemory and are handled by EntitlementReconcilerWorker — this loop
// deliberately skips every non-shared tier.
//
// Returns the count of tenants that had >=1 key evicted. Fail-soft: one
// tenant's error is logged and the sweep continues; a SELECT failure returns
// an error so River retries the tick.
func (w *EnforceStorageQuotaWorker) runRedisEvictionLoop(ctx context.Context) (int, error) {
	if w.evictor == nil {
		// No evictor wired (CUSTOMER_REDIS_URL unset, or constructed via the
		// legacy NewEnforceStorageQuotaWorker). The suspend loop still ran;
		// eviction is simply unavailable this run.
		slog.Warn("jobs.enforce_storage_quota.redis_eviction_skipped",
			"reason", "no RedisKeyEvictor configured")
		return 0, nil
	}

	rows, err := w.db.QueryContext(ctx, `
		SELECT id, token, tier, storage_bytes
		FROM resources
		WHERE status = $1
		  AND resource_type = 'redis'
		ORDER BY created_at
	`, resourceStatusActive)
	if err != nil {
		return 0, fmt.Errorf("EnforceStorageQuotaWorker.redisEvictionLoop: query failed: %w", err)
	}
	defer rows.Close()

	checked, enforced := 0, 0

	for rows.Next() {
		var (
			id           string
			token        string
			tier         string
			storageBytes int64
		)
		if scanErr := rows.Scan(&id, &token, &tier, &storageBytes); scanErr != nil {
			slog.Error("jobs.enforce_storage_quota.redis_eviction_scan_error", "error", scanErr)
			continue
		}

		// Only SHARED-backend Redis tenants are evicted. Paid tiers get
		// dedicated k8s pods (real maxmemory, reconciler-managed) — skip them.
		if !isSharedRedisTier(tier) {
			continue
		}
		checked++

		// All limits come from plans.Registry — never hardcoded (CLAUDE.md #3).
		// StorageLimitMB(tier, "redis") resolves redis_memory_mb from plans.yaml.
		limitMB := w.plans.StorageLimitMB(tier, redisServiceName)
		if limitMB == -1 {
			// Unlimited tier — never evict. (A shared-tier with unlimited Redis
			// would be a plans.yaml misconfiguration, but guard defensively.)
			continue
		}
		limitBytes := int64(limitMB) * 1024 * 1024

		// Cheap pre-filter: the stored storage_bytes (refreshed every 6h by
		// UpdateStorageBytesWorker) lets us skip the SCAN for tenants that are
		// obviously under cap. The evictor re-measures authoritatively before
		// deleting anything, so a stale storage_bytes only ever costs a wasted
		// scan — it can never cause an incorrect deletion.
		if storageBytes < limitBytes {
			continue
		}

		keysDeleted, bytesReclaimed, evErr := w.evictor.EvictTenantToCap(ctx, token, limitBytes)
		if evErr != nil {
			metrics.RedisEvictionFailedTotal.Inc()
			slog.Error("jobs.enforce_storage_quota.redis_eviction_failed",
				"resource_id", id,
				"token", token,
				"tier", tier,
				"limit_mb", limitMB,
				"error", evErr,
			)
			continue // fail-soft — leave the tenant for the next sweep
		}

		if keysDeleted == 0 {
			// Tenant was at/under cap when re-measured (stale storage_bytes) —
			// idempotent no-op.
			continue
		}

		metrics.RedisEvictedKeysTotal.Add(float64(keysDeleted))
		metrics.RedisEvictedBytesTotal.Add(float64(bytesReclaimed))
		metrics.RedisEvictedTenantsTotal.Inc()
		enforced++

		slog.Warn("jobs.enforce_storage_quota.redis_evicted",
			"resource_id", id,
			"token", token,
			"tier", tier,
			"limit_mb", limitMB,
			"keys_deleted", keysDeleted,
			"bytes_reclaimed", bytesReclaimed,
		)
	}
	if err := rows.Err(); err != nil {
		return enforced, fmt.Errorf("EnforceStorageQuotaWorker.redisEvictionLoop: rows error: %w", err)
	}

	slog.Info("jobs.enforce_storage_quota.redis_eviction_loop_done",
		"checked_count", checked,
		"enforced_count", enforced,
	)
	return enforced, nil
}

// runSuspendLoop scans status='active' resources, suspends over-quota ones,
// and returns the IDs of the newly suspended resources. The caller passes
// those IDs to runUnsuspendLoop as a skip-set so a resource cannot be
// suspended and unsuspended within the same Work() tick.
func (w *EnforceStorageQuotaWorker) runSuspendLoop(ctx context.Context) ([]string, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT id, token, resource_type, tier, storage_bytes,
		       COALESCE(provider_resource_id, '')
		FROM resources
		WHERE status = $1
		  AND resource_type IN ('postgres', 'redis', 'mongodb')
		ORDER BY created_at
	`, resourceStatusActive)
	if err != nil {
		return nil, fmt.Errorf("EnforceStorageQuotaWorker.suspendLoop: query failed: %w", err)
	}
	defer rows.Close()

	checked := 0
	suspendedIDs := make([]string, 0)

	for rows.Next() {
		var (
			id                 string
			token              string
			resourceType       string
			tier               string
			storageBytes       int64
			providerResourceID string
		)
		if scanErr := rows.Scan(&id, &token, &resourceType, &tier, &storageBytes, &providerResourceID); scanErr != nil {
			slog.Error("jobs.enforce_storage_quota.scan_error", "error", scanErr)
			continue
		}
		checked++

		limitMB := w.plans.StorageLimitMB(tier, resourceType)
		if limitMB == -1 {
			continue // unlimited tier — never suspend
		}

		uid, parseErr := uuid.Parse(id)
		if parseErr != nil {
			slog.Error("jobs.enforce_storage_quota.invalid_uuid", "id", id, "error", parseErr)
			continue
		}

		_, exceeded, checkErr := checkStorageQuota(ctx, w.db, uid, limitMB)
		if checkErr != nil {
			slog.Error("jobs.enforce_storage_quota.check_error",
				"resource_id", id,
				"error", checkErr,
			)
			continue // fail open — don't suspend on check error
		}

		if !exceeded {
			continue
		}

		// Infra revoke FIRST, then status flip — matches the iron-rule order
		// from api/internal/handlers/resource.go Pause(): "provider-side FIRST
		// so the row stays active if infra fails; row flip is the commit."
		// Here we invert slightly: infra revoke is fail-open (logged warning,
		// not a hard error) so we always proceed to the status flip. This is
		// intentional: a row marked 'suspended' blocks new provisions from the
		// API even when the infra revoke is not available (customer DB down).
		if w.revoker != nil {
			// tier + provider_resource_id are passed so the revoker resolves
			// the EXACT Redis ACL username: the stored provider_resource_id
			// when present (canonical, never re-derived), else a tier-driven
			// derivation (shared usr_<full-token> / legacy dedicated
			// ded_<token[:8]>). See redisUsernameForToken.
			if revokeErr := w.revoker.RevokeAccess(ctx, resourceType, token, tier, providerResourceID); revokeErr != nil {
				// revoker implementations are fail-open (return nil on infra
				// error, log a WARN). A non-nil error here is unexpected —
				// log it but don't abort the row update.
				slog.Error("jobs.enforce_storage_quota.revoke_error",
					"resource_id", id, "token", token, "resource_type", resourceType,
					"error", revokeErr,
				)
			}
		}

		_, updateErr := w.db.ExecContext(ctx, `
			UPDATE resources SET status = $1
			WHERE id = $2 AND status = $3
		`, resourceStatusSuspended, id, resourceStatusActive)
		if updateErr != nil {
			slog.Error("jobs.enforce_storage_quota.suspend_failed",
				"resource_id", id,
				"error", updateErr,
			)
			continue
		}

		slog.Warn("jobs.enforce_storage_quota.suspended",
			"resource_id", id,
			"token", token,
			"resource_type", resourceType,
			"tier", tier,
			"storage_bytes", storageBytes,
			"limit_mb", limitMB,
		)
		suspendedIDs = append(suspendedIDs, id)
	}
	if err := rows.Err(); err != nil {
		return suspendedIDs, fmt.Errorf("EnforceStorageQuotaWorker.suspendLoop: rows error: %w", err)
	}

	slog.Info("jobs.enforce_storage_quota.suspend_loop_done",
		"checked_count", checked,
		"suspended_count", len(suspendedIDs),
	)
	return suspendedIDs, nil
}

// runUnsuspendLoop scans status='suspended' resources, re-checks quota, and
// re-grants infra access + flips to 'active' for those now under the
// hysteresis threshold (quotaUnsuspendHysteresisFactor of the limit).
// skipIDs are resources suspended earlier in this same Work() tick — they are
// never unsuspended here, so one tick cannot suspend-then-unsuspend a row.
// Returns the count of newly unsuspended resources.
func (w *EnforceStorageQuotaWorker) runUnsuspendLoop(ctx context.Context, skipIDs []string) (int, error) {
	// Build the skip-set once. Resources the suspend loop just flipped to
	// 'suspended' must be excluded so this tick cannot undo its own work.
	skip := make(map[string]struct{}, len(skipIDs))
	for _, id := range skipIDs {
		skip[id] = struct{}{}
	}

	rows, err := w.db.QueryContext(ctx, `
		SELECT id, token, resource_type, tier, storage_bytes,
		       COALESCE(provider_resource_id, '')
		FROM resources
		WHERE status = $1
		  AND resource_type IN ('postgres', 'redis', 'mongodb')
		ORDER BY created_at
	`, resourceStatusSuspended)
	if err != nil {
		return 0, fmt.Errorf("EnforceStorageQuotaWorker.unsuspendLoop: query failed: %w", err)
	}
	defer rows.Close()

	unsuspended := 0

	for rows.Next() {
		var (
			id                 string
			token              string
			resourceType       string
			tier               string
			storageBytes       int64
			providerResourceID string
		)
		if scanErr := rows.Scan(&id, &token, &resourceType, &tier, &storageBytes, &providerResourceID); scanErr != nil {
			slog.Error("jobs.enforce_storage_quota.unsuspend_scan_error", "error", scanErr)
			continue
		}

		// Skip any resource the suspend loop flipped this tick — it must not
		// be unsuspended in the same Work() (intra-tick flap guard).
		if _, justSuspended := skip[id]; justSuspended {
			continue
		}

		limitMB := w.plans.StorageLimitMB(tier, resourceType)
		if limitMB == -1 {
			// Unlimited tier shouldn't have been suspended, but unsuspend
			// eagerly to self-heal any historical bad state.
			limitMB = 0 // treat as belowThreshold=true below
		}

		uid, parseErr := uuid.Parse(id)
		if parseErr != nil {
			slog.Error("jobs.enforce_storage_quota.unsuspend_invalid_uuid", "id", id, "error", parseErr)
			continue
		}

		// Hysteresis: a suspend fires at bytesUsed >= limitBytes, but an
		// unsuspend fires only once bytesUsed drops below the hysteresis
		// threshold (90% of the limit). The dead-band between the two
		// thresholds stops a resource sitting at the limit from flip-flopping
		// every tick. limitMB == 0 means unlimited → always below threshold.
		belowThreshold := true
		if limitMB > 0 {
			bytesUsed, checkErr := readStorageBytes(ctx, w.db, uid)
			if checkErr != nil {
				slog.Error("jobs.enforce_storage_quota.unsuspend_check_error",
					"resource_id", id, "error", checkErr)
				continue // fail open — don't unsuspend on check error
			}
			unsuspendThreshold := int64(float64(int64(limitMB)*1024*1024) * quotaUnsuspendHysteresisFactor)
			belowThreshold = bytesUsed < unsuspendThreshold
		}

		if !belowThreshold {
			continue // not yet far enough below the limit — remain suspended
		}

		// Re-grant infra access before flipping the status row.
		if w.revoker != nil {
			// tier + provider_resource_id are passed so the revoker resolves
			// the EXACT Redis ACL username: the stored provider_resource_id
			// when present (canonical, never re-derived), else a tier-driven
			// derivation (shared usr_<full-token> / legacy dedicated
			// ded_<token[:8]>). See redisUsernameForToken.
			if grantErr := w.revoker.GrantAccess(ctx, resourceType, token, tier, providerResourceID); grantErr != nil {
				slog.Error("jobs.enforce_storage_quota.grant_error",
					"resource_id", id, "token", token, "resource_type", resourceType,
					"error", grantErr,
				)
				// Non-nil means unexpected path — still proceed with row flip.
			}
		}

		_, updateErr := w.db.ExecContext(ctx, `
			UPDATE resources SET status = $1
			WHERE id = $2 AND status = $3
		`, resourceStatusActive, id, resourceStatusSuspended)
		if updateErr != nil {
			slog.Error("jobs.enforce_storage_quota.unsuspend_failed",
				"resource_id", id, "error", updateErr)
			continue
		}

		slog.Info("jobs.enforce_storage_quota.unsuspended",
			"resource_id", id,
			"token", token,
			"resource_type", resourceType,
			"tier", tier,
			"storage_bytes", storageBytes,
			"limit_mb", limitMB,
		)
		unsuspended++
	}
	if err := rows.Err(); err != nil {
		return unsuspended, fmt.Errorf("EnforceStorageQuotaWorker.unsuspendLoop: rows error: %w", err)
	}

	return unsuspended, nil
}

// readStorageBytes reads the current resources.storage_bytes for a resource.
// Returns 0 (no error) when the row is gone. Used by the unsuspend loop's
// hysteresis check, which needs the raw byte count rather than a fixed-
// threshold exceeded/not boolean.
func readStorageBytes(ctx context.Context, db *sql.DB, resourceID uuid.UUID) (int64, error) {
	var bytesUsed int64
	err := db.QueryRowContext(ctx,
		`SELECT storage_bytes FROM resources WHERE id = $1`,
		resourceID,
	).Scan(&bytesUsed)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		slog.Error("quota.storage.db_error", "resource_id", resourceID, "error", err)
		return 0, fmt.Errorf("readStorageBytes: %w", err)
	}
	return bytesUsed, nil
}

// checkStorageQuota reads resources.storage_bytes and compares to limitMB.
// Returns (bytesUsed, exceeded, error). Fails open on DB error.
func checkStorageQuota(ctx context.Context, db *sql.DB, resourceID uuid.UUID, limitMB int) (int64, bool, error) {
	if limitMB == -1 {
		return 0, false, nil
	}

	var bytesUsed int64
	err := db.QueryRowContext(ctx,
		`SELECT storage_bytes FROM resources WHERE id = $1`,
		resourceID,
	).Scan(&bytesUsed)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		slog.Error("quota.storage.db_error", "resource_id", resourceID, "error", err)
		return 0, false, fmt.Errorf("checkStorageQuota: %w", err)
	}

	limitBytes := int64(limitMB) * 1024 * 1024
	return bytesUsed, bytesUsed >= limitBytes, nil
}
