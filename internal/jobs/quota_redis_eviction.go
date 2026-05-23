package jobs

// quota_redis_eviction.go — per-tenant key eviction for SHARED-backend Redis.
//
// ── The A4 caveat this closes ────────────────────────────────────────────────
//
// Free/anonymous Redis tenants live on ONE shared `redis-provision` pod as ACL
// users key-scoped to `~{token}:*`. Redis ACL has NO per-user `maxmemory` — so a
// single free tenant can fill the whole shared pod and evict (via the pod-wide
// maxmemory-policy) every other tenant's keys. `EnforceStorageQuotaWorker` can
// suspend a tenant's *access* (ACL SETUSER off) but suspension alone does not
// reclaim the RAM the over-quota tenant already consumed — the noisy neighbour
// keeps starving everyone until the 24h TTL sweep deletes the row.
//
// This file adds an LRU-style, per-tenant key eviction: when a shared-backend
// Redis tenant's measured usage exceeds its tier's `redis_memory_mb` cap, we
// SCAN that ONE tenant's `{token}:*` keyspace and DELETE keys oldest-first until
// the tenant is back under cap. This is the intended free-tier enforcement
// (5 MB tier, 24h TTL) — it is NOT customer-data loss: anonymous/free Redis is
// ephemeral by contract (CLAUDE.md tier model: anonymous = free, 24h TTL).
//
// ── Shared vs dedicated — how they are distinguished ─────────────────────────
//
// Dedicated (k8s-backed) Redis pods get a REAL per-pod `maxmemory` and are
// handled by EntitlementReconcilerWorker's Redis sweep (provisioner Regrade →
// CONFIG SET maxmemory). The provisioner's local/shared backend explicitly does
// NOT implement the Regrader interface (see provisioner backend/redis/backend.go:
// "shared/local ... have no per-tenant maxmemory lever").
//
// The codebase's stable distinguisher for "lives on the shared pod" is the
// TIER: anonymous and free are the ephemeral tiers (sharedRedisTiers below —
// the exact inverse of entitlementEphemeralTiers). Paid tiers (hobby and up)
// either get dedicated k8s pods (real maxmemory, reconciler-managed) or are
// exempt from key eviction entirely. We therefore evict ONLY ephemeral-tier
// redis rows: that is precisely the population on the shared key-scoped pod,
// and it is the population whose `redis_memory_mb` cap (5 MB) the Redis ACL
// cannot enforce.
//
// ── Safety invariant ─────────────────────────────────────────────────────────
//
// Every key this file touches is asserted to begin with the tenant's exact
// `{token}:` prefix before any DEL. A bug that lets the SCAN escape the
// namespace can NEVER delete another tenant's key — assertKeyInTenantPrefix
// is the guard, and TestRedisEviction_CrossTenantIsolation is its regression
// test. SCAN (cursor-based) is used, never KEYS (the provisioner's ACL
// allowlist denies KEYS for exactly this O(N) cross-tenant reason).
//
// ── NR alert recommendation (defense-in-depth) ───────────────────────────────
//
// This per-tenant eviction is the first line of defence. The second line is a
// pod-wide alert: configure a New Relic alert on the shared `redis-provision`
// pod's `redis_memory_used_bytes / redis_memory_maxmemory_bytes` ratio —
//
//     ALERT  redis-provision pod approaching maxmemory
//     WHEN   used_memory / maxmemory > 0.85  for 5 minutes
//     THEN   page — per-tenant eviction is falling behind, a tenant is
//            filling the pod faster than the 6h sweep can reclaim, OR many
//            tenants are simultaneously near-cap. Consider a faster sweep
//            cadence or splitting the shared pod.
//
// The per-tenant counters below (instant_redis_evicted_*) are the leading
// indicator; the pod-wide ratio alert is the backstop.

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	goredis "github.com/redis/go-redis/v9"

	"instant.dev/worker/internal/logsafe"
)

// sharedRedisTiers are the tiers whose Redis resources live on the shared
// `redis-provision` pod as key-scoped ACL users. These are the ephemeral tiers
// (anonymous + free, 24h TTL) — the exact inverse of entitlementEphemeralTiers.
// Paid tiers get dedicated k8s pods with a real maxmemory and are handled by
// the entitlement reconciler's Redis sweep, NOT by key eviction.
//
// Kept as a separate map (rather than reusing entitlementEphemeralTiers) so the
// intent is explicit at the call site: "evict shared-pod tenants", not "skip
// ephemeral tiers". The two maps holding the same keys is intentional — they
// are read in opposite senses.
var sharedRedisTiers = map[string]bool{
	"anonymous": true,
	"free":      true,
}

// isSharedRedisTier reports whether a redis resource of the given tier lives on
// the shared key-scoped pod (and therefore needs per-tenant key eviction).
func isSharedRedisTier(tier string) bool {
	return sharedRedisTiers[tier]
}

// redisEvictionScanCount is the COUNT hint for each SCAN cursor step. 100 keeps
// each round-trip cheap and bounded — it does NOT cap the total keys scanned
// (the cursor loop continues until cursor==0).
const redisEvictionScanCount = 64

// redisEvictionMaxKeys caps how many keys one tenant's eviction pass will
// inspect. A pathological tenant with millions of tiny keys must not pin the
// worker or the shared Redis event loop for an unbounded time; the next 6h
// sweep continues the drain. Mirrors the maxKeys guard in the provisioner's
// LocalBackend.StorageBytes.
const redisEvictionMaxKeys = 5000

// RedisKeyEvictor is the narrow seam the quota worker uses to evict keys from
// an over-quota shared-backend Redis tenant. The production implementation
// (directRedisEvictor) connects to the shared cluster with admin credentials;
// tests inject a miniredis-backed implementation. Keeping the seam narrow means
// the worker's unit tests need no real Redis.
type RedisKeyEvictor interface {
	// EvictTenantToCap deletes keys under exactly `{token}:*` — oldest-first —
	// until the tenant's measured memory usage is at or below limitBytes, or
	// the keyspace is exhausted, or the per-pass key cap is hit.
	//
	// It returns the number of keys deleted and the number of bytes reclaimed
	// (best-effort, summed from MEMORY USAGE before each DEL).
	//
	// It is idempotent: a tenant already under cap is a no-op (0, 0, nil).
	// It is fail-soft: a connectivity error returns the error so the caller
	// can log-and-continue — one tenant's failure must not abort the sweep.
	//
	// SAFETY: the implementation MUST assert every key begins with `{token}:`
	// before issuing DEL. It MUST NOT use KEYS (only cursor-based SCAN).
	EvictTenantToCap(ctx context.Context, token string, limitBytes int64) (keysDeleted int, bytesReclaimed int64, err error)
}

// tenantKeyPrefix returns the exact key-namespace prefix for a token. Every
// key the evictor touches must begin with this string. The provisioner's
// LocalBackend uses `fmt.Sprintf("%s:", token)` — this MUST stay in sync with
// provisioner/internal/backend/redis/local.go.
func tenantKeyPrefix(token string) string {
	return token + ":"
}

// assertKeyInTenantPrefix is the cross-tenant safety guard. It returns an error
// if `key` does not begin with the tenant's `{token}:` prefix. The evictor
// calls this immediately before every DEL; a non-nil result means the SCAN
// returned a key outside the namespace (a bug, an ACL misconfiguration, or a
// MATCH-pattern error) and the DEL is REFUSED.
//
// This function is the thing TestRedisEviction_CrossTenantIsolation guards:
// if a future change makes the scan escape the prefix, the assertion fires and
// the test fails rather than silently deleting another tenant's data.
func assertKeyInTenantPrefix(token, key string) error {
	prefix := tenantKeyPrefix(token)
	if !strings.HasPrefix(key, prefix) {
		return fmt.Errorf("redis eviction SAFETY VIOLATION: key %q is outside tenant prefix %q — DEL refused", key, prefix)
	}
	return nil
}

// directRedisEvictor implements RedisKeyEvictor against the shared Redis
// cluster using the admin Redis URL (CUSTOMER_REDIS_URL). An empty adminURL
// makes EvictTenantToCap a logged no-op (fail-open, matching the revoker's
// posture when credentials are absent).
type directRedisEvictor struct {
	adminURL string // CUSTOMER_REDIS_URL — admin Redis URL for shared cluster
}

// NewDirectRedisEvictor builds a directRedisEvictor. adminURL may be empty —
// when empty, eviction is skipped (logged WARN) and the worker still runs.
func NewDirectRedisEvictor(adminURL string) RedisKeyEvictor {
	return &directRedisEvictor{adminURL: adminURL}
}

// EvictTenantToCap implements RedisKeyEvictor against a real Redis cluster.
func (e *directRedisEvictor) EvictTenantToCap(ctx context.Context, token string, limitBytes int64) (int, int64, error) {
	if e.adminURL == "" {
		slog.Warn("quota_redis_eviction.evict: CUSTOMER_REDIS_URL not set — skipping eviction",
			"token", logsafe.Token(token))
		return 0, 0, nil
	}
	opts, err := goredis.ParseURL(e.adminURL)
	if err != nil {
		// Parse failure is a config error, not a transient one — surface it so
		// the sweep logs it, but return 0/0 so one bad URL doesn't abort.
		slog.Error("quota_redis_eviction.evict: parse CUSTOMER_REDIS_URL failed",
			"token", logsafe.Token(token), "error", err)
		return 0, 0, fmt.Errorf("EvictTenantToCap: parse admin URL: %w", err)
	}
	client := goredis.NewClient(opts)
	defer func() { _ = client.Close() }()
	return evictTenantToCap(ctx, client, token, limitBytes)
}

// redisEvictionClient is the minimal go-redis surface evictTenantToCap needs.
// Both *goredis.Client (production) and a miniredis-backed *goredis.Client
// (tests) satisfy it — declaring the seam lets the core algorithm be unit- and
// integration-tested without a network Redis.
type redisEvictionClient interface {
	Scan(ctx context.Context, cursor uint64, match string, count int64) *goredis.ScanCmd
	MemoryUsage(ctx context.Context, key string, samples ...int) *goredis.IntCmd
	ObjectIdleTime(ctx context.Context, key string) *goredis.DurationCmd
	Del(ctx context.Context, keys ...string) *goredis.IntCmd
}

// scannedKey pairs a key with the metadata used to order eviction.
type scannedKey struct {
	key      string
	idleSecs float64 // OBJECT IDLETIME — higher = colder = evict first
	bytes    int64   // MEMORY USAGE — best-effort, 0 if unavailable
}

// evictTenantToCap is the core LRU-style per-tenant eviction algorithm,
// extracted so it can be driven against a real test Redis (miniredis) without
// the URL-parsing wrapper.
//
// Algorithm:
//  1. Cursor-SCAN `{token}:*` — NEVER KEYS — collecting up to redisEvictionMaxKeys
//     keys with their MEMORY USAGE and OBJECT IDLETIME.
//  2. Sum the bytes. If total <= limitBytes → no-op (idempotent under-cap path).
//  3. Sort coldest-first (OBJECT IDLETIME desc; ties broken by key name for a
//     deterministic order even when IDLETIME is unavailable / equal).
//  4. DELETE keys one at a time, oldest-first, subtracting each key's bytes
//     from the running total, until total <= limitBytes.
//  5. Before EVERY DEL, assertKeyInTenantPrefix refuses any key that is not
//     under `{token}:` — the cross-tenant safety guard.
//
// Fail-soft: a SCAN error aborts THIS tenant only (returns the error); the
// caller logs and moves to the next tenant.
func evictTenantToCap(ctx context.Context, client redisEvictionClient, token string, limitBytes int64) (int, int64, error) {
	if token == "" {
		return 0, 0, fmt.Errorf("evictTenantToCap: empty token")
	}
	match := tenantKeyPrefix(token) + "*"

	var (
		cursor  uint64
		scanned []scannedKey
		total   int64
	)
	for {
		keys, next, err := client.Scan(ctx, cursor, match, redisEvictionScanCount).Result()
		if err != nil {
			return 0, 0, fmt.Errorf("evictTenantToCap: scan %q: %w", match, err)
		}
		for _, key := range keys {
			if len(scanned) >= redisEvictionMaxKeys {
				break
			}
			// MEMORY USAGE — best-effort. A key deleted between SCAN and here
			// returns an error / nil; treat its size as 0 and still consider it
			// for deletion (deleting an already-gone key is harmless).
			var b int64
			if mem, memErr := client.MemoryUsage(ctx, key).Result(); memErr == nil {
				b = mem
			}
			// OBJECT IDLETIME — colder keys evicted first. Unavailable → 0
			// (treated as hottest; key-name tiebreak still gives determinism).
			var idle float64
			if d, idleErr := client.ObjectIdleTime(ctx, key).Result(); idleErr == nil {
				idle = d.Seconds()
			}
			scanned = append(scanned, scannedKey{key: key, idleSecs: idle, bytes: b})
			total += b
		}
		cursor = next
		if cursor == 0 || len(scanned) >= redisEvictionMaxKeys {
			break
		}
	}

	// Idempotent under-cap path: tenant at/under cap → no-op.
	if total <= limitBytes {
		return 0, 0, nil
	}

	// Coldest-first: highest OBJECT IDLETIME evicted first. Key-name tiebreak
	// makes the order fully deterministic when IDLETIME is equal/unavailable —
	// the integration test relies on this determinism.
	sort.Slice(scanned, func(i, j int) bool {
		if scanned[i].idleSecs != scanned[j].idleSecs {
			return scanned[i].idleSecs > scanned[j].idleSecs
		}
		return scanned[i].key < scanned[j].key
	})

	var (
		deleted   int
		reclaimed int64
	)
	for _, sk := range scanned {
		if total <= limitBytes {
			break
		}
		// CROSS-TENANT SAFETY GUARD — refuse any key outside `{token}:`.
		if err := assertKeyInTenantPrefix(token, sk.key); err != nil {
			// This is a hard invariant violation, not a transient error.
			// Abort the tenant's eviction immediately rather than risk a
			// single mis-scoped DEL.
			slog.Error("quota_redis_eviction.evict: prefix assertion failed",
				"token", logsafe.Token(token), "key", sk.key, "error", err)
			return deleted, reclaimed, err
		}
		if delErr := client.Del(ctx, sk.key).Err(); delErr != nil {
			// One key's DEL failed — log and continue with the rest; the next
			// sweep retries. Do not abort: partial progress is still progress.
			slog.Warn("quota_redis_eviction.evict: DEL failed (continuing)",
				"token", logsafe.Token(token), "key", sk.key, "error", delErr)
			continue
		}
		deleted++
		reclaimed += sk.bytes
		total -= sk.bytes
	}

	return deleted, reclaimed, nil
}
