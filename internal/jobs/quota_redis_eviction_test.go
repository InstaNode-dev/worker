package jobs_test

// quota_redis_eviction_test.go — tests for shared-backend Redis per-tenant
// key eviction (A4). Three layers:
//
//  1. Table-driven unit tests for the eviction DECISION via the exported
//     EvictTenantToCap seam against a real test Redis (miniredis): over cap,
//     under cap, exactly at cap, unlimited tier.
//  2. The CROSS-TENANT ISOLATION integration test — the critical regression
//     guard: tenant A over cap is evicted; tenant B (under cap) is 100%
//     untouched. This test FAILS if a future change makes the SCAN escape the
//     `{token}:*` prefix.
//  3. End-to-end through EnforceStorageQuotaWorker.Work() with a sqlmock DB,
//     proving the eviction loop selects the right rows and skips dedicated tiers.
//
// miniredis is a real in-process Redis (RESP server) — go-redis talks to it
// over a real socket, so SCAN / MEMORY USAGE / OBJECT IDLETIME / DEL exercise
// the genuine code paths, not a hand-rolled fake.

import (
	"context"
	"fmt"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"instant.dev/worker/internal/jobs"
)

// newTestRedis starts an in-process miniredis and returns a connected
// go-redis client plus the admin redis:// URL. t.Cleanup tears both down.
func newTestRedis(t *testing.T) (*miniredis.Miniredis, *goredis.Client, string) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(mr.Close)

	url := "redis://" + mr.Addr()
	opts, err := goredis.ParseURL(url)
	if err != nil {
		t.Fatalf("ParseURL(%q): %v", url, err)
	}
	client := goredis.NewClient(opts)
	t.Cleanup(func() { _ = client.Close() })
	return mr, client, url
}

// seedKeys writes n keys under `{token}:k{i}` each holding `valueBytes` bytes
// of payload. Returns the list of written key names.
func seedKeys(t *testing.T, mr *miniredis.Miniredis, token string, n, valueBytes int) []string {
	t.Helper()
	payload := make([]byte, valueBytes)
	for i := range payload {
		payload[i] = 'x'
	}
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%s:k%03d", token, i)
		mr.Set(k, string(payload))
		keys = append(keys, k)
	}
	return keys
}

// ── Layer 1: table-driven eviction-decision tests ────────────────────────────

func TestEvictTenantToCap_Decision(t *testing.T) {
	const mb = 1024 * 1024

	cases := []struct {
		name string
		// keyCount * valueBytes is the tenant's approximate footprint.
		keyCount   int
		valueBytes int
		limitMB    int // -1 = unlimited; converted to limitBytes by the caller
		// wantEvicted: true if at least one key must be deleted.
		wantEvicted bool
		// wantSurvivors: lower bound on keys that must remain (sanity — we only
		// ever evict down to cap, never wipe everything when over).
		wantSomeSurvive bool
	}{
		{
			name:        "over cap — must evict",
			keyCount:    20,
			valueBytes:  100 * 1024, // ~2 MB total
			limitMB:     1,          // 1 MB cap → over
			wantEvicted: true,
			// 1 MB of 100 KB keys ≈ 10 keys survive.
			wantSomeSurvive: true,
		},
		{
			name:        "under cap — no-op",
			keyCount:    3,
			valueBytes:  10 * 1024, // ~30 KB total
			limitMB:     5,         // 5 MB cap → well under
			wantEvicted: false,
		},
		{
			name:        "exactly at/under cap — no-op",
			keyCount:    1,
			valueBytes:  1024, // ~1 KB, far under any positive cap
			limitMB:     1,
			wantEvicted: false,
		},
		{
			name:        "unlimited tier — never evicts even with huge usage",
			keyCount:    50,
			valueBytes:  100 * 1024, // ~5 MB total
			limitMB:     -1,         // unlimited
			wantEvicted: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			mr, _, url := newTestRedis(t)
			token := "tok_decision"
			seeded := seedKeys(t, mr, token, tc.keyCount, tc.valueBytes)

			evictor := jobs.NewDirectRedisEvictor(url)

			// Mirror the worker's plans-driven cap resolution: -1 == unlimited.
			var limitBytes int64
			if tc.limitMB == -1 {
				// The worker skips unlimited tiers before calling the evictor;
				// to exercise the evictor directly we pass a huge cap so the
				// "unlimited == never evict" intent is verified at this layer.
				limitBytes = int64(1<<62 - 1)
			} else {
				limitBytes = int64(tc.limitMB) * mb
			}

			deleted, reclaimed, err := evictor.EvictTenantToCap(context.Background(), token, limitBytes)
			if err != nil {
				t.Fatalf("EvictTenantToCap: unexpected error: %v", err)
			}

			if tc.wantEvicted && deleted == 0 {
				t.Errorf("expected keys to be evicted, got deleted=0")
			}
			if !tc.wantEvicted && deleted != 0 {
				t.Errorf("expected no eviction (no-op), got deleted=%d reclaimed=%d", deleted, reclaimed)
			}

			remaining := len(mr.Keys())
			if tc.wantEvicted {
				if remaining >= len(seeded) {
					t.Errorf("expected fewer keys after eviction; before=%d after=%d", len(seeded), remaining)
				}
				if tc.wantSomeSurvive && remaining == 0 {
					t.Errorf("eviction wiped the entire keyspace — should only evict DOWN to cap")
				}
			} else if remaining != len(seeded) {
				t.Errorf("no-op case must not delete keys; before=%d after=%d", len(seeded), remaining)
			}
		})
	}
}

// ── Layer 2: the cross-tenant isolation regression guard ─────────────────────

// TestRedisEviction_CrossTenantIsolation is the critical regression test the
// brief calls out. It seeds:
//
//   - tenant A's `{tokenA}:*` keyspace ABOVE its cap,
//   - tenant B's `{tokenB}:*` keyspace BELOW its cap,
//
// then evicts tenant A and asserts:
//
//	(a) tenant A is brought DOWN to under cap (keys deleted),
//	(b) tenant B's keys are 100% UNTOUCHED — not one byte deleted,
//	(c) a tenant exactly at/under cap is a no-op.
//
// This test FAILS if a future change makes the eviction SCAN escape the
// `{token}:*` prefix: tenant B's key count would drop, or the prefix-safety
// assertion (assertKeyInTenantPrefix) would fire and surface as a non-nil error.
func TestRedisEviction_CrossTenantIsolation(t *testing.T) {
	const mb = 1024 * 1024
	mr, _, url := newTestRedis(t)

	tokenA := "tokAAAAAAAA" // over-cap tenant
	tokenB := "tokBBBBBBBB" // under-cap tenant — must be untouched

	// Tenant A: 30 keys × 100 KB ≈ 3 MB, cap 1 MB → over cap.
	keysA := seedKeys(t, mr, tokenA, 30, 100*1024)
	// Tenant B: 5 keys × 10 KB ≈ 50 KB, cap 5 MB → comfortably under cap.
	keysB := seedKeys(t, mr, tokenB, 5, 10*1024)

	// Snapshot tenant B's exact key set + values so we can prove byte-for-byte
	// that the eviction never touched it.
	bBefore := map[string]string{}
	for _, k := range keysB {
		v, err := mr.Get(k)
		if err != nil {
			t.Fatalf("seed Get(%q): %v", k, err)
		}
		bBefore[k] = v
	}

	evictor := jobs.NewDirectRedisEvictor(url)

	// (a) Evict tenant A — must delete keys and return no error.
	capA := int64(1) * mb
	deletedA, reclaimedA, err := evictor.EvictTenantToCap(context.Background(), tokenA, capA)
	if err != nil {
		// A non-nil error here can be the prefix-safety assertion firing —
		// that itself is a cross-tenant violation. Fail loudly.
		t.Fatalf("evict tenant A: unexpected error (possible prefix-safety violation): %v", err)
	}
	if deletedA == 0 {
		t.Fatalf("tenant A is over cap but no keys were evicted")
	}
	t.Logf("tenant A: evicted %d keys, reclaimed %d bytes", deletedA, reclaimedA)

	// Tenant A must now be at/under cap. Re-running eviction must be a no-op
	// (idempotent) — proving A was brought down to cap, not arbitrarily.
	deletedA2, _, err := evictor.EvictTenantToCap(context.Background(), tokenA, capA)
	if err != nil {
		t.Fatalf("re-evict tenant A: %v", err)
	}
	if deletedA2 != 0 {
		t.Errorf("tenant A not at cap after first eviction — second pass deleted %d more keys", deletedA2)
	}

	// Tenant A must still have SOME keys (we evict down to cap, never wipe).
	survivingA := 0
	for _, k := range keysA {
		if exists := mr.Exists(k); exists {
			survivingA++
		}
	}
	if survivingA == 0 {
		t.Errorf("tenant A keyspace was fully wiped — eviction must stop at cap, not delete everything")
	}

	// (b) THE CRITICAL ASSERTION — tenant B is 100% untouched.
	for k, want := range bBefore {
		if !mr.Exists(k) {
			t.Errorf("CROSS-TENANT VIOLATION: tenant B key %q was deleted by tenant A's eviction", k)
			continue
		}
		got, gerr := mr.Get(k)
		if gerr != nil {
			t.Errorf("CROSS-TENANT VIOLATION: tenant B key %q unreadable after A eviction: %v", k, gerr)
			continue
		}
		if got != want {
			t.Errorf("CROSS-TENANT VIOLATION: tenant B key %q value changed: want %d bytes, got %d bytes",
				k, len(want), len(got))
		}
	}
	// And tenant B's key count is exactly what we seeded.
	bRemaining := 0
	for _, k := range keysB {
		if mr.Exists(k) {
			bRemaining++
		}
	}
	if bRemaining != len(keysB) {
		t.Errorf("CROSS-TENANT VIOLATION: tenant B had %d keys before, %d after A's eviction",
			len(keysB), bRemaining)
	}

	// (c) Evicting tenant B (under cap) is a strict no-op.
	deletedB, reclaimedB, err := evictor.EvictTenantToCap(context.Background(), tokenB, int64(5)*mb)
	if err != nil {
		t.Fatalf("evict tenant B: %v", err)
	}
	if deletedB != 0 || reclaimedB != 0 {
		t.Errorf("under-cap tenant B must be a no-op; got deleted=%d reclaimed=%d", deletedB, reclaimedB)
	}
}

// TestRedisEviction_PrefixEscapeIsRefused is a direct, focused guard on the
// safety assertion: even if SCAN somehow returned a key outside the tenant's
// namespace, the DEL must be refused. We simulate this by evicting a tenant
// whose token is a PREFIX of another tenant's token — proving the match
// pattern `{token}:*` plus the assertion together never delete the longer
// tenant's keys.
func TestRedisEviction_PrefixEscapeIsRefused(t *testing.T) {
	const mb = 1024 * 1024
	mr, _, url := newTestRedis(t)

	// "tok" is a prefix of "token2" — a naive prefix scan ("tok*") would catch
	// "token2:*" keys. The correct pattern is "tok:*", which must NOT match
	// "token2:*". This test proves the namespace separator (`:`) is honoured.
	shortToken := "tok"
	longToken := "token2"

	shortKeys := seedKeys(t, mr, shortToken, 30, 100*1024) // ~3 MB, over 1 MB cap
	longKeys := seedKeys(t, mr, longToken, 5, 10*1024)     // unrelated tenant

	evictor := jobs.NewDirectRedisEvictor(url)
	deleted, _, err := evictor.EvictTenantToCap(context.Background(), shortToken, int64(1)*mb)
	if err != nil {
		t.Fatalf("evict short-token tenant: %v", err)
	}
	if deleted == 0 {
		t.Fatalf("over-cap short-token tenant: expected eviction")
	}
	_ = shortKeys

	// The longer tenant's keys must be entirely intact.
	for _, k := range longKeys {
		if !mr.Exists(k) {
			t.Errorf("PREFIX-ESCAPE VIOLATION: %q (tenant %q) deleted while evicting prefix-tenant %q",
				k, longToken, shortToken)
		}
	}
}

// ── Layer 3: end-to-end through EnforceStorageQuotaWorker.Work() ─────────────

// stubRedisEvictor records EvictTenantToCap calls so the worker-loop test can
// assert which tenants were selected for eviction (and which were skipped).
type stubRedisEvictor struct {
	calls     []string         // tokens passed to EvictTenantToCap, in order
	deletePer int              // keys "deleted" reported per call
	bytesPer  int64            // bytes "reclaimed" reported per call
	errFor    map[string]error // optional per-token error injection
}

func (s *stubRedisEvictor) EvictTenantToCap(_ context.Context, token string, _ int64) (int, int64, error) {
	s.calls = append(s.calls, token)
	if s.errFor != nil {
		if err, ok := s.errFor[token]; ok {
			return 0, 0, err
		}
	}
	return s.deletePer, s.bytesPer, nil
}

// redisRow is one row of the eviction-loop SELECT projection.
type redisRow struct {
	id, token, tier string
	storageBytes    int64
}

// expectEvictionLoopQuery wires the sqlmock expectations for a full Work() run:
// suspend loop, unsuspend loop, then the redis eviction loop returning rows.
func expectEvictionLoopRows(mock sqlmock.Sqlmock, rows []redisRow) {
	// Suspend loop (status='active', postgres/redis/mongodb) — empty.
	mock.ExpectQuery(`SELECT id, token, resource_type`).
		WithArgs("active").
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "tier", "storage_bytes"}))
	// Unsuspend loop (status='suspended') — empty.
	mock.ExpectQuery(`SELECT id, token, resource_type`).
		WithArgs("suspended").
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "tier", "storage_bytes"}))
	// Redis eviction loop (status='active', resource_type='redis').
	er := sqlmock.NewRows([]string{"id", "token", "tier", "storage_bytes"})
	for _, r := range rows {
		er.AddRow(r.id, r.token, r.tier, r.storageBytes)
	}
	mock.ExpectQuery(`SELECT id, token, tier, storage_bytes\s+FROM resources`).
		WithArgs("active").
		WillReturnRows(er)
}

// TestEnforceStorageQuotaWorker_RedisEviction_SelectsOnlySharedOverCap proves
// the Work()-level eviction loop:
//   - calls the evictor for an over-cap SHARED-tier (anonymous/free) redis row,
//   - SKIPS a dedicated (paid-tier) redis row entirely,
//   - SKIPS a shared-tier row that is under cap (no wasted scan).
func TestEnforceStorageQuotaWorker_RedisEviction_SelectsOnlySharedOverCap(t *testing.T) {
	const mb = 1024 * 1024
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := []redisRow{
		// Shared tier, over cap (5 MB cap, 9 MB used) → MUST be evicted.
		{id: "11111111-1111-1111-1111-111111111111", token: "tok_anon_over", tier: "anonymous", storageBytes: 9 * mb},
		// Dedicated/paid tier → MUST be skipped (k8s pod, reconciler-managed).
		{id: "22222222-2222-2222-2222-222222222222", token: "tok_pro", tier: "pro", storageBytes: 999 * mb},
		// Shared tier, under cap (5 MB cap, 1 MB used) → MUST be skipped.
		{id: "33333333-3333-3333-3333-333333333333", token: "tok_free_under", tier: "free", storageBytes: 1 * mb},
	}
	expectEvictionLoopRows(mock, rows)

	stub := &stubRedisEvictor{deletePer: 4, bytesPer: 4 * mb}
	// mockPlanRegistry returns limitMB for every (tier, service) — 5 MB cap.
	plans := &mockPlanRegistry{limitMB: 5}
	w := jobs.NewEnforceStorageQuotaWorkerWithEvictor(db, plans, nil, stub)

	if err := w.Work(context.Background(), fakeJob[jobs.EnforceStorageQuotaArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}

	// Only the over-cap shared-tier tenant should have been evicted.
	if len(stub.calls) != 1 {
		t.Fatalf("expected exactly 1 eviction call, got %d: %v", len(stub.calls), stub.calls)
	}
	if stub.calls[0] != "tok_anon_over" {
		t.Errorf("expected eviction of tok_anon_over, got %q", stub.calls[0])
	}
}

// TestEnforceStorageQuotaWorker_RedisEviction_UnlimitedTierSkipped proves an
// unlimited Redis cap (StorageLimitMB == -1) is never evicted even when the
// tier is a shared tier and storage_bytes is huge.
func TestEnforceStorageQuotaWorker_RedisEviction_UnlimitedTierSkipped(t *testing.T) {
	const mb = 1024 * 1024
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectEvictionLoopRows(mock, []redisRow{
		{id: "44444444-4444-4444-4444-444444444444", token: "tok_anon_unlim", tier: "anonymous", storageBytes: 9999 * mb},
	})

	stub := &stubRedisEvictor{deletePer: 1, bytesPer: mb}
	plans := &mockPlanRegistry{limitMB: -1} // unlimited
	w := jobs.NewEnforceStorageQuotaWorkerWithEvictor(db, plans, nil, stub)

	if err := w.Work(context.Background(), fakeJob[jobs.EnforceStorageQuotaArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
	if len(stub.calls) != 0 {
		t.Errorf("unlimited tier must never be evicted; got calls %v", stub.calls)
	}
}

// TestEnforceStorageQuotaWorker_RedisEviction_FailSoftPerTenant proves one
// tenant's eviction error does not abort the sweep — a second over-cap tenant
// is still evicted.
func TestEnforceStorageQuotaWorker_RedisEviction_FailSoftPerTenant(t *testing.T) {
	const mb = 1024 * 1024
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	expectEvictionLoopRows(mock, []redisRow{
		{id: "55555555-5555-5555-5555-555555555555", token: "tok_bad", tier: "anonymous", storageBytes: 9 * mb},
		{id: "66666666-6666-6666-6666-666666666666", token: "tok_good", tier: "free", storageBytes: 9 * mb},
	})

	stub := &stubRedisEvictor{
		deletePer: 3, bytesPer: 3 * mb,
		errFor: map[string]error{"tok_bad": fmt.Errorf("redis connection refused")},
	}
	plans := &mockPlanRegistry{limitMB: 5}
	w := jobs.NewEnforceStorageQuotaWorkerWithEvictor(db, plans, nil, stub)

	if err := w.Work(context.Background(), fakeJob[jobs.EnforceStorageQuotaArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
	// Both tenants must have been attempted — the bad one must not abort the sweep.
	if len(stub.calls) != 2 {
		t.Fatalf("expected both tenants attempted (fail-soft), got %d: %v", len(stub.calls), stub.calls)
	}
}

// TestEnforceStorageQuotaWorker_NilEvictor_StillRuns proves the legacy
// constructor (no evictor) keeps working — the eviction loop is a logged
// no-op and the suspend/unsuspend loops are unaffected.
func TestEnforceStorageQuotaWorker_NilEvictor_StillRuns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Suspend + unsuspend loops only — the eviction loop short-circuits before
	// issuing its SELECT when evictor is nil.
	mock.ExpectQuery(`SELECT id, token, resource_type`).
		WithArgs("active").
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "tier", "storage_bytes"}))
	mock.ExpectQuery(`SELECT id, token, resource_type`).
		WithArgs("suspended").
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "tier", "storage_bytes"}))

	plans := &mockPlanRegistry{limitMB: 5}
	w := jobs.NewEnforceStorageQuotaWorker(db, plans, nil) // legacy ctor — no evictor

	if err := w.Work(context.Background(), fakeJob[jobs.EnforceStorageQuotaArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}
