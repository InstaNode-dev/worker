package jobs

// quota_infra_redis_username_test.go — coverage test for the P1 fix
// (BUGHUNT-REPORT-2026-05-17-round2.md): the worker's quota-suspend Redis ACL
// username MUST match — byte-for-byte — the username the provisioner and api
// create at provision time, otherwise `ACL SETUSER <user> off` targets a
// non-existent user and the suspension is a silent no-op.
//
// This is an INTERNAL test (package jobs, not jobs_test) because
// redisUsernameForToken is unexported.
//
// If the provisioner/api ever change their username scheme again, the
// expected-format constants here will diverge from the live provisioner and
// this test must be updated in the SAME PR — that is the anti-drift gate.

import "testing"

// TestRedisUsernameForToken_SharedTier_UsesFullTokenUsrPrefix asserts the
// shared-backend (anonymous/free) ACL username is "usr_" + the FULL token.
//
// Verified against the provisioner and api at fix time:
//   - provisioner/internal/backend/redis/local.go:
//     aclUserPrefix = "usr_"; aclUsername(token) = aclUserPrefix + token
//   - api/internal/providers/cache/redis.go:
//     aclUsernamePrefix = "usr_"; aclUsername(token) = aclUsernamePrefix + token
//
// The pre-fix worker returned "usr_" + token[:8] — matching neither.
func TestRedisUsernameForToken_SharedTier_UsesFullTokenUsrPrefix(t *testing.T) {
	const token = "abcdef0123456789deadbeefcafebabe" // 32-char token

	for _, tier := range []string{"anonymous", "free"} {
		got := redisUsernameForToken(token, tier)
		want := "usr_" + token // FULL token, never truncated
		if got != want {
			t.Errorf("redisUsernameForToken(%q, %q) = %q; want %q (shared backend: usr_+full-token)",
				token, tier, got, want)
		}
		// Explicit anti-regression: the OLD truncated scheme must NOT reappear.
		if got == "usr_"+token[:8] {
			t.Errorf("redisUsernameForToken(%q, %q) regressed to the truncated usr_<token[:8]> scheme",
				token, tier)
		}
	}
}

// TestRedisUsernameForToken_DedicatedTier_UsesDedShortPrefix asserts the
// dedicated-backend (paid tier) ACL username is "ded_" + token[:8].
//
// Verified against provisioner/internal/backend/redis/dedicated.go provisionLocal:
//
//	short := token; if len(short) > 8 { short = short[:8] }
//	username := fmt.Sprintf("ded_%s", short)
func TestRedisUsernameForToken_DedicatedTier_UsesDedShortPrefix(t *testing.T) {
	const token = "abcdef0123456789deadbeefcafebabe"

	for _, tier := range []string{"hobby", "hobby_plus", "pro", "growth", "team"} {
		got := redisUsernameForToken(token, tier)
		want := "ded_" + token[:8]
		if got != want {
			t.Errorf("redisUsernameForToken(%q, %q) = %q; want %q (dedicated backend: ded_+token[:8])",
				token, tier, got, want)
		}
	}
}

// TestRedisUsernameForToken_ShortToken_DedicatedDoesNotPanic guards the
// dedicated path against a token shorter than the 8-char slice length.
func TestRedisUsernameForToken_ShortToken_DedicatedDoesNotPanic(t *testing.T) {
	const shortToken = "abc" // 3 chars — shorter than dedicatedRedisACLUserTokenLen

	if got := redisUsernameForToken(shortToken, "pro"); got != "ded_abc" {
		t.Errorf("redisUsernameForToken(%q, pro) = %q; want %q", shortToken, got, "ded_abc")
	}
	if got := redisUsernameForToken(shortToken, "anonymous"); got != "usr_abc" {
		t.Errorf("redisUsernameForToken(%q, anonymous) = %q; want %q", shortToken, got, "usr_abc")
	}
}

// TestRedisUsernameForToken_TierClassifierMatchesEvictionLoop asserts the
// username-scheme split uses the SAME tier classifier (isSharedRedisTier) the
// Redis key-eviction loop uses. If the two ever diverge, a tenant could be
// evicted as "shared" but have its ACL revoked as "dedicated" (or vice versa).
func TestRedisUsernameForToken_TierClassifierMatchesEvictionLoop(t *testing.T) {
	const token = "0123456789abcdef0123456789abcdef"

	for tier := range sharedRedisTiers {
		if !isSharedRedisTier(tier) {
			t.Fatalf("sharedRedisTiers contains %q but isSharedRedisTier says false", tier)
		}
		// A shared tier must produce the usr_ scheme.
		if got := redisUsernameForToken(token, tier); got[:4] != "usr_" {
			t.Errorf("shared tier %q produced %q; want a usr_ prefix", tier, got)
		}
	}
	// A tier not in the shared set must produce the ded_ scheme.
	if got := redisUsernameForToken(token, "pro"); got[:4] != "ded_" {
		t.Errorf("non-shared tier produced %q; want a ded_ prefix", got)
	}
}
