package jobs

// quota_infra_redis_username_test.go — coverage test for the token-truncation
// fix (BUGHUNT-REPORT-2026-05-17-round2.md, recurring pattern #1): the worker's
// quota-suspend Redis ACL username MUST match — byte-for-byte — the username
// the provisioner/api created at provision time, otherwise
// `ACL SETUSER <user> off` targets a non-existent user and the suspension is a
// silent no-op.
//
// Resolution order under test (see redisUsernameForToken):
//  1. provider_resource_id — the canonical username stamped at provision time.
//  2. shared tier, empty PRID  → usr_<full-token>.
//  3. dedicated tier, empty PRID → LEGACY ded_<token[:8]> (a pre-fix row).
//
// This is an INTERNAL test (package jobs, not jobs_test) because
// redisUsernameForToken is unexported.
//
// If the provisioner/api ever change their username scheme again, the
// expected-format constants here will diverge from the live provisioner and
// this test must be updated in the SAME PR — that is the anti-drift gate.

import "testing"

// TestRedisUsernameForToken_StoredPRID_UsedVerbatim — the core of the
// store-at-provision fix: when the resource row carries a provider_resource_id
// it is the authoritative ACL username and must be used verbatim, never
// re-derived from the token, for BOTH shared and dedicated tiers.
func TestRedisUsernameForToken_StoredPRID_UsedVerbatim(t *testing.T) {
	const token = "abcdef0123456789deadbeefcafebabe" // 32-char token

	cases := []struct {
		tier string
		prid string
	}{
		{"anonymous", "usr_abcdef0123456789deadbeefcafebabe"},
		{"pro", "ded_abcdef0123456789deadbeefcafebabe"},   // new full-token dedicated scheme
		{"team", "ded_abcdef0123456789deadbeefcafebabe"},  // new full-token dedicated scheme
		{"pro", "ded_legacy8"},                            // even a non-derivable PRID is honoured
	}
	for _, c := range cases {
		if got := redisUsernameForToken(token, c.tier, c.prid); got != c.prid {
			t.Errorf("redisUsernameForToken(token, %q, %q) = %q; want the stored PRID verbatim",
				c.tier, c.prid, got)
		}
	}
}

// TestRedisUsernameForToken_SharedTier_UsesFullTokenUsrPrefix asserts the
// shared-backend (anonymous/free) ACL username is "usr_" + the FULL token when
// no provider_resource_id is stored (the shared backend has always returned an
// empty ProviderResourceID — see api/internal/providers/cache/redis.go).
//
// Verified against the provisioner and api at fix time:
//   - provisioner/internal/backend/redis/local.go:
//     aclUserPrefix = "usr_"; aclUsername(token) = aclUserPrefix + token
//   - api/internal/providers/cache/redis.go:
//     aclUsernamePrefix = "usr_"; aclUsername(token) = aclUsernamePrefix + token
func TestRedisUsernameForToken_SharedTier_UsesFullTokenUsrPrefix(t *testing.T) {
	const token = "abcdef0123456789deadbeefcafebabe" // 32-char token

	for _, tier := range []string{"anonymous", "free"} {
		got := redisUsernameForToken(token, tier, "") // empty PRID — shared backend stores none
		want := "usr_" + token                        // FULL token, never truncated
		if got != want {
			t.Errorf("redisUsernameForToken(%q, %q, \"\") = %q; want %q (shared backend: usr_+full-token)",
				token, tier, got, want)
		}
		// Explicit anti-regression: the OLD truncated scheme must NOT reappear.
		if got == "usr_"+token[:8] {
			t.Errorf("redisUsernameForToken(%q, %q, \"\") regressed to the truncated usr_<token[:8]> scheme",
				token, tier)
		}
	}
}

// TestRedisUsernameForToken_DedicatedLegacyRow_FallsBackToTruncated — the
// LEGACY coverage test. A dedicated-Redis resource row with an empty
// provider_resource_id was provisioned BEFORE the token-truncation fix shipped,
// so its ACL user really is under the old ded_<token[:8]> name. The fallback
// MUST reproduce that legacy form so quota-suspend of a pre-fix dedicated
// resource still resolves the right user. This test fails if a future change
// drops the legacy fallback.
func TestRedisUsernameForToken_DedicatedLegacyRow_FallsBackToTruncated(t *testing.T) {
	const token = "abcdef0123456789deadbeefcafebabe"

	for _, tier := range []string{"hobby", "hobby_plus", "pro", "growth", "team"} {
		got := redisUsernameForToken(token, tier, "") // empty PRID == legacy row
		want := "ded_" + token[:dedicatedRedisLegacyTokenLen]
		if got != want {
			t.Errorf("redisUsernameForToken(%q, %q, \"\") = %q; want %q (legacy dedicated: ded_+token[:8])",
				token, tier, got, want)
		}
	}
}

// TestRedisUsernameForToken_ShortToken_DedicatedDoesNotPanic guards the
// dedicated legacy path against a token shorter than the 8-char slice length.
func TestRedisUsernameForToken_ShortToken_DedicatedDoesNotPanic(t *testing.T) {
	const shortToken = "abc" // 3 chars — shorter than dedicatedRedisLegacyTokenLen

	if got := redisUsernameForToken(shortToken, "pro", ""); got != "ded_abc" {
		t.Errorf("redisUsernameForToken(%q, pro, \"\") = %q; want %q", shortToken, got, "ded_abc")
	}
	if got := redisUsernameForToken(shortToken, "anonymous", ""); got != "usr_abc" {
		t.Errorf("redisUsernameForToken(%q, anonymous, \"\") = %q; want %q", shortToken, got, "usr_abc")
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
		// A shared tier must produce the usr_ scheme when no PRID is stored.
		if got := redisUsernameForToken(token, tier, ""); got[:4] != "usr_" {
			t.Errorf("shared tier %q produced %q; want a usr_ prefix", tier, got)
		}
	}
	// A tier not in the shared set must produce the ded_ scheme.
	if got := redisUsernameForToken(token, "pro", ""); got[:4] != "ded_" {
		t.Errorf("non-shared tier produced %q; want a ded_ prefix", got)
	}
}
