package jobs

import "testing"

// storage_minio_prefix_test.go — coverage test for the token-truncation fix
// (BUGHUNT-REPORT-2026-05-17-round2.md, recurring pattern #1) on the storage
// object-key prefix. The shared-key storage backend isolates tenants by prefix
// convention only, so two tenants sharing an 8-char token prefix could read
// each other's objects under the old token[:8] scheme. The fix: new storage
// rows persist the FULL token as provider_resource_id and the scanner uses it
// verbatim; the token[:8] derivation survives only as a legacy fallback.

// TestMinioObjectPrefix_StoredPRID_UsedVerbatim — a storage row provisioned
// after the fix carries the full-token prefix in provider_resource_id; the
// scanner must use it verbatim (just normalising the trailing slash).
func TestMinioObjectPrefix_StoredPRID_UsedVerbatim(t *testing.T) {
	const token = "abc12345deadbeefcafef00d00112233"

	// PRID without a trailing slash → slash appended.
	if got, want := minioObjectPrefix(token, token), token+"/"; got != want {
		t.Errorf("minioObjectPrefix(token, fullTokenPRID) = %q; want %q", got, want)
	}
	// PRID already slash-terminated → unchanged.
	if got, want := minioObjectPrefix(token, token+"/"), token+"/"; got != want {
		t.Errorf("minioObjectPrefix(token, fullTokenPRID/) = %q; want %q", got, want)
	}
}

// TestMinioObjectPrefix_NoCollisionAcrossSharedPrefixTokens — the security
// property: two distinct tokens that share their first 8 hex characters must
// resolve to DIFFERENT object prefixes once each carries its full-token
// provider_resource_id. Under the old token[:8] scheme they collided and
// tenant B could enumerate tenant A's objects.
func TestMinioObjectPrefix_NoCollisionAcrossSharedPrefixTokens(t *testing.T) {
	const (
		tokenA = "abc12345deadbeefcafef00d00112233"
		tokenB = "abc12345111122223333444455556666" // same 8-char prefix as tokenA
	)
	a := minioObjectPrefix(tokenA, tokenA)
	b := minioObjectPrefix(tokenB, tokenB)
	if a == b {
		t.Fatalf("full-token prefixes collided for two tokens sharing an 8-char prefix: both = %q", a)
	}
}

// TestMinioObjectPrefix_LegacyRow_FallsBackToTruncated — the LEGACY coverage
// test. A storage row provisioned BEFORE the fix has an empty
// provider_resource_id and its objects sit under the old token[:8] prefix. The
// scanner MUST reproduce that prefix so pre-fix storage resources are still
// measured. This test fails if a future change drops the legacy fallback.
func TestMinioObjectPrefix_LegacyRow_FallsBackToTruncated(t *testing.T) {
	const token = "abc12345deadbeefcafef00d00112233"

	got := minioObjectPrefix(token, "") // empty PRID == legacy row
	want := token[:legacyStorageObjectPrefixTokenLen] + "/"
	if got != want {
		t.Errorf("minioObjectPrefix(token, \"\") = %q; want legacy %q", got, want)
	}

	// A short token (< 8 chars) must not panic.
	if got := minioObjectPrefix("abc", ""); got != "abc/" {
		t.Errorf("minioObjectPrefix(shortToken, \"\") = %q; want %q", got, "abc/")
	}
	// Empty token + empty PRID → empty prefix (caller treats as error).
	if got := minioObjectPrefix("", ""); got != "" {
		t.Errorf("minioObjectPrefix(\"\", \"\") = %q; want \"\"", got)
	}
}
