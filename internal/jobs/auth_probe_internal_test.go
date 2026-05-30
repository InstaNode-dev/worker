package jobs

// auth_probe_internal_test.go — white-box tests for unexported helpers
// in auth_probe.go that the black-box test package can't reach. Kept
// in a separate file so the rest of the test suite stays in _test.

import "testing"

// TestTruncateForLog_DirectCall exercises the truncateForLog function
// at both branches with explicit string inputs. The black-box test
// (TestTruncateForLog_Truncated) drives the function through a full
// httptest round-trip, which gives the diff-cover gate a hard time
// attributing the line — this test calls the helper directly.
func TestAuthProbe_TruncateForLog_DirectCall(t *testing.T) {
	if got := truncateForLog("short", 10); got != "short" {
		t.Errorf("short: got %q, want short", got)
	}
	if got := truncateForLog("0123456789ABCDEF", 5); got != "01234...[truncated]" {
		t.Errorf("truncate: got %q, want 01234...[truncated]", got)
	}
}
