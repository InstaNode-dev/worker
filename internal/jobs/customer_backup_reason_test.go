package jobs

// customer_backup_reason_test.go — unit tests for the backup-failure reason
// classifier and the customer-safe message sanitizer (2026-06-03 backup
// observability fix). These pin two invariants:
//   1. A credential/auth failure is classified "auth" (SLA-relevant, paged)
//      and distinguished from a transient "dump"/"upload" failure.
//   2. The customer-facing summary NEVER leaks internal detail (host/IP,
//      per-tenant role name, raw pg_dump stderr) — the incident that
//      triggered this fix forwarded exactly that to a user.

import (
	"errors"
	"strings"
	"testing"
)

func TestBackupFailReason(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		// The exact prod stderr that triggered this fix.
		{"prod password auth", errors.New(`pg_dump: error: connection to server at "pg.instanode.dev" (152.42.154.144), port 5432 failed: FATAL: password authentication failed for user "usr_96edf9eed8ed42929036b63298ec5b2b"`), "auth"},
		{"generic auth failed", errors.New("authentication failed"), "auth"},
		{"no password supplied", errors.New("pg_dump: error: no password supplied"), "auth"},
		{"role does not exist", errors.New(`FATAL: role "usr_abc" does not exist`), "auth"},
		{"permission denied", errors.New("permission denied for table users"), "auth"},
		{"server unavailable is transient", errors.New("pg_dump: server unavailable"), "dump"},
		{"connection refused is transient", errors.New("connection refused"), "dump"},
		{"nil err defaults to dump", nil, "dump"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := backupFailReason(c.err); got != c.want {
				t.Fatalf("backupFailReason(%q) = %q, want %q", c.err, got, c.want)
			}
		})
	}
}

func TestSanitizedBackupFailure_NeverLeaksInternals(t *testing.T) {
	// Tokens from the real leaked error that must NEVER appear in a
	// customer-facing summary, regardless of reason.
	leaks := []string{
		"pg.instanode.dev", "152.42.154.144", "5432",
		"usr_96edf9eed8ed42929036b63298ec5b2b", "pg_dump", "password", "FATAL",
	}
	for _, reason := range []string{"auth", "decrypt", "config", "dump", "upload", "other", ""} {
		msg := sanitizedBackupFailure(reason)
		if strings.TrimSpace(msg) == "" {
			t.Fatalf("sanitizedBackupFailure(%q) is empty", reason)
		}
		low := strings.ToLower(msg)
		for _, leak := range leaks {
			if strings.Contains(low, strings.ToLower(leak)) {
				t.Errorf("sanitizedBackupFailure(%q) leaks %q: %s", reason, leak, msg)
			}
		}
	}
}

func TestSanitizedBackupFailure_PerReasonCopy(t *testing.T) {
	// auth → reassuring + "no action needed"; transient → "try again".
	if !strings.Contains(strings.ToLower(sanitizedBackupFailure("auth")), "no action") {
		t.Error("auth message should reassure the user no action is needed")
	}
	for _, r := range []string{"dump", "upload"} {
		if !strings.Contains(strings.ToLower(sanitizedBackupFailure(r)), "try again") {
			t.Errorf("%q message should say we'll retry", r)
		}
	}
	// decrypt/config are internal config issues — surfaced as such, no leak.
	if !strings.Contains(strings.ToLower(sanitizedBackupFailure("config")), "internal configuration") {
		t.Error("config message should name an internal configuration issue")
	}
}
