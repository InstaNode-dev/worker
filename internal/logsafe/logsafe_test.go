package logsafe

import (
	"strings"
	"testing"
)

// TestToken_BasicShapes pins the redaction algorithm — the worker's
// dashboards / alerts may depend on the literal format (e.g. an alert
// query that matches `token_prefix` LIKE 'inst_liv***%'). A future
// edit that changes this format is a contract change.
func TestToken_BasicShapes(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		// Empty stays empty.
		{"", ""},
		// 1-7 char tokens — the entire string is kept as the "prefix",
		// then the *** + length indicator. (Cap evasion isn't a real
		// concern here — these are not real production tokens.)
		{"a", "a*** (len=1)"},
		{"abcdefg", "abcdefg*** (len=7)"},
		// 8-char token — exactly the prefix length.
		{"abcdefgh", "abcdefgh*** (len=8)"},
		// 9+ chars — only first 8 kept.
		{"abcdefghi", "abcdefgh*** (len=9)"},
		// Real production shape — `inst_live_<random>`.
		{"inst_live_aB3xY9zQwErTpLmNvCxZsDfGhJkLpOiUyTrEwQ", "inst_liv*** (len=48)"},
		// Customer UUID-shaped token.
		{"550e8400-e29b-41d4-a716-446655440000", "550e8400*** (len=36)"},
	}
	for _, c := range cases {
		got := Token(c.in)
		if got != c.want {
			t.Errorf("Token(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

// TestItoa covers the internal base-10 itoa helper directly, including
// the n==0 and (defensive) n<0 branches that Token() can never reach via
// a real len() argument. Pinning these keeps the helper safe to reuse.
func TestItoa(t *testing.T) {
	cases := []struct {
		in   int
		want string
	}{
		{0, "0"},
		{1, "1"},
		{7, "7"},
		{42, "42"},
		{1000, "1000"},
		// Defensive negative path — len() can't produce this, but the
		// helper must not crash and must render a leading minus sign.
		{-1, "-1"},
		{-42, "-42"},
	}
	for _, c := range cases {
		if got := itoa(c.in); got != c.want {
			t.Errorf("itoa(%d) = %q; want %q", c.in, got, c.want)
		}
	}
}

// TestToken_NoLeakBeyondPrefix is the substantive regression guard:
// for any token longer than 8 chars, the redacted output must NOT
// contain any character from position [8:] of the original. Catches
// a future "first N chars" bug where N was bumped above the safe
// cap without removing the suffix.
func TestToken_NoLeakBeyondPrefix(t *testing.T) {
	const secretSuffix = "ZZZ_THIS_MUST_NEVER_APPEAR_IN_LOGS_ZZZ"
	token := "inst_live_" + secretSuffix
	out := Token(token)
	if strings.Contains(out, secretSuffix) {
		t.Errorf("Token(%q) = %q — leaked the secret suffix %q. The redactor must keep only the first 8 chars of the token.",
			token, out, secretSuffix)
	}
	if !strings.HasPrefix(out, "inst_liv") {
		t.Errorf("Token(%q) = %q — expected prefix `inst_liv`", token, out)
	}
}

// TestScrubURL pins the credential-redaction algorithm for the SEC-WORKER
// FINDING-3 / FINDING-6 fix: any `scheme://userinfo@host` embedded in an
// error message or persisted column gets its userinfo stripped to `***`.
//
// This is THE regression guard against secrets leaking into NR Logs, the
// dashboard degraded-banner reason, audit_log.metadata, and
// pending_propagations.last_error.
func TestScrubURL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"no_url", "nothing to scrub", "nothing to scrub"},
		{"postgres_userpass", "postgres://doadmin:abc123@host:25060/db?sslmode=require",
			"postgres://***@host:25060/db?sslmode=require"},
		{"mongodb_userpass_in_error",
			"mongo: ping: error connecting mongodb://u:secret@cluster.svc/d",
			"mongo: ping: error connecting mongodb://***@cluster.svc/d"},
		{"redis_password_only", "redis: dial error redis://:pw@127.0.0.1:6379/0",
			"redis: dial error redis://***@127.0.0.1:6379/0"},
		{"mongodb_srv", "mongo: connect mongodb+srv://u:p@cluster.mongodb.net/?retryWrites=true",
			"mongo: connect mongodb+srv://***@cluster.mongodb.net/?retryWrites=true"},
		{"https_url_without_userinfo", "GET https://api.razorpay.com/v1/subscriptions/sub_xx",
			"GET https://api.razorpay.com/v1/subscriptions/sub_xx"},
		{"two_urls_in_one_error",
			"failed: postgres://a:b@h1/d; retry: postgres://a:b@h2/d",
			"failed: postgres://***@h1/d; retry: postgres://***@h2/d"},
		{"case_insensitive_scheme", "Postgres://Doadmin:P@host/db",
			"Postgres://***@host/db"},
		{"already_scrubbed_idempotent", "postgres://***@host/db",
			"postgres://***@host/db"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ScrubURL(c.in)
			if got != c.want {
				t.Errorf("ScrubURL(%q)\n  got:  %q\n  want: %q", c.in, got, c.want)
			}
		})
	}
}

// TestScrubURL_NoLeakOfSecretSuffix is the substantive regression guard:
// after scrubbing, the literal password substring must not appear anywhere
// in the output. Catches a regex regression that over-scopes the userinfo
// group and accidentally preserves the trailing password chars.
func TestScrubURL_NoLeakOfSecretSuffix(t *testing.T) {
	const secret = "ZZZ_PASSWORD_MUST_NEVER_APPEAR_ZZZ"
	in := "connection failed: postgres://admin:" + secret + "@host:5432/db"
	out := ScrubURL(in)
	if strings.Contains(out, secret) {
		t.Errorf("ScrubURL leaked the password substring %q in output %q", secret, out)
	}
	if !strings.Contains(out, "postgres://***@host:5432/db") {
		t.Errorf("ScrubURL output missing expected scrubbed shape: %q", out)
	}
}
