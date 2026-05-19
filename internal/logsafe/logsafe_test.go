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
