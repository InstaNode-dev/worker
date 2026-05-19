package jobs

// token_log_scrub_test.go — T21 P1-2 regression guard (BugBash 2026-05-20).
//
// The fix wraps every `"token", <var>` slog field in `logsafe.Token(...)`
// across ~20 sites (worker/internal/jobs/{quota_infra,quota,storage,expire,
// provisioner_reconciler,entitlement_reconciler,quota_redis_eviction}.go
// plus worker/internal/provisioner/client.go). This file is a SOURCE-LEVEL
// guard: a future edit that reintroduces a raw `"token", token` slog field
// in any worker source file is caught by string-search before the bearer
// token reaches NR Logs.
//
// The check is registry-iterating (no hand-typed list of jobs) per
// CLAUDE.md rule 18: it walks every .go file under worker/internal/ and
// asserts none contains a raw token slog-field pattern.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestNoRawTokenSlogFields walks every .go file under
// worker/internal/ (excluding _test.go) and asserts none carries a raw
// `"token", <something>` slog-field pattern. The accepted shape is
// `"token", logsafe.Token(<something>)`. A future addition that re-introduces
// a raw bearer-token leak shows up here, not in prod logs.
func TestNoRawTokenSlogFields(t *testing.T) {
	// Walk the worker repo's internal/ tree from the package's cwd
	// (test runs in this package's directory).
	root := filepath.Join("..", "..", "internal")
	if _, err := os.Stat(root); err != nil {
		// Fallback when running from a different cwd (e.g. `go test ./...`
		// from repo root): walk relative to that.
		root = "internal"
	}

	// The forbidden pattern is `"token", <bare var or field>,` where the
	// variable name is NOT prefixed by `logsafe.Token(`. Allowed: any
	// occurrence inside a string literal (test fixtures), and any occurrence
	// in a _test.go file (test mocks may use bare tokens).
	//
	// Heuristic: `"token", X` where X starts with a letter and is followed
	// by `,` or `)` or whitespace, AND the line does NOT contain "logsafe."
	// anywhere.
	bareTokenRE := regexp.MustCompile(`"token",\s+[a-zA-Z][a-zA-Z0-9_.\[\]]*[,)\s]`)

	var offenders []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		body := string(src)
		// Scan line-by-line so the "logsafe." check can be applied per
		// line, not per file.
		for lineNo, line := range strings.Split(body, "\n") {
			if !bareTokenRE.MatchString(line) {
				continue
			}
			if strings.Contains(line, "logsafe.") {
				continue
			}
			// Skip lines that are clearly NOT slog field pairs (e.g.
			// "token" appearing in a comment or a string format).
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") || strings.HasPrefix(trimmed, "*") {
				continue
			}
			offenders = append(offenders, path+":"+itoa(lineNo+1)+":\t"+trimmed)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}

	if len(offenders) > 0 {
		t.Errorf("T21 P1-2 regression — %d source line(s) carry a raw `\"token\", <var>` slog field without logsafe.Token() masking. Worker logs ship to NR Logs; a raw bearer token is the same class of leak as the recipient-email leak T22 P1-1 closed.\n\nFix each: wrap the variable in `logsafe.Token(...)`.\n\nOffenders:\n%s",
			len(offenders), strings.Join(offenders, "\n"))
	}
}

// itoa is a tiny base-10 itoa to avoid pulling in strconv in this test.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	if n < 0 {
		return "-" + itoa(-n)
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
