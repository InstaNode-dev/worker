// Package logsafe provides log-safe redactions for PII / credentials.
//
// SEC-WORKER FINDING-3 + FINDING-6 (2026-05-29): driver / gRPC errors that
// propagate up to slog / audit_log / persisted error columns can embed the
// underlying connection URI verbatim. The MongoDB driver in particular
// surfaces `mongodb://user:secret@host/...` in `options.ApplyURI` parse
// errors; lib/pq's connection-refused errors include host/port but not
// usually password; redis.ParseURL can return URLs with embedded auth.
// `ScrubURL` strips the userinfo (`user:password@`) component from any
// `scheme://userinfo@host/...` substring it finds anywhere in the input.
//
// Conservative: matches only well-defined RFC 3986 syntax. Will never
// double-scrub. Idempotent. Safe to call in hot per-row loops.
//
// T21 P1-2 (BugBash 2026-05-20): the worker logs resource bearer tokens
// (inst_live_… / customer UUID tokens) raw at INFO/WARN/ERROR in ~20
// sites — `worker/internal/jobs/quota_infra.go` alone has 12. The
// existing `api/internal/middleware/log_scrubber.go` only redacts
// admin-path prefixes and does NOT redact bearer tokens. The token IS
// the credential a caller uses to authenticate against the resource;
// shipping it to NR Logs in plaintext is the same class of leak as the
// recipient-email leak that T22 P1-1 closed in the email providers.
//
// The reference for the right pattern is `expire_imminent.go:242`:
//
//	tokenPrefix := tokenStr[:min(8, len(tokenStr))]
//	slog.Info("...", "token_prefix", tokenPrefix)
//
// This package exposes `Token()` so every call site adopts ONE policy
// (first 8 chars + length indicator + "***") instead of each file
// reinventing it. A test in `logsafe_test.go` pins the exact shape so
// log dashboards / alerts can rely on a stable format.
package logsafe

import "regexp"

// urlUserinfoRE matches `scheme://userinfo@host` sequences and captures
// the scheme + host so the userinfo can be replaced with `***`. The scheme
// list is conservative: connection URIs we care about (postgres, redis,
// mongodb, amqp, http, https, s3, nats). Matching is case-insensitive on
// the scheme to absorb provider quirks (Postgres://, MongoDB+SRV://, ...).
//
// Why a regexp instead of net/url:
//  1. The input is typically an ERROR MESSAGE with the URI embedded, not a
//     standalone URI — net/url.Parse on `error: failed to connect to
//     mongodb://u:p@host` would fail.
//  2. We can apply over the whole string in one pass and catch every
//     embedded URI even when the error wraps multiple.
//
// The regexp is compiled once at package init.
var urlUserinfoRE = regexp.MustCompile(
	`(?i)([a-z][a-z0-9+.-]*://)([^/@\s]+@)`,
)

// ScrubURL returns s with every `scheme://userinfo@host` sequence rewritten
// to `scheme://***@host`. Idempotent — applying twice is a no-op. Safe on
// strings with no embedded URI (returned unchanged).
//
// Examples:
//   "mongo: failed mongodb://u:p@h/d"      → "mongo: failed mongodb://***@h/d"
//   "postgres://doadmin:abc@host:25060/db" → "postgres://***@host:25060/db"
//   "redis: dial error redis://:pw@127/0"  → "redis: dial error redis://***@127/0"
//   "nothing to scrub here"                → "nothing to scrub here"
func ScrubURL(s string) string {
	if s == "" {
		return s
	}
	return urlUserinfoRE.ReplaceAllString(s, "${1}***@")
}

// Token returns a log-safe rendering of a resource bearer token.
//
//	"inst_live_aB3xY9..."  → "inst_liv*** (len=42)"
//	"abcd"                 → "abcd*** (len=4)"   // short tokens still get an indicator
//	""                     → ""                  // empty stays empty
//
// The first 8 chars are kept so an operator can grep / disambiguate
// across log lines without recovering the secret. The length is
// preserved as metadata: "did the caller send a 64-char real token, or
// an 8-char garbage value?" remains a useful signal during incident
// response.
//
// This function is allocation-light (single string concat) and safe to
// call inside hot per-row loops.
func Token(token string) string {
	if token == "" {
		return ""
	}
	const prefixLen = 8
	prefix := token
	if len(prefix) > prefixLen {
		prefix = prefix[:prefixLen]
	}
	return prefix + "*** (len=" + itoa(len(token)) + ")"
}

// itoa is a tiny base-10 itoa to avoid pulling in strconv in this
// package. Token lengths are at most a few dozen chars, so a 4-digit
// scratch buffer is plenty.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	if n < 0 {
		// Defensive — len() can't return negative, but a caller mistake
		// shouldn't crash the redactor.
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
