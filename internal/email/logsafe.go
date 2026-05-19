package email

// logsafe.go — privacy-preserving helpers for slog lines in the worker
// email providers.
//
// MR-P1-46 / T22 P1-1 (BugBash 2026-05-20): the L1 PII-masking fix
// shipped in api `4078ca3` (commit "email.go:maskEmail") masked recipient
// addresses in api-side slog lines but did NOT touch the worker email
// providers (brevo_provider.go, ses_provider.go) — which are the
// higher-volume emitters because every audit-driven lifecycle / quota /
// expiry email flows through them. Prod logs (worker pod
// `instant-worker-7f6d77699d-tt7gr`, commit_id=7169493) showed full
// recipient addresses in cleartext at INFO/WARN/ERROR on every send —
// PII shipped to New Relic. This file closes that gap with the same
// algorithm the api uses.

import "strings"

// maskEmail returns a privacy-preserving rendering of a recipient
// address for slog lines. Mirrors api/internal/email/email.go:maskEmail
// and api/internal/models/MaskEmail.
//
//	"alice@example.com"            → "a***@example.com"
//	"a@example.com"                → "a@example.com" (1-char local kept)
//	"" / "no-at-sign" / "@only"    → returned unchanged (defensive)
//
// CLAUDE.md feedback memory: "no PII/tokens/secrets in any log line".
func maskEmail(addr string) string {
	at := strings.LastIndex(addr, "@")
	if at <= 0 {
		return addr
	}
	local := addr[:at]
	domain := addr[at:]
	if len(local) == 1 {
		return local + domain
	}
	return local[:1] + "***" + domain
}
