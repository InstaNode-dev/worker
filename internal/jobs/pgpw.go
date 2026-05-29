package jobs

// pgpw.go — small helper used by every pg_dump call-site to pass the
// Postgres password out-of-band (via PGPASSWORD env) instead of inside the
// process-args connection URI.
//
// SEC-WORKER FINDING-1 + FINDING-2 (2026-05-29):
//   - platform_db_backup.go ran `pg_dump <DSN-with-password>` for the
//     daily 02:00 UTC platform-DB backup. The DSN with embedded
//     doadmin password was visible in `ps aux` / /proc/<pid>/cmdline for
//     the entire multi-minute backup window — any sidecar / debug shell /
//     log-shipper / `kubectl describe` crash dump could read it.
//   - customer_backup_runner.go ran `pg_dump -d <DSN-with-password>` for
//     every per-customer hourly Pro/Team backup. Same surface, but the
//     leaked secret is the customer's DB password (decrypted from
//     resources.connection_url AES-GCM ciphertext).
//
// libpq honors PGPASSWORD via env. We strip the password from the URI
// userinfo and set PGPASSWORD on the cmd.Env before exec — the password
// no longer appears in cmdline.
//
// Conservative: if parsing fails, returns the original URL and "" — the
// caller falls back to old behavior (no regression). Callers that want
// hard-fail on parse can check the returned error.

import (
	"fmt"
	"net/url"
)

// splitPGPassword returns the Postgres URL with the userinfo password
// removed, plus the extracted password. If u has no password (e.g. SSL
// cert auth) the returned password is "" and the URL is returned
// unchanged. If u cannot be parsed as a URL, the input is returned
// unchanged along with the parse error.
//
// Examples:
//
//	"postgres://u:p@h:5432/db?sslmode=require"
//	  → ("postgres://u@h:5432/db?sslmode=require", "p", nil)
//
//	"postgres://u@h/db"        → ("postgres://u@h/db", "", nil)
//	"postgres://h/db"          → ("postgres://h/db", "", nil)
func splitPGPassword(rawURL string) (string, string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL, "", fmt.Errorf("parse pg url: %w", err)
	}
	if u.User == nil {
		return rawURL, "", nil
	}
	pw, hasPW := u.User.Password()
	if !hasPW {
		return rawURL, "", nil
	}
	// Reconstruct userinfo with only the username.
	u.User = url.User(u.User.Username())
	return u.String(), pw, nil
}
