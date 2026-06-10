// backup_dump.go — per-resource_type dump strategies for the customer-backup
// runner. Mirrors the pgDumpRunner seam already in customer_backup_runner.go
// so the runner can back up postgres/vector (pg_dump), mongodb (mongodump),
// and redis (redis-cli --rdb) through ONE pipeline (gzip → sha256 → S3) with
// ONE retention/cadence/keep-last-N policy.
//
// R2 (2026-06-10) — closing the durability gap. Before this file the backup
// ladder backed up postgres/vector ONLY; the product sells "backups +
// 1-click restore" for ALL paid resources, so Mongo + Redis had ZERO
// automated backup (worker #103 note + GAP-AUDIT-2026-06-10). This file adds
// the Mongo + Redis dump strategies; the runner dispatches on resource_type.
//
// THE GZIP CONTRACT (why this matters): the existing pg path writes a RAW
// (uncompressed) `pg_dump --format=custom` archive into the runner's gzip
// writer — the pipeline owns compression, the S3 object is `<archive>.gz`,
// and the restore path gunzips then pipes to pg_restore. To keep ONE
// pipeline + ONE object layout + ONE sha256/restore story, every dumpRunner
// here writes RAW (uncompressed) bytes too. Concretely:
//
//   - mongodump: `--archive` (NOT `--archive --gzip`). mongodump's own
//     --gzip would double-compress under the pipeline's gzip layer, bloating
//     the object and breaking the "gunzip → mongorestore --archive" restore
//     symmetry. Restore gunzips, then pipes to `mongorestore --archive`.
//   - redis-cli: `--rdb -` streams the live RDB snapshot to stdout (a single
//     uncompressed RDB blob). The pipeline gzips it to `<id>.dump.gz` exactly
//     like the pg/mongo archives.
//
// SECRET HYGIENE (mirrors SEC-WORKER FINDING-2 on the pg path): the customer
// credential must NOT sit in argv (/proc/<pid>/cmdline, `ps aux`, kubectl
// describe crash archive) for the multi-minute backup window. pg_dump uses
// PGPASSWORD env (splitPGPassword). mongodump accepts the full mongodb URI in
// `--uri` — the URI carries the password, so we pass it on stdin-equivalent…
// mongodump has no env-password knob, BUT it DOES read the URI from
// `--uri=<file>`? No: mongodump's only password-out-of-argv path is
// interactive prompt, which we can't drive. We therefore pass the URI via the
// MONGODB_URI-equivalent the tool honors: mongodump reads `--uri` from argv.
// To keep the secret out of argv we instead write the URI to a 0600 temp file
// and pass `--config=<file>` (mongodump supports a YAML config with a
// `uri:`/`password:` field). See realMongoDumpRunner for the exact mechanism.
package jobs

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
)

// Resource-type string constants — the values stored in resources.resource_type
// and echoed in resource_backups. Kept as named constants (not scattered
// literals) so the dispatch + scheduler + tests reference one source.
const (
	resourceTypePostgres = "postgres"
	resourceTypeVector   = "vector"
	resourceTypeMongoDB  = "mongodb"
	resourceTypeRedis    = "redis"
)

// mongoDumpRunner abstracts `mongodump` execution so tests can substitute a
// fake without a live Mongo. Mirrors pgDumpRunner exactly: Run writes the RAW
// (uncompressed) BSON archive to w; the runner's gzip layer compresses it.
type mongoDumpRunner interface {
	Run(ctx context.Context, connURL string, w io.Writer) error
}

// redisDumpRunner abstracts `redis-cli --rdb -` execution. Run writes the RAW
// (uncompressed) RDB snapshot to w; the runner's gzip layer compresses it.
type redisDumpRunner interface {
	Run(ctx context.Context, connURL string, w io.Writer) error
}

// realMongoDumpRunner shells out to the real `mongodump` binary, streaming a
// `--archive` (uncompressed) BSON archive to stdout.
//
// Secret hygiene: the mongodb URI carries the password in its userinfo. To
// keep it out of argv (mongodump has no PGPASSWORD-style env knob), we write a
// minimal mongodump YAML config file (mode 0600, in the pod's tmpfs) carrying
// `uri:` and pass `--config=<file>`. The file is removed on return. Fail-open
// on a temp-file error: fall back to `--uri` in argv (no regression vs a
// world with no mongo backup at all) and log nothing here — the runner's
// failure path captures any downstream error.
type realMongoDumpRunner struct{}

func (realMongoDumpRunner) Run(ctx context.Context, connURL string, w io.Writer) error {
	// Try the config-file path first so the URI (with password) stays out of
	// argv. mongodump's config file is YAML with a top-level `uri:` key.
	cfgPath, cleanup, cfgErr := writeMongoConfig(connURL)
	var cmd *exec.Cmd
	if cfgErr == nil {
		defer cleanup()
		cmd = exec.CommandContext(ctx, "mongodump",
			"--config", cfgPath,
			"--archive", // uncompressed; the runner pipeline gzips
		)
	} else {
		// Fail-open: pass the URI in argv. Less ideal (secret in cmdline) but
		// strictly better than no backup. The leak window is the dump
		// duration only, same posture the pg path documents for its parse
		// fail-open branch.
		cmd = exec.CommandContext(ctx, "mongodump",
			"--uri", connURL,
			"--archive",
		)
	}
	cmd.Stdout = w
	var stderrBuf limitedBuffer
	cmd.Stderr = &stderrBuf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mongodump: %w (stderr: %s)", err, stderrBuf.String())
	}
	return nil
}

// Test seams for writeMongoConfig's filesystem operations. The chmod / write /
// sync failure arms cannot be forced against a real, freshly created temp file
// (a healthy fd accepts all three), so each op routes through an injectable
// package var — same seam pattern as txtLookupFunc (custom_domain_reconcile.go)
// and deployNotifyResolver (deploy_notify_webhook.go). Production behavior is
// the default literal; tests swap + defer-restore.
var (
	mongoCfgCreateTemp = func() (*os.File, error) {
		return os.CreateTemp("", "instant-mongodump-*.yaml")
	}
	// 0600 — only the worker process can read the URI. CreateTemp already
	// uses 0600 on unix, but set it explicitly so the contract is loud.
	mongoCfgChmod = func(f *os.File) error { return f.Chmod(0o600) }
	// mongodump config YAML: a single `uri:` key. Quote the value so a URI
	// with YAML-special characters (e.g. a password containing ':' or '@')
	// is parsed as a single scalar.
	mongoCfgWriteURI = func(f *os.File, connURL string) error {
		_, err := fmt.Fprintf(f, "uri: %q\n", connURL)
		return err
	}
	mongoCfgSync = func(f *os.File) error { return f.Sync() }
)

// writeMongoConfig writes a mongodump YAML config carrying the connection URI
// to a 0600 temp file and returns its path plus a cleanup func. Keeps the
// password out of argv. The caller MUST invoke cleanup() to remove the file.
func writeMongoConfig(connURL string) (path string, cleanup func(), err error) {
	f, err := mongoCfgCreateTemp()
	if err != nil {
		return "", func() {}, fmt.Errorf("create mongodump config: %w", err)
	}
	cleanup = func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}
	if chmodErr := mongoCfgChmod(f); chmodErr != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("chmod mongodump config: %w", chmodErr)
	}
	if wErr := mongoCfgWriteURI(f, connURL); wErr != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("write mongodump config: %w", wErr)
	}
	if syncErr := mongoCfgSync(f); syncErr != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("sync mongodump config: %w", syncErr)
	}
	return f.Name(), cleanup, nil
}

// realRedisDumpRunner shells out to `redis-cli --rdb -`, streaming the live
// RDB snapshot to stdout. `-` for the filename means stdout (redis-cli
// 4.0+). The runner pipeline gzips the RDB blob.
//
// Secret hygiene: redis-cli accepts the password via the REDISCLI_AUTH env
// var (libredis honors it the same way PGPASSWORD works for pg_dump), so we
// split the password out of the URI and pass host/port/db/tls as flags +
// REDISCLI_AUTH on the env. The password never appears in argv. Fail-open on
// a URI parse error: pass the raw `-u <uri>` form (secret in argv) so a
// malformed-but-valid-to-redis URI still backs up.
type realRedisDumpRunner struct{}

func (realRedisDumpRunner) Run(ctx context.Context, connURL string, w io.Writer) error {
	host, port, password, useTLS, parseErr := splitRedisURL(connURL)
	var cmd *exec.Cmd
	if parseErr == nil {
		args := []string{"-h", host, "-p", port}
		if useTLS {
			args = append(args, "--tls")
		}
		args = append(args, "--rdb", "-") // "-" = stream RDB to stdout
		cmd = exec.CommandContext(ctx, "redis-cli", args...)
		if password != "" {
			// REDISCLI_AUTH keeps the password out of argv (same posture as
			// PGPASSWORD on the pg path).
			cmd.Env = append(os.Environ(), "REDISCLI_AUTH="+password)
		}
	} else {
		// Fail-open: -u <uri> (secret in argv). Strictly better than no
		// backup; the leak window is the dump duration only.
		cmd = exec.CommandContext(ctx, "redis-cli", "-u", connURL, "--rdb", "-")
	}
	cmd.Stdout = w
	var stderrBuf limitedBuffer
	cmd.Stderr = &stderrBuf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("redis-cli --rdb: %w (stderr: %s)", err, stderrBuf.String())
	}
	return nil
}

// splitRedisURL parses a redis://[:password@]host[:port][/db] (or rediss://
// for TLS) URL into its parts so redis-cli can be invoked with the password
// out of argv (via REDISCLI_AUTH). Returns an error if the URL can't be
// parsed; the caller falls back to `-u <uri>` (fail-open). Defaults: port
// 6379, db unset. rediss:// → useTLS=true.
func splitRedisURL(rawURL string) (host, port, password string, useTLS bool, err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", "", false, fmt.Errorf("parse redis url: %w", err)
	}
	switch u.Scheme {
	case "redis":
		useTLS = false
	case "rediss":
		useTLS = true
	default:
		return "", "", "", false, fmt.Errorf("unexpected redis scheme %q", u.Scheme)
	}
	host = u.Hostname()
	if host == "" {
		return "", "", "", false, fmt.Errorf("redis url missing host")
	}
	port = u.Port()
	if port == "" {
		port = "6379"
	}
	if u.User != nil {
		if pw, ok := u.User.Password(); ok {
			password = pw
		}
	}
	return host, port, password, useTLS, nil
}

// backupSupportedResourceType reports whether the customer-backup ladder knows
// how to dump the given resource_type. The scheduler's SQL filter and the
// runner's dispatch both anchor on this single predicate so the "what's
// backed up" set lives in one place (root rule 16/18 — no scattered list).
func backupSupportedResourceType(resourceType string) bool {
	switch resourceType {
	case resourceTypePostgres, resourceTypeVector, resourceTypeMongoDB, resourceTypeRedis:
		return true
	default:
		return false
	}
}

// dumpForResourceType returns the dumpRunner Run func for the given
// resource_type, or nil + a descriptive reason when the type is unsupported.
// The runner uses the returned closure so postgres/vector/mongodb/redis all
// flow through ONE pipeline (gzip → sha256 → S3). Keeping the dispatch here
// (not inline in processBackup) lets the unit test assert the mapping
// directly.
func (w *CustomerBackupRunnerWorker) dumpForResourceType(resourceType string) (func(ctx context.Context, connURL string, out io.Writer) error, string) {
	switch resourceType {
	case resourceTypePostgres, resourceTypeVector:
		return w.pgDump.Run, ""
	case resourceTypeMongoDB:
		if w.mongoDump == nil {
			return nil, "mongo dump runner not configured"
		}
		return w.mongoDump.Run, ""
	case resourceTypeRedis:
		if w.redisDump == nil {
			return nil, "redis dump runner not configured"
		}
		return w.redisDump.Run, ""
	default:
		return nil, fmt.Sprintf("unsupported resource_type %q for backup", resourceType)
	}
}
