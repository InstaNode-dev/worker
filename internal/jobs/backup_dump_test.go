package jobs

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// fakeMongoDump / fakeRedisDump mirror fakePgDump (customer_backup_runner_test.go):
// they record the connURL they were handed and write a fixed payload so the
// runner's gzip+sha+upload pipeline can be exercised without a live Mongo/Redis.
type fakeMongoDump struct {
	payload []byte
	err     error
	gotConn string
}

func (f *fakeMongoDump) Run(_ context.Context, connURL string, w io.Writer) error {
	f.gotConn = connURL
	if f.err != nil {
		return f.err
	}
	_, err := w.Write(f.payload)
	return err
}

type fakeRedisDump struct {
	payload []byte
	err     error
	gotConn string
}

func (f *fakeRedisDump) Run(_ context.Context, connURL string, w io.Writer) error {
	f.gotConn = connURL
	if f.err != nil {
		return f.err
	}
	_, err := w.Write(f.payload)
	return err
}

// TestBackupSupportedResourceType pins the single source of truth for "what's
// backed up". A regression that drops mongodb/redis (or adds an unsupported
// type) trips here. The scheduler SQL filter + runner dispatch both anchor on
// this predicate.
func TestBackupSupportedResourceType(t *testing.T) {
	supported := []string{"postgres", "vector", "mongodb", "redis"}
	for _, rt := range supported {
		if !backupSupportedResourceType(rt) {
			t.Errorf("backupSupportedResourceType(%q) = false; want true (R2 ladder must cover it)", rt)
		}
	}
	for _, rt := range []string{"webhook", "queue", "storage", "deploy", "", "nosql"} {
		if backupSupportedResourceType(rt) {
			t.Errorf("backupSupportedResourceType(%q) = true; want false", rt)
		}
	}
}

// TestDumpForResourceType_Dispatch — the runner picks the right dump strategy
// per resource_type, returns nil + reason for unsupported types, and nil +
// reason when a strategy is unconfigured (misconfigured boot).
func TestDumpForResourceType_Dispatch(t *testing.T) {
	pg := &fakePgDump{payload: []byte("pg")}
	mongo := &fakeMongoDump{payload: []byte("mongo")}
	redis := &fakeRedisDump{payload: []byte("redis")}
	w := &CustomerBackupRunnerWorker{pgDump: pg, mongoDump: mongo, redisDump: redis}

	for _, rt := range []string{"postgres", "vector", "mongodb", "redis"} {
		fn, reason := w.dumpForResourceType(rt)
		if fn == nil {
			t.Errorf("dumpForResourceType(%q) = nil (reason %q); want a dumper", rt, reason)
		}
		if reason != "" {
			t.Errorf("dumpForResourceType(%q) reason = %q; want empty", rt, reason)
		}
	}

	// Unsupported type → nil + reason.
	if fn, reason := w.dumpForResourceType("webhook"); fn != nil || reason == "" {
		t.Errorf("dumpForResourceType(webhook) = (%v, %q); want (nil, non-empty)", fn != nil, reason)
	}

	// Nil mongo strategy (misconfigured boot) → nil + reason, never panic.
	wNoMongo := &CustomerBackupRunnerWorker{pgDump: pg}
	if fn, reason := wNoMongo.dumpForResourceType("mongodb"); fn != nil || reason == "" {
		t.Errorf("nil mongoDump: got (%v, %q); want (nil, non-empty)", fn != nil, reason)
	}
	if fn, reason := wNoMongo.dumpForResourceType("redis"); fn != nil || reason == "" {
		t.Errorf("nil redisDump: got (%v, %q); want (nil, non-empty)", fn != nil, reason)
	}
}

// TestSplitRedisURL — host/port/password/tls extraction so redis-cli can run
// with the password out of argv (REDISCLI_AUTH).
func TestSplitRedisURL(t *testing.T) {
	cases := []struct {
		name      string
		in        string
		host      string
		port      string
		password  string
		tls       bool
		expectErr bool
	}{
		{"full", "redis://:s3cr3t@redis.host:6380/0", "redis.host", "6380", "s3cr3t", false, false},
		{"default_port", "redis://:pw@h", "h", "6379", "pw", false, false},
		{"no_password", "redis://h:6379", "h", "6379", "", false, false},
		{"tls", "rediss://:pw@h:6380", "h", "6380", "pw", true, false},
		{"user_and_pw", "redis://user:pw@h:6379", "h", "6379", "pw", false, false},
		{"bad_scheme", "http://h:80", "", "", "", false, true},
		{"missing_host", "redis://", "", "", "", false, true},
		{"unparseable", "::::not a url", "", "", "", false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			host, port, password, tls, err := splitRedisURL(c.in)
			if c.expectErr {
				if err == nil {
					t.Fatalf("splitRedisURL(%q) err = nil; want error", c.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("splitRedisURL(%q) err = %v; want nil", c.in, err)
			}
			if host != c.host || port != c.port || password != c.password || tls != c.tls {
				t.Errorf("splitRedisURL(%q) = (%q,%q,%q,%v); want (%q,%q,%q,%v)",
					c.in, host, port, password, tls, c.host, c.port, c.password, c.tls)
			}
		})
	}
}

// TestWriteMongoConfig — the mongodump config file is 0600 and carries the
// URI as a quoted YAML scalar so the password never reaches argv.
func TestWriteMongoConfig(t *testing.T) {
	const uri = "mongodb://u:p@h:27017/db?authSource=admin"
	path, cleanup, err := writeMongoConfig(uri)
	if err != nil {
		t.Fatalf("writeMongoConfig: %v", err)
	}
	defer cleanup()

	info, statErr := os.Stat(path)
	if statErr != nil {
		t.Fatalf("stat config: %v", statErr)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Errorf("config perm = %o; want 0600 (URI carries the password)", info.Mode().Perm())
	}
	body, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("read config: %v", readErr)
	}
	if !bytes.Contains(body, []byte("uri:")) {
		t.Errorf("config missing uri: key; got %q", body)
	}
	if !bytes.Contains(body, []byte(uri)) {
		t.Errorf("config missing the URI value; got %q", body)
	}

	// cleanup removes the file.
	cleanup()
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Errorf("config file still present after cleanup: %v", statErr)
	}
}

// installFakeBinary writes a shell-script stand-in for an external CLI
// (mongodump / mongorestore / redis-cli) into a TempDir, prepends it to PATH,
// and returns the dir. The script records argv to <dir>/argv.txt and the
// named env var's value to <dir>/<envfile>, writes a tiny stdout payload, and
// exits 0. Mirrors installFakePgDump (customer_backup_runner_test.go) so the
// secret-hygiene branches in backup_dump.go can be exercised without the real
// binaries.
func installFakeBinary(t *testing.T, name, envVar, envFile string) (dir string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake CLI script is shell-based; worker runs on linux/darwin only")
	}
	dir = t.TempDir()
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$@\" > \"" + dir + "/argv.txt\"\n"
	if envVar != "" {
		script += "printf '%s' \"${" + envVar + ":-}\" > \"" + dir + "/" + envFile + "\"\n"
	}
	script += "printf 'fakebody'\n" +
		"exit 0\n"
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake %s: %v", name, err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return dir
}

func readArgv(t *testing.T, dir string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "argv.txt"))
	if err != nil {
		t.Fatalf("read argv.txt: %v", err)
	}
	s := string(bytes.TrimRight(b, "\n"))
	if s == "" {
		return nil
	}
	parts := bytes.Split([]byte(s), []byte("\n"))
	out := make([]string, len(parts))
	for i, p := range parts {
		out[i] = string(p)
	}
	return out
}

// TestRealMongoDumpRunner_ConfigFileKeepsURIOutOfArgv — mongodump is invoked
// with --config <file> and --archive; the URI (with password) must NOT appear
// in argv (it lives in the 0600 config file instead).
func TestRealMongoDumpRunner_ConfigFileKeepsURIOutOfArgv(t *testing.T) {
	dir := installFakeBinary(t, "mongodump", "", "")
	const secret = "mongo-secret-PW"
	connURL := "mongodb://admin:" + secret + "@m.host:27017/app?authSource=admin"

	var out bytes.Buffer
	if err := (realMongoDumpRunner{}).Run(context.Background(), connURL, &out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.String() != "fakebody" {
		t.Errorf("stdout = %q; want fakebody", out.String())
	}
	argv := readArgv(t, dir)

	sawConfig, sawArchive := false, false
	for _, a := range argv {
		if a == "--config" {
			sawConfig = true
		}
		if a == "--archive" {
			sawArchive = true
		}
		if bytes.Contains([]byte(a), []byte(secret)) {
			t.Errorf("argv leaks mongo password: %q (full argv: %q)", a, argv)
		}
		// The runner must NOT pass --gzip (the pipeline gzips; double-gzip
		// would break the gunzip→mongorestore restore symmetry).
		if a == "--gzip" {
			t.Errorf("mongodump invoked with --gzip; the pipeline owns compression (double-gzip): %q", argv)
		}
	}
	if !sawConfig {
		t.Errorf("mongodump argv missing --config (URI must be out of argv): %q", argv)
	}
	if !sawArchive {
		t.Errorf("mongodump argv missing --archive: %q", argv)
	}
}

// TestRealRedisDumpRunner_PasswordViaEnv — redis-cli is invoked with
// -h/-p/--rdb - and the password is passed via REDISCLI_AUTH env, NOT argv.
func TestRealRedisDumpRunner_PasswordViaEnv(t *testing.T) {
	dir := installFakeBinary(t, "redis-cli", "REDISCLI_AUTH", "auth.txt")
	const secret = "redis-secret-PW"
	connURL := "redis://:" + secret + "@r.host:6380/0"

	var out bytes.Buffer
	if err := (realRedisDumpRunner{}).Run(context.Background(), connURL, &out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.String() != "fakebody" {
		t.Errorf("stdout = %q; want fakebody", out.String())
	}

	argv := readArgv(t, dir)
	auth := mustReadString(t, filepath.Join(dir, "auth.txt"))
	if auth != secret {
		t.Errorf("REDISCLI_AUTH = %q; want %q", auth, secret)
	}
	for _, a := range argv {
		if bytes.Contains([]byte(a), []byte(secret)) {
			t.Errorf("argv leaks redis password: %q (full argv: %q)", a, argv)
		}
	}
	// Must carry the host/port + the `--rdb -` stream-to-stdout flag.
	wantFlags := map[string]bool{"-h": false, "r.host": false, "-p": false, "6380": false, "--rdb": false, "-": false}
	for _, a := range argv {
		if _, ok := wantFlags[a]; ok {
			wantFlags[a] = true
		}
	}
	for flag, seen := range wantFlags {
		if !seen {
			t.Errorf("redis-cli argv missing %q; got %q", flag, argv)
		}
	}
}

// TestRealMongoRestoreRunner_DropAndArchive — mongorestore must run with
// --archive + --drop (the rewind semantic) and keep the URI out of argv.
func TestRealMongoRestoreRunner_DropAndArchive(t *testing.T) {
	dir := installFakeBinary(t, "mongorestore", "", "")
	const secret = "mongo-restore-PW"
	connURL := "mongodb://admin:" + secret + "@m.host:27017/app"

	if err := (realMongoRestoreRunner{}).Run(context.Background(), connURL, bytes.NewReader([]byte("archive-bytes"))); err != nil {
		t.Fatalf("Run: %v", err)
	}
	argv := readArgv(t, dir)
	sawDrop, sawArchive, sawConfig := false, false, false
	for _, a := range argv {
		switch a {
		case "--drop":
			sawDrop = true
		case "--archive":
			sawArchive = true
		case "--config":
			sawConfig = true
		}
		if bytes.Contains([]byte(a), []byte(secret)) {
			t.Errorf("mongorestore argv leaks password: %q (full argv: %q)", a, argv)
		}
	}
	if !sawDrop || !sawArchive || !sawConfig {
		t.Errorf("mongorestore argv = %q; want --config + --archive + --drop", argv)
	}
}
