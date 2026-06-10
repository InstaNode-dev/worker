package jobs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// backup_dump_failure_test.go — error-path + branch-arm coverage for the R2
// mongo/redis dump + restore runners (backup_dump.go,
// customer_restore_runner.go). The happy paths live in backup_dump_test.go;
// this file pins:
//
//   - the fail-open "secret in argv" branches (writeMongoConfig failure →
//     --uri argv for mongodump/mongorestore; redis URL parse failure →
//     `-u <uri>` for redis-cli),
//   - the subprocess-failure returns (non-zero exit → wrapped error carrying
//     stderr),
//   - every writeMongoConfig error arm (CreateTemp / chmod / write / sync)
//     via the mongoCfg* package seams,
//   - the rediss:// → --tls flag arm.

// failMongoCfgCreateTemp swaps the mongoCfgCreateTemp seam for one that always
// fails, restoring the original on test cleanup. Forces the fail-open
// (URI-in-argv) branches of realMongoDumpRunner / realMongoRestoreRunner and
// the first error arm of writeMongoConfig.
func failMongoCfgCreateTemp(t *testing.T) {
	t.Helper()
	orig := mongoCfgCreateTemp
	mongoCfgCreateTemp = func() (*os.File, error) {
		return nil, errors.New("tmpfs exhausted (injected)")
	}
	t.Cleanup(func() { mongoCfgCreateTemp = orig })
}

// installFakeFailingBinary writes a shell-script stand-in that records argv,
// prints stderrMsg on stderr, and exits 1 — so the cmd.Run() error returns
// (which wrap the exit error + captured stderr) can be exercised without the
// real mongodump/mongorestore/redis-cli binaries.
func installFakeFailingBinary(t *testing.T, name, stderrMsg string) (dir string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake CLI script is shell-based; worker runs on linux/darwin only")
	}
	dir = t.TempDir()
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$@\" > \"" + dir + "/argv.txt\"\n" +
		"printf '%s' \"" + stderrMsg + "\" >&2\n" +
		"exit 1\n"
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake %s: %v", name, err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return dir
}

// TestRealMongoDumpRunner_FailOpenURIInArgv — when the 0600 config file can't
// be written, the runner falls open to `--uri <conn>` in argv (documented
// posture: a leaked-cmdline window beats no backup at all). mongodump must
// still get --archive and must NOT get --config.
func TestRealMongoDumpRunner_FailOpenURIInArgv(t *testing.T) {
	failMongoCfgCreateTemp(t)
	dir := installFakeBinary(t, "mongodump", "", "")
	connURL := "mongodb://admin:pw@m.host:27017/app"

	var out bytes.Buffer
	if err := (realMongoDumpRunner{}).Run(context.Background(), connURL, &out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.String() != "fakebody" {
		t.Errorf("stdout = %q; want fakebody", out.String())
	}
	argv := readArgv(t, dir)
	sawURI, sawArchive, sawConfig := false, false, false
	for i, a := range argv {
		switch a {
		case "--uri":
			sawURI = true
			if i+1 >= len(argv) || argv[i+1] != connURL {
				t.Errorf("--uri not followed by connURL; argv = %q", argv)
			}
		case "--archive":
			sawArchive = true
		case "--config":
			sawConfig = true
		}
	}
	if !sawURI || !sawArchive {
		t.Errorf("fail-open mongodump argv = %q; want --uri <conn> --archive", argv)
	}
	if sawConfig {
		t.Errorf("fail-open mongodump argv unexpectedly carries --config: %q", argv)
	}
}

// TestRealMongoDumpRunner_ExecError — a non-zero mongodump exit is wrapped
// with the captured stderr so the backup row's error_summary is actionable.
func TestRealMongoDumpRunner_ExecError(t *testing.T) {
	installFakeFailingBinary(t, "mongodump", "boom-mongodump-stderr")

	var out bytes.Buffer
	err := (realMongoDumpRunner{}).Run(context.Background(), "mongodb://h:27017/db", &out)
	if err == nil {
		t.Fatal("Run err = nil; want wrapped exec error")
	}
	if !strings.Contains(err.Error(), "mongodump:") {
		t.Errorf("err = %q; want it to name mongodump", err)
	}
	if !strings.Contains(err.Error(), "boom-mongodump-stderr") {
		t.Errorf("err = %q; want it to carry the captured stderr", err)
	}
}

// TestWriteMongoConfig_ErrorArms — each filesystem failure arm returns a
// distinctly wrapped error, an empty path, a callable no-op cleanup, and (for
// arms past CreateTemp) removes the partially written temp file.
func TestWriteMongoConfig_ErrorArms(t *testing.T) {
	// captureTempName wraps the real CreateTemp so the test can assert the
	// temp file is gone after a downstream arm fails.
	captureTempName := func(t *testing.T, name *string) {
		t.Helper()
		orig := mongoCfgCreateTemp
		mongoCfgCreateTemp = func() (*os.File, error) {
			f, err := orig()
			if f != nil {
				*name = f.Name()
			}
			return f, err
		}
		t.Cleanup(func() { mongoCfgCreateTemp = orig })
	}

	t.Run("create_temp", func(t *testing.T) {
		failMongoCfgCreateTemp(t)
		path, cleanup, err := writeMongoConfig("mongodb://h/db")
		if err == nil || !strings.Contains(err.Error(), "create mongodump config") {
			t.Fatalf("err = %v; want create mongodump config error", err)
		}
		if path != "" {
			t.Errorf("path = %q; want empty", path)
		}
		cleanup() // must be a callable no-op, never nil
	})

	t.Run("chmod", func(t *testing.T) {
		var tmpName string
		captureTempName(t, &tmpName)
		orig := mongoCfgChmod
		mongoCfgChmod = func(*os.File) error { return errors.New("chmod denied (injected)") }
		t.Cleanup(func() { mongoCfgChmod = orig })

		path, cleanup, err := writeMongoConfig("mongodb://h/db")
		if err == nil || !strings.Contains(err.Error(), "chmod mongodump config") {
			t.Fatalf("err = %v; want chmod mongodump config error", err)
		}
		if path != "" {
			t.Errorf("path = %q; want empty", path)
		}
		cleanup()
		if tmpName == "" {
			t.Fatal("seam never observed a temp file name")
		}
		if _, statErr := os.Stat(tmpName); !os.IsNotExist(statErr) {
			t.Errorf("temp file %s still present after chmod-arm failure (stat err %v); want removed", tmpName, statErr)
		}
	})

	t.Run("write", func(t *testing.T) {
		var tmpName string
		captureTempName(t, &tmpName)
		orig := mongoCfgWriteURI
		mongoCfgWriteURI = func(*os.File, string) error { return errors.New("disk full (injected)") }
		t.Cleanup(func() { mongoCfgWriteURI = orig })

		path, cleanup, err := writeMongoConfig("mongodb://h/db")
		if err == nil || !strings.Contains(err.Error(), "write mongodump config") {
			t.Fatalf("err = %v; want write mongodump config error", err)
		}
		if path != "" {
			t.Errorf("path = %q; want empty", path)
		}
		cleanup()
		if _, statErr := os.Stat(tmpName); !os.IsNotExist(statErr) {
			t.Errorf("temp file %s still present after write-arm failure (stat err %v); want removed", tmpName, statErr)
		}
	})

	t.Run("sync", func(t *testing.T) {
		var tmpName string
		captureTempName(t, &tmpName)
		orig := mongoCfgSync
		mongoCfgSync = func(*os.File) error { return errors.New("fsync io error (injected)") }
		t.Cleanup(func() { mongoCfgSync = orig })

		path, cleanup, err := writeMongoConfig("mongodb://h/db")
		if err == nil || !strings.Contains(err.Error(), "sync mongodump config") {
			t.Fatalf("err = %v; want sync mongodump config error", err)
		}
		if path != "" {
			t.Errorf("path = %q; want empty", path)
		}
		cleanup()
		if _, statErr := os.Stat(tmpName); !os.IsNotExist(statErr) {
			t.Errorf("temp file %s still present after sync-arm failure (stat err %v); want removed", tmpName, statErr)
		}
	})
}

// TestRealRedisDumpRunner_TLSFlag — a rediss:// URL must add --tls to the
// redis-cli invocation (and still keep the password out of argv).
func TestRealRedisDumpRunner_TLSFlag(t *testing.T) {
	dir := installFakeBinary(t, "redis-cli", "REDISCLI_AUTH", "auth.txt")
	const secret = "tls-redis-PW"
	connURL := "rediss://:" + secret + "@r.tls.host:6380"

	var out bytes.Buffer
	if err := (realRedisDumpRunner{}).Run(context.Background(), connURL, &out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	argv := readArgv(t, dir)
	sawTLS := false
	for _, a := range argv {
		if a == "--tls" {
			sawTLS = true
		}
		if strings.Contains(a, secret) {
			t.Errorf("argv leaks redis password: %q (full argv: %q)", a, argv)
		}
	}
	if !sawTLS {
		t.Errorf("rediss:// invocation missing --tls; argv = %q", argv)
	}
	if auth := mustReadString(t, filepath.Join(dir, "auth.txt")); auth != secret {
		t.Errorf("REDISCLI_AUTH = %q; want %q", auth, secret)
	}
}

// TestRealRedisDumpRunner_FailOpenRawURI — an unparseable/unexpected-scheme
// URL falls open to `-u <uri> --rdb -` (secret may sit in argv; documented
// posture: strictly better than no backup).
func TestRealRedisDumpRunner_FailOpenRawURI(t *testing.T) {
	dir := installFakeBinary(t, "redis-cli", "", "")
	connURL := "http://not-a-redis-scheme:80" // splitRedisURL rejects the scheme

	var out bytes.Buffer
	if err := (realRedisDumpRunner{}).Run(context.Background(), connURL, &out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.String() != "fakebody" {
		t.Errorf("stdout = %q; want fakebody", out.String())
	}
	argv := readArgv(t, dir)
	want := []string{"-u", connURL, "--rdb", "-"}
	if len(argv) != len(want) {
		t.Fatalf("fail-open argv = %q; want %q", argv, want)
	}
	for i := range want {
		if argv[i] != want[i] {
			t.Fatalf("fail-open argv = %q; want %q", argv, want)
		}
	}
}

// TestRealRedisDumpRunner_ExecError — non-zero redis-cli exit is wrapped with
// the captured stderr.
func TestRealRedisDumpRunner_ExecError(t *testing.T) {
	installFakeFailingBinary(t, "redis-cli", "boom-redis-stderr")

	var out bytes.Buffer
	err := (realRedisDumpRunner{}).Run(context.Background(), "redis://:pw@h:6379", &out)
	if err == nil {
		t.Fatal("Run err = nil; want wrapped exec error")
	}
	if !strings.Contains(err.Error(), "redis-cli --rdb") {
		t.Errorf("err = %q; want it to name redis-cli --rdb", err)
	}
	if !strings.Contains(err.Error(), "boom-redis-stderr") {
		t.Errorf("err = %q; want it to carry the captured stderr", err)
	}
}

// TestRealMongoRestoreRunner_FailOpenURIInArgv — when the config file can't be
// written, mongorestore falls open to `--uri <conn> --archive --drop`.
func TestRealMongoRestoreRunner_FailOpenURIInArgv(t *testing.T) {
	failMongoCfgCreateTemp(t)
	dir := installFakeBinary(t, "mongorestore", "", "")
	connURL := "mongodb://admin:pw@m.host:27017/app"

	if err := (realMongoRestoreRunner{}).Run(context.Background(), connURL, bytes.NewReader([]byte("archive-bytes"))); err != nil {
		t.Fatalf("Run: %v", err)
	}
	argv := readArgv(t, dir)
	sawURI, sawArchive, sawDrop, sawConfig := false, false, false, false
	for i, a := range argv {
		switch a {
		case "--uri":
			sawURI = true
			if i+1 >= len(argv) || argv[i+1] != connURL {
				t.Errorf("--uri not followed by connURL; argv = %q", argv)
			}
		case "--archive":
			sawArchive = true
		case "--drop":
			sawDrop = true
		case "--config":
			sawConfig = true
		}
	}
	if !sawURI || !sawArchive || !sawDrop {
		t.Errorf("fail-open mongorestore argv = %q; want --uri <conn> --archive --drop", argv)
	}
	if sawConfig {
		t.Errorf("fail-open mongorestore argv unexpectedly carries --config: %q", argv)
	}
}

// TestRealMongoRestoreRunner_ExecError — non-zero mongorestore exit is wrapped
// with the captured stderr so the restore row's error_summary is actionable.
func TestRealMongoRestoreRunner_ExecError(t *testing.T) {
	installFakeFailingBinary(t, "mongorestore", "boom-mongorestore-stderr")

	err := (realMongoRestoreRunner{}).Run(context.Background(), "mongodb://h:27017/db", bytes.NewReader([]byte("archive-bytes")))
	if err == nil {
		t.Fatal("Run err = nil; want wrapped exec error")
	}
	if !strings.Contains(err.Error(), "mongorestore:") {
		t.Errorf("err = %q; want it to name mongorestore", err)
	}
	if !strings.Contains(err.Error(), "boom-mongorestore-stderr") {
		t.Errorf("err = %q; want it to carry the captured stderr", err)
	}
}
