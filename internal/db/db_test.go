package db

// db_test.go — coverage for ConnectPostgres, ConnectRedis, the typed
// error wrappers, and the envInt env-parsing helper. The two Connect
// functions panic on failure (the worker has nothing to do without
// its data layer), so the tests use recover() to assert the panic
// payload's shape rather than letting it kill the test binary.

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// requirePanic runs fn and returns the recovered panic value, failing
// the test if fn does not panic. Centralises the recover() boilerplate.
func requirePanic(t *testing.T, fn func()) (got any) {
	t.Helper()
	defer func() { got = recover() }()
	fn()
	t.Fatal("expected panic, got nil")
	return nil
}

// TestEnvInt_FallsBackOnBadValues — pairs with envDuration's coverage in
// pool_metrics_test.go. Bad values fall back to the default; the worker
// MUST NOT refuse to boot on a typo'd pool-size env var.
func TestEnvInt_FallsBackOnBadValues(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want int
	}{
		{"", 9},
		{"not-an-int", 9},
		{"-1", 9},   // negative → fall back
		{"0", 9},    // zero → fall back (would disable pool)
		{"5", 5},    // happy path
		{"abc12", 9}, // partial-typo → fall back
	} {
		t.Setenv("__WORKER_PG_TEST_ENVINT", tc.raw)
		got := envInt("__WORKER_PG_TEST_ENVINT", 9)
		if got != tc.want {
			t.Errorf("envInt(%q) = %d; want %d", tc.raw, got, tc.want)
		}
	}
}

// TestErrDBConnect_ShapeAndUnwrap pins the typed error contract — the
// caller's errors.As / errors.Unwrap chain must recover the cause.
func TestErrDBConnect_ShapeAndUnwrap(t *testing.T) {
	cause := errors.New("upstream-down")
	e := &ErrDBConnect{Cause: cause}
	if e.Error() == "" {
		t.Fatal("Error() returned empty string")
	}
	if got := e.Unwrap(); got != cause {
		t.Errorf("Unwrap() = %v; want %v", got, cause)
	}
	// errors.As must recover the typed error from a wrapped panic value.
	var typed *ErrDBConnect
	if !errors.As(e, &typed) {
		t.Errorf("errors.As did not recover *ErrDBConnect from %T", e)
	}
}

// TestErrRedisConnect_ShapeAndUnwrap mirrors the above for the Redis arm.
func TestErrRedisConnect_ShapeAndUnwrap(t *testing.T) {
	cause := errors.New("redis-down")
	e := &ErrRedisConnect{Cause: cause}
	if e.Error() == "" {
		t.Fatal("Error() returned empty string")
	}
	if got := e.Unwrap(); got != cause {
		t.Errorf("Unwrap() = %v; want %v", got, cause)
	}
	var typed *ErrRedisConnect
	if !errors.As(e, &typed) {
		t.Errorf("errors.As did not recover *ErrRedisConnect from %T", e)
	}
}

// TestConnectPostgres_Happy — when DATABASE_URL points at a reachable
// Postgres, ConnectPostgres returns a *sql.DB that pings cleanly. We
// rely on the CI-side test postgres (TEST_DATABASE_URL / DATABASE_URL).
// Skipped when neither is set so the test stays runnable on a developer
// laptop without an upstream.
func TestConnectPostgres_Happy(t *testing.T) {
	dsn := testPGDSN(t)

	// Set env knobs to non-default values so we also exercise the env-tunable
	// branches (envInt + envDuration return non-default values).
	t.Setenv("WORKER_PG_MAX_OPEN_CONNS", "4")
	t.Setenv("WORKER_PG_MAX_IDLE_CONNS", "2")
	t.Setenv("WORKER_PG_CONN_MAX_LIFETIME", "30s")
	t.Setenv("WORKER_PG_CONN_MAX_IDLE_TIME", "20s")

	db := ConnectPostgres(dsn)
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Errorf("Ping after ConnectPostgres: %v", err)
	}
	stats := db.Stats()
	if stats.MaxOpenConnections != 4 {
		t.Errorf("MaxOpenConnections = %d; want 4 (env override should win)", stats.MaxOpenConnections)
	}
}

// TestConnectPostgres_BadURL_Panics — sql.Open accepts most malformed
// DSNs but Ping fails. Either way, ConnectPostgres panics with
// *ErrDBConnect. The panic recovery path is critical — it's how the
// worker's main.go decides to exit(1) instead of looping forever.
func TestConnectPostgres_BadURL_Panics(t *testing.T) {
	// Use a syntactically valid but unreachable target so the panic
	// originates from Ping, not Open. A short-lived 1s context-bounded
	// Ping inside ConnectPostgres ensures the test finishes quickly.
	got := requirePanic(t, func() {
		ConnectPostgres("postgres://nobody:nobody@127.0.0.1:1/none?sslmode=disable&connect_timeout=1")
	})
	var typed *ErrDBConnect
	if !errors.As(got.(error), &typed) {
		t.Fatalf("panic was %T (%v); want *ErrDBConnect", got, got)
	}
	if typed.Cause == nil {
		t.Errorf("Cause = nil; want underlying ping error")
	}
}

// TestConnectPostgres_MalformedDSN_Panics — exercises the sql.Open
// failure branch. lib/pq returns an error for DSNs with bad URL syntax
// (e.g. an unparseable port).
func TestConnectPostgres_MalformedDSN_Panics(t *testing.T) {
	got := requirePanic(t, func() {
		// :badport is rejected at parse time by lib/pq.
		ConnectPostgres("postgres://user:pass@host:badport/db")
	})
	if _, ok := got.(error); !ok {
		t.Fatalf("panic payload = %T; want error", got)
	}
}

// TestConnectRedis_Happy — points at a reachable Redis (the CI test
// redis container). Same skip-if-unset behaviour as the Postgres test.
func TestConnectRedis_Happy(t *testing.T) {
	url := testRedisURL(t)
	rdb := ConnectRedis(url)
	defer rdb.Close()
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Errorf("Ping after ConnectRedis: %v", err)
	}
}

// TestConnectRedis_BadURL_Panics — malformed Redis URL is rejected by
// redis.ParseURL with an *ErrRedisConnect panic.
func TestConnectRedis_BadURL_Panics(t *testing.T) {
	got := requirePanic(t, func() {
		ConnectRedis("://not-a-valid-url")
	})
	var typed *ErrRedisConnect
	if !errors.As(got.(error), &typed) {
		t.Fatalf("panic was %T (%v); want *ErrRedisConnect", got, got)
	}
	if typed.Cause == nil {
		t.Error("Cause = nil; want underlying parse error")
	}
}

// TestConnectRedis_Unreachable_Panics — well-formed URL pointing at a
// closed port: ParseURL succeeds, Ping fails. The 1s connect timeout in
// redis.ParseURL won't apply (no such option in the URL form), but the
// ping itself completes quickly when the kernel returns ECONNREFUSED.
func TestConnectRedis_Unreachable_Panics(t *testing.T) {
	got := requirePanic(t, func() {
		ConnectRedis("redis://127.0.0.1:1")
	})
	var typed *ErrRedisConnect
	if !errors.As(got.(error), &typed) {
		t.Fatalf("panic was %T (%v); want *ErrRedisConnect", got, got)
	}
}

// TestEnvIntAndDuration_DefaultsWhenUnset — straightforward "unset env"
// path. Pairs with the table in TestEnvInt_FallsBackOnBadValues for the
// "set but bogus" path.
func TestEnvIntAndDuration_DefaultsWhenUnset(t *testing.T) {
	// Don't set the env var at all (t.Setenv with empty would still set it,
	// so use a guaranteed-unique key that won't collide with anything).
	if got := envInt("__never_set_int_env__", 11); got != 11 {
		t.Errorf("envInt unset = %d; want default 11", got)
	}
	if got := envDuration("__never_set_dur_env__", 2*time.Second); got != 2*time.Second {
		t.Errorf("envDuration unset = %v; want default 2s", got)
	}
}

// testPGDSN returns the test Postgres DSN from env, skipping if absent.
func testPGDSN(t *testing.T) string {
	t.Helper()
	// Same precedence the rest of the worker repo follows: TEST_DATABASE_URL
	// wins over DATABASE_URL, both unset means skip.
	for _, k := range []string{"TEST_DATABASE_URL", "DATABASE_URL"} {
		if v := getenv(k); v != "" {
			return v
		}
	}
	t.Skip("TEST_DATABASE_URL / DATABASE_URL unset — skipping pg happy-path test")
	return ""
}

// testRedisURL returns the test Redis URL from env, skipping if absent.
func testRedisURL(t *testing.T) string {
	t.Helper()
	for _, k := range []string{"TEST_REDIS_URL", "REDIS_URL"} {
		if v := getenv(k); v != "" {
			return v
		}
	}
	t.Skip("TEST_REDIS_URL / REDIS_URL unset — skipping redis happy-path test")
	return ""
}

// getenv is a tiny indirection so the test code reads the live env
// without each helper importing os directly.
func getenv(key string) string {
	return os.Getenv(key)
}
