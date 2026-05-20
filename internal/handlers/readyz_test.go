package handlers_test

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"instant.dev/common/readiness"
	"instant.dev/worker/internal/config"
	"instant.dev/worker/internal/handlers"
)

type fakeRiver struct{ started bool }

func (f fakeRiver) Started() bool { return f.started }

// TestReadyz_AllOK — happy path: DB ping ok, miniredis answers PING,
// River reports started, no Brevo key → brevo check absent. Expect
// 200 + overall=ok.
func TestReadyz_AllOK(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectPing()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	h := handlers.NewReadyzHandler(&config.Config{}, db, rdb, fakeRiver{started: true})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.Get(rr, req)

	require.Equal(t, 200, rr.Code)
	var got readiness.Response
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
	require.Equal(t, readiness.StatusOK, got.Overall)
	require.Equal(t, "instant-worker", got.Service)

	// Pin the registered names — adding a non-critical check
	// shouldn't silently disappear if it returns ok.
	names := map[string]bool{}
	for _, c := range got.Checks {
		names[c.Name] = true
	}
	require.True(t, names["platform_db"])
	require.True(t, names["redis"])
	require.True(t, names["river"])
	require.False(t, names["brevo"], "brevo should not register without BREVO_API_KEY")
}

// TestReadyz_RiverNotStarted_Is503 — River is the worker's main job —
// if it didn't start, the pod is idle. Critical → 503 → pulled out.
func TestReadyz_RiverNotStarted_Is503(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectPing()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	h := handlers.NewReadyzHandler(&config.Config{}, db, rdb, fakeRiver{started: false})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.Get(rr, req)

	require.Equal(t, 503, rr.Code,
		"River not started must return 503 — pod has no main loop, pull from rotation")
	var got readiness.Response
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
	require.Equal(t, readiness.StatusFailed, got.Overall)
}

// TestReadyz_BrevoConfigured_AppearsInChecks — when BREVO_API_KEY is
// set the brevo check is registered. We don't actually probe Brevo in
// this test (no network); we just verify the check is wired so a
// future deploy that strips Brevo from config doesn't silently lose
// the check.
func TestReadyz_BrevoConfigured_AppearsInChecks(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectPing()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	cfg := &config.Config{BrevoAPIKey: "xkeysib-test"}
	h := handlers.NewReadyzHandler(cfg, db, rdb, fakeRiver{started: true})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.Get(rr, req)

	body, _ := io.ReadAll(rr.Body)
	var got readiness.Response
	require.NoError(t, json.Unmarshal(body, &got))

	found := false
	for _, c := range got.Checks {
		if c.Name == "brevo" {
			found = true
			break
		}
	}
	require.True(t, found, "brevo check must register when BREVO_API_KEY is set")
}

// TestReadyz_NoLeakedSecrets — like the api test, ensure the response
// body doesn't include the Brevo api-key. The worker is reachable
// in-cluster only but the principle holds.
func TestReadyz_NoLeakedSecrets(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectPing()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	const apiKey = "xkeysib-LEAKED-WORKER-KEY"
	cfg := &config.Config{BrevoAPIKey: apiKey}
	h := handlers.NewReadyzHandler(cfg, db, rdb, fakeRiver{started: true})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.Get(rr, req)

	body, _ := io.ReadAll(rr.Body)
	require.False(t, strings.Contains(string(body), apiKey),
		"Brevo api-key MUST NOT appear in worker /readyz body")
}

// TestReadyz_NilRiverIsFailed — defensive: a misconfigured boot that
// passes nil for the River provider surfaces as failed (not panic).
func TestReadyz_NilRiverIsFailed(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectPing()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	h := handlers.NewReadyzHandler(&config.Config{}, db, rdb, nil)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.Get(rr, req)

	require.Equal(t, 503, rr.Code)
}
