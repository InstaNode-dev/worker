package jobs

// loops_client_test.go — hermetic tests for the Loops.so HTTP client.
//
// Uses an httptest.Server stand-in for Loops so we never hit the live API.
// Each test exercises exactly one classification path (2xx / 4xx / 5xx /
// network error) and asserts the right loopsResult comes back.
//
// Lives in package `jobs` (not `jobs_test`) so it can construct loopsClient
// directly without exporting the constructor.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeLoops returns a server that responds with the given status code and
// records the incoming request so callers can assert on body + headers.
func fakeLoops(t *testing.T, status int) (*httptest.Server, *recordedReq) {
	t.Helper()
	rr := &recordedReq{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rr.path = r.URL.Path
		rr.method = r.Method
		rr.auth = r.Header.Get("Authorization")
		rr.contentType = r.Header.Get("Content-Type")
		rr.body = body
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	return srv, rr
}

type recordedReq struct {
	path        string
	method      string
	auth        string
	contentType string
	body        []byte
}

// TestLoopsClient_NewWithEmptyKeyReturnsNil verifies the fail-open contract:
// no API key = no client. The forwarder branches on (client == nil), so this
// guarantees the "missing secret" path doesn't accidentally instantiate a
// half-configured client that would fail on every send.
func TestLoopsClient_NewWithEmptyKeyReturnsNil(t *testing.T) {
	if c := newLoopsClient(""); c != nil {
		t.Errorf("newLoopsClient(\"\") = non-nil; want nil so the forwarder can fail open")
	}
}

// TestLoopsClient_2xxReturnsOK is the happy path: Loops accepts the event
// and we return loopsResultOK so the forwarder advances the cursor.
func TestLoopsClient_2xxReturnsOK(t *testing.T) {
	srv, rr := fakeLoops(t, http.StatusOK)
	c := newLoopsClient("test-key")
	c.url = srv.URL

	res := c.sendEvent(context.Background(), loopsEventPayload{
		UserID:    "user@example.com",
		Email:     "user@example.com",
		EventName: loopsEventTeamClaimed,
		EventProperties: map[string]interface{}{
			"signup_source": "github",
		},
	})

	if res != loopsResultOK {
		t.Errorf("sendEvent on 200 = %v; want loopsResultOK", res)
	}
	if rr.method != http.MethodPost {
		t.Errorf("method = %s; want POST", rr.method)
	}
	if rr.auth != "Bearer test-key" {
		t.Errorf("auth = %q; want Bearer test-key", rr.auth)
	}
	if rr.contentType != "application/json" {
		t.Errorf("Content-Type = %q; want application/json", rr.contentType)
	}
	var got loopsEventPayload
	if err := json.Unmarshal(rr.body, &got); err != nil {
		t.Fatalf("unmarshal body: %v; raw=%q", err, rr.body)
	}
	if got.EventName != loopsEventTeamClaimed {
		t.Errorf("eventName = %q; want %q", got.EventName, loopsEventTeamClaimed)
	}
	if got.UserID != "user@example.com" {
		t.Errorf("userId = %q; want user@example.com", got.UserID)
	}
}

// TestLoopsClient_4xxReturnsPermanent verifies the "advance past poisoned
// row" contract: a 401 (or any 4xx) MUST NOT block the queue.
func TestLoopsClient_4xxReturnsPermanent(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity} {
		srv, _ := fakeLoops(t, status)
		c := newLoopsClient("test-key")
		c.url = srv.URL
		res := c.sendEvent(context.Background(), loopsEventPayload{
			UserID:    "x@example.com",
			Email:     "x@example.com",
			EventName: "any",
		})
		if res != loopsResultPermanent4xx {
			t.Errorf("sendEvent on %d = %v; want loopsResultPermanent4xx — holding the cursor on auth errors pins the queue forever", status, res)
		}
	}
}

// TestLoopsClient_5xxReturnsTransient verifies that upstream failures cause
// the cursor to hold — we'll retry next tick.
func TestLoopsClient_5xxReturnsTransient(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		srv, _ := fakeLoops(t, status)
		c := newLoopsClient("test-key")
		c.url = srv.URL
		res := c.sendEvent(context.Background(), loopsEventPayload{UserID: "x", Email: "x", EventName: "any"})
		if res != loopsResultTransient {
			t.Errorf("sendEvent on %d = %v; want loopsResultTransient — Loops 5xx should retry next tick", status, res)
		}
	}
}

// TestLoopsClient_NetworkErrorReturnsTransient verifies that a connection
// failure (server unreachable) classifies as transient. The forwarder
// then holds the cursor — exactly the right behaviour for a flaky network.
func TestLoopsClient_NetworkErrorReturnsTransient(t *testing.T) {
	c := newLoopsClient("test-key")
	// Point at a closed port — connection will fail immediately.
	c.url = "http://127.0.0.1:1/does-not-exist"
	res := c.sendEvent(context.Background(), loopsEventPayload{UserID: "x", Email: "x", EventName: "any"})
	if res != loopsResultTransient {
		t.Errorf("sendEvent on closed port = %v; want loopsResultTransient", res)
	}
}

// TestLoopsClient_RequestPayloadShape verifies the wire format matches what
// Loops expects: userId + email + eventName + eventProperties (object).
// Catches accidental rename of any JSON tag.
func TestLoopsClient_RequestPayloadShape(t *testing.T) {
	srv, rr := fakeLoops(t, http.StatusOK)
	c := newLoopsClient("test-key")
	c.url = srv.URL

	c.sendEvent(context.Background(), loopsEventPayload{
		UserID:    "u@example.com",
		Email:     "u@example.com",
		EventName: "team_claimed",
		EventProperties: map[string]interface{}{
			"signup_source": "google",
		},
	})

	raw := string(rr.body)
	for _, want := range []string{`"userId":"u@example.com"`, `"email":"u@example.com"`, `"eventName":"team_claimed"`, `"signup_source":"google"`} {
		if !strings.Contains(raw, want) {
			t.Errorf("payload missing %q\nraw: %s", want, raw)
		}
	}
}
