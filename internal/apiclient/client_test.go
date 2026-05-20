package apiclient

// client_test.go — exercises the worker→api HTTP wrapper's circuit
// breaker against a controlled httptest.Server. We don't reach for the
// real api anywhere — these are pure-Go unit tests.

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"instant.dev/worker/internal/circuit"
)

// TestClient_5xxTripsBreaker — when the api returns 500 N times in a
// row the breaker opens and Do() short-circuits subsequent calls.
func TestClient_5xxTripsBreaker(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error":"down"}`)
	}))
	defer srv.Close()

	c := New(&http.Client{Timeout: 1 * time.Second})

	// 3 consecutive 5xx → breaker should be open.
	for i := 0; i < apiClientCircuitThreshold; i++ {
		req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
		resp, err := c.Do(req)
		if err != nil {
			t.Fatalf("attempt %d: unexpected error: %v", i+1, err)
		}
		if resp.StatusCode != 500 {
			t.Fatalf("attempt %d: want 500, got %d", i+1, resp.StatusCode)
		}
		_ = resp.Body.Close()
	}
	if c.Breaker().State() != circuit.StateOpen {
		t.Fatalf("breaker should be open after %d 5xx, got %s",
			apiClientCircuitThreshold, c.Breaker().State())
	}

	// Next call should be short-circuited — no server hit.
	hitsBefore := hits.Load()
	req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
	_, err := c.Do(req)
	if !errors.Is(err, circuit.ErrOpen) {
		t.Fatalf("want circuit.ErrOpen, got %v", err)
	}
	if hits.Load() != hitsBefore {
		t.Fatal("server should not be hit while breaker is open")
	}
}

// TestClient_4xxDoesNotTripBreaker — 4xx is a client error, not a
// server outage. 100 consecutive 4xx must NOT open the breaker.
func TestClient_4xxDoesNotTripBreaker(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	c := New(&http.Client{Timeout: 1 * time.Second})
	for i := 0; i < 100; i++ {
		req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
		resp, err := c.Do(req)
		if err != nil {
			t.Fatalf("attempt %d: unexpected error: %v", i+1, err)
		}
		_ = resp.Body.Close()
	}
	if c.Breaker().State() != circuit.StateClosed {
		t.Fatalf("breaker should still be closed after 100 4xx, got %s",
			c.Breaker().State())
	}
}

// TestClient_NetworkErrorTripsBreaker — connection-refused / DNS
// errors count as failures for breaker purposes.
func TestClient_NetworkErrorTripsBreaker(t *testing.T) {
	// Pick an unrouted port so http.Client returns a network error
	// immediately. Using a port-zero httptest server that we close
	// before issuing the request gives us a stable "connection
	// refused" path.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srvURL := srv.URL
	srv.Close()

	c := New(&http.Client{Timeout: 200 * time.Millisecond})
	for i := 0; i < apiClientCircuitThreshold; i++ {
		req, _ := http.NewRequest(http.MethodPost, srvURL, nil)
		_, err := c.Do(req)
		if err == nil {
			t.Fatalf("attempt %d: expected an error against a closed server", i+1)
		}
	}
	if c.Breaker().State() != circuit.StateOpen {
		t.Fatalf("breaker should be open after %d network errors, got %s",
			apiClientCircuitThreshold, c.Breaker().State())
	}
}

// TestClient_SuccessClosesBreakerFromHalfOpen — recovery happy path:
// after the cooldown, the first half-open trial succeeds and the
// breaker fully closes.
func TestClient_SuccessClosesBreakerFromHalfOpen(t *testing.T) {
	healthy := atomic.Bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if healthy.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	// Force a fresh breaker with a tiny cooldown so the test is fast.
	c := &Client{
		http:    &http.Client{Timeout: 1 * time.Second},
		breaker: circuit.NewBreaker("test_recover", 2, 20*time.Millisecond),
	}

	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
		resp, err := c.Do(req)
		if err == nil && resp != nil {
			_ = resp.Body.Close()
		}
	}
	if c.Breaker().State() != circuit.StateOpen {
		t.Fatalf("breaker should be open, got %s", c.Breaker().State())
	}

	// Flip the server healthy, wait cooldown, fire one request.
	healthy.Store(true)
	time.Sleep(30 * time.Millisecond)
	req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("recovery call should succeed, got %v", err)
	}
	_ = resp.Body.Close()
	if c.Breaker().State() != circuit.StateClosed {
		t.Fatalf("breaker should close after successful trial, got %s",
			c.Breaker().State())
	}
}

// TestClient_BreakerErrCarriesSentinel — callers use errors.Is to
// detect the short-circuit case.
func TestClient_BreakerErrCarriesSentinel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := New(&http.Client{Timeout: 200 * time.Millisecond})
	for i := 0; i < apiClientCircuitThreshold; i++ {
		req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
		resp, _ := c.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
	_, err := c.Do(req)
	if !errors.Is(err, circuit.ErrOpen) {
		t.Fatalf("errors.Is(err, circuit.ErrOpen) must be true, got %v", err)
	}
}

// TestClient_NamedCorrectly — the NR runbook references the
// `instant_circuit_breaker_state{name="worker_api_client"}` query.
// Lock in the name.
func TestClient_NamedCorrectly(t *testing.T) {
	c := New(nil)
	if got := c.Breaker().Name(); got != "worker_api_client" {
		t.Errorf("breaker name = %q; want 'worker_api_client'", got)
	}
}

// TestClient_429TripsBreaker is the regression for CIRCUIT-RETRY-AUDIT-2026-05-20
// (worker brief item 3 / audit P3-5): a 429 Too Many Requests response from the
// api signals "back off, I'm rate-limiting you" — the worker must count it as
// a Transient failure so the breaker can shed load. Before this fix, 429 was
// silently ignored as a non-5xx response and the worker would hammer through
// the rate-limit indefinitely.
func TestClient_429TripsBreaker(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":"rate_limited"}`)
	}))
	defer srv.Close()

	c := New(&http.Client{Timeout: 1 * time.Second})

	// apiClientCircuitThreshold consecutive 429s → breaker opens.
	for i := 0; i < apiClientCircuitThreshold; i++ {
		req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
		resp, err := c.Do(req)
		if err != nil {
			t.Fatalf("attempt %d: unexpected error: %v", i+1, err)
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("attempt %d: want 429, got %d", i+1, resp.StatusCode)
		}
		_ = resp.Body.Close()
	}
	if c.Breaker().State() != circuit.StateOpen {
		t.Fatalf("breaker should be OPEN after %d consecutive 429s, got %s",
			apiClientCircuitThreshold, c.Breaker().State())
	}

	// Subsequent calls short-circuit — no server hit.
	hitsBefore := hits.Load()
	req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
	_, err := c.Do(req)
	if !errors.Is(err, circuit.ErrOpen) {
		t.Fatalf("want circuit.ErrOpen after 429-floods, got %v", err)
	}
	if hits.Load() != hitsBefore {
		t.Fatal("server should not be hit while breaker is open")
	}
}

// TestClient_Other4xxStillDoesNotTrip locks in that the 429 carve-out does NOT
// silently bring 400/401/403/404 into the breaker. Those are client-side bugs
// and the breaker is a server-trouble detector — keep them separate.
func TestClient_Other4xxStillDoesNotTrip(t *testing.T) {
	for _, code := range []int{400, 401, 403, 404, 422} {
		code := code
		t.Run(http.StatusText(code), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
			}))
			defer srv.Close()

			c := New(&http.Client{Timeout: 1 * time.Second})
			for i := 0; i < apiClientCircuitThreshold*5; i++ {
				req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
				resp, err := c.Do(req)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				_ = resp.Body.Close()
			}
			if c.Breaker().State() != circuit.StateClosed {
				t.Errorf("breaker should remain CLOSED on consistent %d responses, got %s",
					code, c.Breaker().State())
			}
		})
	}
}

// TestClient_429ResponseBodyStillReadable — the 429 carve-out returns the
// *http.Response unchanged so the caller can read Retry-After / body for
// logging. We rely on that property in github_deploy_dispatcher and
// team_deletion_executor.
func TestClient_429ResponseBodyStillReadable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "42")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":"slow_down"}`)
	}))
	defer srv.Close()

	c := New(&http.Client{Timeout: 1 * time.Second})
	req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status: got %d, want 429", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "42" {
		t.Errorf("Retry-After header lost: got %q, want %q", got, "42")
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Contains(b, []byte("slow_down")) {
		t.Errorf("body lost: got %q", b)
	}
}
