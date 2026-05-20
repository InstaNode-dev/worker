// Package apiclient is the worker's HTTP client for the api's internal
// surface (POST /internal/teams/:id/terminate, POST /deploy/:id/redeploy).
//
// Why this exists:
//
//   - Both call sites (payment_grace_terminator + github_deploy_dispatcher)
//     made their own http.Client and built requests by hand. When the api
//     is down, the request just times out and the job's retry budget burns
//     while the api is unreachable.
//   - This package wraps those calls behind a single circuit breaker so an
//     api outage costs 30s of "circuit-open" log lines instead of a
//     retry storm hammering a known-bad pod.
//   - Callers should treat circuit.ErrOpen as "leave the row pending; the
//     next periodic tick will re-claim it once the breaker closes" — no
//     in-process retry needed.
//
// The package exposes Do() as a thin wrapper around http.Client.Do() so
// the existing call sites' request-building code stays untouched.
package apiclient

import (
	"log/slog"
	"net/http"
	"time"

	"instant.dev/worker/internal/circuit"
)

// Circuit-breaker tuning for the worker → api internal boundary.
//
//   - apiClientCircuitThreshold = 3 — tighter than the api side's
//     provisioner/razorpay breakers because the worker doesn't have a
//     customer-facing impact when it short-circuits: it just leaves the
//     row pending for the next sweep. False positives are cheap.
//   - apiClientCircuitCooldown = 30s — matches the periodic sweep
//     cadence on the most-frequent caller (github_deploy_dispatcher
//     runs every 30s). One sweep miss while the breaker is open, then
//     the next sweep gets the half-open trial.
const (
	apiClientCircuitName      = "worker_api_client"
	apiClientCircuitThreshold = 3
	apiClientCircuitCooldown  = 30 * time.Second
)

// Client wraps an http.Client with a process-singleton circuit breaker
// for the api's internal endpoints. Construct via New(); concurrent use
// is safe.
type Client struct {
	http    *http.Client
	breaker *circuit.Breaker
}

// New constructs a Client around the supplied http.Client. The http.Client
// MUST set its own Timeout — the breaker doesn't replace per-call timeouts;
// it's the safety valve when those timeouts start tripping at scale.
//
// The breaker is constructed inline (one per worker process) rather than
// being lazily-shared like the api's so each test can construct fresh
// Clients without worrying about a leaked-state singleton. The metric
// label is still the constant `worker_api_client`, matching the brief's
// NR metric prefix expectation.
func New(httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	b := circuit.NewBreaker(
		apiClientCircuitName,
		apiClientCircuitThreshold,
		apiClientCircuitCooldown,
	).WithOnOpen(func() {
		slog.Error("worker.api_client.circuit.opened",
			"name", apiClientCircuitName,
			"threshold", apiClientCircuitThreshold,
			"cooldown_seconds", int(apiClientCircuitCooldown.Seconds()),
			"impact", "team_deletion_executor / payment_grace_terminator / github_deploy_dispatcher will skip rows for 30s",
			"runbook", "https://instanode.dev/status",
		)
	})
	return &Client{http: httpClient, breaker: b}
}

// Do is a drop-in replacement for http.Client.Do() with circuit-breaker
// protection. Returns circuit.ErrOpen WITHOUT issuing the request when
// the breaker is open.
//
// Failure recording rules:
//
//   - Network error (dial / timeout / TLS): counts as a failure.
//   - HTTP 5xx response: counts as a failure (the api is signalling it
//     can't serve us). The *http.Response is still returned so the
//     caller can read the body for logging.
//   - HTTP 429 response: counts as a failure. Audit P3-5 / brief item 3:
//     a 429 from the api is a rate-limit feedback signal — the worker
//     should shed (slow down) rather than ignore it. Distinct from a
//     server outage (5xx) but in the same "back off" bucket for breaker
//     purposes. The *http.Response is still returned unchanged so the
//     caller can read Retry-After / body for logging.
//   - Other HTTP 4xx response: NOT a failure (it's our request that's
//     wrong, not the api that's down). Caller decides how to handle.
//   - HTTP 2xx/3xx: success — resets the consecutive-failure counter.
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	if !c.breaker.Allow() {
		return nil, circuit.ErrOpen
	}
	resp, err := c.http.Do(req)
	if err != nil {
		c.breaker.Record(err)
		return resp, err
	}
	// Treat 5xx as a failure for breaker purposes. 4xx (other than 429)
	// is not a server outage signal — those should not trip the breaker.
	if resp.StatusCode >= 500 {
		c.breaker.Record(httpServerError{Code: resp.StatusCode})
		return resp, nil
	}
	// 429 Too Many Requests — count as a Transient failure so the breaker
	// paces the worker against the api's rate-limit feedback. Without this,
	// a misconfigured worker would hammer the api forever; the breaker is
	// the load-shedder of last resort.
	if resp.StatusCode == http.StatusTooManyRequests {
		c.breaker.Record(httpServerError{Code: resp.StatusCode})
		return resp, nil
	}
	c.breaker.Record(nil)
	return resp, nil
}

// Breaker returns the underlying breaker for tests and /healthz. Do
// NOT mutate the returned breaker.
func (c *Client) Breaker() *circuit.Breaker { return c.breaker }

// httpServerError is an internal sentinel used to mark a 5xx response
// as a breaker failure. We don't return it to callers — they see the
// (resp, nil) shape from c.http.Do(), unchanged.
type httpServerError struct {
	Code int
}

func (e httpServerError) Error() string {
	return http.StatusText(e.Code)
}
