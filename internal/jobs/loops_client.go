package jobs

// loops_client.go — thin HTTP client for the Loops.so transactional/events API.
//
// Surface: POST <loopsEventsURL> with a JSON body of the form
//
//	{
//	  "userId":     "<team's primary email>",
//	  "email":      "<team's primary email>",   // populated for first-time contacts
//	  "eventName":  "<one of the supported event names>",
//	  "eventProperties": { ... payload fields ... }
//	}
//
// Auth: Bearer <LOOPS_API_KEY>. Loops uses standard Bearer tokens — the API
// key is created in the Loops dashboard under Settings → API.
//
// Result classification — the caller (loops_event_forwarder.go) needs to know
// whether to advance the cursor or retry next tick:
//
//	2xx                          → ResultOK                — advance cursor
//	4xx (auth / payload error)   → ResultPermanent4xx      — advance cursor (don't get stuck)
//	5xx / network / timeout      → ResultTransient         — DO NOT advance cursor
//
// The 4xx-advances behaviour is deliberate: a single audit row with malformed
// content shouldn't block every event behind it forever. We log loudly so the
// poisoned row is visible in the structured-log stream.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"
)

// loopsEventsURL is the Loops.so events endpoint. Constant so a typo in
// either the client or a fake-server test surfaces at compile time.
// https://loops.so/docs/api-reference/send-event
const loopsEventsURL = "https://app.loops.so/api/v1/events/send"

// loopsHTTPTimeout caps a single Loops POST so a slow upstream can't
// stall a 100-row batch indefinitely. 10s is generous — Loops normally
// responds in <500ms.
const loopsHTTPTimeout = 10 * time.Second

// loopsResult is the three-way classification of a Loops POST outcome.
// The forwarder uses this directly to decide cursor-advance.
type loopsResult int

const (
	// loopsResultOK — Loops accepted the event (HTTP 2xx). Advance cursor.
	loopsResultOK loopsResult = iota

	// loopsResultPermanent4xx — Loops rejected with a 4xx (bad auth, bad
	// payload, unknown event). The audit row will never succeed in its
	// current shape; advance cursor to avoid stalling the queue. Logged
	// at ERROR so the operator sees the poisoned row.
	loopsResultPermanent4xx

	// loopsResultTransient — network error, timeout, or 5xx from Loops.
	// DO NOT advance cursor — retry next tick.
	loopsResultTransient
)

// loopsClient sends events to Loops.so. Constructed once per worker boot
// via newLoopsClient(apiKey). With an empty apiKey it returns nil and the
// forwarder logs + exits cleanly (fail-open contract — see workers.go).
type loopsClient struct {
	apiKey string
	httpc  *http.Client
	// url is overridable so tests can point at an httptest.Server. Always
	// loopsEventsURL in production.
	url string
}

// newLoopsClient builds a client bound to apiKey. Returns nil when apiKey
// is empty so the caller can branch on (client == nil) without a flag.
func newLoopsClient(apiKey string) *loopsClient {
	if apiKey == "" {
		return nil
	}
	return &loopsClient{
		apiKey: apiKey,
		httpc:  &http.Client{Timeout: loopsHTTPTimeout},
		url:    loopsEventsURL,
	}
}

// loopsEventPayload is the JSON body shape Loops expects. eventProperties is
// arbitrary k/v sourced from the audit_log row; the mapping table in
// loops_event_mapping.go decides what goes in.
type loopsEventPayload struct {
	UserID          string                 `json:"userId"`
	Email           string                 `json:"email"`
	EventName       string                 `json:"eventName"`
	EventProperties map[string]interface{} `json:"eventProperties,omitempty"`
}

// sendEvent POSTs one event to Loops and returns the result classification.
// The body shape is the same for every supported event — only eventName and
// eventProperties vary.
func (c *loopsClient) sendEvent(ctx context.Context, p loopsEventPayload) loopsResult {
	body, err := json.Marshal(p)
	if err != nil {
		// Marshalling a map[string]interface{} should only fail if a value
		// contains a non-JSON type — the mapping layer is strict so this is
		// effectively unreachable. Treat as a permanent 4xx to advance the
		// cursor (the row is unsendable in its current shape).
		slog.Error("jobs.loops.marshal_failed",
			"event_name", p.EventName,
			"user_id", p.UserID,
			"error", err,
		)
		return loopsResultPermanent4xx
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		// Request construction can fail on a bad URL — treat as transient
		// because it's almost certainly a programming bug we want to see
		// repeatedly in logs, not silently advance past.
		slog.Error("jobs.loops.request_build_failed",
			"event_name", p.EventName,
			"error", err,
		)
		return loopsResultTransient
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpc.Do(req)
	if err != nil {
		// Network error, timeout, dns failure. Transient by definition.
		slog.Warn("jobs.loops.http_failed",
			"event_name", p.EventName,
			"user_id", p.UserID,
			"error", err,
		)
		return loopsResultTransient
	}
	defer resp.Body.Close()

	// Drain (small) body so the connection can be reused.
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		slog.Info("jobs.loops.event_sent",
			"event_name", p.EventName,
			"user_id", p.UserID,
			"status", resp.StatusCode,
		)
		return loopsResultOK
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// 401/403 = bad API key (every row will fail the same way until
		// someone rotates the secret), 400/422 = bad payload for this row.
		// In both cases advancing the cursor is correct: holding it pins
		// the whole queue on one bad row.
		slog.Error("jobs.loops.permanent_4xx",
			"event_name", p.EventName,
			"user_id", p.UserID,
			"status", resp.StatusCode,
			"body", string(respBody),
		)
		return loopsResultPermanent4xx
	default:
		// 5xx — Loops upstream issue. Hold cursor; retry next tick.
		slog.Warn("jobs.loops.transient_5xx",
			"event_name", p.EventName,
			"user_id", p.UserID,
			"status", resp.StatusCode,
			"body", string(respBody),
		)
		return loopsResultTransient
	}
}

