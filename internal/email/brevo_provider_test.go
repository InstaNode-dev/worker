package email

// brevo_provider_test.go — hermetic tests for the Brevo provider.
//
// Uses an httptest.Server stand-in for Brevo so we never hit the live API.
// Each test exercises exactly one classification path (2xx / 4xx / 5xx /
// network error / missing template) and asserts the right SendError.Class
// (or nil) comes back. Lives in package `email` so it can construct
// BrevoProvider directly and read unexported fields.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"instant.dev/worker/internal/metrics"
)

// fakeBrevo returns a server that responds with the given status code and
// records the incoming request so callers can assert on body + headers.
func fakeBrevo(t *testing.T, status int) (*httptest.Server, *recordedReq) {
	t.Helper()
	rr := &recordedReq{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rr.path = r.URL.Path
		rr.method = r.Method
		rr.apiKey = r.Header.Get(headerAPIKey)
		rr.contentType = r.Header.Get(headerContentType)
		rr.idempotency = r.Header.Get(headerIdempotency)
		rr.body = body
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"messageId":"x"}`))
	}))
	t.Cleanup(srv.Close)
	return srv, rr
}

type recordedReq struct {
	path        string
	method      string
	apiKey      string
	contentType string
	idempotency string
	body        []byte
}

// newTestProvider builds a BrevoProvider pointed at srv with a fixed
// template map. Centralises the boilerplate so each test reads cleanly.
func newTestProvider(t *testing.T, srv *httptest.Server, templates map[string]int) *BrevoProvider {
	t.Helper()
	if templates == nil {
		templates = map[string]int{"subscription.upgraded": 42}
	}
	p, err := NewBrevoProvider(BrevoConfig{APIKey: "test-key", TemplateIDs: templates})
	if err != nil {
		t.Fatalf("NewBrevoProvider: %v", err)
	}
	p.url = srv.URL
	return p
}

// TestBrevoProvider_NewWithEmptyKey_Errors — operator who set EMAIL_PROVIDER=brevo
// without BREVO_API_KEY should see a fast, loud boot failure, not a silent
// no-op. Pairs with TestFactory_NoopOnEmptyProvider over in provider_test.go.
func TestBrevoProvider_NewWithEmptyKey_Errors(t *testing.T) {
	_, err := NewBrevoProvider(BrevoConfig{APIKey: "", TemplateIDs: map[string]int{"x": 1}})
	if err == nil {
		t.Fatal("NewBrevoProvider with empty APIKey returned nil; want error so operator boot-fails on misconfig")
	}
}

// TestBrevoProvider_Name_IsStable — the slog/metric label MUST NOT drift.
func TestBrevoProvider_Name_IsStable(t *testing.T) {
	p, err := NewBrevoProvider(BrevoConfig{APIKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Name(); got != "brevo" {
		t.Errorf("Name() = %q; want brevo (matches providerNameBrevo and docs)", got)
	}
}

// TestBrevoProvider_2xxReturnsNil is the happy path: Brevo accepts the
// event and SendEvent returns nil so the forwarder advances the cursor.
// Also asserts the wire shape — Authorization header is "api-key" not
// "Bearer", body has templateId + to[] + params.
func TestBrevoProvider_2xxReturnsNil(t *testing.T) {
	srv, rr := fakeBrevo(t, http.StatusOK)
	p := newTestProvider(t, srv, map[string]int{"subscription.upgraded": 42})

	err := p.SendEvent(context.Background(), EventEmail{
		Kind:           "subscription.upgraded",
		Recipient:      "user@example.com",
		RecipientName:  "User",
		Params:         map[string]string{"from_tier": "hobby", "to_tier": "pro"},
		IdempotencyKey: "audit-123",
	})

	if err != nil {
		t.Errorf("SendEvent on 200 = %v; want nil", err)
	}
	if rr.method != http.MethodPost {
		t.Errorf("method = %s; want POST", rr.method)
	}
	if rr.apiKey != "test-key" {
		t.Errorf("api-key header = %q; want test-key (Brevo uses `api-key` not `Authorization: Bearer`)", rr.apiKey)
	}
	if rr.contentType != contentTypeJSON {
		t.Errorf("Content-Type = %q; want %s", rr.contentType, contentTypeJSON)
	}
	if rr.idempotency != "audit-123" {
		t.Errorf("X-Mailin-Custom = %q; want audit-123 (the IdempotencyKey from EventEmail)", rr.idempotency)
	}
	var got brevoSendRequest
	if err := json.Unmarshal(rr.body, &got); err != nil {
		t.Fatalf("unmarshal body: %v; raw=%q", err, rr.body)
	}
	if got.TemplateID != 42 {
		t.Errorf("templateId = %d; want 42", got.TemplateID)
	}
	if len(got.To) != 1 || got.To[0].Email != "user@example.com" {
		t.Errorf("to = %+v; want [{user@example.com, User}]", got.To)
	}
	if got.Params["from_tier"] != "hobby" || got.Params["to_tier"] != "pro" {
		t.Errorf("params = %+v; want from_tier=hobby to_tier=pro", got.Params)
	}
}

// TestBrevoProvider_StatusCodeClassification is the table-driven coverage
// over every HTTP status code the provider classifies, expressed as one
// row per code. New codes go here, NOT into a per-class one-off test. The
// table is authoritative for the BugBash 2026-05-20 P0-1 contract; if a
// future change reclassifies a code, this table fails first.
//
// BUG: a bad/expired/revoked BREVO_API_KEY returns 401/403. The pre-2026-05-19
// provider classified that Permanent → the forwarder advanced the cursor →
// every audit row in every batch was silently, unrecoverably dropped. 408
// (Request Timeout), 425 (Too Early), and 429 (Too Many Requests) had the
// same fate. The table below pins the post-fix contract so any future code
// change that re-introduces the regression fails LOUDLY at unit test.
func TestBrevoProvider_StatusCodeClassification(t *testing.T) {
	cases := []struct {
		name   string
		status int
		want   SendClass
	}{
		// success
		{"200 OK", http.StatusOK, -1}, // -1 sentinel: expect nil error
		{"201 Created", http.StatusCreated, -1},
		{"202 Accepted", http.StatusAccepted, -1},

		// permanent (genuine payload rejects — cursor advances)
		{"400 BadRequest", http.StatusBadRequest, SendClassPermanent},
		{"404 NotFound", http.StatusNotFound, SendClassPermanent},
		{"409 Conflict", http.StatusConflict, SendClassPermanent},
		{"422 Unprocessable", http.StatusUnprocessableEntity, SendClassPermanent},

		// transient — auth/account-level (cursor held; token rotate recovers)
		{"401 Unauthorized (P0-1: was Permanent)", http.StatusUnauthorized, SendClassTransient},
		{"403 Forbidden (P0-1: was Permanent)", http.StatusForbidden, SendClassTransient},

		// transient — back-off-and-retry (cursor held)
		{"408 RequestTimeout (BugBash 2026-05-20: explicit)", http.StatusRequestTimeout, SendClassTransient},
		{"425 TooEarly (BugBash 2026-05-20: explicit)", http.StatusTooEarly, SendClassTransient},
		{"429 TooManyRequests (P0-1: was Permanent)", http.StatusTooManyRequests, SendClassTransient},

		// transient — upstream 5xx (cursor held; Brevo issue)
		{"500 InternalServerError", http.StatusInternalServerError, SendClassTransient},
		{"502 BadGateway", http.StatusBadGateway, SendClassTransient},
		{"503 ServiceUnavailable", http.StatusServiceUnavailable, SendClassTransient},
		{"504 GatewayTimeout", http.StatusGatewayTimeout, SendClassTransient},
		{"599 (rfc-violating upstream)", 599, SendClassTransient},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := fakeBrevo(t, tc.status)
			p := newTestProvider(t, srv, nil)
			err := p.SendEvent(context.Background(), EventEmail{
				Kind:           "subscription.upgraded",
				Recipient:      "x@example.com",
				IdempotencyKey: "audit-table-" + tc.name,
			})
			if tc.want == -1 {
				if err != nil {
					t.Errorf("status %d → %v; want nil (success)", tc.status, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("status %d → nil; want SendError(%v)", tc.status, tc.want)
			}
			var se *SendError
			if !errors.As(err, &se) {
				t.Fatalf("status %d → %T; want *SendError", tc.status, err)
			}
			if se.Class != tc.want {
				t.Errorf("status %d → Class=%v; want %v", tc.status, se.Class, tc.want)
			}
		})
	}
}

// TestBrevoProvider_5xxReturnsTransient verifies that upstream failures cause
// the cursor to hold — forwarder retries next tick.
func TestBrevoProvider_5xxReturnsTransient(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		srv, _ := fakeBrevo(t, status)
		p := newTestProvider(t, srv, nil)
		err := p.SendEvent(context.Background(), EventEmail{Kind: "subscription.upgraded", Recipient: "x@example.com"})
		if err == nil {
			t.Errorf("SendEvent on %d = nil; want SendError(Transient)", status)
			continue
		}
		var se *SendError
		if !errors.As(err, &se) {
			t.Errorf("SendEvent on %d returned %T; want *SendError", status, err)
			continue
		}
		if se.Class != SendClassTransient {
			t.Errorf("SendEvent on %d → Class=%v; want SendClassTransient — Brevo 5xx should retry next tick", status, se.Class)
		}
	}
}

// TestBrevoProvider_NetworkErrorReturnsTransient — connection failure
// (server unreachable) classifies as transient.
func TestBrevoProvider_NetworkErrorReturnsTransient(t *testing.T) {
	p, err := NewBrevoProvider(BrevoConfig{APIKey: "k", TemplateIDs: map[string]int{"x": 1}})
	if err != nil {
		t.Fatal(err)
	}
	p.url = "http://127.0.0.1:1/does-not-exist" // closed port
	gotErr := p.SendEvent(context.Background(), EventEmail{Kind: "x", Recipient: "u@e.com"})
	var se *SendError
	if !errors.As(gotErr, &se) || se.Class != SendClassTransient {
		t.Errorf("network error → %v; want SendClassTransient", gotErr)
	}
}

// TestBrevoProvider_MissingTemplate_SkipsNoTemplate verifies the
// "operator hasn't wired up this Kind yet" path: forwarder advances
// silently (the Class is SkippedNoTemplate, not Permanent).
func TestBrevoProvider_MissingTemplate_SkipsNoTemplate(t *testing.T) {
	srv, rr := fakeBrevo(t, http.StatusOK)
	p := newTestProvider(t, srv, map[string]int{"subscription.upgraded": 42})

	err := p.SendEvent(context.Background(), EventEmail{
		Kind:      "experiment.conversion", // not in template map
		Recipient: "x@example.com",
	})
	var se *SendError
	if !errors.As(err, &se) {
		t.Fatalf("SendEvent for unmapped kind = %v; want *SendError", err)
	}
	if se.Class != SendClassSkippedNoTemplate {
		t.Errorf("Class = %v; want SendClassSkippedNoTemplate", se.Class)
	}
	if rr.method != "" {
		t.Errorf("Brevo was called for unmapped kind — should have short-circuited locally")
	}
}

// TestBrevoProvider_EmptyRecipient_ReturnsPermanent — a defensive guard:
// the forwarder filters orphan rows, but if one slips through SendEvent
// MUST advance the cursor (Permanent), not retry it forever.
func TestBrevoProvider_EmptyRecipient_ReturnsPermanent(t *testing.T) {
	srv, _ := fakeBrevo(t, http.StatusOK)
	p := newTestProvider(t, srv, map[string]int{"subscription.upgraded": 42})

	err := p.SendEvent(context.Background(), EventEmail{
		Kind:      "subscription.upgraded",
		Recipient: "",
	})
	var se *SendError
	if !errors.As(err, &se) || se.Class != SendClassPermanent {
		t.Errorf("empty recipient → %v; want SendClassPermanent", err)
	}
}

// TestBrevoProvider_RequestPayloadShape — the JSON wire format must match
// what Brevo expects: to[] + templateId + params. Catches accidental
// rename of any JSON tag.
func TestBrevoProvider_RequestPayloadShape(t *testing.T) {
	srv, rr := fakeBrevo(t, http.StatusOK)
	p := newTestProvider(t, srv, map[string]int{"subscription.upgraded": 7})

	p.SendEvent(context.Background(), EventEmail{
		Kind:          "subscription.upgraded",
		Recipient:     "u@example.com",
		RecipientName: "U",
		Params:        map[string]string{"to_tier": "pro"},
	})

	raw := string(rr.body)
	for _, want := range []string{`"to":[`, `"email":"u@example.com"`, `"name":"U"`, `"templateId":7`, `"to_tier":"pro"`} {
		if !strings.Contains(raw, want) {
			t.Errorf("payload missing %q\nraw: %s", want, raw)
		}
	}
}

// TestBrevoProvider_NoIdempotencyKey_OmitsHeader — when IdempotencyKey is
// empty we MUST NOT send an empty X-Mailin-Custom header, because Brevo
// may interpret "" as a real key and dedupe against unrelated future sends.
func TestBrevoProvider_NoIdempotencyKey_OmitsHeader(t *testing.T) {
	srv, rr := fakeBrevo(t, http.StatusOK)
	p := newTestProvider(t, srv, map[string]int{"subscription.upgraded": 7})
	p.SendEvent(context.Background(), EventEmail{
		Kind:      "subscription.upgraded",
		Recipient: "u@example.com",
	})
	if rr.idempotency != "" {
		t.Errorf("X-Mailin-Custom = %q on empty IdempotencyKey; want '' (header omitted)", rr.idempotency)
	}
}

// TestBrevoProvider_RawHTMLPath_UsesSubjectHTMLSender exercises the
// raw-HTML send path: when EventEmail.HTMLBody is non-empty the provider
// MUST send {sender, subject, htmlContent, textContent} and NOT include
// a templateId. The dashboard-template path is bypassed entirely. This
// is the fix for the "anon.expiry_warning" broken dashboard template.
func TestBrevoProvider_RawHTMLPath_UsesSubjectHTMLSender(t *testing.T) {
	srv, rr := fakeBrevo(t, http.StatusOK)
	// Deliberately empty template map — raw path must NOT consult it.
	p, err := NewBrevoProvider(BrevoConfig{
		APIKey:      "test-key",
		TemplateIDs: map[string]int{}, // empty on purpose
		SenderEmail: "noreply@instanode.dev",
		SenderName:  "instanode",
	})
	if err != nil {
		t.Fatalf("NewBrevoProvider: %v", err)
	}
	p.url = srv.URL

	err = p.SendEvent(context.Background(), EventEmail{
		Kind:           "anon.expiry_warning",
		Recipient:      "u@example.com",
		RecipientName:  "U",
		IdempotencyKey: "audit-xyz",
		Subject:        "Heads up — your instanode postgres expires in 12h",
		HTMLBody:       "<p>hello</p>",
		TextBody:       "hello",
		// Params is set but the raw path should NOT use it as a template
		// substitution map (the body is already rendered).
		Params: map[string]string{"hours_remaining": "12"},
	})
	if err != nil {
		t.Fatalf("SendEvent (raw) returned %v; want nil", err)
	}
	if rr.method != http.MethodPost {
		t.Errorf("method = %s; want POST", rr.method)
	}
	if rr.idempotency != "audit-xyz" {
		t.Errorf("X-Mailin-Custom = %q; want audit-xyz", rr.idempotency)
	}

	// Parse the body into a generic map — the raw payload should have
	// htmlContent, subject, sender; it should NOT have templateId.
	var got map[string]interface{}
	if err := json.Unmarshal(rr.body, &got); err != nil {
		t.Fatalf("unmarshal body: %v; raw=%q", err, rr.body)
	}
	if _, hasTemplateID := got["templateId"]; hasTemplateID {
		t.Errorf("raw path sent templateId; want omitted (raw render must not consult templates). body=%s", string(rr.body))
	}
	if subj, _ := got["subject"].(string); subj != "Heads up — your instanode postgres expires in 12h" {
		t.Errorf("subject = %q; want subject from EventEmail.Subject", subj)
	}
	if hc, _ := got["htmlContent"].(string); hc != "<p>hello</p>" {
		t.Errorf("htmlContent = %q; want '<p>hello</p>'", hc)
	}
	if tc, _ := got["textContent"].(string); tc != "hello" {
		t.Errorf("textContent = %q; want 'hello'", tc)
	}
	sender, _ := got["sender"].(map[string]interface{})
	if sender == nil {
		t.Fatalf("sender object missing from raw payload: %s", string(rr.body))
	}
	if e, _ := sender["email"].(string); e != "noreply@instanode.dev" {
		t.Errorf("sender.email = %q; want noreply@instanode.dev (raw path must not inherit dashboard sender)", e)
	}
	if n, _ := sender["name"].(string); n != "instanode" {
		t.Errorf("sender.name = %q; want instanode", n)
	}
}

// TestBrevoProvider_RawHTMLPath_DefaultSender — when BREVO_SENDER_EMAIL
// is unset (empty BrevoConfig.SenderEmail), the raw path MUST still send
// from noreply@instanode.dev, never from an empty string. The whole
// point of the env-var defaults is that a misconfigured worker cannot
// silently inherit a personal email from the Brevo dashboard.
func TestBrevoProvider_RawHTMLPath_DefaultSender(t *testing.T) {
	srv, rr := fakeBrevo(t, http.StatusOK)
	p, err := NewBrevoProvider(BrevoConfig{APIKey: "k"}) // no SenderEmail / SenderName
	if err != nil {
		t.Fatalf("NewBrevoProvider: %v", err)
	}
	p.url = srv.URL
	if err := p.SendEvent(context.Background(), EventEmail{
		Kind:      "anon.expiry_warning",
		Recipient: "u@example.com",
		Subject:   "S",
		HTMLBody:  "<p>x</p>",
	}); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(rr.body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	sender, _ := got["sender"].(map[string]interface{})
	if e, _ := sender["email"].(string); e != "noreply@instanode.dev" {
		t.Errorf("default sender.email = %q; want noreply@instanode.dev (env unset must NOT yield empty sender)", e)
	}
}

// TestBrevoProvider_RawHTMLPath_EmptySubject_ReturnsPermanent — a raw send
// with an HTML body but no Subject is a programmer bug (the renderer
// should always produce one). We must advance the cursor, not loop.
func TestBrevoProvider_RawHTMLPath_EmptySubject_ReturnsPermanent(t *testing.T) {
	srv, _ := fakeBrevo(t, http.StatusOK)
	p := newTestProvider(t, srv, nil)
	err := p.SendEvent(context.Background(), EventEmail{
		Kind:      "anon.expiry_warning",
		Recipient: "u@example.com",
		Subject:   "", // bug — should never happen
		HTMLBody:  "<p>x</p>",
	})
	var se *SendError
	if !errors.As(err, &se) || se.Class != SendClassPermanent {
		t.Errorf("empty subject in raw path → %v; want SendClassPermanent (advance cursor on programmer bug)", err)
	}
}

// TestBrevoProvider_RawHTMLPath_BypassesMissingTemplate — when HTMLBody is
// set, the provider must NOT fall through to SkippedNoTemplate even if
// the kind has no template id in the map. The raw path is independent
// of the template map entirely.
func TestBrevoProvider_RawHTMLPath_BypassesMissingTemplate(t *testing.T) {
	srv, _ := fakeBrevo(t, http.StatusOK)
	// Map only knows about a different kind — the kind we send is unmapped.
	p := newTestProvider(t, srv, map[string]int{"subscription.upgraded": 99})
	err := p.SendEvent(context.Background(), EventEmail{
		Kind:      "anon.expiry_warning", // not in map
		Recipient: "u@example.com",
		Subject:   "S",
		HTMLBody:  "<p>x</p>",
	})
	if err != nil {
		t.Errorf("raw send with unmapped kind = %v; want nil (raw bypasses template map)", err)
	}
}

// fakeBrevoCustom lets a test specify both status code and exact response
// body — useful for the empty-body 200 + custom-error-envelope cases that
// the fixed-body fakeBrevo helper can't express.
func fakeBrevoCustom(t *testing.T, status int, body string) (*httptest.Server, *recordedReq) {
	t.Helper()
	rr := &recordedReq{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		rr.path = r.URL.Path
		rr.method = r.Method
		rr.apiKey = r.Header.Get(headerAPIKey)
		rr.contentType = r.Header.Get(headerContentType)
		rr.idempotency = r.Header.Get(headerIdempotency)
		rr.body = b
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, rr
}

// TestBrevoProvider_EmptyBody200_IsSuccess — some Brevo template paths
// (notably the dashboard "template not found" failure-mode prior to 2026-05)
// return a 200 with an empty body. The classification contract is
// status-code-driven: 2xx == success regardless of body content. This
// pins that contract so a future "200 + empty body" hand-wringing branch
// doesn't get reintroduced.
func TestBrevoProvider_EmptyBody200_IsSuccess(t *testing.T) {
	srv, _ := fakeBrevoCustom(t, http.StatusOK, "")
	p := newTestProvider(t, srv, map[string]int{"subscription.upgraded": 7})
	err := p.SendEvent(context.Background(), EventEmail{
		Kind:           "subscription.upgraded",
		Recipient:      "u@example.com",
		IdempotencyKey: "audit-empty-body",
	})
	if err != nil {
		t.Errorf("200 with empty body → %v; want nil (success is status-code-driven)", err)
	}
}

// TestBrevoProvider_NilResponse_NetworkError pins the "no response at all"
// path: when http.Client.Do returns (nil, error) — DNS failure, connection
// refused, TLS handshake failure, context deadline before headers — the
// provider MUST return SendClassTransient (cursor held). The metric is
// labelled status_code="0" because there's no real status to record;
// this is verified in TestBrevoProvider_Metrics_NetworkError_Status0 below.
func TestBrevoProvider_NilResponse_NetworkError(t *testing.T) {
	p, err := NewBrevoProvider(BrevoConfig{APIKey: "k", TemplateIDs: map[string]int{"x": 1}})
	if err != nil {
		t.Fatal(err)
	}
	// Use a closed port — no listener will accept, Do returns (nil, err).
	p.url = "http://127.0.0.1:1/closed"
	gotErr := p.SendEvent(context.Background(), EventEmail{
		Kind:           "x",
		Recipient:      "u@e.com",
		IdempotencyKey: "audit-net-err",
	})
	if gotErr == nil {
		t.Fatal("network error → nil; want SendError(Transient)")
	}
	var se *SendError
	if !errors.As(gotErr, &se) {
		t.Fatalf("network error → %T; want *SendError", gotErr)
	}
	if se.Class != SendClassTransient {
		t.Errorf("network error → Class=%v; want SendClassTransient (no status code, no response)", se.Class)
	}
	// Cause must be the underlying transport error (Unwrap chain reachable).
	if se.Cause == nil {
		t.Errorf("Cause = nil; want underlying transport error for errors.Is/As")
	}
}

// brevoErrorCounter snapshots metrics.BrevoSendErrorsTotal with the given
// labels. Returns 0 when no samples exist (the prometheus.CounterVec
// auto-creates labels on first .Inc, so absence == 0). Used by the metric
// regression tests to prove every classification path increments.
func brevoErrorCounter(t *testing.T, classification, statusCode string) float64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	metrics.BrevoSendErrorsTotal.Collect(ch)
	close(ch)
	for m := range ch {
		var d dto.Metric
		if err := m.Write(&d); err != nil {
			t.Fatalf("write metric: %v", err)
		}
		var gotClass, gotStatus string
		for _, lp := range d.Label {
			switch lp.GetName() {
			case "classification":
				gotClass = lp.GetValue()
			case "status_code":
				gotStatus = lp.GetValue()
			}
		}
		if gotClass == classification && gotStatus == statusCode {
			return d.GetCounter().GetValue()
		}
	}
	return 0
}

// TestBrevoProvider_Metrics_ClassifiesAllErrorPaths is the metric-side of
// the classification contract: every non-2xx branch (auth / throttle /
// permanent / 5xx / network) MUST increment
// brevo_send_errors_total{classification,status_code} by 1. If a future
// edit forgets to call .Inc() this test fails immediately, before the
// missing metric makes it to /metrics in prod and an alert silently never
// fires.
func TestBrevoProvider_Metrics_ClassifiesAllErrorPaths(t *testing.T) {
	cases := []struct {
		name           string
		status         int
		classification string
		statusLabel    string
	}{
		{"401 → transient", http.StatusUnauthorized, "transient", "401"},
		{"403 → transient", http.StatusForbidden, "transient", "403"},
		{"408 → transient", http.StatusRequestTimeout, "transient", "408"},
		{"425 → transient", http.StatusTooEarly, "transient", "425"},
		{"429 → transient", http.StatusTooManyRequests, "transient", "429"},
		{"500 → transient", http.StatusInternalServerError, "transient", "500"},
		{"503 → transient", http.StatusServiceUnavailable, "transient", "503"},
		{"400 → permanent", http.StatusBadRequest, "permanent", "400"},
		{"404 → permanent", http.StatusNotFound, "permanent", "404"},
		{"422 → permanent", http.StatusUnprocessableEntity, "permanent", "422"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := brevoErrorCounter(t, tc.classification, tc.statusLabel)
			srv, _ := fakeBrevo(t, tc.status)
			p := newTestProvider(t, srv, nil)
			_ = p.SendEvent(context.Background(), EventEmail{
				Kind:           "subscription.upgraded",
				Recipient:      "x@example.com",
				IdempotencyKey: "audit-metric-" + tc.name,
			})
			after := brevoErrorCounter(t, tc.classification, tc.statusLabel)
			if after-before != 1 {
				t.Errorf("status %d: counter{classification=%s,status_code=%s} delta=%v; want 1",
					tc.status, tc.classification, tc.statusLabel, after-before)
			}
		})
	}
}

// TestBrevoProvider_Metrics_NetworkError_Status0 pins the network-error
// labelling: when there's no response the metric MUST still increment
// (classification=transient, status_code="0"). Without status_code="0" the
// operator can't distinguish "we never reached Brevo" from "Brevo
// returned something" without grepping logs — the whole point of the
// label is to allow that distinction at the metric layer.
func TestBrevoProvider_Metrics_NetworkError_Status0(t *testing.T) {
	before := brevoErrorCounter(t, "transient", "0")
	p, err := NewBrevoProvider(BrevoConfig{APIKey: "k", TemplateIDs: map[string]int{"x": 1}})
	if err != nil {
		t.Fatal(err)
	}
	p.url = "http://127.0.0.1:1/closed"
	_ = p.SendEvent(context.Background(), EventEmail{
		Kind:           "x",
		Recipient:      "u@e.com",
		IdempotencyKey: "audit-net-metric",
	})
	after := brevoErrorCounter(t, "transient", "0")
	if after-before != 1 {
		t.Errorf("network error counter{transient,0} delta=%v; want 1 (status_code='0' distinguishes 'never reached Brevo' from upstream responses)", after-before)
	}
}

// TestBrevoProvider_Metrics_2xxDoesNotIncrement — success path MUST NOT
// touch brevo_send_errors_total. Otherwise the alert
// `rate(brevo_send_errors_total[5m]) > 0` fires on every healthy send,
// rendering the alert useless. This guards against an accidental
// "increment first, decide later" refactor.
func TestBrevoProvider_Metrics_2xxDoesNotIncrement(t *testing.T) {
	// Pre-create the (transient, 200) and (permanent, 200) label rows so
	// brevoErrorCounter has a starting sample to compare against, then
	// confirm a successful send leaves both unchanged.
	before := struct{ transient, permanent float64 }{
		brevoErrorCounter(t, "transient", "200"),
		brevoErrorCounter(t, "permanent", "200"),
	}
	srv, _ := fakeBrevo(t, http.StatusOK)
	p := newTestProvider(t, srv, map[string]int{"subscription.upgraded": 1})
	if err := p.SendEvent(context.Background(), EventEmail{
		Kind:           "subscription.upgraded",
		Recipient:      "u@e.com",
		IdempotencyKey: "audit-success",
	}); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}
	if got := brevoErrorCounter(t, "transient", "200"); got != before.transient {
		t.Errorf("transient/200 counter changed on success: before=%v after=%v", before.transient, got)
	}
	if got := brevoErrorCounter(t, "permanent", "200"); got != before.permanent {
		t.Errorf("permanent/200 counter changed on success: before=%v after=%v", before.permanent, got)
	}
}

// TestBrevoProvider_RetryDecision_Integration is the integration-style
// test the brief requires: stand up an httptest.Server that returns 429,
// drive a real Brevo request through it, then feed the returned error
// into a stand-in for River's "should this job retry?" predicate and
// assert it answers yes. We deliberately do NOT instantiate River — the
// classification semantics are River-agnostic and the test would
// otherwise need a Postgres + the River runtime just to assert a bool.
//
// The predicate mirrors what River's NextRetry does in practice:
// `ClassOf(err) == SendClassTransient` returns true (re-enqueue), any
// other class returns false (drop / dead-letter). When this test passes
// against a real 429 round-trip we have proved (a) the 429 reaches the
// classifier with the correct status, (b) the classifier returns
// Transient, (c) a River-equivalent retry predicate fires.
func TestBrevoProvider_RetryDecision_Integration(t *testing.T) {
	// shouldRetry is the fake retry-decision function the brief asks us to
	// assert. In production this is River's worker policy — here we
	// replicate the exact predicate the forwarder uses so the test pins
	// the contract: anything classified Transient retries, anything else
	// does not. If the classification semantics drift this test fails.
	shouldRetry := func(err error) bool { return ClassOf(err) == SendClassTransient }

	// 429 — must retry. Real round-trip through doRequest, not a fake.
	srv, _ := fakeBrevo(t, http.StatusTooManyRequests)
	p := newTestProvider(t, srv, map[string]int{"subscription.upgraded": 1})
	err := p.SendEvent(context.Background(), EventEmail{
		Kind:           "subscription.upgraded",
		Recipient:      "u@e.com",
		IdempotencyKey: "audit-retry-int-1",
	})
	if !shouldRetry(err) {
		t.Errorf("River-equivalent retry predicate said NO for 429 (err=%v); want YES (Transient → re-enqueue)", err)
	}

	// 400 — must NOT retry; permanent payload reject.
	srv2, _ := fakeBrevo(t, http.StatusBadRequest)
	p2 := newTestProvider(t, srv2, map[string]int{"subscription.upgraded": 1})
	err2 := p2.SendEvent(context.Background(), EventEmail{
		Kind:           "subscription.upgraded",
		Recipient:      "u@e.com",
		IdempotencyKey: "audit-retry-int-2",
	})
	if shouldRetry(err2) {
		t.Errorf("River-equivalent retry predicate said YES for 400 (err=%v); want NO (Permanent → dead-letter)", err2)
	}

	// 401 — must retry; account-level recoverable.
	srv3, _ := fakeBrevo(t, http.StatusUnauthorized)
	p3 := newTestProvider(t, srv3, map[string]int{"subscription.upgraded": 1})
	err3 := p3.SendEvent(context.Background(), EventEmail{
		Kind:           "subscription.upgraded",
		Recipient:      "u@e.com",
		IdempotencyKey: "audit-retry-int-3",
	})
	if !shouldRetry(err3) {
		t.Errorf("River-equivalent retry predicate said NO for 401 (err=%v); want YES (P0-1 contract: token rotation is recoverable)", err3)
	}

	// SkippedNoTemplate — must NOT retry; operator chose not to map this kind.
	srv4, _ := fakeBrevo(t, http.StatusOK)
	p4 := newTestProvider(t, srv4, map[string]int{"other": 1})
	err4 := p4.SendEvent(context.Background(), EventEmail{
		Kind:      "subscription.upgraded", // unmapped
		Recipient: "u@e.com",
	})
	if shouldRetry(err4) {
		t.Errorf("River-equivalent retry predicate said YES for SkippedNoTemplate (err=%v); want NO (advance cursor silently)", err4)
	}
}
