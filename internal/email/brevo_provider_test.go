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

// TestBrevoProvider_4xxReturnsPermanent verifies the "advance past poisoned
// row" contract: a 401 (or any 4xx) → *SendError{Class: SendClassPermanent}.
func TestBrevoProvider_4xxReturnsPermanent(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity} {
		srv, _ := fakeBrevo(t, status)
		p := newTestProvider(t, srv, nil)

		err := p.SendEvent(context.Background(), EventEmail{
			Kind:      "subscription.upgraded",
			Recipient: "x@example.com",
		})
		if err == nil {
			t.Errorf("SendEvent on %d = nil; want SendError(Permanent)", status)
			continue
		}
		var se *SendError
		if !errors.As(err, &se) {
			t.Errorf("SendEvent on %d returned %T; want *SendError", status, err)
			continue
		}
		if se.Class != SendClassPermanent {
			t.Errorf("SendEvent on %d → Class=%v; want SendClassPermanent — holding cursor on auth errors pins the queue forever", status, se.Class)
		}
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
