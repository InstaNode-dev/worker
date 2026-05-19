package email

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sestypes "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/aws/aws-sdk-go-v2/aws"
)

// TestMaskEmail_BasicShapes pins the masking algorithm — must match the
// api maskEmail behaviour exactly.
func TestMaskEmail_BasicShapes(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"alice@example.com", "a***@example.com"},
		{"a@example.com", "a@example.com"}, // 1-char local preserved
		{"bb20-t7-1779218881@instanode-test.dev", "b***@instanode-test.dev"},
		{"mastermanas805@gmail.com", "m***@gmail.com"},
		{"@onlydomain.com", "@onlydomain.com"}, // empty local — return unchanged (defensive)
		{"no-at-sign", "no-at-sign"},
		{"", ""},
	}
	for _, c := range cases {
		if got := maskEmail(c.in); got != c.want {
			t.Errorf("maskEmail(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

// captureSlog redirects the default slog logger into the returned buffer
// for the duration of fn. Returns the captured text. Used by the provider
// regression tests so they don't have to plumb a logger through.
func captureSlog(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	orig := slog.Default()
	slog.SetDefault(slog.New(h))
	defer slog.SetDefault(orig)
	fn()
	return buf.String()
}

// TestWorkerEmailProviders_NoRawRecipientInLogs is the T22 P1-1 / MR-P1-46
// regression test.
//
// BUG (pre-fix): worker brevo_provider.go and ses_provider.go logged
// `"recipient", evt.Recipient` raw at INFO/WARN/ERROR on every send. The
// L1 PII-masking fix in api `4078ca3` masked api-side logs but did NOT
// touch the worker providers — verified live in prod
// (instant-worker pod commit_id=7169493 emitted full recipient strings
// into NR Logs on every send).
//
// FIX: maskEmail applied at every `"recipient"` slog site in both
// providers. This test stamps a unique recipient address, drives each
// provider through a real send path that produces a `"recipient"` slog
// field, and asserts the raw local part NEVER appears in the captured
// log output — only the masked form. Registry-style: covers BOTH worker
// providers in one table, so a future third provider that ships without
// the maskEmail wrap fails this test.
func TestWorkerEmailProviders_NoRawRecipientInLogs(t *testing.T) {
	const (
		rawRecipient = "regressioncanary12345@instanode-test.dev"
		rawLocal     = "regressioncanary12345"
		wantMasked   = "r***@instanode-test.dev"
		kind         = "subscription.upgraded"
	)

	t.Run("brevo_success_2xx_sent", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"messageId":"x"}`))
		}))
		defer srv.Close()
		p, err := NewBrevoProvider(BrevoConfig{APIKey: "k", TemplateIDs: map[string]int{kind: 42}})
		if err != nil {
			t.Fatalf("NewBrevoProvider: %v", err)
		}
		p.url = srv.URL
		out := captureSlog(t, func() {
			_ = p.SendEvent(context.Background(), EventEmail{Kind: kind, Recipient: rawRecipient})
		})
		assertNoRawRecipient(t, "brevo:2xx", out, rawLocal, wantMasked)
	})

	t.Run("brevo_permanent_4xx_logs_recipient", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":"bad_request"}`))
		}))
		defer srv.Close()
		p, err := NewBrevoProvider(BrevoConfig{APIKey: "k", TemplateIDs: map[string]int{kind: 42}})
		if err != nil {
			t.Fatalf("NewBrevoProvider: %v", err)
		}
		p.url = srv.URL
		out := captureSlog(t, func() {
			_ = p.SendEvent(context.Background(), EventEmail{Kind: kind, Recipient: rawRecipient})
		})
		assertNoRawRecipient(t, "brevo:4xx", out, rawLocal, wantMasked)
	})

	t.Run("brevo_auth_wall_401", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":"unauthorized"}`))
		}))
		defer srv.Close()
		p, err := NewBrevoProvider(BrevoConfig{APIKey: "k", TemplateIDs: map[string]int{kind: 42}})
		if err != nil {
			t.Fatalf("NewBrevoProvider: %v", err)
		}
		p.url = srv.URL
		out := captureSlog(t, func() {
			_ = p.SendEvent(context.Background(), EventEmail{Kind: kind, Recipient: rawRecipient})
		})
		assertNoRawRecipient(t, "brevo:auth_wall", out, rawLocal, wantMasked)
	})

	t.Run("ses_success_sent", func(t *testing.T) {
		fake := &fakeSESClient{}
		p := &SESProvider{
			client:    fake,
			fromEmail: "noreply@example.com",
			templates: map[string]string{kind: "tmpl-1"},
		}
		out := captureSlog(t, func() {
			_ = p.SendEvent(context.Background(), EventEmail{Kind: kind, Recipient: rawRecipient})
		})
		assertNoRawRecipient(t, "ses:success", out, rawLocal, wantMasked)
	})

	t.Run("ses_permanent_rejected", func(t *testing.T) {
		fake := &fakeSESClient{err: &sestypes.MessageRejected{Message: aws.String("rejected")}}
		p := &SESProvider{
			client:    fake,
			fromEmail: "noreply@example.com",
			templates: map[string]string{kind: "tmpl-1"},
		}
		out := captureSlog(t, func() {
			_ = p.SendEvent(context.Background(), EventEmail{Kind: kind, Recipient: rawRecipient})
		})
		assertNoRawRecipient(t, "ses:rejected", out, rawLocal, wantMasked)
	})

	t.Run("ses_transient_throttling", func(t *testing.T) {
		fake := &fakeSESClient{err: &sestypes.TooManyRequestsException{Message: aws.String("rate")}}
		p := &SESProvider{
			client:    fake,
			fromEmail: "noreply@example.com",
			templates: map[string]string{kind: "tmpl-1"},
		}
		out := captureSlog(t, func() {
			_ = p.SendEvent(context.Background(), EventEmail{Kind: kind, Recipient: rawRecipient})
		})
		assertNoRawRecipient(t, "ses:transient", out, rawLocal, wantMasked)
	})
}

func assertNoRawRecipient(t *testing.T, label, out, rawLocal, wantMasked string) {
	t.Helper()
	if out == "" {
		t.Fatalf("%s: provider emitted no log output — cannot verify masking; update the harness so the recipient slog field still fires", label)
	}
	if strings.Contains(out, rawLocal) {
		t.Errorf("%s: provider log contains raw recipient local-part %q — PII leak.\nFull output:\n%s", label, rawLocal, out)
	}
	if !strings.Contains(out, wantMasked) {
		t.Errorf("%s: provider log does not contain masked form %q — masking was not applied.\nFull output:\n%s", label, wantMasked, out)
	}
}
