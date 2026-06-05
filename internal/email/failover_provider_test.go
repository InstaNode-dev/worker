package email

// failover_provider_test.go — in-package tests for the ordered multi-ESP
// FailoverProvider and the factory wiring (EMAIL_PROVIDER_FALLBACK).
//
// These pin the safety contract the task hangs on:
//   - inert default: no fallback configured → bare single provider, no wrapper.
//   - primary success short-circuits (no fallback call).
//   - primary failure → fallback engaged, first success wins.
//   - all fail → last error returned.
//   - ordering, Name(), per-outcome metric increments, email masking, guards.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"instant.dev/worker/internal/metrics"
)

// ── fakeProvider — recording EmailProvider test double ──────────────────────

// fakeProvider records each SendEvent call and returns a scripted result. It
// implements EmailProvider so it slots into the FailoverProvider chain and the
// factory. `calls` counts invocations so a test can assert "the fallback was
// NOT called" when the primary succeeds.
type fakeProvider struct {
	name   string
	msgID  string
	err    error
	calls  int
	gotEvt EventEmail
}

func (f *fakeProvider) SendEvent(_ context.Context, evt EventEmail) (string, error) {
	f.calls++
	f.gotEvt = evt
	return f.msgID, f.err
}

func (f *fakeProvider) Name() string { return f.name }

// skipErr / transientErr / permanentErr build *SendError of each class.
func skipErr() error      { return &SendError{Class: SendClassSkippedNoTemplate, Message: "no template"} }
func transientErr() error { return &SendError{Class: SendClassTransient, Message: "5xx"} }
func permanentErr() error { return &SendError{Class: SendClassPermanent, Message: "rejected"} }

// readFailover reads the current value of the failover counter for an outcome.
func readFailover(outcome string) float64 {
	return testutil.ToFloat64(metrics.EmailFailoverTotal.WithLabelValues(outcome))
}

// ── FailoverProvider behavior ───────────────────────────────────────────────

// TestFailover_PrimarySucceeds_NoFallbackCall — the happy path: the primary
// returns success, the chain stops, and the fallback is never invoked. Records
// primary_ok.
func TestFailover_PrimarySucceeds_NoFallbackCall(t *testing.T) {
	before := readFailover(failoverOutcomePrimaryOK)
	primary := &fakeProvider{name: "brevo", msgID: "msg-1"}
	fallback := &fakeProvider{name: "ses"}
	fp, err := NewFailoverProvider(primary, fallback)
	if err != nil {
		t.Fatalf("NewFailoverProvider = %v", err)
	}

	id, sendErr := fp.SendEvent(context.Background(), EventEmail{Kind: "k", Recipient: "u@example.com"})
	if sendErr != nil {
		t.Fatalf("SendEvent = %v; want nil", sendErr)
	}
	if id != "msg-1" {
		t.Errorf("messageID = %q; want msg-1", id)
	}
	if primary.calls != 1 {
		t.Errorf("primary calls = %d; want 1", primary.calls)
	}
	if fallback.calls != 0 {
		t.Errorf("fallback calls = %d; want 0 (primary succeeded)", fallback.calls)
	}
	if got := readFailover(failoverOutcomePrimaryOK); got != before+1 {
		t.Errorf("primary_ok counter = %v; want %v", got, before+1)
	}
}

// TestFailover_PrimaryFails_FallbackSucceeds — primary errors, fallback sends.
// Returns the fallback's messageID and records fallback_ok. Covers both a
// Transient and a Permanent primary error (the P0 case: Brevo "rejects" but
// SES sends).
func TestFailover_PrimaryFails_FallbackSucceeds(t *testing.T) {
	for _, tc := range []struct {
		name    string
		primErr error
	}{
		{"primary-transient", transientErr()},
		{"primary-permanent", permanentErr()}, // the P0 case
		{"primary-skip", skipErr()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := readFailover(failoverOutcomeFallbackOK)
			primary := &fakeProvider{name: "brevo", err: tc.primErr}
			fallback := &fakeProvider{name: "ses", msgID: "ses-9"}
			fp, _ := NewFailoverProvider(primary, fallback)

			id, sendErr := fp.SendEvent(context.Background(), EventEmail{Kind: "k", Recipient: "u@example.com"})
			if sendErr != nil {
				t.Fatalf("SendEvent = %v; want nil (fallback should succeed)", sendErr)
			}
			if id != "ses-9" {
				t.Errorf("messageID = %q; want ses-9 (from fallback)", id)
			}
			if primary.calls != 1 || fallback.calls != 1 {
				t.Errorf("calls primary=%d fallback=%d; want 1,1", primary.calls, fallback.calls)
			}
			if got := readFailover(failoverOutcomeFallbackOK); got != before+1 {
				t.Errorf("fallback_ok counter = %v; want %v", got, before+1)
			}
		})
	}
}

// TestFailover_AllFail_ReturnsLastError — every provider fails; the chain
// returns the LAST provider's error and records all_failed (because at least
// one real error occurred).
func TestFailover_AllFail_ReturnsLastError(t *testing.T) {
	before := readFailover(failoverOutcomeAllFailed)
	lastErr := &SendError{Class: SendClassTransient, Message: "ses-down"}
	primary := &fakeProvider{name: "brevo", err: permanentErr()}
	fallback := &fakeProvider{name: "ses", err: lastErr}
	fp, _ := NewFailoverProvider(primary, fallback)

	_, sendErr := fp.SendEvent(context.Background(), EventEmail{Kind: "k", Recipient: "u@example.com"})
	if !errors.Is(sendErr, lastErr) {
		t.Fatalf("SendEvent err = %v; want the LAST provider's error %v", sendErr, lastErr)
	}
	if primary.calls != 1 || fallback.calls != 1 {
		t.Errorf("calls primary=%d fallback=%d; want 1,1", primary.calls, fallback.calls)
	}
	if got := readFailover(failoverOutcomeAllFailed); got != before+1 {
		t.Errorf("all_failed counter = %v; want %v", got, before+1)
	}
}

// TestFailover_AllSkipped_NoAllFailedMetric — when every provider returns
// SkippedNoTemplate (no ESP has a template for this kind), the chain returns a
// skip error so the forwarder advances silently, and does NOT bump all_failed
// (an all-skip is not a degraded-ESP signal).
func TestFailover_AllSkipped_NoAllFailedMetric(t *testing.T) {
	before := readFailover(failoverOutcomeAllFailed)
	primary := &fakeProvider{name: "brevo", err: skipErr()}
	fallback := &fakeProvider{name: "ses", err: skipErr()}
	fp, _ := NewFailoverProvider(primary, fallback)

	_, sendErr := fp.SendEvent(context.Background(), EventEmail{Kind: "k", Recipient: "u@example.com"})
	if ClassOf(sendErr) != SendClassSkippedNoTemplate {
		t.Fatalf("ClassOf(err) = %v; want SkippedNoTemplate", ClassOf(sendErr))
	}
	if got := readFailover(failoverOutcomeAllFailed); got != before {
		t.Errorf("all_failed counter = %v; want unchanged %v (all-skip is not a failure)", got, before)
	}
}

// TestFailover_Ordering — providers are tried strictly in chain order. A
// three-element chain where the first two fail and the third succeeds must call
// all three in sequence and return the third's id.
func TestFailover_Ordering(t *testing.T) {
	p1 := &fakeProvider{name: "a", err: transientErr()}
	p2 := &fakeProvider{name: "b", err: permanentErr()}
	p3 := &fakeProvider{name: "c", msgID: "c-ok"}
	fp, _ := NewFailoverProvider(p1, p2, p3)

	id, err := fp.SendEvent(context.Background(), EventEmail{Kind: "k", Recipient: "u@example.com"})
	if err != nil {
		t.Fatalf("SendEvent = %v; want nil", err)
	}
	if id != "c-ok" {
		t.Errorf("messageID = %q; want c-ok", id)
	}
	if p1.calls != 1 || p2.calls != 1 || p3.calls != 1 {
		t.Errorf("calls a=%d b=%d c=%d; want 1,1,1", p1.calls, p2.calls, p3.calls)
	}
}

// TestFailover_Name — Name() reflects the ordered chain.
func TestFailover_Name(t *testing.T) {
	fp, _ := NewFailoverProvider(&fakeProvider{name: "brevo"}, &fakeProvider{name: "ses"})
	if got := fp.Name(); got != "failover(brevo,ses)" {
		t.Errorf("Name() = %q; want failover(brevo,ses)", got)
	}
}

// TestNewFailoverProvider_EmptyGuard — a failover with no providers (or only
// nils) is a programming error and must return an error, not a nil-deref at
// send time.
func TestNewFailoverProvider_EmptyGuard(t *testing.T) {
	if _, err := NewFailoverProvider(); err == nil {
		t.Error("NewFailoverProvider() = nil err; want error on empty chain")
	}
	if _, err := NewFailoverProvider(nil, nil); err == nil {
		t.Error("NewFailoverProvider(nil,nil) = nil err; want error on all-nil chain")
	}
}

// TestNewFailoverProvider_DropsNils — a nil entry inside an otherwise-valid
// chain is dropped, not dereferenced. The resulting Name() omits it.
func TestNewFailoverProvider_DropsNils(t *testing.T) {
	fp, err := NewFailoverProvider(nil, &fakeProvider{name: "ses"}, nil)
	if err != nil {
		t.Fatalf("NewFailoverProvider = %v", err)
	}
	if got := fp.Name(); got != "failover(ses)" {
		t.Errorf("Name() = %q; want failover(ses)", got)
	}
}

// TestFailover_MasksEmailInLogs — the recipient must be masked in the log lines
// the FailoverProvider emits (worker convention 7). We capture slog output on
// the fallback-engaged path (INFO) and assert the raw local-part never appears.
func TestFailover_MasksEmailInLogs(t *testing.T) {
	var buf bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(orig)

	primary := &fakeProvider{name: "brevo", err: permanentErr()}
	fallback := &fakeProvider{name: "ses", msgID: "ses-1"}
	fp, _ := NewFailoverProvider(primary, fallback)

	const addr = "alice@example.com"
	if _, err := fp.SendEvent(context.Background(), EventEmail{Kind: "k", Recipient: addr}); err != nil {
		t.Fatalf("SendEvent = %v", err)
	}
	out := buf.String()
	if strings.Contains(out, addr) {
		t.Errorf("log output leaked raw recipient %q:\n%s", addr, out)
	}
	if !strings.Contains(out, maskEmail(addr)) {
		t.Errorf("log output missing masked recipient %q:\n%s", maskEmail(addr), out)
	}
}

// TestFailover_AllFailMasksEmail — the all_failed (ERROR) path must also mask.
func TestFailover_AllFailMasksEmail(t *testing.T) {
	var buf bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(orig)

	fp, _ := NewFailoverProvider(
		&fakeProvider{name: "brevo", err: transientErr()},
		&fakeProvider{name: "ses", err: transientErr()},
	)
	const addr = "bob@example.com"
	_, _ = fp.SendEvent(context.Background(), EventEmail{Kind: "k", Recipient: addr})
	if strings.Contains(buf.String(), addr) {
		t.Errorf("all_failed log leaked raw recipient %q:\n%s", addr, buf.String())
	}
}

// TestFailover_PrimaryOkMasksEmail — the DEBUG happy-path line masks too.
func TestFailover_PrimaryOkMasksEmail(t *testing.T) {
	var buf bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(orig)

	fp, _ := NewFailoverProvider(&fakeProvider{name: "brevo", msgID: "ok"}, &fakeProvider{name: "ses"})
	const addr = "carol@example.com"
	_, _ = fp.SendEvent(context.Background(), EventEmail{Kind: "k", Recipient: addr})
	if strings.Contains(buf.String(), addr) {
		t.Errorf("primary_ok log leaked raw recipient %q:\n%s", addr, buf.String())
	}
}

// TestFailover_PropagatesEventToProviders — the same EventEmail reaches each
// provider unchanged (no mutation as it walks the chain).
func TestFailover_PropagatesEventToProviders(t *testing.T) {
	primary := &fakeProvider{name: "brevo", err: transientErr()}
	fallback := &fakeProvider{name: "ses", msgID: "x"}
	fp, _ := NewFailoverProvider(primary, fallback)
	evt := EventEmail{Kind: "subscription.upgraded", Recipient: "u@example.com", Subject: "Hi"}
	_, _ = fp.SendEvent(context.Background(), evt)
	if fallback.gotEvt.Kind != evt.Kind || fallback.gotEvt.Subject != evt.Subject {
		t.Errorf("fallback got %+v; want %+v", fallback.gotEvt, evt)
	}
}

// ── Factory wiring (EMAIL_PROVIDER_FALLBACK) ────────────────────────────────

// TestFactory_NoFallback_ReturnsBareProvider — THE INERT-DEFAULT CONTRACT.
// With no EMAIL_PROVIDER_FALLBACK, NewProvider returns the bare single
// provider (concrete *BrevoProvider), NOT a FailoverProvider wrapper. This is
// the proof of zero behavior change.
func TestFactory_NoFallback_ReturnsBareProvider(t *testing.T) {
	p, err := NewProvider(Config{
		Provider: providerNameBrevo,
		Brevo:    BrevoConfig{APIKey: "k"},
		// Fallbacks deliberately nil.
	})
	if err != nil {
		t.Fatalf("NewProvider = %v", err)
	}
	if _, ok := p.(*FailoverProvider); ok {
		t.Fatal("got *FailoverProvider with no fallback configured; want bare provider (inert default broken)")
	}
	if _, ok := p.(*BrevoProvider); !ok {
		t.Errorf("got %T; want *BrevoProvider", p)
	}
}

// TestFactory_WithFallback_ReturnsFailover — a configured fallback yields a
// FailoverProvider with the right ordered names.
func TestFactory_WithFallback_ReturnsFailover(t *testing.T) {
	p, err := NewProvider(Config{
		Provider:  providerNameBrevo,
		Brevo:     BrevoConfig{APIKey: "k"},
		SES:       SESConfig{AWSRegion: "us-east-1", AWSAccessKey: "a", AWSSecretKey: "s", FromEmail: "f@x.com"},
		Fallbacks: []string{providerNameSES},
	})
	if err != nil {
		t.Fatalf("NewProvider = %v", err)
	}
	fp, ok := p.(*FailoverProvider)
	if !ok {
		t.Fatalf("got %T; want *FailoverProvider", p)
	}
	if got := fp.Name(); got != "failover(brevo,ses)" {
		t.Errorf("Name() = %q; want failover(brevo,ses)", got)
	}
}

// TestFactory_UnknownFallback_Errors — a typo in EMAIL_PROVIDER_FALLBACK is a
// hard boot error (same strictness as the primary switch).
func TestFactory_UnknownFallback_Errors(t *testing.T) {
	_, err := NewProvider(Config{
		Provider:  providerNameBrevo,
		Brevo:     BrevoConfig{APIKey: "k"},
		Fallbacks: []string{"loops"},
	})
	if err == nil {
		t.Fatal("NewProvider with unknown fallback = nil; want hard boot error")
	}
}

// TestFactory_FallbackMissingCreds_Skipped — a VALID fallback name whose creds
// are unset (SES_* not wired) is skipped-with-warning, NOT a boot crash. With
// SES the only fallback and it skipped, the factory returns the bare primary.
func TestFactory_FallbackMissingCreds_Skipped(t *testing.T) {
	p, err := NewProvider(Config{
		Provider:  providerNameBrevo,
		Brevo:     BrevoConfig{APIKey: "k"},
		SES:       SESConfig{}, // no region/creds → NewSESProvider errors
		Fallbacks: []string{providerNameSES},
	})
	if err != nil {
		t.Fatalf("NewProvider = %v; want nil (missing-creds fallback should be skipped, not fatal)", err)
	}
	if _, ok := p.(*FailoverProvider); ok {
		t.Error("got *FailoverProvider; want bare primary (only fallback was skipped)")
	}
	if _, ok := p.(*BrevoProvider); !ok {
		t.Errorf("got %T; want *BrevoProvider (primary survives skipped fallback)", p)
	}
}

// TestFactory_FallbackWithBlankEntries_Tolerated — empty segments in the list
// (from "ses," or " , ses") are ignored, not treated as unknown providers.
func TestFactory_FallbackWithBlankEntries_Tolerated(t *testing.T) {
	p, err := NewProvider(Config{
		Provider:  providerNameBrevo,
		Brevo:     BrevoConfig{APIKey: "k"},
		SES:       SESConfig{AWSRegion: "us-east-1", AWSAccessKey: "a", AWSSecretKey: "s", FromEmail: "f@x.com"},
		Fallbacks: []string{"", providerNameSES, "  "},
	})
	if err != nil {
		t.Fatalf("NewProvider = %v", err)
	}
	if _, ok := p.(*FailoverProvider); !ok {
		t.Errorf("got %T; want *FailoverProvider", p)
	}
}

// TestFactory_PrimaryUnknown_StillErrors — the primary switch keeps its
// strictness through the buildOne refactor.
func TestFactory_PrimaryUnknown_StillErrors(t *testing.T) {
	if _, err := NewProvider(Config{Provider: "loops"}); err == nil {
		t.Fatal("NewProvider(loops primary) = nil; want error")
	}
}
