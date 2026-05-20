package email

// provider_test.go — interface-level + factory tests. These exist to pin
// the seam contract: anything calling EmailProvider must keep working
// regardless of which concrete provider is selected.

import (
	"context"
	"errors"
	"testing"
)

// TestFactory_EmptyProviderReturnsNoop — the default (operator hasn't set
// EMAIL_PROVIDER) MUST yield a working NoopProvider, not an error. This is
// the fail-open contract.
func TestFactory_EmptyProviderReturnsNoop(t *testing.T) {
	p, err := NewProvider(Config{Provider: ""})
	if err != nil {
		t.Fatalf("NewProvider(\"\") = err %v; want nil — empty provider must fall back to noop", err)
	}
	if p.Name() != providerNameNoop {
		t.Errorf("Name() = %q; want %q", p.Name(), providerNameNoop)
	}
}

// TestFactory_NoopProviderExplicit — operator who explicitly chose "noop"
// gets the same provider as the empty default.
func TestFactory_NoopProviderExplicit(t *testing.T) {
	p, err := NewProvider(Config{Provider: providerNameNoop})
	if err != nil {
		t.Fatalf("NewProvider(noop) = %v", err)
	}
	if _, ok := p.(*NoopProvider); !ok {
		t.Errorf("got %T; want *NoopProvider", p)
	}
}

// TestFactory_BrevoProvider — happy path: name + key produce a real
// BrevoProvider.
func TestFactory_BrevoProvider(t *testing.T) {
	p, err := NewProvider(Config{
		Provider: providerNameBrevo,
		Brevo:    BrevoConfig{APIKey: "k", TemplateIDs: map[string]int{"x": 1}},
	})
	if err != nil {
		t.Fatalf("NewProvider(brevo) = %v", err)
	}
	if p.Name() != providerNameBrevo {
		t.Errorf("Name() = %q; want %q", p.Name(), providerNameBrevo)
	}
}

// TestFactory_BrevoMissingKey_Errors — wiring brevo without the API key
// must boot-fail loudly. The factory propagates the error from
// NewBrevoProvider.
func TestFactory_BrevoMissingKey_Errors(t *testing.T) {
	_, err := NewProvider(Config{Provider: providerNameBrevo, Brevo: BrevoConfig{APIKey: ""}})
	if err == nil {
		t.Fatal("NewProvider(brevo) with empty APIKey = nil; want startup error")
	}
}

// TestFactory_UnknownProvider_Errors — typo in EMAIL_PROVIDER must be a
// startup error, not silently treated as noop.
func TestFactory_UnknownProvider_Errors(t *testing.T) {
	_, err := NewProvider(Config{Provider: "loops"})
	if err == nil {
		t.Fatal("NewProvider(loops) = nil; want error so a typo in EMAIL_PROVIDER fails fast")
	}
}

// TestNoopProvider_SendEventReturnsSkipped — NoopProvider classifies every
// send as SkippedNoTemplate so the forwarder advances the cursor without
// flagging permanent or transient errors.
func TestNoopProvider_SendEventReturnsSkipped(t *testing.T) {
	n := &NoopProvider{}
	_, err := n.SendEvent(context.Background(), EventEmail{
		Kind:      "anything",
		Recipient: "u@example.com",
	})
	var se *SendError
	if !errors.As(err, &se) {
		t.Fatalf("noop SendEvent returned %T; want *SendError", err)
	}
	if se.Class != SendClassSkippedNoTemplate {
		t.Errorf("noop Class = %v; want SendClassSkippedNoTemplate", se.Class)
	}
}

// TestNoopProvider_Name — stable identifier used by log labels.
func TestNoopProvider_Name(t *testing.T) {
	if got := (&NoopProvider{}).Name(); got != providerNameNoop {
		t.Errorf("Name() = %q; want %q", got, providerNameNoop)
	}
}

// TestSendClass_String — the slog field MUST match these stable values
// so dashboard queries (`class:transient`) don't break on a rename.
func TestSendClass_String(t *testing.T) {
	cases := []struct {
		c    SendClass
		want string
	}{
		{SendClassPermanent, "permanent"},
		{SendClassTransient, "transient"},
		{SendClassSkippedNoTemplate, "skipped_no_template"},
	}
	for _, tc := range cases {
		if got := tc.c.String(); got != tc.want {
			t.Errorf("%d.String() = %q; want %q", tc.c, got, tc.want)
		}
	}
}

// TestSendError_ErrorFormat — basic shape of Error(). Catches an
// accidental break in the format that operators read in logs.
func TestSendError_ErrorFormat(t *testing.T) {
	e := &SendError{Class: SendClassTransient, Message: "brevo: 503", Cause: errors.New("EOF")}
	got := e.Error()
	for _, want := range []string{"transient", "brevo: 503", "EOF"} {
		if !contains(got, want) {
			t.Errorf("Error() = %q; want substring %q", got, want)
		}
	}
}

// TestClassOf_NilNonSendError — a non-*SendError must classify as Transient
// (fail-safe: an unknown error type holds the cursor, doesn't advance past).
func TestClassOf_NonSendErrorIsTransient(t *testing.T) {
	if got := ClassOf(errors.New("plain")); got != SendClassTransient {
		t.Errorf("ClassOf(plain) = %v; want SendClassTransient", got)
	}
}

// TestClassOf_SendError — recovers the wrapped class.
func TestClassOf_SendError(t *testing.T) {
	se := &SendError{Class: SendClassPermanent}
	if got := ClassOf(se); got != SendClassPermanent {
		t.Errorf("ClassOf(SendError{Permanent}) = %v; want SendClassPermanent", got)
	}
}

// contains is a tiny local helper so this file doesn't have to import strings
// for one substring check. Returns true iff substr appears in s.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && indexOf(s, substr) >= 0
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
