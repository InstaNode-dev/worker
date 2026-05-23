package email

// coverage_test.go — small-target tests that pin the remaining
// uncovered branches in provider.go (SendError.Error all variants,
// Unwrap, ClassOf(nil), SendClass.String(unknown)) and the
// parseBrevoMessageID parse-failure branch in brevo_provider.go.

import (
	"context"
	"errors"
	"testing"
)

// TestSendError_Error_AllBranches — the four-case switch in
// SendError.Error() (message+cause / message-only / cause-only /
// neither). Three of these were uncovered by the existing test suite
// because the helper happy-paths populate both fields.
func TestSendError_Error_AllBranches(t *testing.T) {
	cause := errors.New("inner")
	cases := []struct {
		name    string
		err     *SendError
		want    string
	}{
		{
			"both-message-and-cause",
			&SendError{Class: SendClassTransient, Message: "ctx-msg", Cause: cause},
			"transient: ctx-msg: inner",
		},
		{
			"message-only",
			&SendError{Class: SendClassPermanent, Message: "msg-only"},
			"permanent: msg-only",
		},
		{
			"cause-only",
			&SendError{Class: SendClassSkippedNoTemplate, Cause: cause},
			"skipped_no_template: inner",
		},
		{
			"class-only",
			&SendError{Class: SendClassTransient},
			"transient",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.err.Error(); got != tc.want {
				t.Errorf("Error() = %q; want %q", got, tc.want)
			}
		})
	}
}

// TestSendError_Unwrap — the errors.As / errors.Is chain depends on
// Unwrap() returning the embedded Cause verbatim.
func TestSendError_Unwrap(t *testing.T) {
	inner := errors.New("wrapped")
	e := &SendError{Class: SendClassTransient, Cause: inner}
	if got := e.Unwrap(); got != inner {
		t.Errorf("Unwrap() = %v; want %v", got, inner)
	}
	// errors.Is must also walk through the unwrap chain.
	if !errors.Is(e, inner) {
		t.Error("errors.Is did not recover the wrapped cause via Unwrap()")
	}

	// Nil-cause Unwrap returns nil — pins the "Cause set explicitly to
	// nil" branch behaviour.
	if got := (&SendError{}).Unwrap(); got != nil {
		t.Errorf("Unwrap() on nil Cause = %v; want nil", got)
	}
}

// TestClassOf_Nil — the "unreachable from a healthy caller" branch is
// nonetheless reachable from a malicious or test caller, and the
// classifier MUST return SendClassPermanent there per the function
// docs.
func TestClassOf_Nil(t *testing.T) {
	if got := ClassOf(nil); got != SendClassPermanent {
		t.Errorf("ClassOf(nil) = %v; want SendClassPermanent (per docs)", got)
	}
}

// TestSendClass_String_Unknown — covers the default branch in
// SendClass.String(). A SendClass beyond the three documented values
// produces "unknown" so dashboards see a stable bucket instead of an
// empty string.
func TestSendClass_String_Unknown(t *testing.T) {
	if got := SendClass(99).String(); got != "unknown" {
		t.Errorf("SendClass(99).String() = %q; want unknown", got)
	}
}

// TestParseBrevoMessageID_InvalidJSON — the parseBrevoMessageID helper
// must return "" on malformed JSON without panicking. The 2xx success
// path's ledger-row fallback (IdempotencyKey) depends on this never
// crashing the worker.
func TestParseBrevoMessageID_InvalidJSON(t *testing.T) {
	for _, body := range [][]byte{
		[]byte(`{not-valid-json`),
		[]byte(`<html>oops</html>`),
		[]byte(`{}`), // valid JSON but no messageId field
		[]byte(`null`),
	} {
		if got := parseBrevoMessageID(body); got != "" {
			t.Errorf("parseBrevoMessageID(%q) = %q; want \"\" (parse failure or missing field)", body, got)
		}
	}
}

// TestParseBrevoMessageID_Empty — empty body short-circuits without
// touching json.Unmarshal.
func TestParseBrevoMessageID_Empty(t *testing.T) {
	if got := parseBrevoMessageID(nil); got != "" {
		t.Errorf("parseBrevoMessageID(nil) = %q; want \"\"", got)
	}
	if got := parseBrevoMessageID([]byte{}); got != "" {
		t.Errorf("parseBrevoMessageID([]) = %q; want \"\"", got)
	}
}

// TestParseBrevoMessageID_Happy — happy path round-trips the
// upstream messageId field.
func TestParseBrevoMessageID_Happy(t *testing.T) {
	got := parseBrevoMessageID([]byte(`{"messageId":"abc-xyz"}`))
	if got != "abc-xyz" {
		t.Errorf("parseBrevoMessageID = %q; want abc-xyz", got)
	}
}

// TestBrevoProvider_DoRequest_BuildRequestFails — when the provider's
// URL contains a NULL byte (or other control char), http.NewRequestWithContext
// returns an error at request-build time. The provider MUST classify
// this as Transient (the URL is a programming bug; we want the operator
// to see it on every tick rather than advancing past silently). This
// covers the `if err != nil` branch immediately after http.NewRequestWithContext
// inside doRequest, which is otherwise unreachable from a healthy URL.
func TestBrevoProvider_DoRequest_BuildRequestFails(t *testing.T) {
	p, err := NewBrevoProvider(BrevoConfig{APIKey: "k", TemplateIDs: map[string]int{"x": 1}})
	if err != nil {
		t.Fatal(err)
	}
	// A NULL byte in the URL trips http.NewRequestWithContext's
	// validateNet token check.
	p.url = "http://localhost\x00:80/path"
	_, gotErr := p.SendEvent(context.Background(), EventEmail{
		Kind:           "x",
		Recipient:      "u@e.com",
		IdempotencyKey: "audit-build-fail",
	})
	if gotErr == nil {
		t.Fatal("malformed URL → nil; want SendError(Transient)")
	}
	var se *SendError
	if !errors.As(gotErr, &se) {
		t.Fatalf("got %T; want *SendError", gotErr)
	}
	if se.Class != SendClassTransient {
		t.Errorf("malformed URL → Class=%v; want SendClassTransient (programming-bug holds cursor)", se.Class)
	}
}
