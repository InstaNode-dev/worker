package email

// ses_provider_test.go — hermetic tests for the SES provider.
//
// Uses a fakeSESClient stand-in for *sesv2.Client (via the sesSendEmailAPI
// interface) so we never hit the live AWS API. Each test exercises exactly
// one classification path:
//
//   - happy POST                                 → nil
//   - MessageRejected / InvalidParameter / etc.  → SendClassPermanent
//   - ThrottlingException / 5xx / network        → SendClassTransient
//   - context.DeadlineExceeded                   → SendClassTransient
//   - missing template                           → SendClassSkippedNoTemplate
//   - empty recipient                            → SendClassPermanent
//   - empty config                               → factory boot-fail
//
// Lives in package `email` so it can construct SESProvider directly with
// a fake client (sesSendEmailAPI is unexported).

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	sestypes "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/aws/smithy-go"
)

// fakeSESClient implements sesSendEmailAPI without touching the network.
// recordedInput captures the last SendEmailInput so tests can assert on
// FromEmailAddress / Destination / Template fields. err is returned on
// each call (nil = success path).
type fakeSESClient struct {
	called        int
	recordedInput *sesv2.SendEmailInput
	err           error
}

func (f *fakeSESClient) SendEmail(_ context.Context, in *sesv2.SendEmailInput, _ ...func(*sesv2.Options)) (*sesv2.SendEmailOutput, error) {
	f.called++
	f.recordedInput = in
	if f.err != nil {
		return nil, f.err
	}
	return &sesv2.SendEmailOutput{MessageId: aws.String("msg-1")}, nil
}

// newTestSESProvider builds an SESProvider with a fake client and the
// given templates. Centralises the boilerplate so each test reads cleanly.
func newTestSESProvider(t *testing.T, client sesSendEmailAPI, templates map[string]string) *SESProvider {
	t.Helper()
	if templates == nil {
		templates = map[string]string{"subscription.upgraded": "tier-upgraded-v1"}
	}
	return &SESProvider{
		client:    client,
		fromEmail: "noreply@example.com",
		templates: templates,
	}
}

// TestSESProvider_New_RequiresRegion — fast boot fail when SES is selected
// without a region. SES is region-scoped, no sensible default.
func TestSESProvider_New_RequiresRegion(t *testing.T) {
	_, err := NewSESProvider(SESConfig{
		AWSAccessKey: "k",
		AWSSecretKey: "s",
		FromEmail:    "noreply@example.com",
	})
	if err == nil {
		t.Fatal("NewSESProvider without region = nil; want error so EMAIL_PROVIDER=ses without SES_AWS_REGION fails fast")
	}
}

// TestSESProvider_New_RequiresAccessKeys — fast boot fail on missing creds.
// Operator opted into SES; a silent no-op would hide the misconfiguration.
func TestSESProvider_New_RequiresAccessKeys(t *testing.T) {
	_, err := NewSESProvider(SESConfig{
		AWSRegion: "us-east-1",
		FromEmail: "noreply@example.com",
	})
	if err == nil {
		t.Fatal("NewSESProvider without access keys = nil; want fast boot failure")
	}
}

// TestSESProvider_New_RequiresFromEmail — SES requires a verified sender
// address; an unset SES_FROM_EMAIL means every send will fail anyway, so
// boot-fail loudly here instead of silently at first send.
func TestSESProvider_New_RequiresFromEmail(t *testing.T) {
	_, err := NewSESProvider(SESConfig{
		AWSRegion:    "us-east-1",
		AWSAccessKey: "k",
		AWSSecretKey: "s",
	})
	if err == nil {
		t.Fatal("NewSESProvider without FromEmail = nil; want fast boot failure (SES requires verified sender)")
	}
}

// TestSESProvider_New_Happy — region + creds + from-email + empty templates
// returns a valid provider (empty templates is operator's "API ready, no
// templates yet" state, not a boot failure).
func TestSESProvider_New_Happy(t *testing.T) {
	p, err := NewSESProvider(SESConfig{
		AWSRegion:    "us-east-1",
		AWSAccessKey: "k",
		AWSSecretKey: "s",
		FromEmail:    "noreply@example.com",
	})
	if err != nil {
		t.Fatalf("NewSESProvider happy path: %v", err)
	}
	if p.Name() != "ses" {
		t.Errorf("Name() = %q; want ses", p.Name())
	}
	if p.fromEmail != "noreply@example.com" {
		t.Errorf("fromEmail = %q; want noreply@example.com", p.fromEmail)
	}
	// Empty templates map is non-nil so SendEvent's lookup is safe.
	if p.templates == nil {
		t.Error("templates = nil; want empty map for safe lookups")
	}
}

// TestSESProvider_Name_IsStable — slog/metric label MUST NOT drift.
func TestSESProvider_Name_IsStable(t *testing.T) {
	p := newTestSESProvider(t, &fakeSESClient{}, nil)
	if got := p.Name(); got != "ses" {
		t.Errorf("Name() = %q; want ses (matches providerNameSES + docs)", got)
	}
}

// TestSESProvider_HappyPath — successful send returns nil and the
// forwarder advances the cursor. Asserts the wire shape: FromEmailAddress,
// ToAddresses[0], Template.TemplateName, and TemplateData is a JSON
// string of the params map.
func TestSESProvider_HappyPath(t *testing.T) {
	fake := &fakeSESClient{}
	p := newTestSESProvider(t, fake, map[string]string{"subscription.upgraded": "tier-upgraded-v1"})

	_, err := p.SendEvent(context.Background(), EventEmail{
		Kind:           "subscription.upgraded",
		Recipient:      "user@example.com",
		RecipientName:  "User",
		Params:         map[string]string{"from_tier": "hobby", "to_tier": "pro"},
		IdempotencyKey: "audit-123",
	})

	if err != nil {
		t.Fatalf("SendEvent happy path = %v; want nil", err)
	}
	if fake.called != 1 {
		t.Errorf("SendEmail called %d times; want 1", fake.called)
	}
	in := fake.recordedInput
	if in == nil {
		t.Fatal("recordedInput = nil; SendEmail was not invoked with an input")
	}
	if aws.ToString(in.FromEmailAddress) != "noreply@example.com" {
		t.Errorf("FromEmailAddress = %q; want noreply@example.com", aws.ToString(in.FromEmailAddress))
	}
	if in.Destination == nil || len(in.Destination.ToAddresses) != 1 || in.Destination.ToAddresses[0] != "user@example.com" {
		t.Errorf("Destination.ToAddresses = %+v; want [user@example.com]", in.Destination)
	}
	if in.Content == nil || in.Content.Template == nil {
		t.Fatalf("Content.Template = nil; want template payload")
	}
	if aws.ToString(in.Content.Template.TemplateName) != "tier-upgraded-v1" {
		t.Errorf("Template.TemplateName = %q; want tier-upgraded-v1", aws.ToString(in.Content.Template.TemplateName))
	}

	// TemplateData must be a JSON string with flat string-keyed params.
	// SES expects this shape exactly — string→string aligns with the
	// EventEmail.Params contract so we don't negotiate JSON types per call.
	td := aws.ToString(in.Content.Template.TemplateData)
	var got map[string]string
	if err := json.Unmarshal([]byte(td), &got); err != nil {
		t.Fatalf("TemplateData not valid JSON: %v (raw=%q)", err, td)
	}
	if got["from_tier"] != "hobby" || got["to_tier"] != "pro" {
		t.Errorf("TemplateData params = %+v; want from_tier=hobby to_tier=pro", got)
	}
}

// TestSESProvider_MissingTemplate_SkipsNoTemplate — operator hasn't wired
// this Kind yet. Forwarder advances silently AND we MUST NOT make an SES
// API call (burns no quota on unconfigured events).
func TestSESProvider_MissingTemplate_SkipsNoTemplate(t *testing.T) {
	fake := &fakeSESClient{}
	p := newTestSESProvider(t, fake, map[string]string{"subscription.upgraded": "tier-upgraded-v1"})

	_, err := p.SendEvent(context.Background(), EventEmail{
		Kind:      "experiment.conversion", // not in template map
		Recipient: "x@example.com",
	})

	var se *SendError
	if !errors.As(err, &se) {
		t.Fatalf("missing template = %v; want *SendError", err)
	}
	if se.Class != SendClassSkippedNoTemplate {
		t.Errorf("Class = %v; want SendClassSkippedNoTemplate", se.Class)
	}
	if fake.called != 0 {
		t.Errorf("SES was called for unmapped kind (%d times); want 0 — should short-circuit locally", fake.called)
	}
}

// TestSESProvider_EmptyTemplates_AllSkip — empty SES_TEMPLATE_NAMES means
// every send returns SkippedNoTemplate. Operator's "credentials set, no
// templates yet" state — mirrors the Brevo provider's behaviour.
func TestSESProvider_EmptyTemplates_AllSkip(t *testing.T) {
	fake := &fakeSESClient{}
	p := newTestSESProvider(t, fake, map[string]string{})

	for _, kind := range []string{"subscription.upgraded", "near_quota_wall", "admin.tier_changed"} {
		_, err := p.SendEvent(context.Background(), EventEmail{
			Kind:      kind,
			Recipient: "x@example.com",
		})
		var se *SendError
		if !errors.As(err, &se) || se.Class != SendClassSkippedNoTemplate {
			t.Errorf("kind=%q empty-templates → %v; want SendClassSkippedNoTemplate", kind, err)
		}
	}
	if fake.called != 0 {
		t.Errorf("SES called %d times with empty templates; want 0", fake.called)
	}
}

// TestSESProvider_EmptyRecipient_ReturnsPermanent — defensive guard:
// orphan rows the forwarder didn't filter out MUST advance, not loop.
func TestSESProvider_EmptyRecipient_ReturnsPermanent(t *testing.T) {
	fake := &fakeSESClient{}
	p := newTestSESProvider(t, fake, nil)

	_, err := p.SendEvent(context.Background(), EventEmail{
		Kind:      "subscription.upgraded",
		Recipient: "",
	})
	var se *SendError
	if !errors.As(err, &se) || se.Class != SendClassPermanent {
		t.Errorf("empty recipient → %v; want SendClassPermanent", err)
	}
	if fake.called != 0 {
		t.Errorf("SES called %d times for empty recipient; want 0 (short-circuit)", fake.called)
	}
}

// TestSESProvider_MessageRejected_IsPermanent — the canonical "bad
// payload" case (SES typed MessageRejected → FaultClient, ErrorCode
// "MessageRejected"). Cursor must advance + ERROR log fires.
func TestSESProvider_MessageRejected_IsPermanent(t *testing.T) {
	fake := &fakeSESClient{err: &sestypes.MessageRejected{Message: aws.String("recipient not verified")}}
	p := newTestSESProvider(t, fake, nil)

	_, err := p.SendEvent(context.Background(), EventEmail{
		Kind:      "subscription.upgraded",
		Recipient: "x@example.com",
	})
	var se *SendError
	if !errors.As(err, &se) {
		t.Fatalf("got %T; want *SendError", err)
	}
	if se.Class != SendClassPermanent {
		t.Errorf("MessageRejected → Class=%v; want SendClassPermanent (cursor must advance past unverified-recipient rows in SES sandbox)", se.Class)
	}
}

// TestSESProvider_NotFoundException_IsPermanent — template-name typo (or
// the template was deleted in the SES console). NotFoundException is a
// client fault and retrying won't help until the operator fixes the map.
func TestSESProvider_NotFoundException_IsPermanent(t *testing.T) {
	fake := &fakeSESClient{err: &sestypes.NotFoundException{Message: aws.String("template not found")}}
	p := newTestSESProvider(t, fake, nil)

	_, err := p.SendEvent(context.Background(), EventEmail{Kind: "subscription.upgraded", Recipient: "x@example.com"})
	var se *SendError
	if !errors.As(err, &se) || se.Class != SendClassPermanent {
		t.Errorf("NotFoundException → %v; want SendClassPermanent", err)
	}
}

// TestSESProvider_BadRequest_IsPermanent — generic 4xx-like client fault.
func TestSESProvider_BadRequest_IsPermanent(t *testing.T) {
	fake := &fakeSESClient{err: &sestypes.BadRequestException{Message: aws.String("invalid template data")}}
	p := newTestSESProvider(t, fake, nil)

	_, err := p.SendEvent(context.Background(), EventEmail{Kind: "subscription.upgraded", Recipient: "x@example.com"})
	var se *SendError
	if !errors.As(err, &se) || se.Class != SendClassPermanent {
		t.Errorf("BadRequestException → %v; want SendClassPermanent", err)
	}
}

// TestSESProvider_AccountSuspended_IsPermanent — operator account is
// suspended (compliance / billing issue). Every send will fail until
// AWS support fixes it; advancing the cursor avoids burning through
// retries while the issue is in someone else's hands.
func TestSESProvider_AccountSuspended_IsPermanent(t *testing.T) {
	fake := &fakeSESClient{err: &sestypes.AccountSuspendedException{Message: aws.String("account suspended")}}
	p := newTestSESProvider(t, fake, nil)

	_, err := p.SendEvent(context.Background(), EventEmail{Kind: "subscription.upgraded", Recipient: "x@example.com"})
	var se *SendError
	if !errors.As(err, &se) || se.Class != SendClassPermanent {
		t.Errorf("AccountSuspendedException → %v; want SendClassPermanent", err)
	}
}

// TestSESProvider_Throttling_IsTransient — SES rate-limit. Forwarder
// holds the cursor and the next tick retries. Note SES TooManyRequests
// is FaultClient but is definitionally retryable, so we override the
// fault → permanent default for this code.
func TestSESProvider_Throttling_IsTransient(t *testing.T) {
	fake := &fakeSESClient{err: &sestypes.TooManyRequestsException{Message: aws.String("rate exceeded")}}
	p := newTestSESProvider(t, fake, nil)

	_, err := p.SendEvent(context.Background(), EventEmail{Kind: "subscription.upgraded", Recipient: "x@example.com"})
	var se *SendError
	if !errors.As(err, &se) {
		t.Fatalf("got %T; want *SendError", err)
	}
	if se.Class != SendClassTransient {
		t.Errorf("TooManyRequestsException → Class=%v; want SendClassTransient (rate-limit should retry next tick, not advance past)", se.Class)
	}
}

// TestSESProvider_SendingPaused_IsTransient — SES sending is paused
// account-wide (operator can unpause from dashboard). Treat as Transient
// so we don't burn through audit rows while paused; once unpaused the
// cursor picks up where it left off.
func TestSESProvider_SendingPaused_IsTransient(t *testing.T) {
	fake := &fakeSESClient{err: &sestypes.SendingPausedException{Message: aws.String("sending paused")}}
	p := newTestSESProvider(t, fake, nil)

	_, err := p.SendEvent(context.Background(), EventEmail{Kind: "subscription.upgraded", Recipient: "x@example.com"})
	var se *SendError
	if !errors.As(err, &se) || se.Class != SendClassTransient {
		t.Errorf("SendingPausedException → %v; want SendClassTransient", err)
	}
}

// TestSESProvider_InternalServiceError_IsTransient — SES is unhealthy
// on their side. Hold cursor, retry next tick.
func TestSESProvider_InternalServiceError_IsTransient(t *testing.T) {
	fake := &fakeSESClient{err: &sestypes.InternalServiceErrorException{Message: aws.String("internal error")}}
	p := newTestSESProvider(t, fake, nil)

	_, err := p.SendEvent(context.Background(), EventEmail{Kind: "subscription.upgraded", Recipient: "x@example.com"})
	var se *SendError
	if !errors.As(err, &se) || se.Class != SendClassTransient {
		t.Errorf("InternalServiceErrorException → %v; want SendClassTransient", err)
	}
}

// TestSESProvider_UnknownServerFault_IsTransient — exercises the ErrorFault()
// fallback path: unknown error code with FaultServer → Transient.
func TestSESProvider_UnknownServerFault_IsTransient(t *testing.T) {
	fake := &fakeSESClient{err: &smithy.GenericAPIError{Code: "SomeFutureUpstreamFailure", Message: "x", Fault: smithy.FaultServer}}
	p := newTestSESProvider(t, fake, nil)

	_, err := p.SendEvent(context.Background(), EventEmail{Kind: "subscription.upgraded", Recipient: "x@example.com"})
	var se *SendError
	if !errors.As(err, &se) || se.Class != SendClassTransient {
		t.Errorf("unknown server-fault → %v; want SendClassTransient (fail-safe for unknown 5xx-likes)", err)
	}
}

// TestSESProvider_UnknownClientFault_IsPermanent — unknown error code
// with FaultClient → Permanent. Fallback for new/undocumented AWS error
// codes that look like 4xx-equivalents.
func TestSESProvider_UnknownClientFault_IsPermanent(t *testing.T) {
	fake := &fakeSESClient{err: &smithy.GenericAPIError{Code: "SomeFutureClientError", Message: "x", Fault: smithy.FaultClient}}
	p := newTestSESProvider(t, fake, nil)

	_, err := p.SendEvent(context.Background(), EventEmail{Kind: "subscription.upgraded", Recipient: "x@example.com"})
	var se *SendError
	if !errors.As(err, &se) || se.Class != SendClassPermanent {
		t.Errorf("unknown client-fault → %v; want SendClassPermanent (advance past unrecognised 4xx)", err)
	}
}

// TestSESProvider_AuthError_IsPermanent — operator's AWS credentials are
// bad (typo / rotated / revoked). Every row will fail the same way until
// someone fixes the secret; advancing the cursor is correct.
func TestSESProvider_AuthError_IsPermanent(t *testing.T) {
	fake := &fakeSESClient{err: &smithy.GenericAPIError{Code: "UnrecognizedClientException", Message: "bad creds", Fault: smithy.FaultClient}}
	p := newTestSESProvider(t, fake, nil)

	_, err := p.SendEvent(context.Background(), EventEmail{Kind: "subscription.upgraded", Recipient: "x@example.com"})
	var se *SendError
	if !errors.As(err, &se) || se.Class != SendClassPermanent {
		t.Errorf("UnrecognizedClientException → %v; want SendClassPermanent", err)
	}
}

// fakeNetErr is a net.Error implementation for testing the network-error
// branch of classifySESError. The real *net.OpError satisfies net.Error
// too; this minimal stand-in keeps the test hermetic.
type fakeNetErr struct{ msg string }

func (f *fakeNetErr) Error() string   { return f.msg }
func (f *fakeNetErr) Timeout() bool   { return false }
func (f *fakeNetErr) Temporary() bool { return true }

var _ net.Error = (*fakeNetErr)(nil)

// TestSESProvider_NetworkError_IsTransient — dns / connection-refused /
// reset / timeout. Definitionally retryable.
func TestSESProvider_NetworkError_IsTransient(t *testing.T) {
	fake := &fakeSESClient{err: &fakeNetErr{msg: "connection refused"}}
	p := newTestSESProvider(t, fake, nil)

	_, err := p.SendEvent(context.Background(), EventEmail{Kind: "subscription.upgraded", Recipient: "x@example.com"})
	var se *SendError
	if !errors.As(err, &se) || se.Class != SendClassTransient {
		t.Errorf("net.Error → %v; want SendClassTransient", err)
	}
}

// TestSESProvider_ContextCanceled_IsTransient — caller's context died.
// Forwarder retries next tick on a fresh context.
func TestSESProvider_ContextCanceled_IsTransient(t *testing.T) {
	// Use a real expired context so errors.Is(err, context.DeadlineExceeded)
	// matches. We pass the error directly through the fake client to skip
	// SES's wrapping behaviour and exercise the classifier directly.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(2 * time.Nanosecond)

	fake := &fakeSESClient{err: ctx.Err()}
	p := newTestSESProvider(t, fake, nil)

	_, err := p.SendEvent(context.Background(), EventEmail{Kind: "subscription.upgraded", Recipient: "x@example.com"})
	var se *SendError
	if !errors.As(err, &se) || se.Class != SendClassTransient {
		t.Errorf("context.DeadlineExceeded → %v; want SendClassTransient", err)
	}
}

// TestSESProvider_UnknownError_IsTransient — fail-safe for an error type
// we don't recognise: hold cursor (mirror ClassOf's default).
func TestSESProvider_UnknownError_IsTransient(t *testing.T) {
	fake := &fakeSESClient{err: errors.New("something we don't model")}
	p := newTestSESProvider(t, fake, nil)

	_, err := p.SendEvent(context.Background(), EventEmail{Kind: "subscription.upgraded", Recipient: "x@example.com"})
	var se *SendError
	if !errors.As(err, &se) || se.Class != SendClassTransient {
		t.Errorf("unknown error → %v; want SendClassTransient (fail-safe hold)", err)
	}
}

// TestSESProvider_TemplateDataIsFlatStringMap — the wire shape MUST be a
// flat string→string JSON object. SES's mustache-style templates can't
// reference nested objects, so any future param shape change must keep
// this property. This test catches an accidental switch to e.g. a typed
// struct that emits non-string values.
func TestSESProvider_TemplateDataIsFlatStringMap(t *testing.T) {
	fake := &fakeSESClient{}
	p := newTestSESProvider(t, fake, map[string]string{"subscription.upgraded": "tier-upgraded-v1"})

	_, err := p.SendEvent(context.Background(), EventEmail{
		Kind:      "subscription.upgraded",
		Recipient: "u@example.com",
		Params:    map[string]string{"from_tier": "hobby", "to_tier": "pro", "mrr": "49"},
	})
	if err != nil {
		t.Fatalf("happy path = %v; want nil", err)
	}

	td := aws.ToString(fake.recordedInput.Content.Template.TemplateData)
	var raw map[string]any
	if err := json.Unmarshal([]byte(td), &raw); err != nil {
		t.Fatalf("TemplateData not valid JSON: %v", err)
	}
	for k, v := range raw {
		if _, ok := v.(string); !ok {
			t.Errorf("TemplateData[%q] = %T; want string (SES templates can't reference non-string values)", k, v)
		}
	}
}

// TestFactory_SESProvider — happy path: name + region + creds + from
// produce a real SESProvider via the factory.
func TestFactory_SESProvider(t *testing.T) {
	p, err := NewProvider(Config{
		Provider: providerNameSES,
		SES: SESConfig{
			AWSRegion:     "us-east-1",
			AWSAccessKey:  "k",
			AWSSecretKey:  "s",
			FromEmail:     "noreply@example.com",
			TemplateNames: map[string]string{"x": "tmpl"},
		},
	})
	if err != nil {
		t.Fatalf("NewProvider(ses) = %v", err)
	}
	if p.Name() != providerNameSES {
		t.Errorf("Name() = %q; want %q", p.Name(), providerNameSES)
	}
}

// TestFactory_SESMissingFromEmail_Errors — factory propagates the
// NewSESProvider validation error so a misconfigured operator boot-fails
// instead of silently noop'ing.
func TestFactory_SESMissingFromEmail_Errors(t *testing.T) {
	_, err := NewProvider(Config{
		Provider: providerNameSES,
		SES: SESConfig{
			AWSRegion:    "us-east-1",
			AWSAccessKey: "k",
			AWSSecretKey: "s",
			// FromEmail intentionally empty
		},
	})
	if err == nil {
		t.Fatal("NewProvider(ses) with empty FromEmail = nil; want startup error")
	}
}
