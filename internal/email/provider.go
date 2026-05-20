package email

// provider.go — the provider-agnostic seam for outbound event email.
//
// The forwarder (internal/jobs/event_email_forwarder.go) holds an
// EmailProvider and never knows whether the bytes go to Brevo, SES,
// SendGrid, or /dev/null. Swapping providers later = one new file
// implementing this interface + one new branch in factory.go.
//
// The interface is deliberately tiny:
//
//   - SendEvent(ctx, EventEmail) error — the one operation the forwarder needs
//   - Name() string                    — for log/metric labels
//
// Everything provider-specific (template ids, html bodies, auth headers,
// rate limits) is hidden inside each implementation. The forwarder passes
// the canonical EventEmail and inspects only SendError.Class on failure.

import (
	"context"
	"fmt"
)

// EmailProvider sends an event-driven email for one audit_log row.
//
// Implementations are responsible for mapping the event Kind to whatever
// the provider needs (template id, html body, subject line, etc.) via
// their own config — the forwarder doesn't know.
//
// Idempotency is best-effort: providers that support a dedupe header
// (e.g. Brevo's X-Mailin-Custom) SHOULD set it from EventEmail.IdempotencyKey
// so a forwarder retry after a transient failure doesn't fire a second
// campaign. Providers that don't support dedupe headers must document the
// duplicate-send risk in their package comment.
type EmailProvider interface {
	// SendEvent attempts to send one event email. Returns (messageID, nil)
	// on success. The messageID is the provider's per-send opaque
	// identifier — Brevo's `messageId` JSON field, SES's MessageID, etc.
	// — which the forwarder persists into forwarder_sent.provider_id so
	// the receiver-side webhook (POST /webhooks/brevo/:secret in the api
	// repo) can match inbound delivery events back to the ledger row.
	// When a provider doesn't surface a per-send id (NoopProvider, or
	// providers that return only an HTTP envelope) it MAY return the
	// empty string — the forwarder falls back to EventEmail.IdempotencyKey
	// in that case so support queries still have *some* anchor.
	//
	// On failure returns ("", *SendError) typed by Class:
	//   - SendClassPermanent          — skip + advance cursor (auth bad, payload rejected, etc.)
	//   - SendClassTransient          — don't advance cursor (5xx, network, retry next tick)
	//   - SendClassSkippedNoTemplate  — skip + advance cursor (this event isn't configured for this provider)
	SendEvent(ctx context.Context, evt EventEmail) (string, error)

	// Name returns the provider identifier for logs/metrics ("brevo", "ses", "noop").
	// Stable for the lifetime of the binary; safe to use as a label cardinality.
	Name() string
}

// EventEmail is the canonical, provider-agnostic representation of one
// outbound event email. It's built by the forwarder from an audit_log row +
// owner email and handed to the configured EmailProvider.
//
// Field semantics:
//
//   - Kind is the audit_log.kind verbatim (e.g. "subscription.upgraded"). The
//     provider uses it as a lookup key into its own template config.
//   - Recipient is the team's primary email. Required — every provider rejects
//     empty.
//   - RecipientName is optional display name (provider may render "Hi Alex,").
//   - Params is the per-event substitution map. Keys are flat strings the
//     provider templates reference (e.g. "from_tier", "mrr"). Values are
//     stringified so providers don't need to negotiate JSON types.
//   - IdempotencyKey is "audit-<row-id>" — providers that support dedupe
//     headers MUST set this to that header.
//
//   - Subject / HTMLBody / TextBody are the "raw render" path. When the
//     caller (the per-kind builder, in practice) renders the email body
//     itself in Go and stuffs the HTML + plain-text + subject into these
//     fields, the provider MUST send those bytes verbatim instead of
//     looking up a dashboard-configured template by Kind. All three are
//     optional and default-empty; the provider takes the raw path only
//     when HTMLBody is non-empty.
//
//     Rationale: the dashboard-template path was a footgun for kinds
//     where the template body referenced params that the worker wasn't
//     yet sending — the email rendered with empty fields and a hardcoded
//     subject. By rendering in code we keep one source of truth and one
//     deploy cycle (no out-of-band Brevo-dashboard edits required).
//     Existing kinds that work fine via template id keep working
//     unchanged: leave Subject / HTMLBody / TextBody empty and the
//     provider falls back to the legacy template-id path.
type EventEmail struct {
	Kind           string
	Recipient      string
	RecipientName  string
	Params         map[string]string
	IdempotencyKey string

	// Raw-render path — see comment above. Optional. If HTMLBody is
	// non-empty the provider sends Subject + HTMLBody + TextBody
	// directly and does NOT look up a template id by Kind.
	Subject  string
	HTMLBody string
	TextBody string
}

// SendClass categorises a SendError so the forwarder can decide whether
// to advance its cursor (Permanent / SkippedNoTemplate) or retry next
// tick (Transient).
//
// The contract is deliberately three-way, not boolean: the forwarder
// distinguishes "no template configured — skip silently" from "the
// provider rejected the payload — log loudly and advance" so a healthy
// stream of SkippedNoTemplate doesn't pollute error dashboards.
type SendClass int

const (
	// SendClassPermanent — the provider rejected this event and retrying
	// won't help (bad auth, malformed payload). Forwarder logs at ERROR
	// and advances the cursor so one bad row can't pin the queue.
	SendClassPermanent SendClass = iota

	// SendClassTransient — network error, timeout, or 5xx. Forwarder
	// holds the cursor and bails out of the rest of the batch — the
	// remaining rows would hit the same wall.
	SendClassTransient

	// SendClassSkippedNoTemplate — the provider has no template configured
	// for this Kind. Forwarder advances the cursor silently (at INFO),
	// because the operator opted into "no email for this event" by
	// leaving it out of their template map. NoopProvider returns this
	// for every send.
	SendClassSkippedNoTemplate
)

// String returns the lower-case identifier — used for slog fields so
// queries like `class:transient` work without case juggling.
func (c SendClass) String() string {
	switch c {
	case SendClassPermanent:
		return "permanent"
	case SendClassTransient:
		return "transient"
	case SendClassSkippedNoTemplate:
		return "skipped_no_template"
	default:
		return "unknown"
	}
}

// SendError is the typed error every EmailProvider returns on failure.
// The Class field drives forwarder behaviour (see SendClass docs). Cause
// is the underlying error (HTTP error, marshal error, etc.) and Message
// is a human-readable context string.
type SendError struct {
	Class   SendClass
	Cause   error
	Message string
}

// Error implements the error interface. Format is "<class>: <message>: <cause>"
// when all parts are present; degenerate forms (no cause, no message) drop the
// missing segments.
func (e *SendError) Error() string {
	switch {
	case e.Message != "" && e.Cause != nil:
		return fmt.Sprintf("%s: %s: %v", e.Class, e.Message, e.Cause)
	case e.Message != "":
		return fmt.Sprintf("%s: %s", e.Class, e.Message)
	case e.Cause != nil:
		return fmt.Sprintf("%s: %v", e.Class, e.Cause)
	default:
		return e.Class.String()
	}
}

// Unwrap exposes the cause for errors.Is / errors.As.
func (e *SendError) Unwrap() error { return e.Cause }

// ClassOf is a helper for the forwarder: returns the SendClass of err if
// it's a *SendError, otherwise SendClassTransient (fail-safe: an unknown
// error type should hold the cursor, not advance past it).
func ClassOf(err error) SendClass {
	if err == nil {
		return SendClassPermanent // unreachable from a healthy caller, kept for total function
	}
	if se, ok := err.(*SendError); ok {
		return se.Class
	}
	return SendClassTransient
}
