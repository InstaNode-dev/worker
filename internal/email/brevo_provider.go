package email

// brevo_provider.go — Brevo (formerly Sendinblue) transactional email
// provider. Implements EmailProvider against the v3 SMTP API:
//
//   POST https://api.brevo.com/v3/smtp/email
//   Headers: api-key: <BREVO_API_KEY>
//            content-type: application/json
//            X-Mailin-Custom: <idempotency key>   (Brevo's dedupe header)
//
// Body shape (the only fields we use):
//
//   {
//     "to":         [{"email": "...", "name": "..."}],
//     "templateId": 12,
//     "params":     { "<key>": "<value>", ... }
//   }
//
// Result classification — the canonical table. Every Brevo POST takes one
// of these branches; tests pin them per-code in brevo_provider_test.go.
//
//   2xx                              → nil                                            (success — forwarder advances cursor)
//   401 / 403 (auth)                 → *SendError{Class: SendClassTransient}          (forwarder HOLDS cursor — token rotation recoverable)
//   408 / 425 / 429                  → *SendError{Class: SendClassTransient}          (forwarder HOLDS cursor — back off + retry)
//   5xx / network / timeout          → *SendError{Class: SendClassTransient}          (forwarder holds cursor)
//   400 / 404 / 422 / other 4xx      → *SendError{Class: SendClassPermanent}          (forwarder advances + logs ERROR)
//   no template configured for kind  → *SendError{Class: SendClassSkippedNoTemplate}  (forwarder advances silently)
//
// The "payload-4xx-advances" behaviour is deliberate: a single audit row
// with malformed content (400/422) shouldn't block every event behind it
// forever. We log loudly so the poisoned row is visible in the log stream.
//
// P0-1 (2026-05-19, re-confirmed BugBash 2026-05-20): 401/403/429 are NOT
// advanced. A bad/expired/revoked API key (401/403) or a rate-limit (429) is
// an ACCOUNT-level condition, recoverable without operator data loss —
// classifying it Permanent made the forwarder silently drop every audit row
// in every batch. These now map to Transient (cursor held) and log the
// alert-able auth_wall / rate_limited entries. BugBash 2026-05-20 added
// 408 (Request Timeout) and 425 (Too Early) to the explicit transient set
// — both are recoverable upstream conditions, not per-row payload defects.
//
// Observability (BugBash 2026-05-20):
//
//   - Every send result (success + every error path) emits a single
//     structured slog line carrying classification=<...>, status_code=<...>,
//     provider="brevo", kind=<audit_log.kind>, idempotency_key=<X-Mailin-Custom>.
//     The IdempotencyKey is "audit-<row-id>" so the log line acts as the
//     audit-log-id cross-reference the brief requires.
//
//   - Every non-2xx outcome increments metrics.BrevoSendErrorsTotal
//     {classification,status_code}. Network/transport errors record
//     status_code="0" so the operator can distinguish "Brevo said no" from
//     "we never reached Brevo" at the metric layer without grepping logs.
//     Exposed on the worker /metrics endpoint registered in main.go.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"instant.dev/worker/internal/metrics"
)

// ── Named constants — no inline strings for headers / endpoints / content-types.

const (
	// brevoSendURL is the Brevo v3 transactional send endpoint. Constant
	// so a typo in either production or a fake-server test surfaces at
	// compile time. https://developers.brevo.com/reference/sendtransacemail
	brevoSendURL = "https://api.brevo.com/v3/smtp/email"

	// brevoHTTPTimeout caps a single POST so a slow upstream can't stall
	// a 100-row batch indefinitely. 10s is generous — Brevo normally
	// responds in <500ms.
	brevoHTTPTimeout = 10 * time.Second

	// HTTP header names. Named constants so a typo trips compile-time
	// (and so the test asserting "api-key" matches the production string).
	headerAPIKey      = "api-key"
	headerContentType = "Content-Type"
	headerIdempotency = "X-Mailin-Custom" // Brevo's per-request dedupe header
	contentTypeJSON   = "application/json"

	// brevoBodyReadCap bounds the response body we read on error paths.
	// 4KiB is plenty for a Brevo error envelope and prevents a malicious
	// or misbehaving upstream from streaming gigabytes into a log line.
	brevoBodyReadCap int64 = 4096
)

// BrevoConfig is the Brevo-specific configuration plucked from env at
// startup. APIKey comes from BREVO_API_KEY; TemplateIDs comes from
// BREVO_TEMPLATE_IDS (JSON object mapping audit_log.kind → numeric
// Brevo template ID).
//
// The mapping is config-not-code so a marketing operator can wire up a
// new event in Brevo and add one line to the JSON without a worker
// release. Kinds not in the map produce SendClassSkippedNoTemplate.
type BrevoConfig struct {
	APIKey      string
	TemplateIDs map[string]int

	// SenderEmail / SenderName are the "From:" identity used by the
	// raw-render path (EventEmail.HTMLBody non-empty). The dashboard-
	// template path ignores these because Brevo's template editor has
	// its own sender field. Both come from env at boot:
	//   BREVO_SENDER_EMAIL — defaults to noreply@instanode.dev
	//   BREVO_SENDER_NAME  — defaults to "instanode"
	//
	// They live in code-controlled config (not the Brevo dashboard) so a
	// rendered email cannot silently inherit a personal email left in
	// the dashboard sender field. The defaults are intentionally
	// production-safe — a worker that boots with the secret absent
	// still sends from noreply@instanode.dev, not someone's gmail.
	SenderEmail string
	SenderName  string
}

// Default sender identity used when BREVO_SENDER_EMAIL / BREVO_SENDER_NAME
// are absent from BrevoConfig. Kept here (not in config.Load) so a test
// or in-process caller that constructs BrevoConfig directly gets the
// same safe defaults the production code path uses.
const (
	defaultBrevoSenderEmail = "noreply@instanode.dev"
	defaultBrevoSenderName  = "instanode"
)

// BrevoProvider is the live implementation. Constructed once at boot via
// NewBrevoProvider and reused across every forwarder tick. http.Client
// is goroutine-safe; templates is read-only after construction.
type BrevoProvider struct {
	apiKey    string
	templates map[string]int
	httpc     *http.Client
	// url is overridable so tests can point at an httptest.Server. Always
	// brevoSendURL in production.
	url string

	// senderEmail / senderName are the From identity for the raw-render
	// path (EventEmail.HTMLBody non-empty). The template path leaves
	// these unused — Brevo's template carries its own sender. Both are
	// populated by NewBrevoProvider from BrevoConfig (with defaults).
	senderEmail string
	senderName  string
}

// NewBrevoProvider validates BrevoConfig and returns the live provider.
// A missing API key is a hard error here — the operator explicitly set
// EMAIL_PROVIDER=brevo, so silently no-op'ing would be more confusing
// than a fast boot failure. (For the "I haven't configured email yet"
// case, leave EMAIL_PROVIDER unset and NoopProvider takes over.)
func NewBrevoProvider(cfg BrevoConfig) (*BrevoProvider, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("brevo: BREVO_API_KEY required when EMAIL_PROVIDER=brevo (leave EMAIL_PROVIDER unset to disable email)")
	}
	// Empty template map isn't fatal — every send just returns
	// SkippedNoTemplate. Operators bring up Brevo with the API key first
	// and add templates one at a time; this allows that flow.
	tmpls := cfg.TemplateIDs
	if tmpls == nil {
		tmpls = map[string]int{}
	}
	senderEmail := cfg.SenderEmail
	if senderEmail == "" {
		senderEmail = defaultBrevoSenderEmail
	}
	senderName := cfg.SenderName
	if senderName == "" {
		senderName = defaultBrevoSenderName
	}
	return &BrevoProvider{
		apiKey:      cfg.APIKey,
		templates:   tmpls,
		httpc:       &http.Client{Timeout: brevoHTTPTimeout},
		url:         brevoSendURL,
		senderEmail: senderEmail,
		senderName:  senderName,
	}, nil
}

// Name returns the stable provider identifier used in slog / metric labels.
func (p *BrevoProvider) Name() string { return providerNameBrevo }

// brevoRecipient mirrors the Brevo `to[]` element. Name is optional;
// Brevo accepts an empty/missing string and falls back to the email
// local-part for personalisation.
type brevoRecipient struct {
	Email string `json:"email"`
	Name  string `json:"name,omitempty"`
}

// brevoSendRequest is the wire payload for POST /v3/smtp/email. Only the
// fields we use — Brevo accepts many more, but the forwarder only needs
// template id + recipient + params.
type brevoSendRequest struct {
	To         []brevoRecipient  `json:"to"`
	TemplateID int               `json:"templateId"`
	Params     map[string]string `json:"params,omitempty"`
}

// brevoSender mirrors the Brevo `sender` object used by the raw-HTML
// path. Brevo accepts {"email": "...", "name": "..."}; the template path
// doesn't send this object because the template carries its own sender.
type brevoSender struct {
	Email string `json:"email"`
	Name  string `json:"name,omitempty"`
}

// brevoRawSendRequest is the wire payload for the raw-HTML path. Used
// when EventEmail.HTMLBody is non-empty — we send subject + htmlContent +
// textContent + sender directly and DO NOT include templateId. This
// bypasses the Brevo dashboard template entirely so the email body is
// fully controlled by Go code at deploy time.
type brevoRawSendRequest struct {
	To          []brevoRecipient `json:"to"`
	Sender      brevoSender      `json:"sender"`
	Subject     string           `json:"subject"`
	HTMLContent string           `json:"htmlContent"`
	TextContent string           `json:"textContent,omitempty"`
}

// SendEvent implements EmailProvider.SendEvent. Two paths:
//
//  1. Raw-render path (preferred for new kinds) — when EventEmail.HTMLBody
//     is non-empty, we send Subject + HTMLBody + TextBody + Sender directly.
//     The template id is NOT consulted; p.templates can lack an entry for
//     this Kind without producing SkippedNoTemplate. This is the path used
//     for "anon.expiry_warning" so the email body is controlled entirely
//     by worker code (no out-of-band Brevo dashboard edit required).
//
//  2. Template path (legacy / dashboard-controlled) — when HTMLBody is
//     empty, look up EventEmail.Kind in p.templates and POST with
//     templateId + params. Kinds with no entry produce SkippedNoTemplate.
//
// Both paths classify the HTTP response identically per the table at the
// top of this file.
func (p *BrevoProvider) SendEvent(ctx context.Context, evt EventEmail) error {
	if evt.Recipient == "" {
		// Defensive — the forwarder filters orphan rows before reaching
		// here, but a future caller path might not. Permanent because
		// the row will never sprout an email retroactively. Checked
		// before the template lookup so a raw-render event with empty
		// recipient short-circuits the same way.
		return &SendError{
			Class:   SendClassPermanent,
			Message: "brevo: empty recipient",
		}
	}

	// Raw-render path takes precedence — explicit HTML body means the
	// caller already rendered the email and wants us to send those bytes
	// verbatim. We do NOT consult p.templates in this branch.
	if evt.HTMLBody != "" {
		return p.sendRaw(ctx, evt)
	}

	tmplID, ok := p.templates[evt.Kind]
	if !ok {
		// Operator hasn't mapped this kind to a Brevo template yet —
		// the forwarder advances the cursor silently. Brevo dedupe
		// would do nothing useful here anyway since we never POSTed.
		return &SendError{
			Class:   SendClassSkippedNoTemplate,
			Message: fmt.Sprintf("brevo: no template configured for kind %q", evt.Kind),
		}
	}

	body, err := json.Marshal(brevoSendRequest{
		To:         []brevoRecipient{{Email: evt.Recipient, Name: evt.RecipientName}},
		TemplateID: tmplID,
		Params:     evt.Params,
	})
	if err != nil {
		// Marshalling map[string]string can only fail for ill-formed
		// keys (never happens) — treat as Permanent so we advance past
		// the row instead of looping on a programmer bug.
		slog.Error("email.brevo.marshal_failed",
			"kind", evt.Kind,
			"recipient", maskEmail(evt.Recipient),
			"error", err,
		)
		return &SendError{Class: SendClassPermanent, Cause: err, Message: "brevo: marshal payload"}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		// Request construction failure is almost certainly a malformed URL —
		// a programming bug. Transient so the operator sees it on every tick
		// until they fix it, instead of advancing past silently.
		slog.Error("email.brevo.request_build_failed",
			"kind", evt.Kind,
			"error", err,
		)
		return &SendError{Class: SendClassTransient, Cause: err, Message: "brevo: build request"}
	}
	req.Header.Set(headerAPIKey, p.apiKey)
	req.Header.Set(headerContentType, contentTypeJSON)
	if evt.IdempotencyKey != "" {
		req.Header.Set(headerIdempotency, evt.IdempotencyKey)
	}

	return p.doRequest(ctx, evt, body, brevoSendPathTemplate, tmplID)
}

// brevoSendPath enumerates which SendEvent branch we took, used only for
// log labels so an operator can tell "we sent via template id 6" from
// "we sent the rendered HTML" at a glance.
type brevoSendPath string

const (
	brevoSendPathTemplate brevoSendPath = "template"
	brevoSendPathRaw      brevoSendPath = "raw_html"
)

// sendRaw is the raw-HTML send path. The caller (typically a per-kind
// builder) has already rendered the subject + html + plain-text body in
// Go; we POST them verbatim with the configured sender identity. The
// dashboard-template path is bypassed entirely — Brevo just relays the
// bytes. This is how anon.expiry_warning escapes the broken dashboard
// template that hardcoded "6 hours" and rendered empty fields.
func (p *BrevoProvider) sendRaw(ctx context.Context, evt EventEmail) error {
	if evt.Subject == "" {
		// Subject is mandatory in the raw path — a Brevo POST with an
		// empty subject string still delivers, but the recipient sees
		// "(no subject)" which is its own bug. Permanent so we advance
		// past the row; the caller is supposed to render a subject.
		slog.Error("email.brevo.raw_missing_subject",
			"kind", evt.Kind,
			"recipient", maskEmail(evt.Recipient),
		)
		return &SendError{
			Class:   SendClassPermanent,
			Message: "brevo: raw send missing subject",
		}
	}
	body, err := json.Marshal(brevoRawSendRequest{
		To:          []brevoRecipient{{Email: evt.Recipient, Name: evt.RecipientName}},
		Sender:      brevoSender{Email: p.senderEmail, Name: p.senderName},
		Subject:     evt.Subject,
		HTMLContent: evt.HTMLBody,
		TextContent: evt.TextBody,
	})
	if err != nil {
		slog.Error("email.brevo.raw_marshal_failed",
			"kind", evt.Kind,
			"recipient", maskEmail(evt.Recipient),
			"error", err,
		)
		return &SendError{Class: SendClassPermanent, Cause: err, Message: "brevo: raw marshal"}
	}
	return p.doRequest(ctx, evt, body, brevoSendPathRaw, 0)
}

// doRequest is the shared HTTP send + response classify path used by
// both template and raw branches. Identical wire-level behavior — the
// only difference is the log label and the absence of a template id in
// the raw path.
func (p *BrevoProvider) doRequest(ctx context.Context, evt EventEmail, body []byte, path brevoSendPath, tmplID int) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		// Request construction failure is almost certainly a malformed URL —
		// a programming bug. Transient so the operator sees it on every tick
		// until they fix it, instead of advancing past silently.
		slog.Error("email.brevo.request_build_failed",
			"kind", evt.Kind,
			"path", string(path),
			"error", err,
		)
		return &SendError{Class: SendClassTransient, Cause: err, Message: "brevo: build request"}
	}
	req.Header.Set(headerAPIKey, p.apiKey)
	req.Header.Set(headerContentType, contentTypeJSON)
	if evt.IdempotencyKey != "" {
		req.Header.Set(headerIdempotency, evt.IdempotencyKey)
	}

	resp, err := p.httpc.Do(req)
	if err != nil {
		// Network error, timeout, dns failure, TLS handshake failure, EOF
		// before headers, context.DeadlineExceeded. By construction we have
		// NO response — there is no status code to record, so the metric is
		// labelled status_code="0" so an operator can distinguish "we never
		// reached Brevo" from "Brevo returned something" at the metric layer
		// without grepping logs. Transient because none of these are
		// per-row payload defects.
		const (
			classification = "transient"
			statusLabel    = "0" // no response observed
		)
		metrics.BrevoSendErrorsTotal.WithLabelValues(classification, statusLabel).Inc()
		slog.Warn("email.brevo.http_failed",
			"provider", providerNameBrevo,
			"classification", classification,
			"status_code", 0,
			"kind", evt.Kind,
			"recipient", maskEmail(evt.Recipient),
			"path", string(path),
			"audit_log_id", evt.IdempotencyKey,
			"error", err,
		)
		return &SendError{Class: SendClassTransient, Cause: err, Message: "brevo: http"}
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, brevoBodyReadCap))
	statusLabel := strconv.Itoa(resp.StatusCode)

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		// Success path. We deliberately log `classification=success` (rather
		// than omitting it) so an NR query for `classification:*` over the
		// Brevo provider stream returns one row per send, not one row per
		// failure. Useful for "send/error ratio" dashboards.
		slog.Info("email.brevo.event_sent",
			"provider", providerNameBrevo,
			"classification", "success",
			"status_code", resp.StatusCode,
			"kind", evt.Kind,
			"recipient", maskEmail(evt.Recipient),
			"path", string(path),
			"template_id", tmplID,
			"audit_log_id", evt.IdempotencyKey,
		)
		return nil

	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		// 401/403 is an ACCOUNT-LEVEL auth failure (bad / expired / revoked
		// BREVO_API_KEY) — NOT a per-row payload problem. Transient from the
		// queue's point of view: it resolves the instant an operator rotates
		// the secret. Classifying Permanent (the pre-2026-05-19 behaviour)
		// made the forwarder advance the cursor past every row in every
		// batch, silently and unrecoverably dropping all email.
		//
		// Logged at ERROR with a distinct, alert-able key (auth_wall) so
		// an operator is paged before a batch is burned — the generic
		// email.brevo.permanent_4xx key is for per-row payload rejects.
		const classification = "transient"
		metrics.BrevoSendErrorsTotal.WithLabelValues(classification, statusLabel).Inc()
		slog.Error("email.brevo.auth_wall",
			"provider", providerNameBrevo,
			"classification", classification,
			"status_code", resp.StatusCode,
			"kind", evt.Kind,
			"recipient", maskEmail(evt.Recipient),
			"path", string(path),
			"audit_log_id", evt.IdempotencyKey,
			"body", string(respBody),
			"note", "account-level Brevo auth failure (bad/expired/revoked BREVO_API_KEY) — classified Transient, cursor held, email NOT lost; rotate the secret",
		)
		return &SendError{
			Class:   SendClassTransient,
			Message: fmt.Sprintf("brevo: auth failure %d %s", resp.StatusCode, string(respBody)),
		}

	case resp.StatusCode == http.StatusRequestTimeout ||
		resp.StatusCode == http.StatusTooEarly ||
		resp.StatusCode == http.StatusTooManyRequests:
		// 408 Request Timeout — Brevo edge dropped our request before
		// finishing reads; retry will work.
		// 425 Too Early — Brevo rejected a replay-suspect early-data
		// request; a fresh handshake on retry succeeds.
		// 429 Too Many Requests — rate limited; the row is fine, Brevo
		// just wants us to back off. All three are recoverable upstream
		// conditions, not per-row payload defects.
		const classification = "transient"
		metrics.BrevoSendErrorsTotal.WithLabelValues(classification, statusLabel).Inc()
		slog.Warn("email.brevo.rate_limited",
			"provider", providerNameBrevo,
			"classification", classification,
			"status_code", resp.StatusCode,
			"kind", evt.Kind,
			"recipient", maskEmail(evt.Recipient),
			"path", string(path),
			"audit_log_id", evt.IdempotencyKey,
			"body", string(respBody),
		)
		return &SendError{
			Class:   SendClassTransient,
			Message: fmt.Sprintf("brevo: throttled %d %s", resp.StatusCode, string(respBody)),
		}

	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// 400/404/422 and other 4xx = genuine per-row payload rejection.
		// The row will never produce a valid send, so advancing the cursor
		// is correct: holding it pins the whole queue on one bad row.
		// (408/425/429 are siphoned off in the case above; auth 401/403
		// in the case before that.)
		const classification = "permanent"
		metrics.BrevoSendErrorsTotal.WithLabelValues(classification, statusLabel).Inc()
		slog.Error("email.brevo.permanent_4xx",
			"provider", providerNameBrevo,
			"classification", classification,
			"status_code", resp.StatusCode,
			"kind", evt.Kind,
			"recipient", maskEmail(evt.Recipient),
			"path", string(path),
			"audit_log_id", evt.IdempotencyKey,
			"body", string(respBody),
		)
		return &SendError{
			Class:   SendClassPermanent,
			Message: fmt.Sprintf("brevo: %d %s", resp.StatusCode, string(respBody)),
		}

	default:
		// 5xx — Brevo upstream issue. Hold cursor; retry next tick.
		// Also catches anything ≥600 (RFC violation by upstream) and the
		// theoretical 1xx/3xx leakage from net/http (which would not be a
		// per-row defect either). All routed Transient as a fail-safe.
		const classification = "transient"
		metrics.BrevoSendErrorsTotal.WithLabelValues(classification, statusLabel).Inc()
		slog.Warn("email.brevo.transient_5xx",
			"provider", providerNameBrevo,
			"classification", classification,
			"status_code", resp.StatusCode,
			"kind", evt.Kind,
			"recipient", maskEmail(evt.Recipient),
			"path", string(path),
			"audit_log_id", evt.IdempotencyKey,
			"body", string(respBody),
		)
		return &SendError{
			Class:   SendClassTransient,
			Message: fmt.Sprintf("brevo: %d %s", resp.StatusCode, string(respBody)),
		}
	}
}
