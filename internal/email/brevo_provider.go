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
// Result classification — these line up exactly with SendClass so the
// forwarder doesn't need to know the wire format:
//
//   2xx                              → nil                                            (success — forwarder advances cursor)
//   4xx (401 / 422 / 400 etc.)       → *SendError{Class: SendClassPermanent}          (forwarder advances + logs ERROR)
//   5xx / network / timeout          → *SendError{Class: SendClassTransient}          (forwarder holds cursor)
//   no template configured for kind  → *SendError{Class: SendClassSkippedNoTemplate}  (forwarder advances silently)
//
// The "4xx-advances" behaviour is deliberate: a single audit row with
// malformed content shouldn't block every event behind it forever. We
// log loudly so the poisoned row is visible in the structured-log stream.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
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
}

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
	return &BrevoProvider{
		apiKey:    cfg.APIKey,
		templates: tmpls,
		httpc:     &http.Client{Timeout: brevoHTTPTimeout},
		url:       brevoSendURL,
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

// SendEvent implements EmailProvider.SendEvent. Maps EventEmail.Kind to a
// Brevo templateId via p.templates, builds the JSON body, POSTs, and
// classifies the response per the table at the top of this file.
func (p *BrevoProvider) SendEvent(ctx context.Context, evt EventEmail) error {
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

	if evt.Recipient == "" {
		// Defensive — the forwarder filters orphan rows before reaching
		// here, but a future caller path might not. Permanent because
		// the row will never sprout an email retroactively.
		return &SendError{
			Class:   SendClassPermanent,
			Message: "brevo: empty recipient",
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
			"recipient", evt.Recipient,
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

	resp, err := p.httpc.Do(req)
	if err != nil {
		// Network error, timeout, dns failure. Transient by definition.
		slog.Warn("email.brevo.http_failed",
			"kind", evt.Kind,
			"recipient", evt.Recipient,
			"error", err,
		)
		return &SendError{Class: SendClassTransient, Cause: err, Message: "brevo: http"}
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, brevoBodyReadCap))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		slog.Info("email.brevo.event_sent",
			"kind", evt.Kind,
			"recipient", evt.Recipient,
			"status", resp.StatusCode,
			"template_id", tmplID,
		)
		return nil

	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// 401/403 = bad API key (every row will fail the same way until
		// someone rotates the secret), 400/422 = bad payload for this row.
		// In both cases advancing the cursor is correct: holding it pins
		// the whole queue on one bad row.
		slog.Error("email.brevo.permanent_4xx",
			"kind", evt.Kind,
			"recipient", evt.Recipient,
			"status", resp.StatusCode,
			"body", string(respBody),
		)
		return &SendError{
			Class:   SendClassPermanent,
			Message: fmt.Sprintf("brevo: %d %s", resp.StatusCode, string(respBody)),
		}

	default:
		// 5xx — Brevo upstream issue. Hold cursor; retry next tick.
		slog.Warn("email.brevo.transient_5xx",
			"kind", evt.Kind,
			"recipient", evt.Recipient,
			"status", resp.StatusCode,
			"body", string(respBody),
		)
		return &SendError{
			Class:   SendClassTransient,
			Message: fmt.Sprintf("brevo: %d %s", resp.StatusCode, string(respBody)),
		}
	}
}
