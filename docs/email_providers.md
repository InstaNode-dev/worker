# Event-email providers

The worker forwards every supported `audit_log` row to an outbound email
provider so customer-facing lifecycle email (claimed, upgraded, near
quota wall, etc.) is event-driven. The provider is **pluggable**: today
that's Brevo; future swaps to SES, SendGrid, or anything else are a
one-file change.

## Architecture

```
audit_log (Postgres)
        │
        ▼
EventEmailForwarderWorker (jobs/event_email_forwarder.go)
        │  builds canonical email.EventEmail{Kind, Recipient, Params, IdempotencyKey}
        ▼
email.EmailProvider (interface)
        │
        ├─ email.BrevoProvider      (live today — internal/email/brevo_provider.go)
        ├─ email.NoopProvider       (fallback when EMAIL_PROVIDER unset)
        ├─ email.SESProvider        (stubbed — internal/email/ses_provider.go)
        └─ email.SendGridProvider   (not implemented)
```

The forwarder lives in `internal/jobs/event_email_forwarder.go` and
**MUST NOT** contain any provider identifier. The grep evidence is in
the PR description:

```
grep -iE "\b(brevo|loops|ses|sendgrid)\b" internal/jobs/event_email_forwarder*.go
→ (empty)
```

## The interface

`internal/email/provider.go`:

```go
type EmailProvider interface {
    SendEvent(ctx context.Context, evt EventEmail) error
    Name() string
}

type EventEmail struct {
    Kind            string             // audit_log.kind verbatim
    Recipient       string             // email
    RecipientName   string             // optional display name
    Params          map[string]string  // per-event substitution map
    IdempotencyKey  string             // "audit-<row-id>"
}
```

Failures return `*SendError` typed by class:

| `SendClass`             | What it means                                | Forwarder action       |
|-------------------------|----------------------------------------------|------------------------|
| `Permanent`             | Auth bad / payload rejected / 4xx            | Advance cursor + log ERROR |
| `Transient`             | Network / timeout / 5xx                      | Hold cursor, retry next tick, halt batch |
| `SkippedNoTemplate`     | No template configured for this `Kind`       | Advance cursor + log INFO |

A `nil` error means success.

## Brevo (current default)

### Configuration

| Env var               | Meaning                                                                                 |
|-----------------------|-----------------------------------------------------------------------------------------|
| `EMAIL_PROVIDER`      | `brevo` to enable; unset or `noop` for the silent fallback.                              |
| `BREVO_API_KEY`       | Brevo v3 API key (Settings → SMTP & API → API Keys).                                     |
| `BREVO_TEMPLATE_IDS`  | JSON object mapping `audit_log.kind` to a numeric Brevo template id.                     |

### Example

```bash
EMAIL_PROVIDER=brevo
BREVO_API_KEY=xkeysib-...
BREVO_TEMPLATE_IDS='{
  "onboarding.claimed":        12,
  "subscription.upgraded":     13,
  "near_quota_wall":           14,
  "resource.expiry_imminent":  15,
  "subscription.downgraded":   16,
  "subscription.canceled":     17,
  "experiment.conversion":     18,
  "admin.tier_changed":        19,
  "admin.promo_issued":        20
}'
```

A kind not present in the map produces `SendClassSkippedNoTemplate`,
which the forwarder logs at INFO and advances past — operators can
bring up Brevo with the API key first and add templates one at a time.

### Wire details

* Endpoint: `POST https://api.brevo.com/v3/smtp/email`
* Auth header: `api-key: <BREVO_API_KEY>` (NOT `Authorization: Bearer`)
* Dedupe header: `X-Mailin-Custom: audit-<row-id>` — Brevo's per-request
  idempotency hint.
* Status code mapping: `2xx` → success, `4xx` → `Permanent`, `5xx` →
  `Transient`.

## NoopProvider (default when nothing is set)

`EMAIL_PROVIDER` unset or `noop` returns the `NoopProvider`, which logs
each send at DEBUG and returns `SendClassSkippedNoTemplate` so the
forwarder advances cursors without sending anything. This is the
**fail-open** contract: a missing env var must not block the audit-log
pipeline.

The factory prints a single WARN line at boot when noop is selected so
the operator knows email is dropped.

## Adding a new provider in <100 LOC — the SES walkthrough

The whole point of this seam is that swapping providers is small. Here's
the recipe for SES:

### 1. Create `internal/email/ses_provider.go`

A working skeleton is already in the repo (commented `not wired`):

```go
package email

import (
    "context"
    "fmt"
)

type SESConfig struct {
    Region          string            // AWS_REGION
    AccessKeyID     string            // AWS_ACCESS_KEY_ID
    SecretAccessKey string            // AWS_SECRET_ACCESS_KEY
    TemplateNames   map[string]string // SES_TEMPLATE_NAMES: kind → SES template name
    SourceAddress   string            // SES_SOURCE_ADDRESS: verified sender
}

type SESProvider struct {
    cfg SESConfig
    // sesClient *sesv2.Client — when wiring for real
}

func NewSESProvider(cfg SESConfig) (*SESProvider, error) {
    if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
        return nil, fmt.Errorf("ses: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY required")
    }
    return &SESProvider{cfg: cfg}, nil
}

func (p *SESProvider) Name() string { return providerNameSES }

func (p *SESProvider) SendEvent(ctx context.Context, evt EventEmail) error {
    tmpl, ok := p.cfg.TemplateNames[evt.Kind]
    if !ok {
        return &SendError{Class: SendClassSkippedNoTemplate,
            Message: fmt.Sprintf("ses: no template for kind %q", evt.Kind)}
    }
    // sesv2.SendEmail with Destination.ToAddresses=[evt.Recipient],
    //                     Content.Template={TemplateName: tmpl, TemplateData: jsonOf(evt.Params)}
    // Then classify the error:
    //   - smithy.APIError code 400/InvalidParameterValue → Permanent
    //   - smithy.APIError code 5xx                        → Transient
    //   - net.Error                                       → Transient
    _ = tmpl
    return nil
}
```

### 2. Add a case in `internal/email/factory.go`

```go
case providerNameSES:
    return NewSESProvider(cfg.SES)
```

(plus an `SES SESConfig` field on `Config`)

### 3. Add env-var parsing in `internal/config/config.go`

```go
SESAccessKey:     os.Getenv("AWS_ACCESS_KEY_ID"),
SESSecretKey:     os.Getenv("AWS_SECRET_ACCESS_KEY"),
SESRegion:        getenv("AWS_REGION", "us-east-1"),
SESTemplateNames: parseStringMap(os.Getenv("SES_TEMPLATE_NAMES")),
SESSourceAddress: os.Getenv("SES_SOURCE_ADDRESS"),
```

### 4. Wire in `internal/jobs/workers.go`

The wiring is **already provider-agnostic** — the existing call is:

```go
emailProvider, err := email.NewProvider(email.Config{
    Provider: cfg.EmailProvider,
    Brevo:    email.BrevoConfig{...},
    SES:      email.SESConfig{...},  // <- only this line is new
})
```

### 5. Tests

Copy `internal/email/brevo_provider_test.go` to
`internal/email/ses_provider_test.go` and replace the httptest assertions
with whatever SES client mock you prefer (aws-sdk-go-v2 has good support).

That's it. **No changes** to `event_email_forwarder.go`,
`event_email_mapping.go`, or any of their tests. That's the test of
agnosticism.

## What lives where

```
internal/email/
  provider.go            EmailProvider interface + EventEmail + SendError + SendClass
  factory.go             NewProvider(cfg) — single switch
  noop_provider.go       Silent-skip implementation (fallback)
  brevo_provider.go      Brevo v3 transactional implementation
  ses_provider.go        SES skeleton (not wired)
  provider_test.go       Factory + interface contract tests
  brevo_provider_test.go Hermetic httptest-backed Brevo tests

internal/jobs/
  event_email_forwarder.go        Provider-agnostic worker
  event_email_forwarder_test.go   Tests using a fake EmailProvider
  event_email_mapping.go          audit_log.kind → EventEmail.Params builders
  event_email_mapping_test.go     Schema invariants between filter + builders
```

## Operator runbook

* **Worker boots with `EMAIL_PROVIDER=brevo` but no `BREVO_API_KEY`** → fast crash with explicit error. Set the secret and redeploy.
* **Want to disable email temporarily** → set `EMAIL_PROVIDER=noop`. Per-tick logs at INFO show rows being skipped silently.
* **Adding a new event kind** → edit `internal/jobs/event_email_mapping.go` (add `auditKindFoo` const + builder + entries in both slice and map). Add the kind to `BREVO_TEMPLATE_IDS`. No provider code changes.
* **Migrating Brevo → SES** → flip `EMAIL_PROVIDER=ses`. Templates in Brevo do not migrate automatically; you maintain `SES_TEMPLATE_NAMES` independently. The forwarder is unchanged.
