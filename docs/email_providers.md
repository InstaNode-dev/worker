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
        ├─ email.BrevoProvider      (live — internal/email/brevo_provider.go)
        ├─ email.SESProvider        (live — internal/email/ses_provider.go)
        ├─ email.NoopProvider       (fallback when EMAIL_PROVIDER unset)
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

## SES (AWS Simple Email Service)

The SES provider is **live** — flip `EMAIL_PROVIDER=ses` and populate the
`SES_*` env vars below to swap from Brevo without touching forwarder code.

### Configuration

| Env var                       | Meaning                                                                                  |
|-------------------------------|------------------------------------------------------------------------------------------|
| `EMAIL_PROVIDER`              | `ses` to enable.                                                                          |
| `SES_AWS_REGION`              | AWS region the SES identity lives in (e.g. `us-east-1`). No default — SES is regional.    |
| `SES_AWS_ACCESS_KEY_ID`       | IAM access key with `ses:SendEmail` permission.                                           |
| `SES_AWS_SECRET_ACCESS_KEY`   | IAM secret key.                                                                           |
| `SES_FROM_EMAIL`              | Verified SES identity (single email or domain identity).                                  |
| `SES_TEMPLATE_NAMES`          | JSON object mapping `audit_log.kind` to an SES **template name** (string, not numeric).   |

Names are scoped `SES_AWS_*` (not bare `AWS_*`) so they can't be confused
with general-purpose AWS creds used elsewhere in the cluster (e.g. the
storage-bytes scanner reads `OBJECT_STORE_*` and may point at a non-AWS
S3-compatible backend).

### Example

```bash
EMAIL_PROVIDER=ses
SES_AWS_REGION=us-east-1
SES_AWS_ACCESS_KEY_ID=AKIA...
SES_AWS_SECRET_ACCESS_KEY=...
SES_FROM_EMAIL=hello@instanode.dev
SES_TEMPLATE_NAMES='{
  "onboarding.claimed":        "instanode-onboarding-claimed-v1",
  "subscription.upgraded":     "instanode-tier-upgraded-v1",
  "near_quota_wall":           "instanode-near-quota-wall-v1",
  "resource.expiry_imminent":  "instanode-resource-expiring-v1",
  "subscription.downgraded":   "instanode-tier-downgraded-v1",
  "subscription.canceled":     "instanode-subscription-canceled-v1",
  "experiment.conversion":     "instanode-experiment-conversion-v1",
  "admin.tier_changed":        "instanode-admin-tier-changed-v1",
  "admin.promo_issued":        "instanode-admin-promo-issued-v1"
}'
```

A kind not present in the map produces `SendClassSkippedNoTemplate`, same
as Brevo — operators can bring up SES with credentials first and add
templates one at a time.

### Wire details

* API: `sesv2.SendEmail` from `github.com/aws/aws-sdk-go-v2/service/sesv2`.
* Sender: `FromEmailAddress = SES_FROM_EMAIL` — must be a verified SES identity.
* Destination: `Destination.ToAddresses = [evt.Recipient]`.
* Template body: `Content.Template.TemplateName + TemplateData` (JSON of
  `evt.Params` — flat `string→string`).
* No dedupe header — SES doesn't support per-request idempotency, so a
  forwarder retry inside the same audit row's 60s window can send a
  duplicate. The forwarder's cursor advance on Permanent / Skipped + hold
  on Transient minimises but doesn't eliminate this. Mitigated in practice
  by SES's own internal deduplication on identical message-id within a
  short window.

### Error classification

| AWS SES error                                                           | `SendClass`         | Forwarder action                            |
|-------------------------------------------------------------------------|---------------------|---------------------------------------------|
| `nil` (2xx equivalent)                                                  | success             | Advance cursor                              |
| `MessageRejected` / `BadRequestException` / `InvalidParameterException` | `Permanent`         | Advance cursor + log ERROR                  |
| `NotFoundException` (template name typo)                                | `Permanent`         | Advance cursor + log ERROR                  |
| `MailFromDomainNotVerifiedException` / `AccountSuspendedException`      | `Permanent`         | Advance cursor + log ERROR                  |
| `UnrecognizedClientException` / `AccessDeniedException` (bad creds)     | `Permanent`         | Advance cursor + log ERROR                  |
| `ThrottlingException` / `TooManyRequestsException` / `SendingPausedException` | `Transient`   | Hold cursor, retry next tick                |
| `InternalServiceErrorException` / `ServiceUnavailableException` (5xx)   | `Transient`         | Hold cursor, retry next tick                |
| `net.Error` (dns / connection / timeout)                                | `Transient`         | Hold cursor, retry next tick                |
| `context.DeadlineExceeded` / `context.Canceled`                         | `Transient`         | Hold cursor (caller's ctx died)             |
| Unknown error type / unrecognised SES code with `FaultClient`           | `Permanent`         | Advance cursor (don't pin queue on unknown) |
| Unknown error type / unrecognised SES code with `FaultServer`           | `Transient`         | Hold cursor, retry                          |

### SES sandbox caveat

Fresh AWS accounts land in the **SES sandbox** where every recipient
address must be individually verified before SES will deliver to it.
An operator flipping `EMAIL_PROVIDER=ses` on a sandbox account will see
`MessageRejected` on every unverified recipient (logged as Permanent —
cursor advances, row burns).

To exit the sandbox: AWS console → SES → Account dashboard → "Request
production access". Approval is usually same-day. Until then, set
`EMAIL_PROVIDER=brevo` (or keep it on Brevo) for real customer email,
and use `EMAIL_PROVIDER=ses` only against verified internal addresses.

### Binary-size cost

The aws-sdk-go-v2 SES dependency adds ~8MB to the worker binary
(~92MB → ~99MB). This is a one-time cost for the deployment image; the
runtime memory overhead is negligible (the SDK lazy-loads region endpoints).

## Adding a new provider in <100 LOC

The seam is provider-agnostic. To add (say) SendGrid:

1. Create `internal/email/sendgrid_provider.go` implementing `EmailProvider`
   (look at `ses_provider.go` for a template — same shape, different SDK).
2. Add `case providerNameSendGrid: return NewSendGridProvider(cfg.SendGrid)`
   to `internal/email/factory.go` plus a `SendGrid SendGridConfig` field on
   `Config`.
3. Add `SendGrid*` env-var parsing in `internal/config/config.go`.
4. Pass `cfg.SendGrid*` into `email.SendGridConfig{...}` in
   `internal/jobs/workers.go`.
5. Tests: mirror `ses_provider_test.go` with a fake client interface.

**No changes** to `event_email_forwarder.go`, `event_email_mapping.go`,
or any of their tests. That's the test of agnosticism.

## What lives where

```
internal/email/
  provider.go            EmailProvider interface + EventEmail + SendError + SendClass
  factory.go             NewProvider(cfg) — single switch
  noop_provider.go       Silent-skip implementation (fallback)
  brevo_provider.go      Brevo v3 transactional implementation
  ses_provider.go        AWS SES v2 implementation
  provider_test.go       Factory + interface contract tests
  brevo_provider_test.go Hermetic httptest-backed Brevo tests
  ses_provider_test.go   Hermetic fake-client SES tests

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
* **Migrating Brevo → SES** → flip `EMAIL_PROVIDER=ses` and populate `SES_AWS_REGION`, `SES_AWS_ACCESS_KEY_ID`, `SES_AWS_SECRET_ACCESS_KEY`, `SES_FROM_EMAIL`, `SES_TEMPLATE_NAMES`. Templates in Brevo do not migrate automatically; you maintain `SES_TEMPLATE_NAMES` independently. The forwarder is unchanged. **SES sandbox** — fresh AWS accounts can only send to verified recipient addresses until you request production access in the SES console.
* **Worker boots with `EMAIL_PROVIDER=ses` but no `SES_AWS_REGION` / creds / `SES_FROM_EMAIL`** → fast crash with explicit error per missing field. Operator opted into SES, silently noop'ing would hide the misconfiguration.
