package email

// ses_provider.go — SKELETON, NOT WIRED.
//
// Lives in this package as a deliberate proof point: adding a new
// provider after Brevo is one new file that implements EmailProvider +
// one new case in factory.go. This file compiles, satisfies the
// EmailProvider interface (verified by the package-level _ assertion at
// the bottom), and is intentionally absent from factory.go's switch so
// production never picks it up.
//
// To wire SES for real:
//
//   1. Add `SES SESConfig` field on Config (factory.go).
//   2. Add `case providerNameSES: return NewSESProvider(cfg.SES)` to
//      NewProvider in factory.go.
//   3. Add AWS_* env parsing in internal/config/config.go.
//   4. Replace the TODO inside SendEvent with an aws-sdk-go-v2 sesv2
//      client call and classify the smithy.APIError into SendClass
//      (4xx → Permanent, 5xx + net.Error → Transient, etc.).
//   5. Mirror brevo_provider_test.go for hermetic test coverage.
//
// Estimated additional LOC: ~80 (real implementation) + ~150 (tests).
// Forwarder, mapping, mapping tests: zero changes.
//
// Documented in detail in docs/email_providers.md.

import (
	"context"
	"fmt"
)

// SESConfig is the SES-specific configuration. Populated from env at
// startup once SES is wired. Today the struct exists only so the
// stub compiles and so docs/email_providers.md can reference it.
type SESConfig struct {
	Region          string            // AWS_REGION
	AccessKeyID     string            // AWS_ACCESS_KEY_ID
	SecretAccessKey string            // AWS_SECRET_ACCESS_KEY
	TemplateNames   map[string]string // SES_TEMPLATE_NAMES: kind → SES template name
	SourceAddress   string            // SES_SOURCE_ADDRESS: verified sender address
}

// SESProvider is the (not-wired) SES implementation. The struct holds
// only what's needed to demonstrate interface satisfaction; the real
// implementation will hold an *sesv2.Client.
type SESProvider struct {
	cfg SESConfig
}

// NewSESProvider validates the config the same way NewBrevoProvider does
// — explicit, loud failure when EMAIL_PROVIDER=ses is set without
// credentials.
func NewSESProvider(cfg SESConfig) (*SESProvider, error) {
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, fmt.Errorf("ses: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY required when EMAIL_PROVIDER=ses")
	}
	if cfg.SourceAddress == "" {
		return nil, fmt.Errorf("ses: SES_SOURCE_ADDRESS required (must be a verified SES identity)")
	}
	return &SESProvider{cfg: cfg}, nil
}

// Name returns the stable provider identifier.
func (p *SESProvider) Name() string { return providerNameSES }

// SendEvent — TODO. The real implementation will:
//   - Look up p.cfg.TemplateNames[evt.Kind]; nil → SendClassSkippedNoTemplate.
//   - Call sesv2.SendEmail with Destination.ToAddresses=[evt.Recipient],
//     Content.Template={TemplateName: tmpl, TemplateData: jsonOf(evt.Params)},
//     FromEmailAddress=p.cfg.SourceAddress.
//   - On smithy.APIError 4xx → SendClassPermanent.
//   - On smithy.APIError 5xx or net.Error → SendClassTransient.
//
// Until then, every send returns SkippedNoTemplate so the stub is a
// well-behaved citizen if anyone wires it by accident.
func (p *SESProvider) SendEvent(_ context.Context, evt EventEmail) error {
	return &SendError{
		Class:   SendClassSkippedNoTemplate,
		Message: fmt.Sprintf("ses: provider stubbed (kind=%q); see docs/email_providers.md for wiring instructions", evt.Kind),
	}
}

// Compile-time interface satisfaction check — proves the seam fits
// before SES is wired. If a future EmailProvider change breaks SES, the
// build fails here, not silently when an operator flips EMAIL_PROVIDER=ses.
var _ EmailProvider = (*SESProvider)(nil)
