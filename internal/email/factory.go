package email

// factory.go — single switch that turns config into a concrete EmailProvider.
//
// Adding a provider = 1 new case + 1 new file implementing EmailProvider.
// That's the "minimal future change" bar set by the brief: SES support
// later is `case providerNameSES: return NewSESProvider(cfg.SES)` plus
// `internal/email/ses_provider.go`. No forwarder change, no test change,
// no audit-mapping change.

import (
	"fmt"
	"log/slog"
)

// Provider identifier constants — used both by env-var parsing in
// config.Load() and by factory switching here. Named constants instead of
// inline strings so a typo trips compile-time and so the operator-facing
// docs (docs/email_providers.md) can reference them by name.
const (
	providerNameNoop     = "noop"
	providerNameBrevo    = "brevo"
	providerNameSES      = "ses"      // reserved — stub in ses_provider.go
	providerNameSendGrid = "sendgrid" // reserved — not yet implemented
)

// Config is the provider-agnostic config the factory needs. It groups
// per-provider sub-configs (Brevo today; SES/SendGrid later) plus the
// top-level Provider selector.
//
// Adding SES = add an SESConfig field here, a `case providerNameSES`
// branch below, and the SES file. config.Load() reads the new env vars
// into Config.SES. Nothing else changes.
type Config struct {
	// Provider is the EMAIL_PROVIDER env var: "brevo" / "noop" / "" /
	// future "ses" / "sendgrid". Empty string is treated as "noop"
	// (fail-open — operators who haven't set the var get a silent no-op
	// rather than a boot crash).
	Provider string

	// Brevo holds Brevo-specific configuration. Populated only when
	// Provider == providerNameBrevo.
	Brevo BrevoConfig

	// Future: SES SESConfig, SendGrid SendGridConfig. Keep grouped so
	// adding a provider doesn't pollute the top-level Config namespace.
}

// NewProvider builds the EmailProvider selected by cfg.Provider. Returns
// an error only when the provider name is set to an unknown value — empty
// or "noop" deliberately succeeds with NoopProvider so a missing env var
// doesn't crash the worker at boot.
func NewProvider(cfg Config) (EmailProvider, error) {
	switch cfg.Provider {
	case "", providerNameNoop:
		// Fail-open: an operator who hasn't configured an email provider
		// gets silent no-ops, not a boot crash. The warning surfaces
		// this clearly so it's not invisible.
		slog.Warn("email.provider.disabled",
			"reason", "EMAIL_PROVIDER unset or 'noop' — event emails will be dropped",
		)
		return &NoopProvider{}, nil

	case providerNameBrevo:
		return NewBrevoProvider(cfg.Brevo)

	// case providerNameSES:
	//     return NewSESProvider(cfg.SES)
	// case providerNameSendGrid:
	//     return NewSendGridProvider(cfg.SendGrid)

	default:
		return nil, fmt.Errorf("email: unknown EMAIL_PROVIDER %q (supported: %q, %q, %q)",
			cfg.Provider, providerNameNoop, providerNameBrevo, "")
	}
}
