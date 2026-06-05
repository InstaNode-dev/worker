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
	"strings"
)

// Provider identifier constants — used both by env-var parsing in
// config.Load() and by factory switching here. Named constants instead of
// inline strings so a typo trips compile-time and so the operator-facing
// docs (docs/email_providers.md) can reference them by name.
const (
	providerNameNoop     = "noop"
	providerNameBrevo    = "brevo"
	providerNameSES      = "ses"      // live — see ses_provider.go
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

	// SES holds SES-specific configuration. Populated only when
	// Provider == providerNameSES.
	SES SESConfig

	// Future: SendGrid SendGridConfig. Keep grouped so adding a
	// provider doesn't pollute the top-level Config namespace.

	// Fallbacks is the ordered EMAIL_PROVIDER_FALLBACK list (comma-separated
	// provider names, e.g. ["ses"]). When non-empty, NewProvider builds the
	// primary (cfg.Provider) plus each named fallback via the SAME switch and
	// returns a FailoverProvider that tries them in order. When empty/unset
	// the behavior is byte-identical to single-provider mode — this is the
	// inert-by-default safety contract (root CLAUDE.md flag-protect rule):
	// with no secondary configured the worker returns the bare single
	// provider exactly as it did before this feature existed.
	//
	// Each fallback uses the same per-provider sub-config already on Config
	// (Brevo/SES). An operator wiring SES as a fallback sets EMAIL_PROVIDER=
	// brevo, EMAIL_PROVIDER_FALLBACK=ses, and the SES_* env vars; both share
	// this one Config.
	Fallbacks []string
}

// NewProvider builds the EmailProvider selected by cfg.Provider, optionally
// wrapped in a FailoverProvider when cfg.Fallbacks is non-empty.
//
// Returns an error only when a provider name (primary OR a fallback) is set to
// an unknown value — empty or "noop" for the PRIMARY deliberately succeeds
// with NoopProvider so a missing env var doesn't crash the worker at boot.
//
// Inert default: cfg.Fallbacks empty/unset → returns the bare single provider
// exactly as before (no FailoverProvider wrapper). This is the safety contract;
// TestFactory_NoFallback_ReturnsBareProvider pins it.
//
// Fallback construction is fail-OPEN on missing creds: a fallback whose
// provider name is valid but whose required creds are unset (e.g. SES_* not
// yet wired) is logged with a clear warning and SKIPPED, not fatal. This lets
// an operator set EMAIL_PROVIDER_FALLBACK=ses ahead of the SES_* secrets
// landing without wedging the worker boot. An UNKNOWN fallback name is still a
// hard error (same strictness as the primary switch) — that's a config typo,
// not a not-yet-provisioned credential. If every fallback skips, the worker
// boots with just the primary (no wrapper), preserving today's behavior.
func NewProvider(cfg Config) (EmailProvider, error) {
	primary, err := buildOne(cfg.Provider, cfg)
	if err != nil {
		return nil, err
	}

	// Inert default — no fallbacks configured → single provider, byte-identical
	// to the pre-failover behavior. No FailoverProvider, no metric, no wrapper.
	if len(cfg.Fallbacks) == 0 {
		return primary, nil
	}

	chain := []EmailProvider{primary}
	for _, name := range cfg.Fallbacks {
		name = strings.TrimSpace(name)
		if name == "" {
			continue // tolerate "ses," / " , ses" sloppiness in the env var
		}
		fb, ferr := buildOne(name, cfg)
		if ferr != nil {
			if isMissingCredsErr(name, ferr) {
				// Fail-open: valid provider, creds not yet wired. Skip it so
				// the operator can stage EMAIL_PROVIDER_FALLBACK ahead of the
				// secret. Warn loudly so the gap is visible.
				slog.Warn("email.failover.fallback_skipped",
					"fallback", name,
					"reason", "required credentials unset — fallback unavailable until configured",
					"error", ferr,
				)
				continue
			}
			// Unknown provider name (config typo) → hard boot error.
			return nil, fmt.Errorf("email: invalid EMAIL_PROVIDER_FALLBACK %q: %w", name, ferr)
		}
		chain = append(chain, fb)
	}

	// Every fallback skipped (creds unset) → fall back to the bare primary so
	// the worker still boots and sends via the primary. No wrapper, same as
	// the inert default.
	if len(chain) == 1 {
		slog.Warn("email.failover.no_usable_fallbacks",
			"reason", "EMAIL_PROVIDER_FALLBACK set but every fallback was skipped (creds unset) — using primary only",
			"primary", primary.Name(),
		)
		return primary, nil
	}

	return NewFailoverProvider(chain...)
}

// buildOne constructs a single concrete EmailProvider by name, sharing the
// per-provider sub-configs on cfg. This is the ONE switch used by both the
// primary and every fallback — adding a provider means one new case here and
// it is instantly available as both a primary and a fallback (no duplicated
// switch). An empty/"noop" name yields NoopProvider (fail-open default for the
// primary); used as a fallback it is a harmless no-op that always skips.
func buildOne(name string, cfg Config) (EmailProvider, error) {
	switch name {
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

	case providerNameSES:
		return NewSESProvider(cfg.SES)

	// case providerNameSendGrid:
	//     return NewSendGridProvider(cfg.SendGrid)

	default:
		return nil, fmt.Errorf("email: unknown provider %q (supported: %q, %q, %q, %q)",
			name, providerNameNoop, providerNameBrevo, providerNameSES, "")
	}
}

// isMissingCredsErr reports whether a fallback construction error is a
// "required credential unset" condition (skip-with-warning) rather than an
// unknown-name typo (hard error). The provider constructors return a known
// non-nil error in exactly the missing-creds case; an unknown name is caught
// by the buildOne default branch and its message begins "email: unknown
// provider". We classify by "is the name a real provider" — if buildOne knew
// the name, any error from it is a construction/creds error, which we treat as
// skip-with-warning. This keeps the policy decision in one place.
func isMissingCredsErr(name string, _ error) bool {
	switch name {
	case providerNameBrevo, providerNameSES:
		return true // known provider whose constructor failed → missing creds
	default:
		return false // unknown name → hard error
	}
}
