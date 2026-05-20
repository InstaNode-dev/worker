package email

// noop_provider.go — the default provider when EMAIL_PROVIDER is unset
// or "noop". Returns SendClassSkippedNoTemplate on every send so the
// forwarder advances its cursor (no retries, no DB rows pile up).
//
// This is the fail-open contract the worker has always had with
// lifecycle email: a missing API key MUST NOT block the audit_log
// pipeline behind it. The warning logged once at boot (factory.go) is
// the only operator-visible signal that emails are being dropped — the
// per-send logs here stay at DEBUG so a healthy worker isn't drowning
// in lines.

import (
	"context"
	"log/slog"
)

// NoopProvider is the inert EmailProvider used when EMAIL_PROVIDER is
// empty or "noop". Zero-value safe — `&NoopProvider{}` is the standard
// constructor pattern.
type NoopProvider struct{}

// SendEvent returns a *SendError with SendClassSkippedNoTemplate so the
// forwarder advances its cursor past the row. The "skipped" class is the
// right signal here: the operator chose to disable email by leaving the
// provider unset, not that the row is poisoned (Permanent) or that the
// provider is having a bad day (Transient).
//
// Returns "" for the messageId — the noop provider never reaches a real
// upstream, so there's no per-send id to surface. The forwarder's
// markSent call falls back to EventEmail.IdempotencyKey in that case
// (the historical behaviour pre the brief's worker change).
func (n *NoopProvider) SendEvent(_ context.Context, evt EventEmail) (string, error) {
	slog.Debug("email.noop.skip",
		"kind", evt.Kind,
		"recipient", evt.Recipient,
	)
	return "", &SendError{
		Class:   SendClassSkippedNoTemplate,
		Message: "noop provider — EMAIL_PROVIDER unset",
	}
}

// Name returns the stable provider identifier.
func (n *NoopProvider) Name() string { return providerNameNoop }
