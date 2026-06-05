package email

// failover_provider.go — ordered multi-ESP failover for outbound event email.
//
// A FailoverProvider wraps an ordered slice of EmailProvider (primary first,
// then one or more secondaries) and implements EmailProvider itself, so the
// forwarder is unchanged: it still holds a single EmailProvider and never
// knows whether one ESP or three sit behind the seam.
//
// Why this exists (task #35): the standing P0 is that the primary ESP (Brevo)
// returns 201 then internally rejects every send because the sender domain is
// unvalidated — so today *every* transactional email is silently lost at the
// relay. A failover lets the operator wire a second ESP (SES) so that when the
// primary errors or hard-rejects a send, the same EventEmail is immediately
// retried through the secondary. The unit of success is "did SOME provider
// accept the bytes", which is exactly the signal the forwarder needs for its
// claim-after-2xx contract (root CLAUDE.md / worker convention 6): the row is
// claimed iff at least one provider returned success.
//
// ── Decision rule (which result advances to the next provider) ──────────────
//
// SendEvent walks the chain in order and, for each provider, looks at the
// (messageID, error) pair:
//
//   - success (err == nil)                  → STOP, return success. Record
//                                             primary_ok if it was the first
//                                             provider, else fallback_ok.
//   - SendClassTransient  (5xx/network/429) → try the next provider. The
//                                             primary is unhealthy; the
//                                             secondary may still deliver.
//   - SendClassPermanent  (4xx/auth/reject) → try the next provider. This is
//                                             the P0 case: Brevo "rejects" the
//                                             payload at the account level, but
//                                             a healthy SES can still send it.
//                                             Treating Permanent as fall-through
//                                             is the whole point of the feature.
//   - SendClassSkippedNoTemplate            → try the next provider. The primary
//                                             has no template for this kind but a
//                                             secondary might. IMPORTANT: if EVERY
//                                             provider skips (none has a template),
//                                             the chain returns that last
//                                             SkippedNoTemplate verbatim so the
//                                             forwarder advances its cursor
//                                             silently — a kind nobody is
//                                             configured to send is not an error.
//                                             See the all-skipped note below.
//
// In short: ANY non-success result advances to the next provider; the first
// success wins; if all fail we return the LAST error. This is deliberately
// uniform (we do not stop early on a Permanent from the primary) because the
// secondary's verdict on the same payload is independent — a payload Brevo
// rejects for account reasons can be perfectly acceptable to SES.
//
// ── all-skipped vs all-failed ──────────────────────────────────────────────
//
// "All skipped" (every provider returned SendClassSkippedNoTemplate) is NOT a
// failure: it means no configured ESP has a template/renderer for this kind,
// which is the operator's explicit opt-out. We surface the last skip error so
// ClassOf() reports SkippedNoTemplate and the forwarder advances the cursor
// silently — and we do NOT bump the all_failed counter for it (an all-skip is
// not a degraded-ESP signal). "All failed" with at least one real Transient/
// Permanent error is the alert-able all_failed outcome.
//
// ── Observability ──────────────────────────────────────────────────────────
//
//   - metrics.EmailFailoverTotal{outcome} on every SendEvent:
//       primary_ok  — first provider succeeded.
//       fallback_ok — a later provider succeeded after the primary failed.
//       all_failed  — every provider failed (at least one real error).
//     (An all-skipped chain records primary_ok-equivalent? No — it is neither
//      a send nor a degraded signal, so it records NO outcome label, keeping
//      the counter strictly about real send attempts. See the loop below.)
//   - INFO log when a fallback engages (work performed — worker convention 1).
//   - DEBUG log when the primary succeeds first (idle/happy path stays quiet).
//   - Recipient emails are masked (worker convention 7 — stub right of '@').
//
// The FailoverProvider holds no network state of its own; all I/O is delegated
// to the wrapped providers, each of which already enforces its own timeout.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"instant.dev/worker/internal/metrics"
)

// Failover outcome labels for metrics.EmailFailoverTotal. Named constants so a
// typo trips compile-time and the infra alert/dashboard can reference them by
// name (no scattered string literals — CLAUDE.md no-hardcoded-strings rule).
const (
	failoverOutcomePrimaryOK  = "primary_ok"
	failoverOutcomeFallbackOK = "fallback_ok"
	failoverOutcomeAllFailed  = "all_failed"
)

// FailoverProvider sends through an ordered chain of EmailProvider, advancing
// to the next on any non-success result and returning the first success. It
// implements EmailProvider so it drops into the forwarder unchanged.
type FailoverProvider struct {
	// providers is the ordered chain, primary first. Guaranteed non-empty by
	// NewFailoverProvider (which errors on an empty/all-nil slice). Read-only
	// after construction; SendEvent only iterates.
	providers []EmailProvider

	// name is the precomputed "failover(brevo,ses)" label, built once at
	// construction so Name() is allocation-free on the hot path.
	name string
}

// NewFailoverProvider builds a FailoverProvider from an ordered chain. The
// first element is the primary. Returns an error when the chain is empty or
// contains only nils — a failover with nothing to fail over to is a
// programming error, not a runtime condition, so we surface it at boot rather
// than no-op'ing silently. Nil entries inside an otherwise-valid chain are
// dropped (defensive: a caller that conditionally skips a provider can pass a
// nil rather than rebuilding the slice).
func NewFailoverProvider(providers ...EmailProvider) (*FailoverProvider, error) {
	cleaned := make([]EmailProvider, 0, len(providers))
	for _, p := range providers {
		if p != nil {
			cleaned = append(cleaned, p)
		}
	}
	if len(cleaned) == 0 {
		return nil, fmt.Errorf("email: failover provider requires at least one non-nil provider")
	}
	names := make([]string, 0, len(cleaned))
	for _, p := range cleaned {
		names = append(names, p.Name())
	}
	return &FailoverProvider{
		providers: cleaned,
		name:      fmt.Sprintf("failover(%s)", strings.Join(names, ",")),
	}, nil
}

// Name returns the chain identifier, e.g. "failover(brevo,ses)". Stable for
// the lifetime of the binary; safe as a metric/log label.
func (p *FailoverProvider) Name() string { return p.name }

// SendEvent walks the provider chain in order. See the package-level decision
// rule for the full semantics. Returns the first provider's success, or the
// last provider's error if every provider fails.
func (p *FailoverProvider) SendEvent(ctx context.Context, evt EventEmail) (string, error) {
	var lastErr error
	sawRealError := false // at least one Transient/Permanent (not a bare skip)

	for i, prov := range p.providers {
		msgID, err := prov.SendEvent(ctx, evt)
		if err == nil {
			// Success. The chain stops here. Distinguish primary (idle/happy
			// path, DEBUG) from a fallback engagement (work performed, INFO).
			if i == 0 {
				metrics.EmailFailoverTotal.WithLabelValues(failoverOutcomePrimaryOK).Inc()
				slog.Debug("email.failover.primary_ok",
					"provider", prov.Name(),
					"kind", evt.Kind,
					"recipient", maskEmail(evt.Recipient),
				)
			} else {
				metrics.EmailFailoverTotal.WithLabelValues(failoverOutcomeFallbackOK).Inc()
				slog.Info("email.failover.fallback_ok",
					"failed_provider", p.providers[0].Name(),
					"succeeded_provider", prov.Name(),
					"chain_index", i,
					"kind", evt.Kind,
					"recipient", maskEmail(evt.Recipient),
					"note", "primary ESP failed/skipped; secondary accepted the send — email NOT lost",
				)
			}
			return msgID, nil
		}

		// Non-success. Record the error and decide whether it counts as a
		// "real" failure (Transient/Permanent) vs a bare skip. A skip means
		// "this provider has no template for this kind" — not a degraded ESP.
		lastErr = err
		if ClassOf(err) != SendClassSkippedNoTemplate {
			sawRealError = true
		}

		// Advance to the next provider. Log at INFO only when there IS a next
		// provider to engage (work is about to be performed); the terminal
		// failure is logged once after the loop.
		if i < len(p.providers)-1 {
			slog.Info("email.failover.advancing",
				"failed_provider", prov.Name(),
				"next_provider", p.providers[i+1].Name(),
				"class", ClassOf(err).String(),
				"kind", evt.Kind,
				"recipient", maskEmail(evt.Recipient),
			)
		}
	}

	// Every provider returned non-success.
	if sawRealError {
		// At least one provider hit a real error — this is the alert-able
		// all_failed outcome (both ESPs refusing → email being lost).
		metrics.EmailFailoverTotal.WithLabelValues(failoverOutcomeAllFailed).Inc()
		slog.Error("email.failover.all_failed",
			"chain", p.name,
			"kind", evt.Kind,
			"recipient", maskEmail(evt.Recipient),
			"last_error", lastErr,
			"note", "every ESP in the failover chain refused this send — email lost; forwarder holds cursor on the last error class",
		)
	} else {
		// Every provider returned SkippedNoTemplate — no configured ESP has a
		// template for this kind. Not a degraded-ESP signal, so NO all_failed
		// metric and only a DEBUG line. The returned (skip) error makes
		// ClassOf() report SkippedNoTemplate so the forwarder advances silently.
		slog.Debug("email.failover.all_skipped",
			"chain", p.name,
			"kind", evt.Kind,
			"recipient", maskEmail(evt.Recipient),
		)
	}

	// Return the last error verbatim so its SendClass drives the forwarder's
	// cursor decision (Transient → hold, Permanent/Skipped → advance). Since
	// we walk in order, the last error is the last (lowest-priority) provider's
	// verdict — a deliberate choice: if the operator put the more-reliable ESP
	// last, its classification is what the forwarder should act on.
	return "", lastErr
}

// Compile-time interface satisfaction check — proves the seam fits. If a
// future EmailProvider change breaks FailoverProvider, the build fails here.
var _ EmailProvider = (*FailoverProvider)(nil)
