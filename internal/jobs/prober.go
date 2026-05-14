package jobs

// prober.go — common interface used by provisioner_reconciler and
// resource_heartbeat to verify that a customer-facing resource is reachable.
//
// Foundation copied from the W5-A heartbeat worktree so the real prober
// (real_prober.go in this PR) can be wired against the same interface even
// before W5-A's heartbeat job lands. If/when W5-A merges first, the duplicate
// definition is removed in the rebase; if THIS lands first, W5-A rebases on
// top with no interface diff. Either order works because both branches treat
// this file as the contract.
//
// Resource types this interface supports (resource_type literal → action):
//
//   postgres / vector → SELECT 1
//   redis             → PING
//   mongodb           → adminCommand({ping: 1})
//   storage           → HEAD against the bucket endpoint
//   queue             → NATS /healthz monitoring HTTP probe
//   webhook           → skip — customer-managed; brief explicitly excludes
//                       webhooks because we don't run the receiver
//
// Probe must respect the passed ctx for deadlines / cancellation; both jobs
// pass a 5s context deadline per resource.

import (
	"context"
	"errors"
)

// ProbeOutcome is returned by ResourceProber to disambiguate the three
// outcomes the reconciler needs to act on:
//
//   ProbeReachable   — connection succeeded; for reconciler this means "the
//                      provisioner did create this row, flip status=active".
//   ProbeUnreachable — connection definitively failed; for reconciler this
//                      means "abandon; flip status=failed".
//   ProbeSkip        — probe not applicable (webhook, unknown type). Heartbeat
//                      treats this as "leave row alone"; reconciler treats it
//                      as "can't tell, leave pending for a retry next tick".
type ProbeOutcome int

const (
	// ProbeReachable means the prober opened a connection and the lightweight
	// liveness check (SELECT 1 / PING / etc.) succeeded.
	ProbeReachable ProbeOutcome = iota

	// ProbeUnreachable means the prober tried and failed within the deadline.
	// The error returned alongside this outcome is the user-facing reason
	// (truncated to 500 chars before being stored in degraded_reason).
	ProbeUnreachable

	// ProbeSkip means this resource_type has no defined probe (webhook today,
	// or an unknown future type). Not an error; both jobs no-op the row.
	ProbeSkip
)

// ResourceProber verifies that a customer's resource is reachable. The
// implementation is responsible for translating resource_type + the (still-
// encrypted) connection_url bytes into a concrete connection attempt.
//
// Returning (ProbeUnreachable, err) MUST set err to a non-nil value — the
// jobs use that text in audit_log metadata and in resources.degraded_reason.
// Returning (ProbeReachable, _) ignores err; callers MUST NOT rely on the
// error value being nil in that branch.
type ResourceProber interface {
	Probe(ctx context.Context, resourceType, connectionURL string) (ProbeOutcome, error)
}

// NoopProber is the default when no real prober is wired. Every probe
// returns ProbeReachable so:
//
//   * resource_heartbeat keeps last_seen_at fresh on every tick and never
//     trips degraded=true.
//   * provisioner_reconciler treats every pending-but-stuck row as recovered
//     (status='active') — NOT the desired behaviour in prod, but the brief's
//     fail-open contract for "we can't actually check" is "prefer
//     false-negatives over false-positives", and a row that's still genuinely
//     broken will surface via the dashboard's resource-list query (where its
//     connection_url will fail when the customer's app tries to use it).
//
// Operators MUST wire a real prober before the brief's "any pending > 30 min"
// alert is meaningful. See CHANGES.md (worker PR) for the rollout plan.
type NoopProber struct{}

// Probe always returns ProbeReachable. nil error.
func (NoopProber) Probe(_ context.Context, _, _ string) (ProbeOutcome, error) {
	return ProbeReachable, nil
}

// ErrProberUnconfigured is the canonical error a prober returns when it
// detects an unrecoverable misconfiguration mid-probe (e.g. AES_KEY missing
// and the connection_url decrypts to garbage). Callers translate this into
// ProbeSkip rather than ProbeUnreachable — a config gap is not the
// customer's resource being broken.
var ErrProberUnconfigured = errors.New("prober: not configured")
