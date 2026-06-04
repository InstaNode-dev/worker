package jobs

// billing_reconciler_integration_test.go — REAL-Postgres integration test for
// the billing reconciler's terminal-downgrade round-trip.
//
// Per INTEGRATION-COVERAGE-PLAN-2026-06-04.md §5 #1, billing_reconciler is a
// Tier-1 (money-adjacent) job whose DB round-trip was fake-tested only — the
// sibling billing_reconciler_test.go drives Work() with a stubGrace and asserts
// the stub's call counts, never the persisted teams row. THIS test seeds a real
// paid (pro) team carrying a Razorpay subscription id, runs Work() against a
// live platform Postgres with a stub fetcher that reports the subscription
// `cancelled` (a terminal status), and asserts the worker actually wrote:
//
//   - teams.plan_tier flipped pro → "hobby" (terminalDowngradeTier — never the
//     ephemeral "free" tier; the cross-repo contract D28 F1), AND
//   - a subscription.canceled audit_log row was emitted for the team (the
//     event the email forwarder dispatches the cancellation email from).
//
// The Razorpay leg stays stubbed (no live Razorpay — CLAUDE P0: recurring not
// enabled, plan §4 BLOCKED-bypass: drive the state directly). The DB leg —
// the candidate SELECT, the UpdatePlanTier UPDATE, the audit INSERT — is REAL.
// This is the convergence a sqlmock test cannot prove: that the downgrade
// UPDATE's WHERE id = $2 matched the live row and the audit row landed.
//
// Determinism on a shared local DB: the reconciler scans EVERY team with a
// non-empty stripe_customer_id, not just ours. The stub fetcher therefore
// keys on subscription_id — it returns `cancelled` ONLY for the seeded team's
// subscription and a benign `active`-at-hobby (a no-op for any already-hobby
// team) for everything else, so the sweep cannot mutate unrelated rows in a way
// that perturbs this assertion. grace is left nil so the real dbGracePeriodOpener
// runs against the live DB (its TerminateActiveGracePeriod is an idempotent
// no-op when the team has no active grace row).
//
// GATING: testhelpers.SetupTestDB skips under -short / no-DB, so the regular
// `make gate` stays green without a Postgres; the non-short CI integration job
// runs it for real.

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"instant.dev/worker/internal/testhelpers"
)

// subKeyedFetcher is a subscriptionFetcher that returns a caller-supplied
// status for ONE target subscription id and a benign no-op status (`active` at
// the lowest paid tier) for every other id. Keyed-by-sub so the reconciler's
// full-table sweep cannot perturb the assertion on a shared DB.
type subKeyedFetcher struct {
	targetSub    string
	targetStatus string
	planID       string
}

func (f *subKeyedFetcher) FetchSubscriptionForReconciler(_ context.Context, subID string) (*reconcilerSubscriptionDetails, error) {
	if subID == f.targetSub {
		return &reconcilerSubscriptionDetails{
			Status:    f.targetStatus,
			PlanID:    f.planID,
			PaidCount: 1,
		}, nil
	}
	// Any other team in the shared DB: report `created` — a guaranteed
	// no-action status (rzpStatusClassNoAction). This is strictly safer than
	// `active`, which (with an unrecognised plan_id) resolves to "hobby" and
	// could upgrade an anonymous/free team that happens to carry a
	// subscription id in a polluted local DB. `created` mutates nothing, so
	// the sweep touches ONLY the seeded target team.
	return &reconcilerSubscriptionDetails{Status: "created", PlanID: "", PaidCount: 0}, nil
}

func fakeBillingJob() *river.Job[BillingReconcilerArgs] {
	return &river.Job[BillingReconcilerArgs]{JobRow: &rivertype.JobRow{ID: 1}}
}

// TestIntegration_BillingReconciler_TerminalDowngradePersistsHobby seeds a
// pro-tier team with a Razorpay subscription id, runs the reconciler with a
// fetcher that reports that subscription `cancelled`, and asserts the worker
// persisted plan_tier='hobby' to the REAL team row AND emitted the
// subscription.canceled audit row.
func TestIntegration_BillingReconciler_TerminalDowngradePersistsHobby(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	subID := "sub_itest_" + uuid.New().String()[:12]
	teamID := testhelpers.SeedTeamWithSubscription(t, db, "pro", subID)

	fetcher := &subKeyedFetcher{targetSub: subID, targetStatus: "cancelled"}
	// grace == nil → real dbGracePeriodOpener against the live DB.
	w := NewBillingReconcilerWorker(db, fetcher, nil)

	if err := w.Work(context.Background(), fakeBillingJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	// The team's plan_tier must have flipped pro → hobby (terminalDowngradeTier).
	got := testhelpers.TeamPlanTier(t, db, teamID)
	if got != terminalDowngradeTier {
		t.Errorf("plan_tier = %q after terminal downgrade, want %q (terminalDowngradeTier)", got, terminalDowngradeTier)
	}
	if got == "free" {
		t.Error("terminal downgrade landed on 'free' — must be 'hobby' (D28 F1: 'free' strands permanent paid resources)")
	}

	// A subscription.canceled audit row must have been emitted for the team —
	// this is what the event-email forwarder dispatches the cancellation
	// confirmation email from.
	if n := testhelpers.CountAuditLogByTeam(t, db, teamID, "subscription.canceled"); n < 1 {
		t.Errorf("subscription.canceled audit rows = %d, want >= 1 (the downgrade must emit the cancellation audit)", n)
	}
}

// TestIntegration_BillingReconciler_ActiveNoDriftLeavesTierUntouched is the
// complement: a paid team Razorpay reports `active` at its CURRENT tier must
// NOT be re-written (no gap → continue) and NO audit row is emitted. This pins
// the live "DB tier already at/above expected" no-op branch against real data —
// a regression that always-writes would burn a tier UPDATE + a spurious
// upgrade email every 15-minute tick. It is also fast: the target sorts first
// and the stub reports a no-action for everything else.
func TestIntegration_BillingReconciler_ActiveNoDriftLeavesTierUntouched(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	subID := "sub_itest_" + uuid.New().String()[:12]
	teamID := testhelpers.SeedTeamWithSubscription(t, db, "pro", subID)

	// Razorpay reports active; with an empty plan_id the expected tier resolves
	// to "hobby" — and the team is already "pro" (>= hobby) → no-op.
	fetcher := &subKeyedFetcher{targetSub: subID, targetStatus: "active"}
	w := NewBillingReconcilerWorker(db, fetcher, nil)

	if err := w.Work(context.Background(), fakeBillingJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	// Tier untouched (still pro — NOT downgraded to the resolved-hobby).
	if got := testhelpers.TeamPlanTier(t, db, teamID); got != "pro" {
		t.Errorf("plan_tier = %q for an at-or-above-expected team, want \"pro\" (untouched)", got)
	}
	// No upgrade / cancel audit emitted for a no-op.
	if n := testhelpers.CountAuditLogByTeam(t, db, teamID, "subscription.upgraded"); n != 0 {
		t.Errorf("subscription.upgraded audit rows = %d for a no-op tick, want 0", n)
	}
	if n := testhelpers.CountAuditLogByTeam(t, db, teamID, "subscription.canceled"); n != 0 {
		t.Errorf("subscription.canceled audit rows = %d for a no-op tick, want 0", n)
	}
}
