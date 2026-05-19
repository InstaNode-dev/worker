package jobs

// checkout_reconcile_test.go — invariant + behaviour guards for the
// CheckoutReconcileWorker and its checkout.abandoned email kind.
//
// Package-internal (package jobs) so the tests can read the unexported
// tunables and the unexported reconcilerSubscriptionDetails / builder /
// renderer surfaces directly — matching deploy_status_reconcile_test.go and
// event_email_mapping_test.go.

import (
	"strings"
	"testing"
	"time"
)

// TestCheckoutReconcile_GracePeriodIsPositive guards the brief's contract:
// the grace window must be a positive duration. A zero/negative window
// would email the customer the instant the api inserts the pending_checkouts
// row — before they have even loaded Razorpay's checkout page.
func TestCheckoutReconcile_GracePeriodIsPositive(t *testing.T) {
	if checkoutReconcileGracePeriod <= 0 {
		t.Errorf("checkoutReconcileGracePeriod = %s; must be > 0 or every checkout is emailed instantly", checkoutReconcileGracePeriod)
	}
	// The brief pins 15 minutes explicitly.
	if checkoutReconcileGracePeriod != 15*time.Minute {
		t.Errorf("checkoutReconcileGracePeriod = %s; the cross-repo contract specifies 15 minutes", checkoutReconcileGracePeriod)
	}
}

// TestCheckoutReconcile_IntervalSaneVsGrace pins the sizing relationship:
// the sweep cadence must be shorter than the grace window, otherwise an
// abandoned checkout could sit a full grace-window-plus-interval before the
// next tick even looks at it.
func TestCheckoutReconcile_IntervalSaneVsGrace(t *testing.T) {
	if checkoutReconcileInterval <= 0 {
		t.Fatalf("checkoutReconcileInterval = %s; must be > 0", checkoutReconcileInterval)
	}
	if checkoutReconcileInterval >= checkoutReconcileGracePeriod {
		t.Errorf("checkoutReconcileInterval (%s) should be < checkoutReconcileGracePeriod (%s) so a "+
			"checkout is picked up within roughly one interval of crossing the grace boundary",
			checkoutReconcileInterval, checkoutReconcileGracePeriod)
	}
}

// TestCheckoutReconcile_BatchLimitIsPositive — a zero/negative LIMIT would
// make every sweep a no-op (Postgres rejects LIMIT < 0; LIMIT 0 returns no
// rows) and silently never email anyone.
func TestCheckoutReconcile_BatchLimitIsPositive(t *testing.T) {
	if checkoutReconcileBatchLimit <= 0 {
		t.Errorf("checkoutReconcileBatchLimit = %d; must be > 0 or the sweep fetches no candidates", checkoutReconcileBatchLimit)
	}
}

// TestCheckoutReconcile_EmailKindFullyWired is the registry-iterating
// coverage guard mandated by CLAUDE.md rules 18 + 70. The new
// checkout.abandoned kind MUST be in all three registries — the SQL filter
// (supportedAuditKinds), the builder map, and the Go-renderer map — or the
// EventEmailForwarder silently drops the customer email.
//
// It does NOT hand-type "checkout.abandoned" as a separate list: it asserts
// the kind is present in supportedAuditKinds (the live SQL filter) and then
// re-runs the same three-registry check the forwarder depends on, so a
// future edit that half-registers the kind fails CI here.
func TestCheckoutReconcile_EmailKindFullyWired(t *testing.T) {
	found := false
	for _, k := range supportedAuditKinds {
		if k == auditKindCheckoutAbandonedEmail {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("checkout.abandoned (%q) is not in supportedAuditKinds — the forwarder's SQL filter "+
			"never fetches the row, so the abandoned-checkout email is never sent", auditKindCheckoutAbandonedEmail)
	}
	if _, ok := eventEmailBuilders[auditKindCheckoutAbandonedEmail]; !ok {
		t.Errorf("checkout.abandoned has no eventEmailBuilders entry — forwarder hits no_builder_for_kind and skips the row")
	}
	if _, ok := eventEmailBodyRenderers[auditKindCheckoutAbandonedEmail]; !ok {
		t.Errorf("checkout.abandoned has no eventEmailBodyRenderers entry — falls through to the legacy broken Brevo template path (CLAUDE rule 70)")
	}
}

// TestCheckoutReconcile_KindConstantsMatch pins the cross-file invariant:
// the producer-side constant (auditKindCheckoutAbandoned, written to
// audit_log by checkout_reconcile.go) and the consumer-side alias
// (auditKindCheckoutAbandonedEmail, used in the email registries) must be
// byte-for-byte identical, or the SQL filter never matches the rows the
// worker writes (CLAUDE rule 16 — single source of the string).
func TestCheckoutReconcile_KindConstantsMatch(t *testing.T) {
	if auditKindCheckoutAbandoned != auditKindCheckoutAbandonedEmail {
		t.Errorf("auditKindCheckoutAbandoned (%q) != auditKindCheckoutAbandonedEmail (%q) — "+
			"the worker writes one literal and the forwarder filters on the other; no email is ever sent",
			auditKindCheckoutAbandoned, auditKindCheckoutAbandonedEmail)
	}
}

// TestCheckoutReconcile_BuilderExtractsParams exercises buildCheckoutAbandoned
// end to end: with a resolvable recipient it must succeed and surface the
// per-kind params; with no recipient it must bail (ok=false) so the
// forwarder advances past a malformed row.
func TestCheckoutReconcile_BuilderExtractsParams(t *testing.T) {
	b := eventEmailBuilders[auditKindCheckoutAbandonedEmail]
	if b == nil {
		t.Fatal("no builder registered for checkout.abandoned")
	}

	// Recipient resolves from metadata.email (pending_checkouts customers
	// have no users row, so OwnerEmail is empty — the COALESCE fallback).
	params, ok := b(auditRow{
		ID:       "a-1",
		TeamID:   "t-1",
		Kind:     auditKindCheckoutAbandonedEmail,
		Metadata: []byte(`{"email":"buyer@example.com","subscription_id":"sub_ABC","plan_tier":"pro"}`),
	})
	if !ok {
		t.Fatal("builder returned ok=false with a metadata.email recipient")
	}
	if params["team_id"] != "t-1" {
		t.Errorf("builder did not set base team_id param: got %q", params["team_id"])
	}
	if params["subscription_id"] != "sub_ABC" {
		t.Errorf("subscription_id param = %q; want sub_ABC", params["subscription_id"])
	}
	if params["plan_tier"] != "pro" {
		t.Errorf("plan_tier param = %q; want pro", params["plan_tier"])
	}

	// No recipient anywhere — builder must bail.
	if _, ok := b(auditRow{ID: "a-2", TeamID: "t-2", Kind: auditKindCheckoutAbandonedEmail}); ok {
		t.Error("builder returned ok=true for a row with no resolvable recipient")
	}
}

// TestCheckoutReconcile_RendererProducesEmail exercises renderCheckoutAbandoned
// and asserts the rendered email is non-empty and on-message — it must NOT
// leak the broken expiry-template "expires in 6 hours" copy that triggered
// the three prior regressions.
func TestCheckoutReconcile_RendererProducesEmail(t *testing.T) {
	r := eventEmailBodyRenderers[auditKindCheckoutAbandonedEmail]
	if r == nil {
		t.Fatal("no renderer registered for checkout.abandoned")
	}
	subject, html, text := r(map[string]string{"plan_tier": "pro", "team_id": "t-1"})
	if strings.TrimSpace(subject) == "" {
		t.Error("renderer produced an empty subject")
	}
	if strings.TrimSpace(html) == "" {
		t.Error("renderer produced an empty HTML body")
	}
	if strings.TrimSpace(text) == "" {
		t.Error("renderer produced an empty plain-text body")
	}
	if strings.Contains(strings.ToLower(html), "expires in 6 hours") {
		t.Error("renderer leaked the broken expiry-template copy")
	}
	// The plan_tier param should reach the body.
	if !strings.Contains(html, "pro") {
		t.Errorf("renderer did not surface plan_tier in the HTML body: %q", html)
	}
}

// TestCheckoutReconcile_SubscriptionLooksResolved pins the best-effort
// Razorpay double-check decision table: a subscription is treated as
// "actually upgraded" (skip the email) when Razorpay reports it
// active/authenticated OR with any successful charge cycle. Anything else
// (created/pending/halted/cancelled, or a nil details) is NOT resolved —
// the customer still gets the abandoned-checkout email.
func TestCheckoutReconcile_SubscriptionLooksResolved(t *testing.T) {
	cases := []struct {
		name   string
		det    *reconcilerSubscriptionDetails
		expect bool
	}{
		{"nil details", nil, false},
		{"active status", &reconcilerSubscriptionDetails{Status: "active"}, true},
		{"authenticated status", &reconcilerSubscriptionDetails{Status: "authenticated"}, true},
		{"paid_count positive", &reconcilerSubscriptionDetails{Status: "created", PaidCount: 1}, true},
		{"created, no payments", &reconcilerSubscriptionDetails{Status: "created"}, false},
		{"pending, no payments", &reconcilerSubscriptionDetails{Status: "pending"}, false},
		{"halted, no payments", &reconcilerSubscriptionDetails{Status: "halted"}, false},
		{"cancelled, no payments", &reconcilerSubscriptionDetails{Status: "cancelled"}, false},
		{"empty status, no payments", &reconcilerSubscriptionDetails{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := checkoutSubscriptionLooksResolved(tc.det); got != tc.expect {
				t.Errorf("checkoutSubscriptionLooksResolved(%+v) = %v; want %v", tc.det, got, tc.expect)
			}
		})
	}
}
