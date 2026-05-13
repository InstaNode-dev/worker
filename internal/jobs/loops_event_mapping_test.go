package jobs

// loops_event_mapping_test.go — exercises every per-kind builder and the
// invariants between supportedAuditKinds and loopsEventBuilders.

import (
	"testing"
)

// TestLoops_AllSupportedKindsHaveBuilder is the schema invariant: every
// kind in the SQL filter MUST have a builder, otherwise the forwarder
// hits the "no_builder_for_kind" log path and advances past real events.
func TestLoops_AllSupportedKindsHaveBuilder(t *testing.T) {
	for _, k := range supportedAuditKinds {
		if _, ok := loopsEventBuilders[k]; !ok {
			t.Errorf("supportedAuditKinds includes %q but loopsEventBuilders has no entry — adding the kind to the SQL filter without a builder makes the forwarder advance past real events", k)
		}
	}
	// And the other direction — no orphan builder for an unfilter'd kind.
	supportedSet := make(map[string]bool, len(supportedAuditKinds))
	for _, k := range supportedAuditKinds {
		supportedSet[k] = true
	}
	for k := range loopsEventBuilders {
		if !supportedSet[k] {
			t.Errorf("loopsEventBuilders has entry for %q but supportedAuditKinds doesn't include it — the SQL filter will never fetch this kind, making the builder dead code", k)
		}
	}
}

// TestLoops_BuilderEventNames pins the (audit_kind, loops_event_name) pairs
// so a typo in either side is caught at compile + test time. The brief
// names these explicitly — they are part of the Loops campaign config and
// MUST NOT drift.
func TestLoops_BuilderEventNames(t *testing.T) {
	cases := []struct {
		auditKind string
		wantEvent string
	}{
		{auditKindOnboardingClaimed, loopsEventTeamClaimed},
		{auditKindSubscriptionUpgraded, loopsEventTierUpgraded},
		{auditKindNearQuotaWall, loopsEventNearQuotaWall},
		{auditKindResourceExpiryImminent, loopsEventResourceExpiring},
		{auditKindSubscriptionDowngraded, loopsEventTierDowngraded},
		{auditKindSubscriptionCanceled, loopsEventSubscriptionCancel},
		{auditKindExperimentConversion, loopsEventExperimentClicked},
		{auditKindAdminTierChanged, loopsEventTierChangedByAdmin},
		{auditKindAdminPromoIssued, loopsEventPromoCodeReceived},
	}
	for _, tc := range cases {
		b := loopsEventBuilders[tc.auditKind]
		if b == nil {
			t.Errorf("no builder for %q", tc.auditKind)
			continue
		}
		row := auditRow{
			ID:         "x",
			TeamID:     "team",
			Kind:       tc.auditKind,
			OwnerEmail: "e@example.com",
		}
		p, ok := b(row)
		if !ok {
			t.Errorf("builder for %q returned ok=false with valid email", tc.auditKind)
			continue
		}
		if p.EventName != tc.wantEvent {
			t.Errorf("kind=%q → eventName=%q; want %q", tc.auditKind, p.EventName, tc.wantEvent)
		}
		if p.UserID != "e@example.com" {
			t.Errorf("kind=%q → userId=%q; want e@example.com (per Loops dedupe contract)", tc.auditKind, p.UserID)
		}
	}
}

// TestLoops_BuilderReturnsFalseOnNoEmail verifies every builder skips a row
// with no owner email — the forwarder relies on this to advance past
// orphan rows.
func TestLoops_BuilderReturnsFalseOnNoEmail(t *testing.T) {
	for kind, b := range loopsEventBuilders {
		_, ok := b(auditRow{Kind: kind, OwnerEmail: ""})
		if ok {
			t.Errorf("builder for %q returned ok=true with empty email — every builder must require an owner email for Loops.userId", kind)
		}
	}
}

// TestLoops_BuilderPropagatesMetadata verifies that metadata fields from
// the audit row flow into eventProperties. Spot-checks the upgrade case
// because it has the most operationally-important fields (from_tier,
// to_tier, mrr).
func TestLoops_BuilderPropagatesMetadata(t *testing.T) {
	row := auditRow{
		ID:         "x",
		TeamID:     "t",
		Kind:       auditKindSubscriptionUpgraded,
		OwnerEmail: "u@example.com",
		Metadata:   []byte(`{"from_tier":"hobby","to_tier":"pro","mrr":49}`),
	}
	p, ok := buildTierUpgraded(row)
	if !ok {
		t.Fatal("builder returned ok=false unexpectedly")
	}
	if p.EventProperties["from_tier"] != "hobby" {
		t.Errorf("from_tier = %v; want hobby", p.EventProperties["from_tier"])
	}
	if p.EventProperties["to_tier"] != "pro" {
		t.Errorf("to_tier = %v; want pro", p.EventProperties["to_tier"])
	}
	// JSON numbers decode to float64 — assert the value not the type.
	if got, _ := p.EventProperties["mrr"].(float64); got != 49 {
		t.Errorf("mrr = %v; want 49", p.EventProperties["mrr"])
	}
}

// TestLoops_DecodeMetaHandlesNil verifies the nil-safe contract — a row
// with no metadata JSONB MUST NOT panic the builder.
func TestLoops_DecodeMetaHandlesNil(t *testing.T) {
	m := decodeMeta(nil)
	if m == nil {
		t.Error("decodeMeta(nil) returned nil; want empty map so builders can index without nil-check")
	}
	m = decodeMeta([]byte(`{"not-json`)) // malformed
	if m == nil {
		t.Error("decodeMeta(malformed) returned nil; want empty map so builders can index without nil-check")
	}
}
