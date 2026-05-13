package jobs

// event_email_mapping_test.go — exercises every per-kind builder and the
// invariants between supportedAuditKinds and eventEmailBuilders.

import (
	"testing"
)

// TestEventEmail_AllSupportedKindsHaveBuilder is the schema invariant: every
// kind in the SQL filter MUST have a builder, otherwise the forwarder
// hits the "no_builder_for_kind" log path and advances past real events.
func TestEventEmail_AllSupportedKindsHaveBuilder(t *testing.T) {
	for _, k := range supportedAuditKinds {
		if _, ok := eventEmailBuilders[k]; !ok {
			t.Errorf("supportedAuditKinds includes %q but eventEmailBuilders has no entry — adding the kind to the SQL filter without a builder makes the forwarder advance past real events", k)
		}
	}
	// And the other direction — no orphan builder for an unfilter'd kind.
	supportedSet := make(map[string]bool, len(supportedAuditKinds))
	for _, k := range supportedAuditKinds {
		supportedSet[k] = true
	}
	for k := range eventEmailBuilders {
		if !supportedSet[k] {
			t.Errorf("eventEmailBuilders has entry for %q but supportedAuditKinds doesn't include it — the SQL filter will never fetch this kind, making the builder dead code", k)
		}
	}
}

// TestEventEmail_BuilderReturnsParams pins that every per-kind builder
// produces a non-nil Params map when given a row with a valid email.
// The keys vary per kind — this is the minimum contract: "if you give me
// a valid row, I produce a map" — so a refactor that accidentally returns
// nil on the happy path fails this test.
func TestEventEmail_BuilderReturnsParams(t *testing.T) {
	for _, k := range supportedAuditKinds {
		b := eventEmailBuilders[k]
		if b == nil {
			t.Errorf("no builder for %q", k)
			continue
		}
		row := auditRow{
			ID:         "x",
			TeamID:     "team-1",
			Kind:       k,
			Summary:    "summary text",
			OwnerEmail: "e@example.com",
		}
		params, ok := b(row)
		if !ok {
			t.Errorf("builder for %q returned ok=false with valid email", k)
			continue
		}
		if params == nil {
			t.Errorf("builder for %q returned nil params on ok=true", k)
			continue
		}
		// Every builder MUST include team_id (the base param).
		if params["team_id"] != "team-1" {
			t.Errorf("builder for %q: team_id = %q; want team-1", k, params["team_id"])
		}
	}
}

// TestEventEmail_BuilderReturnsFalseOnNoEmail verifies every builder skips
// a row with no owner email — the forwarder relies on this to advance
// past orphan rows.
func TestEventEmail_BuilderReturnsFalseOnNoEmail(t *testing.T) {
	for kind, b := range eventEmailBuilders {
		_, ok := b(auditRow{Kind: kind, OwnerEmail: ""})
		if ok {
			t.Errorf("builder for %q returned ok=true with empty email — every builder must require an owner email", kind)
		}
	}
}

// TestEventEmail_BuilderPropagatesMetadata verifies that metadata fields from
// the audit row flow into params. Spot-checks the upgrade case because it
// has the most operationally-important fields (from_tier, to_tier, mrr).
// Values are stringified — providers receive flat string params.
func TestEventEmail_BuilderPropagatesMetadata(t *testing.T) {
	row := auditRow{
		ID:         "x",
		TeamID:     "t",
		Kind:       auditKindSubscriptionUpgraded,
		OwnerEmail: "u@example.com",
		Metadata:   []byte(`{"from_tier":"hobby","to_tier":"pro","mrr":49}`),
	}
	params, ok := buildTierUpgraded(row)
	if !ok {
		t.Fatal("builder returned ok=false unexpectedly")
	}
	if params["from_tier"] != "hobby" {
		t.Errorf("from_tier = %q; want hobby", params["from_tier"])
	}
	if params["to_tier"] != "pro" {
		t.Errorf("to_tier = %q; want pro", params["to_tier"])
	}
	// JSON numbers decode to float64 — fmt.Sprint gives "49"
	if params["mrr"] != "49" {
		t.Errorf("mrr = %q; want \"49\" (numbers stringified via fmt.Sprint)", params["mrr"])
	}
}

// TestEventEmail_DecodeMetaHandlesNil verifies the nil-safe contract — a
// row with no metadata JSONB MUST NOT panic the builder.
func TestEventEmail_DecodeMetaHandlesNil(t *testing.T) {
	m := decodeMeta(nil)
	if m == nil {
		t.Error("decodeMeta(nil) returned nil; want empty map so builders can index without nil-check")
	}
	m = decodeMeta([]byte(`{"not-json`)) // malformed
	if m == nil {
		t.Error("decodeMeta(malformed) returned nil; want empty map so builders can index without nil-check")
	}
}

// TestEventEmail_BuildResourceExpiringIncludesResourceType — spot-check on
// the kind that pulls a column (not just metadata) into params.
func TestEventEmail_BuildResourceExpiringIncludesResourceType(t *testing.T) {
	row := auditRow{
		ID:           "x",
		TeamID:       "t",
		Kind:         auditKindResourceExpiryImminent,
		ResourceType: "postgres",
		OwnerEmail:   "u@example.com",
		Metadata:     []byte(`{"hours_remaining":4}`),
	}
	params, ok := buildResourceExpiring(row)
	if !ok {
		t.Fatal("builder returned ok=false unexpectedly")
	}
	if params["resource_type"] != "postgres" {
		t.Errorf("resource_type = %q; want postgres (column flowed into params)", params["resource_type"])
	}
	if params["hours_remaining"] != "4" {
		t.Errorf("hours_remaining = %q; want \"4\"", params["hours_remaining"])
	}
}
