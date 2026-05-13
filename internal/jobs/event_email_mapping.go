package jobs

// event_email_mapping.go — canonical, provider-agnostic catalogue of the
// audit_log.kind values this worker forwards to email. The mapping is
// deliberately small: which kinds are supported, and how to turn each
// row's flat columns + JSONB metadata into the EventEmail.Params map.
//
// What this file DELIBERATELY does NOT contain:
//   - Any provider identifier — those live in internal/email/ only
//   - Provider-specific payload shapes (no provider-named JSON tags)
//   - Provider auth (no API keys, no headers)
//
// The forwarder uses these builders to construct EventEmail, then hands
// EventEmail to the configured email.EmailProvider. Adding a new
// audit_log.kind = one builder + one entry in supportedAuditKinds and
// eventEmailBuilders. Adding a new provider = zero changes here.

import (
	"encoding/json"
	"fmt"
	"time"
)

// ── audit_log.kind constants — must match the literal strings emitted by ──
// ── the API. A typo here means the SQL filter silently excludes the row. ──
//
// These are package-private named constants per project convention
// (no inline string literals scattered across handlers).
const (
	auditKindOnboardingClaimed      = "onboarding.claimed"
	auditKindSubscriptionUpgraded   = "subscription.upgraded"
	auditKindNearQuotaWall          = "near_quota_wall"
	auditKindResourceExpiryImminent = "resource.expiry_imminent"
	auditKindSubscriptionDowngraded = "subscription.downgraded"
	auditKindSubscriptionCanceled   = "subscription.canceled"
	auditKindExperimentConversion   = "experiment.conversion"
	auditKindAdminTierChanged       = "admin.tier_changed"
	auditKindAdminPromoIssued       = "admin.promo_issued"
	// auditKindChurnRiskFlagged is written by the daily ChurnPredictorWorker
	// (churn_predictor.go). The Brevo template configured under this key
	// (operator wires it via BREVO_TEMPLATE_IDS) is the "we miss you"
	// reactivation email. Metadata carries tier, last_activity_days_ago,
	// active_resource_count, email — see buildChurnRiskFlagged below.
	auditKindChurnRiskFlagged       = "churn.risk_flagged"
)

// auditRow is the projection of audit_log + users used by the forwarder.
// Only the columns we actually need to build an EventEmail.
type auditRow struct {
	ID           string
	TeamID       string
	Kind         string
	ResourceType string
	Summary      string
	Metadata     []byte // raw JSONB bytes — may be nil
	CreatedAt    time.Time
	OwnerEmail   string // resolved via LEFT JOIN users(team_id) — may be ""
}

// eventEmailBuilder converts an auditRow into Params for an EventEmail.
// Returns ok=false when the row is missing required fields (e.g. no owner
// email) — the forwarder logs and advances the cursor in that case.
//
// The builder returns only Params (the per-event substitution map) —
// the forwarder fills Kind, Recipient, RecipientName, IdempotencyKey
// from the audit row itself. This keeps each builder small and prevents
// drift between builders on the boilerplate fields.
type eventEmailBuilder func(row auditRow) (params map[string]string, ok bool)

// supportedAuditKinds is the SQL filter for the forwarder query — only these
// kinds get pulled into a batch. Exported as a slice (not a map) so the
// query can pass it via `kind = ANY($1::text[])` with a pq.Array.
var supportedAuditKinds = []string{
	auditKindOnboardingClaimed,
	auditKindSubscriptionUpgraded,
	auditKindNearQuotaWall,
	auditKindResourceExpiryImminent,
	auditKindSubscriptionDowngraded,
	auditKindSubscriptionCanceled,
	auditKindExperimentConversion,
	auditKindAdminTierChanged,
	auditKindAdminPromoIssued,
	auditKindChurnRiskFlagged,
}

// eventEmailBuilders maps an audit_log.kind to the builder that produces
// the Params for an EventEmail. Keep this in sync with supportedAuditKinds
// — the test TestEventEmail_AllSupportedKindsHaveBuilder enforces that.
var eventEmailBuilders = map[string]eventEmailBuilder{
	auditKindOnboardingClaimed:      buildTeamClaimed,
	auditKindSubscriptionUpgraded:   buildTierUpgraded,
	auditKindNearQuotaWall:          buildNearQuotaWall,
	auditKindResourceExpiryImminent: buildResourceExpiring,
	auditKindSubscriptionDowngraded: buildTierDowngraded,
	auditKindSubscriptionCanceled:   buildSubscriptionCanceled,
	auditKindExperimentConversion:   buildExperimentClicked,
	auditKindAdminTierChanged:       buildTierChangedByAdmin,
	auditKindAdminPromoIssued:       buildPromoCodeReceived,
	auditKindChurnRiskFlagged:       buildChurnRiskFlagged,
}

// ── Builder helpers ───────────────────────────────────────────────────────

// decodeMeta deserializes the raw JSONB into a generic map. Returns an empty
// map on nil / invalid metadata so callers don't have to nil-check every
// lookup. We deliberately swallow the unmarshal error — the only sensible
// fallback for a malformed metadata payload is "send the event with what we
// know" rather than block the entire pipeline.
func decodeMeta(raw []byte) map[string]interface{} {
	if len(raw) == 0 {
		return map[string]interface{}{}
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return map[string]interface{}{}
	}
	return m
}

// baseParams populates the always-present params: team_id and summary.
// Every event includes these so a provider template can render a
// fallback line if a kind-specific param is missing.
func baseParams(row auditRow) map[string]string {
	return map[string]string{
		"team_id": row.TeamID,
		"summary": row.Summary,
	}
}

// copyMetaStr copies meta[key] into params[outKey] as a string, if present.
// Numbers are stringified via fmt.Sprint so providers get a stable scalar
// representation regardless of JSON decode quirks (numbers come through
// as float64). Skips entries that aren't present in the metadata.
func copyMetaStr(params map[string]string, meta map[string]interface{}, key, outKey string) {
	v, ok := meta[key]
	if !ok {
		return
	}
	params[outKey] = fmt.Sprint(v)
}

// requireEmail returns ok=false when row.OwnerEmail is empty.
// Used by every builder — an event email with no recipient is malformed
// (the forwarder logs and advances past it).
func requireEmail(row auditRow) bool {
	return row.OwnerEmail != ""
}

// ── Per-kind builders ─────────────────────────────────────────────────────
//
// Each builder is small and shaped the same way:
//   1. Bail with ok=false when the row can't possibly produce a sendable
//      email (no owner email).
//   2. Decode metadata into a map.
//   3. Start from baseParams + copy the per-kind keys we care about.
//
// Adding a new kind = copy one of these.

func buildTeamClaimed(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "signup_source", "signup_source")
	copyMetaStr(params, meta, "fingerprint_ip", "fingerprint_ip")
	return params, true
}

func buildTierUpgraded(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "from_tier", "from_tier")
	copyMetaStr(params, meta, "to_tier", "to_tier")
	copyMetaStr(params, meta, "mrr", "mrr")
	return params, true
}

func buildNearQuotaWall(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "axis", "axis")
	copyMetaStr(params, meta, "percent_used", "percent_used")
	copyMetaStr(params, meta, "tier", "tier")
	return params, true
}

func buildResourceExpiring(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	if row.ResourceType != "" {
		params["resource_type"] = row.ResourceType
	}
	copyMetaStr(params, meta, "expires_at", "expires_at")
	copyMetaStr(params, meta, "hours_remaining", "hours_remaining")
	return params, true
}

func buildTierDowngraded(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "from_tier", "from_tier")
	copyMetaStr(params, meta, "to_tier", "to_tier")
	copyMetaStr(params, meta, "reason", "reason")
	return params, true
}

func buildSubscriptionCanceled(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "last_tier", "last_tier")
	copyMetaStr(params, meta, "canceled_at", "canceled_at")
	return params, true
}

func buildExperimentClicked(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "experiment", "experiment")
	copyMetaStr(params, meta, "variant", "variant")
	copyMetaStr(params, meta, "action_taken", "action_taken")
	return params, true
}

func buildTierChangedByAdmin(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "from_tier", "from_tier")
	copyMetaStr(params, meta, "to_tier", "to_tier")
	copyMetaStr(params, meta, "by_admin", "by_admin")
	return params, true
}

func buildPromoCodeReceived(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "code", "code")
	copyMetaStr(params, meta, "kind", "kind")
	copyMetaStr(params, meta, "value", "value")
	copyMetaStr(params, meta, "expires_at", "expires_at")
	return params, true
}

// buildChurnRiskFlagged is the per-kind builder for "we miss you"
// reactivation emails (audit_log.kind = "churn.risk_flagged"). The
// daily ChurnPredictorWorker writes these rows; this builder reads
// them back into the Params map that the configured email provider
// (Brevo today) uses to fill the template.
//
// Params shape:
//   tier                    — team's current plan tier (hobby/pro/growth)
//   last_activity_days_ago  — float; 0 means "no recorded activity ever"
//   active_resource_count   — int; how many resources still standing
//
// All numbers stringify via fmt.Sprint (copyMetaStr); the JSON decode
// surfaces them as float64 so "7" arrives as "7" not "7.000000".
func buildChurnRiskFlagged(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "tier", "tier")
	copyMetaStr(params, meta, "last_activity_days_ago", "last_activity_days_ago")
	copyMetaStr(params, meta, "active_resource_count", "active_resource_count")
	return params, true
}
