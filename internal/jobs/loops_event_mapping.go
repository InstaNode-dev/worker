package jobs

// loops_event_mapping.go — translation layer between instanode's audit_log
// row schema and the named events Loops.so triggers email campaigns from.
//
// The mapping is deliberately a flat table (one entry per supported
// audit_log.kind) so adding a new event is a single-line change. A row with
// a kind not in the table is unreachable: the forwarder's SQL filter already
// excludes those via `kind = ANY($supportedKinds)`.
//
// Shape contract per Loops event (set by Loops side, not us): a Loops "event"
// is { eventName, userId, eventProperties }. We always set userId = the team's
// primary email (resolved by the forwarder via users.team_id = audit.team_id);
// eventProperties is built per-kind from the audit_log row's metadata JSONB
// plus a few fixed columns (resource_type, etc.).
//
// "Skip" semantics: a kind listed here with builder == nil is documented as
// intentionally not forwarded (e.g. vault.promoted is internal). Such kinds
// must NOT appear in supportedAuditKinds — the forwarder will never see them.

import (
	"encoding/json"
	"time"
)

// ── Event name constants — the literal strings Loops triggers off ─────────

const (
	loopsEventTeamClaimed         = "team_claimed"
	loopsEventTierUpgraded        = "tier_upgraded"
	loopsEventNearQuotaWall       = "near_quota_wall"
	loopsEventResourceExpiring    = "resource_expiring_soon"
	loopsEventTierDowngraded      = "tier_downgraded"
	loopsEventSubscriptionCancel  = "subscription_canceled"
	loopsEventExperimentClicked   = "experiment_clicked"
	loopsEventTierChangedByAdmin  = "tier_changed_by_admin"
	loopsEventPromoCodeReceived   = "promo_code_received"
)

// ── audit_log.kind constants — must match the literal strings emitted by ──
// ── the API. A typo here means the SQL filter silently excludes the row. ──

const (
	auditKindOnboardingClaimed     = "onboarding.claimed"
	auditKindSubscriptionUpgraded  = "subscription.upgraded"
	auditKindNearQuotaWall         = "near_quota_wall"
	auditKindResourceExpiryImminent = "resource.expiry_imminent"
	auditKindSubscriptionDowngraded = "subscription.downgraded"
	auditKindSubscriptionCanceled  = "subscription.canceled"
	auditKindExperimentConversion  = "experiment.conversion"
	auditKindAdminTierChanged      = "admin.tier_changed"
	auditKindAdminPromoIssued      = "admin.promo_issued"
)

// auditRow is the projection of audit_log + users used by the forwarder.
// Only the columns we actually need to build a Loops payload.
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

// loopsEventBuilder converts an auditRow into a fully-populated Loops payload.
// Returns ok=false when the row is missing required fields (e.g. no owner
// email) — the forwarder logs and advances the cursor in that case.
type loopsEventBuilder func(row auditRow) (loopsEventPayload, bool)

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
}

// loopsEventBuilders maps an audit_log.kind to the builder that produces the
// Loops payload. Keep this in sync with supportedAuditKinds — the test
// TestLoops_AllSupportedKindsHaveBuilder enforces that they line up.
var loopsEventBuilders = map[string]loopsEventBuilder{
	auditKindOnboardingClaimed:      buildTeamClaimed,
	auditKindSubscriptionUpgraded:   buildTierUpgraded,
	auditKindNearQuotaWall:          buildNearQuotaWall,
	auditKindResourceExpiryImminent: buildResourceExpiring,
	auditKindSubscriptionDowngraded: buildTierDowngraded,
	auditKindSubscriptionCanceled:   buildSubscriptionCanceled,
	auditKindExperimentConversion:   buildExperimentClicked,
	auditKindAdminTierChanged:       buildTierChangedByAdmin,
	auditKindAdminPromoIssued:       buildPromoCodeReceived,
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

// baseProps populates the always-present properties: team_id and summary.
// Every event includes these so a Loops campaign template can render a
// fallback line if a kind-specific property is missing.
func baseProps(row auditRow) map[string]interface{} {
	return map[string]interface{}{
		"team_id": row.TeamID,
		"summary": row.Summary,
	}
}

// requireEmail returns ok=false when row.OwnerEmail is empty. Used by every
// builder — a Loops event with no userId is malformed.
func requireEmail(row auditRow) (string, bool) {
	if row.OwnerEmail == "" {
		return "", false
	}
	return row.OwnerEmail, true
}

// ── Per-kind builders ─────────────────────────────────────────────────────

func buildTeamClaimed(row auditRow) (loopsEventPayload, bool) {
	email, ok := requireEmail(row)
	if !ok {
		return loopsEventPayload{}, false
	}
	meta := decodeMeta(row.Metadata)
	props := baseProps(row)
	if v, ok := meta["signup_source"]; ok {
		props["signup_source"] = v
	}
	if v, ok := meta["fingerprint_ip"]; ok {
		props["fingerprint_ip"] = v
	}
	return loopsEventPayload{
		UserID:          email,
		Email:           email,
		EventName:       loopsEventTeamClaimed,
		EventProperties: props,
	}, true
}

func buildTierUpgraded(row auditRow) (loopsEventPayload, bool) {
	email, ok := requireEmail(row)
	if !ok {
		return loopsEventPayload{}, false
	}
	meta := decodeMeta(row.Metadata)
	props := baseProps(row)
	if v, ok := meta["from_tier"]; ok {
		props["from_tier"] = v
	}
	if v, ok := meta["to_tier"]; ok {
		props["to_tier"] = v
	}
	if v, ok := meta["mrr"]; ok {
		props["mrr"] = v
	}
	return loopsEventPayload{
		UserID:          email,
		Email:           email,
		EventName:       loopsEventTierUpgraded,
		EventProperties: props,
	}, true
}

func buildNearQuotaWall(row auditRow) (loopsEventPayload, bool) {
	email, ok := requireEmail(row)
	if !ok {
		return loopsEventPayload{}, false
	}
	meta := decodeMeta(row.Metadata)
	props := baseProps(row)
	if v, ok := meta["axis"]; ok {
		props["axis"] = v
	}
	if v, ok := meta["percent_used"]; ok {
		props["percent_used"] = v
	}
	if v, ok := meta["tier"]; ok {
		props["tier"] = v
	}
	return loopsEventPayload{
		UserID:          email,
		Email:           email,
		EventName:       loopsEventNearQuotaWall,
		EventProperties: props,
	}, true
}

func buildResourceExpiring(row auditRow) (loopsEventPayload, bool) {
	email, ok := requireEmail(row)
	if !ok {
		return loopsEventPayload{}, false
	}
	meta := decodeMeta(row.Metadata)
	props := baseProps(row)
	if row.ResourceType != "" {
		props["resource_type"] = row.ResourceType
	}
	if v, ok := meta["expires_at"]; ok {
		props["expires_at"] = v
	}
	if v, ok := meta["hours_remaining"]; ok {
		props["hours_remaining"] = v
	}
	return loopsEventPayload{
		UserID:          email,
		Email:           email,
		EventName:       loopsEventResourceExpiring,
		EventProperties: props,
	}, true
}

func buildTierDowngraded(row auditRow) (loopsEventPayload, bool) {
	email, ok := requireEmail(row)
	if !ok {
		return loopsEventPayload{}, false
	}
	meta := decodeMeta(row.Metadata)
	props := baseProps(row)
	if v, ok := meta["from_tier"]; ok {
		props["from_tier"] = v
	}
	if v, ok := meta["to_tier"]; ok {
		props["to_tier"] = v
	}
	if v, ok := meta["reason"]; ok {
		props["reason"] = v
	}
	return loopsEventPayload{
		UserID:          email,
		Email:           email,
		EventName:       loopsEventTierDowngraded,
		EventProperties: props,
	}, true
}

func buildSubscriptionCanceled(row auditRow) (loopsEventPayload, bool) {
	email, ok := requireEmail(row)
	if !ok {
		return loopsEventPayload{}, false
	}
	meta := decodeMeta(row.Metadata)
	props := baseProps(row)
	if v, ok := meta["last_tier"]; ok {
		props["last_tier"] = v
	}
	if v, ok := meta["canceled_at"]; ok {
		props["canceled_at"] = v
	}
	return loopsEventPayload{
		UserID:          email,
		Email:           email,
		EventName:       loopsEventSubscriptionCancel,
		EventProperties: props,
	}, true
}

func buildExperimentClicked(row auditRow) (loopsEventPayload, bool) {
	email, ok := requireEmail(row)
	if !ok {
		return loopsEventPayload{}, false
	}
	meta := decodeMeta(row.Metadata)
	props := baseProps(row)
	if v, ok := meta["experiment"]; ok {
		props["experiment"] = v
	}
	if v, ok := meta["variant"]; ok {
		props["variant"] = v
	}
	if v, ok := meta["action_taken"]; ok {
		props["action_taken"] = v
	}
	return loopsEventPayload{
		UserID:          email,
		Email:           email,
		EventName:       loopsEventExperimentClicked,
		EventProperties: props,
	}, true
}

func buildTierChangedByAdmin(row auditRow) (loopsEventPayload, bool) {
	email, ok := requireEmail(row)
	if !ok {
		return loopsEventPayload{}, false
	}
	meta := decodeMeta(row.Metadata)
	props := baseProps(row)
	if v, ok := meta["from_tier"]; ok {
		props["from_tier"] = v
	}
	if v, ok := meta["to_tier"]; ok {
		props["to_tier"] = v
	}
	if v, ok := meta["by_admin"]; ok {
		props["by_admin"] = v
	}
	return loopsEventPayload{
		UserID:          email,
		Email:           email,
		EventName:       loopsEventTierChangedByAdmin,
		EventProperties: props,
	}, true
}

func buildPromoCodeReceived(row auditRow) (loopsEventPayload, bool) {
	email, ok := requireEmail(row)
	if !ok {
		return loopsEventPayload{}, false
	}
	meta := decodeMeta(row.Metadata)
	props := baseProps(row)
	if v, ok := meta["code"]; ok {
		props["code"] = v
	}
	if v, ok := meta["kind"]; ok {
		props["kind"] = v
	}
	if v, ok := meta["value"]; ok {
		props["value"] = v
	}
	if v, ok := meta["expires_at"]; ok {
		props["expires_at"] = v
	}
	return loopsEventPayload{
		UserID:          email,
		Email:           email,
		EventName:       loopsEventPromoCodeReceived,
		EventProperties: props,
	}, true
}
