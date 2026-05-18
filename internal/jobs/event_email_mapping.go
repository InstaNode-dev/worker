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

	// Deploy TTL audit kinds (Wave FIX-J). Migrated 2026-05-14 from the
	// legacy worker.email.* Resend path (which was silently NoopClient in
	// production because RESEND_API_KEY is unset) to the BrevoForwarder.
	// The DeploymentReminderWorker / DeploymentExpirerWorker now emit only
	// the audit_log row; the forwarder picks it up on the next 60s tick
	// and POSTs to Brevo using BREVO_TEMPLATE_IDS-configured templates.
	//
	// auditKindDeployTTLSet stays unmapped (no email — "user set a custom
	// TTL" is a dashboard event, not a notification).
	// auditKindTeamSettingsChanged stays unmapped (settings audit, not
	// email-worthy).
	auditKindDeployMadePermanent = "deploy.made_permanent"
	auditKindDeployTTLSet        = "deploy.ttl_set"
	auditKindDeployExpiringSoon  = "deploy.expiring_soon"
	auditKindDeployExpired       = "deploy.expired"
	auditKindTeamSettingsChanged = "team.settings_changed"

	// Weekly digest + anonymous-expiry warning (FOLLOWUP-5, 2026-05-14).
	// Producer:
	//   digest.weekly        — WeeklyDigestWorker (email.go), Mon 08:00 UTC
	//   anon.expiry_warning  — ExpiryReminderWorker (expiry_reminder.go), hourly
	//
	// Both previously routed through the legacy Resend EmailClient
	// (RESEND_API_KEY was unset → NoopClient → silent drop). Now the
	// audit_log row IS the trigger; the BrevoForwarder picks the row up on
	// its next 60s tick and POSTs to Brevo using BREVO_TEMPLATE_IDS[kind].
	//
	// Template reuse strategy (no new Brevo templates introduced):
	//   digest.weekly       → template 2 (WELCOME). Both are "we're keeping
	//                         in touch" warm emails; the substitution map
	//                         lets the same template render either copy.
	//   anon.expiry_warning → template 6 (RESOURCE_EXPIRING). Same template
	//                         used by resource.expiry_imminent. The
	//                         agent_action copy ("claim to keep" vs
	//                         "upgrade to keep") is driven by the
	//                         {{ params.audit_kind }} flag the template
	//                         body branches on; if the operator later
	//                         clones a dedicated anon variant, they only
	//                         flip BREVO_TEMPLATE_IDS["anon.expiry_warning"]
	//                         to the new id — no worker change needed.
	auditKindDigestWeekly      = "digest.weekly"
	auditKindAnonExpiryWarning = "anon.expiry_warning"

	// Email-confirmed deletion lifecycle (Wave FIX-I, api migration 044).
	// Producer: api/internal/handlers/deletion_confirm.go on every step of
	// the two-step paid-tier delete flow (deploy + stack), PLUS the
	// worker's pending_deletion_expirer for the TTL-expired branch.
	//
	// Email semantics (template choices in BREVO_TEMPLATE_IDS):
	//   requested  — "click to confirm deletion" CTA (template 6).
	//   confirmed  — "resource is being torn down" notice (template 6).
	//   cancelled  — "good news, you cancelled the deletion" (template 3
	//                — positive confirmation, closest match in current set).
	//   expired    — "window elapsed, your resource is safe" (template 3
	//                — also positive: resource stays, no destructive action).
	//
	// Only deploy.* registered for now (per brief scope: "DeployEmails to
	// Brevo"). stack.deletion_* mirrors the same metadata shape and can be
	// wired the same way if/when the stack flow needs email coverage.
	auditKindDeployDeletionRequested = "deploy.deletion_requested"
	auditKindDeployDeletionConfirmed = "deploy.deletion_confirmed"
	auditKindDeployDeletionCancelled = "deploy.deletion_cancelled"
	auditKindDeployDeletionExpired   = "deploy.deletion_expired"

	// Storage-quota suspend/unsuspend lifecycle (follow-up to commit 49639e7).
	// Producer: EnforceStorageQuotaWorker (quota.go) — emitQuotaAuditRow writes
	// these rows after a resource's status is flipped to 'suspended' (over its
	// storage limit) or back to 'active' (usage receded below the hysteresis
	// threshold).
	//
	// 49639e7 added the audit_log rows but never registered the kinds in this
	// pipeline, so the rows were emitted and the dashboard showed them but NO
	// customer email was ever sent — the whole point of the change (a customer
	// whose database gets suspended hears about it). These two constants close
	// that gap; they MUST equal quota.go's quotaSuspendedKind /
	// quotaUnsuspendedKind byte-for-byte (asserted by
	// TestEventEmail_QuotaKindsMatchQuotaGoConstants). The audit metadata JSON
	// carries resource_id / resource_type / name — same shape the
	// resource.expiry_imminent builder reads.
	auditKindResourceQuotaSuspended   = quotaSuspendedKind
	auditKindResourceQuotaUnsuspended = quotaUnsuspendedKind

	// ── W2 (P1-W2-01/02, P2-W2-13/14) — kinds whose audit rows were being
	// ── written but had NO email path (missing from supportedAuditKinds +
	// ── eventEmailBuilders + eventEmailBodyRenderers). The rows were emitted,
	// ── the dashboard showed them, but no customer email was ever sent.
	//
	// Payment dunning lifecycle. A customer whose card fails currently gets
	// ZERO notification before their subscription terminates. The four kinds
	// below are the dunning emails.
	//
	//   started    — billing.go::emitPaymentGraceStartedAudit. Metadata:
	//                subscription_id, grace_id, started_at, expires_at,
	//                attempted_amount (paise, may be null).
	//   reminder   — payment_grace_reminder.go. Metadata: grace_id,
	//                hours_remaining, grace_ends_at.
	//   recovered  — billing.go::emitPaymentGraceRecoveredAudit. Metadata:
	//                subscription_id, grace_id, started_at, recovered_at.
	//   terminated — payment_grace_terminator.go. Metadata: grace_id,
	//                grace_ends_at.
	//
	// grace_started / grace_recovered have no pre-existing worker constant
	// (their producer is the api side); reminder + terminated DO — reuse
	// those byte-for-byte so the SQL filter matches the producer's literal
	// (CLAUDE rule 16: single source of the string).
	auditKindPaymentGraceStarted   = "payment.grace_started"
	auditKindPaymentGraceRecovered = "payment.grace_recovered"
	// auditKindPaymentGraceReminderEmail / auditKindPaymentGraceTerminatedEmail
	// alias the existing producer-side constants so the email pipeline reads
	// from the SAME literal the worker writes to audit_log.
	auditKindPaymentGraceReminderEmail   = auditKindPaymentGraceReminder
	auditKindPaymentGraceTerminatedEmail = auditKindPaymentGraceTerminated

	// Admin-initiated cancellation. The api emits this distinct kind (instead
	// of the customer-initiated subscription.canceled) specifically so the
	// email reads "canceled by support" — admin_customers.go on the demote
	// path. Metadata: cancel_attempted, cancel_succeeded, cancel_error.
	auditKindSubscriptionCanceledByAdmin = "subscription.canceled_by_admin"

	// Backup / restore lifecycle. The customer-backup pipeline emits these on
	// terminal failure / success. Reuse the producer-side constants from
	// backup_audit.go byte-for-byte.
	//
	//   backup.failed     — customer_backup_runner.go::markFailed. Metadata:
	//                       backup_id, error_summary, duration_seconds, tier.
	//   restore.succeeded — customer_restore_runner.go. Metadata: restore_id,
	//                       backup_id, duration_seconds.
	//   restore.failed    — customer_restore_runner.go::markRestoreFailed.
	//                       Metadata: restore_id, backup_id, error_summary,
	//                       duration_seconds.
	auditKindBackupFailedEmail     = auditKindBackupFailed
	auditKindRestoreSucceededEmail = auditKindRestoreSucceeded
	auditKindRestoreFailedEmail    = auditKindRestoreFailed

	// Deploy failure. The api emits deploy.failed on a terminal build/rollout
	// failure. Reuse the producer-side constant from deploy_notify_webhook.go.
	// Metadata: deploy_id, team_id, failure_stage (build|rollout),
	// error_summary.
	auditKindDeployFailedEmail = auditKindDeployFailed
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
	// Wave FIX-J deploy TTL emails (migrated from Resend → Brevo 2026-05-14):
	auditKindDeployExpiringSoon,
	auditKindDeployExpired,
	auditKindDeployMadePermanent,
	// Wave FIX-I email-confirmed deletion lifecycle (migrated from inline
	// api Resend send → audit-driven Brevo 2026-05-14).
	//
	// NOTE: deploy.deletion_requested is INTENTIONALLY omitted. The api
	// (deletion_confirm.go:200) sends the "click to confirm" email
	// synchronously via its own Brevo-routed Client because the user is
	// waiting on the HTTP response (we need to roll back the pending row
	// if the send fails). Registering _requested here would dispatch a
	// duplicate. The audit row IS written by the api as observability,
	// just not as an email trigger.
	auditKindDeployDeletionConfirmed,
	auditKindDeployDeletionCancelled,
	auditKindDeployDeletionExpired,
	// FOLLOWUP-5 migration (2026-05-14) — replaces the legacy Resend
	// EmailClient.SendWeeklyDigest / SendExpiryReminder paths.
	auditKindDigestWeekly,
	auditKindAnonExpiryWarning,
	// Storage-quota suspend/unsuspend (follow-up to 49639e7) — without these
	// in the SQL filter the forwarder never fetches the rows quota.go emits.
	auditKindResourceQuotaSuspended,
	auditKindResourceQuotaUnsuspended,
	// W2 (P1-W2-01/02, P2-W2-13/14) — payment dunning, admin-cancel,
	// backup/restore, deploy-failure. Same omission class as 49639e7: the
	// rows are written but were absent from the SQL filter, so no email.
	auditKindPaymentGraceStarted,
	auditKindPaymentGraceReminderEmail,
	auditKindPaymentGraceRecovered,
	auditKindPaymentGraceTerminatedEmail,
	auditKindSubscriptionCanceledByAdmin,
	auditKindBackupFailedEmail,
	auditKindRestoreSucceededEmail,
	auditKindRestoreFailedEmail,
	auditKindDeployFailedEmail,
}

// auditKindDeployTTLSet and auditKindTeamSettingsChanged are emitted by the
// api but INTENTIONALLY NOT mapped to an email — the inflection point is a
// dashboard event, not a customer notification. Listing them as `_ = ...`
// so they stay referenced (and don't trip `unused constant` linters), and
// so a future contributor sees the rationale next to the active mappings.
var _ = []string{
	auditKindDeployTTLSet,
	auditKindTeamSettingsChanged,
}

// eventEmailBodyRenderer turns a per-row params map into the (subject,
// HTML body, plain-text body) triple that the BrevoProvider sends
// verbatim via the raw-HTML path. Kinds registered in
// eventEmailBodyRenderers below take this path instead of the
// dashboard-template lookup.
//
// Why bother: a dashboard-controlled template body lets a Brevo operator
// drift out of sync with the params the worker emits — the production
// bug that triggered this refactor was a template that hardcoded "6
// hours" in the subject and referenced fields the worker didn't send,
// rendering empty Type / Token / Expires cells in the body. By rendering
// in Go, both the params and the body ship together in one worker
// image and one deploy.
type eventEmailBodyRenderer func(params map[string]string) (subject, html, text string)

// eventEmailBodyRenderers maps an audit_log.kind to a Go renderer.
//
// AS OF 2026-05-15 (THIRD REGRESSION FIX): EVERY email-sending audit
// kind is registered here. NO kind falls through to the legacy
// dashboard-template path anymore. The Brevo template path in
// brevo_provider.go is left intact as dead fallback code only — the
// registry-iterating test TestEveryEmailKindHasAGoRenderer asserts that
// every kind in eventEmailBuilders has an entry here, so a 19th kind
// added without a renderer fails CI rather than shipping a broken email.
//
// Why this map had to grow from 2 → 18: the dashboard-template path was
// the root cause of three consecutive production regressions. Multiple
// distinct kinds (near_quota_wall, the deploy/deletion lifecycle kinds,
// digest.weekly, ...) were all wired — via BREVO_TEMPLATE_IDS — to the
// SAME Brevo template id 6, whose body hardcodes "Your resource expires
// in 6 hours" with empty Type/Token/Expires placeholders. A
// near_quota_wall email therefore arrived as a broken expiry email
// (production log: kind=near_quota_wall path=template template_id=6).
// The first two fixes patched only anon.expiry_warning and
// resource.expiry_imminent. This entry covers ALL the rest.
//
//   anon.expiry_warning / resource.expiry_imminent — renderAnonExpiryEmail
//     (expiry_reminder_email.go). Identical payload shape.
//   every other kind — a dedicated renderer in lifecycle_emails.go.
var eventEmailBodyRenderers = map[string]eventEmailBodyRenderer{
	// Expiry kinds — shared renderer (identical payload shape).
	auditKindAnonExpiryWarning:      renderAnonExpiryEmail,
	auditKindResourceExpiryImminent: renderAnonExpiryEmail,
	// Onboarding + subscription lifecycle.
	auditKindOnboardingClaimed:      renderOnboardingClaimed,
	auditKindSubscriptionUpgraded:   renderTierUpgraded,
	auditKindSubscriptionDowngraded: renderTierDowngraded,
	auditKindSubscriptionCanceled:   renderSubscriptionCanceled,
	// Quota nudge — the kind that triggered the third regression.
	auditKindNearQuotaWall: renderNearQuotaWall,
	// Experiment / admin / churn.
	auditKindExperimentConversion: renderExperimentConversion,
	auditKindAdminTierChanged:     renderAdminTierChanged,
	auditKindAdminPromoIssued:     renderPromoCodeReceived,
	auditKindChurnRiskFlagged:     renderChurnRiskFlagged,
	// Deploy TTL lifecycle.
	auditKindDeployExpiringSoon:  renderDeployExpiringSoon,
	auditKindDeployExpired:       renderDeployExpired,
	auditKindDeployMadePermanent: renderDeployMadePermanent,
	// Deploy deletion lifecycle.
	auditKindDeployDeletionConfirmed: renderDeployDeletionConfirmed,
	auditKindDeployDeletionCancelled: renderDeployDeletionCancelled,
	auditKindDeployDeletionExpired:   renderDeployDeletionExpired,
	// Weekly digest.
	auditKindDigestWeekly: renderDigestWeekly,
	// Storage-quota suspend/unsuspend (follow-up to 49639e7).
	auditKindResourceQuotaSuspended:   renderResourceQuotaSuspended,
	auditKindResourceQuotaUnsuspended: renderResourceQuotaUnsuspended,
	// W2 (P1-W2-01/02, P2-W2-13/14) — dunning, admin-cancel, backup/restore,
	// deploy-failure. Go-rendered like every other kind (no shared Brevo
	// dashboard template — see lifecycle_emails.go header).
	auditKindPaymentGraceStarted:         renderPaymentGraceStarted,
	auditKindPaymentGraceReminderEmail:   renderPaymentGraceReminder,
	auditKindPaymentGraceRecovered:       renderPaymentGraceRecovered,
	auditKindPaymentGraceTerminatedEmail: renderPaymentGraceTerminated,
	auditKindSubscriptionCanceledByAdmin: renderSubscriptionCanceledByAdmin,
	auditKindBackupFailedEmail:           renderBackupFailed,
	auditKindRestoreSucceededEmail:       renderRestoreSucceeded,
	auditKindRestoreFailedEmail:          renderRestoreFailed,
	auditKindDeployFailedEmail:           renderDeployFailed,
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
	// Wave FIX-J deploy TTL emails (migrated 2026-05-14).
	auditKindDeployExpiringSoon:  buildDeployExpiringSoon,
	auditKindDeployExpired:       buildDeployExpired,
	auditKindDeployMadePermanent: buildDeployMadePermanent,
	// Wave FIX-I email-confirmed deletion (migrated 2026-05-14).
	// _requested intentionally absent — sent synchronously by the api;
	// see supportedAuditKinds comment.
	auditKindDeployDeletionConfirmed: buildDeployDeletionConfirmed,
	auditKindDeployDeletionCancelled: buildDeployDeletionCancelled,
	auditKindDeployDeletionExpired:   buildDeployDeletionExpired,
	// FOLLOWUP-5 migration (2026-05-14).
	auditKindDigestWeekly:      buildDigestWeekly,
	auditKindAnonExpiryWarning: buildAnonExpiryWarning,
	// Storage-quota suspend/unsuspend (follow-up to 49639e7).
	auditKindResourceQuotaSuspended:   buildResourceQuotaSuspended,
	auditKindResourceQuotaUnsuspended: buildResourceQuotaUnsuspended,
	// W2 (P1-W2-01/02, P2-W2-13/14) — dunning, admin-cancel, backup/restore,
	// deploy-failure.
	auditKindPaymentGraceStarted:         buildPaymentGraceStarted,
	auditKindPaymentGraceReminderEmail:   buildPaymentGraceReminder,
	auditKindPaymentGraceRecovered:       buildPaymentGraceRecovered,
	auditKindPaymentGraceTerminatedEmail: buildPaymentGraceTerminated,
	auditKindSubscriptionCanceledByAdmin: buildSubscriptionCanceledByAdmin,
	auditKindBackupFailedEmail:           buildBackupFailed,
	auditKindRestoreSucceededEmail:       buildRestoreSucceeded,
	auditKindRestoreFailedEmail:          buildRestoreFailed,
	auditKindDeployFailedEmail:           buildDeployFailed,
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

// resolveRecipient returns the recipient email for an audit row, falling
// back to the metadata `email` field when the joined users row produced
// nothing. W3 (P1-W3-10): anonymous teams have no users row, so
// auditRow.OwnerEmail is empty for them; the producers stash the address
// in metadata.email. fetchBatch already COALESCEs this at the SQL layer —
// this builder-side fallback is belt-and-braces for any row that reaches a
// builder without the SQL COALESCE (e.g. a hand-constructed row in a test,
// or a future caller). Returns "" when neither source has an address.
func resolveRecipient(row auditRow) string {
	if row.OwnerEmail != "" {
		return row.OwnerEmail
	}
	meta := decodeMeta(row.Metadata)
	if v, ok := meta["email"]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// requireEmail returns ok=false when the row has no resolvable recipient
// (neither a joined users-row email nor a metadata.email fallback). Used by
// every builder — an event email with no recipient is malformed (the
// forwarder logs and advances past it).
func requireEmail(row auditRow) bool {
	return resolveRecipient(row) != ""
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
	params["audit_kind"] = row.Kind
	if row.ResourceType != "" {
		params["resource_type"] = row.ResourceType
	}
	copyMetaStr(params, meta, "expires_at", "expires_at")
	copyMetaStr(params, meta, "hours_remaining", "hours_remaining")
	// 2026-05-15 — the paid expiry path now shares the Go renderer
	// (renderAnonExpiryEmail) with the anon path. There is no multi-stage
	// reminder cadence for paid/authenticated resources (single-fire), but
	// the renderer reads reminder_index to decide between "Heads up" /
	// "Reminder" / "Final reminder" subject prefixes — pin to "1" so paid
	// emails read as "Heads up — your instanode <type> expires in Nh".
	// Also surface resource_id / token_prefix / upgrade_url / resource_url
	// so the body's Type/Token/Expires panel and CTA links render correctly
	// (the previous Brevo dashboard template referenced these but the
	// worker wasn't sending them — rendering empty cells in production).
	params["reminder_index"] = "1"
	copyMetaStr(params, meta, "resource_id", "resource_id")
	copyMetaStr(params, meta, "token_prefix", "token_prefix")
	copyMetaStr(params, meta, "upgrade_url", "upgrade_url")
	copyMetaStr(params, meta, "resource_url", "resource_url")
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

// ── Wave FIX-J deploy TTL builders ────────────────────────────────────────
//
// Metadata shapes (set by deployment_reminder.go / deployment_expirer.go /
// api/internal/handlers/deploy_ttl.go):
//
//   deploy.expiring_soon  — {deploy_id, team_id, reminder_index, hours_remaining, expires_at, app_id, deploy_url, make_permanent_url}
//   deploy.expired        — {deploy_id, team_id, expires_at, ttl_policy, app_id}
//   deploy.made_permanent — {deploy_id, team_id, source, previous_ttl_policy}
//
// The Brevo template body references {{ params.deploy_name }},
// {{ params.deploy_url }}, {{ params.make_permanent_url }} so the same
// RESOURCE_EXPIRING template can render copy that's specific to a deploy
// vs a postgres/redis/mongo resource.

func buildDeployExpiringSoon(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "deploy_id", "deploy_id")
	copyMetaStr(params, meta, "app_id", "deploy_name")
	copyMetaStr(params, meta, "deploy_url", "deploy_url")
	copyMetaStr(params, meta, "make_permanent_url", "make_permanent_url")
	copyMetaStr(params, meta, "hours_remaining", "hours_remaining")
	copyMetaStr(params, meta, "expires_at", "expires_at")
	copyMetaStr(params, meta, "reminder_index", "reminder_index")
	return params, true
}

func buildDeployExpired(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "deploy_id", "deploy_id")
	copyMetaStr(params, meta, "app_id", "deploy_name")
	copyMetaStr(params, meta, "expires_at", "expires_at")
	copyMetaStr(params, meta, "ttl_policy", "ttl_policy")
	return params, true
}

func buildDeployMadePermanent(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "deploy_id", "deploy_id")
	copyMetaStr(params, meta, "source", "source")
	copyMetaStr(params, meta, "previous_ttl_policy", "previous_ttl_policy")
	return params, true
}

// ── Wave FIX-I email-confirmed deletion builders ──────────────────────────
//
// Metadata shapes (set by api/internal/handlers/deletion_confirm.go and
// worker/internal/jobs/pending_deletion_expirer.go):
//
//   deploy.deletion_confirmed — {team_id, resource_id, pending_deletion_id, freed_at, age_seconds_in_pending}
//   deploy.deletion_cancelled — {team_id, resource_id, pending_deletion_id}
//   deploy.deletion_expired   — {team_id, resource_id, pending_deletion_id, age_seconds}
//
// deploy.deletion_requested is sent SYNCHRONOUSLY by the api (the user is
// waiting on the HTTP response) so it does not have an event-driven
// builder — see supportedAuditKinds for the duplicate-suppression note.

func buildDeployDeletionConfirmed(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "resource_id", "resource_id")
	copyMetaStr(params, meta, "pending_deletion_id", "pending_deletion_id")
	copyMetaStr(params, meta, "freed_at", "freed_at")
	copyMetaStr(params, meta, "age_seconds_in_pending", "age_seconds_in_pending")
	return params, true
}

func buildDeployDeletionCancelled(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "resource_id", "resource_id")
	copyMetaStr(params, meta, "pending_deletion_id", "pending_deletion_id")
	return params, true
}

func buildDeployDeletionExpired(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "resource_id", "resource_id")
	copyMetaStr(params, meta, "pending_deletion_id", "pending_deletion_id")
	copyMetaStr(params, meta, "age_seconds", "age_seconds")
	return params, true
}

// ── FOLLOWUP-5 builders (2026-05-14) ──────────────────────────────────────
//
// Metadata shapes (set by email.go::emitWeeklyDigestAudit and
// expiry_reminder.go::emitAnonExpiryWarningAudit):
//
//   digest.weekly        — {email, team_name, total_active_resources,
//                           resource_breakdown}
//   anon.expiry_warning  — {resource_id, resource_type, hours_remaining,
//                           expires_at, email}
//
// audit_kind is injected into params so a Brevo template body that
// shares an underlying template id between multiple kinds (template 6
// covers both resource.expiry_imminent AND anon.expiry_warning) can
// branch on {{ params.audit_kind }} to render the right CTA copy.

func buildDigestWeekly(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	params["audit_kind"] = row.Kind
	copyMetaStr(params, meta, "team_name", "team_name")
	copyMetaStr(params, meta, "total_active_resources", "total_active_resources")
	copyMetaStr(params, meta, "resource_breakdown", "resource_breakdown")
	return params, true
}

func buildAnonExpiryWarning(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	params["audit_kind"] = row.Kind
	// resource_type lives in the column (and the column flows through the
	// audit insert), so prefer the column over the metadata field — same
	// pattern as buildResourceExpiring.
	if row.ResourceType != "" {
		params["resource_type"] = row.ResourceType
	} else {
		copyMetaStr(params, meta, "resource_type", "resource_type")
	}
	copyMetaStr(params, meta, "resource_id", "resource_id")
	copyMetaStr(params, meta, "hours_remaining", "hours_remaining")
	copyMetaStr(params, meta, "expires_at", "expires_at")
	// 2026-05-15 multi-stage rework: the worker now writes reminder_index
	// (1/2/3), stage_label, token_prefix, upgrade_url, and resource_url.
	// Template body should read {{ params.hours_remaining }} (NOT hardcode
	// a number), branch on reminder_index for the subject line ("First /
	// Second / Final reminder"), and surface upgrade_url + resource_url as
	// CTAs. token_prefix identifies the resource without leaking the secret.
	copyMetaStr(params, meta, "reminder_index", "reminder_index")
	copyMetaStr(params, meta, "stage_label", "stage_label")
	copyMetaStr(params, meta, "token_prefix", "token_prefix")
	copyMetaStr(params, meta, "upgrade_url", "upgrade_url")
	copyMetaStr(params, meta, "resource_url", "resource_url")
	return params, true
}

// ── Storage-quota suspend/unsuspend builders (follow-up to 49639e7) ───────
//
// Metadata shape (set by quota.go::emitQuotaAuditRow):
//
//   resource.quota_suspended   — {resource_id, resource_type, name}
//   resource.quota_unsuspended — {resource_id, resource_type, name}
//
// resource_type also flows through the audit_log.resource_type COLUMN, so —
// like buildResourceExpiring / buildAnonExpiryWarning — prefer the column
// and fall back to the metadata field. The renderer's subject + body read
// resource_type and name; resource_id is carried for forensic linkage.

func buildResourceQuotaSuspended(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	params["audit_kind"] = row.Kind
	if row.ResourceType != "" {
		params["resource_type"] = row.ResourceType
	} else {
		copyMetaStr(params, meta, "resource_type", "resource_type")
	}
	copyMetaStr(params, meta, "resource_id", "resource_id")
	copyMetaStr(params, meta, "name", "name")
	return params, true
}

func buildResourceQuotaUnsuspended(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	params["audit_kind"] = row.Kind
	if row.ResourceType != "" {
		params["resource_type"] = row.ResourceType
	} else {
		copyMetaStr(params, meta, "resource_type", "resource_type")
	}
	copyMetaStr(params, meta, "resource_id", "resource_id")
	copyMetaStr(params, meta, "name", "name")
	return params, true
}

// ── W2 builders — payment dunning, admin-cancel, backup/restore, deploy ───
//
// Each follows the established shape: bail on no recipient, decode
// metadata, start from baseParams, copy the per-kind keys the renderer
// reads. See the W2 constant block for each kind's metadata shape.

func buildPaymentGraceStarted(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "grace_id", "grace_id")
	copyMetaStr(params, meta, "expires_at", "expires_at")
	copyMetaStr(params, meta, "attempted_amount", "attempted_amount")
	return params, true
}

func buildPaymentGraceReminder(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "grace_id", "grace_id")
	copyMetaStr(params, meta, "hours_remaining", "hours_remaining")
	copyMetaStr(params, meta, "grace_ends_at", "grace_ends_at")
	return params, true
}

func buildPaymentGraceRecovered(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "grace_id", "grace_id")
	copyMetaStr(params, meta, "recovered_at", "recovered_at")
	return params, true
}

func buildPaymentGraceTerminated(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "grace_id", "grace_id")
	copyMetaStr(params, meta, "grace_ends_at", "grace_ends_at")
	return params, true
}

func buildSubscriptionCanceledByAdmin(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "cancel_attempted", "cancel_attempted")
	copyMetaStr(params, meta, "cancel_succeeded", "cancel_succeeded")
	return params, true
}

func buildBackupFailed(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	if row.ResourceType != "" {
		params["resource_type"] = row.ResourceType
	} else {
		copyMetaStr(params, meta, "resource_type", "resource_type")
	}
	copyMetaStr(params, meta, "backup_id", "backup_id")
	copyMetaStr(params, meta, "error_summary", "error_summary")
	return params, true
}

func buildRestoreSucceeded(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	if row.ResourceType != "" {
		params["resource_type"] = row.ResourceType
	} else {
		copyMetaStr(params, meta, "resource_type", "resource_type")
	}
	copyMetaStr(params, meta, "restore_id", "restore_id")
	copyMetaStr(params, meta, "backup_id", "backup_id")
	return params, true
}

func buildRestoreFailed(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	if row.ResourceType != "" {
		params["resource_type"] = row.ResourceType
	} else {
		copyMetaStr(params, meta, "resource_type", "resource_type")
	}
	copyMetaStr(params, meta, "restore_id", "restore_id")
	copyMetaStr(params, meta, "backup_id", "backup_id")
	copyMetaStr(params, meta, "error_summary", "error_summary")
	return params, true
}

func buildDeployFailed(row auditRow) (map[string]string, bool) {
	if !requireEmail(row) {
		return nil, false
	}
	meta := decodeMeta(row.Metadata)
	params := baseParams(row)
	copyMetaStr(params, meta, "deploy_id", "deploy_id")
	copyMetaStr(params, meta, "failure_stage", "failure_stage")
	copyMetaStr(params, meta, "error_summary", "error_summary")
	return params, true
}
