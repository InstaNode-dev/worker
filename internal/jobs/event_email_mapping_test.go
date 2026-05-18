package jobs

// event_email_mapping_test.go — exercises every per-kind builder and the
// invariants between supportedAuditKinds and eventEmailBuilders.

import (
	"testing"
)

// TestEventEmail_EverySupportedKindFullyWired is the W2 (P1-W2-01/02,
// P2-W2-13/14) registry-iterating coverage test mandated by CLAUDE.md
// rule 18. It iterates the LIVE supportedAuditKinds slice — the SQL filter
// the forwarder actually queries with — and asserts every kind has BOTH an
// eventEmailBuilders entry AND an eventEmailBodyRenderers entry.
//
// This is the structural guard against the modal failure of this codebase:
// an audit kind whose rows are emitted by a producer but never sent as an
// email because it was missing from one of the three registries. It caught
// payment.grace_*, subscription.canceled_by_admin, backup.failed,
// restore.{succeeded,failed}, and deploy.failed — all rows written, none
// emailed. A future kind half-registered (added to supportedAuditKinds and
// a builder but no renderer, or vice versa) fails CI here instead of
// silently dropping a customer email in prod.
func TestEventEmail_EverySupportedKindFullyWired(t *testing.T) {
	for _, kind := range supportedAuditKinds {
		if _, ok := eventEmailBuilders[kind]; !ok {
			t.Errorf("supportedAuditKinds includes %q but eventEmailBuilders has NO entry — the "+
				"forwarder fetches the row, hits no_builder_for_kind, and advances past it: the "+
				"customer email is silently dropped.", kind)
		}
		if _, ok := eventEmailBodyRenderers[kind]; !ok {
			t.Errorf("supportedAuditKinds includes %q but eventEmailBodyRenderers has NO entry — the "+
				"kind falls through to the legacy broken Brevo dashboard-template path (the root cause "+
				"of three consecutive expiry-email regressions).", kind)
		}
	}
}

// TestEventEmail_W2KindsRegistered pins the W2 fix: the nine audit kinds
// whose rows were being written by a producer but had NO email path. A
// paying customer whose card fails (payment.grace_*) previously got ZERO
// notification. Each MUST be in supportedAuditKinds, have a builder, and
// have a renderer.
//
// FAILS on master (kinds weren't registered). PASSES post-W2.
func TestEventEmail_W2KindsRegistered(t *testing.T) {
	w2Kinds := []string{
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
	supportedSet := map[string]bool{}
	for _, k := range supportedAuditKinds {
		supportedSet[k] = true
	}
	for _, k := range w2Kinds {
		if !supportedSet[k] {
			t.Errorf("W2: %q missing from supportedAuditKinds — the forwarder's SQL filter never fetches the row, so no email is sent", k)
		}
		if _, ok := eventEmailBuilders[k]; !ok {
			t.Errorf("W2: %q has no eventEmailBuilders entry — forwarder hits no_builder_for_kind and skips the row", k)
		}
		if _, ok := eventEmailBodyRenderers[k]; !ok {
			t.Errorf("W2: %q has no eventEmailBodyRenderers entry — falls through to the broken dashboard-template path", k)
		}
		// Exercise the builder end-to-end with a valid email.
		if b := eventEmailBuilders[k]; b != nil {
			params, ok := b(auditRow{ID: "a-1", TeamID: "t-1", Kind: k, OwnerEmail: "u@example.com"})
			if !ok {
				t.Errorf("W2: builder for %q returned ok=false with a valid owner email", k)
			} else if params["team_id"] != "t-1" {
				t.Errorf("W2: builder for %q did not set base team_id param", k)
			}
		}
	}
}

// TestEventEmail_W2KindsMatchProducerConstants pins the cross-file contract
// (CLAUDE rule 16): the email-pipeline aliases for the backup/restore/
// deploy/grace kinds MUST equal the producer-side constants byte-for-byte.
// If they drift, the SQL filter queries a literal no producer ever writes
// and the email is silently never sent.
func TestEventEmail_W2KindsMatchProducerConstants(t *testing.T) {
	cases := []struct{ alias, producer, name string }{
		{auditKindPaymentGraceReminderEmail, auditKindPaymentGraceReminder, "payment.grace_reminder"},
		{auditKindPaymentGraceTerminatedEmail, auditKindPaymentGraceTerminated, "payment.grace_terminated"},
		{auditKindBackupFailedEmail, auditKindBackupFailed, "backup.failed"},
		{auditKindRestoreSucceededEmail, auditKindRestoreSucceeded, "restore.succeeded"},
		{auditKindRestoreFailedEmail, auditKindRestoreFailed, "restore.failed"},
		{auditKindDeployFailedEmail, auditKindDeployFailed, "deploy.failed"},
	}
	for _, c := range cases {
		if c.alias != c.producer {
			t.Errorf("%s: email-pipeline alias %q != producer constant %q — the SQL filter would never match the producer's audit row", c.name, c.alias, c.producer)
		}
	}
	// The grace_started/recovered literals have no worker-side producer
	// constant (the api emits them); pin the exact strings the api uses.
	if auditKindPaymentGraceStarted != "payment.grace_started" {
		t.Errorf("auditKindPaymentGraceStarted = %q; want \"payment.grace_started\" (api models.AuditKindPaymentGraceStarted)", auditKindPaymentGraceStarted)
	}
	if auditKindPaymentGraceRecovered != "payment.grace_recovered" {
		t.Errorf("auditKindPaymentGraceRecovered = %q; want \"payment.grace_recovered\"", auditKindPaymentGraceRecovered)
	}
	if auditKindSubscriptionCanceledByAdmin != "subscription.canceled_by_admin" {
		t.Errorf("auditKindSubscriptionCanceledByAdmin = %q; want \"subscription.canceled_by_admin\"", auditKindSubscriptionCanceledByAdmin)
	}
}

// TestEventEmail_AnonRecipientFallback is the W3 (P1-W3-10) regression
// guard: anonymous teams have no users row, so a builder's only recipient
// source is the audit row's metadata.email. requireEmail / resolveRecipient
// must accept that fallback — otherwise the highest-volume free-funnel
// email (anon.expiry_warning) is structurally undeliverable.
func TestEventEmail_AnonRecipientFallback(t *testing.T) {
	// Row with NO OwnerEmail (the LEFT JOIN found no users row) but an
	// email in metadata — exactly the anonymous-tier shape.
	row := auditRow{
		ID:         "a-1",
		TeamID:     "t-1",
		Kind:       auditKindAnonExpiryWarning,
		OwnerEmail: "", // anonymous team — no users row
		Metadata:   []byte(`{"email":"anon@example.com","resource_type":"postgres","hours_remaining":3}`),
	}
	if got := resolveRecipient(row); got != "anon@example.com" {
		t.Errorf("resolveRecipient fell back wrong: got %q; want anon@example.com (metadata.email)", got)
	}
	if !requireEmail(row) {
		t.Error("requireEmail returned false for an anonymous row whose recipient is in metadata.email — the anon expiry email would be dropped")
	}
	params, ok := buildAnonExpiryWarning(row)
	if !ok {
		t.Fatal("buildAnonExpiryWarning returned ok=false for a row with a metadata.email recipient — W3 not fixed")
	}
	if params == nil {
		t.Fatal("buildAnonExpiryWarning returned nil params")
	}
	// A row with neither source still produces no recipient.
	if resolveRecipient(auditRow{ID: "a-2", Kind: auditKindAnonExpiryWarning}) != "" {
		t.Error("resolveRecipient must return \"\" when neither OwnerEmail nor metadata.email is present")
	}
}

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

// TestEventEmail_FixIJKindsRegistered pins the 2026-05-14 Resend→Brevo
// migration: every FIX-I/J deploy + deletion kind that's expected to fire
// via the BrevoForwarder MUST be in supportedAuditKinds, MUST have a
// builder, and MUST extract the kind-specific params the Brevo template
// body references.
//
// Note: deploy.deletion_requested is deliberately NOT in this list — the
// api sends that email synchronously (the user is waiting on the HTTP
// response). Registering it would duplicate the send.
//
// FAILS on master (kinds weren't in the slice). PASSES post-migration.
func TestEventEmail_FixIJKindsRegistered(t *testing.T) {
	expected := []string{
		auditKindDeployExpiringSoon,
		auditKindDeployExpired,
		auditKindDeployMadePermanent,
		auditKindDeployDeletionConfirmed,
		auditKindDeployDeletionCancelled,
		auditKindDeployDeletionExpired,
	}
	supportedSet := map[string]bool{}
	for _, k := range supportedAuditKinds {
		supportedSet[k] = true
	}
	for _, k := range expected {
		if !supportedSet[k] {
			t.Errorf("FIX-I/J migration: %q missing from supportedAuditKinds — BrevoForwarder will never pick up the audit row", k)
		}
		if _, ok := eventEmailBuilders[k]; !ok {
			t.Errorf("FIX-I/J migration: %q has no eventEmailBuilders entry — forwarder will hit no_builder_for_kind and skip the row", k)
		}
	}

	// And the inverse: _requested must NOT be wired up here — that would
	// duplicate the api's synchronous send.
	if _, ok := eventEmailBuilders[auditKindDeployDeletionRequested]; ok {
		t.Errorf("deploy.deletion_requested MUST NOT have an eventEmailBuilder — the api sends this email synchronously; registering it here would duplicate the send")
	}
	for _, k := range supportedAuditKinds {
		if k == auditKindDeployDeletionRequested {
			t.Errorf("deploy.deletion_requested MUST NOT be in supportedAuditKinds — the api sends this email synchronously; the audit row is observability only")
		}
	}
}

// TestEventEmail_BuildDeployExpiringSoonHasAllEmailTemplateFields verifies
// the audit metadata flows the fields the Brevo email template's
// substitution variables expect: deploy_url, make_permanent_url,
// hours_remaining, reminder_index. A missing deploy_url renders an empty
// link in the email body — broken UX even though the send technically
// succeeded.
//
// BugBash 2026-05-18 W3 T3: the builder deliberately does NOT map app_id
// into deploy_name — app_id is an opaque hex slug, not a human-readable
// name, and the deployments table has no name column. deploy_name must
// stay unset so the renderer falls back to "your deployment".
func TestEventEmail_BuildDeployExpiringSoonHasAllEmailTemplateFields(t *testing.T) {
	row := auditRow{
		ID:         "x",
		TeamID:     "t",
		Kind:       auditKindDeployExpiringSoon,
		OwnerEmail: "u@example.com",
		Metadata: []byte(`{
			"deploy_id":"deploy-1",
			"app_id":"6fffcc21",
			"deploy_url":"https://6fffcc21.deployment.instanode.dev",
			"make_permanent_url":"https://api.instanode.dev/api/v1/deployments/deploy-1/make-permanent",
			"hours_remaining":4,
			"expires_at":"2026-05-14T12:00:00Z",
			"reminder_index":3
		}`),
	}
	params, ok := buildDeployExpiringSoon(row)
	if !ok {
		t.Fatal("builder returned ok=false unexpectedly")
	}
	// deploy_name MUST NOT be populated from the app_id slug — see the
	// deploy_name NOTE in event_email_mapping.go. An "6fffcc21" deploy_name
	// would render "Your deployment 6fffcc21 expires..." to the customer.
	if _, present := params["deploy_name"]; present {
		t.Errorf("deploy_name = %q; want UNSET — the app_id slug is not a human-readable name, the renderer must fall back to \"your deployment\"", params["deploy_name"])
	}
	if params["deploy_url"] != "https://6fffcc21.deployment.instanode.dev" {
		t.Errorf("deploy_url = %q; want full https URL", params["deploy_url"])
	}
	if params["make_permanent_url"] != "https://api.instanode.dev/api/v1/deployments/deploy-1/make-permanent" {
		t.Errorf("make_permanent_url = %q; want make-permanent endpoint", params["make_permanent_url"])
	}
	if params["hours_remaining"] != "4" {
		t.Errorf("hours_remaining = %q; want \"4\"", params["hours_remaining"])
	}
	if params["reminder_index"] != "3" {
		t.Errorf("reminder_index = %q; want \"3\"", params["reminder_index"])
	}
}

// TestEventEmail_FollowUp5_NewKindsRegistered pins the FOLLOWUP-5
// (2026-05-14) finish-line of the Resend→Brevo migration: WeeklyDigest +
// anonymous-tier ExpiryReminder were the last two callers of the legacy
// EmailClient. Both now route via audit_log. Both kinds MUST be in
// supportedAuditKinds AND have a builder, otherwise the BrevoForwarder
// silently drops the row.
//
// FAILS on master (kinds aren't registered). PASSES post-FOLLOWUP-5.
func TestEventEmail_FollowUp5_NewKindsRegistered(t *testing.T) {
	cases := []struct {
		kind     string
		hint     string
		exemplar map[string]string
	}{
		{
			kind: auditKindDigestWeekly,
			hint: "WeeklyDigestWorker writes this every Mon 08:00 UTC — see email.go::emitWeeklyDigestAudit",
			exemplar: map[string]string{
				"team_name":              "Acme",
				"total_active_resources": "7",
			},
		},
		{
			kind: auditKindAnonExpiryWarning,
			hint: "ExpiryReminderWorker writes this hourly — see expiry_reminder.go::emitAnonExpiryWarningAudit",
			exemplar: map[string]string{
				"resource_id":     "r-1",
				"hours_remaining": "4",
				"expires_at":      "2026-05-14T12:00:00Z",
			},
		},
	}

	supportedSet := map[string]bool{}
	for _, k := range supportedAuditKinds {
		supportedSet[k] = true
	}

	for _, tc := range cases {
		if !supportedSet[tc.kind] {
			t.Errorf("FOLLOWUP-5 migration: %q missing from supportedAuditKinds — %s", tc.kind, tc.hint)
		}
		builder, ok := eventEmailBuilders[tc.kind]
		if !ok {
			t.Errorf("FOLLOWUP-5 migration: %q has no eventEmailBuilders entry — %s", tc.kind, tc.hint)
			continue
		}
		// Exercise the builder against a minimal valid auditRow.
		var metadata []byte = nil
		if len(tc.exemplar) > 0 {
			// Build a metadata JSON object the builder can decode.
			var buf []byte
			buf = append(buf, '{')
			first := true
			for k, v := range tc.exemplar {
				if !first {
					buf = append(buf, ',')
				}
				first = false
				buf = append(buf, '"')
				buf = append(buf, k...)
				buf = append(buf, '"', ':', '"')
				buf = append(buf, v...)
				buf = append(buf, '"')
			}
			buf = append(buf, '}')
			metadata = buf
		}
		row := auditRow{
			ID:         "audit-1",
			TeamID:     "team-1",
			Kind:       tc.kind,
			Summary:    "test summary",
			OwnerEmail: "u@example.com",
			Metadata:   metadata,
		}
		params, ok := builder(row)
		if !ok {
			t.Errorf("FOLLOWUP-5: builder for %q returned ok=false with a valid email", tc.kind)
			continue
		}
		if params["audit_kind"] != tc.kind {
			t.Errorf("FOLLOWUP-5: builder for %q did NOT set audit_kind param — Brevo template body branches on this", tc.kind)
		}
	}
}

// TestEventEmail_BuildDigestWeeklyFlowsEmailAndBreakdown — pins the
// digest.weekly builder reads email + breakdown out of the metadata so
// the Brevo template body can render the per-resource-type table.
func TestEventEmail_BuildDigestWeeklyFlowsEmailAndBreakdown(t *testing.T) {
	row := auditRow{
		ID:         "x",
		TeamID:     "t",
		Kind:       auditKindDigestWeekly,
		OwnerEmail: "u@example.com",
		Metadata: []byte(`{
			"email":"u@example.com",
			"team_name":"Acme",
			"total_active_resources":12,
			"resource_breakdown":"[{\"ResourceType\":\"postgres\",\"Count\":2}]"
		}`),
	}
	params, ok := buildDigestWeekly(row)
	if !ok {
		t.Fatal("builder returned ok=false unexpectedly")
	}
	if params["team_name"] != "Acme" {
		t.Errorf("team_name = %q; want Acme", params["team_name"])
	}
	if params["total_active_resources"] != "12" {
		t.Errorf("total_active_resources = %q; want 12 (number stringified via fmt.Sprint)", params["total_active_resources"])
	}
	if params["resource_breakdown"] == "" {
		t.Errorf("resource_breakdown should be the embedded JSON string; got empty")
	}
	if params["audit_kind"] != auditKindDigestWeekly {
		t.Errorf("audit_kind = %q; want %q (template branches on this)", params["audit_kind"], auditKindDigestWeekly)
	}
}

// TestEventEmail_BuildAnonExpiryWarningFlowsResourceContext — pins the
// anon.expiry_warning builder reads resource_type from the column AND
// flows hours_remaining + expires_at out of the metadata. The audit_kind
// param is what lets template 6 render different CTA copy from the
// resource.expiry_imminent (paid) variant.
func TestEventEmail_BuildAnonExpiryWarningFlowsResourceContext(t *testing.T) {
	row := auditRow{
		ID:           "x",
		TeamID:       "t",
		Kind:         auditKindAnonExpiryWarning,
		ResourceType: "postgres",
		OwnerEmail:   "u@example.com",
		Metadata: []byte(`{
			"resource_id":"res-1",
			"resource_type":"postgres",
			"hours_remaining":3,
			"expires_at":"2026-05-14T12:00:00Z"
		}`),
	}
	params, ok := buildAnonExpiryWarning(row)
	if !ok {
		t.Fatal("builder returned ok=false unexpectedly")
	}
	if params["resource_type"] != "postgres" {
		t.Errorf("resource_type = %q; want postgres", params["resource_type"])
	}
	if params["hours_remaining"] != "3" {
		t.Errorf("hours_remaining = %q; want 3", params["hours_remaining"])
	}
	if params["resource_id"] != "res-1" {
		t.Errorf("resource_id = %q; want res-1", params["resource_id"])
	}
	if params["audit_kind"] != auditKindAnonExpiryWarning {
		t.Errorf("audit_kind = %q; want %q (template branches on anon vs paid CTA)", params["audit_kind"], auditKindAnonExpiryWarning)
	}
}

// TestEventEmail_BuildDeployDeletionConfirmedFlowsContext — the
// "deletion confirmed" email body references resource_id, pending_deletion_id,
// and freed_at. Pin the builder reads them out of the metadata correctly.
func TestEventEmail_BuildDeployDeletionConfirmedFlowsContext(t *testing.T) {
	row := auditRow{
		ID:         "x",
		TeamID:     "t",
		Kind:       auditKindDeployDeletionConfirmed,
		OwnerEmail: "u@example.com",
		Metadata: []byte(`{
			"resource_id":"deploy-2",
			"pending_deletion_id":"pd-1",
			"freed_at":"2026-05-14T13:00:00Z",
			"age_seconds_in_pending":120
		}`),
	}
	params, ok := buildDeployDeletionConfirmed(row)
	if !ok {
		t.Fatal("builder returned ok=false unexpectedly")
	}
	if params["resource_id"] != "deploy-2" {
		t.Errorf("resource_id = %q; want deploy-2", params["resource_id"])
	}
	if params["pending_deletion_id"] != "pd-1" {
		t.Errorf("pending_deletion_id = %q; want pd-1", params["pending_deletion_id"])
	}
	if params["freed_at"] != "2026-05-14T13:00:00Z" {
		t.Errorf("freed_at = %q; want \"2026-05-14T13:00:00Z\"", params["freed_at"])
	}
}

// TestEventEmail_QuotaKindsMatchQuotaGoConstants pins the cross-module
// contract: the audit kinds registered in the email pipeline MUST equal
// quota.go's quotaSuspendedKind / quotaUnsuspendedKind byte-for-byte.
// quota.go writes the audit_log row with those constants; if the pipeline
// constants drift, the SQL filter silently never matches and no email is
// ever sent (the exact gap commit 49639e7 left open). Both live in the
// `jobs` package, so this test compares the actual values.
func TestEventEmail_QuotaKindsMatchQuotaGoConstants(t *testing.T) {
	if auditKindResourceQuotaSuspended != quotaSuspendedKind {
		t.Errorf("auditKindResourceQuotaSuspended = %q but quota.go's quotaSuspendedKind = %q — "+
			"the email pipeline must use the SAME literal quota.go writes to audit_log, else the "+
			"SQL filter never matches and no email is sent", auditKindResourceQuotaSuspended, quotaSuspendedKind)
	}
	if auditKindResourceQuotaUnsuspended != quotaUnsuspendedKind {
		t.Errorf("auditKindResourceQuotaUnsuspended = %q but quota.go's quotaUnsuspendedKind = %q — "+
			"the email pipeline must use the SAME literal quota.go writes to audit_log", auditKindResourceQuotaUnsuspended, quotaUnsuspendedKind)
	}
}

// TestEventEmail_QuotaKindsFullyWired is the regression guard for the
// 49639e7 follow-up: commit 49639e7 emitted the quota audit rows but never
// registered the kinds in the email pipeline, so no customer email was
// ever sent. Both quota kinds MUST be in supportedAuditKinds (the SQL
// filter) AND have an eventEmailBuilders entry AND an
// eventEmailBodyRenderers entry. A future edit that half-registers them
// (e.g. adds the builder but drops the renderer) fails here.
func TestEventEmail_QuotaKindsFullyWired(t *testing.T) {
	supportedSet := map[string]bool{}
	for _, k := range supportedAuditKinds {
		supportedSet[k] = true
	}
	for _, k := range []string{auditKindResourceQuotaSuspended, auditKindResourceQuotaUnsuspended} {
		if !supportedSet[k] {
			t.Errorf("49639e7 follow-up: %q missing from supportedAuditKinds — the forwarder's SQL "+
				"filter will never fetch the audit row, so the customer never gets an email", k)
		}
		if _, ok := eventEmailBuilders[k]; !ok {
			t.Errorf("49639e7 follow-up: %q has no eventEmailBuilders entry — the forwarder hits "+
				"no_builder_for_kind and skips the row", k)
		}
		if _, ok := eventEmailBodyRenderers[k]; !ok {
			t.Errorf("49639e7 follow-up: %q has no eventEmailBodyRenderers entry — it would fall "+
				"through to the legacy broken dashboard-template path", k)
		}
	}

	// Exercise both builders end-to-end: a valid row produces params that
	// carry the resource context the renderer's subject/body reads.
	for _, kind := range []string{auditKindResourceQuotaSuspended, auditKindResourceQuotaUnsuspended} {
		row := auditRow{
			ID:           "audit-1",
			TeamID:       "team-1",
			Kind:         kind,
			ResourceType: "postgres",
			Summary:      "test summary",
			OwnerEmail:   "u@example.com",
			Metadata:     []byte(`{"resource_id":"res-1","resource_type":"postgres","name":"prod-db"}`),
		}
		params, ok := eventEmailBuilders[kind](row)
		if !ok {
			t.Errorf("builder for %q returned ok=false with a valid email", kind)
			continue
		}
		if params["resource_type"] != "postgres" {
			t.Errorf("builder for %q: resource_type = %q; want postgres (column flows into params)", kind, params["resource_type"])
		}
		if params["name"] != "prod-db" {
			t.Errorf("builder for %q: name = %q; want prod-db (metadata flows into params)", kind, params["name"])
		}
		if params["resource_id"] != "res-1" {
			t.Errorf("builder for %q: resource_id = %q; want res-1", kind, params["resource_id"])
		}
	}
}
