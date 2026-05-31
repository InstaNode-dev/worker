package jobs

// lifecycle_emails_test.go — the structural guarantee against a FOURTH
// regression of the "broken shared template" bug.
//
// Context: three times running, distinct audit kinds (near_quota_wall
// most painfully) were routed through the same Brevo dashboard template
// id 6 — a template hardcoding "Your resource expires in 6 hours". The
// comprehensive fix gives every email-sending kind its own Go-rendered
// body. These tests make it IMPOSSIBLE to add a 19th kind without a
// renderer, and assert no renderer ever emits the broken copy.

import (
	"strings"
	"testing"
)

// TestEveryEmailKindHasAGoRenderer is the registry-iterating coverage
// test mandated by CLAUDE.md rule 18. It iterates eventEmailBuilders —
// the canonical set of email-sending kinds — and asserts EVERY kind also
// has an entry in eventEmailBodyRenderers.
//
// WHY THIS TEST IS THE PERMANENT GUARD: the broken-email bug shipped
// three times because new kinds silently fell through to the
// dashboard-template path and reused a mislabeled template. After this
// fix every kind is Go-rendered. If a contributor adds a 19th kind to
// eventEmailBuilders without a renderer, THIS TEST FAILS — the kind
// never reaches the dashboard-template path in production because CI
// blocks the merge first. That makes the regression structurally
// impossible to repeat.
func TestEveryEmailKindHasAGoRenderer(t *testing.T) {
	for kind := range eventEmailBuilders {
		if _, ok := eventEmailBodyRenderers[kind]; !ok {
			t.Errorf("kind %q has an eventEmailBuilders entry but NO eventEmailBodyRenderers entry — "+
				"it would fall through to the legacy Brevo dashboard-template path, which is the exact "+
				"root cause of the three consecutive 'broken expiry email' regressions. Add a Go renderer "+
				"in lifecycle_emails.go and register it in eventEmailBodyRenderers.", kind)
		}
	}
	// Inverse direction: no orphan renderer for a kind that has no
	// builder (would be dead code — the forwarder only renders kinds it
	// also built params for).
	for kind := range eventEmailBodyRenderers {
		if _, ok := eventEmailBuilders[kind]; !ok {
			t.Errorf("kind %q has an eventEmailBodyRenderers entry but no eventEmailBuilders entry — "+
				"the forwarder never reaches the renderer without a builder; this renderer is dead code", kind)
		}
	}
}

// expiryKinds is the set of kinds for which expiry copy in the subject
// is GENUINELY correct (the resource/deployment really did, or is about
// to, expire). Every OTHER kind asserting expiry copy is the bug.
//
// deploy.expired is included: the deployment already expired, so "has
// expired" in the subject is accurate. The thing the regression guard
// must catch is a non-expiry kind (near_quota_wall, a deletion notice,
// the weekly digest) wearing expiry copy — never these four.
var expiryKinds = map[string]bool{
	auditKindAnonExpiryWarning:      true,
	auditKindResourceExpiryImminent: true,
	auditKindDeployExpiringSoon:     true,
	auditKindDeployExpired:          true,
}

// representativeParams returns a fully-populated params map for a kind —
// every key the kind's builder can emit, so the render test exercises
// the non-degenerate path.
func representativeParams(kind string) map[string]string {
	common := map[string]string{
		"team_id": "team-1",
		"summary": "test summary",
	}
	per := map[string]map[string]string{
		auditKindOnboardingClaimed:      {"signup_source": "claim_link"},
		auditKindSubscriptionUpgraded:   {"from_tier": "hobby", "to_tier": "pro", "mrr": "49"},
		auditKindSubscriptionDowngraded: {"from_tier": "pro", "to_tier": "hobby", "reason": "payment_failed"},
		auditKindSubscriptionCanceled:   {"last_tier": "pro", "canceled_at": "2026-05-15T00:00:00Z"},
		auditKindNearQuotaWall:          {"axis": "storage", "percent_used": "92", "tier": "hobby"},
		auditKindExperimentConversion:   {"experiment": "upgrade_button", "variant": "B", "action_taken": "checkout_started"},
		auditKindAdminTierChanged:       {"from_tier": "hobby", "to_tier": "team", "by_admin": "ops@instanode.dev"},
		auditKindAdminPromoIssued:       {"code": "LAUNCH20", "kind": "percent", "value": "20", "expires_at": "2026-06-15"},
		auditKindChurnRiskFlagged:       {"tier": "pro", "last_activity_days_ago": "21", "active_resource_count": "3"},
		auditKindDeployExpiringSoon:     {"deploy_name": "myapp", "deploy_url": "https://myapp.deployment.instanode.dev", "make_permanent_url": "https://api.instanode.dev/make-permanent", "hours_remaining": "4", "expires_at": "2026-05-16T00:00:00Z", "reminder_index": "2"},
		auditKindDeployExpired:          {"deploy_name": "myapp", "expires_at": "2026-05-15T00:00:00Z", "ttl_policy": "24h"},
		auditKindDeployMadePermanent:    {"source": "dashboard", "previous_ttl_policy": "24h"},
		auditKindDeployDeletionConfirmed: {"resource_id": "deploy-2", "pending_deletion_id": "pd-1", "freed_at": "2026-05-15T01:00:00Z", "age_seconds_in_pending": "120"},
		auditKindDeployDeletionCancelled: {"resource_id": "deploy-2", "pending_deletion_id": "pd-1"},
		auditKindDeployDeletionExpired:   {"resource_id": "deploy-2", "pending_deletion_id": "pd-1", "age_seconds": "3600"},
		auditKindDigestWeekly:            {"team_name": "Acme", "total_active_resources": "12", "resource_breakdown": "[]"},
		auditKindAnonExpiryWarning:       {"resource_type": "postgres", "hours_remaining": "12", "expires_at": "2026-05-16T00:00:00Z", "reminder_index": "1", "token_prefix": "ist_abc1", "upgrade_url": "https://instanode.dev/pricing", "resource_url": "https://instanode.dev/dashboard"},
		auditKindResourceExpiryImminent:  {"resource_type": "redis", "hours_remaining": "6", "expires_at": "2026-05-16T00:00:00Z", "reminder_index": "1", "token_prefix": "ist_xyz9", "upgrade_url": "https://instanode.dev/pricing", "resource_url": "https://instanode.dev/dashboard"},
		auditKindResourceQuotaSuspended:   {"resource_type": "postgres", "name": "prod-db", "resource_id": "res-1", "audit_kind": "resource.quota_suspended"},
		auditKindResourceQuotaUnsuspended: {"resource_type": "postgres", "name": "prod-db", "resource_id": "res-1", "audit_kind": "resource.quota_unsuspended"},
		// W2 kinds.
		auditKindPaymentGraceStarted:         {"grace_id": "g-1", "expires_at": "2026-05-22T00:00:00Z", "attempted_amount": "4900"},
		auditKindPaymentGraceReminderEmail:   {"grace_id": "g-1", "hours_remaining": "48", "grace_ends_at": "2026-05-22T00:00:00Z"},
		auditKindPaymentGraceRecovered:       {"grace_id": "g-1", "recovered_at": "2026-05-18T00:00:00Z"},
		auditKindPaymentGraceTerminatedEmail: {"grace_id": "g-1", "grace_ends_at": "2026-05-22T00:00:00Z"},
		auditKindSubscriptionCanceledByAdmin: {"cancel_attempted": "true", "cancel_succeeded": "true"},
		auditKindBackupFailedEmail:           {"resource_type": "postgres", "backup_id": "bk-1", "error_summary": "pg_dump exited 1"},
		auditKindRestoreSucceededEmail:       {"resource_type": "postgres", "restore_id": "rs-1", "backup_id": "bk-1"},
		auditKindRestoreFailedEmail:          {"resource_type": "postgres", "restore_id": "rs-1", "backup_id": "bk-1", "error_summary": "pg_restore exited 1"},
		auditKindDeployFailedEmail:           {"deploy_id": "dp-1", "failure_stage": "build", "error_summary": "kaniko build failed"},
	}
	for k, v := range per[kind] {
		common[k] = v
	}
	return common
}

// TestLifecycleEmail_EveryRendererProducesCorrectEmail runs every
// registered renderer with representative params and asserts:
//   (a) subject is non-empty,
//   (b) subject does NOT contain "expires in 6 hours" unless the kind is
//       genuinely an expiry kind — the exact broken-copy assertion,
//   (c) html body is non-empty,
//   (d) no "<no value>" template-miss artifacts in html or text,
//   (e) the html carries the instanode wordmark (shared shell applied).
func TestLifecycleEmail_EveryRendererProducesCorrectEmail(t *testing.T) {
	for kind, renderer := range eventEmailBodyRenderers {
		t.Run(kind, func(t *testing.T) {
			subject, html, text := renderer(representativeParams(kind))

			// (a) subject non-empty.
			if strings.TrimSpace(subject) == "" {
				t.Fatalf("kind %q: empty subject — Brevo raw send rejects empty subjects (sendRaw raw_missing_subject)", kind)
			}

			// (b) the broken-copy assertion. The literal that shipped
			// three times. Allowed ONLY for genuine expiry kinds.
			lowSubject := strings.ToLower(subject)
			if strings.Contains(lowSubject, "expires in 6 hours") && !expiryKinds[kind] {
				t.Errorf("kind %q: subject %q contains the hardcoded broken copy 'expires in 6 hours' "+
					"but this kind is NOT an expiry kind — this is the regression", kind, subject)
			}
			// Belt-and-braces: a non-expiry kind should not mention
			// expiry at all in its subject.
			if !expiryKinds[kind] && strings.Contains(lowSubject, "expire") {
				t.Errorf("kind %q: non-expiry kind has 'expire' in subject %q — verify this is intentional", kind, subject)
			}

			// (c) html non-empty.
			if strings.TrimSpace(html) == "" {
				t.Errorf("kind %q: empty html body", kind)
			}

			// (d) no template-miss artifacts.
			for _, body := range []struct{ name, content string }{{"html", html}, {"text", text}} {
				if strings.Contains(body.content, "<no value>") {
					t.Errorf("kind %q: %s body contains '<no value>' — a template references a field "+
						"the view struct doesn't provide", kind, body.name)
				}
			}

			// (e) shared shell applied — the wordmark proves the
			// instanode-branded chrome wrapped the body.
			if !strings.Contains(html, "instanode") {
				t.Errorf("kind %q: html body missing the instanode wordmark — shared shell not applied", kind)
			}

			// text alternative should also be non-empty.
			if strings.TrimSpace(text) == "" {
				t.Errorf("kind %q: empty plain-text body", kind)
			}
		})
	}
}

// TestLifecycleEmail_NearQuotaWallIsNotAnExpiryEmail is the targeted
// regression test for the exact kind that bit the user. near_quota_wall
// is a quota nudge — its subject and body must talk about plan limits,
// never about a resource expiring.
func TestLifecycleEmail_NearQuotaWallIsNotAnExpiryEmail(t *testing.T) {
	renderer, ok := eventEmailBodyRenderers[auditKindNearQuotaWall]
	if !ok {
		t.Fatal("near_quota_wall has no renderer — it would fall back to the broken Brevo template 6")
	}
	subject, html, text := renderer(representativeParams(auditKindNearQuotaWall))

	if strings.Contains(strings.ToLower(subject), "expire") {
		t.Errorf("near_quota_wall subject %q mentions expiry — it's a quota nudge, not an expiry email", subject)
	}
	if !strings.Contains(strings.ToLower(subject), "limit") {
		t.Errorf("near_quota_wall subject %q should mention the plan limit", subject)
	}
	// The body must surface quota context, not expiry context.
	if !strings.Contains(strings.ToLower(html), "limit") {
		t.Errorf("near_quota_wall html body should mention the plan limit")
	}
	if strings.Contains(text, "6 hours") {
		t.Errorf("near_quota_wall text body contains '6 hours' — the broken copy from template 6")
	}
}

// TestLifecycleEmail_SubjectsAreKindSpecific asserts no two distinct
// kinds produce the identical subject line — the symptom of multiple
// kinds sharing one mislabeled template.
func TestLifecycleEmail_SubjectsAreKindSpecific(t *testing.T) {
	seen := map[string]string{} // subject -> first kind that produced it
	for kind, renderer := range eventEmailBodyRenderers {
		subject, _, _ := renderer(representativeParams(kind))
		if prev, dup := seen[subject]; dup {
			// anon.expiry_warning and resource.expiry_imminent share
			// renderAnonExpiryEmail by design (identical payload) — the
			// representative params differ (postgres 12h vs redis 6h) so
			// their subjects differ anyway. A genuine collision between
			// two unrelated kinds is the bug.
			t.Errorf("kinds %q and %q produced the IDENTICAL subject %q — "+
				"distinct kinds must have distinct subjects (shared-template symptom)", prev, kind, subject)
		}
		seen[subject] = kind
	}
}

// TestDeployExpiringSoon_EscalatingCadence is the F3 regression test
// (BugBash 2026-05-19).
//
// BUG: deployment_reminder fired SIX identical "expires in Nh" emails per
// deploy over 12h — read as spam. The fix is a 3-stage escalating cadence
// (maxDeployReminders == 3) with a reminder_index-keyed subject prefix
// ("Heads up" / "Reminder" / "Final reminder"), matching
// anon.expiry_warning. This test pins both: the cap is 3, and the three
// stages produce three DISTINCT, escalating subject prefixes. It FAILS
// against the pre-fix renderer (which ignored reminder_index — all six
// subjects were identical).
func TestDeployExpiringSoon_EscalatingCadence(t *testing.T) {
	if maxDeployReminders != 3 {
		t.Fatalf("maxDeployReminders = %d; want 3 — F3 reduced the deploy reminder cadence from 6 to a 3-stage escalation", maxDeployReminders)
	}

	subjects := map[string]string{}
	for _, idx := range []string{"1", "2", "3"} {
		subject, _, _ := renderDeployExpiringSoon(map[string]string{
			"reminder_index":  idx,
			"hours_remaining": "6",
			"deploy_name":     "myapp",
		})
		subjects[idx] = subject
	}

	// All three must be distinct — escalation, not six identical emails.
	if subjects["1"] == subjects["2"] || subjects["2"] == subjects["3"] || subjects["1"] == subjects["3"] {
		t.Errorf("deploy-reminder subjects are not escalating (1=%q 2=%q 3=%q) — every stage must read differently (F3)",
			subjects["1"], subjects["2"], subjects["3"])
	}
	// The final stage must signal urgency.
	if !strings.HasPrefix(subjects["3"], "Final reminder") {
		t.Errorf("reminder_index=3 subject = %q; want a \"Final reminder\" prefix", subjects["3"])
	}
	if !strings.HasPrefix(subjects["1"], "Heads up") {
		t.Errorf("reminder_index=1 subject = %q; want a gentle \"Heads up\" prefix", subjects["1"])
	}
	// An absent reminder_index must default to the gentlest stage — never
	// a false "Final reminder".
	dflt, _, _ := renderDeployExpiringSoon(map[string]string{"hours_remaining": "6", "deploy_name": "myapp"})
	if !strings.HasPrefix(dflt, "Heads up") {
		t.Errorf("missing reminder_index subject = %q; want the gentle \"Heads up\" default", dflt)
	}
}

// TestRenderDeployMadePermanent_SourceIsFriendly is the 2026-05-31 regression
// guard for the raw-enum-leaking-into-email bug class. mastermanas805 received
// the literal string "(changed via make_permanent_endpoint)" in their email —
// the worker template was printing the api's machine-readable `source` enum
// straight into the HTML body. Every enum value emitted by the api MUST resolve
// to a friendly string here OR drop the clause entirely (empty string).
func TestRenderDeployMadePermanent_SourceIsFriendly(t *testing.T) {
	// The complete set of source values currently emitted by the api. If a new
	// emit site lands without an entry here, the test will fail when the team
	// pairs it with renderDeployMadePermanent.
	emittedSources := []string{
		"make_permanent_endpoint",      // api/internal/handlers/deploy_ttl.go
		"deploy_new",                   // api/internal/handlers/deploy.go (initial-permanent path)
		"team_setting",                 // hypothetical future setter
		"default_deployment_ttl_policy",
		"tier_upgrade",                 // pending api PR — Pro-upgrade auto-promote
	}
	for _, src := range emittedSources {
		_, html, _ := renderDeployMadePermanent(map[string]string{"source": src})
		if strings.Contains(html, src) {
			t.Errorf("rendered email leaked the raw enum %q into the body (must map to friendly text or drop):\n%s",
				src, html)
		}
		mapped := friendlyMakePermanentSource(src)
		if mapped == "" {
			t.Errorf("emit site %q has no friendly mapping — add one in friendlyMakePermanentSource", src)
		}
		if !strings.Contains(html, mapped) {
			t.Errorf("friendly text %q for source %q didn't appear in rendered body:\n%s", mapped, src, html)
		}
	}
}

// TestRenderExperimentConversion_ActionIsFriendly is the registry-iterating
// regression guard for the raw-enum-leaking-into-email bug class — the same
// class as the 2026-05-31 make_permanent_endpoint incident, repeated here in
// renderExperimentConversion. The dashboard fires
// POST /api/v1/experiments/converted with snake_case action identifiers
// ("checkout_started", "overview_upgrade_clicked") and the worker template
// inlined them verbatim — users would receive "We noticed you
// checkout_started — nice." in their inbox.
//
// This test enumerates EVERY action value the dashboard + instanode-web
// emit today. If a new emit site lands without an entry in
// friendlyExperimentAction, the test fails before the broken copy reaches
// a real inbox. Source-of-truth call sites:
//
//   dashboard/src/components/UpgradeButton.tsx        → checkout_started
//   dashboard/src/pages/OverviewPage.tsx              → overview_upgrade_clicked
//   instanode-web/src/components/UpgradeButton.tsx    → checkout_started
//   instanode-web/src/pages/OverviewPage.tsx          → overview_upgrade_clicked
//
// If a future emitter adds a new action value, add it to both
// friendlyExperimentAction (with a human phrase) and to
// emittedExperimentActions below. The test fails loudly otherwise.
func TestRenderExperimentConversion_ActionIsFriendly(t *testing.T) {
	emittedExperimentActions := []string{
		"checkout_started",         // UpgradeButton (dashboard + instanode-web)
		"overview_upgrade_clicked", // OverviewPage upsell (dashboard + instanode-web)
	}
	for _, action := range emittedExperimentActions {
		_, html, _ := renderExperimentConversion(map[string]string{
			"experiment":   "upgrade_button",
			"variant":      "B",
			"action_taken": action,
		})
		if strings.Contains(html, action) {
			t.Errorf("rendered email leaked the raw action enum %q into the body (must map to friendly text or drop):\n%s",
				action, html)
		}
		mapped := friendlyExperimentAction(action)
		if mapped == "" {
			t.Errorf("emit site %q has no friendly mapping — add one in friendlyExperimentAction", action)
		}
		if !strings.Contains(html, mapped) {
			t.Errorf("friendly text %q for action %q didn't appear in rendered body:\n%s", mapped, action, html)
		}
	}
}

// TestRenderDeployMadePermanent_UnknownSourceDropsClause asserts the safety
// net: a brand-new source value the worker doesn't know about (e.g., a future
// emit site) renders with the clause OMITTED entirely. Better to drop than
// leak the raw enum.
func TestRenderDeployMadePermanent_UnknownSourceDropsClause(t *testing.T) {
	_, html, _ := renderDeployMadePermanent(map[string]string{"source": "some_brand_new_unmapped_source_v99"})
	if strings.Contains(html, "some_brand_new_unmapped_source_v99") {
		t.Errorf("unknown source leaked into rendered body:\n%s", html)
	}
	if strings.Contains(html, "changed via") {
		t.Errorf("unknown source should drop the 'changed via X' clause entirely, got:\n%s", html)
	}
}

// TestRenderExperimentConversion_UnknownActionDropsClause asserts the safety
// net mirroring TestRenderDeployMadePermanent_UnknownSourceDropsClause: a
// brand-new action value the worker doesn't know about renders with the
// "We noticed you X — nice." clause OMITTED entirely. Better to drop the
// clause than leak the raw enum.
func TestRenderExperimentConversion_UnknownActionDropsClause(t *testing.T) {
	_, html, _ := renderExperimentConversion(map[string]string{
		"experiment":   "upgrade_button",
		"variant":      "B",
		"action_taken": "some_brand_new_unmapped_action_v99",
	})
	if strings.Contains(html, "some_brand_new_unmapped_action_v99") {
		t.Errorf("unknown action leaked into rendered body:\n%s", html)
	}
	if strings.Contains(html, "We noticed you") {
		t.Errorf("unknown action should drop the 'We noticed you X — nice.' clause entirely, got:\n%s", html)
	}
}

// TestRenderExperimentConversion_ExperimentNameIsFriendly is the sibling
// guard for the experiment-id field (line 155 of lifecycle_emails.go inlines
// {{ .Experiment }} into "Experiment: <name> (variant)"). The dashboard /
// instanode-web fires only one experiment today — "upgrade_button" — but the
// same enum-as-English risk applies.
func TestRenderExperimentConversion_ExperimentNameIsFriendly(t *testing.T) {
	emittedExperiments := []string{
		"upgrade_button", // dashboard + instanode-web UpgradeButton
	}
	for _, exp := range emittedExperiments {
		_, html, _ := renderExperimentConversion(map[string]string{
			"experiment":   exp,
			"variant":      "B",
			"action_taken": "checkout_started",
		})
		if strings.Contains(html, exp) {
			t.Errorf("rendered email leaked the raw experiment id %q into the body (must map to friendly text or drop):\n%s",
				exp, html)
		}
		mapped := friendlyExperimentName(exp)
		if mapped == "" {
			t.Errorf("emit site %q has no friendly mapping — add one in friendlyExperimentName", exp)
		}
		if !strings.Contains(html, mapped) {
			t.Errorf("friendly text %q for experiment %q didn't appear in rendered body:\n%s", mapped, exp, html)
		}
	}
}
