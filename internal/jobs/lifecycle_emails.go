package jobs

// lifecycle_emails.go — Go-rendered HTML/text bodies for every
// non-expiry lifecycle email kind the worker forwards.
//
// WHY THIS FILE EXISTS — the third regression of the same bug.
//
// The BrevoProvider has two send paths (brevo_provider.go):
//
//   1. sendRaw     — POSTs Subject + HTMLBody + TextBody verbatim. Taken
//                    when EventEmail.HTMLBody is non-empty.
//   2. template    — POSTs templateId + params, body lives in the Brevo
//                    dashboard. Taken when HTMLBody is empty.
//
// The dashboard-template path is a footgun: multiple distinct audit
// kinds were wired (via BREVO_TEMPLATE_IDS) to the SAME Brevo template
// id 6 — a template whose body hardcodes "Your resource expires in 6
// hours" and references Type/Token/Expires placeholders. A
// `near_quota_wall` (a quota nudge) routed through template 6 therefore
// arrived in the user's inbox as a broken expiry email. Production log:
//
//   email.brevo.event_sent kind=near_quota_wall path=template template_id=6 status=201
//
// Two prior fixes patched only anon.expiry_warning and
// resource.expiry_imminent — 2 of ~16 affected kinds. This file is the
// comprehensive fix: a Go renderer for EVERY remaining email-sending
// kind so NONE depend on a shared dashboard template. After this, the
// template path is dead code for these kinds — the BREVO_TEMPLATE_IDS
// env becomes vestigial (left intact only as a fallback for any future
// kind that hasn't yet got a renderer; the registry-iterating test
// TestEveryEmailKindHasAGoRenderer guarantees that never happens for a
// registered kind).
//
// Each renderer has signature:
//
//   func renderXxx(params map[string]string) (subject, html, text string)
//
// matching eventEmailBodyRenderer. The params map keys are exactly what
// the corresponding builder in event_email_mapping.go emits — each
// renderer below documents the builder it pairs with.
//
// All templates are parsed at init via template.Must so a malformed
// template fails the worker at boot (and in CI), never at runtime.

import (
	"bytes"
	"html/template"
	"strings"
	textTemplate "text/template"
)

// ── Shared instanode-branded HTML shell ───────────────────────────────────
//
// Every lifecycle email shares one look: a wordmark header, a white card
// body, an optional CTA button, and a muted footer. Factoring the shell
// into one helper means a brand tweak is a one-line change, not 16.
//
// emailShellView is the data the shell template renders around the
// per-kind body. Body is pre-rendered, trusted HTML (it comes from a
// sibling html/template in this file, which auto-escapes the
// user-controlled values) so it is typed template.HTML — NOT a raw
// string — and is emitted without re-escaping.
type emailShellView struct {
	Title    string        // <title> + visually-hidden preheader
	Heading  string        // the <h1> at the top of the card
	Body     template.HTML // pre-rendered, already-escaped inner HTML
	CTALabel string        // optional button label; button omitted when ""
	CTAURL   string        // optional button href
}

// emailShellTmpl wraps a per-kind body in the shared instanode chrome.
// Inline CSS only — email clients strip <style> blocks unpredictably.
var emailShellTmpl = template.Must(template.New("email_shell").Parse(`<!DOCTYPE html>
<html>
  <head><meta charset="utf-8"><title>{{ .Title }}</title></head>
  <body style="margin:0;padding:0;background:#f2f2f4;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;color:#111;line-height:1.5;">
    <div style="max-width:560px;margin:0 auto;padding:24px;">
      <div style="font-size:18px;font-weight:700;letter-spacing:-0.5px;padding:8px 0 16px;color:#111;">
        instanode<span style="color:#0a7;">.dev</span>
      </div>
      <div style="background:#fff;border-radius:10px;padding:28px 28px 24px;">
        <h1 style="margin:0 0 16px;font-size:20px;font-weight:600;">{{ .Heading }}</h1>
        {{ .Body }}
        {{ if .CTALabel }}
        <p style="margin:24px 0 4px;">
          <a href="{{ .CTAURL }}"
             style="background:#000;color:#fff;text-decoration:none;padding:12px 20px;border-radius:6px;display:inline-block;font-weight:600;font-size:14px;">
            {{ .CTALabel }}
          </a>
        </p>
        {{ end }}
      </div>
      <p style="font-size:12px;color:#999;margin:20px 4px 0;line-height:1.6;">
        You're receiving this because you have an instanode account.
        Manage your resources at
        <a href="https://instanode.dev/dashboard" style="color:#999;">instanode.dev/dashboard</a>.
      </p>
    </div>
  </body>
</html>
`))

// renderShell executes emailShellTmpl. The body argument is already-safe
// HTML produced by a sibling html/template (which escaped its inputs);
// it is wrapped in template.HTML so the shell emits it verbatim.
func renderShell(v emailShellView) string {
	var buf bytes.Buffer
	if err := emailShellTmpl.Execute(&buf, v); err != nil {
		// emailShellTmpl is validated at init by template.Must — Execute
		// can only fail on a view-shape bug. Degrade to the bare body so
		// the recipient still gets a usable email.
		return string(v.Body)
	}
	return buf.String()
}

// ── Per-kind body templates ───────────────────────────────────────────────
//
// Each body template is the INNER HTML of the card — the shell adds the
// header, heading, CTA button and footer. Bodies use html/template so
// every interpolated value (tier names, codes, deploy names) is
// auto-escaped against markup injection.
//
// The plain-text alternative for each kind is built by lifecycleText
// (defined below) so the 16 kinds share one text layout too.

var (
	bodyOnboardingClaimed = template.Must(template.New("b_claim").Parse(
		`<p style="margin:0 0 14px;">Your instanode account is live. Provision a Postgres, Redis, MongoDB, queue, object store, webhook receiver, or a full deployment — each is a single HTTP call, no Docker, no setup.</p>
<p style="margin:0 0 4px;color:#555;font-size:14px;">Signed up via: <strong>{{ if .SignupSource }}{{ .SignupSource }}{{ else }}direct{{ end }}</strong></p>`))

	bodyTierUpgraded = template.Must(template.New("b_up").Parse(
		`<p style="margin:0 0 14px;">Your plan is now <strong>{{ .ToTier }}</strong>{{ if .FromTier }} (upgraded from {{ .FromTier }}){{ end }}. Higher storage limits and connection counts apply to all your resources, effective immediately.</p>
<p style="margin:0 0 4px;color:#555;font-size:14px;">Thanks for backing instanode — you're now on the {{ .ToTier }} tier.</p>`))

	bodyNearQuotaWall = template.Must(template.New("b_quota").Parse(
		`<p style="margin:0 0 14px;">Heads up — one of your resources is approaching its {{ .Tier }}-tier limit.</p>
<table cellpadding="6" cellspacing="0" style="background:#f7f7f8;border-radius:6px;font-size:14px;margin:4px 0 14px;width:100%;">
  <tr><td style="color:#666;width:140px;">Limit reached</td><td><strong>{{ if .Axis }}{{ .Axis }}{{ else }}usage{{ end }}</strong></td></tr>
  <tr><td style="color:#666;">Currently at</td><td><strong>{{ if .PercentUsed }}{{ .PercentUsed }}%{{ else }}near capacity{{ end }}</strong></td></tr>
  <tr><td style="color:#666;">Plan tier</td><td>{{ .Tier }}</td></tr>
</table>
<p style="margin:0 0 4px;">Upgrade to raise the ceiling before writes start getting rejected.</p>`))

	bodyTierDowngraded = template.Must(template.New("b_down").Parse(
		`<p style="margin:0 0 14px;">Your plan has changed to <strong>{{ .ToTier }}</strong>{{ if .FromTier }} (from {{ .FromTier }}){{ end }}.{{ if .Reason }} Reason: {{ .Reason }}.{{ end }}</p>
<p style="margin:0 0 4px;color:#555;font-size:14px;">Resources you provisioned before this change keep their previous limits. New resources will use the {{ .ToTier }}-tier limits.</p>`))

	bodySubscriptionCanceled = template.Must(template.New("b_cancel").Parse(
		`<p style="margin:0 0 14px;">Your instanode subscription has been cancelled{{ if .CanceledAt }} as of {{ .CanceledAt }}{{ end }}.{{ if .LastTier }} Your account was on the {{ .LastTier }} tier.{{ end }}</p>
<p style="margin:0 0 4px;color:#555;font-size:14px;">Your data isn't deleted immediately — you can resubscribe any time to restore full access to your resources.</p>`))

	bodyExperimentConversion = template.Must(template.New("b_exp").Parse(
		`<p style="margin:0 0 14px;">Thanks for trying the new instanode experience.{{ if .ActionTaken }} We noticed you {{ .ActionTaken }} — nice.{{ end }}</p>
<p style="margin:0 0 4px;color:#555;font-size:14px;">{{ if .Experiment }}Experiment: {{ .Experiment }}{{ if .Variant }} ({{ .Variant }}){{ end }}{{ end }}</p>`))

	bodyAdminTierChanged = template.Must(template.New("b_admin").Parse(
		`<p style="margin:0 0 14px;">Your account tier was changed to <strong>{{ .ToTier }}</strong>{{ if .FromTier }} (from {{ .FromTier }}){{ end }} by the instanode team{{ if .ByAdmin }} ({{ .ByAdmin }}){{ end }}.</p>
<p style="margin:0 0 4px;color:#555;font-size:14px;">If you didn't expect this change, reply to this email and we'll look into it.</p>`))

	bodyPromoCodeReceived = template.Must(template.New("b_promo").Parse(
		`<p style="margin:0 0 14px;">A promo code has been added to your instanode account.</p>
<table cellpadding="6" cellspacing="0" style="background:#f7f7f8;border-radius:6px;font-size:14px;margin:4px 0 14px;width:100%;">
  <tr><td style="color:#666;width:120px;">Code</td><td><code>{{ .Code }}</code></td></tr>
  {{ if .Value }}<tr><td style="color:#666;">Value</td><td><strong>{{ .Value }}</strong></td></tr>{{ end }}
  {{ if .ExpiresAt }}<tr><td style="color:#666;">Use before</td><td>{{ .ExpiresAt }}</td></tr>{{ end }}
</table>
<p style="margin:0 0 4px;">It applies automatically on your next billing cycle.</p>`))

	bodyChurnRiskFlagged = template.Must(template.New("b_churn").Parse(
		`<p style="margin:0 0 14px;">We noticed you haven't used instanode in a while{{ if .LastActivityDaysAgo }} (about {{ .LastActivityDaysAgo }} days){{ end }}{{ if .ActiveResourceCount }}, and you still have {{ .ActiveResourceCount }} active resource(s) running{{ end }}.</p>
<p style="margin:0 0 4px;">Whatever you were building, we'd love to help you ship it. Jump back into your dashboard — your resources are right where you left them.</p>`))

	bodyDeployExpiringSoon = template.Must(template.New("b_dexp").Parse(
		`<p style="margin:0 0 14px;">Your deployment <strong>{{ if .DeployName }}{{ .DeployName }}{{ else }}app{{ end }}</strong> will expire in about {{ if .HoursRemaining }}{{ .HoursRemaining }}{{ else }}a few{{ end }} hour(s){{ if .ExpiresAt }}, at {{ .ExpiresAt }} UTC{{ end }}.</p>
<p style="margin:0 0 4px;">Free-tier deployments are temporary. Make it permanent to keep it running.</p>`))

	bodyDeployExpired = template.Must(template.New("b_dexpd").Parse(
		`<p style="margin:0 0 14px;">Your deployment <strong>{{ if .DeployName }}{{ .DeployName }}{{ else }}app{{ end }}</strong> has expired and is no longer serving traffic{{ if .ExpiresAt }} (expired {{ .ExpiresAt }} UTC){{ end }}.</p>
<p style="margin:0 0 4px;">You can redeploy at any time. Upgrade your plan to get deployments that don't expire.</p>`))

	bodyDeployMadePermanent = template.Must(template.New("b_dperm").Parse(
		`<p style="margin:0 0 14px;">Your deployment is now <strong>permanent</strong> — it will no longer expire automatically{{ if .Source }} (changed via {{ .Source }}){{ end }}.</p>
<p style="margin:0 0 4px;color:#555;font-size:14px;">It'll keep serving traffic until you delete it explicitly.</p>`))

	bodyDeployDeletionConfirmed = template.Must(template.New("b_ddc").Parse(
		`<p style="margin:0 0 14px;">The deletion of your deployment has been confirmed and the resource has been torn down{{ if .FreedAt }} at {{ .FreedAt }}{{ end }}.</p>
<p style="margin:0 0 4px;color:#555;font-size:14px;">This action is complete. If this wasn't you, reply to this email right away.</p>`))

	bodyDeployDeletionCancelled = template.Must(template.New("b_ddcx").Parse(
		`<p style="margin:0 0 14px;">Good news — the pending deletion of your deployment has been <strong>cancelled</strong>. Your resource is safe and still running.</p>
<p style="margin:0 0 4px;color:#555;font-size:14px;">No further action is needed.</p>`))

	bodyDeployDeletionExpired = template.Must(template.New("b_ddex").Parse(
		`<p style="margin:0 0 14px;">The confirmation window for deleting your deployment has elapsed. Because the deletion was never confirmed, your resource was <strong>kept</strong> and is still running.</p>
<p style="margin:0 0 4px;color:#555;font-size:14px;">If you do want to delete it, start the deletion flow again from your dashboard.</p>`))

	bodyDigestWeekly = template.Must(template.New("b_digest").Parse(
		`<p style="margin:0 0 14px;">Here's your weekly instanode summary{{ if .TeamName }} for <strong>{{ .TeamName }}</strong>{{ end }}.</p>
<table cellpadding="6" cellspacing="0" style="background:#f7f7f8;border-radius:6px;font-size:14px;margin:4px 0 14px;width:100%;">
  <tr><td style="color:#666;width:160px;">Active resources</td><td><strong>{{ if .TotalActiveResources }}{{ .TotalActiveResources }}{{ else }}0{{ end }}</strong></td></tr>
</table>
<p style="margin:0 0 4px;color:#555;font-size:14px;">Open your dashboard for the full per-resource breakdown.</p>`))

	bodyResourceQuotaSuspended = template.Must(template.New("b_qsusp").Parse(
		`<p style="margin:0 0 14px;">Your {{ .ResourceType }} resource <strong>{{ .Name }}</strong> has been <strong>suspended</strong> because it exceeded its storage limit.</p>
<table cellpadding="6" cellspacing="0" style="background:#f7f7f8;border-radius:6px;font-size:14px;margin:4px 0 14px;width:100%;">
  <tr><td style="color:#666;width:140px;">Resource</td><td><strong>{{ .Name }}</strong></td></tr>
  <tr><td style="color:#666;">Type</td><td>{{ .ResourceType }}</td></tr>
  <tr><td style="color:#666;">Status</td><td><strong>suspended</strong></td></tr>
</table>
<p style="margin:0 0 4px;">To restore access, either delete data to drop usage back under the limit, or upgrade your plan for a higher storage limit. Access is restored automatically once usage is back under the limit.</p>`))

	bodyResourceQuotaUnsuspended = template.Must(template.New("b_qunsusp").Parse(
		`<p style="margin:0 0 14px;">Your {{ .ResourceType }} resource <strong>{{ .Name }}</strong> is <strong>active again</strong> — storage usage is back under its limit and access has been restored.</p>
<table cellpadding="6" cellspacing="0" style="background:#f7f7f8;border-radius:6px;font-size:14px;margin:4px 0 14px;width:100%;">
  <tr><td style="color:#666;width:140px;">Resource</td><td><strong>{{ .Name }}</strong></td></tr>
  <tr><td style="color:#666;">Type</td><td>{{ .ResourceType }}</td></tr>
  <tr><td style="color:#666;">Status</td><td><strong>active</strong></td></tr>
</table>
<p style="margin:0 0 4px;color:#555;font-size:14px;">No action is needed. To avoid another suspension, keep usage below the limit or upgrade for more headroom.</p>`))

	// ── W2 body templates — dunning, admin-cancel, backup/restore, deploy ──

	bodyPaymentGraceStarted = template.Must(template.New("b_gstart").Parse(
		`<p style="margin:0 0 14px;">We couldn't process your most recent instanode payment. Your account has entered a 7-day grace period{{ if .ExpiresAt }}, ending {{ .ExpiresAt }} UTC{{ end }}.</p>
<p style="margin:0 0 4px;">Update your payment method before the grace period ends to keep your resources running. If the grace period expires without a successful payment, your subscription will be terminated.</p>`))

	bodyPaymentGraceReminder = template.Must(template.New("b_gremind").Parse(
		`<p style="margin:0 0 14px;">Your instanode account is still in a payment grace period{{ if .HoursRemaining }} — about {{ .HoursRemaining }} hour(s) remain{{ end }}{{ if .GraceEndsAt }} (ends {{ .GraceEndsAt }} UTC){{ end }}.</p>
<p style="margin:0 0 4px;">Update your payment method now to avoid termination of your subscription and your resources.</p>`))

	bodyPaymentGraceRecovered = template.Must(template.New("b_grecov").Parse(
		`<p style="margin:0 0 14px;">Good news — your instanode payment went through and your account is back in good standing{{ if .RecoveredAt }} as of {{ .RecoveredAt }} UTC{{ end }}.</p>
<p style="margin:0 0 4px;color:#555;font-size:14px;">No further action is needed. Thanks for staying with instanode.</p>`))

	bodyPaymentGraceTerminated = template.Must(template.New("b_gterm").Parse(
		`<p style="margin:0 0 14px;">Your instanode subscription has been terminated because the payment grace period ended{{ if .GraceEndsAt }} ({{ .GraceEndsAt }} UTC){{ end }} without a successful payment.</p>
<p style="margin:0 0 4px;">You can resubscribe at any time to restore access. Resubscribe soon — terminated resources are subject to deletion.</p>`))

	bodySubscriptionCanceledByAdmin = template.Must(template.New("b_admincancel").Parse(
		`<p style="margin:0 0 14px;">Your instanode subscription has been cancelled by our support team.</p>
<p style="margin:0 0 4px;color:#555;font-size:14px;">Your data isn't deleted immediately — you can resubscribe any time to restore full access. If you didn't request this, reply to this email and we'll look into it.</p>`))

	bodyBackupFailed = template.Must(template.New("b_bkfail").Parse(
		`<p style="margin:0 0 14px;">A backup of your {{ if .ResourceType }}{{ .ResourceType }} {{ end }}resource failed to complete.</p>
<table cellpadding="6" cellspacing="0" style="background:#f7f7f8;border-radius:6px;font-size:14px;margin:4px 0 14px;width:100%;">
  {{ if .BackupID }}<tr><td style="color:#666;width:140px;">Backup</td><td><code>{{ .BackupID }}</code></td></tr>{{ end }}
  {{ if .ErrorSummary }}<tr><td style="color:#666;">Error</td><td>{{ .ErrorSummary }}</td></tr>{{ end }}
</table>
<p style="margin:0 0 4px;">No data was lost — only the backup did not complete. You can trigger a new backup from your dashboard. If this keeps happening, reply to this email.</p>`))

	bodyRestoreSucceeded = template.Must(template.New("b_rsok").Parse(
		`<p style="margin:0 0 14px;">A restore of your {{ if .ResourceType }}{{ .ResourceType }} {{ end }}resource completed successfully.</p>
<table cellpadding="6" cellspacing="0" style="background:#f7f7f8;border-radius:6px;font-size:14px;margin:4px 0 14px;width:100%;">
  {{ if .RestoreID }}<tr><td style="color:#666;width:140px;">Restore</td><td><code>{{ .RestoreID }}</code></td></tr>{{ end }}
  {{ if .BackupID }}<tr><td style="color:#666;">From backup</td><td><code>{{ .BackupID }}</code></td></tr>{{ end }}
</table>
<p style="margin:0 0 4px;color:#555;font-size:14px;">Your resource is back to the restored state. No further action is needed.</p>`))

	bodyRestoreFailed = template.Must(template.New("b_rsfail").Parse(
		`<p style="margin:0 0 14px;">A restore of your {{ if .ResourceType }}{{ .ResourceType }} {{ end }}resource failed to complete.</p>
<table cellpadding="6" cellspacing="0" style="background:#f7f7f8;border-radius:6px;font-size:14px;margin:4px 0 14px;width:100%;">
  {{ if .RestoreID }}<tr><td style="color:#666;width:140px;">Restore</td><td><code>{{ .RestoreID }}</code></td></tr>{{ end }}
  {{ if .BackupID }}<tr><td style="color:#666;">From backup</td><td><code>{{ .BackupID }}</code></td></tr>{{ end }}
  {{ if .ErrorSummary }}<tr><td style="color:#666;">Error</td><td>{{ .ErrorSummary }}</td></tr>{{ end }}
</table>
<p style="margin:0 0 4px;">Your resource was not modified. You can retry the restore from your dashboard. If this keeps happening, reply to this email.</p>`))

	bodyDeployFailed = template.Must(template.New("b_depfail").Parse(
		`<p style="margin:0 0 14px;">Your instanode deployment failed{{ if .FailureStage }} at the {{ .FailureStage }} stage{{ end }}.</p>
<table cellpadding="6" cellspacing="0" style="background:#f7f7f8;border-radius:6px;font-size:14px;margin:4px 0 14px;width:100%;">
  {{ if .DeployID }}<tr><td style="color:#666;width:140px;">Deployment</td><td><code>{{ .DeployID }}</code></td></tr>{{ end }}
  {{ if .FailureStage }}<tr><td style="color:#666;">Stage</td><td>{{ .FailureStage }}</td></tr>{{ end }}
  {{ if .ErrorSummary }}<tr><td style="color:#666;">Error</td><td>{{ .ErrorSummary }}</td></tr>{{ end }}
</table>
<p style="margin:0 0 4px;">Open your dashboard for the full build log, fix the issue, and redeploy.</p>`))

	bodyCheckoutAbandoned = template.Must(template.New("b_ckabandon").Parse(
		`<p style="margin:0 0 14px;">It looks like your recent instanode upgrade{{ if .PlanTier }} to the {{ .PlanTier }} plan{{ end }} didn't complete — no payment went through, so your plan hasn't changed.</p>
<p style="margin:0 0 4px;">Nothing was charged. You can try the upgrade again whenever you're ready. If you hit a problem on the checkout page, reply to this email and we'll help you get it sorted.</p>`))
)

// ── Body view structs — flat, compiler-checked field references ───────────

type viewOnboardingClaimed struct{ SignupSource string }
type viewTierChange struct{ FromTier, ToTier, Reason string }
type viewNearQuotaWall struct{ Axis, PercentUsed, Tier string }
type viewSubscriptionCanceled struct{ LastTier, CanceledAt string }
type viewExperimentConversion struct{ Experiment, Variant, ActionTaken string }
type viewAdminTierChanged struct{ FromTier, ToTier, ByAdmin string }
type viewPromoCodeReceived struct{ Code, Value, ExpiresAt string }
type viewChurnRiskFlagged struct {
	LastActivityDaysAgo string
	ActiveResourceCount string
}
type viewDeployExpiring struct{ DeployName, HoursRemaining, ExpiresAt string }
type viewDeployExpired struct{ DeployName, ExpiresAt string }
type viewDeployMadePermanent struct{ Source string }
type viewDeployDeletionConfirmed struct{ FreedAt string }
type viewDigestWeekly struct{ TeamName, TotalActiveResources string }
type viewResourceQuota struct{ ResourceType, Name string }

// ── W2 view structs ───────────────────────────────────────────────────────
type viewPaymentGraceStarted struct{ ExpiresAt string }
type viewPaymentGraceReminder struct{ HoursRemaining, GraceEndsAt string }
type viewPaymentGraceRecovered struct{ RecoveredAt string }
type viewPaymentGraceTerminated struct{ GraceEndsAt string }
type viewBackupFailed struct{ ResourceType, BackupID, ErrorSummary string }
type viewRestoreResult struct{ ResourceType, RestoreID, BackupID, ErrorSummary string }
type viewDeployFailed struct{ DeployID, FailureStage, ErrorSummary string }
type viewCheckoutAbandoned struct{ PlanTier string }

// ── Render helpers ────────────────────────────────────────────────────────

// renderBody executes a per-kind body template against its view struct.
// Returns template.HTML (safe — the body template auto-escaped its
// inputs) so renderShell can embed it without re-escaping.
func renderBody(tmpl *template.Template, view interface{}) template.HTML {
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, view); err != nil {
		// Body templates are validated at init via template.Must; an
		// Execute failure is a view-shape bug. Degrade to an empty body —
		// the shell still renders the heading + CTA.
		return template.HTML("")
	}
	return template.HTML(buf.String())
}

// lifecycleTextTmpl is the shared plain-text layout. Every kind's text
// alternative uses this — heading + body lines + optional CTA. text/template
// (not html/template) because plain text needs no escaping.
var lifecycleTextTmpl = textTemplate.Must(textTemplate.New("lifecycle_text").Parse(
	`{{ .Heading }}

{{ .Body }}
{{ if .CTALabel }}
{{ .CTALabel }}: {{ .CTAURL }}
{{ end }}
— instanode.dev
Manage your resources: https://instanode.dev/dashboard
`))

// lifecycleTextView is the data lifecycleTextTmpl renders. Body is a
// plain string (newline-joined lines).
type lifecycleTextView struct {
	Heading  string
	Body     string
	CTALabel string
	CTAURL   string
}

// lifecycleText renders the shared plain-text body.
func lifecycleText(v lifecycleTextView) string {
	var buf bytes.Buffer
	if err := lifecycleTextTmpl.Execute(&buf, v); err != nil {
		return v.Heading + "\n\n" + v.Body + "\n"
	}
	return buf.String()
}

// dashboardURL is the canonical fallback CTA target — the dashboard
// landing page. Used when a kind has no kind-specific link in its params.
const dashboardURL = "https://instanode.dev/dashboard"

// pricingURL is the upgrade/checkout target for nudge-style emails.
const pricingURL = "https://instanode.dev/pricing"

// ── Per-kind renderers ────────────────────────────────────────────────────
//
// Each renderer pairs with the builder of the same kind in
// event_email_mapping.go — the param key names below are exactly what
// that builder emits. Every subject is kind-specific and correct: NONE
// say "expires in 6 hours" unless the kind is genuinely an expiry kind.

// renderOnboardingClaimed — pairs with buildTeamClaimed (onboarding.claimed).
func renderOnboardingClaimed(params map[string]string) (string, string, string) {
	subject := "Welcome to instanode — your account is ready"
	heading := "Welcome to instanode"
	body := renderBody(bodyOnboardingClaimed, viewOnboardingClaimed{
		SignupSource: params["signup_source"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "Your instanode account is live. Provision a database, cache, queue, object store, webhook, or full deployment with a single HTTP call.",
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderTierUpgraded — pairs with buildTierUpgraded (subscription.upgraded).
func renderTierUpgraded(params map[string]string) (string, string, string) {
	to := orDefault(params["to_tier"], "a new tier")
	subject := "Your instanode plan is now " + to
	heading := "Plan upgraded"
	body := renderBody(bodyTierUpgraded, viewTierChange{
		FromTier: params["from_tier"], ToTier: params["to_tier"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "View your dashboard", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "Your instanode plan is now " + to + ". Higher limits apply to all your resources immediately.",
		CTALabel: "View your dashboard", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderNearQuotaWall — pairs with buildNearQuotaWall (near_quota_wall).
// THIS is the kind that bit the user: it was routed through Brevo
// template 6 and arrived as a broken "expires in 6 hours" email.
func renderNearQuotaWall(params map[string]string) (string, string, string) {
	subject := "You're approaching your instanode plan limit"
	heading := "You're close to a plan limit"
	body := renderBody(bodyNearQuotaWall, viewNearQuotaWall{
		Axis: params["axis"], PercentUsed: params["percent_used"], Tier: orDefault(params["tier"], "current"),
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Upgrade your plan", CTAURL: pricingURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "One of your resources is approaching its plan limit. Upgrade to raise the ceiling before writes start getting rejected.",
		CTALabel: "Upgrade your plan", CTAURL: pricingURL,
	})
	return subject, html, text
}

// renderTierDowngraded — pairs with buildTierDowngraded (subscription.downgraded).
func renderTierDowngraded(params map[string]string) (string, string, string) {
	to := orDefault(params["to_tier"], "a lower tier")
	subject := "Your instanode plan changed to " + to
	heading := "Plan changed"
	body := renderBody(bodyTierDowngraded, viewTierChange{
		FromTier: params["from_tier"], ToTier: params["to_tier"], Reason: params["reason"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "View your dashboard", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "Your instanode plan changed to " + to + ". Resources provisioned before this change keep their previous limits.",
		CTALabel: "View your dashboard", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderSubscriptionCanceled — pairs with buildSubscriptionCanceled
// (subscription.canceled).
func renderSubscriptionCanceled(params map[string]string) (string, string, string) {
	subject := "Your instanode subscription has been cancelled"
	heading := "Subscription cancelled"
	body := renderBody(bodySubscriptionCanceled, viewSubscriptionCanceled{
		LastTier: params["last_tier"], CanceledAt: params["canceled_at"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Resubscribe", CTAURL: pricingURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "Your instanode subscription has been cancelled. Your data isn't deleted immediately — resubscribe any time to restore full access.",
		CTALabel: "Resubscribe", CTAURL: pricingURL,
	})
	return subject, html, text
}

// friendlyExperimentAction maps the dashboard's machine-readable `action`
// identifier (set by UpgradeButton + sibling experiment emitters in the
// dashboard / instanode-web) to a user-facing description for the email
// body. Unknown values render as empty (which makes the surrounding
// template clause "We noticed you X — nice." disappear entirely — better
// than leaking the raw enum to the user).
//
// Before this map, an /experiments/converted call from the UpgradeButton
// caused the email to render "We noticed you checkout_started — nice."
// — exposing the dashboard's internal action identifier as if it were a
// past-tense English verb (same bug class as the 2026-05-31
// make_permanent_endpoint incident: a worker email reading a snake_case
// enum from params and inlining it as user-facing copy).
func friendlyExperimentAction(raw string) string {
	switch raw {
	case "checkout_started":
		return "started the checkout flow"
	case "overview_upgrade_clicked":
		return "clicked Upgrade on the dashboard"
	}
	return "" // unknown/empty action → template clause disappears
}

// friendlyExperimentName maps the experiments registry's machine-readable
// experiment id (api/internal/experiments) to a user-facing label for the
// email body. Unknown values render as empty (clause drops). Same rule as
// friendlyExperimentAction — never leak a snake_case identifier into copy.
func friendlyExperimentName(raw string) string {
	switch raw {
	case "upgrade_button":
		return "the Upgrade button rollout"
	case "onboarding_v2":
		return "the new onboarding flow"
	}
	return "" // unknown/empty experiment → template clause disappears
}

// renderExperimentConversion — pairs with buildExperimentClicked
// (experiment.conversion).
func renderExperimentConversion(params map[string]string) (string, string, string) {
	subject := "Thanks for trying instanode"
	heading := "Thanks for the feedback"
	body := renderBody(bodyExperimentConversion, viewExperimentConversion{
		Experiment:  friendlyExperimentName(params["experiment"]),
		Variant:     params["variant"],
		ActionTaken: friendlyExperimentAction(params["action_taken"]),
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "Thanks for trying the new instanode experience. We appreciate you helping us make the product better.",
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderAdminTierChanged — pairs with buildTierChangedByAdmin
// (admin.tier_changed).
func renderAdminTierChanged(params map[string]string) (string, string, string) {
	to := orDefault(params["to_tier"], "a new tier")
	subject := "Your instanode account tier was changed to " + to
	heading := "Your account tier changed"
	body := renderBody(bodyAdminTierChanged, viewAdminTierChanged{
		FromTier: params["from_tier"], ToTier: params["to_tier"], ByAdmin: params["by_admin"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "View your dashboard", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "Your instanode account tier was changed to " + to + " by the instanode team. If you didn't expect this, reply to this email.",
		CTALabel: "View your dashboard", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderPromoCodeReceived — pairs with buildPromoCodeReceived
// (admin.promo_issued).
func renderPromoCodeReceived(params map[string]string) (string, string, string) {
	subject := "A promo code was added to your instanode account"
	heading := "You've got a promo code"
	body := renderBody(bodyPromoCodeReceived, viewPromoCodeReceived{
		Code: params["code"], Value: params["value"], ExpiresAt: params["expires_at"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "View your billing", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "A promo code has been added to your instanode account. Code: " + params["code"] + ". It applies on your next billing cycle.",
		CTALabel: "View your billing", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderChurnRiskFlagged — pairs with buildChurnRiskFlagged
// (churn.risk_flagged). The "we miss you" win-back email.
func renderChurnRiskFlagged(params map[string]string) (string, string, string) {
	subject := "We miss you at instanode"
	heading := "Still building? We're here to help"
	body := renderBody(bodyChurnRiskFlagged, viewChurnRiskFlagged{
		LastActivityDaysAgo: params["last_activity_days_ago"],
		ActiveResourceCount: params["active_resource_count"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Jump back in", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "We noticed you haven't used instanode in a while. Whatever you were building, we'd love to help you ship it — your resources are right where you left them.",
		CTALabel: "Jump back in", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// deployReminderStagePrefix returns the escalating subject-line prefix for
// a deploy-expiry reminder, keyed on reminder_index (F3, BugBash
// 2026-05-19). The 3-stage cadence mirrors anon.expiry_warning's
// "Heads up" / "Reminder" / "Final reminder" escalation so a customer
// can tell the urgency apart instead of receiving identical emails.
//
// reminder_index is "1".."3" (see deployment_reminder.go, capped at
// maxDeployReminders). An absent / unparseable value defaults to the
// gentlest "Heads up" — never a false "Final reminder".
func deployReminderStagePrefix(reminderIndex string) string {
	switch reminderIndex {
	case "2":
		return "Reminder"
	case "3":
		return "Final reminder"
	default:
		return "Heads up"
	}
}

// renderDeployExpiringSoon — pairs with buildDeployExpiringSoon
// (deploy.expiring_soon). This IS an expiry kind — the subject genuinely
// references expiry, but with the real hours_remaining, not a hardcoded 6.
func renderDeployExpiringSoon(params map[string]string) (string, string, string) {
	hours := orDefault(params["hours_remaining"], "a few")
	name := orDefault(params["deploy_name"], "your deployment")
	// F3: escalating subject prefix keyed on reminder_index.
	prefix := deployReminderStagePrefix(params["reminder_index"])
	subject := prefix + ": your instanode deployment " + name + " expires in " + hours + "h"
	heading := "Your deployment is about to expire"
	body := renderBody(bodyDeployExpiringSoon, viewDeployExpiring{
		DeployName: params["deploy_name"], HoursRemaining: params["hours_remaining"], ExpiresAt: params["expires_at"],
	})
	cta := orDefault(params["make_permanent_url"], dashboardURL)
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Make it permanent", CTAURL: cta,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "Your deployment " + name + " will expire in about " + hours + " hour(s). Make it permanent to keep it running.",
		CTALabel: "Make it permanent", CTAURL: cta,
	})
	return subject, html, text
}

// renderDeployExpired — pairs with buildDeployExpired (deploy.expired).
func renderDeployExpired(params map[string]string) (string, string, string) {
	name := orDefault(params["deploy_name"], "your deployment")
	subject := "Your instanode deployment " + name + " has expired"
	heading := "Your deployment has expired"
	body := renderBody(bodyDeployExpired, viewDeployExpired{
		DeployName: params["deploy_name"], ExpiresAt: params["expires_at"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Redeploy now", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "Your deployment " + name + " has expired and is no longer serving traffic. You can redeploy at any time.",
		CTALabel: "Redeploy now", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// friendlyMakePermanentSource maps the audit-log machine-readable `source`
// enum (set by the api emitters in deploy.go and deploy_ttl.go) to a user-
// facing description for the email body. Unknown values render as empty
// (which makes the surrounding template clause "(changed via X)" disappear
// entirely — better than leaking the raw enum to the user).
//
// Before this map, a Pro upgrade that auto-promoted a deploy's TTL caused
// the email to render "(changed via make_permanent_endpoint)" — exposing
// the api's internal endpoint identifier as if it were a sentence (2026-05-31
// incident reported by mastermanas805).
func friendlyMakePermanentSource(raw string) string {
	switch raw {
	case "make_permanent_endpoint":
		return "the API"
	case "deploy_new":
		return "your initial deploy request"
	case "team_setting", "default_deployment_ttl_policy":
		return "your team default-TTL setting"
	case "tier_upgrade":
		return "your plan upgrade"
	}
	return "" // unknown/empty source → template clause disappears
}

// renderDeployMadePermanent — pairs with buildDeployMadePermanent
// (deploy.made_permanent).
func renderDeployMadePermanent(params map[string]string) (string, string, string) {
	subject := "Your instanode deployment is now permanent"
	heading := "Your deployment is now permanent"
	body := renderBody(bodyDeployMadePermanent, viewDeployMadePermanent{
		Source: friendlyMakePermanentSource(params["source"]),
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "View your deployments", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "Your deployment is now permanent — it will no longer expire automatically and keeps serving traffic until you delete it.",
		CTALabel: "View your deployments", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderDeployDeletionConfirmed — pairs with buildDeployDeletionConfirmed
// (deploy.deletion_confirmed).
func renderDeployDeletionConfirmed(params map[string]string) (string, string, string) {
	subject := "Your instanode deployment has been deleted"
	heading := "Deletion confirmed"
	body := renderBody(bodyDeployDeletionConfirmed, viewDeployDeletionConfirmed{
		FreedAt: params["freed_at"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "The deletion of your deployment has been confirmed and the resource has been torn down. If this wasn't you, reply to this email right away.",
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderDeployDeletionCancelled — pairs with buildDeployDeletionCancelled
// (deploy.deletion_cancelled).
func renderDeployDeletionCancelled(params map[string]string) (string, string, string) {
	subject := "Your instanode deployment deletion was cancelled"
	heading := "Deletion cancelled — your resource is safe"
	body := renderBody(bodyDeployDeletionCancelled, struct{}{})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "Good news — the pending deletion of your deployment has been cancelled. Your resource is safe and still running.",
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderDeployDeletionExpired — pairs with buildDeployDeletionExpired
// (deploy.deletion_expired).
func renderDeployDeletionExpired(params map[string]string) (string, string, string) {
	subject := "Your instanode deployment was kept — deletion window elapsed"
	heading := "Deletion not confirmed — your resource was kept"
	body := renderBody(bodyDeployDeletionExpired, struct{}{})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "The confirmation window for deleting your deployment has elapsed. Because the deletion was never confirmed, your resource was kept and is still running.",
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderDigestWeekly — pairs with buildDigestWeekly (digest.weekly).
func renderDigestWeekly(params map[string]string) (string, string, string) {
	subject := "Your weekly instanode summary"
	heading := "Your week on instanode"
	body := renderBody(bodyDigestWeekly, viewDigestWeekly{
		TeamName: params["team_name"], TotalActiveResources: params["total_active_resources"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "View your dashboard", CTAURL: dashboardURL,
	})
	count := orDefault(params["total_active_resources"], "0")
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "Here's your weekly instanode summary. You have " + count + " active resource(s). Open your dashboard for the full breakdown.",
		CTALabel: "View your dashboard", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderResourceQuotaSuspended — pairs with buildResourceQuotaSuspended
// (resource.quota_suspended). The customer's database/cache/store was
// suspended for exceeding its storage limit. NOT an expiry kind — the
// subject must not mention expiry.
func renderResourceQuotaSuspended(params map[string]string) (string, string, string) {
	rtype := orDefault(params["resource_type"], "resource")
	name := orDefault(params["name"], "your resource")
	subject := "Your instanode " + rtype + " resource was suspended — storage limit exceeded"
	heading := "Your resource was suspended"
	body := renderBody(bodyResourceQuotaSuspended, viewResourceQuota{
		ResourceType: rtype, Name: name,
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Upgrade your plan", CTAURL: pricingURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body: "Your " + rtype + " resource " + name + " was suspended because it exceeded its storage limit. " +
			"To restore access, delete data to drop usage back under the limit, or upgrade your plan for a higher limit. " +
			"Access is restored automatically once usage is back under the limit.",
		CTALabel: "Upgrade your plan", CTAURL: pricingURL,
	})
	return subject, html, text
}

// renderResourceQuotaUnsuspended — pairs with buildResourceQuotaUnsuspended
// (resource.quota_unsuspended). The customer's resource is back under its
// storage limit and access has been restored.
func renderResourceQuotaUnsuspended(params map[string]string) (string, string, string) {
	rtype := orDefault(params["resource_type"], "resource")
	name := orDefault(params["name"], "your resource")
	subject := "Your instanode " + rtype + " resource " + name + " is active again"
	heading := "Your resource is active again"
	body := renderBody(bodyResourceQuotaUnsuspended, viewResourceQuota{
		ResourceType: rtype, Name: name,
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body: "Your " + rtype + " resource " + name + " is active again — storage usage is back under its limit " +
			"and access has been restored. No action is needed.",
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// ── W2 renderers — dunning, admin-cancel, backup/restore, deploy ──────────
//
// Each pairs with the builder of the same kind in event_email_mapping.go.
// None is an expiry kind — the subjects say what actually happened.

// renderPaymentGraceStarted — pairs with buildPaymentGraceStarted
// (payment.grace_started). The first dunning email — a customer whose card
// failed previously got NO notification at all (P1-W2-01).
func renderPaymentGraceStarted(params map[string]string) (string, string, string) {
	subject := "Action needed — your instanode payment failed"
	heading := "We couldn't process your payment"
	body := renderBody(bodyPaymentGraceStarted, viewPaymentGraceStarted{
		ExpiresAt: params["expires_at"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Update payment method", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "We couldn't process your most recent instanode payment. Your account has entered a 7-day grace period. Update your payment method before it ends to keep your resources running.",
		CTALabel: "Update payment method", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderPaymentGraceReminder — pairs with buildPaymentGraceReminder
// (payment.grace_reminder).
func renderPaymentGraceReminder(params map[string]string) (string, string, string) {
	subject := "Reminder — update your instanode payment method"
	heading := "Your payment is still pending"
	body := renderBody(bodyPaymentGraceReminder, viewPaymentGraceReminder{
		HoursRemaining: params["hours_remaining"], GraceEndsAt: params["grace_ends_at"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Update payment method", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "Your instanode account is still in a payment grace period. Update your payment method now to avoid termination of your subscription and your resources.",
		CTALabel: "Update payment method", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderPaymentGraceRecovered — pairs with buildPaymentGraceRecovered
// (payment.grace_recovered).
func renderPaymentGraceRecovered(params map[string]string) (string, string, string) {
	subject := "Your instanode payment went through"
	heading := "You're back in good standing"
	body := renderBody(bodyPaymentGraceRecovered, viewPaymentGraceRecovered{
		RecoveredAt: params["recovered_at"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "Good news — your instanode payment went through and your account is back in good standing. No further action is needed.",
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderPaymentGraceTerminated — pairs with buildPaymentGraceTerminated
// (payment.grace_terminated).
func renderPaymentGraceTerminated(params map[string]string) (string, string, string) {
	subject := "Your instanode subscription has been terminated"
	heading := "Your subscription was terminated"
	body := renderBody(bodyPaymentGraceTerminated, viewPaymentGraceTerminated{
		GraceEndsAt: params["grace_ends_at"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Resubscribe", CTAURL: pricingURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "Your instanode subscription has been terminated because the payment grace period ended without a successful payment. You can resubscribe at any time to restore access.",
		CTALabel: "Resubscribe", CTAURL: pricingURL,
	})
	return subject, html, text
}

// renderSubscriptionCanceledByAdmin — pairs with buildSubscriptionCanceledByAdmin
// (subscription.canceled_by_admin). Distinct from the customer-initiated
// subscription.canceled email — this copy says "by support" (P1-W2-02).
func renderSubscriptionCanceledByAdmin(params map[string]string) (string, string, string) {
	subject := "Your instanode subscription was cancelled by support"
	heading := "Subscription cancelled by support"
	body := renderBody(bodySubscriptionCanceledByAdmin, struct{}{})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Resubscribe", CTAURL: pricingURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "Your instanode subscription has been cancelled by our support team. Your data isn't deleted immediately — you can resubscribe any time. If you didn't request this, reply to this email.",
		CTALabel: "Resubscribe", CTAURL: pricingURL,
	})
	return subject, html, text
}

// renderBackupFailed — pairs with buildBackupFailed (backup.failed).
func renderBackupFailed(params map[string]string) (string, string, string) {
	rtype := orDefault(params["resource_type"], "resource")
	subject := "An instanode backup of your " + rtype + " failed"
	heading := "Backup failed"
	body := renderBody(bodyBackupFailed, viewBackupFailed{
		ResourceType: params["resource_type"], BackupID: params["backup_id"], ErrorSummary: params["error_summary"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "A backup of your " + rtype + " resource failed to complete. No data was lost — only the backup did not complete. You can trigger a new backup from your dashboard.",
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderRestoreSucceeded — pairs with buildRestoreSucceeded (restore.succeeded).
func renderRestoreSucceeded(params map[string]string) (string, string, string) {
	rtype := orDefault(params["resource_type"], "resource")
	subject := "Your instanode " + rtype + " restore completed"
	heading := "Restore completed"
	body := renderBody(bodyRestoreSucceeded, viewRestoreResult{
		ResourceType: params["resource_type"], RestoreID: params["restore_id"], BackupID: params["backup_id"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "A restore of your " + rtype + " resource completed successfully. Your resource is back to the restored state. No further action is needed.",
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderRestoreFailed — pairs with buildRestoreFailed (restore.failed).
func renderRestoreFailed(params map[string]string) (string, string, string) {
	rtype := orDefault(params["resource_type"], "resource")
	subject := "An instanode restore of your " + rtype + " failed"
	heading := "Restore failed"
	body := renderBody(bodyRestoreFailed, viewRestoreResult{
		ResourceType: params["resource_type"], RestoreID: params["restore_id"],
		BackupID: params["backup_id"], ErrorSummary: params["error_summary"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "A restore of your " + rtype + " resource failed to complete. Your resource was not modified. You can retry the restore from your dashboard.",
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderDeployFailed — pairs with buildDeployFailed (deploy.failed).
func renderDeployFailed(params map[string]string) (string, string, string) {
	subject := "Your instanode deployment failed"
	heading := "Deployment failed"
	body := renderBody(bodyDeployFailed, viewDeployFailed{
		DeployID: params["deploy_id"], FailureStage: params["failure_stage"], ErrorSummary: params["error_summary"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading: heading,
		Body:    "Your instanode deployment failed. Open your dashboard for the full build log, fix the issue, and redeploy.",
		CTALabel: "Open your dashboard", CTAURL: dashboardURL,
	})
	return subject, html, text
}

// renderCheckoutAbandoned — pairs with buildCheckoutAbandoned
// (checkout.abandoned). The dunning email for a Razorpay checkout that was
// started but never completed — the customer reached the hosted checkout
// page and left (or it failed there) without a payment being created, so
// Razorpay sent no webhook and this is the only nudge they get. The CTA
// sends them back to pricing to retry the upgrade.
func renderCheckoutAbandoned(params map[string]string) (string, string, string) {
	subject := "Your instanode upgrade didn't go through"
	heading := "Your upgrade didn't complete"
	body := renderBody(bodyCheckoutAbandoned, viewCheckoutAbandoned{
		PlanTier: params["plan_tier"],
	})
	html := renderShell(emailShellView{
		Title: subject, Heading: heading, Body: body,
		CTALabel: "Try the upgrade again", CTAURL: pricingURL,
	})
	text := lifecycleText(lifecycleTextView{
		Heading:  heading,
		Body:     "It looks like your recent instanode upgrade didn't complete — no payment went through, so your plan hasn't changed. You can try again any time; nothing was charged. If you ran into a problem at checkout, reply to this email and we'll help.",
		CTALabel: "Try the upgrade again", CTAURL: pricingURL,
	})
	return subject, html, text
}

// orDefault returns s if it's non-empty (after trimming), else fallback.
// Keeps the renderers above terse and avoids "<no value>" / empty
// interpolations leaking into subjects.
func orDefault(s, fallback string) string {
	if strings.TrimSpace(s) == "" {
		return fallback
	}
	return s
}
