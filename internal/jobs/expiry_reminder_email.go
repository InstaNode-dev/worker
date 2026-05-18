package jobs

// expiry_reminder_email.go — Go-rendered email body for the
// anon.expiry_warning audit_log.kind. Replaces the Brevo dashboard
// template which had two bugs in production (2026-05-15):
//
//   1. Subject was hardcoded "Your resource expires in 6 hours" — every
//      reminder regardless of hours_remaining said "6 hours".
//   2. The Type / Token / Expires panel rendered empty because the
//      template body referenced params the worker wasn't yet sending.
//
// By rendering in Go we have one source of truth (this file) deployed
// with the worker image — no out-of-band Brevo dashboard edit, no drift
// between the params the worker emits and the template that reads them.
//
// Wiring: event_email_forwarder.go calls renderAnonExpiryEmail after
// buildAnonExpiryWarning produces params. The rendered subject + html
// + text flow into EventEmail.Subject/HTMLBody/TextBody. The BrevoProvider
// takes the raw-HTML path when HTMLBody is set (see brevo_provider.go).
//
// Template params reference — every key surfaced here must be present in
// the params map (buildAnonExpiryWarning is the producer):
//
//   resource_type   — postgres / redis / mongodb / queue / storage
//   hours_remaining — int floored at 1, stringified ("1" .. "12")
//   expires_at      — RFC3339 UTC ("2026-05-16T00:00:00Z")
//   reminder_index  — "1" | "2" | "3"
//   stage_label     — "stage_12h" | "stage_6h" | "stage_1h"  (debug/A-B only)
//   token_prefix    — first 8 chars of the resource token (never the full token)
//   upgrade_url     — dashboard checkout link
//   resource_url    — dashboard resource detail link
//
// Missing keys render as empty strings — safe degradation, no panic.

import (
	"bytes"
	"fmt"
	"html/template"
	textTemplate "text/template"
)

// anonExpirySubject is the subject-line template. Worker code calls it
// directly rather than rendering a text/template so the subject is one
// fmt.Sprintf away (cheap, predictable, easy to grep).
//
// Examples (reminder_index 1/2/3 with hours_remaining 12/6/1):
//
//   "Heads up — your instanode postgres expires in 12h"
//   "Reminder — your instanode postgres expires in 6h"
//   "Final reminder — your instanode postgres expires in 1h"
//
// We DELIBERATELY use the "Nh" short form (no "hour(s)" word) — concise
// and identical across singular/plural so the truncated mobile subject
// preview still reads cleanly.
func anonExpirySubject(reminderIndex, resourceType, hoursRemaining string) string {
	prefix := "Heads up"
	switch reminderIndex {
	case "2":
		prefix = "Reminder"
	case "3":
		prefix = "Final reminder"
	}
	if resourceType == "" {
		resourceType = "resource"
	}
	if hoursRemaining == "" {
		hoursRemaining = "1"
	}
	return fmt.Sprintf("%s — your instanode %s expires in %sh", prefix, resourceType, hoursRemaining)
}

// anonExpiryHTMLTmpl is the html/template for the email body. Mirrors
// the structure documented in expiry_reminder.brevo-template.md but
// inlines Brevo-independent values — `{{ .Field }}` instead of
// `{{ params.* }}`. html/template auto-escapes user-controlled fields
// (resource type, token prefix) so a future malicious value can't
// inject markup.
var anonExpiryHTMLTmpl = template.Must(template.New("anon_expiry").Parse(`<!DOCTYPE html>
<html>
  <body style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;color:#111;line-height:1.5;max-width:560px;margin:0 auto;padding:24px;">
    <h2 style="margin:0 0 16px;font-size:20px;">
      Your {{ .ResourceType }} expires in {{ .HoursRemaining }} hour{{ if .Plural }}s{{ end }}
    </h2>

    <p style="margin:0 0 16px;">
      This is reminder {{ .ReminderIndex }} of 3 for the free-tier resource
      you provisioned on instanode. Free-tier resources expire 24 hours after
      provisioning unless you upgrade to Hobby.
    </p>

    <table cellpadding="6" cellspacing="0" style="background:#f7f7f8;border-radius:6px;font-size:14px;margin:16px 0;width:100%;">
      <tr><td style="color:#666;width:120px;">Type</td><td><strong>{{ .ResourceType }}</strong></td></tr>
      <tr><td style="color:#666;">Token (first 8)</td><td><code>{{ .TokenPrefix }}…</code></td></tr>
      <tr><td style="color:#666;">Expires</td><td>{{ .ExpiresAt }} UTC</td></tr>
      <tr><td style="color:#666;">Time left</td><td><strong>{{ .HoursRemaining }} hour{{ if .Plural }}s{{ end }}</strong></td></tr>
    </table>

    <p style="margin:24px 0 16px;">
      <a href="{{ .UpgradeURL }}"
         style="background:#000;color:#fff;text-decoration:none;padding:12px 18px;border-radius:6px;display:inline-block;font-weight:600;">
        Upgrade to Hobby ($9/mo) — keep this resource
      </a>
    </p>

    <p style="margin:0 0 24px;">
      Or just open the resource in your dashboard:
      <a href="{{ .ResourceURL }}" style="color:#0a7;">view resource details →</a>
    </p>

    <p style="font-size:12px;color:#888;margin:32px 0 0;">
      If you're just kicking the tires, you can ignore this email — the
      resource will be deleted automatically. You'll get a maximum of 3
      reminders per resource (this is {{ .ReminderIndex }} of 3).
    </p>
  </body>
</html>
`))

// anonExpiryTextTmpl is the plain-text alternative body. Brevo
// negotiates HTML vs text based on the recipient's mail client. We use
// text/template here (not html/template) because plain text doesn't need
// or want HTML escaping; the token_prefix and resource_type values are
// already constrained to safe characters by the worker.
var anonExpiryTextTmpl = textTemplate.Must(textTemplate.New("anon_expiry_text").Parse(
	`Your instanode {{ .ResourceType }} expires in {{ .HoursRemaining }} hour{{ if .Plural }}s{{ end }}.

This is reminder {{ .ReminderIndex }} of 3.

Type:    {{ .ResourceType }}
Token:   {{ .TokenPrefix }}…
Expires: {{ .ExpiresAt }} UTC

Keep it: {{ .UpgradeURL }}
Details: {{ .ResourceURL }}

You'll get at most 3 reminders per resource. After expiry it deletes automatically.
`))

// hourWord returns the correctly-pluralised noun for the "N hour(s)"
// copy used in the renderAnonExpiryEmail fallback bodies. The primary
// html/text templates use `{{ if .Plural }}s{{ end }}` inline; the
// Sprintf fallback paths must match that, otherwise a 1-hour reminder
// degrades to the grammatically-wrong "1 hours" (BugBash 2026-05-18 P3,
// "anonExpiry plurality"). Plural is true unless HoursRemaining == "1".
func hourWord(plural bool) string {
	if plural {
		return "hours"
	}
	return "hour"
}

// anonExpiryView is the (small) shape passed to both templates. Flat
// struct (not the params map) so the template can use direct field
// references and the compiler catches typos.
type anonExpiryView struct {
	ResourceType   string
	HoursRemaining string
	ExpiresAt      string
	ReminderIndex  string
	TokenPrefix    string
	UpgradeURL     string
	ResourceURL    string
	Plural         bool // true unless HoursRemaining == "1"
}

// renderAnonExpiryEmail turns the per-row params from
// buildAnonExpiryWarning into a (subject, html, text) triple. Never
// returns an error — template execution against a fixed template and
// flat string fields can't fail (the templates are tested at boot via
// template.Must). On the off-chance a future template change breaks
// parsing, the panic surfaces in CI rather than at runtime.
//
// Missing param keys render as empty strings. The caller (forwarder)
// is responsible for not invoking this for kinds other than
// anon.expiry_warning.
func renderAnonExpiryEmail(params map[string]string) (subject, html, text string) {
	view := anonExpiryView{
		ResourceType:   params["resource_type"],
		HoursRemaining: params["hours_remaining"],
		ExpiresAt:      params["expires_at"],
		ReminderIndex:  params["reminder_index"],
		TokenPrefix:    params["token_prefix"],
		UpgradeURL:     params["upgrade_url"],
		ResourceURL:    params["resource_url"],
		Plural:         params["hours_remaining"] != "1",
	}

	subject = anonExpirySubject(view.ReminderIndex, view.ResourceType, view.HoursRemaining)

	var htmlBuf bytes.Buffer
	if err := anonExpiryHTMLTmpl.Execute(&htmlBuf, view); err != nil {
		// Template parsing is validated at init by template.Must, so
		// Execute can only fail on a programming error in the view
		// struct shape. Fall back to a minimal, valid HTML body so the
		// recipient still gets a usable email rather than a 500 in the
		// forwarder. The bug will show up in slog (downstream) — the
		// forwarder logs every send.
		htmlBuf.Reset()
		htmlBuf.WriteString(fmt.Sprintf(
			"<p>Your instanode %s resource expires in %s %s. Visit %s to keep it.</p>",
			view.ResourceType, view.HoursRemaining, hourWord(view.Plural), view.UpgradeURL,
		))
	}
	html = htmlBuf.String()

	var textBuf bytes.Buffer
	if err := anonExpiryTextTmpl.Execute(&textBuf, view); err != nil {
		textBuf.Reset()
		textBuf.WriteString(fmt.Sprintf(
			"Your instanode %s resource expires in %s %s. Visit %s to keep it.\n",
			view.ResourceType, view.HoursRemaining, hourWord(view.Plural), view.UpgradeURL,
		))
	}
	text = textBuf.String()

	return subject, html, text
}
