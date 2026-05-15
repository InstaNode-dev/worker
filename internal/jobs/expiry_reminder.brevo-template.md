# Brevo template — `RESOURCE_EXPIRING` (anon.expiry_warning)

Operator action: open the Brevo dashboard → Templates → `RESOURCE_EXPIRING`
(the template id wired in `BREVO_TEMPLATE_IDS["anon.expiry_warning"]`),
replace the body with the HTML below, and **delete any hardcoded `6 hours`
or `Type:` / `Token:` / `Expires:` lines** that were leftover from the
previous static draft.

Every value below comes from worker-emitted audit_log metadata via
`buildAnonExpiryWarning` (`event_email_mapping.go`). Brevo placeholder
syntax is `{{ params.* }}`.

## Subject line

```
{{ if eq params.reminder_index "3" }}Final reminder{{ else if eq params.reminder_index "2" }}Reminder{{ else }}Heads up{{ end }} — your {{ params.resource_type }} on instanode expires in {{ params.hours_remaining }}h
```

If your Brevo plan doesn't support conditional subject lines, use:

```
Your instanode {{ params.resource_type }} expires in {{ params.hours_remaining }} hours
```

## Body (HTML, paste into the visual editor or "Code your own")

```html
<!DOCTYPE html>
<html>
  <body style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;color:#111;line-height:1.5;max-width:560px;margin:0 auto;padding:24px;">
    <h2 style="margin:0 0 16px;font-size:20px;">
      Your resource expires in {{ params.hours_remaining }} hour{{ if eq params.hours_remaining "1" }}{{ else }}s{{ end }}
    </h2>

    <p style="margin:0 0 16px;">
      This is reminder {{ params.reminder_index }} of 3 for the free-tier resource
      you provisioned on instanode. Free-tier resources expire 24 hours after
      provisioning unless you upgrade to Hobby.
    </p>

    <table cellpadding="6" cellspacing="0" style="background:#f7f7f8;border-radius:6px;font-size:14px;margin:16px 0;width:100%;">
      <tr><td style="color:#666;width:120px;">Type</td><td><strong>{{ params.resource_type }}</strong></td></tr>
      <tr><td style="color:#666;">Token (first 8)</td><td><code>{{ params.token_prefix }}…</code></td></tr>
      <tr><td style="color:#666;">Expires</td><td>{{ params.expires_at }} UTC</td></tr>
      <tr><td style="color:#666;">Time left</td><td><strong>{{ params.hours_remaining }} hour{{ if eq params.hours_remaining "1" }}{{ else }}s{{ end }}</strong></td></tr>
    </table>

    <p style="margin:24px 0 16px;">
      <a href="{{ params.upgrade_url }}"
         style="background:#000;color:#fff;text-decoration:none;padding:12px 18px;border-radius:6px;display:inline-block;font-weight:600;">
        Upgrade to Hobby ($9/mo) — keep this resource
      </a>
    </p>

    <p style="margin:0 0 24px;">
      Or just open the resource in your dashboard:
      <a href="{{ params.resource_url }}" style="color:#0a7;">view resource details →</a>
    </p>

    <p style="font-size:12px;color:#888;margin:32px 0 0;">
      If you're just kicking the tires, you can ignore this email — the
      resource will be deleted automatically. You'll get a maximum of 3
      reminders per resource (this is {{ params.reminder_index }} of 3).
    </p>
  </body>
</html>
```

## Plain-text body (for clients without HTML)

```
Your instanode {{ params.resource_type }} expires in {{ params.hours_remaining }} hour(s).

This is reminder {{ params.reminder_index }} of 3.

Type:    {{ params.resource_type }}
Token:   {{ params.token_prefix }}…
Expires: {{ params.expires_at }} UTC

Keep it: {{ params.upgrade_url }}
Details: {{ params.resource_url }}

You'll get at most 3 reminders per resource. After expiry it deletes automatically.
```

## Template params reference

Every key the worker emits as of `expiry_reminder.go` (2026-05-15):

| Param            | Type    | Example                                  | Notes |
|------------------|---------|------------------------------------------|-------|
| `resource_type`  | string  | `postgres`                               | column-sourced; same as `auditRow.ResourceType` |
| `resource_id`    | string  | `b2ba09f1-34fe-4a51-ab33-f3e2268dcecb`   | UUID |
| `hours_remaining`| string  | `10`                                     | int floored at 1; **never hardcode in template** |
| `expires_at`     | string  | `2026-05-15T18:24:11Z`                   | RFC3339 UTC |
| `reminder_index` | string  | `1` / `2` / `3`                          | always 1..3; "of 3" copy is safe |
| `stage_label`    | string  | `stage_12h` / `stage_6h` / `stage_1h`    | logging/A-B; rarely shown to users |
| `token_prefix`   | string  | `abc12345`                               | first 8 chars only; never the full token |
| `upgrade_url`    | string  | `https://instanode.dev/app/billing?upgrade=hobby&source=expiry_reminder&stage=stage_12h` | already URL-encoded |
| `resource_url`   | string  | `https://instanode.dev/app/resources/<uuid>` | dashboard detail page |
| `email`          | string  | recipient address                        | forwarder also resolves separately |
| `audit_kind`     | string  | `anon.expiry_warning`                    | for templates shared across kinds |

## What this changes vs the previous template

1. `hours_remaining` is now a real variable. Body & subject lines reference
   `{{ params.hours_remaining }}` instead of the literal "6 hours".
2. The Type/Token/Expires panel actually renders values (was empty before
   because the template was reading params that weren't being sent).
3. Two CTAs: a primary "Upgrade to Hobby" button → dashboard checkout,
   and a secondary "view resource details" link → resource page.
4. Reminder index ("1 of 3" / "Final") so the recipient knows how many
   more to expect. Worker enforces max 3.
