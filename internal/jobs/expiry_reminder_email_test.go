package jobs

// expiry_reminder_email_test.go — verifies the Go-rendered anon expiry
// warning email gets every fix the broken Brevo dashboard template was
// missing:
//
//   1. Subject reflects actual hours_remaining (NOT hardcoded "6 hours").
//   2. Subject prefix changes by reminder_index (Heads up / Reminder /
//      Final reminder).
//   3. HTML body contains the resource_type, token_prefix, expires_at,
//      upgrade_url, and resource_url — none of which were rendering
//      before (the template referenced params that weren't being sent).
//   4. Plural-aware "hour" vs "hours" copy.
//   5. Renderer is registered for auditKindAnonExpiryWarning so the
//      forwarder picks it up.

import (
	"strings"
	"testing"
)

func TestRenderAnonExpiryEmail_SubjectReflectsHoursAndIndex(t *testing.T) {
	tests := []struct {
		name           string
		params         map[string]string
		wantSubject    string
	}{
		{
			name: "stage 1 — Heads up, 12h, postgres",
			params: map[string]string{
				"reminder_index":  "1",
				"resource_type":   "postgres",
				"hours_remaining": "12",
			},
			wantSubject: "Heads up — your instanode postgres expires in 12h",
		},
		{
			name: "stage 2 — Reminder, 6h, redis",
			params: map[string]string{
				"reminder_index":  "2",
				"resource_type":   "redis",
				"hours_remaining": "6",
			},
			wantSubject: "Reminder — your instanode redis expires in 6h",
		},
		{
			name: "stage 3 — Final reminder, 1h, mongodb",
			params: map[string]string{
				"reminder_index":  "3",
				"resource_type":   "mongodb",
				"hours_remaining": "1",
			},
			wantSubject: "Final reminder — your instanode mongodb expires in 1h",
		},
		{
			name: "missing resource_type falls back to 'resource'",
			params: map[string]string{
				"reminder_index":  "1",
				"resource_type":   "",
				"hours_remaining": "4",
			},
			wantSubject: "Heads up — your instanode resource expires in 4h",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			subject, _, _ := renderAnonExpiryEmail(tc.params)
			if subject != tc.wantSubject {
				t.Errorf("subject = %q; want %q (this is the BUG — production was hardcoding 'expires in 6 hours')", subject, tc.wantSubject)
			}
		})
	}
}

func TestRenderAnonExpiryEmail_HTMLBodyContainsAllFields(t *testing.T) {
	params := map[string]string{
		"reminder_index":  "2",
		"resource_type":   "postgres",
		"hours_remaining": "6",
		"expires_at":      "2026-05-16T00:00:00Z",
		"token_prefix":    "abc12345",
		"upgrade_url":     "https://instanode.dev/app/billing?upgrade=hobby",
		"resource_url":    "https://instanode.dev/app/resources/uuid-1",
	}
	_, html, text := renderAnonExpiryEmail(params)

	for _, want := range []string{
		"postgres",
		"6 hour",  // the plural-aware body
		"2026-05-16T00:00:00Z",
		"abc12345",
		"https://instanode.dev/app/billing?upgrade=hobby",
		"https://instanode.dev/app/resources/uuid-1",
		"reminder 2 of 3",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("HTML body missing %q\n--- BODY ---\n%s", want, html)
		}
	}

	for _, want := range []string{
		"postgres",
		"6 hour",
		"2026-05-16T00:00:00Z",
		"abc12345",
		"https://instanode.dev/app/billing?upgrade=hobby",
		"https://instanode.dev/app/resources/uuid-1",
		"reminder 2 of 3",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("text body missing %q\n--- BODY ---\n%s", want, text)
		}
	}
}

// TestRenderAnonExpiryEmail_PluralCopy — "1 hour" vs "N hours". A small
// detail but the user-facing copy noticed it (the broken template
// said "6 hours" even when the recipient had 1h left).
func TestRenderAnonExpiryEmail_PluralCopy(t *testing.T) {
	_, html1, _ := renderAnonExpiryEmail(map[string]string{
		"reminder_index": "3", "resource_type": "postgres", "hours_remaining": "1",
	})
	if !strings.Contains(html1, "1 hour") || strings.Contains(html1, "1 hours") {
		t.Errorf("1h body must say '1 hour' (singular). html=%s", html1)
	}
	_, html6, _ := renderAnonExpiryEmail(map[string]string{
		"reminder_index": "2", "resource_type": "postgres", "hours_remaining": "6",
	})
	if !strings.Contains(html6, "6 hours") {
		t.Errorf("6h body must say '6 hours' (plural). html=%s", html6)
	}
}

// TestEventEmailBodyRenderers_RegistersAnonExpiryWarning verifies the
// kind→renderer registry is wired. If a future contributor removes this
// entry, the forwarder silently falls back to the broken Brevo template
// path — this test catches that regression.
func TestEventEmailBodyRenderers_RegistersAnonExpiryWarning(t *testing.T) {
	renderer, ok := eventEmailBodyRenderers[auditKindAnonExpiryWarning]
	if !ok {
		t.Fatalf("anon.expiry_warning has no registered renderer; the forwarder would fall back to the broken dashboard template")
	}
	subject, html, text := renderer(map[string]string{
		"reminder_index": "1", "resource_type": "postgres", "hours_remaining": "12",
	})
	if subject == "" || html == "" || text == "" {
		t.Errorf("renderer returned empty fields: subject=%q html_len=%d text_len=%d", subject, len(html), len(text))
	}
}

// TestEventEmailBodyRenderers_CoversBothExpiryKinds locks in the
// 2026-05-15 follow-up fix: the broken Brevo dashboard template_id=6
// was wired to BOTH anon.expiry_warning AND resource.expiry_imminent.
// The first fix only registered the renderer for anon — the
// authenticated paid path still routed through the broken template
// and the user received the broken email at 04:12 UTC.
//
// This test FAILS on the post-first-fix master (only one kind
// registered) and PASSES after the second fix registers both kinds.
// Fail-fast in CI if anyone adds a third expiry kind and forgets to
// register a renderer.
func TestEventEmailBodyRenderers_CoversBothExpiryKinds(t *testing.T) {
	for _, kind := range []string{"anon.expiry_warning", "resource.expiry_imminent"} {
		if _, ok := eventEmailBodyRenderers[kind]; !ok {
			t.Errorf("eventEmailBodyRenderers missing renderer for %q — email would route through Brevo template_id and lose all the new params", kind)
		}
	}
}

// TestBuildResourceExpiring_EmitsAllRendererParams asserts the paid
// expiry builder emits every key the shared renderer reads. Caught the
// production bug where buildResourceExpiring emitted only three keys
// (resource_type / expires_at / hours_remaining) and the renderer's
// Type/Token/Expires panel rendered empty cells.
func TestBuildResourceExpiring_EmitsAllRendererParams(t *testing.T) {
	row := auditRow{
		ID:           "x",
		TeamID:       "t",
		Kind:         auditKindResourceExpiryImminent,
		ResourceType: "postgres",
		OwnerEmail:   "test@example.com",
		Metadata: []byte(`{
			"hours_remaining":"4",
			"expires_at":"2026-05-16T00:00:00Z",
			"resource_id":"abc-123",
			"token_prefix":"abc12345",
			"upgrade_url":"https://instanode.dev/app/billing?upgrade=hobby",
			"resource_url":"https://instanode.dev/app/resources/abc-123"
		}`),
	}
	params, ok := buildResourceExpiring(row)
	if !ok {
		t.Fatal("buildResourceExpiring returned ok=false unexpectedly")
	}
	for _, k := range []string{
		"resource_type",
		"hours_remaining",
		"expires_at",
		"reminder_index",
		"token_prefix",
		"upgrade_url",
		"resource_url",
	} {
		if params[k] == "" {
			t.Errorf("buildResourceExpiring must emit non-empty %q; got params=%v", k, params)
		}
	}
	// reminder_index must pin to "1" (paid path is single-fire — no
	// stage cadence) so the subject reads "Heads up", not "Reminder"
	// or "Final reminder".
	if params["reminder_index"] != "1" {
		t.Errorf("reminder_index = %q; want \"1\" (paid path is single-fire)", params["reminder_index"])
	}
}

// TestRenderAnonExpiryEmail_NeverSaysSixHoursWhenItsNot is a guard
// against the regression that prompted this whole change: the old
// template hardcoded "6 hours" in the subject regardless of the actual
// reminder stage.
func TestRenderAnonExpiryEmail_NeverSaysSixHoursWhenItsNot(t *testing.T) {
	for _, hrs := range []string{"1", "4", "10", "12"} {
		subj, html, _ := renderAnonExpiryEmail(map[string]string{
			"reminder_index": "1", "resource_type": "postgres", "hours_remaining": hrs,
		})
		if strings.Contains(subj, "6 hours") || strings.Contains(subj, "6h") {
			if hrs != "6" {
				t.Errorf("hours_remaining=%s but subject mentions 6: %q (the production bug we are fixing)", hrs, subj)
			}
		}
		if !strings.Contains(subj, hrs+"h") {
			t.Errorf("hours_remaining=%s but subject %q doesn't include %qh", hrs, subj, hrs)
		}
		// Body should ALSO reflect real hours, not "6".
		if hrs != "6" && strings.Contains(html, "expires in 6 hour") {
			t.Errorf("hours_remaining=%s but body says 'expires in 6 hour': %s", hrs, html)
		}
	}
}
