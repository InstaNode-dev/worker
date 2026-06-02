package jobs

// deploy_success_email_test.go — coverage for the deploy success lifecycle
// emails (deploy.created + deploy.healthy, 2026-06-02). The api wrote these
// audit rows on every deploy but the email forwarder had no path for them, so
// a user who deployed got zero email confirmation. These tests pin that the
// builders flow the metadata and the renderers surface the live URL.

import (
	"strings"
	"testing"
)

// TestEventEmail_DeploySuccessKindsRegistered pins that both new kinds are
// fully wired into all three registries — a future edit that drops one fails
// here rather than silently dropping the email at runtime.
func TestEventEmail_DeploySuccessKindsRegistered(t *testing.T) {
	for _, kind := range []string{auditKindDeployCreated, auditKindDeployHealthy} {
		t.Run(kind, func(t *testing.T) {
			found := false
			for _, k := range supportedAuditKinds {
				if k == kind {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("%s missing from supportedAuditKinds — forwarder won't fetch the row", kind)
			}
			if _, ok := eventEmailBuilders[kind]; !ok {
				t.Errorf("%s missing from eventEmailBuilders", kind)
			}
			if _, ok := eventEmailBodyRenderers[kind]; !ok {
				t.Errorf("%s missing from eventEmailBodyRenderers", kind)
			}
		})
	}
}

func TestEventEmail_BuildDeployCreatedFlowsContext(t *testing.T) {
	row := auditRow{
		ID:         "x",
		TeamID:     "t",
		Kind:       auditKindDeployCreated,
		OwnerEmail: "u@example.com",
		Metadata: []byte(`{
			"deploy_id":"deploy-1",
			"app_name":"my-app",
			"env":"production",
			"ttl_policy":"permanent"
		}`),
	}
	params, ok := buildDeployCreated(row)
	if !ok {
		t.Fatal("buildDeployCreated returned ok=false unexpectedly")
	}
	for k, want := range map[string]string{
		"deploy_id":  "deploy-1",
		"app_name":   "my-app",
		"env":        "production",
		"ttl_policy": "permanent",
	} {
		if params[k] != want {
			t.Errorf("params[%q] = %q; want %q", k, params[k], want)
		}
	}
}

func TestEventEmail_BuildDeployHealthyFlowsURL(t *testing.T) {
	row := auditRow{
		ID:         "x",
		TeamID:     "t",
		Kind:       auditKindDeployHealthy,
		OwnerEmail: "u@example.com",
		Metadata: []byte(`{
			"deploy_id":"deploy-1",
			"app_name":"my-app",
			"env":"production",
			"app_url":"https://my-app.deployment.instanode.dev",
			"time_to_healthy_seconds":31
		}`),
	}
	params, ok := buildDeployHealthy(row)
	if !ok {
		t.Fatal("buildDeployHealthy returned ok=false unexpectedly")
	}
	if params["app_url"] != "https://my-app.deployment.instanode.dev" {
		t.Errorf("app_url = %q; want the live URL", params["app_url"])
	}
	if params["time_to_healthy_seconds"] != "31" {
		t.Errorf("time_to_healthy_seconds = %q; want \"31\"", params["time_to_healthy_seconds"])
	}
}

// TestEventEmail_BuildDeploySuccess_NoEmailReturnsFalse pins the fail-soft
// posture shared by every builder: no owner email → ok=false (the forwarder
// advances the cursor rather than sending to nobody).
func TestEventEmail_BuildDeploySuccess_NoEmailReturnsFalse(t *testing.T) {
	row := auditRow{Kind: auditKindDeployHealthy, OwnerEmail: ""}
	if _, ok := buildDeployHealthy(row); ok {
		t.Error("buildDeployHealthy with no owner email: ok=true; want false")
	}
	if _, ok := buildDeployCreated(row); ok {
		t.Error("buildDeployCreated with no owner email: ok=true; want false")
	}
}

// TestLifecycle_RenderDeployHealthy_SurfacesURL is the user-facing assertion:
// the "live" email must put the real app URL in the body (and use it as the
// CTA target), never a hardcoded/placeholder link.
func TestLifecycle_RenderDeployHealthy_SurfacesURL(t *testing.T) {
	const url = "https://my-app.deployment.instanode.dev"
	subject, html, text := renderDeployHealthy(map[string]string{
		"app_name":                "6fffcc21",
		"env":                     "production",
		"app_url":                 url,
		"time_to_healthy_seconds": "31",
	})
	// app_name is an opaque hex slug, so it must NOT appear in the subject as
	// a prose name (bug #23) — the subject is generic and the URL identifies
	// the app. The slug appears in the body only as a labeled identifier.
	if strings.Contains(subject, "6fffcc21") {
		t.Errorf("subject %q must not render the opaque app_id slug as a name", subject)
	}
	if !strings.Contains(html, url) {
		t.Errorf("html body should contain the live URL %q", url)
	}
	if !strings.Contains(html, "6fffcc21") {
		t.Errorf("html body should show the app_id as an identifier")
	}
	if !strings.Contains(text, url) {
		t.Errorf("text body should contain the live URL %q", url)
	}
}

// TestLifecycle_RenderDeployHealthy_NoURLFallsBackToDashboard guards the
// in-place-redeploy / missing-URL path: no app_url → CTA targets the
// dashboard instead of an empty href.
func TestLifecycle_RenderDeployHealthy_NoURLFallsBackToDashboard(t *testing.T) {
	_, html, _ := renderDeployHealthy(map[string]string{
		"app_name": "my-app",
		"env":      "production",
	})
	if !strings.Contains(html, dashboardURL) {
		t.Errorf("html body should fall back to the dashboard CTA when app_url is empty")
	}
}

// TestLifecycle_RenderDeployCreated_NoURLPromise pins that the "started"
// email does NOT fabricate a URL (it fires before the build, so none exists)
// and links to the dashboard.
func TestLifecycle_RenderDeployCreated_NoURLPromise(t *testing.T) {
	subject, html, _ := renderDeployCreated(map[string]string{
		"app_name": "6fffcc21",
		"env":      "production",
	})
	// Opaque slug must not appear as a prose name in the subject (bug #23).
	if strings.Contains(subject, "6fffcc21") {
		t.Errorf("subject %q must not render the opaque app_id slug as a name", subject)
	}
	if !strings.Contains(html, "6fffcc21") {
		t.Errorf("started email should still show the app_id as an identifier in the body")
	}
	if !strings.Contains(html, dashboardURL) {
		t.Errorf("started email should link to the dashboard")
	}
	if strings.Contains(html, "deployment.instanode.dev") {
		t.Errorf("started email must not invent a live URL before the build runs")
	}
}
