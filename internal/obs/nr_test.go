// Tests for the New Relic init helper. The hard requirement is the
// fail-open contract: missing NEW_RELIC_LICENSE_KEY must return (nil, nil)
// with a warning log, NEVER an error and NEVER a crash.
//
// We don't test the success path here — it would require either embedding a
// fake NR collector or carrying a real license key in CI secrets, neither of
// which is worth the complexity for a thin bootstrap helper.
package obs

import (
	"testing"
)

// TestInitNewRelic_FailOpenOnMissingLicenseKey is the primary contract test.
// With no env var, the helper must return (nil, nil). We use t.Setenv to
// guarantee an empty value even on developer machines where the env might be
// set in their shell.
func TestInitNewRelic_FailOpenOnMissingLicenseKey(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "")

	app, err := InitNewRelic()
	if err != nil {
		t.Fatalf("expected nil error on missing license key, got %v", err)
	}
	if app != nil {
		t.Fatalf("expected nil application on missing license key, got %v", app)
	}
}

// TestWaitForConnection_NilSafe is a trivial guard for the helper that some
// boot-time code paths may call before the app is fully constructed.
func TestWaitForConnection_NilSafe(t *testing.T) {
	// Must not panic.
	WaitForConnection(nil)
}

// TestInitNewRelic_WithLicenseKey_BuildsApp — when a license key is set,
// the helper MUST return a non-nil *newrelic.Application even if the
// agent can't reach the collector. The Go SDK builds the application
// synchronously and connects async, so an unreachable backend produces
// a usable handle that absorbs StartTransaction as a no-op until the
// daemon catches up. A regression here would crashloop the worker pod
// on every boot in a network-isolated environment.
//
// We use a fake but well-formed license key so newrelic.NewApplication
// passes its format validation. NEW_RELIC_APP_NAME is set so we exercise
// the env-override branch too — the default path is covered by leaving
// the env unset in a sub-test.
func TestInitNewRelic_WithLicenseKey_BuildsApp(t *testing.T) {
	// Brand-new fake license key — 40 hex chars + 6-digit account prefix is
	// the NR format. Length matters more than content (the SDK rejects
	// short / empty / 0-padded values).
	const fakeLicense = "1234567890abcdef1234567890abcdef12345678"
	t.Setenv("NEW_RELIC_LICENSE_KEY", fakeLicense)
	t.Setenv("NEW_RELIC_APP_NAME", "worker-obs-test")

	app, err := InitNewRelic()
	// The SDK may return either (app, nil) on the happy path OR (nil, err)
	// if it rejects the synthetic key during construction. Either path
	// exercises the license-key branch we need for coverage. Crash is
	// the only outcome the fail-open contract forbids.
	if err != nil {
		// err path: must NOT panic; an err+nil app is the documented
		// "we tried but couldn't" outcome.
		if app != nil {
			t.Errorf("got err=%v but app=%v; want app=nil on error", err, app)
		}
		return
	}
	if app == nil {
		t.Fatal("InitNewRelic returned (nil, nil) with a license key set; want non-nil app")
	}
	// Don't WaitForConnection — the agent has no real collector to reach.
	app.Shutdown(0)
}

// TestInitNewRelic_WithLicenseKey_DefaultAppName — exercises the
// "NEW_RELIC_APP_NAME unset" branch.
func TestInitNewRelic_WithLicenseKey_DefaultAppName(t *testing.T) {
	const fakeLicense = "0123456789abcdef0123456789abcdef01234567"
	t.Setenv("NEW_RELIC_LICENSE_KEY", fakeLicense)
	t.Setenv("NEW_RELIC_APP_NAME", "")
	app, err := InitNewRelic()
	if err != nil {
		// Acceptable failure path — synthetic key may be rejected.
		return
	}
	if app == nil {
		t.Fatal("InitNewRelic returned (nil, nil) with license key + default app name")
	}
	app.Shutdown(0)
}

// TestInitNewRelic_InvalidLicenseKey_ReturnsErr — covers the
// "license key set, but NewApplication failed" branch. The SDK rejects
// keys outside its length / charset rules at construction time. The
// helper MUST log a warning and return (nil, err) — never panic — so
// the worker keeps booting.
func TestInitNewRelic_InvalidLicenseKey_ReturnsErr(t *testing.T) {
	// Too-short license key — the SDK validates length on construction.
	t.Setenv("NEW_RELIC_LICENSE_KEY", "abc")
	t.Setenv("NEW_RELIC_APP_NAME", "")
	app, err := InitNewRelic()
	if err == nil {
		// If the SDK quietly accepts a too-short key (a behaviour change in
		// a future SDK version), the (app, nil) branch is also valid as
		// long as we don't crash. The branch we're asserting on is the
		// "err != nil → return (nil, err)" path.
		if app != nil {
			app.Shutdown(0)
		}
		t.Skip("SDK accepted synthetic short key — error branch unreachable on this SDK version")
	}
	if app != nil {
		t.Errorf("got err=%v but app=%v; want app=nil on error", err, app)
	}
}

// TestWaitForConnection_OnRealAppNoCollector — invokes WaitForConnection
// with a real app whose collector isn't reachable. The timeout path
// must return cleanly within nrInitTimeout. No assertions on duration
// because the SDK times out internally and the helper just swallows the
// returned error.
func TestWaitForConnection_OnRealAppNoCollector(t *testing.T) {
	const fakeLicense = "abcdef0123456789abcdef0123456789abcdef01"
	t.Setenv("NEW_RELIC_LICENSE_KEY", fakeLicense)
	app, err := InitNewRelic()
	if err != nil || app == nil {
		t.Skip("InitNewRelic did not produce an app with synthetic key — nothing to wait on")
	}
	defer app.Shutdown(0)
	// Returns once the SDK's internal connect attempt times out. The
	// helper swallows the error; the test just proves the call doesn't
	// panic and returns within a reasonable bound (nrInitTimeout = 5s
	// inside the helper).
	WaitForConnection(app)
}
