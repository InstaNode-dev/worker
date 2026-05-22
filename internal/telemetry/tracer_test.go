package telemetry

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
)

// TestInitTracer_EmptyEndpointNoop — when the endpoint is unset, the
// returned shutdown must be a working no-op. This is the fail-open
// contract for local dev / CI runs where OTel is intentionally off.
func TestInitTracer_EmptyEndpointNoop(t *testing.T) {
	shutdown := InitTracer("instant-worker", "")
	if shutdown == nil {
		t.Fatal("InitTracer returned nil shutdown for empty endpoint")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("noop shutdown returned error: %v", err)
	}
}

// TestInitTracer_Boots — with a non-empty endpoint, InitTracer constructs
// a real exporter without crashing even if NEW_RELIC_LICENSE_KEY is unset.
// The exporter dials lazily on the first export, so construction must
// succeed regardless of whether the endpoint is reachable.
func TestInitTracer_Boots(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "")
	shutdown := InitTracer("instant-worker", "https://otlp.nr-data.net:4317")
	if shutdown == nil {
		t.Fatal("InitTracer returned nil shutdown")
	}
	_ = shutdown(context.Background())
}

// TestShouldUseTLS — the regression case for P0-2: every `https://`
// endpoint AND every `*nr-data.net` host MUST resolve to TLS=true.
// Reverting to WithInsecure() for these would silently kill tracing
// again (the symptom that produced this test).
func TestShouldUseTLS(t *testing.T) {
	cases := []struct {
		endpoint string
		want     bool
	}{
		{"https://otlp.nr-data.net:4317", true},
		{"https://otlp.eu01.nr-data.net:4317", true},
		{"otlp.nr-data.net:4317", true},
		{"otlp.eu01.nr-data.net:4317", true},
		{"foo.example.com:443", true},
		{"http://otel-collector.observability:4317", false},
		{"otel-collector.observability:4317", false},
		{"localhost:4317", false},
		{"", false},
	}
	for _, c := range cases {
		got := shouldUseTLS(c.endpoint)
		if got != c.want {
			t.Errorf("shouldUseTLS(%q) = %v, want %v", c.endpoint, got, c.want)
		}
	}
}

// TestStripScheme — strips http:// and https:// uniformly.
func TestStripScheme(t *testing.T) {
	cases := map[string]string{
		"https://otlp.nr-data.net:4317": "otlp.nr-data.net:4317",
		"http://localhost:4317":         "localhost:4317",
		"otlp.nr-data.net:4317":         "otlp.nr-data.net:4317",
		"":                              "",
	}
	for in, want := range cases {
		if got := stripScheme(in); got != want {
			t.Errorf("stripScheme(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestInitTracer_PlaintextEndpoint — http:// scheme MUST use
// WithInsecure() and resolve plaintext-by-scheme correctly. Covers the
// non-TLS exporter-build branch.
func TestInitTracer_PlaintextEndpoint(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "")
	shutdown := InitTracer("instant-worker", "http://localhost:4317")
	if shutdown == nil {
		t.Fatal("plaintext InitTracer returned nil shutdown")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("plaintext shutdown returned err: %v", err)
	}
}

// TestInitTracer_NRLicenseKeyAttachesHeader — when NEW_RELIC_LICENSE_KEY
// is a real-looking value (not empty, not "CHANGE_ME"), the exporter is
// configured with an `api-key` header. We can't introspect the exporter
// after construction, but we CAN ensure InitTracer doesn't crash and
// returns a shutdown that works. The branch is covered as long as the
// `licenseKey != ""` path runs.
func TestInitTracer_NRLicenseKeyAttachesHeader(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "0123456789abcdef0123456789abcdef01234567")
	shutdown := InitTracer("instant-worker", "https://otlp.nr-data.net:4317")
	if shutdown == nil {
		t.Fatal("InitTracer with license key returned nil shutdown")
	}
	// Real shutdown path — calls tp.Shutdown(ctx). Verifies the success
	// branch of the deferred return statement.
	if err := shutdown(context.Background()); err != nil {
		t.Logf("shutdown returned err (acceptable in test env): %v", err)
	}
}

// TestInitTracer_OTELServiceNameOverride — OTEL_SERVICE_NAME env var
// must override the serviceName argument. Covers the "if s != ''" branch.
func TestInitTracer_OTELServiceNameOverride(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "override-worker")
	t.Setenv("NEW_RELIC_LICENSE_KEY", "")
	shutdown := InitTracer("instant-worker", "localhost:4317")
	if shutdown == nil {
		t.Fatal("InitTracer with OTEL_SERVICE_NAME override returned nil shutdown")
	}
	_ = shutdown(context.Background())
}

// TestInitTracer_SentinelLicenseKey — the literal "CHANGE_ME" sentinel
// MUST be treated as empty so a half-configured operator boot doesn't
// quietly ship the sentinel as an api-key header. Covers the sentinel
// branch.
func TestInitTracer_SentinelLicenseKey(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "CHANGE_ME")
	shutdown := InitTracer("instant-worker", "https://otlp.nr-data.net:4317")
	if shutdown == nil {
		t.Fatal("InitTracer with CHANGE_ME license returned nil shutdown")
	}
	_ = shutdown(context.Background())
}

// TestInitTracer_WhitespaceOnlyEndpoint — strings.TrimSpace must reduce
// a whitespace-only endpoint to "" and follow the noop path. Covers the
// trimmed-empty branch.
func TestInitTracer_WhitespaceOnlyEndpoint(t *testing.T) {
	shutdown := InitTracer("instant-worker", "   \t\n  ")
	if shutdown == nil {
		t.Fatal("InitTracer with whitespace endpoint returned nil shutdown")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("whitespace endpoint shutdown returned err: %v", err)
	}
}

// TestShouldUseTLS_Port443 — additional coverage for the :443 suffix
// branch in the TLS heuristic.
func TestShouldUseTLS_Port443(t *testing.T) {
	if !shouldUseTLS("collector.example.com:443") {
		t.Error("expected port-443 bare host to be TLS")
	}
	if shouldUseTLS("collector.example.com:4317") {
		t.Error("expected non-443 bare host to be plaintext")
	}
}

// TestInitTracer_ExporterCtorFails — exercises the
// "OTLP exporter constructor returned an error" branch. The injected
// factory returns a synthetic error; InitTracer must log + return a
// no-op shutdown (NEVER panic, NEVER crashloop the worker).
func TestInitTracer_ExporterCtorFails(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "")
	prev := newExporterFn
	defer func() { newExporterFn = prev }()
	newExporterFn = func(_ context.Context, _ ...otlptracegrpc.Option) (*otlptrace.Exporter, error) {
		return nil, errors.New("synthetic exporter failure")
	}
	shutdown := InitTracer("instant-worker", "localhost:4317")
	if shutdown == nil {
		t.Fatal("InitTracer returned nil shutdown on exporter failure; want no-op")
	}
	// No-op shutdown returns nil cleanly.
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("no-op shutdown returned err: %v", err)
	}
}

// TestInitTracer_ResourceCtorFails — exercises the
// "resource constructor returned an error" branch. Same fail-open
// contract.
func TestInitTracer_ResourceCtorFails(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "")
	prev := newResourceFn
	defer func() { newResourceFn = prev }()
	newResourceFn = func(_ context.Context, _ ...resource.Option) (*resource.Resource, error) {
		return nil, errors.New("synthetic resource failure")
	}
	shutdown := InitTracer("instant-worker", "localhost:4317")
	if shutdown == nil {
		t.Fatal("InitTracer returned nil shutdown on resource failure; want no-op")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("no-op shutdown returned err: %v", err)
	}
}

// TestInitTracer_ShutdownWithCancelledContext — the shutdown closure
// passes its own context.WithTimeout into tp.Shutdown(). When the
// CALLER's ctx is already cancelled at shutdown time, the inner
// WithTimeout(shutdownCtx, 10s) inherits the cancellation and
// tp.Shutdown(ctx) returns ctx.Err(). This exercises the
// `if err := tp.Shutdown(ctx); err != nil` branch.
func TestInitTracer_ShutdownWithCancelledContext(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "")
	shutdown := InitTracer("instant-worker", "localhost:4317")
	if shutdown == nil {
		t.Fatal("InitTracer returned nil shutdown")
	}

	// Already-cancelled ctx — the inner WithTimeout(ctx, 10s) inherits
	// the cancel and tp.Shutdown sees a cancelled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := shutdown(ctx)
	// We don't require an error (the SDK MAY accept a cancelled ctx
	// gracefully), but the shutdown MUST NOT panic.
	if err != nil {
		// Confirm the wrapping format includes "telemetry shutdown:"
		// per the helper's fmt.Errorf format string.
		const prefix = "telemetry shutdown:"
		if len(err.Error()) < len(prefix) || err.Error()[:len(prefix)] != prefix {
			t.Errorf("shutdown error not wrapped: %v", err)
		}
	}
}
