package jobs_test

// orphan_sweep_canceler_test.go — regression tests for the Razorpay
// Idempotency-Key fix from CIRCUIT-RETRY-AUDIT-2026-05-20 (worker brief item 2).
//
// The Razorpay cancel verb is a MUTATING call. River retries / next-tick
// re-sweeps that re-issue the same logical cancel must carry a stable
// Idempotency-Key header so Razorpay handles the duplicate consistently —
// without it, two cancel calls within the in-flight window can race the
// subscription's state machine and produce confusing results.
//
// These tests exercise the production razorpayOrphanCanceler via a fake
// SDK client (RazorpaySubCancelClient) that captures every (subID, data,
// headers) call. The fake doesn't talk to Razorpay; it just records what
// the SDK adapter would have sent.

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"instant.dev/worker/internal/jobs"
)

// fakeCancelSDK records every CancelSubscription call so the test can
// assert the headers that were passed.
type fakeCancelSDK struct {
	mu    sync.Mutex
	calls []struct {
		subID   string
		data    map[string]interface{}
		headers map[string]string
	}
	// err lets a test simulate an SDK failure.
	err error
}

func (f *fakeCancelSDK) CancelSubscription(subID string, data map[string]interface{}, headers map[string]string) (map[string]interface{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, struct {
		subID   string
		data    map[string]interface{}
		headers map[string]string
	}{subID, data, headers})
	if f.err != nil {
		return nil, f.err
	}
	return map[string]interface{}{"id": subID, "status": "cancelled"}, nil
}

// TestOrphanCanceler_PassesIdempotencyKeyHeader is the load-bearing regression
// from worker brief item 2 (CIRCUIT-RETRY-AUDIT-2026-05-20): the Razorpay
// cancel verb MUST carry an Idempotency-Key header so River retries don't
// double-trigger the Razorpay state machine.
func TestOrphanCanceler_PassesIdempotencyKeyHeader(t *testing.T) {
	sdk := &fakeCancelSDK{}
	canceler := jobs.NewRazorpayOrphanCancelerFromClient(sdk)

	if err := canceler.CancelSubscription(context.Background(), "sub_test_123"); err != nil {
		t.Fatalf("CancelSubscription: %v", err)
	}

	if len(sdk.calls) != 1 {
		t.Fatalf("want 1 SDK call, got %d", len(sdk.calls))
	}
	call := sdk.calls[0]
	if call.subID != "sub_test_123" {
		t.Errorf("subID: got %q, want %q", call.subID, "sub_test_123")
	}
	if call.headers == nil {
		t.Fatal("Idempotency-Key header must be set; got nil headers map")
	}
	key, ok := call.headers["Idempotency-Key"]
	if !ok {
		t.Fatalf("Idempotency-Key header missing; got headers=%v", call.headers)
	}
	if strings.TrimSpace(key) == "" {
		t.Fatal("Idempotency-Key must be non-empty")
	}
	// UUID-shaped: 8-4-4-4-12 hex.
	uuidShape := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	if !uuidShape.MatchString(key) {
		t.Errorf("Idempotency-Key %q does not match UUID shape", key)
	}
}

// TestOrphanCanceler_IdempotencyKeyIsDeterministic — the same subscription_id
// MUST yield the same key across invocations so River retries dedupe at
// Razorpay. This is the load-bearing property of the fix.
func TestOrphanCanceler_IdempotencyKeyIsDeterministic(t *testing.T) {
	const subID = "sub_deterministic_xyz"
	k1 := jobs.RazorpayCancelIdempotencyKey(subID)
	k2 := jobs.RazorpayCancelIdempotencyKey(subID)
	k3 := jobs.RazorpayCancelIdempotencyKey(subID)
	if k1 != k2 || k2 != k3 {
		t.Fatalf("Idempotency-Key not deterministic: %q vs %q vs %q", k1, k2, k3)
	}
}

// TestOrphanCanceler_DifferentSubsGetDifferentKeys — two distinct
// subscriptions MUST get distinct keys; otherwise Razorpay would treat
// unrelated cancels as duplicates.
func TestOrphanCanceler_DifferentSubsGetDifferentKeys(t *testing.T) {
	k1 := jobs.RazorpayCancelIdempotencyKey("sub_alpha")
	k2 := jobs.RazorpayCancelIdempotencyKey("sub_beta")
	if k1 == k2 {
		t.Fatalf("distinct subscription_ids must yield distinct keys; both were %q", k1)
	}
}

// TestOrphanCanceler_ReplayUsesSameKey — replaying CancelSubscription for the
// same subscription_id sends the SAME Idempotency-Key — the property that
// makes the fix load-bearing for River retries.
func TestOrphanCanceler_ReplayUsesSameKey(t *testing.T) {
	sdk := &fakeCancelSDK{}
	canceler := jobs.NewRazorpayOrphanCancelerFromClient(sdk)

	for i := 0; i < 3; i++ {
		if err := canceler.CancelSubscription(context.Background(), "sub_replay"); err != nil {
			t.Fatalf("retry %d: %v", i+1, err)
		}
	}
	if len(sdk.calls) != 3 {
		t.Fatalf("want 3 SDK calls, got %d", len(sdk.calls))
	}
	k0 := sdk.calls[0].headers["Idempotency-Key"]
	for i, c := range sdk.calls {
		if got := c.headers["Idempotency-Key"]; got != k0 {
			t.Errorf("call %d key %q != first-call key %q", i, got, k0)
		}
	}
}

// TestOrphanCanceler_EmptySubIDIsNoop — empty subscription_id is a vacuous
// success per the canceler's contract; no SDK call must be issued.
func TestOrphanCanceler_EmptySubIDIsNoop(t *testing.T) {
	sdk := &fakeCancelSDK{}
	canceler := jobs.NewRazorpayOrphanCancelerFromClient(sdk)
	if err := canceler.CancelSubscription(context.Background(), ""); err != nil {
		t.Fatalf("empty subID must be a vacuous success, got %v", err)
	}
	if err := canceler.CancelSubscription(context.Background(), "   "); err != nil {
		t.Fatalf("whitespace-only subID must be a vacuous success, got %v", err)
	}
	if len(sdk.calls) != 0 {
		t.Errorf("expected no SDK calls for empty/whitespace subID, got %d", len(sdk.calls))
	}
}

// TestOrphanCanceler_TerminalErrorIsSuccess — Razorpay returning "already
// cancelled" is treated as success (the post-condition is met). The
// idempotency-key path must not regress this.
func TestOrphanCanceler_TerminalErrorIsSuccess(t *testing.T) {
	sdk := &fakeCancelSDK{
		err: errors.New("BAD_REQUEST: subscription has already been cancelled"),
	}
	canceler := jobs.NewRazorpayOrphanCancelerFromClient(sdk)
	if err := canceler.CancelSubscription(context.Background(), "sub_already"); err != nil {
		t.Fatalf("already-cancelled error must be treated as success, got %v", err)
	}
}

// TestWorkerRazorpayHTTPTimeoutSeconds_Is30s pins the timeout constant the
// audit (P1-6) requires: 30s — explicitly set via SetTimeout — rather than
// the SDK default of 10s. A future tweak that drops the constant below
// 30s without an audit update should fail this test.
func TestWorkerRazorpayHTTPTimeoutSeconds_Is30s(t *testing.T) {
	got := jobs.WorkerRazorpayHTTPTimeoutSeconds()
	if got != 30 {
		t.Fatalf("workerRazorpayHTTPTimeoutSeconds: got %d, want 30 (audit P1-6 / brief item 1)", got)
	}
}

// TestNewRazorpayOrphanCanceler_AppliesSDKTimeout — when Razorpay creds are
// configured, the production constructor MUST install the 30s SDK timeout
// (audit P1-6 / brief item 1). Without this the reconciler could hang for
// the entire River job budget on a slow Razorpay.
//
// We construct the canceler, then reach in via reflection to confirm the
// SDK's underlying http.Client.Timeout is at least the configured value.
// The reflection is intentionally narrow — if the Razorpay SDK refactors,
// this test points the operator at the right place rather than silently
// passing through a fresh default.
func TestNewRazorpayOrphanCanceler_AppliesSDKTimeout(t *testing.T) {
	t.Setenv("RAZORPAY_KEY_ID", "rzp_test_timeout")
	t.Setenv("RAZORPAY_KEY_SECRET", "rzp_test_secret_timeout")

	c, err := jobs.NewRazorpayOrphanCanceler()
	if err != nil {
		t.Fatalf("NewRazorpayOrphanCanceler: %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil canceler when creds are set")
	}

	// Reach in via reflection to the underlying razorpay.Client's HTTP
	// timeout. The path is:
	//   razorpayOrphanCanceler.client (interface) → *razorpaySubCancelAdapter
	//   → adapter.c (*razorpay.Client) → c.Request.HTTPClient.Timeout.
	got := reachRazorpayHTTPTimeout(t, c)
	want := time.Duration(jobs.WorkerRazorpayHTTPTimeoutSeconds()) * time.Second
	if got != want {
		t.Fatalf("razorpay SDK HTTP timeout: got %s, want %s (constant pin)", got, want)
	}
}

// reachRazorpayHTTPTimeout walks the canceler → SDK client → http.Client
// chain via reflection. Localised so a future SDK refactor only affects
// this test, not the production code's contract with the audit.
func reachRazorpayHTTPTimeout(t *testing.T, c jobs.OrphanSubscriptionCanceler) time.Duration {
	t.Helper()
	v := reflect.ValueOf(c)
	// canceler is *razorpayOrphanCanceler — dereference.
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	clientFld := v.FieldByName("client")
	if !clientFld.IsValid() {
		t.Fatalf("canceler has no `client` field (SDK refactor?). type=%s", v.Type())
	}
	// clientFld is razorpaySubCancelClient (interface).
	adapter := clientFld.Elem()
	for adapter.Kind() == reflect.Ptr {
		adapter = adapter.Elem()
	}
	cFld := adapter.FieldByName("c")
	if !cFld.IsValid() {
		t.Fatalf("adapter has no `c` field. type=%s", adapter.Type())
	}
	// cFld is *razorpay.Client; deref then read Request.HTTPClient.Timeout.
	sdkClient := cFld
	for sdkClient.Kind() == reflect.Ptr {
		sdkClient = sdkClient.Elem()
	}
	reqFld := sdkClient.FieldByName("Request")
	if !reqFld.IsValid() {
		t.Fatalf("razorpay.Client has no `Request` field. type=%s", sdkClient.Type())
	}
	req := reqFld
	for req.Kind() == reflect.Ptr {
		req = req.Elem()
	}
	httpFld := req.FieldByName("HTTPClient")
	if !httpFld.IsValid() {
		t.Fatalf("requests.Request has no `HTTPClient`. type=%s", req.Type())
	}
	httpClient := httpFld
	for httpClient.Kind() == reflect.Ptr {
		httpClient = httpClient.Elem()
	}
	timeoutFld := httpClient.FieldByName("Timeout")
	if !timeoutFld.IsValid() {
		t.Fatalf("http.Client has no `Timeout` field. type=%s", httpClient.Type())
	}
	return time.Duration(timeoutFld.Int())
}
