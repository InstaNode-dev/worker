package jobs_test

// billing_reconciler_fetcher_test.go — unit tests for razorpaySubFetcher.
//
// These tests verify:
//
//  1. Razorpay response → reconcilerSubscriptionDetails mapping for each
//     status class (active, halted/grace, cancelled/terminal).
//  2. plan_id extraction from top-level "plan_id" field.
//  3. plan_id extraction from nested "plan" → "id" field (fetch-subscription format).
//  4. paid_count is extracted as int64 from float64 JSON number.
//  5. paid_count == 0 is preserved (not defaulted to a non-zero value).
//  6. Unknown/extra fields in the Razorpay response are silently ignored.
//  7. A Razorpay API error propagates as a non-nil error from FetchSubscriptionForReconciler.
//  8. NewRazorpaySubFetcher returns nil (and no error) when RAZORPAY_KEY_ID is unset,
//     so the caller correctly falls back to noopSubFetcher.
//  9. NewRazorpaySubFetcher returns a non-nil fetcher when both key env vars are set.
//
// The SDK HTTP layer is mocked via mockRazorpaySDKClient — no real Razorpay API
// calls are made.

import (
	"context"
	"errors"
	"os"
	"testing"

	"instant.dev/worker/internal/jobs"
)

// ── mock SDK client ───────────────────────────────────────────────────────────

// mockRazorpaySDKClient implements jobs.RazorpaySDKClient for tests.
// It drives FetchSubscription via a closure so each test case can control
// the returned payload and error independently.
type mockRazorpaySDKClient struct {
	fn func(subID string) (map[string]interface{}, error)
}

func (m *mockRazorpaySDKClient) FetchSubscription(subID string, _ map[string]interface{}, _ map[string]string) (map[string]interface{}, error) {
	return m.fn(subID)
}

// newMockClient is a shorthand for constructing a mockRazorpaySDKClient that
// always returns the provided payload.
func newMockClient(payload map[string]interface{}, err error) *mockRazorpaySDKClient {
	return &mockRazorpaySDKClient{
		fn: func(_ string) (map[string]interface{}, error) {
			return payload, err
		},
	}
}

// ── §1–6: Razorpay response → ReconcilerSubDetails mapping ───────────────────

// TestRazorpaySubFetcher_StatusMapping verifies that each Razorpay status
// string is preserved verbatim in details.Status for the three action classes.
func TestRazorpaySubFetcher_StatusMapping(t *testing.T) {
	cases := []struct {
		razorpayStatus string
		planID         string
		paidCount      float64
		wantStatus     string
		wantPlanID     string
		wantPaidCount  int64
	}{
		// rzpStatusClassActive
		{
			razorpayStatus: "active",
			planID:         "plan_pro_monthly",
			paidCount:      5,
			wantStatus:     "active",
			wantPlanID:     "plan_pro_monthly",
			wantPaidCount:  5,
		},
		{
			razorpayStatus: "authenticated",
			planID:         "plan_hobby_test",
			paidCount:      0,
			wantStatus:     "authenticated",
			wantPlanID:     "plan_hobby_test",
			wantPaidCount:  0,
		},
		// rzpStatusClassGrace
		{
			razorpayStatus: "halted",
			planID:         "plan_pro_monthly",
			paidCount:      3,
			wantStatus:     "halted",
			wantPlanID:     "plan_pro_monthly",
			wantPaidCount:  3,
		},
		{
			razorpayStatus: "paused",
			planID:         "",
			paidCount:      1,
			wantStatus:     "paused",
			wantPlanID:     "",
			wantPaidCount:  1,
		},
		// rzpStatusClassTerminal
		{
			razorpayStatus: "cancelled",
			planID:         "",
			paidCount:      7,
			wantStatus:     "cancelled",
			wantPlanID:     "",
			wantPaidCount:  7,
		},
		{
			razorpayStatus: "completed",
			planID:         "plan_team_monthly",
			paidCount:      24,
			wantStatus:     "completed",
			wantPlanID:     "plan_team_monthly",
			wantPaidCount:  24,
		},
		{
			razorpayStatus: "expired",
			planID:         "",
			paidCount:      0,
			wantStatus:     "expired",
			wantPlanID:     "",
			wantPaidCount:  0,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.razorpayStatus, func(t *testing.T) {
			payload := map[string]interface{}{
				"id":         "sub_test",
				"status":     tc.razorpayStatus,
				"plan_id":    tc.planID,
				"paid_count": tc.paidCount,
			}
			mockClient := newMockClient(payload, nil)
			fetcher := jobs.NewRazorpaySubFetcherFromClient(mockClient)

			got, err := fetcher.FetchSubscriptionForReconciler(context.Background(), "sub_test")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got == nil {
				t.Fatal("expected non-nil details, got nil")
			}
			if got.Status != tc.wantStatus {
				t.Errorf("Status: got %q, want %q", got.Status, tc.wantStatus)
			}
			if got.PlanID != tc.wantPlanID {
				t.Errorf("PlanID: got %q, want %q", got.PlanID, tc.wantPlanID)
			}
			if got.PaidCount != tc.wantPaidCount {
				t.Errorf("PaidCount: got %d, want %d", got.PaidCount, tc.wantPaidCount)
			}
		})
	}
}

// §3: plan_id from nested "plan" → "id" (the GET /v1/subscriptions/{id} response format).
func TestRazorpaySubFetcher_PlanIDFromNestedPlanObject(t *testing.T) {
	payload := map[string]interface{}{
		"id":     "sub_nested",
		"status": "active",
		// No top-level plan_id; plan_id is inside the plan object.
		"plan": map[string]interface{}{
			"id":       "plan_pro_nested",
			"interval": 1,
			"period":   "monthly",
		},
		"paid_count": float64(2),
	}
	mockClient := newMockClient(payload, nil)
	fetcher := jobs.NewRazorpaySubFetcherFromClient(mockClient)

	got, err := fetcher.FetchSubscriptionForReconciler(context.Background(), "sub_nested")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.PlanID != "plan_pro_nested" {
		t.Errorf("PlanID from nested plan object: got %q, want %q", got.PlanID, "plan_pro_nested")
	}
}

// §2: top-level plan_id takes precedence over nested plan.id when both present.
func TestRazorpaySubFetcher_TopLevelPlanIDTakesPrecedence(t *testing.T) {
	payload := map[string]interface{}{
		"id":      "sub_both",
		"status":  "active",
		"plan_id": "plan_top_level",
		"plan": map[string]interface{}{
			"id": "plan_nested_should_not_win",
		},
		"paid_count": float64(1),
	}
	mockClient := newMockClient(payload, nil)
	fetcher := jobs.NewRazorpaySubFetcherFromClient(mockClient)

	got, err := fetcher.FetchSubscriptionForReconciler(context.Background(), "sub_both")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.PlanID != "plan_top_level" {
		t.Errorf("PlanID: got %q, want top-level %q", got.PlanID, "plan_top_level")
	}
}

// §5: paid_count == 0 is preserved, not silently promoted to 1.
func TestRazorpaySubFetcher_ZeroPaidCountPreserved(t *testing.T) {
	payload := map[string]interface{}{
		"id":         "sub_zero",
		"status":     "cancelled",
		"plan_id":    "",
		"paid_count": float64(0),
	}
	mockClient := newMockClient(payload, nil)
	fetcher := jobs.NewRazorpaySubFetcherFromClient(mockClient)

	got, err := fetcher.FetchSubscriptionForReconciler(context.Background(), "sub_zero")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.PaidCount != 0 {
		t.Errorf("PaidCount: got %d, want 0", got.PaidCount)
	}
}

// §6: Extra/unknown fields in the Razorpay response are silently ignored.
func TestRazorpaySubFetcher_UnknownFieldsIgnored(t *testing.T) {
	payload := map[string]interface{}{
		"id":                    "sub_extra",
		"status":                "active",
		"plan_id":               "plan_test",
		"paid_count":            float64(1),
		"quantity":              1,
		"total_count":           12,
		"start_at":              1710000000,
		"end_at":                nil,
		"charge_at":             1712678400,
		"customer_id":           "cust_test",
		"cancel_at_cycle_end":   false,
		"completely_unknown_v9": "some_future_razorpay_field",
	}
	mockClient := newMockClient(payload, nil)
	fetcher := jobs.NewRazorpaySubFetcherFromClient(mockClient)

	got, err := fetcher.FetchSubscriptionForReconciler(context.Background(), "sub_extra")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != "active" {
		t.Errorf("Status: got %q, want %q", got.Status, "active")
	}
	if got.PlanID != "plan_test" {
		t.Errorf("PlanID: got %q, want %q", got.PlanID, "plan_test")
	}
	if got.PaidCount != 1 {
		t.Errorf("PaidCount: got %d, want 1", got.PaidCount)
	}
}

// §7: Razorpay API error propagates as non-nil error.
func TestRazorpaySubFetcher_APIError_Propagates(t *testing.T) {
	apiErr := errors.New("razorpay: 429 Too Many Requests")
	mockClient := newMockClient(nil, apiErr)
	fetcher := jobs.NewRazorpaySubFetcherFromClient(mockClient)

	_, err := fetcher.FetchSubscriptionForReconciler(context.Background(), "sub_err")
	if err == nil {
		t.Fatal("expected non-nil error from Razorpay API failure, got nil")
	}
}

// §8: NewRazorpaySubFetcher returns (nil, nil) when RAZORPAY_KEY_ID is unset.
// This is the "noop fallback" signal — the caller substitutes noopSubFetcher.
func TestNewRazorpaySubFetcher_Unconfigured_ReturnsNil(t *testing.T) {
	// Ensure both env vars are absent.
	t.Setenv("RAZORPAY_KEY_ID", "")
	os.Unsetenv("RAZORPAY_KEY_ID")
	t.Setenv("RAZORPAY_KEY_SECRET", "")
	os.Unsetenv("RAZORPAY_KEY_SECRET")

	fetcher, err := jobs.NewRazorpaySubFetcher()
	if err != nil {
		t.Fatalf("unexpected error when unconfigured: %v", err)
	}
	if fetcher != nil {
		t.Errorf("expected nil fetcher when unconfigured, got %T", fetcher)
	}
}

// §8b: NewRazorpaySubFetcher returns (nil, nil) when only KEY_ID is set but SECRET is missing.
func TestNewRazorpaySubFetcher_MissingSecret_ReturnsNil(t *testing.T) {
	t.Setenv("RAZORPAY_KEY_ID", "rzp_test_someid")
	t.Setenv("RAZORPAY_KEY_SECRET", "")
	os.Unsetenv("RAZORPAY_KEY_SECRET")

	fetcher, err := jobs.NewRazorpaySubFetcher()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fetcher != nil {
		t.Errorf("expected nil fetcher when SECRET is unset, got %T", fetcher)
	}
}

// §9: NewRazorpaySubFetcher returns a non-nil fetcher when both env vars are set.
// We don't make a real Razorpay call — just verify the returned object is non-nil.
func TestNewRazorpaySubFetcher_Configured_ReturnsNonNil(t *testing.T) {
	t.Setenv("RAZORPAY_KEY_ID", "rzp_test_key")
	t.Setenv("RAZORPAY_KEY_SECRET", "rzp_test_secret")

	fetcher, err := jobs.NewRazorpaySubFetcher()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fetcher == nil {
		t.Error("expected non-nil fetcher when RAZORPAY_KEY_ID+SECRET are set")
	}
}

// ── Context cancellation ──────────────────────────────────────────────────────

// TestRazorpaySubFetcher_CancelledContext_ReturnsError verifies the fetcher
// respects context cancellation and returns context.Canceled without hitting
// the (mock) SDK.
func TestRazorpaySubFetcher_CancelledContext_ReturnsError(t *testing.T) {
	// Mock that would succeed if called — but the context is already cancelled.
	mockClient := newMockClient(map[string]interface{}{
		"id": "sub_ctx", "status": "active", "plan_id": "plan_x", "paid_count": float64(1),
	}, nil)
	fetcher := jobs.NewRazorpaySubFetcherFromClient(mockClient)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	_, err := fetcher.FetchSubscriptionForReconciler(ctx, "sub_ctx")
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}
