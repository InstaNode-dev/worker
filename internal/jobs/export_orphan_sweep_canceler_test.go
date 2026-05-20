package jobs

// export_orphan_sweep_canceler_test.go — test-only exports for
// orphan_sweep_canceler.go + billing_reconciler.go internals related to
// the CIRCUIT-RETRY-AUDIT-2026-05-20 fixes.
//
// Only visible to _test.go files because the file ends in _test.go itself.

import "context"

// RazorpaySubCancelClient exports the internal razorpaySubCancelClient interface
// so tests can inject a fake Razorpay SDK without standing up the real client.
type RazorpaySubCancelClient = razorpaySubCancelClient

// NewRazorpayOrphanCancelerFromClient exports the internal constructor that
// wires a pre-built (faked) SDK client into a razorpayOrphanCanceler. Returns
// the OrphanSubscriptionCanceler interface so the test can drive
// CancelSubscription end-to-end with a deterministic mock.
func NewRazorpayOrphanCancelerFromClient(c RazorpaySubCancelClient) OrphanSubscriptionCanceler {
	return newRazorpayOrphanCancelerFromClient(c)
}

// RazorpayCancelIdempotencyKey exports the per-subscription idempotency-key
// derivation so the regression test can assert determinism + format directly.
func RazorpayCancelIdempotencyKey(subscriptionID string) string {
	return razorpayCancelIdempotencyKey(subscriptionID)
}

// WorkerRazorpayHTTPTimeoutSeconds exports the SDK timeout constant so the
// regression test can pin it without re-declaring the magic number in the test.
func WorkerRazorpayHTTPTimeoutSeconds() int16 {
	return workerRazorpayHTTPTimeoutSeconds
}

// ReconcilerBreakerFilter exports the breaker error-filter helper so the
// regression test can verify which errors are suppressed.
func ReconcilerBreakerFilter(ctx context.Context, err error) error {
	return reconcilerBreakerFilter(ctx, err)
}

// ErrSubFetcherNotConfigured exports the not-configured sentinel so the
// breaker-filter regression test can assert it is suppressed.
var ErrSubFetcherNotConfigured = errSubFetcherNotConfigured
