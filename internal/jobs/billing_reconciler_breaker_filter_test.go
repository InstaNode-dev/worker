package jobs_test

// billing_reconciler_breaker_filter_test.go — regression tests for the
// reconciler-breaker error-filter from CIRCUIT-RETRY-AUDIT-2026-05-20
// (worker brief item 5, mirror of api P1-1).
//
// The reconciler's Razorpay circuit breaker only trips on consecutive
// SERVER trouble. Caller-driven cancellations (ctx.Done) and config
// sentinels (errReconcilerCircuitOpen / errSubFetcherNotConfigured) MUST
// NOT count toward tripping — otherwise a noisy local cancellation or a
// brief unconfigured boot window could self-inflict a 60s outage on the
// breaker.

import (
	"context"
	"errors"
	"testing"

	"instant.dev/worker/internal/jobs"
)

// TestReconcilerBreakerFilter_SuppressesContextCanceled — a caller-driven
// cancellation must not feed the breaker.
func TestReconcilerBreakerFilter_SuppressesContextCanceled(t *testing.T) {
	if got := jobs.ReconcilerBreakerFilter(context.Background(), context.Canceled); got != nil {
		t.Errorf("context.Canceled must be filtered to nil, got %v", got)
	}
}

// TestReconcilerBreakerFilter_SuppressesDeadlineExceeded — same, for
// deadline-exceeded.
func TestReconcilerBreakerFilter_SuppressesDeadlineExceeded(t *testing.T) {
	if got := jobs.ReconcilerBreakerFilter(context.Background(), context.DeadlineExceeded); got != nil {
		t.Errorf("context.DeadlineExceeded must be filtered to nil, got %v", got)
	}
}

// TestReconcilerBreakerFilter_SuppressesAnyErrorWhenCtxDone — if the
// fetcher returned a non-context error but the caller's ctx is already
// done, we treat the call as caller-abandoned and suppress.
func TestReconcilerBreakerFilter_SuppressesAnyErrorWhenCtxDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	otherErr := errors.New("razorpay: 503")
	if got := jobs.ReconcilerBreakerFilter(ctx, otherErr); got != nil {
		t.Errorf("any error must be filtered when ctx is done, got %v", got)
	}
}

// TestReconcilerBreakerFilter_SuppressesCircuitOpenSentinel — the
// breaker's own sentinel must not feed back into the breaker.
func TestReconcilerBreakerFilter_SuppressesCircuitOpenSentinel(t *testing.T) {
	if got := jobs.ReconcilerBreakerFilter(context.Background(), jobs.ErrReconcilerCircuitOpen); got != nil {
		t.Errorf("errReconcilerCircuitOpen must be filtered, got %v", got)
	}
}

// TestReconcilerBreakerFilter_SuppressesNotConfiguredSentinel — boot-time
// "Razorpay not configured" must not trip the breaker either.
func TestReconcilerBreakerFilter_SuppressesNotConfiguredSentinel(t *testing.T) {
	if got := jobs.ReconcilerBreakerFilter(context.Background(), jobs.ErrSubFetcherNotConfigured); got != nil {
		t.Errorf("errSubFetcherNotConfigured must be filtered, got %v", got)
	}
}

// TestReconcilerBreakerFilter_PassesRealRazorpayErrors — actual Razorpay
// transport / API errors MUST still be passed through; the breaker is the
// load-shedder of last resort and must trip on real upstream trouble.
func TestReconcilerBreakerFilter_PassesRealRazorpayErrors(t *testing.T) {
	cases := []error{
		errors.New("razorpay: 500 Internal Server Error"),
		errors.New("razorpay: 502 Bad Gateway"),
		errors.New("razorpay: dial tcp: i/o timeout"),
		errors.New("razorpaySubFetcher.Fetch: connection refused"),
	}
	for _, e := range cases {
		t.Run(e.Error(), func(t *testing.T) {
			got := jobs.ReconcilerBreakerFilter(context.Background(), e)
			if got == nil {
				t.Errorf("real Razorpay error %q must pass through, got nil", e)
			}
			if !errors.Is(got, e) {
				t.Errorf("filter must preserve the original error chain; got %v", got)
			}
		})
	}
}

// TestReconcilerBreakerFilter_NilIsNil — a nil error remains nil
// (success — resets the consecutive counter).
func TestReconcilerBreakerFilter_NilIsNil(t *testing.T) {
	if got := jobs.ReconcilerBreakerFilter(context.Background(), nil); got != nil {
		t.Errorf("nil in → nil out, got %v", got)
	}
}

// TestReconcilerBreakerFilter_WrappedContextCanceled — errors.Is climbs
// the wrap chain, so a Razorpay SDK that wraps context.Canceled still
// gets filtered.
func TestReconcilerBreakerFilter_WrappedContextCanceled(t *testing.T) {
	wrapped := errors.Join(errors.New("razorpaySubFetcher.Fetch"), context.Canceled)
	if got := jobs.ReconcilerBreakerFilter(context.Background(), wrapped); got != nil {
		t.Errorf("wrapped context.Canceled must be filtered, got %v", got)
	}
}
