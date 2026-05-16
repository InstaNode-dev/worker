package jobs

// export_billing_reconciler_test.go — test-only exports for
// billing_reconciler.go internals. These symbols are only accessible to tests
// in the jobs_test package (external test package). The file is compiled only
// when running `go test` because its name ends in _test.go.

// ReconcilerSubDetails exports reconcilerSubscriptionDetails for test stubs.
type ReconcilerSubDetails = reconcilerSubscriptionDetails

// BillingReconcilerStatusClass returns the action class string for a given
// Razorpay status, for use in table-driven mapping tests.
func BillingReconcilerStatusClass(status string) string {
	class, ok := razorpayStatusClass[status]
	if !ok {
		return "unknown"
	}
	switch class {
	case rzpStatusClassActive:
		return "active"
	case rzpStatusClassGrace:
		return "grace"
	case rzpStatusClassTerminal:
		return "terminal"
	case rzpStatusClassNoAction:
		return "no_action"
	default:
		return "unknown"
	}
}

// BillingReconcilerPlanIDToTier exports billingReconcilerPlanIDToTier for tests.
func BillingReconcilerPlanIDToTier(planID string) string {
	return billingReconcilerPlanIDToTier(planID)
}

// BillingTierRank exports billingTierRank for ordering tests.
func BillingTierRank(tier string) int {
	return billingTierRank(tier)
}

// ErrReconcilerCircuitOpen exports errReconcilerCircuitOpen for circuit-open tests.
var ErrReconcilerCircuitOpen = errReconcilerCircuitOpen

// RazorpaySDKClient exports the razorpaySDKClient interface for test injection.
type RazorpaySDKClient = razorpaySDKClient

// NewRazorpaySubFetcherFromClient exports the internal constructor that injects
// a mock SDK client. Used in fetcher unit tests to avoid hitting the real Razorpay API.
func NewRazorpaySubFetcherFromClient(c RazorpaySDKClient) subscriptionFetcher {
	return newRazorpaySubFetcherFromClient(c)
}
