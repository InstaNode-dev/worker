package jobs

// orphan_sweep_canceler.go — the production OrphanSubscriptionCanceler.
//
// The orphan-sweep reconciler's PASS 2 cancels Razorpay subscriptions that
// are still live for a team that has been tombstoned. The worker already
// links the Razorpay Go SDK (billing_reconciler.go uses it for the
// subscription fetcher), so we add a thin Cancel wrapper here rather than
// routing a cancel call through the api over HTTP — fewer hops, and the
// reconciler is the system of record for "this subscription must die".
//
// IDEMPOTENCY
//
// Razorpay's cancel endpoint returns an error for a subscription that is
// already in a terminal state (cancelled / completed / expired). The
// reconciler must treat "already cancelled" as success or it would loop on
// the same orphan forever. razorpayOrphanCanceler.CancelSubscription
// inspects the error text and returns nil for the terminal-state cases —
// the same already-gone-is-success contract DeleteNamespace uses.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	razorpay "github.com/razorpay/razorpay-go"
)

// razorpaySubCancelClient is the subset of razorpay.Client the canceler
// needs. Narrow interface so tests inject a fake without an httptest
// server. Mirrors razorpaySDKClient in billing_reconciler.go.
type razorpaySubCancelClient interface {
	CancelSubscription(subID string, data map[string]interface{}, extraHeaders map[string]string) (map[string]interface{}, error)
}

// razorpaySubCancelAdapter wraps razorpay.Client to satisfy the interface.
type razorpaySubCancelAdapter struct{ c *razorpay.Client }

func (a *razorpaySubCancelAdapter) CancelSubscription(subID string, data map[string]interface{}, extraHeaders map[string]string) (map[string]interface{}, error) {
	return a.c.Subscription.Cancel(subID, data, extraHeaders)
}

// razorpayOrphanCanceler implements OrphanSubscriptionCanceler.
type razorpayOrphanCanceler struct {
	client razorpaySubCancelClient
}

// NewRazorpayOrphanCanceler constructs the canceler from RAZORPAY_KEY_ID /
// RAZORPAY_KEY_SECRET. Returns (nil, nil) when Razorpay is unconfigured so
// the caller can pass nil into the reconciler — PASS 2 is then skipped with
// a WARN. Same unconfigured contract as NewRazorpaySubFetcher.
//
// The underlying razorpay.Client has its HTTP timeout pinned to
// workerRazorpayHTTPTimeoutSeconds via SetTimeout — see that constant in
// billing_reconciler.go for the audit rationale (P1-6, P0-2 mirror).
func NewRazorpayOrphanCanceler() (OrphanSubscriptionCanceler, error) {
	keyID := os.Getenv("RAZORPAY_KEY_ID")
	keySecret := os.Getenv("RAZORPAY_KEY_SECRET")
	if keyID == "" || keySecret == "" {
		return nil, nil // unconfigured — reconciler skips PASS 2
	}
	c := razorpay.NewClient(keyID, keySecret)
	// Pin the SDK's HTTP client timeout explicitly. Same value the
	// billing-reconciler fetcher uses (30s) — see billing_reconciler.go.
	c.SetTimeout(workerRazorpayHTTPTimeoutSeconds)
	return &razorpayOrphanCanceler{client: &razorpaySubCancelAdapter{c: c}}, nil
}

// newRazorpayOrphanCancelerFromClient builds a canceler from a pre-built
// client. Used by tests to inject a fake.
func newRazorpayOrphanCancelerFromClient(c razorpaySubCancelClient) *razorpayOrphanCanceler {
	return &razorpayOrphanCanceler{client: c}
}

// CancelSubscription issues an immediate (cancel_at_cycle_end=0) cancel.
//
// A subscription already in a terminal state is treated as success — the
// reconciler's goal is "this subscription is not charging the card", and an
// already-cancelled subscription satisfies that. Without this, the orphan
// would be re-detected and re-attempted on every sweep forever.
//
// IDEMPOTENCY-KEY (audit P1-2 / worker-side mirror): the call carries an
// `Idempotency-Key` HTTP header deterministically derived from
// (subscription_id, "cancel"). River retries / next-tick re-sweeps that
// re-issue the same logical cancel reuse the same key so Razorpay handles a
// double-fire consistently — without it the second call may see a different
// transient state and the same orphan could oscillate between "in-flight
// cancel pending" and "cancel re-attempted". See razorpayCancelIdempotencyKey
// for the derivation.
func (rc *razorpayOrphanCanceler) CancelSubscription(_ context.Context, subscriptionID string) error {
	if strings.TrimSpace(subscriptionID) == "" {
		// Nothing to cancel — vacuously satisfied.
		return nil
	}
	headers := map[string]string{
		"Idempotency-Key": razorpayCancelIdempotencyKey(subscriptionID),
	}
	_, err := rc.client.CancelSubscription(subscriptionID,
		map[string]interface{}{"cancel_at_cycle_end": 0}, headers)
	if err == nil {
		return nil
	}
	if isRazorpayTerminalCancelError(err) {
		// Already cancelled / completed / expired — the money is already
		// stopped, which is exactly the post-condition we want.
		return nil
	}
	return fmt.Errorf("razorpayOrphanCanceler.CancelSubscription %q: %w", subscriptionID, err)
}

// razorpayCancelIdempotencyKey deterministically derives a UUID-formatted
// Idempotency-Key from (subscription_id, action). The same input always
// yields the same key, so a retry of the same logical cancel — by River
// retry or by the next orphan-sweep tick — is treated by Razorpay as a
// dedup of the original mutating call rather than a brand-new mutation.
//
// We use SHA-256(subscription_id|"cancel") truncated to 16 bytes, formatted
// as an RFC-4122 v5-shaped string. The key itself is opaque to Razorpay —
// it just needs to be a stable string per logical operation. Using a UUID
// shape keeps it inside Razorpay's accepted header format (≤128 chars) and
// matches how the api side constructs idempotency keys.
//
// Naming the action ("cancel") in the input means a future "halt" or
// "pause" verb on the same subscription_id gets a DIFFERENT key — preventing
// an unrelated mutation from being deduped against a cancel.
func razorpayCancelIdempotencyKey(subscriptionID string) string {
	return razorpayActionIdempotencyKey(subscriptionID, "cancel")
}

// razorpayActionIdempotencyKey is the shared per-action key derivation —
// extracted so future Razorpay mutating verbs (e.g. pause, resume, halt)
// can reuse the same shape. NOT exported; helpers in this package call it.
func razorpayActionIdempotencyKey(subscriptionID, action string) string {
	h := sha256.Sum256([]byte(subscriptionID + "|" + action))
	// Truncate to 16 bytes and shape as a UUID string. SHA-256 collision
	// resistance dominates the 128-bit truncation; this is comfortably more
	// than enough for an idempotency key.
	b := h[:16]
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]),
	)
}

// isRazorpayTerminalCancelError reports whether a Razorpay cancel error
// means the subscription is already in a non-charging terminal state.
// Razorpay returns a 400 with a message like "subscription is not in a
// valid state to perform this operation" / "...already been cancelled".
// We match on the substrings rather than parse the JSON body because the
// SDK surfaces the error as an opaque error value.
func isRazorpayTerminalCancelError(err error) bool {
	msg := strings.ToLower(err.Error())
	for _, frag := range []string{
		"already been cancelled",
		"already cancelled",
		"not in a valid state",
		"cannot be cancelled",
		"completed",
		"expired",
	} {
		if strings.Contains(msg, frag) {
			return true
		}
	}
	return false
}
