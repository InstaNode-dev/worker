package jobs_test

// payment_probe_test.go — hermetic tests for the Layer-3 PaymentProbeWorker.
//
// Each test stands up an httptest.Server that simulates the api's payment-funnel
// surface (POST /api/v1/billing/checkout, GET /api/v1/billing[/invoices], POST
// /razorpay/webhook) for one scenario, then asserts:
//   - the per-leg outcome metric is bumped with the right (leg, result) label,
//   - an audit_log row is inserted on result=fail (and NOT on pass/degraded),
//   - the flag-off path probes NOTHING (zero probes, no HTTP, no DB),
//   - the optional upgrade leg mints → injects a signed test webhook → asserts
//     the tier flip (rule-12 truth surface) → reaps the cohort team.
//
// Metric emissions are captured through a fakePaymentProbeMetrics so the
// process-global Prom registry isn't polluted across tests. DB-touching paths
// (upgrade leg + audit) use go-sqlmock with the regexp query matcher. The sweep
// is driven via the shared fakeJob[T] helper (expire_test.go), the same way the
// flow_synthetic / deploy_probe suites drive Work.

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"instant.dev/worker/internal/jobs"
)

// ─── fake metrics ─────────────────────────────────────────────────────────────

type fakePaymentOutcome struct{ leg, result string }
type fakePaymentLatency struct {
	leg string
	d   time.Duration
}

// fakePaymentProbeMetrics captures every IncOutcome / ObserveLatency call.
type fakePaymentProbeMetrics struct {
	mu        sync.Mutex
	outcomes  []fakePaymentOutcome
	latencies []fakePaymentLatency
}

func (f *fakePaymentProbeMetrics) IncOutcome(leg, result string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.outcomes = append(f.outcomes, fakePaymentOutcome{leg, result})
}

func (f *fakePaymentProbeMetrics) ObserveLatency(leg string, d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latencies = append(f.latencies, fakePaymentLatency{leg, d})
}

// resultFor returns the recorded result for a leg ("" if the leg never emitted).
func (f *fakePaymentProbeMetrics) resultFor(leg string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, o := range f.outcomes {
		if o.leg == leg {
			return o.result
		}
	}
	return ""
}

func (f *fakePaymentProbeMetrics) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.outcomes)
}

// ─── api stub server ──────────────────────────────────────────────────────────

// paymentAPIState configures the stub api's per-endpoint responses for one
// scenario. The defaults (paymentHappyState) are the all-healthy contract.
type paymentAPIState struct {
	checkoutStatus int
	checkoutBody   string

	billingStatus int
	billingBody   string

	invoicesStatus int
	invoicesBody   string

	// webhook responses. A request carrying a non-empty X-Razorpay-Signature is
	// treated as "signed" (the upgrade leg) and gets webhookSignedResp. Any other
	// is "unsigned" (the security leg) and gets webhookUnsignedRsp/Bod.
	webhookSignedResp  int    // status for a signed webhook (upgrade leg)
	webhookUnsignedRsp int    // status for an unsigned/garbage webhook (security leg)
	webhookUnsignedBod string // body for the unsigned rejection (carries error_code)
}

func paymentHappyState() paymentAPIState {
	return paymentAPIState{
		checkoutStatus:     http.StatusOK,
		checkoutBody:       `{"short_url":"https://rzp.test/x"}`,
		billingStatus:      http.StatusOK,
		billingBody:        `{"plan_tier":"free"}`,
		invoicesStatus:     http.StatusOK,
		invoicesBody:       `{"invoices":[]}`,
		webhookSignedResp:  http.StatusOK,
		webhookUnsignedRsp: http.StatusBadRequest,
		webhookUnsignedBod: `{"ok":false,"error":"invalid_signature","message":"bad sig"}`,
	}
}

// newPaymentAPIServer builds the stub api server for a scenario. The webhook
// handler branches on the presence of an X-Razorpay-Signature header — so the
// upgrade leg's signed POST is accepted while the security leg's unsigned POST
// is rejected, exactly the real api's contract surface.
func newPaymentAPIServer(st paymentAPIState) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/billing/checkout", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(st.checkoutStatus)
		_, _ = w.Write([]byte(st.checkoutBody))
	})
	mux.HandleFunc("/api/v1/billing/invoices", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(st.invoicesStatus)
		_, _ = w.Write([]byte(st.invoicesBody))
	})
	mux.HandleFunc("/api/v1/billing", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(st.billingStatus)
		_, _ = w.Write([]byte(st.billingBody))
	})
	mux.HandleFunc("/razorpay/webhook", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Razorpay-Signature") != "" {
			w.WriteHeader(st.webhookSignedResp)
			_, _ = w.Write([]byte(`{"ok":true}`))
			return
		}
		w.WriteHeader(st.webhookUnsignedRsp)
		_, _ = w.Write([]byte(st.webhookUnsignedBod))
	})
	return httptest.NewServer(mux)
}

// ─── fixture ──────────────────────────────────────────────────────────────────

// paymentFixture bundles a worker wired against the stub server, its captured
// metrics, and (optionally) a sqlmock DB.
type paymentFixture struct {
	w    *jobs.PaymentProbeWorker
	fm   *fakePaymentProbeMetrics
	db   *sql.DB
	mock sqlmock.Sqlmock
}

// newPaymentFixtureNoDB wires a worker with a nil DB — exercises the prod-safe
// legs only (the upgrade leg degrades on the nil-DB guard). An empty JWTSecret
// degrades the authed legs on the JWT_SECRET-unset guard.
func newPaymentFixtureNoDB(t *testing.T, srv *httptest.Server, cfg jobs.PaymentProbeConfig) *paymentFixture {
	t.Helper()
	cfg.BaseURL = srv.URL
	cfg.Enabled = true
	fm := &fakePaymentProbeMetrics{}
	w := jobs.NewPaymentProbeWorker(nil, srv.Client(), fm, nil, cfg)
	return &paymentFixture{w: w, fm: fm}
}

// newPaymentFixtureDB wires a worker with a sqlmock DB (upgrade leg + audit).
func newPaymentFixtureDB(t *testing.T, srv *httptest.Server, cfg jobs.PaymentProbeConfig) *paymentFixture {
	t.Helper()
	cfg.BaseURL = srv.URL
	cfg.Enabled = true
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	fm := &fakePaymentProbeMetrics{}
	w := jobs.NewPaymentProbeWorker(db, srv.Client(), fm, nil, cfg)
	return &paymentFixture{w: w, fm: fm, db: db, mock: mock}
}

func (f *paymentFixture) done() {
	if f.db != nil {
		_ = f.db.Close()
	}
}

// run drives one full sweep via Work + the shared fakeJob helper.
func (f *paymentFixture) run(t *testing.T) {
	t.Helper()
	if err := f.w.Work(context.Background(), fakeJob[jobs.PaymentProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// ─── flag-off inert ───────────────────────────────────────────────────────────

// TestPaymentProbe_FlagOff_ProbesNothing is the headline DoD proof: with
// Enabled=false the worker no-ops — ZERO outcome emissions, no HTTP, no DB. This
// is the inert-in-prod guarantee until the operator lights PAYMENT_PROBE_ENABLED.
func TestPaymentProbe_FlagOff_ProbesNothing(t *testing.T) {
	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		hit = true
	}))
	defer srv.Close()

	fm := &fakePaymentProbeMetrics{}
	w := jobs.NewPaymentProbeWorker(nil, srv.Client(), fm, nil, jobs.PaymentProbeConfig{
		Enabled: false, // master flag OFF
		BaseURL: srv.URL,
	})
	if err := w.Work(context.Background(), fakeJob[jobs.PaymentProbeArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if fm.count() != 0 {
		t.Errorf("flag off: want 0 outcome emissions, got %d (%v)", fm.count(), fm.outcomes)
	}
	if hit {
		t.Error("flag off: prober made an HTTP request — must be fully inert")
	}
}

// ─── prod-safe legs: happy path ───────────────────────────────────────────────

// TestPaymentProbe_HappyPath_AuthedLegsPass drives all prod-safe legs green with
// a session JWT minted (JWT_SECRET set). checkout + billing + invoices +
// webhook_security all pass; the upgrade leg degrades (no test secret / no DB).
func TestPaymentProbe_HappyPath_AuthedLegsPass(t *testing.T) {
	srv := newPaymentAPIServer(paymentHappyState())
	defer srv.Close()

	f := newPaymentFixtureNoDB(t, srv, jobs.PaymentProbeConfig{
		JWTSecret: "test-jwt-secret-32-bytes-long-xxx",
	})
	f.run(t)

	for _, leg := range []string{"checkout_reachable", "billing_state", "invoices_reachable", "webhook_security"} {
		if got := f.fm.resultFor(leg); got != "pass" {
			t.Errorf("%s: want pass, got %q", leg, got)
		}
	}
	if got := f.fm.resultFor("upgrade_webhook_e2e"); got != "degraded" {
		t.Errorf("upgrade leg: want degraded (no db/secret), got %q", got)
	}
	if f.fm.count() != 5 {
		t.Errorf("want 5 leg outcomes, got %d (%v)", f.fm.count(), f.fm.outcomes)
	}
}

// TestPaymentProbe_NoJWTSecret_AuthedLegsDegrade proves the authed prod-safe
// legs degrade (config drift, never page) when JWT_SECRET is unset, while the
// no-auth webhook_security leg still runs and passes.
func TestPaymentProbe_NoJWTSecret_AuthedLegsDegrade(t *testing.T) {
	srv := newPaymentAPIServer(paymentHappyState())
	defer srv.Close()

	f := newPaymentFixtureNoDB(t, srv, jobs.PaymentProbeConfig{ /* no JWTSecret */ })
	f.run(t)

	for _, leg := range []string{"checkout_reachable", "billing_state", "invoices_reachable"} {
		if got := f.fm.resultFor(leg); got != "degraded" {
			t.Errorf("%s: want degraded (no JWT_SECRET), got %q", leg, got)
		}
	}
	if got := f.fm.resultFor("webhook_security"); got != "pass" {
		t.Errorf("webhook_security: want pass (no auth needed), got %q", got)
	}
}

// ─── checkout leg ─────────────────────────────────────────────────────────────

// TestPaymentProbe_Checkout5xx_Fails proves a 5xx from /billing/checkout fails
// the leg (a handler crash/regression) AND writes a fail audit row.
func TestPaymentProbe_Checkout5xx_Fails(t *testing.T) {
	st := paymentHappyState()
	st.checkoutStatus = http.StatusInternalServerError
	st.checkoutBody = `{"error":"boom"}`
	srv := newPaymentAPIServer(st)
	defer srv.Close()

	f := newPaymentFixtureDB(t, srv, jobs.PaymentProbeConfig{
		JWTSecret: "test-jwt-secret-32-bytes-long-xxx",
	})
	defer f.done()
	f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	// upgrade leg degrades (no test secret) → no further DB writes.

	f.run(t)

	if got := f.fm.resultFor("checkout_reachable"); got != "fail" {
		t.Errorf("checkout 5xx: want fail, got %q", got)
	}
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestPaymentProbe_CheckoutBlocked_Non5xx_Passes proves the "alive-but-blocked"
// shape (402 billing_not_configured) is a PASS — only a 5xx crash fails.
func TestPaymentProbe_CheckoutBlocked_Non5xx_Passes(t *testing.T) {
	st := paymentHappyState()
	st.checkoutStatus = http.StatusPaymentRequired
	st.checkoutBody = `{"error":"billing_not_configured"}`
	srv := newPaymentAPIServer(st)
	defer srv.Close()

	f := newPaymentFixtureNoDB(t, srv, jobs.PaymentProbeConfig{
		JWTSecret: "test-jwt-secret-32-bytes-long-xxx",
	})
	f.run(t)

	if got := f.fm.resultFor("checkout_reachable"); got != "pass" {
		t.Errorf("checkout 402 blocked-but-alive: want pass, got %q", got)
	}
}

// ─── billing / invoices legs ──────────────────────────────────────────────────

// TestPaymentProbe_BillingState5xx_Fails proves a 5xx on GET /api/v1/billing
// fails the billing_state leg.
func TestPaymentProbe_BillingState5xx_Fails(t *testing.T) {
	st := paymentHappyState()
	st.billingStatus = http.StatusServiceUnavailable
	srv := newPaymentAPIServer(st)
	defer srv.Close()

	f := newPaymentFixtureNoDB(t, srv, jobs.PaymentProbeConfig{
		JWTSecret: "test-jwt-secret-32-bytes-long-xxx",
	})
	f.run(t)

	if got := f.fm.resultFor("billing_state"); got != "fail" {
		t.Errorf("billing 503: want fail, got %q", got)
	}
}

// TestPaymentProbe_Invoices5xx_Fails proves a 5xx on GET /api/v1/billing/invoices
// fails the invoices_reachable leg.
func TestPaymentProbe_Invoices5xx_Fails(t *testing.T) {
	st := paymentHappyState()
	st.invoicesStatus = http.StatusInternalServerError
	srv := newPaymentAPIServer(st)
	defer srv.Close()

	f := newPaymentFixtureNoDB(t, srv, jobs.PaymentProbeConfig{
		JWTSecret: "test-jwt-secret-32-bytes-long-xxx",
	})
	f.run(t)

	if got := f.fm.resultFor("invoices_reachable"); got != "fail" {
		t.Errorf("invoices 500: want fail, got %q", got)
	}
}

// ─── webhook security leg ─────────────────────────────────────────────────────

// TestPaymentProbe_WebhookSecurity_RejectsUnsigned proves the security leg
// PASSES when the api rejects the unsigned garbage payload with 400
// invalid_signature (the positive proof the signature gate is live).
func TestPaymentProbe_WebhookSecurity_RejectsUnsigned(t *testing.T) {
	srv := newPaymentAPIServer(paymentHappyState())
	defer srv.Close()

	f := newPaymentFixtureNoDB(t, srv, jobs.PaymentProbeConfig{})
	f.run(t)

	if got := f.fm.resultFor("webhook_security"); got != "pass" {
		t.Errorf("webhook security (unsigned rejected): want pass, got %q", got)
	}
}

// TestPaymentProbe_WebhookSecurity_AcceptedUnsigned_Fails is the critical
// security-regression case: the api ACCEPTS an unsigned payload (2xx) → the leg
// FAILS loudly (the gate would let a forged success through).
func TestPaymentProbe_WebhookSecurity_AcceptedUnsigned_Fails(t *testing.T) {
	st := paymentHappyState()
	st.webhookUnsignedRsp = http.StatusOK // gate broken: accepts unsigned
	st.webhookUnsignedBod = `{"ok":true}`
	srv := newPaymentAPIServer(st)
	defer srv.Close()

	f := newPaymentFixtureDB(t, srv, jobs.PaymentProbeConfig{})
	defer f.done()
	f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1)) // security fail audit

	f.run(t)

	if got := f.fm.resultFor("webhook_security"); got != "fail" {
		t.Errorf("webhook security (unsigned ACCEPTED): want fail, got %q", got)
	}
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestPaymentProbe_WebhookSecurity_WrongErrorCode_Fails proves a 400 with an
// unexpected error_code (not invalid_signature) fails the leg — the gate must
// reject with the canonical contract code.
func TestPaymentProbe_WebhookSecurity_WrongErrorCode_Fails(t *testing.T) {
	st := paymentHappyState()
	st.webhookUnsignedBod = `{"ok":false,"error":"some_other_error"}`
	srv := newPaymentAPIServer(st)
	defer srv.Close()

	f := newPaymentFixtureNoDB(t, srv, jobs.PaymentProbeConfig{})
	f.run(t)

	if got := f.fm.resultFor("webhook_security"); got != "fail" {
		t.Errorf("webhook security (wrong error_code): want fail, got %q", got)
	}
}

// ─── upgrade leg (optional, test-mode) ────────────────────────────────────────

// upgradeConfig returns a config with the test webhook secret + plan id set so
// the upgrade leg runs.
func upgradeConfig() jobs.PaymentProbeConfig {
	return jobs.PaymentProbeConfig{
		JWTSecret:         "test-jwt-secret-32-bytes-long-xxx",
		TestWebhookSecret: "test-webhook-secret",
		TestPlanIDPro:     "plan_test_pro_xyz",
		Tier:              "free",
	}
}

// TestPaymentProbe_Upgrade_TierFlips_Passes drives the full upgrade proof: mint
// cohort → signed test webhook 200 → tier read-back returns "pro" → reap. The
// rule-12 truth surface (the tier flip), not the webhook 200, is the pass.
func TestPaymentProbe_Upgrade_TierFlips_Passes(t *testing.T) {
	srv := newPaymentAPIServer(paymentHappyState()) // webhook accepts signed → 200
	defer srv.Close()

	f := newPaymentFixtureDB(t, srv, upgradeConfig())
	defer f.done()
	// upgrade leg DB ops: mint team → read tier → reap.
	f.mock.ExpectExec(`INSERT INTO teams`).WillReturnResult(sqlmock.NewResult(0, 1))
	f.mock.ExpectQuery(`SELECT plan_tier FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"plan_tier"}).AddRow("pro"))
	f.mock.ExpectExec(`DELETE FROM teams`).WillReturnResult(sqlmock.NewResult(0, 1))

	f.run(t)

	if got := f.fm.resultFor("upgrade_webhook_e2e"); got != "pass" {
		t.Errorf("upgrade tier-flip: want pass, got %q", got)
	}
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestPaymentProbe_Upgrade_TierDoesNotFlip_Fails proves the rule-12 discipline:
// a webhook 200 with the tier STILL at free is a FAIL (the upgrade pipeline is
// broken even though the webhook was accepted). The cohort team is still reaped.
func TestPaymentProbe_Upgrade_TierDoesNotFlip_Fails(t *testing.T) {
	srv := newPaymentAPIServer(paymentHappyState())
	defer srv.Close()

	f := newPaymentFixtureDB(t, srv, upgradeConfig())
	defer f.done()
	// Execution order: mint team → read tier → (deferred) reap → record fail audit.
	// The deferred DELETE runs when legUpgradeWebhook returns, BEFORE record's
	// audit insert.
	f.mock.ExpectExec(`INSERT INTO teams`).WillReturnResult(sqlmock.NewResult(0, 1))
	f.mock.ExpectQuery(`SELECT plan_tier FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"plan_tier"}).AddRow("free")) // did NOT flip
	f.mock.ExpectExec(`DELETE FROM teams`).WillReturnResult(sqlmock.NewResult(0, 1))     // deferred reap
	f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1)) // fail audit

	f.run(t)

	if got := f.fm.resultFor("upgrade_webhook_e2e"); got != "fail" {
		t.Errorf("upgrade no-flip: want fail, got %q", got)
	}
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestPaymentProbe_Upgrade_WebhookNon200_Fails proves a non-200 from the signed
// test webhook fails the leg (the webhook path itself broke), and the cohort is
// still reaped.
func TestPaymentProbe_Upgrade_WebhookNon200_Fails(t *testing.T) {
	st := paymentHappyState()
	st.webhookSignedResp = http.StatusInternalServerError // signed webhook 500
	srv := newPaymentAPIServer(st)
	defer srv.Close()

	f := newPaymentFixtureDB(t, srv, upgradeConfig())
	defer f.done()
	// mint team → (webhook 500, no tier read) → (deferred) reap → record fail audit.
	f.mock.ExpectExec(`INSERT INTO teams`).WillReturnResult(sqlmock.NewResult(0, 1))
	f.mock.ExpectExec(`DELETE FROM teams`).WillReturnResult(sqlmock.NewResult(0, 1))     // deferred reap
	f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1)) // fail audit

	f.run(t)

	if got := f.fm.resultFor("upgrade_webhook_e2e"); got != "fail" {
		t.Errorf("upgrade webhook 500: want fail, got %q", got)
	}
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestPaymentProbe_Upgrade_NoTestSecret_Degrades proves the leg skips clean
// (degraded, never page) when the test webhook secret is unset — even with a DB.
func TestPaymentProbe_Upgrade_NoTestSecret_Degrades(t *testing.T) {
	srv := newPaymentAPIServer(paymentHappyState())
	defer srv.Close()

	f := newPaymentFixtureDB(t, srv, jobs.PaymentProbeConfig{
		JWTSecret: "test-jwt-secret-32-bytes-long-xxx",
		// no TestWebhookSecret
	})
	defer f.done()
	f.run(t)

	if got := f.fm.resultFor("upgrade_webhook_e2e"); got != "degraded" {
		t.Errorf("upgrade no-secret: want degraded, got %q", got)
	}
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations (should be none): %v", err)
	}
}

// TestPaymentProbe_Upgrade_NoTestPlanID_Degrades proves the leg degrades when
// the secret is set but the test plan id is missing (can't resolve a tier).
func TestPaymentProbe_Upgrade_NoTestPlanID_Degrades(t *testing.T) {
	srv := newPaymentAPIServer(paymentHappyState())
	defer srv.Close()

	f := newPaymentFixtureDB(t, srv, jobs.PaymentProbeConfig{
		JWTSecret:         "test-jwt-secret-32-bytes-long-xxx",
		TestWebhookSecret: "test-webhook-secret",
		// no TestPlanIDPro
	})
	defer f.done()
	f.run(t)

	if got := f.fm.resultFor("upgrade_webhook_e2e"); got != "degraded" {
		t.Errorf("upgrade no-plan: want degraded, got %q", got)
	}
}

// TestPaymentProbe_Upgrade_NoDB_Degrades proves the leg degrades when there is
// no DB (it cannot mint a cohort team), even with the test secret set.
func TestPaymentProbe_Upgrade_NoDB_Degrades(t *testing.T) {
	srv := newPaymentAPIServer(paymentHappyState())
	defer srv.Close()

	f := newPaymentFixtureNoDB(t, srv, upgradeConfig())
	f.run(t)

	if got := f.fm.resultFor("upgrade_webhook_e2e"); got != "degraded" {
		t.Errorf("upgrade no-db: want degraded, got %q", got)
	}
}

// ─── degraded latency ─────────────────────────────────────────────────────────

// TestPaymentProbe_DegradedLatency drives the over-budget branch via a 0-budget
// override: the prod-safe legs respond 200 but cross the (0) budget → degraded.
func TestPaymentProbe_DegradedLatency(t *testing.T) {
	srv := newPaymentAPIServer(paymentHappyState())
	defer srv.Close()

	f := newPaymentFixtureNoDB(t, srv, jobs.PaymentProbeConfig{
		JWTSecret: "test-jwt-secret-32-bytes-long-xxx",
	})
	f.w.SetBudgetOverrideForTest(map[string]time.Duration{
		"checkout_reachable": 0,
		"billing_state":      0,
		"invoices_reachable": 0,
		"webhook_security":   0,
	})
	f.run(t)

	if got := f.fm.resultFor("checkout_reachable"); got != "degraded" {
		t.Errorf("0-budget checkout: want degraded, got %q", got)
	}
	if got := f.fm.resultFor("webhook_security"); got != "degraded" {
		t.Errorf("0-budget webhook_security: want degraded, got %q", got)
	}
}

// ─── panic boundary ───────────────────────────────────────────────────────────

// TestPaymentProbe_PanicBoundary proves runLeg's recover() converts a panicking
// leg into result=fail rather than crashing the sweep. We force a panic by
// pointing the worker at a transport that panics on the first request. nil DB so
// no audit insert is attempted (the panic path's fail still emits the metric).
func TestPaymentProbe_PanicBoundary(t *testing.T) {
	srv := newPaymentAPIServer(paymentHappyState())
	defer srv.Close()

	fm := &fakePaymentProbeMetrics{}
	pw := jobs.NewPaymentProbeWorker(nil, &http.Client{Transport: panickingTransport{}}, fm, nil, jobs.PaymentProbeConfig{
		Enabled:   true,
		BaseURL:   srv.URL,
		JWTSecret: "test-jwt-secret-32-bytes-long-xxx",
	})
	if err := pw.Work(context.Background(), fakeJob[jobs.PaymentProbeArgs]()); err != nil {
		t.Fatalf("Work must not propagate a panic: %v", err)
	}
	if got := fm.resultFor("checkout_reachable"); got != "fail" {
		t.Errorf("panic boundary: checkout leg want fail, got %q", got)
	}
}

// ─── leg vocabulary + signer parity ───────────────────────────────────────────

// TestPaymentProbe_LegsVocabularyStable asserts the canonical leg-id set the
// dashboard grid + alert NRQL key on — a rename would red this (rule 16).
func TestPaymentProbe_LegsVocabularyStable(t *testing.T) {
	want := []string{
		"checkout_reachable",
		"billing_state",
		"invoices_reachable",
		"webhook_security",
		"upgrade_webhook_e2e",
	}
	got := jobs.PaymentProbeLegsForTest()
	if len(got) != len(want) {
		t.Fatalf("legs: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("leg[%d]: got %q, want %q", i, got[i], want[i])
		}
	}
}

// TestPaymentProbe_SignerParity asserts the prober's HMAC-SHA256 raw-body
// signing scheme produces a stable 64-hex digest (the api's verifyRazorpaySignature
// requires exactly 64 hex chars). Parity is the whole point — a drift here means
// the upgrade leg's signed webhook would be rejected as invalid_signature.
func TestPaymentProbe_SignerParity(t *testing.T) {
	sig := jobs.PaymentProbeSignForTest("secret", []byte(`{"a":1}`))
	if len(sig) != 64 {
		t.Errorf("signature length = %d, want 64 hex chars", len(sig))
	}
	if jobs.PaymentProbeSignForTest("secret", []byte(`{"a":1}`)) != sig {
		t.Error("signer is not deterministic")
	}
	if jobs.PaymentProbeSignForTest("other", []byte(`{"a":1}`)) == sig {
		t.Error("different secret must yield a different signature")
	}
}

// TestPaymentProbe_ValidateBaseURL covers the startup validator's branches.
func TestPaymentProbe_ValidateBaseURL(t *testing.T) {
	if err := jobs.ValidatePaymentProbeBaseURL(""); err != nil {
		t.Errorf("empty base url should be accepted, got %v", err)
	}
	if err := jobs.ValidatePaymentProbeBaseURL("https://api.instanode.dev"); err != nil {
		t.Errorf("valid https url rejected: %v", err)
	}
	if err := jobs.ValidatePaymentProbeBaseURL("ftp://nope"); err == nil {
		t.Error("non-http(s) scheme should be rejected")
	}
	if err := jobs.ValidatePaymentProbeBaseURL("http://"); err == nil {
		t.Error("host-less url should be rejected")
	}
	// A control character makes url.Parse itself error (the parse-error branch).
	if err := jobs.ValidatePaymentProbeBaseURL("http://exa\x7fmple"); err == nil {
		t.Error("unparseable url should be rejected")
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// panickingTransport panics on every RoundTrip — used to exercise runLeg's
// recover() boundary deterministically without a real slow/crashing server.
type panickingTransport struct{}

func (panickingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	panic("synthetic transport panic for the payment-probe recover() boundary test")
}
