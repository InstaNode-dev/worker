package jobs

// payment_probe_internal_test.go — white-box tests for unexported helpers in
// payment_probe.go that the black-box (jobs_test) package can't reach: the
// production PromMetrics impl, the clock seam, the default-client construction,
// the nil-db reap guard, and the per-leg http-error / build-error branches.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// TestPaymentProbe_PromMetrics_DoesNotPanic exercises the production
// PaymentProbePromMetrics impl (IncOutcome + ObserveLatency) — they write to the
// process-global Prom registry, so the assertion is simply "no panic / no
// duplicate-registration explosion".
func TestPaymentProbe_PromMetrics_DoesNotPanic(t *testing.T) {
	m := PaymentProbePromMetrics{}
	m.IncOutcome(paymentProbeLegCheckout, paymentProbeResultPass)
	m.ObserveLatency(paymentProbeLegCheckout, 42*time.Millisecond)
}

// TestPaymentProbe_Kind asserts the River worker key is stable (the periodic
// registration + UniqueOpts key on it).
func TestPaymentProbe_Kind(t *testing.T) {
	if got := (PaymentProbeArgs{}).Kind(); got != "payment_probe" {
		t.Errorf("Kind() = %q, want payment_probe", got)
	}
}

// TestPaymentProbe_EffectiveNowUnix covers the clock seam: the override fires
// when set, and the real clock fires when not.
func TestPaymentProbe_EffectiveNowUnix(t *testing.T) {
	w := &PaymentProbeWorker{}
	// Real clock path.
	if w.effectiveNowUnix() <= 0 {
		t.Error("effectiveNowUnix real clock should be positive")
	}
	// Seam path.
	w.SetNowUnixForTest(func() int64 { return 12345 })
	if got := w.effectiveNowUnix(); got != 12345 {
		t.Errorf("effectiveNowUnix seam = %d, want 12345", got)
	}
}

// TestPaymentProbe_NewWorker_DefaultClient covers the nil-httpCli branch in
// NewPaymentProbeWorker (installs a default client with the global timeout +
// refuse-redirect hook).
func TestPaymentProbe_NewWorker_DefaultClient(t *testing.T) {
	w := NewPaymentProbeWorker(nil, nil, PaymentProbePromMetrics{}, nil, PaymentProbeConfig{})
	if w.httpCli == nil {
		t.Fatal("nil httpCli should install a default client")
	}
	if w.httpCli.Timeout != paymentProbeHTTPTimeout {
		t.Errorf("default client timeout = %v, want %v", w.httpCli.Timeout, paymentProbeHTTPTimeout)
	}
	// The refuse-redirect hook returns http.ErrUseLastResponse.
	if err := w.httpCli.CheckRedirect(nil, nil); err != http.ErrUseLastResponse {
		t.Errorf("default client CheckRedirect = %v, want ErrUseLastResponse", err)
	}
}

// TestPaymentProbe_Defaults covers the Defaults() fill branches (empty base URL,
// email, tier → defaults; a set base URL is trimmed).
func TestPaymentProbe_Defaults(t *testing.T) {
	out := PaymentProbeConfig{BaseURL: "https://x.example/"}.Defaults()
	if out.BaseURL != "https://x.example" {
		t.Errorf("trailing slash not trimmed: %q", out.BaseURL)
	}
	if out.Email == "" || out.Tier == "" {
		t.Errorf("email/tier defaults not filled: email=%q tier=%q", out.Email, out.Tier)
	}
	full := PaymentProbeConfig{}.Defaults()
	if full.BaseURL != paymentProbeDefaultBaseURL {
		t.Errorf("empty base url default = %q, want %q", full.BaseURL, paymentProbeDefaultBaseURL)
	}
}

// TestPaymentProbe_ReapNilDB covers the db==nil guard in reapUpgradeCohortTeam
// (fail-open: no panic, no-op). Reachable directly only via the white-box seam.
func TestPaymentProbe_ReapNilDB(t *testing.T) {
	w := &PaymentProbeWorker{} // nil db
	w.reapUpgradeCohortTeam(context.Background(), "00000000-0000-0000-0000-000000000000", "run")
}

// TestPaymentProbe_LegCheckout_HTTPError covers the http_error branch of
// legCheckout (the http client returns an error — e.g. a closed server).
func TestPaymentProbe_LegCheckout_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Close() // immediately closed → the request errors

	w := NewPaymentProbeWorker(nil, srv.Client(), PaymentProbePromMetrics{}, nil, PaymentProbeConfig{
		BaseURL: srv.URL, Enabled: true,
	})
	r := w.legCheckout(context.Background(), "bearer")
	if r.result != paymentProbeResultFail {
		t.Errorf("legCheckout http error: want fail, got %q (%s)", r.result, r.reason)
	}
}

// TestPaymentProbe_LegGetReachable_HTTPError covers the http_error branch of the
// shared GET leg.
func TestPaymentProbe_LegGetReachable_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Close()

	w := NewPaymentProbeWorker(nil, srv.Client(), PaymentProbePromMetrics{}, nil, PaymentProbeConfig{
		BaseURL: srv.URL, Enabled: true,
	})
	r := w.legGetReachable(context.Background(), paymentProbeLegBillingState, "/api/v1/billing", "bearer")
	if r.result != paymentProbeResultFail {
		t.Errorf("legGetReachable http error: want fail, got %q (%s)", r.result, r.reason)
	}
}

// TestPaymentProbe_LegWebhookSecurity_HTTPError covers the http_error branch of
// the webhook-security leg.
func TestPaymentProbe_LegWebhookSecurity_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Close()

	w := NewPaymentProbeWorker(nil, srv.Client(), PaymentProbePromMetrics{}, nil, PaymentProbeConfig{
		BaseURL: srv.URL, Enabled: true,
	})
	r := w.legWebhookSecurity(context.Background())
	if r.result != paymentProbeResultFail {
		t.Errorf("legWebhookSecurity http error: want fail, got %q (%s)", r.result, r.reason)
	}
}

// TestPaymentProbe_Upgrade_MintFails_Degrades covers the mint-failure branch of
// legUpgradeWebhook: a DB error on the team INSERT degrades the leg (DB drift,
// not a payment outage) — never a page.
func TestPaymentProbe_Upgrade_MintFails_Degrades(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO teams`).WillReturnError(context.DeadlineExceeded)

	w := NewPaymentProbeWorker(db, http.DefaultClient, PaymentProbePromMetrics{}, nil, PaymentProbeConfig{
		Enabled:           true,
		TestWebhookSecret: "s",
		TestPlanIDPro:     "plan_test_pro",
		Tier:              "free",
	})
	r := w.legUpgradeWebhook(context.Background(), "run")
	if r.result != paymentProbeResultDegraded {
		t.Errorf("mint fail: want degraded, got %q (%s)", r.result, r.reason)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock: %v", err)
	}
}

// TestPaymentProbe_Upgrade_ReapFails covers the DELETE-error branch of
// reapUpgradeCohortTeam (logged, best-effort). Driven via the upgrade leg with a
// webhook that fails (so the leg returns before tier read) and a reap that errors.
func TestPaymentProbe_Upgrade_ReapFails(t *testing.T) {
	// A server whose webhook returns 500 so the leg fails fast after mint.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"ok":false}`))
	}))
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO teams`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM teams`).WillReturnError(context.DeadlineExceeded) // reap fails (logged)

	w := NewPaymentProbeWorker(db, srv.Client(), PaymentProbePromMetrics{}, nil, PaymentProbeConfig{
		Enabled:           true,
		BaseURL:           srv.URL,
		TestWebhookSecret: "s",
		TestPlanIDPro:     "plan_test_pro",
		Tier:              "free",
	})
	r := w.legUpgradeWebhook(context.Background(), "run")
	if r.result != paymentProbeResultFail {
		t.Errorf("webhook 500: want fail, got %q (%s)", r.result, r.reason)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock: %v", err)
	}
}

// TestPaymentProbe_Upgrade_TierReadFails covers the tier read-back error branch:
// a webhook 200 but the SELECT errors → fail.
func TestPaymentProbe_Upgrade_TierReadFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO teams`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT plan_tier FROM teams`).WillReturnError(context.DeadlineExceeded)
	mock.ExpectExec(`DELETE FROM teams`).WillReturnResult(sqlmock.NewResult(0, 1)) // deferred reap

	w := NewPaymentProbeWorker(db, srv.Client(), PaymentProbePromMetrics{}, nil, PaymentProbeConfig{
		Enabled:           true,
		BaseURL:           srv.URL,
		TestWebhookSecret: "s",
		TestPlanIDPro:     "plan_test_pro",
		Tier:              "free",
	})
	r := w.legUpgradeWebhook(context.Background(), "run")
	if r.result != paymentProbeResultFail {
		t.Errorf("tier read fail: want fail, got %q (%s)", r.result, r.reason)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock: %v", err)
	}
}

// TestPaymentProbe_Upgrade_DegradedLatency covers the over-budget branch of the
// upgrade leg: tier flips to pro but the (0) budget is crossed → degraded.
func TestPaymentProbe_Upgrade_DegradedLatency(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO teams`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT plan_tier FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"plan_tier"}).AddRow("pro"))
	mock.ExpectExec(`DELETE FROM teams`).WillReturnResult(sqlmock.NewResult(0, 1))

	w := NewPaymentProbeWorker(db, srv.Client(), PaymentProbePromMetrics{}, nil, PaymentProbeConfig{
		Enabled:           true,
		BaseURL:           srv.URL,
		TestWebhookSecret: "s",
		TestPlanIDPro:     "plan_test_pro",
		Tier:              "free",
	})
	w.SetBudgetOverrideForTest(map[string]time.Duration{paymentProbeLegUpgrade: 0})
	r := w.legUpgradeWebhook(context.Background(), "run")
	if r.result != paymentProbeResultDegraded {
		t.Errorf("upgrade 0-budget: want degraded, got %q (%s)", r.result, r.reason)
	}
}

// TestPaymentProbe_EmitFailed_AuditInsertErrors covers the audit-insert error
// branch of emitPaymentProbeFailed (the DB write fails → logged, fail-open).
func TestPaymentProbe_EmitFailed_AuditInsertErrors(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnError(context.DeadlineExceeded)

	w := NewPaymentProbeWorker(db, http.DefaultClient, PaymentProbePromMetrics{}, nil, PaymentProbeConfig{})
	w.emitPaymentProbeFailed(context.Background(), "run", paymentProbeLegResult{
		leg: paymentProbeLegCheckout, result: paymentProbeResultFail, reason: "boom",
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock: %v", err)
	}
}

// TestPaymentProbe_RunLeg_HardWallCap covers runLeg's hardWall cap branch: a
// budget larger than the global HTTP timeout is clamped to the global ceiling.
func TestPaymentProbe_RunLeg_HardWallCap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_signature"}`))
	}))
	defer srv.Close()

	w := NewPaymentProbeWorker(nil, srv.Client(), PaymentProbePromMetrics{}, nil, PaymentProbeConfig{
		Enabled: true, BaseURL: srv.URL,
	})
	// A budget > the global HTTP timeout → hardWall clamps to the global ceiling.
	w.SetBudgetOverrideForTest(map[string]time.Duration{
		paymentProbeLegWebhookSecurity: paymentProbeHTTPTimeout * 4,
	})
	got := w.runLeg(context.Background(), "run", paymentProbeLegWebhookSecurity, func(ctx context.Context) paymentProbeLegResult {
		return w.legWebhookSecurity(ctx)
	})
	if got != paymentProbeResultPass {
		t.Errorf("hardWall-cap leg: want pass, got %q", got)
	}
}

// TestPaymentProbe_RunLeg_FillsEmptyLeg covers runLeg's `if r.leg == ""` guard:
// a fn that returns a zero-leg result has its leg backfilled from the leg arg so
// the metric/event always carries a leg label.
func TestPaymentProbe_RunLeg_FillsEmptyLeg(t *testing.T) {
	fm := &capturingPayMetrics{}
	w := NewPaymentProbeWorker(nil, http.DefaultClient, fm, nil, PaymentProbeConfig{Enabled: true})
	got := w.runLeg(context.Background(), "run", paymentProbeLegCheckout, func(context.Context) paymentProbeLegResult {
		// Deliberately omit leg → the guard must backfill it.
		return paymentProbeLegResult{result: paymentProbeResultPass}
	})
	if got != paymentProbeResultPass {
		t.Errorf("runLeg result = %q, want pass", got)
	}
	if fm.lastLeg != paymentProbeLegCheckout {
		t.Errorf("empty leg not backfilled: metric leg = %q, want %q", fm.lastLeg, paymentProbeLegCheckout)
	}
}

// TestPaymentProbe_WebhookSecurity_Non400Status covers the webhook-security
// leg's "not 2xx, not 400" branch (e.g. a 500 from the webhook endpoint).
func TestPaymentProbe_WebhookSecurity_Non400Status(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable) // 503: not 2xx, not 400
		_, _ = w.Write([]byte(`{"error":"down"}`))
	}))
	defer srv.Close()

	w := NewPaymentProbeWorker(nil, srv.Client(), PaymentProbePromMetrics{}, nil, PaymentProbeConfig{
		Enabled: true, BaseURL: srv.URL,
	})
	r := w.legWebhookSecurity(context.Background())
	if r.result != paymentProbeResultFail {
		t.Errorf("webhook 503: want fail, got %q (%s)", r.result, r.reason)
	}
}

// TestPaymentProbe_Upgrade_WebhookTransportError covers the upgrade leg's
// "webhook http_error" branch (the signed-webhook POST transport fails — a
// closed server) AND postSignedWebhook's Do-error return. The cohort team is
// still reaped.
func TestPaymentProbe_Upgrade_WebhookTransportError(t *testing.T) {
	closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	base := closed.URL
	closed.Close() // the webhook POST now errors at the transport layer

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO teams`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM teams`).WillReturnResult(sqlmock.NewResult(0, 1)) // deferred reap
	// nil db on the worker would skip the audit; here db is set so the fail audit
	// also runs after the deferred reap.
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))

	w := NewPaymentProbeWorker(db, &http.Client{}, PaymentProbePromMetrics{}, nil, PaymentProbeConfig{
		Enabled:           true,
		BaseURL:           base,
		TestWebhookSecret: "s",
		TestPlanIDPro:     "plan_test_pro",
		Tier:              "free",
	})
	// Drive via runLeg so the deferred reap + the fail audit (record) both fire.
	got := w.runLeg(context.Background(), "run", paymentProbeLegUpgrade, func(ctx context.Context) paymentProbeLegResult {
		return w.legUpgradeWebhook(ctx, "run")
	})
	if got != paymentProbeResultFail {
		t.Errorf("upgrade webhook transport error: want fail, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock: %v", err)
	}
}

// capturingPayMetrics records the last (leg, result) for the empty-leg guard test.
type capturingPayMetrics struct {
	lastLeg, lastResult string
}

func (c *capturingPayMetrics) IncOutcome(leg, result string) { c.lastLeg, c.lastResult = leg, result }
func (c *capturingPayMetrics) ObserveLatency(string, time.Duration) {}

// TestPaymentProbe_SubscriptionChargedBody covers the body builder: it produces
// valid JSON carrying the team_id in notes, the plan_id, and the created_at.
func TestPaymentProbe_SubscriptionChargedBody(t *testing.T) {
	w := &PaymentProbeWorker{}
	body := w.subscriptionChargedBody("team-uuid", "sub_x", "plan_test_pro", 1700000000)
	s := string(body)
	for _, want := range []string{"team-uuid", "plan_test_pro", "subscription.charged", "1700000000"} {
		if !strings.Contains(s, want) {
			t.Errorf("subscriptionChargedBody missing %q in %s", want, s)
		}
	}
}
