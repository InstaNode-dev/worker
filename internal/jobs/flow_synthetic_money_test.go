package jobs_test

// flow_synthetic_money_test.go — branch-coverage tests for the money/value
// journey legs (flow_synthetic_money.go): claim, deploy_status, checkout, and
// magic_link. The shared harness (newFlowAPIServer / newFixture / happyFlowState
// / expectForwarder*) lives in flow_synthetic_test.go; these tests drive the
// per-leg contract + error + degraded branches the happy-path sweep doesn't hit
// (headroom-tier deploy + reap, checkout 5xx, magic_link failure classification,
// the no-row-async path, build_request failures, the classifier test seam).

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"instant.dev/common/analyticsevent"
	"instant.dev/worker/internal/jobs"
)

// TestFlowMoney_DeployHeadroom_AcceptedThenReaped drives the headroom-tier
// branch of flowDeployStatus: /deploy/new returns 201 with an id → the leg
// reaps the created app via DELETE /api/v1/deployments/:id (cleanup ledger,
// rule 24) and passes. Covers reapDeployment's happy path.
func TestFlowMoney_DeployHeadroom_AcceptedThenReaped(t *testing.T) {
	st := happyFlowState()
	st.deployStatus = http.StatusCreated
	st.deployBody = `{"id":"dep-11111111-1111-4111-8111-111111111111"}`
	st.deployDelStat = http.StatusOK
	srv := newFlowAPIServer(st)
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	expectReapAudit(f.mock)                          // provision_reap inline reap
	expectForwarderClassification(f.mock, "success") // magic_link
	expectReapAudit(f.mock)                          // deploy_status reap ledger row
	expectOrphanSweepEmpty(f.mock)

	f.run(t)

	if got := f.fm.resultFor("deploy_status"); got != analyticsevent.ResultPass {
		t.Errorf("deploy headroom: want pass, got %q", got)
	}
	var deployReaped bool
	for _, r := range f.fm.reapOutcomes() {
		if r.flow == "deploy_status" && r.outcome == "reaped" {
			deployReaped = true
		}
	}
	if !deployReaped {
		t.Error("deploy headroom: want a deploy_status reaped outcome (cleanup ledger)")
	}
}

// TestFlowMoney_DeployReapLeak drives reapDeployment's leak branch: the deploy
// is accepted (201) but the DELETE returns 500 → the app leaked (recorded on
// the reap counter). The leg itself still passes (the gate/create contract held;
// the leak is surfaced separately, same posture as the provision reap).
func TestFlowMoney_DeployReapLeak(t *testing.T) {
	st := happyFlowState()
	st.deployStatus = http.StatusCreated
	st.deployBody = `{"id":"dep-22222222-2222-4222-8222-222222222222"}`
	st.deployDelStat = http.StatusInternalServerError // delete fails → leak
	srv := newFlowAPIServer(st)
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	expectReapAudit(f.mock)                          // provision_reap inline reap
	expectForwarderClassification(f.mock, "success") // magic_link
	expectReapAudit(f.mock)                          // deploy_status leaked ledger row
	expectOrphanSweepEmpty(f.mock)

	f.run(t)

	var deployLeaked bool
	for _, r := range f.fm.reapOutcomes() {
		if r.flow == "deploy_status" && r.outcome == "leaked" {
			deployLeaked = true
		}
	}
	if !deployLeaked {
		t.Error("deploy reap leak: want a deploy_status leaked outcome recorded")
	}
}

// TestFlowMoney_DeployReapHTTPError drives reapDeployment's transport-error
// branch: the deploy is accepted (201) but the deployment DELETE hijacks +
// closes the connection so httpCli.Do returns a transport error → leaked.
func TestFlowMoney_DeployReapHTTPError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"commit_id":"abc1234"}`))
	})
	mux.HandleFunc("/auth/me", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"email":"synthetic+flowtest@instanode.dev"}`))
	})
	mux.HandleFunc("/db/new", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"11111111-1111-4111-8111-111111111111"}`))
	})
	mux.HandleFunc("/api/v1/resources/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK) // provision_reap reaps cleanly
	})
	mux.HandleFunc("/claim/preview", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_token"}`))
	})
	mux.HandleFunc("/auth/email/start", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	mux.HandleFunc("/deploy/new", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"dep-33333333-3333-4333-8333-333333333333"}`))
	})
	mux.HandleFunc("/api/v1/billing/checkout", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"short_url":"https://rzp.example/x"}`))
	})
	// The deployment DELETE hijacks + closes → client sees a transport error.
	mux.HandleFunc("/api/v1/deployments/", func(w http.ResponseWriter, _ *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		conn, _, err := hj.Hijack()
		if err == nil {
			_ = conn.Close()
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f := newFixtureCfg(t, srv, enabledConfig(srv))
	defer f.done()
	expectSeed(f.mock)
	expectReapAudit(f.mock)                          // provision_reap inline reap
	expectForwarderClassification(f.mock, "success") // magic_link
	expectReapAudit(f.mock)                          // deploy_status leaked ledger row (transport error)
	expectOrphanSweepEmpty(f.mock)

	f.run(t)
	var deployLeaked bool
	for _, r := range f.fm.reapOutcomes() {
		if r.flow == "deploy_status" && r.outcome == "leaked" {
			deployLeaked = true
		}
	}
	if !deployLeaked {
		t.Error("deploy reap transport-error: want a deploy_status leaked outcome")
	}
}

// TestFlowMoney_Deploy4xxNon402 drives flowDeployStatus's "4xx other than 402"
// branch (e.g. a 400 missing-tarball on a headroom tier) → the gate/validation
// responded, non-5xx contract held → pass.
func TestFlowMoney_Deploy4xxNon402(t *testing.T) {
	st := happyFlowState()
	st.deployStatus = http.StatusBadRequest
	st.deployBody = `{"error":"missing_tarball"}`
	srv := newFlowAPIServer(st)
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	expectReapAudit(f.mock)
	expectForwarderClassification(f.mock, "success")
	expectOrphanSweepEmpty(f.mock)

	f.run(t)
	if got := f.fm.resultFor("deploy_status"); got != analyticsevent.ResultPass {
		t.Errorf("deploy 4xx-non-402: want pass (non-5xx contract held), got %q", got)
	}
}

// TestFlowMoney_Deploy5xxFails drives flowDeployStatus's 5xx fail branch (the
// deploy endpoint crashed) → fail + audit row.
func TestFlowMoney_Deploy5xxFails(t *testing.T) {
	st := happyFlowState()
	st.deployStatus = http.StatusInternalServerError
	st.deployBody = `{"error":"boom"}`
	srv := newFlowAPIServer(st)
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	expectReapAudit(f.mock)
	expectForwarderClassification(f.mock, "success")
	f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1)) // deploy_status fail
	expectOrphanSweepEmpty(f.mock)

	f.run(t)
	if got := f.fm.resultFor("deploy_status"); got != analyticsevent.ResultFail {
		t.Errorf("deploy 5xx: want fail, got %q", got)
	}
}

// TestFlowMoney_Checkout5xxFails drives flowCheckout's 5xx fail branch (the
// checkout handler crashed/regressed) → fail + audit row.
func TestFlowMoney_Checkout5xxFails(t *testing.T) {
	st := happyFlowState()
	st.checkoutStatus = http.StatusInternalServerError
	st.checkoutBody = `{"error":"panic"}`
	srv := newFlowAPIServer(st)
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	expectReapAudit(f.mock)
	expectForwarderClassification(f.mock, "success")
	f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1)) // checkout fail
	expectOrphanSweepEmpty(f.mock)

	f.run(t)
	if got := f.fm.resultFor("checkout"); got != analyticsevent.ResultFail {
		t.Errorf("checkout 5xx: want fail, got %q", got)
	}
}

// TestFlowMoney_CheckoutBlocked4xx asserts a Razorpay-blocked 402/409 (non-5xx)
// from checkout passes (contract-only — alive-but-blocked is acceptable).
func TestFlowMoney_CheckoutBlocked4xx(t *testing.T) {
	st := happyFlowState()
	st.checkoutStatus = http.StatusBadGateway // 502 is a non-5xx? no — 502 IS 5xx
	// Use a 402 to represent the operator-blocked-but-alive shape.
	st.checkoutStatus = http.StatusPaymentRequired
	st.checkoutBody = `{"error":"recurring_not_enabled"}`
	srv := newFlowAPIServer(st)
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	expectReapAudit(f.mock)
	expectForwarderClassification(f.mock, "success")
	expectOrphanSweepEmpty(f.mock)

	f.run(t)
	if got := f.fm.resultFor("checkout"); got != analyticsevent.ResultPass {
		t.Errorf("checkout blocked-4xx: want pass (alive-but-blocked), got %q", got)
	}
}

// TestFlowMoney_ClaimWrongError drives flowClaimPreview's "400 but unexpected
// error" branch: a 400 whose body is not invalid_token/missing_token is itself
// a contract regression → fail.
func TestFlowMoney_ClaimWrongError(t *testing.T) {
	st := happyFlowState()
	st.claimStatus = http.StatusBadRequest
	st.claimBody = `{"error":"some_other_thing"}`
	srv := newFlowAPIServer(st)
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1)) // claim fail
	expectReapAudit(f.mock)
	expectForwarderClassification(f.mock, "success")
	expectOrphanSweepEmpty(f.mock)

	f.run(t)
	if got := f.fm.resultFor("claim"); got != analyticsevent.ResultFail {
		t.Errorf("claim wrong-error: want fail, got %q", got)
	}
}

// TestFlowMoney_ClaimWrongStatus drives flowClaimPreview's non-400 branch (a
// 200 instead of the 400 contract means the claim path stopped validating).
func TestFlowMoney_ClaimWrongStatus(t *testing.T) {
	st := happyFlowState()
	st.claimStatus = http.StatusOK
	st.claimBody = `{"token_valid":true}`
	srv := newFlowAPIServer(st)
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1)) // claim fail
	expectReapAudit(f.mock)
	expectForwarderClassification(f.mock, "success")
	expectOrphanSweepEmpty(f.mock)

	f.run(t)
	if got := f.fm.resultFor("claim"); got != analyticsevent.ResultFail {
		t.Errorf("claim wrong-status: want fail, got %q", got)
	}
}

// TestFlowMoney_MagicLinkRejected drives flowMagicLink's failure-classification
// branch: the 202 succeeds but the forwarder_sent truth surface reports
// classification=rejected (the Brevo-unvalidated-sender reality) → fail. This is
// the rule-12 surface: a 202 is NOT a delivery, the ledger is.
func TestFlowMoney_MagicLinkRejected(t *testing.T) {
	srv := newFlowAPIServer(happyFlowState())
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	expectReapAudit(f.mock)
	expectForwarderClassification(f.mock, "rejected")                                    // the truth surface
	f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1)) // magic_link fail
	expectOrphanSweepEmpty(f.mock)

	f.run(t)
	if got := f.fm.resultFor("magic_link"); got != analyticsevent.ResultFail {
		t.Errorf("magic_link rejected: want fail (rule-12 truth surface), got %q", got)
	}
}

// TestFlowMoney_MagicLinkNoRow drives flowMagicLink's async-no-verdict branch:
// the 202 succeeds and there is no recent forwarder_sent row yet → the leg
// passes on the request-accepted state (the forwarder is async).
func TestFlowMoney_MagicLinkNoRow(t *testing.T) {
	srv := newFlowAPIServer(happyFlowState())
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	expectReapAudit(f.mock)
	expectForwarderNoRow(f.mock) // no recent send → no verdict
	expectOrphanSweepEmpty(f.mock)

	f.run(t)
	if got := f.fm.resultFor("magic_link"); got != analyticsevent.ResultPass {
		t.Errorf("magic_link no-row: want pass (async, request accepted), got %q", got)
	}
}

// TestFlowMoney_MagicLinkNon202 drives flowMagicLink's non-202 fail branch (the
// send-request path itself is broken). No forwarder read happens.
func TestFlowMoney_MagicLinkNon202(t *testing.T) {
	st := happyFlowState()
	st.emailStartStat = http.StatusInternalServerError
	st.emailStartBody = `{"error":"boom"}`
	srv := newFlowAPIServer(st)
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	expectReapAudit(f.mock)
	f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1)) // magic_link fail
	expectOrphanSweepEmpty(f.mock)

	f.run(t)
	if got := f.fm.resultFor("magic_link"); got != analyticsevent.ResultFail {
		t.Errorf("magic_link non-202: want fail, got %q", got)
	}
}

// TestFlowMoney_MagicLinkNoRowDegraded drives the no-row + over-budget branch:
// 202 accepted, no forwarder row, but latency over the (0) budget → degraded.
func TestFlowMoney_MagicLinkNoRowDegraded(t *testing.T) {
	srv := newFlowAPIServer(happyFlowState())
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	f.w.SetBudgetOverrideForTest(map[string]time.Duration{"magic_link": 0})
	expectSeed(f.mock)
	expectReapAudit(f.mock)
	expectForwarderNoRow(f.mock)
	expectOrphanSweepEmpty(f.mock)

	f.run(t)
	if got := f.fm.resultFor("magic_link"); got != "degraded" {
		t.Errorf("magic_link no-row degraded: want degraded, got %q", got)
	}
}

// TestFlowMoney_ClassifierSeam covers the exported classifier test seam +
// the healthy-vs-failure partition (case-insensitive + trimmed).
func TestFlowMoney_ClassifierSeam(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"rejected", true},
		{"  REJECTED  ", true}, // trimmed + case-insensitive
		{"bounced_hard", true},
		{"error", true},
		{"success", false},
		{"delivered", false},
		{"", false},
	}
	for _, c := range cases {
		if got := jobs.IsForwarderFailureClassificationForTest(c.in); got != c.want {
			t.Errorf("IsForwarderFailureClassification(%q): want %v, got %v", c.in, c.want, got)
		}
	}
}
