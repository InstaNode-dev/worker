package jobs_test

// flow_synthetic_test.go — hermetic tests for FlowSyntheticWorker.
//
// Each test stands up an httptest.Server simulating the api surface the flow
// matrix probes (/healthz, /auth/me, /db/new, DELETE /api/v1/resources/:id),
// wires the worker against it + an sqlmock DB for the idempotent seed + audit
// rows, and asserts:
//   - the per-flow outcome metric is bumped with the right
//     (flow, actor, tier, layer, result) label tuple,
//   - the InstantFlowTest NR event is recorded (cohort=synthetic),
//   - resources created by a flow are reaped (cleanup ledger),
//   - the flag-off no-op writes NOTHING,
//   - the per-flow kill switch + JWT-mint failure degrade rather than page.
//
// Metric + event paths are exercised through fakes so the process-global Prom
// registry / a real NR app are never touched.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"instant.dev/common/analyticsevent"
	"instant.dev/worker/internal/jobs"
)

// errSeed is the synthetic DB error used to drive the seed-failure path.
var errSeed = errors.New("seed boom")

// ─── fakes ───────────────────────────────────────────────────────────────

// fakeFlowMetrics captures every IncOutcome / ObserveLatency / IncReaped call.
type fakeFlowMetrics struct {
	mu        sync.Mutex
	outcomes  []fakeFlowOutcome
	latencies []fakeFlowLatency
	reaps     []fakeFlowReap
}

type fakeFlowOutcome struct{ flow, actor, tier, layer, result string }
type fakeFlowLatency struct {
	flow, actor, tier, layer string
	d                        time.Duration
}
type fakeFlowReap struct{ flow, outcome string }

func (f *fakeFlowMetrics) IncOutcome(flow, actor, tier, layer, result string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.outcomes = append(f.outcomes, fakeFlowOutcome{flow, actor, tier, layer, result})
}

func (f *fakeFlowMetrics) ObserveLatency(flow, actor, tier, layer string, d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latencies = append(f.latencies, fakeFlowLatency{flow, actor, tier, layer, d})
}

func (f *fakeFlowMetrics) IncReaped(flow, outcome string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reaps = append(f.reaps, fakeFlowReap{flow, outcome})
}

// resultFor returns the recorded result for a flow (first match), or "".
func (f *fakeFlowMetrics) resultFor(flow string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, o := range f.outcomes {
		if o.flow == flow {
			return o.result
		}
	}
	return ""
}

func (f *fakeFlowMetrics) reapOutcomes() []fakeFlowReap {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeFlowReap, len(f.reaps))
	copy(out, f.reaps)
	return out
}

func (f *fakeFlowMetrics) outcomeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.outcomes)
}

// fakeFlowEmitter captures every Record call so a test can assert the
// InstantFlowTest event was pushed with the right attrs.
type fakeFlowEmitter struct {
	mu     sync.Mutex
	events []fakeFlowEvent
}

type fakeFlowEvent struct {
	eventType string
	attrs     map[string]any
}

func (e *fakeFlowEmitter) Record(_ context.Context, eventType string, attrs map[string]any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp := make(map[string]any, len(attrs))
	for k, v := range attrs {
		cp[k] = v
	}
	e.events = append(e.events, fakeFlowEvent{eventType, cp})
}

func (e *fakeFlowEmitter) Name() string { return "fake" }
func (e *fakeFlowEmitter) Close() error { return nil }
func (e *fakeFlowEmitter) count() int   { e.mu.Lock(); defer e.mu.Unlock(); return len(e.events) }
func (e *fakeFlowEmitter) flowEvents() []fakeFlowEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]fakeFlowEvent, len(e.events))
	copy(out, e.events)
	return out
}

// ─── shared wiring ─────────────────────────────────────────────────────────

// flowAPIServer simulates the api surface the matrix probes. The handler is
// configurable per-test via the *flowAPIState it closes over.
type flowAPIState struct {
	healthzStatus int
	healthzBody   string
	authMeStatus  int
	authMeBody    string
	dbNewStatus   int
	dbNewBody     string
	deleteStatus  int
}

func newFlowAPIServer(st *flowAPIState) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(st.healthzStatus)
		_, _ = w.Write([]byte(st.healthzBody))
	})
	mux.HandleFunc("/auth/me", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(st.authMeStatus)
		_, _ = w.Write([]byte(st.authMeBody))
	})
	mux.HandleFunc("/db/new", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(st.dbNewStatus)
		_, _ = w.Write([]byte(st.dbNewBody))
	})
	// DELETE /api/v1/resources/:id — any path under the prefix.
	mux.HandleFunc("/api/v1/resources/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(st.deleteStatus)
	})
	return httptest.NewServer(mux)
}

// happyFlowState returns a state where every leg passes.
func happyFlowState() *flowAPIState {
	return &flowAPIState{
		healthzStatus: http.StatusOK,
		healthzBody:   `{"commit_id":"abc1234"}`,
		authMeStatus:  http.StatusOK,
		authMeBody:    `{"email":"synthetic+flowtest@instanode.dev"}`,
		dbNewStatus:   http.StatusCreated,
		dbNewBody:     `{"ok":true,"id":"11111111-1111-4111-8111-111111111111","token":"22222222-2222-4222-8222-222222222222"}`,
		deleteStatus:  http.StatusOK,
	}
}

// enabledConfig wires a fully-enabled config against the test server with a
// real JWT secret so the mint + authed flows run.
func enabledConfig(srv *httptest.Server) jobs.FlowSyntheticConfig {
	return jobs.FlowSyntheticConfig{
		Enabled:   true,
		BaseURL:   srv.URL,
		JWTSecret: "test-jwt-secret-0123456789abcdef",
		Email:     "synthetic+flowtest@instanode.dev",
		Tier:      "free",
	}
}

// expectSeed sets the sqlmock expectations for one ensureSyntheticTeam call:
// BeginTx + 3 ExecContext + Commit. Call once per Work() invocation that seeds.
func expectSeed(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO teams`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO users`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE resources`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
}

// expectReapAudit sets the expectation for one synthetic.reaped audit insert.
func expectReapAudit(mock sqlmock.Sqlmock) {
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
}

// expectOrphanSweepEmpty sets the expectation for reapOrphans finding nothing.
func expectOrphanSweepEmpty(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT id::text\s+FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
}

// flowFixture bundles a worker wired against an sqlmock DB + httptest server,
// plus the metric + event capture fakes and the mock for setting expectations.
type flowFixture struct {
	w    *jobs.FlowSyntheticWorker
	fm   *fakeFlowMetrics
	fe   *fakeFlowEmitter
	mock sqlmock.Sqlmock
	done func()
}

// newFixture builds the full test harness with the fully-enabled config.
func newFixture(t *testing.T, srv *httptest.Server) *flowFixture {
	return newFixtureCfg(t, srv, enabledConfig(srv))
}

// newFixtureCfg builds the harness with a caller-supplied config (for the
// kill-switch / no-JWT-secret / flag-off cases).
func newFixtureCfg(t *testing.T, srv *httptest.Server, cfg jobs.FlowSyntheticConfig) *flowFixture {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	fm := &fakeFlowMetrics{}
	fe := &fakeFlowEmitter{}
	w := jobs.NewFlowSyntheticWorker(db, srv.Client(), fm, fe, cfg)
	return &flowFixture{
		w:    w,
		fm:   fm,
		fe:   fe,
		mock: mock,
		done: func() {
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet sqlmock expectations: %v", err)
			}
		},
	}
}

// run invokes Work once and fails on error.
func (f *flowFixture) run(t *testing.T) {
	t.Helper()
	if err := f.w.Work(context.Background(), fakeJob[jobs.FlowSyntheticArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// ─── tests ───────────────────────────────────────────────────────────────

// TestFlowSynthetic_Disabled_NoOp asserts the master flag off makes Work a pure
// no-op: no metric, no event, no DB query (the DoD habit — inert in prod).
func TestFlowSynthetic_Disabled_NoOp(t *testing.T) {
	srv := newFlowAPIServer(happyFlowState())
	defer srv.Close()

	cfg := enabledConfig(srv)
	cfg.Enabled = false // the flag under test
	f := newFixtureCfg(t, srv, cfg)
	// No mock expectations: a single DB call would fail "unexpected".
	defer f.done()

	f.run(t)

	if n := f.fm.outcomeCount(); n != 0 {
		t.Errorf("flag off: want 0 metric outcomes, got %d", n)
	}
	if n := f.fe.count(); n != 0 {
		t.Errorf("flag off: want 0 events, got %d", n)
	}
}

// TestFlowSynthetic_HappyPath_AllFlowsPass drives the full matrix against a
// healthy api: every flow is pass, the provision→reap round-trip reaps the
// resource (cleanup ledger), and one InstantFlowTest event is pushed per flow
// with cohort=synthetic + the commitId correlation.
func TestFlowSynthetic_HappyPath_AllFlowsPass(t *testing.T) {
	srv := newFlowAPIServer(happyFlowState())
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	expectReapAudit(f.mock) // the provision→reap leg's synthetic.reaped row
	expectOrphanSweepEmpty(f.mock)

	f.run(t)

	for _, flow := range []string{"healthz", "auth_me", "provision_reap"} {
		if got := f.fm.resultFor(flow); got != analyticsevent.ResultPass {
			t.Errorf("flow %s: want result=pass, got %q", flow, got)
		}
	}
	// The provision→reap flow reaped exactly one resource, nothing leaked.
	var reaped int
	for _, r := range f.fm.reapOutcomes() {
		if r.outcome == "reaped" {
			reaped++
		}
		if r.outcome == "leaked" {
			t.Errorf("unexpected leak: %+v", r)
		}
	}
	if reaped != 1 {
		t.Errorf("want 1 reaped resource, got %d", reaped)
	}
	// One InstantFlowTest event per flow, all cohort=synthetic + commitId.
	evs := f.fe.flowEvents()
	if len(evs) != 3 {
		t.Fatalf("want 3 events, got %d", len(evs))
	}
	for _, e := range evs {
		if e.eventType != analyticsevent.EventFlowTest {
			t.Errorf("event type: want %s, got %s", analyticsevent.EventFlowTest, e.eventType)
		}
		if e.attrs[analyticsevent.AttrCohort] != analyticsevent.CohortSynthetic {
			t.Errorf("event missing cohort=synthetic: %+v", e.attrs)
		}
		if e.attrs[analyticsevent.AttrCommitID] != "abc1234" {
			t.Errorf("event missing commitId correlation: %+v", e.attrs)
		}
	}
}

// TestFlowSynthetic_HealthzDown_FailsAndAudits asserts a 503 /healthz fails the
// healthz flow with an audit row + event result=fail. The authed flows still
// run (commitId just comes back empty).
func TestFlowSynthetic_HealthzDown_FailsAndAudits(t *testing.T) {
	st := happyFlowState()
	st.healthzStatus = http.StatusServiceUnavailable
	st.healthzBody = `{"error":"down"}`
	srv := newFlowAPIServer(st)
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	// healthz fail → audit row for flow_test_failed.
	f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1))
	expectReapAudit(f.mock) // provision→reap still reaps
	expectOrphanSweepEmpty(f.mock)

	f.run(t)

	if got := f.fm.resultFor("healthz"); got != analyticsevent.ResultFail {
		t.Errorf("healthz down: want fail, got %q", got)
	}
}

// TestFlowSynthetic_ProvisionReapLeak_Fails asserts a provision that succeeds
// but whose delete returns 500 records a leak (outcome=leaked) and fails the
// flow — the rule-24 promise-breach surface.
func TestFlowSynthetic_ProvisionReapLeak_Fails(t *testing.T) {
	st := happyFlowState()
	st.deleteStatus = http.StatusInternalServerError
	srv := newFlowAPIServer(st)
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	expectReapAudit(f.mock)                                                              // the leaked reap still writes a ledger row
	f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1)) // flow_test_failed for the leak
	expectOrphanSweepEmpty(f.mock)

	f.run(t)

	if got := f.fm.resultFor("provision_reap"); got != analyticsevent.ResultFail {
		t.Errorf("leak: want provision_reap=fail, got %q", got)
	}
	var leaked bool
	for _, r := range f.fm.reapOutcomes() {
		if r.outcome == "leaked" {
			leaked = true
		}
	}
	if !leaked {
		t.Error("want a leaked reap outcome recorded")
	}
}

// TestFlowSynthetic_ProvisionFails_NoReap asserts a 402/over-limit provision
// fails the flow and performs NO reap (nothing was created).
func TestFlowSynthetic_ProvisionFails_NoReap(t *testing.T) {
	st := happyFlowState()
	st.dbNewStatus = http.StatusPaymentRequired
	st.dbNewBody = `{"error":"over_limit"}`
	srv := newFlowAPIServer(st)
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1)) // provision fail audit
	expectOrphanSweepEmpty(f.mock)

	f.run(t)

	if got := f.fm.resultFor("provision_reap"); got != analyticsevent.ResultFail {
		t.Errorf("provision fail: want fail, got %q", got)
	}
	for _, r := range f.fm.reapOutcomes() {
		if r.flow == "provision_reap" {
			t.Errorf("no reap should occur when nothing was provisioned, got %+v", r)
		}
	}
}

// TestFlowSynthetic_PerFlowKillSwitch_Degrades asserts a flow in the
// FLOW_SYNTHETIC_DISABLED kill list is silenced as degraded (not run, not
// paging) while the others still run.
func TestFlowSynthetic_PerFlowKillSwitch_Degrades(t *testing.T) {
	srv := newFlowAPIServer(happyFlowState())
	defer srv.Close()

	cfg := enabledConfig(srv)
	cfg.DisabledFlows = []string{"auth_me"}
	f := newFixtureCfg(t, srv, cfg)
	defer f.done()
	expectSeed(f.mock)
	expectReapAudit(f.mock)
	expectOrphanSweepEmpty(f.mock)

	f.run(t)

	if got := f.fm.resultFor("auth_me"); got != "degraded" {
		t.Errorf("killed flow: want degraded, got %q", got)
	}
	if got := f.fm.resultFor("provision_reap"); got != analyticsevent.ResultPass {
		t.Errorf("non-killed flow should still run: got %q", got)
	}
}

// TestFlowSynthetic_NoJWTSecret_AuthedFlowsDegrade asserts an unset JWT secret
// degrades the authenticated flows (config drift, not an outage) while the anon
// healthz flow still passes.
func TestFlowSynthetic_NoJWTSecret_AuthedFlowsDegrade(t *testing.T) {
	srv := newFlowAPIServer(happyFlowState())
	defer srv.Close()

	cfg := enabledConfig(srv)
	cfg.JWTSecret = "" // the field under test
	f := newFixtureCfg(t, srv, cfg)
	defer f.done()
	expectSeed(f.mock) // seed still runs (DB present); only the mint fails
	expectOrphanSweepEmpty(f.mock)

	f.run(t)

	if got := f.fm.resultFor("healthz"); got != analyticsevent.ResultPass {
		t.Errorf("anon healthz should pass: got %q", got)
	}
	for _, flow := range []string{"auth_me", "provision_reap"} {
		if got := f.fm.resultFor(flow); got != "degraded" {
			t.Errorf("flow %s with no JWT: want degraded, got %q", flow, got)
		}
	}
}

// TestFlowSynthetic_OrphanSweep_ReapsBackstop asserts the reaper backstop sweeps
// a stale synthetic resource the inline reap missed, writing the cleanup ledger.
func TestFlowSynthetic_OrphanSweep_ReapsBackstop(t *testing.T) {
	srv := newFlowAPIServer(happyFlowState())
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	expectReapAudit(f.mock) // provision→reap inline
	// Orphan sweep finds one stale resource → UPDATE + ledger row.
	f.mock.ExpectQuery(`SELECT id::text\s+FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("99999999-9999-4999-8999-999999999999"))
	f.mock.ExpectExec(`UPDATE resources SET status = 'deleted'`).WillReturnResult(sqlmock.NewResult(0, 1))
	f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1)) // orphan ledger

	f.run(t)

	var orphanReaped bool
	for _, r := range f.fm.reapOutcomes() {
		if r.flow == "orphan_sweep" && r.outcome == "reaped" {
			orphanReaped = true
		}
	}
	if !orphanReaped {
		t.Error("want orphan_sweep reaped outcome from the backstop")
	}
}

// TestFlowSynthetic_SeedFails_AuthedFlowsDegrade asserts a DB seed failure
// degrades the authed flows (no team → no JWT mint) while anon healthz passes.
func TestFlowSynthetic_SeedFails_AuthedFlowsDegrade(t *testing.T) {
	srv := newFlowAPIServer(happyFlowState())
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	// Seed begins then the team insert errors → rollback, seedOK=false.
	f.mock.ExpectBegin()
	f.mock.ExpectExec(`INSERT INTO teams`).WillReturnError(errSeed)
	f.mock.ExpectRollback()
	expectOrphanSweepEmpty(f.mock)

	f.run(t)

	if got := f.fm.resultFor("healthz"); got != analyticsevent.ResultPass {
		t.Errorf("anon healthz should still pass on seed failure: got %q", got)
	}
	for _, flow := range []string{"auth_me", "provision_reap"} {
		if got := f.fm.resultFor(flow); got != "degraded" {
			t.Errorf("flow %s on seed failure: want degraded, got %q", flow, got)
		}
	}
}

// TestFlowSynthetic_HealthzBadBody_Fails covers the body-parse + missing-field
// branches on the anon healthz flow.
func TestFlowSynthetic_HealthzBadBody_Fails(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"unparseable", `not json`},
		{"missing_commit_id", `{"foo":"bar"}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := happyFlowState()
			st.healthzBody = c.body
			srv := newFlowAPIServer(st)
			defer srv.Close()

			f := newFixture(t, srv)
			defer f.done()
			expectSeed(f.mock)
			f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1)) // healthz fail
			expectReapAudit(f.mock)
			expectOrphanSweepEmpty(f.mock)

			f.run(t)
			if got := f.fm.resultFor("healthz"); got != analyticsevent.ResultFail {
				t.Errorf("%s: want healthz fail, got %q", c.name, got)
			}
		})
	}
}

// TestFlowSynthetic_AuthMeBadBody_Fails covers the auth_me unauthorized +
// missing-email branches.
func TestFlowSynthetic_AuthMeBadBody_Fails(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"unauthorized", http.StatusUnauthorized, `{"error":"bad token"}`},
		{"missing_email", http.StatusOK, `{"foo":"bar"}`},
		{"unparseable", http.StatusOK, `not json`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := happyFlowState()
			st.authMeStatus = c.status
			st.authMeBody = c.body
			srv := newFlowAPIServer(st)
			defer srv.Close()

			f := newFixture(t, srv)
			defer f.done()
			expectSeed(f.mock)
			f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1)) // auth_me fail
			expectReapAudit(f.mock)
			expectOrphanSweepEmpty(f.mock)

			f.run(t)
			if got := f.fm.resultFor("auth_me"); got != analyticsevent.ResultFail {
				t.Errorf("%s: want auth_me fail, got %q", c.name, got)
			}
		})
	}
}

// TestFlowSynthetic_ProvisionBadBody_Fails covers provisionDB's parse + missing
// id branches (a 201 with a malformed body is itself a regression).
func TestFlowSynthetic_ProvisionBadBody_Fails(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"unparseable", `not json`},
		{"missing_id", `{"ok":true}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := happyFlowState()
			st.dbNewBody = c.body
			srv := newFlowAPIServer(st)
			defer srv.Close()

			f := newFixture(t, srv)
			defer f.done()
			expectSeed(f.mock)
			f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnResult(sqlmock.NewResult(1, 1)) // provision fail
			expectOrphanSweepEmpty(f.mock)

			f.run(t)
			if got := f.fm.resultFor("provision_reap"); got != analyticsevent.ResultFail {
				t.Errorf("%s: want provision_reap fail, got %q", c.name, got)
			}
		})
	}
}

// TestFlowSynthetic_AllFlowsKilled covers flowActorForFlow for every flow id by
// killing all three — each becomes degraded with its actor class set.
func TestFlowSynthetic_AllFlowsKilled(t *testing.T) {
	srv := newFlowAPIServer(happyFlowState())
	defer srv.Close()

	cfg := enabledConfig(srv)
	cfg.DisabledFlows = []string{"healthz", "auth_me", "provision_reap"}
	f := newFixtureCfg(t, srv, cfg)
	defer f.done()
	expectSeed(f.mock)
	expectOrphanSweepEmpty(f.mock)

	f.run(t)

	for _, flow := range []string{"healthz", "auth_me", "provision_reap"} {
		if got := f.fm.resultFor(flow); got != "degraded" {
			t.Errorf("killed flow %s: want degraded, got %q", flow, got)
		}
	}
}

// TestFlowSynthetic_NilDB_FlowsStillRun asserts the anon healthz flow runs and
// passes even with a nil DB (fail-open) — seed/audit/reap-ledger are skipped but
// the matrix still reports. The authed flows degrade (no seed → no JWT).
func TestFlowSynthetic_NilDB_FlowsStillRun(t *testing.T) {
	srv := newFlowAPIServer(happyFlowState())
	defer srv.Close()

	fm := &fakeFlowMetrics{}
	fe := &fakeFlowEmitter{}
	w := jobs.NewFlowSyntheticWorker(nil, srv.Client(), fm, fe, enabledConfig(srv))
	if err := w.Work(context.Background(), fakeJob[jobs.FlowSyntheticArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if got := fm.resultFor("healthz"); got != analyticsevent.ResultPass {
		t.Errorf("nil db: anon healthz should pass, got %q", got)
	}
	if got := fm.resultFor("auth_me"); got != "degraded" {
		t.Errorf("nil db: auth_me should degrade, got %q", got)
	}
}

// TestFlowSyntheticConfig_Defaults asserts the Defaults() fill + base-URL trim.
func TestFlowSyntheticConfig_Defaults(t *testing.T) {
	got := jobs.FlowSyntheticConfig{}.Defaults()
	if got.BaseURL != "https://api.instanode.dev" {
		t.Errorf("BaseURL default: got %q", got.BaseURL)
	}
	if got.Email == "" || got.Tier == "" {
		t.Errorf("Email/Tier defaults unset: %+v", got)
	}
	trimmed := jobs.FlowSyntheticConfig{BaseURL: "https://x.dev/"}.Defaults()
	if trimmed.BaseURL != "https://x.dev" {
		t.Errorf("BaseURL trailing-slash trim: got %q", trimmed.BaseURL)
	}
}

// TestFlowSynthetic_NilHTTPClient_Default asserts NewFlowSyntheticWorker installs
// a default client when passed nil (the prod wiring path), and the disabled flag
// still no-ops cleanly without a real round-trip.
func TestFlowSynthetic_NilHTTPClient_Default(t *testing.T) {
	fm := &fakeFlowMetrics{}
	cfg := jobs.FlowSyntheticConfig{Enabled: false}
	w := jobs.NewFlowSyntheticWorker(nil, nil, fm, nil, cfg) // nil http client
	if err := w.Work(context.Background(), fakeJob[jobs.FlowSyntheticArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if n := fm.outcomeCount(); n != 0 {
		t.Errorf("disabled + nil client: want 0 outcomes, got %d", n)
	}
}

// TestFlowSynthetic_AuditInsertError_NonFatal asserts a failed audit insert on a
// flow failure does not crash the sweep (fail-open) — the metric/event still
// recorded.
func TestFlowSynthetic_AuditInsertError_NonFatal(t *testing.T) {
	st := happyFlowState()
	st.healthzStatus = http.StatusInternalServerError
	srv := newFlowAPIServer(st)
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	f.mock.ExpectExec(`INSERT INTO audit_log`).WillReturnError(errSeed) // audit write fails
	expectReapAudit(f.mock)
	expectOrphanSweepEmpty(f.mock)

	f.run(t) // must not panic / error
	if got := f.fm.resultFor("healthz"); got != analyticsevent.ResultFail {
		t.Errorf("audit-fail path: want healthz fail recorded, got %q", got)
	}
}

// TestFlowSynthetic_OrphanQueryError_NonFatal asserts a reaper-backstop query
// error degrades gracefully (logged, not fatal).
func TestFlowSynthetic_OrphanQueryError_NonFatal(t *testing.T) {
	srv := newFlowAPIServer(happyFlowState())
	defer srv.Close()

	f := newFixture(t, srv)
	defer f.done()
	expectSeed(f.mock)
	expectReapAudit(f.mock)
	f.mock.ExpectQuery(`SELECT id::text\s+FROM resources`).WillReturnError(errSeed)

	f.run(t) // must not panic
}

// TestFlowSyntheticArgs_Kind pins the River worker key.
func TestFlowSyntheticArgs_Kind(t *testing.T) {
	if got := (jobs.FlowSyntheticArgs{}).Kind(); got != "flow_synthetic" {
		t.Errorf("Kind: want flow_synthetic, got %q", got)
	}
}

// TestFlowSynthetic_PromMetrics exercises the production Prom metric impl so the
// real counters/histogram get registered + observed without a fake.
func TestFlowSynthetic_PromMetrics(t *testing.T) {
	m := jobs.FlowSyntheticPromMetrics{}
	// These bump the process-global Prom registry; just assert they don't panic.
	m.IncOutcome("healthz", "prober", "anonymous", "api", "pass")
	m.ObserveLatency("healthz", "prober", "anonymous", "api", 5*time.Millisecond)
	m.IncReaped("provision_reap", "reaped")
}

// TestSplitFlowSyntheticDisabled covers the comma-list parser.
func TestSplitFlowSyntheticDisabled(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"", 0},
		{"   ", 0},
		{"db_new", 1},
		{"db_new, deploy_new", 2},
		{"a,,b, ,c", 3},
	}
	for _, c := range cases {
		got := jobs.SplitFlowSyntheticDisabledForTest(c.in)
		if len(got) != c.want {
			t.Errorf("split(%q): want %d, got %d (%v)", c.in, c.want, len(got), got)
		}
	}
}

// TestValidateFlowSyntheticBaseURL covers the startup-time URL validator.
func TestValidateFlowSyntheticBaseURL(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
	}{
		{"", false},
		{"https://api.instanode.dev", false},
		{"http://localhost:8080", false},
		{"ftp://nope", true},
		{"://missing-scheme", true},
		{"https://", true},
	}
	for _, c := range cases {
		err := jobs.ValidateFlowSyntheticBaseURL(c.in)
		if (err != nil) != c.wantErr {
			t.Errorf("ValidateFlowSyntheticBaseURL(%q): err=%v wantErr=%v", c.in, err, c.wantErr)
		}
	}
}
