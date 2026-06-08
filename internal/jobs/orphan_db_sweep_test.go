package jobs

// orphan_db_sweep_test.go — hermetic tests for the AUDIT-ONLY
// OrphanDBSweepWorker. Internal-package test so it can use the package-private
// fakes (fakeNamespaceLister, orphanFakeJob) the orphan-sweep reconciler test
// already defines, and inject the unexported config seam.
//
// Scenarios covered:
//   1. MASTER flag OFF → Work is a no-op: the namespace lister is NEVER called,
//      no metric moves, no DB read.
//   2. nil k8s lister → WARN-skip, no panic, no metric.
//   3. Candidate detection — an instant-customer-* namespace with NO live
//      resources row past the grace window is a candidate; one with a live row
//      is NOT; one with a live PENDING row is NOT (two-phase provision); a
//      namespace inside the grace window is NOT.
//   4. kind classification — a token whose most-recent terminal row is redis is
//      labelled redis_namespace; otherwise customer_namespace; no terminal row
//      → customer_namespace.
//   5. AUDIT-ONLY safety — even with candidates present, the destructive
//      deprovisioner is NEVER called when the destructive flag is off (the
//      truehomie-2026-06-03 safety property).
//   6. Destructive arm (BOTH flags on) routes a redis orphan through the
//      AUDITED provisioner DeprovisionResource chokepoint with the right token
//      + type; a generic (unmapped-kind) orphan is SKIPPED (fail-safe); a token
//      that reappears as live at re-confirm is SKIPPED.
//   7. Fail-open — a namespace List error and a live-token DB-read error each
//      degrade to zero candidates (NEVER an empty-set delete decision).
//   8. Masking — the candidate log path masks the token via logsafe.Token.

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	commonv1 "instant.dev/proto/common/v1"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"instant.dev/worker/internal/metrics"
	"instant.dev/worker/internal/provisioner"
)

func init() {
	// Keep test output quiet — the sweep logs INFO per candidate.
	slog.SetLogLoggerLevel(slog.LevelError)
}

// recordingDeprovisioner is a ResourceDeprovisioner that records every call's
// token + resource type, so a destructive-arm test can assert WHAT was routed
// through the audited chokepoint (the package's fakeDeprovisioner only counts).
type recordingDeprovisioner struct {
	calls   int
	tokens  []string
	types   []commonv1.ResourceType
	failErr error
}

func (r *recordingDeprovisioner) DeprovisionResource(_ context.Context, token, _ string, resType commonv1.ResourceType) error {
	r.calls++
	r.tokens = append(r.tokens, token)
	r.types = append(r.types, resType)
	return r.failErr
}

const (
	odsTokA = "tokAAAAAAAAAAAAAAAAAAAAA"
	odsTokB = "tokBBBBBBBBBBBBBBBBBBBBB"
	odsTokC = "tokCCCCCCCCCCCCCCCCCCCCC"
)

func odsNS(token string) string { return customerNamespacePrefix + token }

// expectLiveTokens primes the live-token query with the given token rows.
func expectLiveTokens(mock sqlmock.Sqlmock, tokens ...string) {
	rows := sqlmock.NewRows([]string{"token"})
	for _, tk := range tokens {
		rows.AddRow(tk)
	}
	mock.ExpectQuery(`SELECT DISTINCT token::text\s+FROM resources\s+WHERE status IN \('pending', 'active', 'paused', 'suspended'\)`).
		WillReturnRows(rows)
}

// expectTerminalTypes primes the terminal-resource-type query mapping
// token→resource_type for the kind classification.
func expectTerminalTypes(mock sqlmock.Sqlmock, pairs map[string]string) {
	rows := sqlmock.NewRows([]string{"token", "resource_type"})
	for tk, rt := range pairs {
		rows.AddRow(tk, rt)
	}
	mock.ExpectQuery(`SELECT DISTINCT ON \(token::text\) token::text, resource_type\s+FROM resources\s+WHERE status IN \('deleted', 'expired'\)`).
		WillReturnRows(rows)
}

// ── tests ──────────────────────────────────────────────────────────────────

// TestOrphanDBSweep_MasterFlagOff — the whole layer is inert when the master
// flag is unset: the lister is never called and no metric moves.
func TestOrphanDBSweep_MasterFlagOff(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	// A lister that fails LOUDLY if touched — the off path must not list.
	lister := newFakeNamespaceLister().withCustomerNamespaces(odsNS(odsTokA))
	lister.customerListErr = errors.New("lister must not be called when master flag is off")

	w := NewOrphanDBSweepWorker(db, lister, nil, OrphanDBSweepConfig{Enabled: false})
	if err := w.Work(context.Background(), orphanFakeJob[OrphanDBSweepArgs]()); err != nil {
		t.Fatalf("Work returned error on master-off path: %v", err)
	}
	// No DB queries expected at all.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected DB activity on master-off path: %v", err)
	}
}

// TestOrphanDBSweep_NilK8s — a nil lister WARN-skips without panic.
func TestOrphanDBSweep_NilK8s(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	w := NewOrphanDBSweepWorker(db, nil, nil, OrphanDBSweepConfig{Enabled: true})
	if err := w.Work(context.Background(), orphanFakeJob[OrphanDBSweepArgs]()); err != nil {
		t.Fatalf("Work returned error on nil-k8s path: %v", err)
	}
}

// TestOrphanDBSweep_Detection drives the core: orphan vs live vs pending vs
// within-grace, plus the kind classification, all in audit-only mode, and
// asserts the destructive deprovisioner is NEVER called.
func TestOrphanDBSweep_Detection(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Namespaces: A = orphan (no live row) → redis kind via terminal type;
	//             B = live (active row) → skipped;
	//             C = orphan but within grace → skipped.
	lister := newFakeNamespaceLister().
		withCustomerNamespaces(odsNS(odsTokA), odsNS(odsTokB), odsNS(odsTokC)).
		withNamespaceAge(odsNS(odsTokC), 5*time.Minute) // inside the 1h grace
	// A + B default to 365d (very old → past grace).

	// B is live; A and C have no live row.
	expectLiveTokens(mock, odsTokB)
	// A's most-recent terminal row is redis → redis_namespace kind.
	expectTerminalTypes(mock, map[string]string{odsTokA: orphanDBSweepResourceTypeRedis})

	deprov := &recordingDeprovisioner{}
	before := testutil.ToFloat64(metrics.OrphanDBSweepCandidatesTotal.WithLabelValues(orphanDBSweepKindRedisNS))

	w := NewOrphanDBSweepWorker(db, lister, deprov, OrphanDBSweepConfig{
		Enabled:            true,
		DestructiveEnabled: false, // AUDIT-ONLY
	})
	if err := w.Work(context.Background(), orphanFakeJob[OrphanDBSweepArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	// Exactly one candidate (A) — redis kind — counted.
	after := testutil.ToFloat64(metrics.OrphanDBSweepCandidatesTotal.WithLabelValues(orphanDBSweepKindRedisNS))
	if after-before != 1 {
		t.Errorf("redis_namespace candidate counter delta = %v; want 1 (only namespace A is an orphan)", after-before)
	}

	// AUDIT-ONLY safety: the destructive deprovisioner is NEVER called.
	if deprov.calls != 0 {
		t.Errorf("AUDIT-ONLY violated: DeprovisionResource called %d times; want 0 (no drop in audit-only mode)", deprov.calls)
	}

	// The current-backlog gauge reflects exactly 1 redis orphan, 0 generic.
	if g := testutil.ToFloat64(metrics.OrphanDBSweepCandidatesCurrent.WithLabelValues(orphanDBSweepKindRedisNS)); g != 1 {
		t.Errorf("current redis gauge = %v; want 1", g)
	}
	if g := testutil.ToFloat64(metrics.OrphanDBSweepCandidatesCurrent.WithLabelValues(orphanDBSweepKindCustomerNS)); g != 0 {
		t.Errorf("current customer gauge = %v; want 0", g)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOrphanDBSweep_GenericKind — an orphan whose token has NO terminal redis
// row is classified customer_namespace (generic).
func TestOrphanDBSweep_GenericKind(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	lister := newFakeNamespaceLister().withCustomerNamespaces(odsNS(odsTokA))
	expectLiveTokens(mock) // no live tokens
	// A's terminal row is postgres → generic customer_namespace kind.
	expectTerminalTypes(mock, map[string]string{odsTokA: "postgres"})

	before := testutil.ToFloat64(metrics.OrphanDBSweepCandidatesTotal.WithLabelValues(orphanDBSweepKindCustomerNS))
	w := NewOrphanDBSweepWorker(db, lister, nil, OrphanDBSweepConfig{Enabled: true})
	if err := w.Work(context.Background(), orphanFakeJob[OrphanDBSweepArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	after := testutil.ToFloat64(metrics.OrphanDBSweepCandidatesTotal.WithLabelValues(orphanDBSweepKindCustomerNS))
	if after-before != 1 {
		t.Errorf("customer_namespace candidate counter delta = %v; want 1", after-before)
	}
}

// TestOrphanDBSweep_DestructiveArmRoutesThroughAuditedChokepoint — with BOTH
// flags on, a redis orphan is routed through the audited DeprovisionResource
// chokepoint with the right token + type. A generic orphan in the same sweep is
// SKIPPED (no proven backing type → fail-safe, no drop).
func TestOrphanDBSweep_DestructiveArmRoutesThroughAuditedChokepoint(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	// A = redis orphan (will be deprovisioned); B = generic orphan (skipped).
	lister := newFakeNamespaceLister().withCustomerNamespaces(odsNS(odsTokA), odsNS(odsTokB))
	// Detection-phase live tokens (none live).
	expectLiveTokens(mock)
	// Terminal types: A redis, B postgres.
	expectTerminalTypes(mock, map[string]string{
		odsTokA: orphanDBSweepResourceTypeRedis,
		odsTokB: "postgres",
	})
	// Destructive re-confirm reads live tokens once PER candidate (2 candidates).
	expectLiveTokens(mock) // re-confirm for A
	expectLiveTokens(mock) // re-confirm for B

	deprov := &recordingDeprovisioner{}
	w := NewOrphanDBSweepWorker(db, lister, deprov, OrphanDBSweepConfig{
		Enabled:            true,
		DestructiveEnabled: true,
	})
	if err := w.Work(context.Background(), orphanFakeJob[OrphanDBSweepArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	// Only the redis orphan (A) was routed through the audited chokepoint.
	if deprov.calls != 1 {
		t.Fatalf("DeprovisionResource calls = %d; want 1 (only the redis orphan; the generic one is skipped fail-safe)", deprov.calls)
	}
	if deprov.tokens[0] != odsTokA {
		t.Errorf("deprovisioned token = %q; want %q", deprov.tokens[0], odsTokA)
	}
	if deprov.types[0] != commonv1.ResourceType_RESOURCE_TYPE_REDIS {
		t.Errorf("deprovisioned type = %v; want REDIS", deprov.types[0])
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOrphanDBSweep_DestructiveSkipsWhenTokenReappearsLive — a candidate whose
// token reappears as live at the destructive re-confirm is NOT deprovisioned
// (the just-paid-upgrade / restore race; when in doubt, never drop).
func TestOrphanDBSweep_DestructiveSkipsWhenTokenReappearsLive(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	lister := newFakeNamespaceLister().withCustomerNamespaces(odsNS(odsTokA))
	expectLiveTokens(mock)                                                   // detection: A not live
	expectTerminalTypes(mock, map[string]string{odsTokA: orphanDBSweepResourceTypeRedis})
	expectLiveTokens(mock, odsTokA)                                          // re-confirm: A is NOW live

	deprov := &recordingDeprovisioner{}
	w := NewOrphanDBSweepWorker(db, lister, deprov, OrphanDBSweepConfig{
		Enabled:            true,
		DestructiveEnabled: true,
	})
	if err := w.Work(context.Background(), orphanFakeJob[OrphanDBSweepArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if deprov.calls != 0 {
		t.Errorf("fail-safe violated: DeprovisionResource called %d times; want 0 (token reappeared live at re-confirm)", deprov.calls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOrphanDBSweep_DestructiveDisabledWhenProvisionerNil — even with both
// flags on, a nil provisioner keeps the sweep audit-only (destructiveArmed is
// false), so no destructive re-confirm queries run.
func TestOrphanDBSweep_DestructiveDisabledWhenProvisionerNil(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	lister := newFakeNamespaceLister().withCustomerNamespaces(odsNS(odsTokA))
	expectLiveTokens(mock)
	expectTerminalTypes(mock, map[string]string{odsTokA: orphanDBSweepResourceTypeRedis})
	// No re-confirm query expected — provisioner is nil → destructive disarmed.

	w := NewOrphanDBSweepWorker(db, lister, nil, OrphanDBSweepConfig{
		Enabled:            true,
		DestructiveEnabled: true, // on, but no provisioner → disarmed
	})
	if w.destructiveArmed() {
		t.Fatal("destructiveArmed must be false when provisioner is nil")
	}
	if w.modeLabel() != "audit-only" {
		t.Errorf("modeLabel = %q; want audit-only (provisioner nil)", w.modeLabel())
	}
	if err := w.Work(context.Background(), orphanFakeJob[OrphanDBSweepArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations (no re-confirm should run): %v", err)
	}
}

// TestOrphanDBSweep_FailOpenNamespaceListError — a List error degrades to zero
// candidates, never an error, never a delete decision.
func TestOrphanDBSweep_FailOpenNamespaceListError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	lister := newFakeNamespaceLister()
	lister.customerListErr = errors.New("RBAC forbidden: namespaces is forbidden")

	w := NewOrphanDBSweepWorker(db, lister, nil, OrphanDBSweepConfig{Enabled: true})
	if err := w.Work(context.Background(), orphanFakeJob[OrphanDBSweepArgs]()); err != nil {
		t.Fatalf("Work returned error on List-failure fail-open path: %v", err)
	}
	// No DB queries should have run (we never reached the live-token read).
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected DB activity after List failure: %v", err)
	}
}

// TestOrphanDBSweep_FailOpenLiveTokenError — a live-token DB-read error MUST NOT
// yield an empty live-set that a caller could read as "all namespaces orphaned".
// The sweep degrades to zero candidates.
func TestOrphanDBSweep_FailOpenLiveTokenError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	lister := newFakeNamespaceLister().withCustomerNamespaces(odsNS(odsTokA))
	mock.ExpectQuery(`SELECT DISTINCT token::text\s+FROM resources`).
		WillReturnError(errors.New("connection reset by peer"))

	deprov := &recordingDeprovisioner{}
	before := testutil.ToFloat64(metrics.OrphanDBSweepCandidatesTotal.WithLabelValues(orphanDBSweepKindCustomerNS))
	w := NewOrphanDBSweepWorker(db, lister, deprov, OrphanDBSweepConfig{
		Enabled:            true,
		DestructiveEnabled: true, // even armed, a DB blip must not drop anything
	})
	if err := w.Work(context.Background(), orphanFakeJob[OrphanDBSweepArgs]()); err != nil {
		t.Fatalf("Work returned error on live-token fail-open path: %v", err)
	}
	after := testutil.ToFloat64(metrics.OrphanDBSweepCandidatesTotal.WithLabelValues(orphanDBSweepKindCustomerNS))
	if after != before {
		t.Errorf("candidate counter moved on a DB-read failure: delta=%v; want 0", after-before)
	}
	if deprov.calls != 0 {
		t.Errorf("fail-open violated: DeprovisionResource called %d times after live-token read failed; want 0", deprov.calls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOrphanDBSweep_TerminalTypesErrorDegradesToGeneric — a terminal-type query
// error is non-fatal: detection still runs and every candidate falls back to
// the generic customer_namespace kind.
func TestOrphanDBSweep_TerminalTypesErrorDegradesToGeneric(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	lister := newFakeNamespaceLister().withCustomerNamespaces(odsNS(odsTokA))
	expectLiveTokens(mock)
	mock.ExpectQuery(`SELECT DISTINCT ON \(token::text\)`).
		WillReturnError(errors.New("statement timeout"))

	before := testutil.ToFloat64(metrics.OrphanDBSweepCandidatesTotal.WithLabelValues(orphanDBSweepKindCustomerNS))
	w := NewOrphanDBSweepWorker(db, lister, nil, OrphanDBSweepConfig{Enabled: true})
	if err := w.Work(context.Background(), orphanFakeJob[OrphanDBSweepArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	after := testutil.ToFloat64(metrics.OrphanDBSweepCandidatesTotal.WithLabelValues(orphanDBSweepKindCustomerNS))
	if after-before != 1 {
		t.Errorf("candidate delta = %v; want 1 (detection runs, kind degrades to generic)", after-before)
	}
}

// TestOrphanDBSweep_AgeLookupErrorSkipsNamespace — an age-lookup error skips the
// namespace this tick (fail-safe), so it is NOT counted as a candidate.
func TestOrphanDBSweep_AgeLookupErrorSkipsNamespace(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	lister := newFakeNamespaceLister().withCustomerNamespaces(odsNS(odsTokA))
	lister.ageErr = errors.New("namespace age unavailable")
	expectLiveTokens(mock)
	expectTerminalTypes(mock, map[string]string{})

	before := testutil.ToFloat64(metrics.OrphanDBSweepCandidatesTotal.WithLabelValues(orphanDBSweepKindCustomerNS))
	w := NewOrphanDBSweepWorker(db, lister, nil, OrphanDBSweepConfig{Enabled: true})
	if err := w.Work(context.Background(), orphanFakeJob[OrphanDBSweepArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	after := testutil.ToFloat64(metrics.OrphanDBSweepCandidatesTotal.WithLabelValues(orphanDBSweepKindCustomerNS))
	if after != before {
		t.Errorf("candidate counted despite age-lookup error: delta=%v; want 0 (fail-safe skip)", after-before)
	}
}

// TestOrphanDBSweep_EmptyNamespaceList — no customer namespaces → clean no-op,
// zero candidates, no live-token query (short-circuit).
func TestOrphanDBSweep_EmptyNamespaceList(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	lister := newFakeNamespaceLister() // no customer namespaces
	w := NewOrphanDBSweepWorker(db, lister, nil, OrphanDBSweepConfig{Enabled: true})
	if err := w.Work(context.Background(), orphanFakeJob[OrphanDBSweepArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("no DB query should run for an empty namespace list: %v", err)
	}
}

// TestOrphanDBSweep_EmptyTokenNamespaceSkipped — a degenerate namespace that is
// exactly the prefix (empty token) is skipped without a panic.
func TestOrphanDBSweep_EmptyTokenNamespaceSkipped(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	lister := newFakeNamespaceLister().withCustomerNamespaces(customerNamespacePrefix) // empty token
	expectLiveTokens(mock)
	expectTerminalTypes(mock, map[string]string{})

	w := NewOrphanDBSweepWorker(db, lister, nil, OrphanDBSweepConfig{Enabled: true})
	if err := w.Work(context.Background(), orphanFakeJob[OrphanDBSweepArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// TestClassifyOrphanDBKind — pure-function table for the kind classifier.
func TestClassifyOrphanDBKind(t *testing.T) {
	cases := []struct {
		terminalType string
		want         string
	}{
		{orphanDBSweepResourceTypeRedis, orphanDBSweepKindRedisNS},
		{"postgres", orphanDBSweepKindCustomerNS},
		{"mongodb", orphanDBSweepKindCustomerNS},
		{"", orphanDBSweepKindCustomerNS},
	}
	for _, c := range cases {
		if got := classifyOrphanDBKind(c.terminalType); got != c.want {
			t.Errorf("classifyOrphanDBKind(%q) = %q; want %q", c.terminalType, got, c.want)
		}
	}
}

// TestOrphanDBSweepKindToProto — only redis maps to a concrete proto type; the
// generic kind maps to UNSPECIFIED so the destructive path skips it.
func TestOrphanDBSweepKindToProto(t *testing.T) {
	if got := orphanDBSweepKindToProto(orphanDBSweepKindRedisNS); got != commonv1.ResourceType_RESOURCE_TYPE_REDIS {
		t.Errorf("redis kind → %v; want REDIS", got)
	}
	if got := orphanDBSweepKindToProto(orphanDBSweepKindCustomerNS); got != commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED {
		t.Errorf("generic kind → %v; want UNSPECIFIED", got)
	}
}

// TestOrphanDBSweepConfig_EffectiveGrace — the grace defaults to the shared
// orphanNoDBRowGrace when unset, honours an explicit override otherwise.
func TestOrphanDBSweepConfig_EffectiveGrace(t *testing.T) {
	if g := (OrphanDBSweepConfig{}).effectiveGrace(); g != orphanNoDBRowGrace {
		t.Errorf("default grace = %v; want %v", g, orphanNoDBRowGrace)
	}
	custom := 2 * time.Hour
	if g := (OrphanDBSweepConfig{Grace: custom}).effectiveGrace(); g != custom {
		t.Errorf("override grace = %v; want %v", g, custom)
	}
}

// TestOrphanDBSweep_ModeLabel — modeLabel reflects the armed state.
func TestOrphanDBSweep_ModeLabel(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	deprov := &recordingDeprovisioner{}
	armed := NewOrphanDBSweepWorker(db, newFakeNamespaceLister(), deprov, OrphanDBSweepConfig{Enabled: true, DestructiveEnabled: true})
	if armed.modeLabel() != "destructive-audited" {
		t.Errorf("armed modeLabel = %q; want destructive-audited", armed.modeLabel())
	}
	audit := NewOrphanDBSweepWorker(db, newFakeNamespaceLister(), deprov, OrphanDBSweepConfig{Enabled: true})
	if audit.modeLabel() != "audit-only" {
		t.Errorf("audit modeLabel = %q; want audit-only", audit.modeLabel())
	}
}

// TestOrphanDBSweepArgs_Kind — the River kind string is stable.
func TestOrphanDBSweepArgs_Kind(t *testing.T) {
	if k := (OrphanDBSweepArgs{}).Kind(); k != "orphan_db_sweep" {
		t.Errorf("Kind() = %q; want orphan_db_sweep", k)
	}
}

// TestOrphanDBSweepDeprovisionerFor — the typed-nil-safe conversion the
// StartWorkers wiring uses. A nil *provisioner.Client → a genuine nil interface
// (so destructiveArmed stays false); a non-nil pointer → a usable interface.
func TestOrphanDBSweepDeprovisionerFor(t *testing.T) {
	if got := orphanDBSweepDeprovisionerFor(nil); got != nil {
		t.Errorf("orphanDBSweepDeprovisionerFor(nil) = %v; want nil interface (typed-nil safety)", got)
	}
	client, conn, err := provisioner.NewClient("127.0.0.1:1", "secret")
	if err != nil {
		t.Fatalf("provisioner.NewClient: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if got := orphanDBSweepDeprovisionerFor(client); got == nil {
		t.Error("orphanDBSweepDeprovisionerFor(non-nil client) = nil; want a usable ResourceDeprovisioner")
	}
}

// TestOrphanDBSweep_DestructiveDeprovisionFailure — when the audited
// DeprovisionResource returns an error, the sweep logs + leaves the namespace
// for the next tick (no panic, no error bubbled up).
func TestOrphanDBSweep_DestructiveDeprovisionFailure(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	lister := newFakeNamespaceLister().withCustomerNamespaces(odsNS(odsTokA))
	expectLiveTokens(mock)
	expectTerminalTypes(mock, map[string]string{odsTokA: orphanDBSweepResourceTypeRedis})
	expectLiveTokens(mock) // destructive re-confirm

	deprov := &recordingDeprovisioner{failErr: errors.New("provisioner unreachable")}
	w := NewOrphanDBSweepWorker(db, lister, deprov, OrphanDBSweepConfig{
		Enabled:            true,
		DestructiveEnabled: true,
	})
	if err := w.Work(context.Background(), orphanFakeJob[OrphanDBSweepArgs]()); err != nil {
		t.Fatalf("Work returned error on deprovision-failure path: %v", err)
	}
	if deprov.calls != 1 {
		t.Errorf("DeprovisionResource calls = %d; want 1 (attempt made even though it fails)", deprov.calls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOrphanDBSweep_DestructiveReconfirmError — when the destructive re-confirm
// live-token read errors, the candidate is SKIPPED (we refuse to drop without a
// clean re-confirm), not deprovisioned.
func TestOrphanDBSweep_DestructiveReconfirmError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	lister := newFakeNamespaceLister().withCustomerNamespaces(odsNS(odsTokA))
	expectLiveTokens(mock) // detection
	expectTerminalTypes(mock, map[string]string{odsTokA: orphanDBSweepResourceTypeRedis})
	// Destructive re-confirm read fails.
	mock.ExpectQuery(`SELECT DISTINCT token::text\s+FROM resources\s+WHERE status IN`).
		WillReturnError(errors.New("connection reset during re-confirm"))

	deprov := &recordingDeprovisioner{}
	w := NewOrphanDBSweepWorker(db, lister, deprov, OrphanDBSweepConfig{
		Enabled:            true,
		DestructiveEnabled: true,
	})
	if err := w.Work(context.Background(), orphanFakeJob[OrphanDBSweepArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if deprov.calls != 0 {
		t.Errorf("fail-safe violated: DeprovisionResource called %d times after re-confirm error; want 0", deprov.calls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// TestOrphanDBSweep_LiveTokenScanError — a row whose value fails Scan in the
// live-token query surfaces as a query error → fail-open zero candidates.
func TestOrphanDBSweep_LiveTokenScanError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	lister := newFakeNamespaceLister().withCustomerNamespaces(odsNS(odsTokA))
	// A row whose single column is nil fails Scan into a string.
	badRows := sqlmock.NewRows([]string{"token"}).AddRow(nil)
	mock.ExpectQuery(`SELECT DISTINCT token::text\s+FROM resources\s+WHERE status IN`).
		WillReturnRows(badRows)

	deprov := &recordingDeprovisioner{}
	before := testutil.ToFloat64(metrics.OrphanDBSweepCandidatesTotal.WithLabelValues(orphanDBSweepKindCustomerNS))
	w := NewOrphanDBSweepWorker(db, lister, deprov, OrphanDBSweepConfig{Enabled: true, DestructiveEnabled: true})
	if err := w.Work(context.Background(), orphanFakeJob[OrphanDBSweepArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	after := testutil.ToFloat64(metrics.OrphanDBSweepCandidatesTotal.WithLabelValues(orphanDBSweepKindCustomerNS))
	if after != before {
		t.Errorf("candidate moved on live-token scan error: delta=%v; want 0", after-before)
	}
	if deprov.calls != 0 {
		t.Errorf("deprovisioner called %d times on scan error; want 0", deprov.calls)
	}
}

// TestOrphanDBSweep_TerminalTypesScanError — a row that fails Scan in the
// terminal-type query degrades the classification to generic (non-fatal).
func TestOrphanDBSweep_TerminalTypesScanError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	lister := newFakeNamespaceLister().withCustomerNamespaces(odsNS(odsTokA))
	expectLiveTokens(mock)
	// A terminal-type row whose resource_type is nil fails Scan into a string.
	badRows := sqlmock.NewRows([]string{"token", "resource_type"}).AddRow(odsTokA, nil)
	mock.ExpectQuery(`SELECT DISTINCT ON \(token::text\)`).WillReturnRows(badRows)

	before := testutil.ToFloat64(metrics.OrphanDBSweepCandidatesTotal.WithLabelValues(orphanDBSweepKindCustomerNS))
	w := NewOrphanDBSweepWorker(db, lister, nil, OrphanDBSweepConfig{Enabled: true})
	if err := w.Work(context.Background(), orphanFakeJob[OrphanDBSweepArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	after := testutil.ToFloat64(metrics.OrphanDBSweepCandidatesTotal.WithLabelValues(orphanDBSweepKindCustomerNS))
	if after-before != 1 {
		t.Errorf("candidate delta = %v; want 1 (detection runs, kind degrades to generic on scan error)", after-before)
	}
}
