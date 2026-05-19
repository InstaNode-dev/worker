package jobs

// orphan_sweep_reconciler_test.go — hermetic tests for the
// OrphanSweepReconciler. Internal-package test so it can inject the
// unexported teamTeardownExecutor seam with a fake.
//
// Scenarios covered:
//   1. PASS 1 — a stuck deletion_pending team is detected and its teardown
//      is completed via the executor seam (the orphan-sweep "finishes a
//      partial deletion" property).
//   2. PASS 1 failure — when the executor's teardown errors, the team is
//      left for the next sweep and a team.orphan_sweep_failed audit row
//      lands.
//   3. PASS 2 — an orphaned Razorpay subscription on a tombstoned team is
//      cancelled and the stripe_customer_id column is cleared (the "stop
//      the money" backstop).
//   4. PASS 3 — an instant-deploy-* namespace with no live owner is
//      deleted; a namespace with a live owner is left alone.
//   5. Idempotency — re-running the full sweep over already-clean state is
//      a no-op (zero orphans, zero actions, no error).

import (
	"context"
	"database/sql/driver"
	"errors"
	"log/slog"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

// orphanFakeJob builds a minimal *river.Job for Work() — the internal-
// package twin of jobs_test's fakeJob (which lives in expire_test.go and is
// not visible here).
func orphanFakeJob[T river.JobArgs]() *river.Job[T] {
	return &river.Job[T]{JobRow: &rivertype.JobRow{ID: 1}}
}

// ── fakes ────────────────────────────────────────────────────────────────

// fakeTeardownExecutor records which teams it was asked to finish and can
// be told to fail for a specific team.
type fakeTeardownExecutor struct {
	processed []uuid.UUID
	failFor   map[uuid.UUID]error
}

func (f *fakeTeardownExecutor) processTeam(_ context.Context, c teamPendingDeletion) error {
	f.processed = append(f.processed, c.teamID)
	if err := f.failFor[c.teamID]; err != nil {
		return err
	}
	return nil
}

// fakeOrphanCanceler records cancelled subscription ids and can fail for a
// specific id.
type fakeOrphanCanceler struct {
	cancelled []string
	failFor   map[string]error
}

func (f *fakeOrphanCanceler) CancelSubscription(_ context.Context, subID string) error {
	if err := f.failFor[subID]; err != nil {
		return err
	}
	f.cancelled = append(f.cancelled, subID)
	return nil
}

// fakeNamespaceLister extends fakeNamespaceDeleter behaviour with the
// cluster-wide lists PASS 3 (deploy namespaces) and PASS 4 (customer
// namespaces) need. listErr / customerListErr, when set, make the
// corresponding List return that error — used to exercise the fail-open
// posture for an RBAC Forbidden / transient k8s failure.
type fakeNamespaceLister struct {
	namespaces         []string // instant-deploy-* — returned by ListDeployNamespaces
	customerNamespaces []string // instant-customer-* — returned by ListCustomerNamespaces
	deleted            []string
	failOn             map[string]error
	listErr            error
	customerListErr    error
}

func newFakeNamespaceLister(namespaces ...string) *fakeNamespaceLister {
	return &fakeNamespaceLister{
		namespaces: namespaces,
		failOn:     map[string]error{},
	}
}

// withCustomerNamespaces sets the instant-customer-* namespaces the fake
// reports to PASS 4. Returns the fake for chaining.
func (f *fakeNamespaceLister) withCustomerNamespaces(ns ...string) *fakeNamespaceLister {
	f.customerNamespaces = ns
	return f
}

func (f *fakeNamespaceLister) DeleteNamespace(_ context.Context, ns string) error {
	if err := f.failOn[ns]; err != nil {
		return err
	}
	f.deleted = append(f.deleted, ns)
	return nil
}

func (f *fakeNamespaceLister) NamespaceExists(_ context.Context, ns string) (bool, error) {
	for _, n := range f.namespaces {
		if n == ns {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeNamespaceLister) ListDeployNamespaces(_ context.Context) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := make([]string, len(f.namespaces))
	copy(out, f.namespaces)
	return out, nil
}

func (f *fakeNamespaceLister) ListCustomerNamespaces(_ context.Context) ([]string, error) {
	if f.customerListErr != nil {
		return nil, f.customerListErr
	}
	out := make([]string, len(f.customerNamespaces))
	copy(out, f.customerNamespaces)
	return out, nil
}

// expectEmptyPass4 queues the sqlmock expectation for PASS 4's live-token
// query returning nothing — used when the fake reports customer namespaces
// but a test only cares about the earlier passes, OR when the fake reports
// no customer namespaces (PASS 4 short-circuits before the query, so callers
// with an empty customer-namespace set must NOT queue this).
func expectEmptyPass4(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT DISTINCT token::text\s+FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"token"}))
}

// captureBytesArgOSR captures a JSONB column's bytes for audit assertions.
type captureBytesArgOSR struct{ out *[]byte }

func (c *captureBytesArgOSR) Match(v driver.Value) bool {
	switch b := v.(type) {
	case []byte:
		*c.out = append((*c.out)[:0], b...)
		return true
	case string:
		*c.out = append((*c.out)[:0], []byte(b)...)
		return true
	case nil:
		return true
	}
	return false
}

// expectEmptyPasses queues the sqlmock expectations for PASS 1/2/3 returning
// nothing, so a test exercising only one pass can ignore the others.
func expectEmptyPass1(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`FROM teams\s+WHERE status = 'deletion_pending'`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "deletion_requested_at"}))
}

func expectEmptyPass2(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`FROM teams\s+WHERE status IN \('tombstoned', 'deletion_pending'\)`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "stripe_customer_id"}))
}

// ── scenario 1: PASS 1 completes a stuck pending team ────────────────────

func TestOrphanSweep_Pass1_CompletesStuckPendingTeam(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()

	// PASS 1: one stuck deletion_pending team.
	mock.ExpectQuery(`FROM teams\s+WHERE status = 'deletion_pending'`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "deletion_requested_at"}).
			AddRow(teamID, time.Now().UTC().Add(-40*24*time.Hour)))
	// The reconciled team gets a team.orphan_reclaimed audit row.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", auditKindOrphanReclaimed,
			sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// PASS 2 + 3 skipped (nil canceler, nil k8s) — no queries queued.

	exec := &fakeTeardownExecutor{failFor: map[uuid.UUID]error{}}
	// nil k8s → PASS 3 skipped (no query).
	w := NewOrphanSweepReconciler(db, exec, nil, nil)
	if err := w.Work(context.Background(), orphanFakeJob[OrphanSweepReconcilerArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	if len(exec.processed) != 1 || exec.processed[0] != teamID {
		t.Errorf("executor.processed = %v, want [%s]", exec.processed, teamID)
	}
}

// ── scenario 2: PASS 1 teardown failure → orphan_sweep_failed audit ──────

func TestOrphanSweep_Pass1_TeardownFailure_EmitsFailedAudit(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()

	mock.ExpectQuery(`FROM teams\s+WHERE status = 'deletion_pending'`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "deletion_requested_at"}).
			AddRow(teamID, time.Now().UTC().Add(-40*24*time.Hour)))
	var capturedMeta []byte
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", auditKindOrphanSweepFailed,
			sqlmock.AnyArg(), &captureBytesArgOSR{out: &capturedMeta}).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// PASS 2 + 3 skipped (nil canceler, nil k8s).

	exec := &fakeTeardownExecutor{
		failFor: map[uuid.UUID]error{teamID: errors.New("provisioner unreachable")},
	}
	w := NewOrphanSweepReconciler(db, exec, nil, nil)
	if err := w.Work(context.Background(), orphanFakeJob[OrphanSweepReconcilerArgs]()); err != nil {
		t.Fatalf("Work must isolate per-team failure, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	if len(capturedMeta) == 0 {
		t.Fatal("expected a team.orphan_sweep_failed audit row with metadata")
	}
}

// ── scenario 3: PASS 2 cancels an orphaned Razorpay subscription ─────────

func TestOrphanSweep_Pass2_CancelsOrphanedSubscription(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()
	subID := "sub_orphaned_123"

	// PASS 1 is skipped (nil executor) — no query queued for it.
	// PASS 2: a tombstoned team still carrying a subscription id.
	mock.ExpectQuery(`FROM teams\s+WHERE status IN \('tombstoned', 'deletion_pending'\)`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "stripe_customer_id"}).
			AddRow(teamID, subID))
	// After a successful cancel the column is cleared.
	mock.ExpectExec(`UPDATE teams SET stripe_customer_id = NULL`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// And a team.orphan_reclaimed audit row lands.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", auditKindOrphanReclaimed,
			sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	canceler := &fakeOrphanCanceler{failFor: map[string]error{}}
	w := NewOrphanSweepReconciler(db, nil, canceler, nil)
	if err := w.Work(context.Background(), orphanFakeJob[OrphanSweepReconcilerArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	if len(canceler.cancelled) != 1 || canceler.cancelled[0] != subID {
		t.Errorf("cancelled = %v, want [%s]", canceler.cancelled, subID)
	}
}

// TestOrphanSweep_Pass2_CancelFailure_EmitsFailedAudit — a cancel failure
// leaves the subscription id in place (re-detected next sweep) and emits
// team.orphan_sweep_failed so an operator is alerted that a deleted
// customer may still be billed.
func TestOrphanSweep_Pass2_CancelFailure_EmitsFailedAudit(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()
	subID := "sub_wont_cancel"

	// PASS 1 skipped (nil executor).
	mock.ExpectQuery(`FROM teams\s+WHERE status IN \('tombstoned', 'deletion_pending'\)`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "stripe_customer_id"}).
			AddRow(teamID, subID))
	// No UPDATE clearing the column — the cancel failed.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", auditKindOrphanSweepFailed,
			sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	canceler := &fakeOrphanCanceler{
		failFor: map[string]error{subID: errors.New("razorpay 503")},
	}
	w := NewOrphanSweepReconciler(db, nil, canceler, nil)
	if err := w.Work(context.Background(), orphanFakeJob[OrphanSweepReconcilerArgs]()); err != nil {
		t.Fatalf("Work must isolate cancel failure: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// ── scenario 4: PASS 3 deletes orphaned namespace, keeps live one ────────

func TestOrphanSweep_Pass3_DeletesOrphanedNamespaceKeepsLive(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	liveApp := "liveapp"
	orphanApp := "orphanapp"
	liveNS := deployNamespacePrefixTDE + liveApp
	orphanNS := deployNamespacePrefixTDE + orphanApp

	// PASS 1 + 2 skipped (nil executor, nil canceler).
	// PASS 3: the "live app ids" query returns only liveApp.
	mock.ExpectQuery(`SELECT d.app_id\s+FROM deployments d\s+JOIN teams t`).
		WillReturnRows(sqlmock.NewRows([]string{"app_id"}).AddRow(liveApp))

	lister := newFakeNamespaceLister(liveNS, orphanNS)
	w := NewOrphanSweepReconciler(db, nil, nil, lister)
	if err := w.Work(context.Background(), orphanFakeJob[OrphanSweepReconcilerArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	if len(lister.deleted) != 1 || lister.deleted[0] != orphanNS {
		t.Errorf("deleted namespaces = %v, want [%s] (live namespace must be kept)",
			lister.deleted, orphanNS)
	}
}

// ── scenario 5: idempotent no-op sweep over clean state ──────────────────

func TestOrphanSweep_AllPasses_NoOrphans_Idempotent(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	exec := &fakeTeardownExecutor{failFor: map[uuid.UUID]error{}}
	canceler := &fakeOrphanCanceler{failFor: map[string]error{}}
	lister := newFakeNamespaceLister() // empty cluster

	// Run the full sweep twice — every pass returns nothing both times.
	for range 2 {
		expectEmptyPass1(mock)
		expectEmptyPass2(mock)
		mock.ExpectQuery(`SELECT d.app_id\s+FROM deployments d\s+JOIN teams t`).
			WillReturnRows(sqlmock.NewRows([]string{"app_id"}))
	}

	w := NewOrphanSweepReconciler(db, exec, canceler, lister)
	for i := 0; i < 2; i++ {
		if err := w.Work(context.Background(), orphanFakeJob[OrphanSweepReconcilerArgs]()); err != nil {
			t.Fatalf("sweep %d: %v", i+1, err)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	if len(exec.processed) != 0 || len(canceler.cancelled) != 0 || len(lister.deleted) != 0 {
		t.Errorf("clean-state sweep took actions: processed=%v cancelled=%v deleted=%v",
			exec.processed, canceler.cancelled, lister.deleted)
	}
}

// ── scenario 6: PASS 3 namespace-list Forbidden → fail-open, job succeeds ─
//
// Regression for the prod ERROR-spam regression: when the worker's
// ServiceAccount lacks the cluster-scoped `namespaces` list permission,
// ListDeployNamespaces returns a k8s Forbidden error. The reconciler must
// NOT fail the whole job over it — that has River retry the dispatch every
// ~60s and spam ERROR logs over a missing RBAC grant. Instead PASS 3
// degrades to exactly one structured WARN and a zero-orphan result; PASS 1
// (stuck-pending teardown) and PASS 2 (orphaned-subscription cancel) still
// run and the job returns success.
func TestOrphanSweep_Pass3_ForbiddenNamespaceList_FailsOpen(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	logs := captureSlog(t)

	pendingTeamID := uuid.New()
	subTeamID := uuid.New()
	subID := "sub_orphaned_failopen"

	// PASS 1: one stuck deletion_pending team — must still be reconciled.
	mock.ExpectQuery(`FROM teams\s+WHERE status = 'deletion_pending'`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "deletion_requested_at"}).
			AddRow(pendingTeamID, time.Now().UTC().Add(-40*24*time.Hour)))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(pendingTeamID, "system", auditKindOrphanReclaimed,
			sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// PASS 2: one orphaned subscription — must still be cancelled.
	mock.ExpectQuery(`FROM teams\s+WHERE status IN \('tombstoned', 'deletion_pending'\)`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "stripe_customer_id"}).
			AddRow(subTeamID, subID))
	mock.ExpectExec(`UPDATE teams SET stripe_customer_id = NULL`).
		WithArgs(subTeamID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(subTeamID, "system", auditKindOrphanReclaimed,
			sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// PASS 3: the namespace List is Forbidden. NO further DB query is
	// queued — the live-app-ids query must NOT run once the list fails.
	exec := &fakeTeardownExecutor{failFor: map[uuid.UUID]error{}}
	canceler := &fakeOrphanCanceler{failFor: map[string]error{}}
	lister := newFakeNamespaceLister()
	lister.listErr = errors.New(`namespaces is forbidden: User ` +
		`"system:serviceaccount:instant-infra:instant-worker" cannot list ` +
		`resource "namespaces" in API group "" at the cluster scope`)

	w := NewOrphanSweepReconciler(db, exec, canceler, lister)

	// The job MUST succeed despite the Forbidden namespace list.
	if err := w.Work(context.Background(), orphanFakeJob[OrphanSweepReconcilerArgs]()); err != nil {
		t.Fatalf("Work must fail-open on a Forbidden namespace list, got error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}

	// PASS 1 still ran — the stuck team was reconciled.
	if len(exec.processed) != 1 || exec.processed[0] != pendingTeamID {
		t.Errorf("PASS 1 did not run: executor.processed = %v, want [%s]",
			exec.processed, pendingTeamID)
	}
	// PASS 2 still ran — the orphaned subscription was cancelled.
	if len(canceler.cancelled) != 1 || canceler.cancelled[0] != subID {
		t.Errorf("PASS 2 did not run: cancelled = %v, want [%s]",
			canceler.cancelled, subID)
	}
	// PASS 3 deleted nothing — the list failed.
	if len(lister.deleted) != 0 {
		t.Errorf("PASS 3 deleted namespaces despite a failed list: %v", lister.deleted)
	}

	// Exactly one WARN for the failed namespace list — no ERROR, no spam.
	var pass3Warns int
	for _, r := range *logs {
		if r.msg == "jobs.orphan_sweep.pass3_namespace_list_failed" {
			if r.level != slog.LevelWarn {
				t.Errorf("pass3 namespace-list-failed log level = %v, want WARN", r.level)
			}
			pass3Warns++
		}
		if r.level >= slog.LevelError {
			t.Errorf("fail-open sweep emitted an ERROR log: %q %v", r.msg, r.attrs)
		}
	}
	if pass3Warns != 1 {
		t.Errorf("expected exactly one pass3_namespace_list_failed WARN, got %d", pass3Warns)
	}
}

// ── scenario 7: PASS 4 reclaims orphaned instant-customer-* namespaces ────
//
// TestOrphanSweep_Pass4_ReclaimsOrphanedCustomerNamespace is the MR-P0-1b
// regression guard (BugBash 2026-05-20, cross-confirmed by T1/T5/T20/T24 — the
// headline finding).
//
// THE BUG: every db/redis/mongo/queue resource gets a dedicated
// instant-customer-<token> namespace. The reaper used to mark a resources row
// 'deleted' even when its backend teardown FAILED (MR-P0-1a); a 'deleted' row
// is terminal and invisible to PASS 1's per-team path, so the namespace — with
// a live Postgres/Redis pod inside — was orphaned forever. The orphan-sweep
// reconciler's PASS 3 only ever swept instant-deploy-* namespaces; NOTHING
// swept instant-customer-*. 188 such namespaces leaked in prod, pushing the
// cluster to 98–99% CPU.
//
// THE FIX: PASS 4 lists every instant-customer-* namespace and deletes any
// whose <token> has no active/paused/suspended resources row.
//
// THE ASSERTION: given two customer namespaces — one whose token IS still a
// live resource, one whose token is NOT — PASS 4 deletes ONLY the orphan and
// leaves the live one untouched. If a future edit drops the PASS 4 sweep, the
// orphan is never deleted and this test fails.
func TestOrphanSweep_Pass4_ReclaimsOrphanedCustomerNamespace(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	liveToken := "tok-live-resource"
	orphanToken := "tok-orphan-no-row"
	liveNS := customerNamespacePrefix + liveToken
	orphanNS := customerNamespacePrefix + orphanToken

	// PASS 1 + 2 skipped (nil executor, nil canceler).
	// PASS 3: the live-deploy-app-ids query returns nothing (no deploy
	// namespaces reported by the fake).
	mock.ExpectQuery(`SELECT d.app_id\s+FROM deployments d\s+JOIN teams t`).
		WillReturnRows(sqlmock.NewRows([]string{"app_id"}))
	// PASS 4: the live-resource-tokens query returns ONLY liveToken — so
	// orphanNS (whose token has no active/paused/suspended row) is the orphan.
	mock.ExpectQuery(`SELECT DISTINCT token::text\s+FROM resources\s+WHERE status IN \('active', 'paused', 'suspended'\)`).
		WillReturnRows(sqlmock.NewRows([]string{"token"}).AddRow(liveToken))
	// The reclaimed customer namespace gets a cluster-scoped orphan_reclaimed
	// event — emitted as a structured log (teamID is uuid.Nil), no audit row.

	lister := newFakeNamespaceLister().withCustomerNamespaces(liveNS, orphanNS)
	w := NewOrphanSweepReconciler(db, nil, nil, lister)
	if err := w.Work(context.Background(), orphanFakeJob[OrphanSweepReconcilerArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	// ONLY the orphan namespace is deleted; the namespace backed by a live
	// resource row is left alone.
	if len(lister.deleted) != 1 || lister.deleted[0] != orphanNS {
		t.Errorf("MR-P0-1b regression: deleted customer namespaces = %v, want [%s] "+
			"(the orphan must be reclaimed; the live-resource namespace must be kept)",
			lister.deleted, orphanNS)
	}
}

// TestOrphanSweep_Pass4_NoCustomerNamespaces_NoQuery proves PASS 4 short-
// circuits when the cluster has no instant-customer-* namespaces — it must
// NOT run the live-token query (and so a test with an empty customer set need
// not queue it). Also guards against a delete-everything bug if the query is
// ever moved before the empty check.
func TestOrphanSweep_Pass4_NoCustomerNamespaces_NoQuery(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// PASS 3 query only — no customer namespaces means PASS 4 queues nothing.
	mock.ExpectQuery(`SELECT d.app_id\s+FROM deployments d\s+JOIN teams t`).
		WillReturnRows(sqlmock.NewRows([]string{"app_id"}))

	lister := newFakeNamespaceLister() // no deploy, no customer namespaces
	w := NewOrphanSweepReconciler(db, nil, nil, lister)
	if err := w.Work(context.Background(), orphanFakeJob[OrphanSweepReconcilerArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	if len(lister.deleted) != 0 {
		t.Errorf("PASS 4 deleted namespaces on an empty customer set: %v", lister.deleted)
	}
}
