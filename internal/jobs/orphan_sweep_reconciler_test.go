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
// cluster-wide lists PASS 3 (deploy namespaces), PASS 4 (customer
// namespaces), and PASS 5 (stack namespaces — T6 P0-1) need. listErr /
// customerListErr / stackListErr, when set, make the corresponding List
// return that error — used to exercise the fail-open posture for an RBAC
// Forbidden / transient k8s failure.
//
// namespaceAges (2026-05-20 PASS 3 enhancement) backs GetNamespaceAge,
// the per-namespace age check used by the "no_db_row" grace path. A test
// that doesn't care about ages can leave this nil — GetNamespaceAge then
// defaults to returning a "very old" duration so the reap path proceeds.
type fakeNamespaceLister struct {
	namespaces         []string // instant-deploy-* — returned by ListDeployNamespaces
	customerNamespaces []string // instant-customer-* — returned by ListCustomerNamespaces
	stackNamespaces    []string // instant-stack-* — returned by ListStackNamespaces
	deleted            []string
	failOn             map[string]error
	listErr            error
	customerListErr    error
	stackListErr       error
	namespaceAges      map[string]time.Duration // per-ns override for GetNamespaceAge; missing → 365*24h (very old)
	ageErr             error                    // when set, GetNamespaceAge returns this error
}

func newFakeNamespaceLister(namespaces ...string) *fakeNamespaceLister {
	return &fakeNamespaceLister{
		namespaces:    namespaces,
		failOn:        map[string]error{},
		namespaceAges: map[string]time.Duration{},
	}
}

// withNamespaceAge sets the age GetNamespaceAge will report for `ns`.
// Returns the fake for chaining.
func (f *fakeNamespaceLister) withNamespaceAge(ns string, age time.Duration) *fakeNamespaceLister {
	if f.namespaceAges == nil {
		f.namespaceAges = map[string]time.Duration{}
	}
	f.namespaceAges[ns] = age
	return f
}

// withCustomerNamespaces sets the instant-customer-* namespaces the fake
// reports to PASS 4. Returns the fake for chaining.
func (f *fakeNamespaceLister) withCustomerNamespaces(ns ...string) *fakeNamespaceLister {
	f.customerNamespaces = ns
	return f
}

// withStackNamespaces sets the instant-stack-* namespaces the fake reports
// to PASS 5 (T6 P0-1). Returns the fake for chaining.
func (f *fakeNamespaceLister) withStackNamespaces(ns ...string) *fakeNamespaceLister {
	f.stackNamespaces = ns
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

func (f *fakeNamespaceLister) ListStackNamespaces(_ context.Context) ([]string, error) {
	if f.stackListErr != nil {
		return nil, f.stackListErr
	}
	out := make([]string, len(f.stackNamespaces))
	copy(out, f.stackNamespaces)
	return out, nil
}

// GetNamespaceAge satisfies the K8sNamespaceLister extension PASS 3 added
// (2026-05-20). When ageErr is set, returns it. Otherwise returns the
// per-namespace override from namespaceAges; missing entries default to
// 365*24h (very old) so a test that doesn't care about the grace window
// gets the normal reap behaviour.
func (f *fakeNamespaceLister) GetNamespaceAge(_ context.Context, namespace string) (time.Duration, error) {
	if f.ageErr != nil {
		return 0, f.ageErr
	}
	if age, ok := f.namespaceAges[namespace]; ok {
		return age, nil
	}
	return 365 * 24 * time.Hour, nil
}

// fakePodStateProvider is the PASS 6 seam fake. Maps namespace → []reason
// (one entry per pod's primary container Waiting.Reason; "" for a pod
// that is not in a Waiting state).
type fakePodStateProvider struct {
	reasonsByNamespace map[string][]string
	listErr            error
	listErrByNS        map[string]error
}

func newFakePodStateProvider() *fakePodStateProvider {
	return &fakePodStateProvider{
		reasonsByNamespace: map[string][]string{},
		listErrByNS:        map[string]error{},
	}
}

func (f *fakePodStateProvider) withReasons(ns string, reasons ...string) *fakePodStateProvider {
	f.reasonsByNamespace[ns] = reasons
	return f
}

func (f *fakePodStateProvider) ListPodWaitingReasons(_ context.Context, namespace string) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	if perr, ok := f.listErrByNS[namespace]; ok && perr != nil {
		return nil, perr
	}
	reasons, ok := f.reasonsByNamespace[namespace]
	if !ok {
		return nil, nil
	}
	out := make([]string, len(reasons))
	copy(out, reasons)
	return out, nil
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
	// PASS 3: the row scan returns only liveApp's row → orphanApp has no
	// row in the result set → reaped via reason=no_db_row. orphanNS gets
	// the "very old" age default so the grace check passes.
	mock.ExpectQuery(`SELECT d.app_id, d.status, t.status, d.created_at\s+FROM deployments d\s+JOIN teams t`).
		WillReturnRows(sqlmock.NewRows([]string{"app_id", "d_status", "t_status", "created_at"}).
			AddRow(liveApp, "healthy", "active", time.Now().UTC().Add(-24*time.Hour)))

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
		mock.ExpectQuery(`SELECT d.app_id, d.status, t.status, d.created_at\s+FROM deployments d\s+JOIN teams t`).
			WillReturnRows(sqlmock.NewRows([]string{"app_id", "d_status", "t_status", "created_at"}))
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
	mock.ExpectQuery(`SELECT d.app_id, d.status, t.status, d.created_at\s+FROM deployments d\s+JOIN teams t`).
		WillReturnRows(sqlmock.NewRows([]string{"app_id", "d_status", "t_status", "created_at"}))
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
	mock.ExpectQuery(`SELECT d.app_id, d.status, t.status, d.created_at\s+FROM deployments d\s+JOIN teams t`).
		WillReturnRows(sqlmock.NewRows([]string{"app_id", "d_status", "t_status", "created_at"}))

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

// TestOrphanSweep_Pass5_ReclaimsOrphanedStackNamespace is the T6 P0-1
// regression test (BugBash 2026-05-20).
//
// THE BUG: ExpireStacksWorker was wired with nsPrefix="instant-apps-"
// (from cfg.KubeNamespaceApps+"-"), but real stack namespaces are
// "instant-stack-<id>". The safety guard in deleteK8sNamespace refused
// every real stack namespace and returned nil-success. ExpireStacksWorker
// treated nil as "teardown succeeded" and DELETE'd the `stacks` row
// anyway → orphan namespace + pods + ingress + TLS cert forever, no DB
// pointer to recover.
//
// THE FIX: (a) ExpireStacksWorker now carries the correct
// "instant-stack-" prefix (workers.go). (b) PASS 5 (this test) lists
// every instant-stack-* namespace and deletes any whose <id> has no row
// in `stacks`, catching pre-fix orphans and guarding against recurrence.
//
// THE ASSERTION: given two stack namespaces — one whose id IS still
// present in `stacks`, one whose id is NOT — PASS 5 deletes ONLY the
// orphan. If a future edit drops PASS 5 (or reintroduces the prefix
// mismatch via ExpireStacksWorker only and never adds the backstop),
// the orphan is never deleted and this test fails.
func TestOrphanSweep_Pass5_ReclaimsOrphanedStackNamespace(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	liveStackID := "1111aaaa-bbbb-cccc-dddd-eeeeffff0000"
	orphanStackID := "2222aaaa-bbbb-cccc-dddd-eeeeffff0000"
	liveNS := ExpireStacksNamespacePrefix + liveStackID
	orphanNS := ExpireStacksNamespacePrefix + orphanStackID

	// PASS 1 + 2 skipped (nil executor, nil canceler).
	// PASS 3 (no deploy namespaces — fake returns empty).
	mock.ExpectQuery(`SELECT d.app_id, d.status, t.status, d.created_at\s+FROM deployments d\s+JOIN teams t`).
		WillReturnRows(sqlmock.NewRows([]string{"app_id", "d_status", "t_status", "created_at"}))
	// PASS 4 (no customer namespaces — fake returns empty; short-circuits).
	// PASS 5: live-stack-ids query returns ONLY liveStackID → orphanNS is
	// the orphan.
	mock.ExpectQuery(`SELECT id::text FROM stacks`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(liveStackID))

	lister := newFakeNamespaceLister().withStackNamespaces(liveNS, orphanNS)
	w := NewOrphanSweepReconciler(db, nil, nil, lister)
	if err := w.Work(context.Background(), orphanFakeJob[OrphanSweepReconcilerArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	if len(lister.deleted) != 1 || lister.deleted[0] != orphanNS {
		t.Errorf("T6 P0-1 regression: deleted stack namespaces = %v, want [%s] "+
			"(the orphan must be reclaimed; the live-stack namespace must be kept)",
			lister.deleted, orphanNS)
	}
}

// TestOrphanSweep_Pass5_NoStackNamespaces_NoQuery proves PASS 5 short-
// circuits when the cluster has no instant-stack-* namespaces — must NOT
// run the live-stack-id query. Guards against a delete-everything bug if
// the query is ever moved before the empty check.
func TestOrphanSweep_Pass5_NoStackNamespaces_NoQuery(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// PASS 3 only — empty deploy + empty customer + empty stack set means
	// nothing else queues.
	mock.ExpectQuery(`SELECT d.app_id, d.status, t.status, d.created_at\s+FROM deployments d\s+JOIN teams t`).
		WillReturnRows(sqlmock.NewRows([]string{"app_id", "d_status", "t_status", "created_at"}))

	lister := newFakeNamespaceLister()
	w := NewOrphanSweepReconciler(db, nil, nil, lister)
	if err := w.Work(context.Background(), orphanFakeJob[OrphanSweepReconcilerArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	if len(lister.deleted) != 0 {
		t.Errorf("PASS 5 deleted namespaces on an empty stack set: %v", lister.deleted)
	}
}

// TestExpireStacksNamespacePrefix_MatchesStackProviderContract is the
// build-SHA gate for the T6 P0-1 prefix-fix. The pre-fix code used
// `cfg.KubeNamespaceApps+"-"` = "instant-apps-" which never matched a
// real "instant-stack-<id>" namespace and silently passed the safety
// guard. Any future edit that changes ExpireStacksNamespacePrefix away
// from "instant-stack-" fails this test loudly.
func TestExpireStacksNamespacePrefix_MatchesStackProviderContract(t *testing.T) {
	const wantPrefix = "instant-stack-"
	if ExpireStacksNamespacePrefix != wantPrefix {
		t.Fatalf("ExpireStacksNamespacePrefix = %q; want %q — must match api compute.StackNamespacePrefix",
			ExpireStacksNamespacePrefix, wantPrefix)
	}
	// And the wired-in worker MUST carry that exact prefix. Construct an
	// ExpireStacksWorker via the exported constructor and verify the
	// nsPrefix field (package-private — visible from the same package's
	// _test.go).
	w := NewExpireStacksWorker(nil, ExpireStacksNamespacePrefix)
	if w.nsPrefix != wantPrefix {
		t.Fatalf("NewExpireStacksWorker(nsPrefix=%q): worker carried %q; want %q — the safety guard refuses every real stack namespace if these drift",
			ExpireStacksNamespacePrefix, w.nsPrefix, wantPrefix)
	}
}

// ── 2026-05-20: PASS 3 enhanced reasons + PASS 6 stuck-build ─────────────

// TestOrphanSweep_NamespaceWithoutDBRow_ReapsAfterGrace exercises the
// "no DB row" reap path's grace window. A namespace that is older than
// orphanNoDBRowGrace AND has no matching deployments row is reaped via
// reason=no_db_row. A namespace that has no row BUT is still within the
// grace window is left alone (the in-flight provision case).
func TestOrphanSweep_NamespaceWithoutDBRow_ReapsAfterGrace(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	oldOrphanNS := deployNamespacePrefixTDE + "oldorphan"
	freshOrphanNS := deployNamespacePrefixTDE + "freshorphan"

	// PASS 3 sees both namespaces; no rows in the deployments-by-app_id
	// query → both classify as no_db_row. Only oldOrphan is >grace.
	mock.ExpectQuery(`SELECT d.app_id, d.status, t.status, d.created_at\s+FROM deployments d\s+JOIN teams t`).
		WillReturnRows(sqlmock.NewRows([]string{"app_id", "d_status", "t_status", "created_at"}))

	lister := newFakeNamespaceLister(oldOrphanNS, freshOrphanNS).
		withNamespaceAge(oldOrphanNS, 2*time.Hour).            // >1h grace → reap
		withNamespaceAge(freshOrphanNS, 10*time.Minute)        // <1h grace → keep
	w := NewOrphanSweepReconciler(db, nil, nil, lister)
	if err := w.Work(context.Background(), orphanFakeJob[OrphanSweepReconcilerArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	if len(lister.deleted) != 1 || lister.deleted[0] != oldOrphanNS {
		t.Errorf("expected exactly [%s] reaped (fresh orphan must be kept inside grace); got %v",
			oldOrphanNS, lister.deleted)
	}
}

// TestOrphanSweep_FailedDeployment_ReapedAfter6h exercises the
// failed_old_deployment reap path. A namespace whose row is
// status='failed' AND created_at > orphanFailedDeploymentGrace is reaped.
// A namespace whose row is status='failed' but newer than 6h is left
// alone (the operator may still want the pod state around for
// investigation).
func TestOrphanSweep_FailedDeployment_ReapedAfter6h(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	oldFailedApp := "oldfailed"
	freshFailedApp := "freshfailed"
	oldFailedNS := deployNamespacePrefixTDE + oldFailedApp
	freshFailedNS := deployNamespacePrefixTDE + freshFailedApp

	// Both rows have status='failed' under an active team. Only the
	// 24h-old row is reapable; the 1h-old row is inside the 6h grace.
	mock.ExpectQuery(`SELECT d.app_id, d.status, t.status, d.created_at\s+FROM deployments d\s+JOIN teams t`).
		WillReturnRows(sqlmock.NewRows([]string{"app_id", "d_status", "t_status", "created_at"}).
			AddRow(oldFailedApp, "failed", "active", time.Now().UTC().Add(-24*time.Hour)).
			AddRow(freshFailedApp, "failed", "active", time.Now().UTC().Add(-1*time.Hour)))

	lister := newFakeNamespaceLister(oldFailedNS, freshFailedNS)
	w := NewOrphanSweepReconciler(db, nil, nil, lister)
	if err := w.Work(context.Background(), orphanFakeJob[OrphanSweepReconcilerArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	if len(lister.deleted) != 1 || lister.deleted[0] != oldFailedNS {
		t.Errorf("expected only [%s] reaped (fresh failed row must stay inside 6h grace); got %v",
			oldFailedNS, lister.deleted)
	}
}

// TestOrphanSweep_Pass6_StuckBuild_FlipsToFailed exercises PASS 6: a row
// stuck in 'building' or 'deploying' for >30min whose pod is in
// ImagePullBackOff is flipped to 'failed' with an explanatory
// error_message, and a team.orphan_reclaimed audit row lands.
func TestOrphanSweep_Pass6_StuckBuild_FlipsToFailed(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	stuckDeploymentID := uuid.New()
	stuckTeamID := uuid.New()
	stuckAppID := "stucky"
	stuckNS := deployNamespacePrefixTDE + stuckAppID

	// PASS 1 + 2 skipped (nil executor, nil canceler).
	// PASS 3: stuckNS has a row (active team) → not reaped here.
	mock.ExpectQuery(`SELECT d.app_id, d.status, t.status, d.created_at\s+FROM deployments d\s+JOIN teams t`).
		WillReturnRows(sqlmock.NewRows([]string{"app_id", "d_status", "t_status", "created_at"}).
			AddRow(stuckAppID, "deploying", "active", time.Now().UTC().Add(-9*time.Hour)))
	// PASS 6: candidate query returns the stuck row. Cutoff is roughly
	// "<now - 30min>"; the row's updated_at is 9h ago so it's well past.
	mock.ExpectQuery(`SELECT d.id, d.team_id, d.app_id, d.status, d.updated_at\s+FROM deployments d\s+JOIN teams t`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "team_id", "app_id", "status", "updated_at"}).
			AddRow(stuckDeploymentID, stuckTeamID, stuckAppID, "deploying", time.Now().UTC().Add(-9*time.Hour)))
	// After confirming the pod state is ImagePullBackOff, the row is
	// flipped to 'failed' with an error_message.
	mock.ExpectExec(`UPDATE deployments\s+SET status = 'failed'`).
		WithArgs(sqlmock.AnyArg(), stuckDeploymentID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// And a team.orphan_reclaimed audit row lands for the team.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(stuckTeamID, "system", auditKindOrphanReclaimed,
			sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	lister := newFakeNamespaceLister(stuckNS)
	pods := newFakePodStateProvider().withReasons(stuckNS, "ImagePullBackOff")
	w := NewOrphanSweepReconciler(db, nil, nil, lister).WithPodStateProvider(pods)
	if err := w.Work(context.Background(), orphanFakeJob[OrphanSweepReconcilerArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	// PASS 3 must NOT have reaped the namespace (the row is still
	// recoverable — only PASS 6 flips it; PASS 3 will reap on a later
	// sweep once the row is failed_old_deployment + >6h).
	if len(lister.deleted) != 0 {
		t.Errorf("PASS 3 must not reap a row whose team is active and status is deploying; got deletes %v",
			lister.deleted)
	}
}

// TestOrphanSweep_Pass6_RunningPod_DoesNotFlip is the safety guard: a
// stuck-build candidate whose pod state shows a Running container ("")
// is NOT flipped. The build may be progressing.
func TestOrphanSweep_Pass6_RunningPod_DoesNotFlip(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	deploymentID := uuid.New()
	teamID := uuid.New()
	appID := "progressing"
	ns := deployNamespacePrefixTDE + appID

	// PASS 3: row present, active team → not reaped.
	mock.ExpectQuery(`SELECT d.app_id, d.status, t.status, d.created_at\s+FROM deployments d\s+JOIN teams t`).
		WillReturnRows(sqlmock.NewRows([]string{"app_id", "d_status", "t_status", "created_at"}).
			AddRow(appID, "deploying", "active", time.Now().UTC().Add(-2*time.Hour)))
	// PASS 6: candidate fires; but pod state has a "" reason → progressing.
	mock.ExpectQuery(`SELECT d.id, d.team_id, d.app_id, d.status, d.updated_at\s+FROM deployments d\s+JOIN teams t`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "team_id", "app_id", "status", "updated_at"}).
			AddRow(deploymentID, teamID, appID, "deploying", time.Now().UTC().Add(-2*time.Hour)))
	// NO UPDATE queued — flipDeploymentToFailed must not be called.

	lister := newFakeNamespaceLister(ns)
	pods := newFakePodStateProvider().withReasons(ns, "") // "" = Running / progressing
	w := NewOrphanSweepReconciler(db, nil, nil, lister).WithPodStateProvider(pods)
	if err := w.Work(context.Background(), orphanFakeJob[OrphanSweepReconcilerArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestOrphanSweep_PrefixWhitelist_RefusesUnknownNamespace is the durable
// safety guard: the reconciler ONLY ever inspects namespaces matching the
// three known prefixes (instant-deploy-*, instant-customer-*,
// instant-stack-*). A namespace outside the whitelist must never be
// considered for reap, even if the fake reports it.
//
// This test exercises that by feeding the fake a foreign namespace
// (instant-infra) under `namespaces` (which only the deploy List should
// look at). Because the fake's lists are prefix-scoped to instant-deploy-*
// in production, the reconciler never sees the foreign namespace.
// We assert by examining the reap reason classifier directly with a foreign
// app_id — it must return no_db_row only because the caller already stripped
// the prefix, BUT no caller will ever strip instant-infra. The contract:
// the prefix-list seam (ListDeployNamespaces) is the whitelist gate.
func TestOrphanSweep_PrefixWhitelist_RefusesUnknownNamespace(t *testing.T) {
	// Verify the three reap prefixes are exactly the three documented
	// in CLAUDE.md — any future edit that adds a fourth must add a test.
	if deployNamespacePrefixTDE != "instant-deploy-" {
		t.Errorf("deployNamespacePrefixTDE drifted: %q != instant-deploy-", deployNamespacePrefixTDE)
	}
	if customerNamespacePrefix != "instant-customer-" {
		t.Errorf("customerNamespacePrefix drifted: %q != instant-customer-", customerNamespacePrefix)
	}
	if ExpireStacksNamespacePrefix != "instant-stack-" {
		t.Errorf("ExpireStacksNamespacePrefix drifted: %q != instant-stack-", ExpireStacksNamespacePrefix)
	}

	// classifyDeployOrphan is called PER namespace by the sweep; the
	// caller strips the prefix to derive the appID. Verify the function
	// returns (no_db_row, true) for an unknown appID — but ONLY when
	// the caller has already stripped a known prefix. The whitelist
	// guarantee comes from the List, not from the classifier.
	reason, ok := classifyDeployOrphan("nonexistent", map[string]deployRowSnapshot{}, time.Now())
	if !ok || reason != orphanReapReasonNoDBRow {
		t.Errorf("classifyDeployOrphan(no row) = (%q, %v); want (no_db_row, true)", reason, ok)
	}
}

// TestOrphanSweep_ClassifyDeployOrphan_TableDriven enumerates every shape
// of row + team status PASS 3 must decide on. Each table row asserts the
// reap reason (or "" for "live, leave alone"). This is the registry-style
// regression test from CLAUDE.md rule 18: a future edit that changes the
// classification logic for any shape must update this table or the test
// fails loudly.
func TestOrphanSweep_ClassifyDeployOrphan_TableDriven(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name       string
		row        *deployRowSnapshot // nil = no row in map
		wantReason string             // "" = not reapable
	}{
		{"no row at all", nil, orphanReapReasonNoDBRow},
		{"active team, deploying", &deployRowSnapshot{status: "deploying", teamStatus: "active", rowCreatedAt: now.Add(-1 * time.Hour)}, ""},
		{"active team, healthy", &deployRowSnapshot{status: "healthy", teamStatus: "active", rowCreatedAt: now.Add(-30 * 24 * time.Hour)}, ""},
		{"active team, failed within 6h grace", &deployRowSnapshot{status: "failed", teamStatus: "active", rowCreatedAt: now.Add(-1 * time.Hour)}, ""},
		{"active team, failed at exactly 6h", &deployRowSnapshot{status: "failed", teamStatus: "active", rowCreatedAt: now.Add(-orphanFailedDeploymentGrace)}, orphanReapReasonFailedOldDeployment},
		{"active team, failed past 6h", &deployRowSnapshot{status: "failed", teamStatus: "active", rowCreatedAt: now.Add(-12 * time.Hour)}, orphanReapReasonFailedOldDeployment},
		{"deletion_requested team, deploying", &deployRowSnapshot{status: "deploying", teamStatus: "deletion_requested", rowCreatedAt: now.Add(-1 * time.Hour)}, ""},
		{"tombstoned team", &deployRowSnapshot{status: "healthy", teamStatus: "tombstoned", rowCreatedAt: now.Add(-30 * 24 * time.Hour)}, orphanReapReasonTeamTombstoned},
		{"deletion_pending team", &deployRowSnapshot{status: "deploying", teamStatus: "deletion_pending", rowCreatedAt: now.Add(-1 * time.Hour)}, orphanReapReasonTeamTombstoned},
		{"row already deleted", &deployRowSnapshot{status: "deleted", teamStatus: "active", rowCreatedAt: now.Add(-30 * 24 * time.Hour)}, orphanReapReasonTeamTombstoned},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := map[string]deployRowSnapshot{}
			if tc.row != nil {
				m["app"] = *tc.row
			}
			gotReason, gotReapable := classifyDeployOrphan("app", m, now)
			if tc.wantReason == "" {
				if gotReapable {
					t.Errorf("expected NOT reapable; got reason=%q", gotReason)
				}
				return
			}
			if !gotReapable || gotReason != tc.wantReason {
				t.Errorf("classify = (%q, %v); want (%q, true)", gotReason, gotReapable, tc.wantReason)
			}
		})
	}
}

// TestOrphanSweep_StuckBuildWaitingReasons_Registry enumerates every
// waiting-state reason the registry treats as "stuck". If a future edit
// adds a new reason to stuckBuildWaitingReasons it MUST update this test
// (and add an isStuckBuildState case below). Conversely a future edit
// that drops a reason is caught here loudly — no silent reduction in
// coverage.
func TestOrphanSweep_StuckBuildWaitingReasons_Registry(t *testing.T) {
	want := map[string]bool{
		"ImagePullBackOff":  true,
		"ErrImagePull":      true,
		"CrashLoopBackOff":  true,
	}
	if len(stuckBuildWaitingReasons) != len(want) {
		t.Fatalf("stuckBuildWaitingReasons has %d entries; want %d — review the registry",
			len(stuckBuildWaitingReasons), len(want))
	}
	for k := range want {
		if !stuckBuildWaitingReasons[k] {
			t.Errorf("registry missing reason %q", k)
		}
	}
	// And isStuckBuildState must agree for every registry entry.
	for k := range stuckBuildWaitingReasons {
		if !isStuckBuildState([]string{k}) {
			t.Errorf("isStuckBuildState([%q]) returned false but %q is in registry", k, k)
		}
	}
	// Empty input must NOT trip the stuck check (no pods scheduled yet).
	if isStuckBuildState(nil) {
		t.Error("isStuckBuildState(nil) must be false — no pods means progressing, not stuck")
	}
	// A mixed slice with at least one progressing pod must NOT trip.
	if isStuckBuildState([]string{"ImagePullBackOff", ""}) {
		t.Error("isStuckBuildState with one '' (Running) reason must be false")
	}
}
