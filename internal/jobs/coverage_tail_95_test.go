package jobs

// coverage_tail_95_test.go — closes the last reachable per-function gaps in
// the jobs package so the CI-measured package total clears the 95% floor.
//
// Every test here runs under the EXISTING coverage.yml CI environment
// (postgres/redis/mongo service containers + TEST_* env). NONE of them
// depend on TEST_WORKER_STARTUP_DSN — that env var is NOT set in CI, so the
// StartWorkers boot test it gates SKIPS there. We add coverage via
// sqlmock + an SDK-disabled New Relic application + pure-value calls that
// need no live infra at all.
//
// Targets (each was < 95% in the CI-measured profile):
//   * middleware.go        Work — the w.nrApp != nil transaction path +
//                          the txn.NoticeError(err) error arm.
//   * event_email_mapping  buildBackupFailed / buildRestoreSucceeded /
//                          buildRestoreFailed — the `row.ResourceType != ""`
//                          column-wins branch (the metadata-fallback else
//                          branch is already covered).
//   * billing_reconciler   emitUpgradeAudit / emitCancelAudit — the
//                          fail-open `err != nil` arm.
//   * checkout_reconcile   emailAbandonedCheckout — the claim-row
//                          non-ErrNoRows DB-error arm.

import (
	"context"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/newrelic/go-agent/v3/newrelic"

	"instant.dev/common/logctx"
)

// newDisabledNRApp builds a real, non-nil *newrelic.Application whose
// transactions are no-ops and which performs NO network I/O. ConfigEnabled(false)
// is the SDK's documented hermetic mode — StartTransaction returns a live
// (but inert) *Transaction, exercising the wrapper's nrApp != nil path
// without a daemon, a license key, or any harvest cycle.
func newDisabledNRApp(t *testing.T) *newrelic.Application {
	t.Helper()
	app, err := newrelic.NewApplication(
		newrelic.ConfigAppName("instant-worker-test"),
		newrelic.ConfigEnabled(false),
	)
	if err != nil {
		t.Fatalf("newrelic.NewApplication(disabled): %v", err)
	}
	return app
}

// TestWithObservability_NRPresent_Success drives the w.nrApp != nil arm of
// Work on the success path: StartTransaction + NewContext + defer End all
// execute, then the inner worker returns nil.
func TestWithObservability_NRPresent_Success(t *testing.T) {
	fake := &fakeWorker{}
	wrapped := WithObservability[fakeArgs](fake, newDisabledNRApp(t))

	if err := wrapped.Work(context.Background(), newJob(42)); err != nil {
		t.Fatalf("Work returned error on success path: %v", err)
	}
	// The wrapper must still have stamped the ctx ids on the way through the
	// NR-present branch — same contract as the nil-app path.
	if got := logctx.TIDFromContext(fake.gotCtx); got == "" {
		t.Error("tid not stamped on ctx in NR-present path")
	}
	if got := logctx.TraceIDFromContext(fake.gotCtx); got == "" {
		t.Error("trace_id not stamped on ctx in NR-present path")
	}
}

// TestWithObservability_NRPresent_Error drives the txn.NoticeError(err) arm:
// nrApp != nil AND the inner worker fails, so both the transaction-open
// branch and the error-notice branch execute.
func TestWithObservability_NRPresent_Error(t *testing.T) {
	wantErr := errors.New("inner work blew up")
	fake := &fakeWorker{returns: wantErr}
	wrapped := WithObservability[fakeArgs](fake, newDisabledNRApp(t))

	err := wrapped.Work(context.Background(), newJob(7))
	if !errors.Is(err, wantErr) {
		t.Fatalf("Work should bubble the inner error unchanged, got %v", err)
	}
}

// TestEventEmail_Builders_ResourceTypeFromColumn covers the
// `if row.ResourceType != ""` arm of the three backup/restore builders —
// when the audit row carries a ResourceType column, it wins over the
// metadata fallback. The else (metadata) arm is covered elsewhere.
func TestEventEmail_Builders_ResourceTypeFromColumn(t *testing.T) {
	cases := []struct {
		name    string
		builder func(auditRow) (map[string]string, bool)
	}{
		{"buildBackupFailed", buildBackupFailed},
		{"buildRestoreSucceeded", buildRestoreSucceeded},
		{"buildRestoreFailed", buildRestoreFailed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			row := auditRow{
				ID:           "a-rt-1",
				TeamID:       "t-rt-1",
				OwnerEmail:   "owner@example.com",
				ResourceType: "postgres", // non-empty → column-wins branch
			}
			params, ok := c.builder(row)
			if !ok {
				t.Fatalf("%s returned ok=false with a valid owner email", c.name)
			}
			if params["resource_type"] != "postgres" {
				t.Errorf("%s: resource_type = %q; want the column value %q",
					c.name, params["resource_type"], "postgres")
			}
		})
	}
}

// TestBillingReconciler_EmitUpgradeAudit_FailOpen drives the fail-open arm of
// emitUpgradeAudit: the audit INSERT errors, so RecordFailOpen runs and the
// method returns without panicking (tier change already committed upstream).
func TestBillingReconciler_EmitUpgradeAudit_FailOpen(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnError(errors.New("audit DB brownout"))

	w := &BillingReconcilerWorker{db: db}
	// Must not panic; the fail-open path swallows the error.
	w.emitUpgradeAudit(context.Background(), uuid.New(), "hobby", "pro", "sub_x")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestBillingReconciler_EmitCancelAudit_FailOpen — same fail-open arm for the
// cancel audit row.
func TestBillingReconciler_EmitCancelAudit_FailOpen(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnError(errors.New("audit DB brownout"))

	w := &BillingReconcilerWorker{db: db}
	w.emitCancelAudit(context.Background(), uuid.New(), "pro", "hobby", "sub_x")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestCheckoutReconcile_EmailAbandonedCheckout_ClaimError drives the
// non-ErrNoRows error arm of emailAbandonedCheckout's claim SELECT: a generic
// DB error (not sql.ErrNoRows) must propagate as a wrapped Work error so the
// tx rolls back and the sweep records the failure.
func TestCheckoutReconcile_EmailAbandonedCheckout_ClaimError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	// The claim SELECT returns a generic error (NOT sql.ErrNoRows) → the
	// `if err != nil` arm wraps and returns it.
	mock.ExpectQuery(`SELECT subscription_id\s+FROM pending_checkouts`).
		WithArgs("sub_claim_err").
		WillReturnError(errors.New("lock wait timeout"))
	mock.ExpectRollback()

	w := &CheckoutReconcileWorker{db: db}
	gotErr := w.emailAbandonedCheckout(context.Background(), checkoutRow{
		subscriptionID: "sub_claim_err",
		teamID:         uuid.New().String(),
		customerEmail:  "buyer@example.com",
		planTier:       "pro",
	})
	if gotErr == nil {
		t.Fatal("expected a wrapped claim-row error, got nil")
	}
}

// unmarshalableMeta returns a metadata map that json.Marshal genuinely
// cannot encode (a channel value has no JSON representation). This drives
// the audit-row marshal-error degradation arms that are otherwise
// unreachable with primitive-only maps — without any source-level seam.
func unmarshalableMeta() map[string]any {
	return map[string]any{"bad": make(chan int)}
}

// TestCustomerRestoreRunner_WriteAudit_MarshalError drives the
// audit_marshal_failed degradation arm: an unmarshalable meta map makes
// json.Marshal fail, so writeAudit logs + returns BEFORE touching the DB
// (db is nil here, proving the early return).
func TestCustomerRestoreRunner_WriteAudit_MarshalError(t *testing.T) {
	w := &CustomerRestoreRunnerWorker{db: nil}
	// Must not panic and must not dereference the nil db — the marshal
	// failure short-circuits ahead of ExecContext.
	w.writeAudit(context.Background(), uuid.New(), uuid.New().String(),
		"postgres", "restore.failed", "summary", unmarshalableMeta())
}

// TestCustomerBackupRunner_WriteAudit_MarshalError — same degradation arm
// on the backup runner's writeAudit.
func TestCustomerBackupRunner_WriteAudit_MarshalError(t *testing.T) {
	w := &CustomerBackupRunnerWorker{db: nil}
	w.writeAudit(context.Background(), uuid.New(), uuid.New().String(),
		"postgres", "backup.failed", "summary", unmarshalableMeta())
}

// TestPlatformDBBackup_WriteAudit_MarshalError — the platform-DB backup
// writeAudit checks w.db == nil first, so we pass a sqlmock DB (with NO
// expectations: the marshal failure returns before any ExecContext).
func TestPlatformDBBackup_WriteAudit_MarshalError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	w := &PlatformDBBackupWorker{db: db}
	w.writeAudit(context.Background(), "backup.failed", "summary", unmarshalableMeta())
	// No DB call expected — the marshal error returns first.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected DB call on marshal-error path: %v", err)
	}
}

// TestPropagationRunner_InsertAuditRow_MarshalError drives the
// audit_meta_marshal_failed arm of insertPropagationAuditRow: an
// unmarshalable meta map short-circuits before the INSERT (db nil proves it).
func TestPropagationRunner_InsertAuditRow_MarshalError(t *testing.T) {
	w := &PropagationRunnerWorker{db: nil}
	w.insertPropagationAuditRow(context.Background(),
		propagationRow{id: uuid.New(), teamID: uuid.New(), kind: "regrade"},
		"propagation.failed", "summary", unmarshalableMeta())
}

// TestProvisionerReconciler_MarkAbandoned_UpdateError drives the reachable
// `UPDATE resources ... err != nil` arm of markAbandoned: a DB error on the
// status flip must propagate (the audit INSERT is never reached).
func TestProvisionerReconciler_MarkAbandoned_UpdateError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE resources`).
		WillReturnError(errors.New("update blew up"))

	w := &ProvisionerReconcilerWorker{db: db}
	gotErr := w.markAbandoned(context.Background(),
		reconcilerCandidate{id: uuid.New(), token: uuid.New(), resourceType: "postgres"},
		errors.New("probe failed"))
	if gotErr == nil {
		t.Fatal("expected the UPDATE error to propagate, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}
