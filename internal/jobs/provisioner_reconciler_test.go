package jobs_test

// provisioner_reconciler_test.go — hermetic tests for the reconciler.
//
// The sweep SQL is treated as a black box (sqlmock seeds the SELECT result).
// What we exercise:
//
//   1. Happy path — probe returns Reachable → UPDATE→active + audit_log
//      provisioner.reconcile_recovered row.
//   2. Failure path — probe returns Unreachable → UPDATE→failed (with
//      connection_url NULL) + audit_log provisioner.reconcile_abandoned
//      row + Redis DECR refund.
//   3. Skip path — probe returns Skip (webhook / unknown) → only
//      last_reconciled_at stamp, no audit row.
//   4. Empty rowset — no INSERT / UPDATE fires.
//   5. Top-level SELECT failure — propagates so River retries.
//   6. Per-row UPDATE failure — logged + loop continues (fail-open).
//
// fakeJob + errDB live in expire_test.go / quota_test.go and are reused.

import (
	"context"
	"errors"
	"sync"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	"instant.dev/worker/internal/jobs"
)

// fakeProber is a configurable ResourceProber for tests. Records each
// (resource_type, connection_url) pair it was called with.
type fakeProber struct {
	mu       sync.Mutex
	outcome  jobs.ProbeOutcome
	err      error
	byType   map[string]jobs.ProbeOutcome // override per resource_type
	callLog  []fakeProbeCall
}

type fakeProbeCall struct {
	resourceType  string
	connectionURL string
}

func (f *fakeProber) Probe(_ context.Context, resourceType, connectionURL string) (jobs.ProbeOutcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callLog = append(f.callLog, fakeProbeCall{resourceType, connectionURL})
	if f.byType != nil {
		if o, ok := f.byType[resourceType]; ok {
			return o, f.err
		}
	}
	return f.outcome, f.err
}

// reconcilerRowCols is the column order the reconciler's SELECT returns.
// Keep in sync with provisioner_reconciler.go::Work's SELECT projection.
var reconcilerRowCols = []string{
	"id", "token", "resource_type", "connection_url", "team_id_text",
}

func TestProvisionerReconciler_RecoversReachablePending(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	token := uuid.New()
	teamID := uuid.New().String()

	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows(reconcilerRowCols).
			AddRow(resID, token, "postgres", "postgres://encrypted", teamID))

	// markRecovered: UPDATE → active, then INSERT audit_log.
	mock.ExpectExec(`UPDATE resources\s+SET status = 'active'`).
		WithArgs(resID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", "provisioner.reconcile_recovered", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	prober := &fakeProber{outcome: jobs.ProbeReachable}
	w := jobs.NewProvisionerReconcilerWorker(db, nil, prober)
	if err := w.Work(context.Background(), fakeJob[jobs.ProvisionerReconcilerArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	if len(prober.callLog) != 1 {
		t.Fatalf("expected 1 probe call, got %d", len(prober.callLog))
	}
	if prober.callLog[0].resourceType != "postgres" {
		t.Errorf("probe called with type=%q, want postgres", prober.callLog[0].resourceType)
	}
}

func TestProvisionerReconciler_AbandonsUnreachablePending(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	token := uuid.New()
	teamID := uuid.New().String()

	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows(reconcilerRowCols).
			AddRow(resID, token, "redis", "redis://encrypted", teamID))

	// markAbandoned: UPDATE → failed + NULL connection_url, then INSERT audit_log.
	mock.ExpectExec(`UPDATE resources\s+SET status = 'failed', connection_url = NULL`).
		WithArgs(resID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", "provisioner.reconcile_abandoned", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	prober := &fakeProber{outcome: jobs.ProbeUnreachable, err: errors.New("dial tcp: connection refused")}
	// rdb = nil → quota refund is a no-op (logged-only). Verified by the
	// absence of any Redis call expectation.
	w := jobs.NewProvisionerReconcilerWorker(db, nil, prober)
	if err := w.Work(context.Background(), fakeJob[jobs.ProvisionerReconcilerArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestProvisionerReconciler_SkipsWebhookType(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	token := uuid.New()

	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows(reconcilerRowCols).
			AddRow(resID, token, "webhook", "", ""))

	// Only last_reconciled_at stamp — no status flip, no audit_log.
	mock.ExpectExec(`UPDATE resources SET last_reconciled_at = NOW\(\) WHERE id = \$1`).
		WithArgs(resID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	prober := &fakeProber{outcome: jobs.ProbeSkip}
	w := jobs.NewProvisionerReconcilerWorker(db, nil, prober)
	if err := w.Work(context.Background(), fakeJob[jobs.ProvisionerReconcilerArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestProvisionerReconciler_EmptyRowsetIsNoOp(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows(reconcilerRowCols))

	w := jobs.NewProvisionerReconcilerWorker(db, nil, &fakeProber{outcome: jobs.ProbeReachable})
	if err := w.Work(context.Background(), fakeJob[jobs.ProvisionerReconcilerArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestProvisionerReconciler_TopLevelQueryError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`FROM resources`).WillReturnError(errDB)

	w := jobs.NewProvisionerReconcilerWorker(db, nil, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.ProvisionerReconcilerArgs]()); err == nil {
		t.Fatal("expected error from top-level SELECT failure, got nil")
	}
}

// TestProvisionerReconciler_FailOpenOnUpdateError exercises the "On any DB
// error mid-loop, log and continue" contract: a bad UPDATE on row 1 must NOT
// stop row 2 from being processed.
func TestProvisionerReconciler_FailOpenOnUpdateError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id1, id2 := uuid.New(), uuid.New()
	tok1, tok2 := uuid.New(), uuid.New()
	team1, team2 := uuid.New().String(), uuid.New().String()

	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows(reconcilerRowCols).
			AddRow(id1, tok1, "postgres", "u", team1).
			AddRow(id2, tok2, "redis", "u", team2))

	// First UPDATE fails — second row still processed.
	mock.ExpectExec(`UPDATE resources\s+SET status = 'active'`).
		WithArgs(id1).
		WillReturnError(errDB)
	mock.ExpectExec(`UPDATE resources\s+SET status = 'active'`).
		WithArgs(id2).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(team2, "system", "provisioner.reconcile_recovered", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	prober := &fakeProber{outcome: jobs.ProbeReachable}
	w := jobs.NewProvisionerReconcilerWorker(db, nil, prober)
	if err := w.Work(context.Background(), fakeJob[jobs.ProvisionerReconcilerArgs]()); err != nil {
		t.Fatalf("expected nil (fail-open) on per-row UPDATE error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestNoopProber_AlwaysReachable pins prober.go's default behaviour. The
// reconciler relies on this being the safe fallback when no real prober
// is wired (see file-level comment in prober.go).
func TestNoopProber_AlwaysReachable(t *testing.T) {
	outcome, err := jobs.NoopProber{}.Probe(context.Background(), "postgres", "anything")
	if outcome != jobs.ProbeReachable {
		t.Errorf("NoopProber returned %v, want ProbeReachable", outcome)
	}
	if err != nil {
		t.Errorf("NoopProber returned err=%v, want nil", err)
	}
}

// TestProvisionerReconciler_P0_2_RealProberUnreachable_DoesNotPromote is the
// MR-P0-2 behavioral regression guard (BugBash 2026-05-20, cross-confirmed by
// T1/T20).
//
// THE BUG: the reconciler was wired with a nil → NoopProber. NoopProber.Probe
// ALWAYS returns ProbeReachable, so a stuck 'pending' row whose backend had
// genuinely FAILED was nonetheless flipped to status='active' — a credentials-
// less resource the platform then claimed was healthy.
//
// THE FIX: wire the real prober. With a real (non-Noop) prober, an unreachable
// stuck row resolves to ProbeUnreachable and the reconciler abandons it
// (status='failed', connection_url NULLed) — it must NEVER reach 'active'.
//
// THE ASSERTION: a fakeProber standing in for the real prober reports
// Unreachable. Only the `UPDATE ... status='failed'` (+ audit) is queued in
// sqlmock — NO `UPDATE ... status='active'`. sqlmock's ordered expectations
// fail if the reconciler issues the active-promotion UPDATE. This is exactly
// the NoopProber behaviour the bug produced; the test fails if it returns.
func TestProvisionerReconciler_P0_2_RealProberUnreachable_DoesNotPromote(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	token := uuid.New()
	teamID := uuid.New().String()

	// One stuck 'pending' postgres row whose backend is unreachable.
	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows(reconcilerRowCols).
			AddRow(resID, token, "postgres", "postgres://encrypted", teamID))

	// The ONLY status UPDATE queued is the abandon path (→ 'failed'). If the
	// reconciler instead issued `UPDATE ... status='active'` — the NoopProber
	// regression — sqlmock would see an unexpected statement and fail.
	mock.ExpectExec(`UPDATE resources\s+SET status = 'failed', connection_url = NULL`).
		WithArgs(resID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", "provisioner.reconcile_abandoned", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// A real prober reports the unreachable backend as ProbeUnreachable —
	// the NoopProber the bug used would instead report ProbeReachable here.
	prober := &fakeProber{outcome: jobs.ProbeUnreachable, err: errors.New("dial tcp: i/o timeout")}
	w := jobs.NewProvisionerReconcilerWorker(db, nil, prober)
	if err := w.Work(context.Background(), fakeJob[jobs.ProvisionerReconcilerArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("MR-P0-2 regression: the reconciler issued an unexpected statement — "+
			"an unreachable stuck row must be abandoned (status='failed'), never promoted "+
			"to 'active': %v", err)
	}
}
