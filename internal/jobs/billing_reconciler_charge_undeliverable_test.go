package jobs

// billing_reconciler_charge_undeliverable_test.go — B11-F3 (BugBash
// 2026-05-20). The api emits audit_log rows with
// kind='billing.charge_undeliverable' when a Razorpay webhook trust-pass
// fails. The worker scans for new rows on every reconciler tick and
// increments BillingChargeUndeliverableTotal so a single NR alert keys
// on the metric regardless of which service wrote the audit row.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"instant.dev/worker/internal/metrics"
)

// TestScanChargeUndeliverable_CountsNewRows seeds the audit_log with 2
// new rows; the scanner must increment by 2 and advance the cursor to
// the latest row.
func TestScanChargeUndeliverable_CountsNewRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	now := time.Now().UTC().Truncate(time.Second)
	row1 := now.Add(-30 * time.Minute)
	row2 := now.Add(-5 * time.Minute)

	mock.ExpectQuery(`SELECT created_at FROM audit_log`).
		WithArgs(chargeUndeliverableAuditKind, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}).
			AddRow(row1).
			AddRow(row2))

	w := &BillingReconcilerWorker{db: db}

	before := testutil.ToFloat64(metrics.BillingChargeUndeliverableTotal)
	got := w.scanChargeUndeliverable(context.Background())
	after := testutil.ToFloat64(metrics.BillingChargeUndeliverableTotal)

	if got != 2 {
		t.Fatalf("scanChargeUndeliverable: got %d new rows, want 2", got)
	}
	if delta := after - before; delta != 2 {
		t.Fatalf("counter delta: got %f, want 2", delta)
	}
	w.chargeUndeliverableMu.Lock()
	if !w.chargeUndeliverableCursor.Equal(row2) {
		t.Fatalf("cursor: got %v, want %v", w.chargeUndeliverableCursor, row2)
	}
	w.chargeUndeliverableMu.Unlock()
}

// TestScanChargeUndeliverable_NoNewRows verifies a zero-row scan does
// NOT increment the metric but DOES advance the cursor to now().
func TestScanChargeUndeliverable_NoNewRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT created_at FROM audit_log`).
		WithArgs(chargeUndeliverableAuditKind, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}))

	w := &BillingReconcilerWorker{db: db}
	prev := time.Now().UTC().Add(-2 * time.Hour)
	w.chargeUndeliverableMu.Lock()
	w.chargeUndeliverableCursor = prev
	w.chargeUndeliverableMu.Unlock()

	before := testutil.ToFloat64(metrics.BillingChargeUndeliverableTotal)
	got := w.scanChargeUndeliverable(context.Background())
	after := testutil.ToFloat64(metrics.BillingChargeUndeliverableTotal)

	if got != 0 {
		t.Fatalf("expected 0 new rows, got %d", got)
	}
	if after != before {
		t.Fatalf("counter must not advance on zero-row scan: before=%f after=%f", before, after)
	}
	w.chargeUndeliverableMu.Lock()
	defer w.chargeUndeliverableMu.Unlock()
	if !w.chargeUndeliverableCursor.After(prev) {
		t.Fatalf("cursor should advance to now() even on zero rows: prev=%v cur=%v", prev, w.chargeUndeliverableCursor)
	}
}

// TestScanChargeUndeliverable_DBErrorFailsOpen — on DB error, returns 0
// and does NOT advance the cursor.
func TestScanChargeUndeliverable_DBErrorFailsOpen(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT created_at FROM audit_log`).
		WillReturnError(errors.New("connection refused"))

	w := &BillingReconcilerWorker{db: db}
	seed := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Second)
	w.chargeUndeliverableMu.Lock()
	w.chargeUndeliverableCursor = seed
	w.chargeUndeliverableMu.Unlock()

	got := w.scanChargeUndeliverable(context.Background())
	if got != 0 {
		t.Fatalf("on DB error: want 0, got %d", got)
	}
	w.chargeUndeliverableMu.Lock()
	defer w.chargeUndeliverableMu.Unlock()
	if !w.chargeUndeliverableCursor.Equal(seed) {
		t.Fatalf("cursor must NOT advance on DB error: seed=%v cur=%v", seed, w.chargeUndeliverableCursor)
	}
}

// TestScanChargeUndeliverable_FirstTickUsesLookback — zero-value cursor
// causes the scanner to seed at now()-1h on the first tick after pod
// boot.
func TestScanChargeUndeliverable_FirstTickUsesLookback(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT created_at FROM audit_log`).
		WithArgs(chargeUndeliverableAuditKind, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}))

	w := &BillingReconcilerWorker{db: db}
	before := time.Now().UTC()
	_ = w.scanChargeUndeliverable(context.Background())

	w.chargeUndeliverableMu.Lock()
	cursor := w.chargeUndeliverableCursor
	w.chargeUndeliverableMu.Unlock()
	if cursor.Before(before) {
		t.Fatalf("first-tick cursor should advance to ~now: cursor=%v before=%v", cursor, before)
	}
}
