package jobs_test

// payment_grace_terminator_test.go — hermetic tests for
// PaymentGraceTerminatorWorker.
//
// The api side is mocked via httptest — we assert that the worker
// (a) reads the expired-active rows from the dunning table, (b) POSTs
// to /internal/teams/:id/terminate with a Bearer token, and (c) emits
// the payment.grace_terminated audit row on a 2xx response. Failure
// paths assert NO audit emit.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	"instant.dev/worker/internal/jobs"
)

var graceTerminatorRowCols = []string{"id", "team_id", "expires_at"}

const (
	auditKindPaymentGraceTerminatedLiteral = "payment.grace_terminated"
	terminatorTestJWTSecret                = "test-shared-hmac-secret-32-bytes!"
)

// TestPaymentGraceTerminator_HappyPath covers the success flow: one
// expired-active row, the api returns 200, audit row is emitted.
func TestPaymentGraceTerminator_HappyPath(t *testing.T) {
	var (
		gotAuth string
		gotPath string
		hits    int32
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	graceID := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(-1 * time.Hour) // past expiry

	mock.ExpectQuery(`FROM payment_grace_periods`).
		WillReturnRows(sqlmock.NewRows(graceTerminatorRowCols).AddRow(graceID, teamID, expires))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", auditKindPaymentGraceTerminatedLiteral, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := jobs.NewPaymentGraceTerminatorWorker(db, srv.URL, terminatorTestJWTSecret, srv.Client())
	if err := w.Work(context.Background(), fakeJob[jobs.PaymentGraceTerminatorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	if hits != 1 {
		t.Fatalf("expected 1 api hit, got %d", hits)
	}
	if !strings.HasPrefix(gotAuth, "Bearer ") {
		t.Errorf("Authorization = %q, want Bearer …", gotAuth)
	}
	wantPath := "/internal/teams/" + teamID.String() + "/terminate"
	if gotPath != wantPath {
		t.Errorf("path = %q, want %q", gotPath, wantPath)
	}
}

// TestPaymentGraceTerminator_APIError_SkipsAudit covers the failure
// path: the api returns 500, so the worker MUST NOT emit the
// payment.grace_terminated audit row (lying about a non-event).
func TestPaymentGraceTerminator_APIError_SkipsAudit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	graceID := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(-1 * time.Hour)

	mock.ExpectQuery(`FROM payment_grace_periods`).
		WillReturnRows(sqlmock.NewRows(graceTerminatorRowCols).AddRow(graceID, teamID, expires))
	// No audit INSERT expected — strict mode fails if one fires.

	w := jobs.NewPaymentGraceTerminatorWorker(db, srv.URL, terminatorTestJWTSecret, srv.Client())
	if err := w.Work(context.Background(), fakeJob[jobs.PaymentGraceTerminatorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v (api failure is fail-open per-row)", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestPaymentGraceTerminator_MissingConfig_NoOpNoError covers the
// safe-default path: when INSTANT_API_INTERNAL_URL or
// WORKER_INTERNAL_JWT_SECRET is empty the worker logs a WARN and
// returns nil without touching the DB. Boot should not crash on a
// half-configured cluster.
func TestPaymentGraceTerminator_MissingConfig_NoOpNoError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	// No query expected — strict mode fails if one fires.

	w := jobs.NewPaymentGraceTerminatorWorker(db, "", terminatorTestJWTSecret, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.PaymentGraceTerminatorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestPaymentGraceTerminator_TopLevelQueryError_Returns covers the
// query-level failure path: River must see the error so it retries.
func TestPaymentGraceTerminator_TopLevelQueryError_Returns(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`FROM payment_grace_periods`).WillReturnError(errors.New("boom"))

	w := jobs.NewPaymentGraceTerminatorWorker(db, "http://fake", terminatorTestJWTSecret, &http.Client{})
	if err := w.Work(context.Background(), fakeJob[jobs.PaymentGraceTerminatorArgs]()); err == nil {
		t.Fatal("expected error from top-level SELECT, got nil")
	}
}
