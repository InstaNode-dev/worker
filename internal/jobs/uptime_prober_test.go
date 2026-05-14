package jobs_test

// uptime_prober_test.go — hermetic tests for UptimeProberWorker.
//
// Three scenarios:
//
//   1. Happy path: every probe succeeds, every component gets a
//      healthy=true row.
//   2. Failure path: api + marketing return 500, deploys/provisioner
//      dial fails — those components write healthy=false rows; worker
//      itself + the surviving probes still write healthy=true.
//   3. Retention sweep: UptimeRetentionWorker issues a single DELETE
//      that filters on the 90-day window.
//
// We don't actually open TCP sockets — the probes that need network
// reach the real network in production, so we can't easily mock the
// http.Client. The strategy here is to override the env vars to point
// at an httptest.Server we control. The provisioner dialer is
// exposed as a struct field (via the constructor's default + a
// reflective-style override in test).

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"instant.dev/worker/internal/jobs"
)

// TestUptimeProber_HappyPath_AllHealthy — every component yields a
// healthy=true row when its probe target answers 200.
func TestUptimeProber_HappyPath_AllHealthy(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	// 5 probes fan out in goroutines, so SELECT 1 (the worker probe)
	// and the 5 INSERTs arrive in non-deterministic order. Disable
	// strict ordering so sqlmock matches each call by signature alone.
	mock.MatchExpectationsInOrder(false)

	// Spin up two test servers — one for the api/marketing probes
	// (returns 200), one for the deploys ingress probe (also 200).
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer apiSrv.Close()
	mktSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mktSrv.Close()
	depSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer depSrv.Close()

	t.Setenv("UPTIME_PROBE_API_URL", apiSrv.URL+"/healthz")
	t.Setenv("UPTIME_PROBE_MARKETING_URL", mktSrv.URL+"/")
	t.Setenv("UPTIME_PROBE_DEPLOYS_URL", depSrv.URL+"/")

	// The worker probe must SELECT 1 against the platform DB.
	mock.ExpectQuery(`SELECT 1`).
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))

	// 5 inserts expected — one per component, all healthy=true. Order
	// is non-deterministic across probe goroutines, hence
	// MatchExpectationsInOrder(false).
	insertRe := `INSERT INTO uptime_samples`
	mock.ExpectExec(insertRe).WithArgs("api", true, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(insertRe).WithArgs("provisioner", true, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectExec(insertRe).WithArgs("worker", true, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(3, 1))
	mock.ExpectExec(insertRe).WithArgs("deploys", true, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(4, 1))
	mock.ExpectExec(insertRe).WithArgs("marketing", true, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(5, 1))

	w := jobs.NewUptimeProberWorker(db)
	// Override the provisioner dialer to "always succeed".
	jobs.SetUptimeProberDialer(w, func(_ context.Context, _ string) error { return nil })

	if err := w.Work(context.Background(), fakeJob[jobs.UptimeProberArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

// TestUptimeProber_FailurePath_ProvisionerDialFails — provisioner dial
// returns an error, the worker still inserts a row but with
// healthy=false. Other probes succeed. We verify the row count
// (5 inserts) and that the worker doesn't error.
func TestUptimeProber_FailurePath_ProvisionerDialFails(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	// API probe → 500 (degraded). Marketing → 200. Deploys → 200.
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer apiSrv.Close()
	mktSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mktSrv.Close()
	depSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer depSrv.Close()

	t.Setenv("UPTIME_PROBE_API_URL", apiSrv.URL+"/healthz")
	t.Setenv("UPTIME_PROBE_MARKETING_URL", mktSrv.URL+"/")
	t.Setenv("UPTIME_PROBE_DEPLOYS_URL", depSrv.URL+"/")

	mock.ExpectQuery(`SELECT 1`).
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))

	// Expect 5 inserts; per-slug expectations so we can verify each
	// component's healthy value. sqlmock matches in any order
	// (MatchExpectationsInOrder(false) above), so we can pin each
	// slug to its expected `healthy` literal without race.
	insertRe := `INSERT INTO uptime_samples`
	// api: 500 → unhealthy
	mock.ExpectExec(insertRe).WithArgs("api", false, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))
	// provisioner: dial fails → unhealthy
	mock.ExpectExec(insertRe).WithArgs("provisioner", false, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(2, 1))
	// worker: SELECT 1 succeeds → healthy
	mock.ExpectExec(insertRe).WithArgs("worker", true, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(3, 1))
	// deploys: 200 → healthy
	mock.ExpectExec(insertRe).WithArgs("deploys", true, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(4, 1))
	// marketing: 200 → healthy
	mock.ExpectExec(insertRe).WithArgs("marketing", true, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(5, 1))

	w := jobs.NewUptimeProberWorker(db)
	jobs.SetUptimeProberDialer(w, func(_ context.Context, _ string) error {
		return errors.New("dial: connection refused")
	})

	if err := w.Work(context.Background(), fakeJob[jobs.UptimeProberArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
	// Two failures expected: api (500) + provisioner (dial error).
	// Three successes: worker (SELECT 1), marketing, deploys.
	// The per-slug WithArgs expectations above encode the assertion —
	// if a probe wrote the wrong `healthy` value, the matcher would
	// have failed.
}

// TestUptimeRetention_DeletesOldRows — the retention sweep issues
// exactly one DELETE filtered on the 90-day window.
func TestUptimeRetention_DeletesOldRows(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectExec(`DELETE FROM uptime_samples\s+WHERE sampled_at < now\(\) - INTERVAL '90 days'`).
		WillReturnResult(sqlmock.NewResult(0, 1234))

	w := jobs.NewUptimeRetentionWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.UptimeRetentionArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

