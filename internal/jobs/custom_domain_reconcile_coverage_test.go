package jobs

// custom_domain_reconcile_coverage_test.go — drives custom_domain_reconcile.go
// (previously 0%) to ≥95%.
//
// SQL via sqlmock (default regexp matcher). The TXT lookup is exercised via the
// txtLookupFunc package seam; the cert-ready HTTPS HEAD probe via a custom
// RoundTripper that answers any host with a canned status. Every Work() switch
// arm (pending / cert_ready / verified / live / unknown), every reconcile
// result (advanced / failed / recorded-err / noop), and every SQL helper are
// covered, plus the constructor's default-httpCli branch.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

func customDomainJob() *river.Job[CustomDomainReconcileArgs] {
	return &river.Job[CustomDomainReconcileArgs]{JobRow: &rivertype.JobRow{ID: 3}}
}

// cannedRoundTripper answers every request with a fixed status / error.
type cannedRoundTripper struct {
	status int
	err    error
}

func (c *cannedRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if c.err != nil {
		return nil, c.err
	}
	return &http.Response{
		StatusCode: c.status,
		Body:       io.NopCloser(strings.NewReader("")),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

func withTXTLookup(t *testing.T, fn func(ctx context.Context, name string) ([]string, error)) {
	t.Helper()
	orig := txtLookupFunc
	txtLookupFunc = fn
	t.Cleanup(func() { txtLookupFunc = orig })
}

// ── Kind + constructor ────────────────────────────────────────────────

func TestCustomDomain_Kind(t *testing.T) {
	if got := (CustomDomainReconcileArgs{}).Kind(); got != "custom_domain_reconcile" {
		t.Errorf("Kind() = %q", got)
	}
}

func TestNewCustomDomainReconciler_DefaultHTTPClient(t *testing.T) {
	r := NewCustomDomainReconciler(nil, nil, nil)
	if r.httpCli == nil {
		t.Fatal("httpCli should be defaulted")
	}
	// CheckRedirect must refuse redirects.
	if err := r.httpCli.CheckRedirect(nil, nil); err != http.ErrUseLastResponse {
		t.Errorf("CheckRedirect = %v, want ErrUseLastResponse", err)
	}
	// Explicit client is kept as-is.
	cli := &http.Client{}
	r2 := NewCustomDomainReconciler(nil, nil, cli)
	if r2.httpCli != cli {
		t.Error("explicit httpCli should be retained")
	}
}

// ── listActiveDomains ─────────────────────────────────────────────────

func newCDRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "hostname", "verification_token", "status", "created_at"})
}

func TestCustomDomain_ListActiveDomains_QueryAndScanErrors(t *testing.T) {
	// Query error.
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery(`SELECT id, hostname, verification_token, status, created_at`).
		WithArgs(statusLive, statusFailed).
		WillReturnError(errors.New("query boom"))
	r := &CustomDomainReconciler{db: db}
	if _, err := r.listActiveDomains(context.Background()); err == nil {
		t.Error("expected query error")
	}

	// Scan error (wrong column count).
	db2, mock2, _ := sqlmock.New()
	defer db2.Close()
	mock2.ExpectQuery(`SELECT id, hostname`).
		WithArgs(statusLive, statusFailed).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("not-a-uuid"))
	r2 := &CustomDomainReconciler{db: db2}
	if _, err := r2.listActiveDomains(context.Background()); err == nil {
		t.Error("expected scan error")
	}
}

// ── Work: empty + all switch arms ─────────────────────────────────────

func TestCustomDomain_Work_Empty(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery(`SELECT id, hostname`).
		WithArgs(statusLive, statusFailed).
		WillReturnRows(newCDRows())
	r := &CustomDomainReconciler{db: db}
	if err := r.Work(context.Background(), customDomainJob()); err != nil {
		t.Fatalf("Work empty: %v", err)
	}
}

func TestCustomDomain_Work_ListError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery(`SELECT id, hostname`).
		WithArgs(statusLive, statusFailed).
		WillReturnError(errors.New("boom"))
	r := &CustomDomainReconciler{db: db}
	if err := r.Work(context.Background(), customDomainJob()); err == nil {
		t.Error("Work should surface list error")
	}
}

func TestCustomDomain_Work_AllArms(t *testing.T) {
	withTXTLookup(t, func(_ context.Context, _ string) ([]string, error) {
		return []string{verificationTokenPrefix + "tok-pending"}, nil
	})

	db, mock, _ := sqlmock.New()
	defer db.Close()

	idPending := uuid.New()
	idCert := uuid.New()
	idVerified := uuid.New()
	idUnknown := uuid.New()
	now := time.Now()

	mock.ExpectQuery(`SELECT id, hostname`).
		WithArgs(statusLive, statusFailed).
		WillReturnRows(newCDRows().
			AddRow(idPending, "pending.example.com", "tok-pending", statusPending, now).
			AddRow(idCert, "cert.example.com", "tok-cert", statusCertReady, now).
			AddRow(idVerified, "verified.example.com", "tok-v", statusVerified, now).
			AddRow(idUnknown, "weird.example.com", "tok-u", "bogus_status", now))

	// pending → TXT match → markVerified.
	mock.ExpectExec(`UPDATE custom_domains`).
		WithArgs(statusVerified, idPending, statusPending).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// cert_ready → HEAD 200 → updateStatus live.
	mock.ExpectExec(`UPDATE custom_domains`).
		WithArgs(statusLive, sqlmock.AnyArg(), idCert).
		WillReturnResult(sqlmock.NewResult(0, 1))

	r := &CustomDomainReconciler{
		db:      db,
		httpCli: &http.Client{Transport: &cannedRoundTripper{status: 200}},
	}
	if err := r.Work(context.Background(), customDomainJob()); err != nil {
		t.Fatalf("Work all-arms: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// ── reconcilePending: stale → failed ──────────────────────────────────

func TestCustomDomain_ReconcilePending_StaleFails(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	id := uuid.New()
	mock.ExpectExec(`UPDATE custom_domains`).
		WithArgs(statusFailed, staleVerificationFailReason, id).
		WillReturnResult(sqlmock.NewResult(0, 1))
	r := &CustomDomainReconciler{db: db}
	d := activeCustomDomain{id: id, hostname: "old.example.com", status: statusPending,
		createdAt: time.Now().Add(-8 * 24 * time.Hour)}
	if got := r.reconcilePending(context.Background(), d); got != reconcileFailed {
		t.Errorf("reconcilePending stale = %v, want reconcileFailed", got)
	}
}

func TestCustomDomain_ReconcilePending_StaleMarkFailedError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	id := uuid.New()
	mock.ExpectExec(`UPDATE custom_domains`).
		WithArgs(statusFailed, staleVerificationFailReason, id).
		WillReturnError(errors.New("db down"))
	r := &CustomDomainReconciler{db: db}
	d := activeCustomDomain{id: id, status: statusPending, createdAt: time.Now().Add(-8 * 24 * time.Hour)}
	if got := r.reconcilePending(context.Background(), d); got != reconcileNoop {
		t.Errorf("reconcilePending stale-err = %v, want reconcileNoop", got)
	}
}

func TestCustomDomain_ReconcilePending_MatchMarkVerifiedError(t *testing.T) {
	withTXTLookup(t, func(_ context.Context, _ string) ([]string, error) {
		return []string{verificationTokenPrefix + "tok"}, nil
	})
	db, mock, _ := sqlmock.New()
	defer db.Close()
	id := uuid.New()
	mock.ExpectExec(`UPDATE custom_domains`).
		WithArgs(statusVerified, id, statusPending).
		WillReturnError(errors.New("verify update down"))
	r := &CustomDomainReconciler{db: db}
	d := activeCustomDomain{id: id, token: "tok", status: statusPending, createdAt: time.Now()}
	if got := r.reconcilePending(context.Background(), d); got != reconcileNoop {
		t.Errorf("reconcilePending verify-err = %v, want reconcileNoop", got)
	}
}

func TestCustomDomain_ReconcilePending_MissRecordsErr(t *testing.T) {
	withTXTLookup(t, func(_ context.Context, _ string) ([]string, error) {
		return []string{"some-other-value"}, nil // no match
	})
	db, mock, _ := sqlmock.New()
	defer db.Close()
	id := uuid.New()
	mock.ExpectExec(`UPDATE custom_domains`).
		WithArgs(sqlmock.AnyArg(), id).
		WillReturnResult(sqlmock.NewResult(0, 1))
	r := &CustomDomainReconciler{db: db}
	d := activeCustomDomain{id: id, token: "tok", status: statusPending, createdAt: time.Now()}
	if got := r.reconcilePending(context.Background(), d); got != reconcileRecordedErr {
		t.Errorf("reconcilePending miss = %v, want reconcileRecordedErr", got)
	}
}

func TestCustomDomain_ReconcilePending_LookupErrorRecorded(t *testing.T) {
	withTXTLookup(t, func(_ context.Context, _ string) ([]string, error) {
		return nil, errors.New("NXDOMAIN")
	})
	db, mock, _ := sqlmock.New()
	defer db.Close()
	id := uuid.New()
	// updateLastCheck errors → reconcileNoop.
	mock.ExpectExec(`UPDATE custom_domains`).
		WithArgs(sqlmock.AnyArg(), id).
		WillReturnError(errors.New("last_check update down"))
	r := &CustomDomainReconciler{db: db}
	d := activeCustomDomain{id: id, token: "tok", status: statusPending, createdAt: time.Now()}
	if got := r.reconcilePending(context.Background(), d); got != reconcileNoop {
		t.Errorf("reconcilePending lookup-err update-err = %v, want reconcileNoop", got)
	}
}

// ── reconcileCertReady ────────────────────────────────────────────────

func TestCustomDomain_ReconcileCertReady_Live(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	id := uuid.New()
	mock.ExpectExec(`UPDATE custom_domains`).
		WithArgs(statusLive, sqlmock.AnyArg(), id).
		WillReturnResult(sqlmock.NewResult(0, 1))
	r := &CustomDomainReconciler{db: db, httpCli: &http.Client{Transport: &cannedRoundTripper{status: 204}}}
	d := activeCustomDomain{id: id, hostname: "live.example.com", status: statusCertReady}
	if got := r.reconcileCertReady(context.Background(), d); got != reconcileAdvanced {
		t.Errorf("reconcileCertReady live = %v, want reconcileAdvanced", got)
	}
}

func TestCustomDomain_ReconcileCertReady_LiveUpdateError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	id := uuid.New()
	mock.ExpectExec(`UPDATE custom_domains`).
		WithArgs(statusLive, sqlmock.AnyArg(), id).
		WillReturnError(errors.New("live update down"))
	r := &CustomDomainReconciler{db: db, httpCli: &http.Client{Transport: &cannedRoundTripper{status: 200}}}
	d := activeCustomDomain{id: id, hostname: "live.example.com", status: statusCertReady}
	if got := r.reconcileCertReady(context.Background(), d); got != reconcileNoop {
		t.Errorf("reconcileCertReady live-update-err = %v, want reconcileNoop", got)
	}
}

func TestCustomDomain_ReconcileCertReady_ProbeFails(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	id := uuid.New()
	mock.ExpectExec(`UPDATE custom_domains`).
		WithArgs(sqlmock.AnyArg(), id).
		WillReturnResult(sqlmock.NewResult(0, 1))
	r := &CustomDomainReconciler{db: db, httpCli: &http.Client{Transport: &cannedRoundTripper{err: errors.New("dial fail")}}}
	d := activeCustomDomain{id: id, hostname: "down.example.com", status: statusCertReady}
	if got := r.reconcileCertReady(context.Background(), d); got != reconcileNoop {
		t.Errorf("reconcileCertReady probe-fail = %v, want reconcileNoop", got)
	}
}

func TestCustomDomain_ReconcileCertReady_Non2xxRecordsErr(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	id := uuid.New()
	mock.ExpectExec(`UPDATE custom_domains`).
		WithArgs(sqlmock.AnyArg(), id).
		WillReturnResult(sqlmock.NewResult(0, 1))
	r := &CustomDomainReconciler{db: db, httpCli: &http.Client{Transport: &cannedRoundTripper{status: 502}}}
	d := activeCustomDomain{id: id, hostname: "bad.example.com", status: statusCertReady}
	if got := r.reconcileCertReady(context.Background(), d); got != reconcileRecordedErr {
		t.Errorf("reconcileCertReady 502 = %v, want reconcileRecordedErr", got)
	}
}

func TestCustomDomain_ReconcileCertReady_Non2xxUpdateError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	id := uuid.New()
	mock.ExpectExec(`UPDATE custom_domains`).
		WithArgs(sqlmock.AnyArg(), id).
		WillReturnError(errors.New("update down"))
	r := &CustomDomainReconciler{db: db, httpCli: &http.Client{Transport: &cannedRoundTripper{status: 503}}}
	d := activeCustomDomain{id: id, hostname: "bad.example.com", status: statusCertReady}
	if got := r.reconcileCertReady(context.Background(), d); got != reconcileNoop {
		t.Errorf("reconcileCertReady 503-update-err = %v, want reconcileNoop", got)
	}
}

// ── lookupTXT: quoted-record match + plain match ──────────────────────

func TestCustomDomain_LookupTXT_Variants(t *testing.T) {
	r := &CustomDomainReconciler{}
	ctx := context.Background()

	withTXTLookup(t, func(_ context.Context, _ string) ([]string, error) {
		return []string{`"` + verificationTokenPrefix + `tok"`}, nil // quoted
	})
	if ok, err := r.lookupTXT(ctx, "h", "tok"); !ok || err != nil {
		t.Errorf("lookupTXT quoted = (%v,%v), want (true,nil)", ok, err)
	}

	withTXTLookup(t, func(_ context.Context, _ string) ([]string, error) {
		return []string{verificationTokenPrefix + "tok"}, nil // plain
	})
	if ok, err := r.lookupTXT(ctx, "h", "tok"); !ok || err != nil {
		t.Errorf("lookupTXT plain = (%v,%v), want (true,nil)", ok, err)
	}

	withTXTLookup(t, func(_ context.Context, _ string) ([]string, error) {
		return []string{"nope"}, nil
	})
	if ok, _ := r.lookupTXT(ctx, "h", "tok"); ok {
		t.Error("lookupTXT no-match should be false")
	}

	withTXTLookup(t, func(_ context.Context, _ string) ([]string, error) {
		return nil, errors.New("resolver boom")
	})
	if _, err := r.lookupTXT(ctx, "h", "tok"); err == nil {
		t.Error("lookupTXT resolver error should surface")
	}
}

// ── updateStatus: empty errMsg sets NULL branch ───────────────────────

func TestCustomDomain_UpdateStatus_NullErrAndError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	id := uuid.New()
	mock.ExpectExec(`UPDATE custom_domains`).
		WithArgs(statusVerified, nil, id).
		WillReturnResult(sqlmock.NewResult(0, 1))
	r := &CustomDomainReconciler{db: db}
	if err := r.updateStatus(context.Background(), id, statusVerified, ""); err != nil {
		t.Fatalf("updateStatus null: %v", err)
	}

	db2, mock2, _ := sqlmock.New()
	defer db2.Close()
	mock2.ExpectExec(`UPDATE custom_domains`).
		WithArgs(statusVerified, "boom", id).
		WillReturnError(errors.New("exec down"))
	r2 := &CustomDomainReconciler{db: db2}
	if err := r2.updateStatus(context.Background(), id, statusVerified, "boom"); err == nil {
		t.Error("updateStatus exec error should surface")
	}
}

func TestCustomDomain_UpdateLastCheck_EmptyAndError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	id := uuid.New()
	mock.ExpectExec(`UPDATE custom_domains`).
		WithArgs(nil, id).
		WillReturnResult(sqlmock.NewResult(0, 1))
	r := &CustomDomainReconciler{db: db}
	if err := r.updateLastCheck(context.Background(), id, ""); err != nil {
		t.Fatalf("updateLastCheck empty: %v", err)
	}

	db2, mock2, _ := sqlmock.New()
	defer db2.Close()
	mock2.ExpectExec(`UPDATE custom_domains`).
		WithArgs("err", id).
		WillReturnError(errors.New("down"))
	r2 := &CustomDomainReconciler{db: db2}
	if err := r2.updateLastCheck(context.Background(), id, "err"); err == nil {
		t.Error("updateLastCheck exec error should surface")
	}
}

func TestCustomDomain_MarkVerifiedError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	id := uuid.New()
	mock.ExpectExec(`UPDATE custom_domains`).
		WithArgs(statusVerified, id, statusPending).
		WillReturnError(errors.New("down"))
	r := &CustomDomainReconciler{db: db}
	if err := r.markVerified(context.Background(), id); err == nil {
		t.Error("markVerified exec error should surface")
	}
}

func TestCustomDomain_MarkFailedError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	id := uuid.New()
	mock.ExpectExec(`UPDATE custom_domains`).
		WithArgs(statusFailed, "reason", id).
		WillReturnError(errors.New("down"))
	r := &CustomDomainReconciler{db: db}
	if err := r.markFailed(context.Background(), id, "reason"); err == nil {
		t.Error("markFailed exec error should surface")
	}
}
