package jobs

// custom_domain_reconcile_keyset_test.go — keyset-pagination coverage for
// CustomDomainReconciler.listActiveDomains. Bug-bash 2026-06-03: the scan
// previously issued ONE unbounded `SELECT ... FROM custom_domains`; it now
// streams the non-terminal domains in keyset-paginated batches of
// customDomainScanBatchLimit (WHERE id::text > $cursor ORDER BY id::text ASC
// LIMIT n), returning the WHOLE non-terminal set per call.
//
// Mirrors the orphan_sweep fetchLiveStackIDs keyset tests: multi-page advance
// (a FULL first page forces a second query whose cursor is page 1's tail),
// second-page error, and mid-stream rows.Err().

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

// cdKeysetID renders a zero-padded, lexicographically-sortable UUID matching
// the id::text keyset ordering.
func cdKeysetID(i int) uuid.UUID {
	return uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", i))
}

// TestCustomDomain_ListActiveDomains_KeysetPagination proves listActiveDomains
// pages through a non-terminal set larger than one batch. Page 1 is FULL
// (customDomainScanBatchLimit rows) so the loop must issue a SECOND query whose
// cursor ($3) is page 1's tail id; page 2 is short → the scan ends. Every
// domain from BOTH pages is returned, and the second query's keyset arg equals
// page 1's tail.
func TestCustomDomain_ListActiveDomains_KeysetPagination(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	cols := []string{"id", "hostname", "verification_token", "status", "created_at"}
	page1 := sqlmock.NewRows(cols)
	var lastPage1ID uuid.UUID
	for i := 0; i < customDomainScanBatchLimit; i++ {
		id := cdKeysetID(i)
		page1.AddRow(id, "h.example.com", "tok", statusPending, time.Now())
		lastPage1ID = id
	}
	page2ID := cdKeysetID(customDomainScanBatchLimit)

	queryRE := `SELECT id, hostname[\s\S]+FROM custom_domains[\s\S]+id::text > \$3[\s\S]+ORDER BY id::text ASC[\s\S]+LIMIT \$4`
	mock.ExpectQuery(queryRE).
		WithArgs(statusLive, statusFailed, "", customDomainScanBatchLimit).
		WillReturnRows(page1)
	mock.ExpectQuery(queryRE).
		WithArgs(statusLive, statusFailed, lastPage1ID.String(), customDomainScanBatchLimit).
		WillReturnRows(sqlmock.NewRows(cols).AddRow(page2ID, "h2.example.com", "tok", statusPending, time.Now()))

	r := &CustomDomainReconciler{db: db}
	got, err := r.listActiveDomains(context.Background())
	if err != nil {
		t.Fatalf("listActiveDomains: %v", err)
	}
	wantCount := customDomainScanBatchLimit + 1
	if len(got) != wantCount {
		t.Fatalf("domain count = %d; want %d (both pages merged)", len(got), wantCount)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations (keyset did not advance to page 2 with the right cursor): %v", err)
	}
}

// TestCustomDomain_ListActiveDomains_SecondPageError proves a DB error on a
// LATER page propagates — listActiveDomains must not return a partial set
// (which could leave a live domain un-reconciled).
func TestCustomDomain_ListActiveDomains_SecondPageError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	cols := []string{"id", "hostname", "verification_token", "status", "created_at"}
	page1 := sqlmock.NewRows(cols)
	for i := 0; i < customDomainScanBatchLimit; i++ {
		page1.AddRow(cdKeysetID(i), "h.example.com", "tok", statusPending, time.Now())
	}
	queryRE := `SELECT id, hostname[\s\S]+FROM custom_domains`
	mock.ExpectQuery(queryRE).WillReturnRows(page1)
	mock.ExpectQuery(queryRE).WillReturnError(errors.New("conn lost mid-sweep"))

	r := &CustomDomainReconciler{db: db}
	if _, err := r.listActiveDomains(context.Background()); err == nil {
		t.Fatal("expected error from second-page query failure, got nil")
	}
}

// TestCustomDomain_ListActiveDomains_KeysetRowsErr proves a mid-stream
// row-iteration error propagates.
func TestCustomDomain_ListActiveDomains_KeysetRowsErr(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	cols := []string{"id", "hostname", "verification_token", "status", "created_at"}
	rows := sqlmock.NewRows(cols).
		AddRow(cdKeysetID(0), "h.example.com", "tok", statusPending, time.Now()).
		RowError(0, errors.New("conn reset mid-stream"))
	mock.ExpectQuery(`SELECT id, hostname[\s\S]+FROM custom_domains`).
		WillReturnRows(rows)

	r := &CustomDomainReconciler{db: db}
	if _, err := r.listActiveDomains(context.Background()); err == nil {
		t.Fatal("expected rows.Err() to propagate, got nil")
	}
}
