package jobs_test

// expire_keyset_test.go — keyset-pagination coverage for the two TTL reaper
// batch scans (ExpireAnonymousWorker.Work + ExpireStacksWorker.Work). Bug-bash
// 2026-06-03: each reaper previously issued ONE unbounded batch SELECT; both
// now stream candidates in keyset-paginated batches (WHERE id::text > $cursor
// ORDER BY id::text ASC LIMIT n), processing the WHOLE expired set per tick.
//
// Mirrors the orphan_sweep fetchLiveStackIDs keyset tests: multi-page advance
// (a FULL first page forces a second query whose cursor is page 1's tail),
// second-page error, and mid-stream rows.Err().

import (
	"context"
	"errors"
	"fmt"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"instant.dev/worker/internal/jobs"
)

// ── ExpireAnonymousWorker.Work ────────────────────────────────────────────

// expireBatchCols is the 4-column projection the reaper batch SELECT scans.
var expireBatchCols = []string{"id", "token", "resource_type", "provider_resource_id"}

// expireUUID renders a zero-padded, lexicographically-sortable id matching the
// id::text keyset ordering.
func expireUUID(i int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", i) }

// TestExpireAnonymousWorker_KeysetPagination proves the reaper pages through an
// expired set larger than one batch. Page 1 is FULL (jobs.ExpireScanBatchLimit
// rows) so the loop must issue a SECOND batch query whose cursor ($1) is page
// 1's tail id; page 2 is short → the scan ends. Every candidate is then reaped;
// to keep this test focused on the keyset advance, the per-row FOR UPDATE
// re-confirm returns false (race_skipped) so no UPDATE fires.
func TestExpireAnonymousWorker_KeysetPagination(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	page1 := sqlmock.NewRows(expireBatchCols)
	var lastPage1ID string
	for i := 0; i < jobs.ExpireScanBatchLimit; i++ {
		id := expireUUID(i)
		page1.AddRow(id, "tok", "postgres", "")
		lastPage1ID = id
	}
	page2ID := expireUUID(jobs.ExpireScanBatchLimit)

	batchRE := `SELECT r\.id::text, r\.token::text[\s\S]+FROM resources r[\s\S]+r\.id::text > \$1[\s\S]+ORDER BY r\.id::text ASC[\s\S]+LIMIT \$2`
	mock.ExpectQuery(batchRE).
		WithArgs("", jobs.ExpireScanBatchLimit).
		WillReturnRows(page1)
	mock.ExpectQuery(batchRE).
		WithArgs(lastPage1ID, jobs.ExpireScanBatchLimit).
		WillReturnRows(sqlmock.NewRows(expireBatchCols).AddRow(page2ID, "tok", "postgres", ""))

	// Per-candidate reapOne: BeginTx → EXISTS re-confirm returns false
	// (race_skipped) → Rollback. No UPDATE. One set per candidate (101 total).
	total := jobs.ExpireScanBatchLimit + 1
	for i := 0; i < total; i++ {
		mock.ExpectBegin()
		mock.ExpectQuery(`SELECT EXISTS\s*\(\s*SELECT 1\s+FROM resources r`).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		mock.ExpectRollback()
	}
	// Final active-anonymous count metric query.
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	w := jobs.NewExpireAnonymousWorker(db, nil, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations (keyset did not advance to page 2 with the right cursor): %v", err)
	}
}

// TestExpireAnonymousWorker_SecondPageError proves a DB error on a LATER batch
// page propagates out of Work (no silent partial reap).
func TestExpireAnonymousWorker_SecondPageError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	page1 := sqlmock.NewRows(expireBatchCols)
	for i := 0; i < jobs.ExpireScanBatchLimit; i++ {
		page1.AddRow(expireUUID(i), "tok", "postgres", "")
	}
	batchRE := `SELECT r\.id::text, r\.token::text[\s\S]+FROM resources r`
	mock.ExpectQuery(batchRE).WillReturnRows(page1)
	mock.ExpectQuery(batchRE).WillReturnError(errors.New("conn lost mid-sweep"))

	w := jobs.NewExpireAnonymousWorker(db, nil, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err == nil {
		t.Fatal("expected error from second-page query failure, got nil")
	}
}

// TestExpireAnonymousWorker_KeysetRowsErr proves a mid-stream row-iteration
// error propagates out of Work.
func TestExpireAnonymousWorker_KeysetRowsErr(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows(expireBatchCols).
		AddRow(expireUUID(0), "tok", "postgres", "").
		RowError(0, errors.New("conn reset mid-stream"))
	mock.ExpectQuery(`SELECT r\.id::text, r\.token::text[\s\S]+FROM resources r`).
		WillReturnRows(rows)

	w := jobs.NewExpireAnonymousWorker(db, nil, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err == nil {
		t.Fatal("expected rows.Err() to propagate, got nil")
	}
}

// ── ExpireStacksWorker.Work ───────────────────────────────────────────────

// expireStacksBatchCols is the 3-column projection the stack reaper scans.
var expireStacksBatchCols = []string{"id", "slug", "namespace"}

// TestExpireStacksWorker_KeysetPagination proves the stack reaper pages through
// an expired set larger than one batch. Page 1 is FULL
// (jobs.ExpireStacksScanBatchLimit rows) → a SECOND query fires with page 1's
// tail as the cursor; page 2 is short → the scan ends. The worker is built with
// an empty nsPrefix and no in-cluster client, so every expired row's namespace
// is non-empty AND k8sClient is nil → the "not in-cluster" branch logs and
// skips the DELETE (continue) — no DB DELETE fires, keeping the test focused on
// the keyset advance.
func TestExpireStacksWorker_KeysetPagination(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	page1 := sqlmock.NewRows(expireStacksBatchCols)
	var lastPage1ID string
	for i := 0; i < jobs.ExpireStacksScanBatchLimit; i++ {
		id := expireUUID(i)
		// Non-empty namespace + nil k8sClient → "not in-cluster" skip branch.
		page1.AddRow(id, "slug", "instant-stack-"+id)
		lastPage1ID = id
	}
	page2ID := expireUUID(jobs.ExpireStacksScanBatchLimit)

	batchRE := `FROM stacks[\s\S]+id::text > \$1[\s\S]+ORDER BY id::text ASC[\s\S]+LIMIT \$2`
	mock.ExpectQuery(batchRE).
		WithArgs("", jobs.ExpireStacksScanBatchLimit).
		WillReturnRows(page1)
	mock.ExpectQuery(batchRE).
		WithArgs(lastPage1ID, jobs.ExpireStacksScanBatchLimit).
		WillReturnRows(sqlmock.NewRows(expireStacksBatchCols).AddRow(page2ID, "slug", "instant-stack-"+page2ID))

	// nsPrefix "" + nil k8sClient (not in-cluster) → no DELETE FROM stacks.
	w := jobs.NewExpireStacksWorker(db, "")
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireStacksArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations (keyset did not advance to page 2 with the right cursor): %v", err)
	}
}

// TestExpireStacksWorker_SecondPageError proves a DB error on a LATER batch
// page propagates out of Work.
func TestExpireStacksWorker_SecondPageError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	page1 := sqlmock.NewRows(expireStacksBatchCols)
	for i := 0; i < jobs.ExpireStacksScanBatchLimit; i++ {
		id := expireUUID(i)
		page1.AddRow(id, "slug", "instant-stack-"+id)
	}
	batchRE := `FROM stacks`
	mock.ExpectQuery(batchRE).WillReturnRows(page1)
	mock.ExpectQuery(batchRE).WillReturnError(errors.New("conn lost mid-sweep"))

	w := jobs.NewExpireStacksWorker(db, "")
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireStacksArgs]()); err == nil {
		t.Fatal("expected error from second-page query failure, got nil")
	}
}

// TestExpireStacksWorker_KeysetRowsErr proves a mid-stream row-iteration error
// propagates out of Work.
func TestExpireStacksWorker_KeysetRowsErr(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows(expireStacksBatchCols).
		AddRow(expireUUID(0), "slug", "instant-stack-x").
		RowError(0, errors.New("conn reset mid-stream"))
	mock.ExpectQuery(`FROM stacks`).WillReturnRows(rows)

	w := jobs.NewExpireStacksWorker(db, "")
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireStacksArgs]()); err == nil {
		t.Fatal("expected rows.Err() to propagate, got nil")
	}
}
