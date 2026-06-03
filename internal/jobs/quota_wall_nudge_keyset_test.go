package jobs

// quota_wall_nudge_keyset_test.go — keyset-pagination coverage for the
// QuotaWallNudgeWorker.Work team scan. Bug-bash 2026-06-03: the scan previously
// issued ONE unbounded `SELECT id, plan_tier FROM teams`; it now streams the
// eligible teams in keyset-paginated batches of quotaWallNudgeScanBatchLimit
// (WHERE id::text > $cursor ORDER BY id::text ASC LIMIT n), evaluating the
// WHOLE eligible set per tick.
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
)

// wallTeamID renders a zero-padded, lexicographically-sortable UUID-shaped id
// matching the id::text keyset ordering.
func wallTeamID(i int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", i) }

// TestQuotaWallNudge_KeysetPagination proves the team scan pages through an
// eligible set larger than one batch. Page 1 is FULL
// (quotaWallNudgeScanBatchLimit rows) so the loop must issue a SECOND query
// whose cursor ($1) is page 1's tail id; page 2 is short → the scan ends. To
// keep the test focused on the keyset advance, every team's dedupe query
// returns "recently nudged" so no evaluate/insert fires (skipped path).
func TestQuotaWallNudge_KeysetPagination(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	page1 := sqlmock.NewRows([]string{"id", "plan_tier"})
	var lastPage1ID string
	for i := 0; i < quotaWallNudgeScanBatchLimit; i++ {
		id := wallTeamID(i)
		page1.AddRow(id, "hobby")
		lastPage1ID = id
	}
	page2ID := wallTeamID(quotaWallNudgeScanBatchLimit)

	teamsRE := `SELECT id, plan_tier\s+FROM teams[\s\S]+id::text > \$1[\s\S]+ORDER BY id::text ASC[\s\S]+LIMIT \$2`
	mock.ExpectQuery(teamsRE).
		WithArgs("", quotaWallNudgeScanBatchLimit).
		WillReturnRows(page1)
	mock.ExpectQuery(teamsRE).
		WithArgs(lastPage1ID, quotaWallNudgeScanBatchLimit).
		WillReturnRows(sqlmock.NewRows([]string{"id", "plan_tier"}).AddRow(page2ID, "hobby"))

	// Per-team dedupe read returns a row → recentlyNudged=true → skipped.
	// MatchExpectationsInOrder(false) lets the interleaved dedupe reads match
	// regardless of which page they belong to.
	mock.MatchExpectationsInOrder(false)
	total := quotaWallNudgeScanBatchLimit + 1
	for i := 0; i < total; i++ {
		mock.ExpectQuery(`SELECT 1\s+FROM audit_log`).
			WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
	}

	w := NewQuotaWallNudgeWorker(db, &mockWallPlanRegistryCov{storageMB: 10})
	if err := w.Work(context.Background(), quotaWallNudgeJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations (keyset did not advance to page 2 with the right cursor): %v", err)
	}
}

// TestQuotaWallNudge_SecondPageError proves a DB error on a LATER team page
// propagates out of Work.
func TestQuotaWallNudge_SecondPageError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	page1 := sqlmock.NewRows([]string{"id", "plan_tier"})
	for i := 0; i < quotaWallNudgeScanBatchLimit; i++ {
		page1.AddRow(wallTeamID(i), "hobby")
	}
	teamsRE := `SELECT id, plan_tier\s+FROM teams`
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery(teamsRE).
		WithArgs("", quotaWallNudgeScanBatchLimit).
		WillReturnRows(page1)
	// Every page-1 team is skipped via a recently-nudged dedupe hit.
	for i := 0; i < quotaWallNudgeScanBatchLimit; i++ {
		mock.ExpectQuery(`SELECT 1\s+FROM audit_log`).
			WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
	}
	mock.ExpectQuery(teamsRE).
		WithArgs(wallTeamID(quotaWallNudgeScanBatchLimit-1), quotaWallNudgeScanBatchLimit).
		WillReturnError(errors.New("conn lost mid-sweep"))

	w := NewQuotaWallNudgeWorker(db, &mockWallPlanRegistryCov{storageMB: 10})
	if err := w.Work(context.Background(), quotaWallNudgeJob()); err == nil {
		t.Fatal("expected error from second-page query failure, got nil")
	}
}

// TestQuotaWallNudge_KeysetRowsErr proves a mid-stream row-iteration error
// propagates out of Work.
func TestQuotaWallNudge_KeysetRowsErr(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "plan_tier"}).
		AddRow(wallTeamID(0), "hobby").
		RowError(0, errors.New("conn reset mid-stream"))
	mock.ExpectQuery(`SELECT id, plan_tier\s+FROM teams`).
		WillReturnRows(rows)

	w := NewQuotaWallNudgeWorker(db, &mockWallPlanRegistryCov{storageMB: 10})
	if err := w.Work(context.Background(), quotaWallNudgeJob()); err == nil {
		t.Fatal("expected rows.Err() to propagate, got nil")
	}
}
