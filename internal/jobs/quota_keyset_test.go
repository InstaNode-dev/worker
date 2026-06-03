package jobs

// quota_keyset_test.go — keyset-pagination coverage for the three quota.go
// scan loops (suspend / unsuspend / redis-eviction). Bug-bash 2026-06-03:
// each loop previously issued ONE unbounded `SELECT ... FROM resources`; it
// now streams the eligible set in keyset-paginated batches of
// quotaScanBatchLimit (WHERE id::text > $cursor ORDER BY id::text ASC LIMIT n)
// so the per-fetch result set stays bounded regardless of table size.
//
// These tests mirror the orphan_sweep fetchLiveStackIDs keyset tests:
//   - multi-page advance: a FULL first page forces a SECOND query whose cursor
//     ($cursor) is the last id of page 1 — proving the loop pages the WHOLE set
//     rather than dropping rows past the limit;
//   - second-page error: a DB error on a LATER page propagates (no silent
//     partial processing);
//   - mid-stream rows.Err(): a row-iteration error propagates.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// quotaKeysetCols is the 8-column projection the suspend/unsuspend loops scan.
var quotaKeysetCols = []string{
	"id", "token", "resource_type", "tier", "storage_bytes",
	"provider_resource_id", "team_id", "name",
}

// keysetID renders a zero-padded, lexicographically-sortable id so page rows
// come back in the same order they are added (the id::text keyset relies on
// lexical ordering).
func keysetID(i int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", i) }

// ── runSuspendLoop ────────────────────────────────────────────────────────

// TestRunSuspendLoop_KeysetPagination proves the suspend loop pages through a
// table larger than one batch: page 1 is FULL (quotaScanBatchLimit rows, every
// row unlimited-tier so none are suspended), forcing a SECOND query whose
// cursor is page 1's tail id. Page 2 is short → loop ends. All rows are
// scanned (the loop processes the WHOLE eligible set), and the second query's
// keyset arg equals page 1's tail.
func TestRunSuspendLoop_KeysetPagination(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	page1 := sqlmock.NewRows(quotaKeysetCols)
	var lastPage1ID string
	for i := 0; i < quotaScanBatchLimit; i++ {
		id := keysetID(i)
		// tier="" → StorageLimitMB returns -1 (unlimited) via mockPlanRegistryCov,
		// so no row is suspended and no nested check/UPDATE fires.
		page1.AddRow(id, "tok", "postgres", "", int64(0), "", nil, "")
		lastPage1ID = id
	}
	queryRE := `SELECT id, token, resource_type[\s\S]+FROM resources[\s\S]+id::text > \$2[\s\S]+ORDER BY id::text ASC[\s\S]+LIMIT \$3`
	mock.ExpectQuery(queryRE).
		WithArgs("active", "", quotaScanBatchLimit).
		WillReturnRows(page1)
	mock.ExpectQuery(queryRE).
		WithArgs("active", lastPage1ID, quotaScanBatchLimit).
		WillReturnRows(sqlmock.NewRows(quotaKeysetCols).
			AddRow(keysetID(quotaScanBatchLimit), "tok", "postgres", "", int64(0), "", nil, ""))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: -1}, nil)
	ids, err := w.runSuspendLoop(context.Background())
	if err != nil {
		t.Fatalf("runSuspendLoop: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("no unlimited-tier row should be suspended; got %v", ids)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations (keyset did not advance to page 2 with the right cursor): %v", err)
	}
}

// TestRunSuspendLoop_SecondPageError proves a DB error on a LATER keyset page
// propagates out — the loop must not silently stop after a partial scan.
func TestRunSuspendLoop_SecondPageError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	page1 := sqlmock.NewRows(quotaKeysetCols)
	for i := 0; i < quotaScanBatchLimit; i++ {
		page1.AddRow(keysetID(i), "tok", "postgres", "", int64(0), "", nil, "")
	}
	queryRE := `SELECT id, token, resource_type[\s\S]+FROM resources`
	mock.ExpectQuery(queryRE).WillReturnRows(page1)
	mock.ExpectQuery(queryRE).WillReturnError(errors.New("conn lost mid-sweep"))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: -1}, nil)
	if _, err := w.runSuspendLoop(context.Background()); err == nil {
		t.Fatal("expected error from second-page query failure, got nil")
	}
}

// TestRunSuspendLoop_KeysetRowsErr proves a row-iteration error (rows.Err() non-nil)
// propagates rather than truncating the scan silently.
func TestRunSuspendLoop_KeysetRowsErr(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows(quotaKeysetCols).
		AddRow(keysetID(0), "tok", "postgres", "", int64(0), "", nil, "").
		RowError(0, errors.New("conn reset mid-stream"))
	mock.ExpectQuery(`SELECT id, token, resource_type[\s\S]+FROM resources`).
		WillReturnRows(rows)

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: -1}, nil)
	if _, err := w.runSuspendLoop(context.Background()); err == nil {
		t.Fatal("expected rows.Err() to propagate, got nil")
	}
}

// ── runUnsuspendLoop ──────────────────────────────────────────────────────

// TestRunUnsuspendLoop_KeysetPagination proves the unsuspend loop pages a
// suspended set larger than one batch. Page 1 is FULL → a SECOND query fires
// with page 1's tail as the cursor; page 2 is short → loop ends. Every row is
// unlimited-tier so none are flipped (no nested UPDATE), keeping the test
// focused purely on the keyset advance.
func TestRunUnsuspendLoop_KeysetPagination(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Build page 1 (FULL) and capture every id into a skip-set so the loop
	// short-circuits each row immediately after scan — before any nested
	// storage read or UPDATE. This keeps the test focused purely on the keyset
	// advance (no interleaved nested queries to order against).
	page1 := sqlmock.NewRows(quotaKeysetCols)
	var lastPage1ID string
	skipIDs := make([]string, 0, quotaScanBatchLimit+1)
	for i := 0; i < quotaScanBatchLimit; i++ {
		id := keysetID(i)
		page1.AddRow(id, "tok", "postgres", "hobby", int64(0), "", nil, "")
		skipIDs = append(skipIDs, id)
		lastPage1ID = id
	}
	page2ID := keysetID(quotaScanBatchLimit)
	skipIDs = append(skipIDs, page2ID)

	queryRE := `SELECT id, token, resource_type[\s\S]+FROM resources[\s\S]+id::text > \$2[\s\S]+ORDER BY id::text ASC[\s\S]+LIMIT \$3`
	mock.ExpectQuery(queryRE).
		WithArgs("suspended", "", quotaScanBatchLimit).
		WillReturnRows(page1)
	mock.ExpectQuery(queryRE).
		WithArgs("suspended", lastPage1ID, quotaScanBatchLimit).
		WillReturnRows(sqlmock.NewRows(quotaKeysetCols).
			AddRow(page2ID, "tok", "postgres", "hobby", int64(0), "", nil, ""))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 10}, nil)
	n, err := w.runUnsuspendLoop(context.Background(), skipIDs)
	if err != nil {
		t.Fatalf("runUnsuspendLoop: %v", err)
	}
	if n != 0 {
		t.Errorf("all rows skipped → none unsuspended; got %d", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations (keyset did not advance to page 2 with the right cursor): %v", err)
	}
}

// TestRunUnsuspendLoop_SecondPageError proves a DB error on a later page
// propagates.
func TestRunUnsuspendLoop_SecondPageError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Skip every page-1 row so no nested storage read / UPDATE fires — the
	// test asserts only that a second-page query error propagates.
	page1 := sqlmock.NewRows(quotaKeysetCols)
	skipIDs := make([]string, 0, quotaScanBatchLimit)
	for i := 0; i < quotaScanBatchLimit; i++ {
		id := keysetID(i)
		page1.AddRow(id, "tok", "postgres", "hobby", int64(0), "", nil, "")
		skipIDs = append(skipIDs, id)
	}
	queryRE := `SELECT id, token, resource_type[\s\S]+FROM resources`
	mock.ExpectQuery(queryRE).WillReturnRows(page1)
	mock.ExpectQuery(queryRE).WillReturnError(errors.New("conn lost mid-sweep"))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 10}, nil)
	if _, err := w.runUnsuspendLoop(context.Background(), skipIDs); err == nil {
		t.Fatal("expected error from second-page query failure, got nil")
	}
}

// TestRunUnsuspendLoop_KeysetRowsErr proves a mid-stream row-iteration error
// propagates.
func TestRunUnsuspendLoop_KeysetRowsErr(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows(quotaKeysetCols).
		AddRow(keysetID(0), "tok", "postgres", "hobby", int64(100*1024*1024), "", nil, "").
		RowError(0, errors.New("conn reset mid-stream"))
	mock.ExpectQuery(`SELECT id, token, resource_type[\s\S]+FROM resources`).
		WillReturnRows(rows)

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 10}, nil)
	if _, err := w.runUnsuspendLoop(context.Background(), nil); err == nil {
		t.Fatal("expected rows.Err() to propagate, got nil")
	}
}

// ── runRedisEvictionLoop ──────────────────────────────────────────────────

// quotaEvictKeysetCols is the 4-column projection the redis-eviction loop scans.
var quotaEvictKeysetCols = []string{"id", "token", "tier", "storage_bytes"}

// TestRunRedisEvictionLoop_KeysetPagination proves the eviction loop pages a
// redis set larger than one batch. Page 1 is FULL (all dedicated-tier rows so
// none are evicted), forcing a SECOND query with page 1's tail as the cursor.
func TestRunRedisEvictionLoop_KeysetPagination(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	page1 := sqlmock.NewRows(quotaEvictKeysetCols)
	var lastPage1ID string
	for i := 0; i < quotaScanBatchLimit; i++ {
		id := keysetID(i)
		// tier="pro" is NOT a shared redis tier → isSharedRedisTier false →
		// skipped, no evictor call. Keeps this a pure pagination test.
		page1.AddRow(id, "tok", "pro", int64(0))
		lastPage1ID = id
	}
	queryRE := `SELECT id, token, tier, storage_bytes[\s\S]+FROM resources[\s\S]+resource_type = 'redis'[\s\S]+id::text > \$2[\s\S]+ORDER BY id::text ASC[\s\S]+LIMIT \$3`
	mock.ExpectQuery(queryRE).
		WithArgs("active", "", quotaScanBatchLimit).
		WillReturnRows(page1)
	mock.ExpectQuery(queryRE).
		WithArgs("active", lastPage1ID, quotaScanBatchLimit).
		WillReturnRows(sqlmock.NewRows(quotaEvictKeysetCols).
			AddRow(keysetID(quotaScanBatchLimit), "tok", "pro", int64(0)))

	w := NewEnforceStorageQuotaWorkerWithEvictor(db, &mockPlanRegistryCov{limitMB: 5}, nil, &stubEvictor{})
	n, err := w.runRedisEvictionLoop(context.Background())
	if err != nil {
		t.Fatalf("runRedisEvictionLoop: %v", err)
	}
	if n != 0 {
		t.Errorf("no dedicated-tier row should be evicted; enforced=%d", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations (keyset did not advance to page 2 with the right cursor): %v", err)
	}
}

// TestRunRedisEvictionLoop_SecondPageError proves a DB error on a later page
// propagates.
func TestRunRedisEvictionLoop_SecondPageError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	page1 := sqlmock.NewRows(quotaEvictKeysetCols)
	for i := 0; i < quotaScanBatchLimit; i++ {
		page1.AddRow(keysetID(i), "tok", "pro", int64(0))
	}
	queryRE := `SELECT id, token, tier, storage_bytes[\s\S]+FROM resources[\s\S]+resource_type = 'redis'`
	mock.ExpectQuery(queryRE).WillReturnRows(page1)
	mock.ExpectQuery(queryRE).WillReturnError(errors.New("conn lost mid-sweep"))

	w := NewEnforceStorageQuotaWorkerWithEvictor(db, &mockPlanRegistryCov{limitMB: 5}, nil, &stubEvictor{})
	if _, err := w.runRedisEvictionLoop(context.Background()); err == nil {
		t.Fatal("expected error from second-page query failure, got nil")
	}
}

// TestRunRedisEvictionLoop_KeysetRowsErr proves a mid-stream row-iteration error
// propagates.
func TestRunRedisEvictionLoop_KeysetRowsErr(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows(quotaEvictKeysetCols).
		AddRow(keysetID(0), "tok", "pro", int64(0)).
		RowError(0, errors.New("conn reset mid-stream"))
	mock.ExpectQuery(`SELECT id, token, tier, storage_bytes[\s\S]+FROM resources[\s\S]+resource_type = 'redis'`).
		WillReturnRows(rows)

	w := NewEnforceStorageQuotaWorkerWithEvictor(db, &mockPlanRegistryCov{limitMB: 5}, nil, &stubEvictor{})
	if _, err := w.runRedisEvictionLoop(context.Background()); err == nil {
		t.Fatal("expected rows.Err() to propagate, got nil")
	}
}
