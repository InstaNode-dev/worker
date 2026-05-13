package jobs

// loops_event_forwarder_test.go — hermetic tests for the audit_log → Loops
// forwarder. Mocks the SQL layer via sqlmock, the cursor store via an
// in-memory implementation of loopsCursorStore, and the Loops HTTP layer
// via httptest.
//
// In package `jobs` (not `jobs_test`) so it can construct the worker via
// newLoopsEventForwarderWorkerForTest. The brief specifies six cases —
// each maps to one TestLoopsForwarder_* below.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

// memCursor is an in-memory loopsCursorStore for tests. Goroutine-safe.
type memCursor struct {
	mu sync.Mutex
	c  loopsCursor
}

func (m *memCursor) read(_ context.Context) (loopsCursor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.c, nil
}
func (m *memCursor) write(_ context.Context, c loopsCursor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.c = c
	return nil
}

// fakeJobLocal mirrors expire_test.go's fakeJob but lives in package `jobs`
// so we don't have to cross the package boundary.
func fakeJobLocal[T river.JobArgs]() *river.Job[T] {
	return &river.Job[T]{JobRow: &rivertype.JobRow{ID: 1}}
}

// auditRowsCols are the columns the forwarder's fetchBatch expects to scan.
var auditRowsCols = []string{"id", "team_id", "kind", "resource_type", "summary", "metadata", "created_at", "owner_email"}

// TestLoopsForwarder_EmptyAPIKey_NoFetches verifies the fail-open path: if
// loops is nil (LOOPS_API_KEY unset), the worker MUST exit before doing any
// DB or cursor work. sqlmock strict mode catches an accidental query.
func TestLoopsForwarder_EmptyAPIKey_NoFetches(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	cursor := &memCursor{}
	// loops=nil → worker should short-circuit. No SQL or cursor expected.
	w := newLoopsEventForwarderWorkerForTest(db, cursor, nil)

	if err := w.Work(context.Background(), fakeJobLocal[LoopsEventForwarderArgs]()); err != nil {
		t.Fatalf("unexpected error on nil-loops path: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations (nil-loops path should make zero queries): %v", err)
	}
	if !cursor.c.zero() {
		t.Errorf("cursor was modified on nil-loops path: %+v", cursor.c)
	}
}

// TestLoopsForwarder_SupportedKind_PostsAndAdvances verifies the headline
// guarantee: a supported audit_log row triggers a POST to Loops AND the
// cursor advances past that row's (created_at, id).
func TestLoopsForwarder_SupportedKind_PostsAndAdvances(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Date(2026, 5, 13, 8, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("audit-id-1", "team-1", auditKindOnboardingClaimed, "", "team claimed", []byte(`{"signup_source":"github"}`), createdAt, "owner@example.com"))

	var posts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&posts, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	loops := newLoopsClient("test-key")
	loops.url = srv.URL

	cursor := &memCursor{}
	w := newLoopsEventForwarderWorkerForTest(db, cursor, loops)
	if err := w.Work(context.Background(), fakeJobLocal[LoopsEventForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := atomic.LoadInt32(&posts); got != 1 {
		t.Errorf("expected 1 POST to Loops, got %d", got)
	}
	if cursor.c.ID != "audit-id-1" {
		t.Errorf("cursor.ID = %q; want audit-id-1 (the row we processed)", cursor.c.ID)
	}
	if !cursor.c.CreatedAt.Equal(createdAt) {
		t.Errorf("cursor.CreatedAt = %v; want %v", cursor.c.CreatedAt, createdAt)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestLoopsForwarder_4xxAdvancesCursor verifies the "don't get stuck on a
// poisoned row" contract: Loops returns 401 → log + advance cursor.
func TestLoopsForwarder_4xxAdvancesCursor(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Date(2026, 5, 13, 9, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("audit-id-2", "team-2", auditKindSubscriptionUpgraded, "", "upgraded to pro", []byte(`{"to_tier":"pro"}`), createdAt, "owner@example.com"))

	var posts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&posts, 1)
		w.WriteHeader(http.StatusUnauthorized) // 401 — poisoned row
	}))
	defer srv.Close()

	loops := newLoopsClient("bad-key")
	loops.url = srv.URL

	cursor := &memCursor{}
	w := newLoopsEventForwarderWorkerForTest(db, cursor, loops)
	if err := w.Work(context.Background(), fakeJobLocal[LoopsEventForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := atomic.LoadInt32(&posts); got != 1 {
		t.Errorf("expected 1 POST attempt, got %d", got)
	}
	// Despite the 4xx, the cursor MUST advance — holding it would pin the
	// queue behind the poisoned row.
	if cursor.c.ID != "audit-id-2" {
		t.Errorf("cursor.ID = %q after 401; want audit-id-2 (advance past poisoned row)", cursor.c.ID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestLoopsForwarder_5xxHoldsCursor verifies the retry contract: Loops 503
// → no cursor advance, batch halts.
func TestLoopsForwarder_5xxHoldsCursor(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Date(2026, 5, 13, 10, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("audit-id-3", "team-3", auditKindOnboardingClaimed, "", "claim", []byte(`{}`), createdAt, "owner@example.com"))

	var posts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&posts, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	loops := newLoopsClient("test-key")
	loops.url = srv.URL

	cursor := &memCursor{}
	w := newLoopsEventForwarderWorkerForTest(db, cursor, loops)
	if err := w.Work(context.Background(), fakeJobLocal[LoopsEventForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := atomic.LoadInt32(&posts); got != 1 {
		t.Errorf("expected 1 POST attempt, got %d", got)
	}
	// Cursor MUST NOT advance — next tick retries.
	if !cursor.c.zero() {
		t.Errorf("cursor advanced on 5xx (%+v); want zero so next tick retries", cursor.c)
	}
}

// TestLoopsForwarder_BatchHaltsOn5xx verifies that a 5xx mid-batch stops
// processing the rest of the batch (per loops_event_forwarder.go contract).
// We give the worker two rows, return 5xx on the first POST, and assert
// only one POST was attempted.
func TestLoopsForwarder_BatchHaltsOn5xx(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	ca1 := time.Date(2026, 5, 13, 11, 0, 0, 0, time.UTC)
	ca2 := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("row-a", "team-a", auditKindOnboardingClaimed, "", "x", []byte(`{}`), ca1, "a@example.com").
			AddRow("row-b", "team-b", auditKindOnboardingClaimed, "", "y", []byte(`{}`), ca2, "b@example.com"))

	var posts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&posts, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	loops := newLoopsClient("test-key")
	loops.url = srv.URL

	cursor := &memCursor{}
	w := newLoopsEventForwarderWorkerForTest(db, cursor, loops)
	if err := w.Work(context.Background(), fakeJobLocal[LoopsEventForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	// Only the first row should have been attempted — the 5xx halts the batch.
	if got := atomic.LoadInt32(&posts); got != 1 {
		t.Errorf("expected exactly 1 POST attempt (halt on 5xx), got %d", got)
	}
}

// TestLoopsForwarder_NoOwnerEmailAdvances verifies the builder-skip path:
// a row without an owner email returns ok=false from its builder. The
// forwarder MUST advance the cursor (the row will never produce a valid
// payload) and MUST NOT POST.
func TestLoopsForwarder_NoOwnerEmailAdvances(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Date(2026, 5, 13, 13, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("orphan-row", "team-x", auditKindOnboardingClaimed, "", "x", []byte(`{}`), createdAt, "")) // no email

	var posts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&posts, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	loops := newLoopsClient("test-key")
	loops.url = srv.URL

	cursor := &memCursor{}
	w := newLoopsEventForwarderWorkerForTest(db, cursor, loops)
	if err := w.Work(context.Background(), fakeJobLocal[LoopsEventForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := atomic.LoadInt32(&posts); got != 0 {
		t.Errorf("expected 0 POSTs for orphan row, got %d", got)
	}
	if cursor.c.ID != "orphan-row" {
		t.Errorf("cursor.ID = %q; want orphan-row (must advance past unsendable row)", cursor.c.ID)
	}
}

// TestLoopsForwarder_BatchLimitIsHonored verifies the SQL passes the
// loopsBatchLimit constant — protects against a refactor that drops the
// LIMIT clause and tries to drain millions of rows per tick.
func TestLoopsForwarder_BatchLimitIsHonored(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Match the LIMIT clause literally — if a future refactor drops it,
	// this test fails first.
	mock.ExpectQuery(`LIMIT \$4`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols))

	loops := newLoopsClient("test-key")
	cursor := &memCursor{}
	w := newLoopsEventForwarderWorkerForTest(db, cursor, loops)
	if err := w.Work(context.Background(), fakeJobLocal[LoopsEventForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	if loopsBatchLimit != 100 {
		t.Errorf("loopsBatchLimit = %d; want 100 (the brief specifies 100-row batches)", loopsBatchLimit)
	}
}

// TestLoopsForwarder_NoRowsExitsClean verifies that an empty batch is a
// no-op — no POSTs, no cursor change, no error.
func TestLoopsForwarder_NoRowsExitsClean(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("Loops POST attempted on empty batch")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	loops := newLoopsClient("test-key")
	loops.url = srv.URL

	cursor := &memCursor{}
	w := newLoopsEventForwarderWorkerForTest(db, cursor, loops)
	if err := w.Work(context.Background(), fakeJobLocal[LoopsEventForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if !cursor.c.zero() {
		t.Errorf("cursor changed on empty batch: %+v", cursor.c)
	}
}
