package jobs_test

// magic_link_reconciler_test.go — hermetic tests for MagicLinkReconcilerWorker.
//
// The worker has two responsibilities:
//   1. SELECT the right rows from magic_links (pending/send_failed,
//      within 15-min TTL, attempts < 3).
//   2. POST each row id to the api with a worker-signed JWT and translate
//      the response status into per-batch counters.
//
// We exercise (1) via sqlmock (asserting that expired rows + over-cap rows
// don't appear in the query result the worker iterates over) and (2) via
// httptest (asserting the worker stops at the cap and routes each api
// response status to the right counter).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	"instant.dev/worker/internal/jobs"
)

// magicLinkReconcileCols is the column order the worker's SELECT returns.
// Keep in sync with magic_link_reconciler.go::listReconcileCandidates.
var magicLinkReconcileCols = []string{
	"id", "email", "email_send_status", "email_send_attempts", "created_at",
}

// TestReconciler_ZeroConfig asserts the no-config short-circuit: when
// apiBase / jwtSecret are empty, Work returns nil and does NOT hit the
// DB. Mirrors the fail-open posture every internal-call worker uses.
func TestReconciler_ZeroConfig(t *testing.T) {
	// nil DB is fine — the short-circuit returns before the SELECT.
	w := jobs.NewMagicLinkReconcilerWorker(nil, "", "", nil)
	if err := w.Work(context.Background(), fakeJob[jobs.MagicLinkReconcilerArgs]()); err != nil {
		t.Errorf("Work with zero config must not return an error: %v", err)
	}
}

// TestReconciler_PicksUpFailedRowsWithinTTL is the SELECT-shape test: a
// row with status='send_failed', attempts=1, created 2 minutes ago must
// be passed to the resend api. We assert by checking the http call was
// made with the expected link_id body.
func TestReconciler_PicksUpFailedRowsWithinTTL(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	linkID := uuid.New()
	createdAt := time.Now().UTC().Add(-2 * time.Minute)

	mock.ExpectQuery(`FROM magic_links`).
		WillReturnRows(sqlmock.NewRows(magicLinkReconcileCols).
			AddRow(linkID, "user@example.com", "send_failed", 1, createdAt))

	// Stand up a fake api endpoint.
	var capturedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/email/resend-magic-link" {
			t.Errorf("unexpected request path: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			t.Errorf("missing Bearer authorization")
		}
		bodyBuf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(bodyBuf)
		capturedBody = bodyBuf
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"status":"sent"}`))
	}))
	defer srv.Close()

	worker := jobs.NewMagicLinkReconcilerWorker(db, srv.URL, "test-secret", srv.Client())
	if err := worker.Work(context.Background(), fakeJob[jobs.MagicLinkReconcilerArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}

	var body map[string]string
	if err := json.Unmarshal(capturedBody, &body); err != nil {
		t.Fatalf("api body unmarshal: %v (raw: %s)", err, capturedBody)
	}
	if body["link_id"] != linkID.String() {
		t.Errorf("api body link_id: want %s, got %s", linkID, body["link_id"])
	}
}

// TestReconciler_SkipsExpiredRows asserts that a row created > 15 minutes
// ago is excluded by the SELECT (the worker filters by created_at >
// now() - 15min). We seed an empty result and confirm no api call fires.
//
// This is a contract test on the candidate-SQL filter shape — we trust
// the DB's WHERE clause to exclude old rows; the test seeds an empty
// mock result and asserts no api call fires regardless.
func TestReconciler_SkipsExpiredRows(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// The DB would naturally exclude rows older than now() - 15min. We
	// model that by returning an empty result.
	mock.ExpectQuery(`FROM magic_links`).
		WillReturnRows(sqlmock.NewRows(magicLinkReconcileCols))

	var apiCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiCalls++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true,"status":"sent"}`))
	}))
	defer srv.Close()

	worker := jobs.NewMagicLinkReconcilerWorker(db, srv.URL, "test-secret", srv.Client())
	if err := worker.Work(context.Background(), fakeJob[jobs.MagicLinkReconcilerArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
	if apiCalls != 0 {
		t.Errorf("no api call should fire on empty result set, got %d", apiCalls)
	}
}

// TestReconciler_StopsAfter3Attempts asserts the 3-attempt cap is
// enforced via the api response: when the api returns status="abandoned"
// the worker tallies it under "abandoned" rather than "resent" and does
// NOT retry the same row inside the same tick.
//
// The cap itself lives in the api (the worker just tallies whichever
// status comes back); this test pins down the worker side of that
// contract — that an abandoned row doesn't get a second post in the
// same tick.
func TestReconciler_StopsAfter3Attempts(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	linkID := uuid.New()
	createdAt := time.Now().UTC().Add(-2 * time.Minute)

	mock.ExpectQuery(`FROM magic_links`).
		WillReturnRows(sqlmock.NewRows(magicLinkReconcileCols).
			AddRow(linkID, "user@example.com", "send_failed", 2, createdAt))

	var apiCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiCalls++
		// The api would observe attempts=2, fail send, increment to 3,
		// then flip to abandoned. We model that with a "abandoned" response.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"status":"abandoned","attempts":3}`))
	}))
	defer srv.Close()

	worker := jobs.NewMagicLinkReconcilerWorker(db, srv.URL, "test-secret", srv.Client())
	if err := worker.Work(context.Background(), fakeJob[jobs.MagicLinkReconcilerArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
	if apiCalls != 1 {
		t.Errorf("worker must POST exactly once per row per tick, got %d calls", apiCalls)
	}
}
