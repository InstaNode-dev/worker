package migrations

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestReader_Get_OK exercises the happy path: DB returns a filename + a
// count, Get returns StatusOK with both populated.
func TestReader_Get_OK(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT filename FROM schema_migrations`).
		WillReturnRows(sqlmock.NewRows([]string{"filename"}).AddRow("061_forwarder_sent_delivery.sql"))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM schema_migrations`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(61))

	r := NewReader(db, 100*time.Millisecond, nil)
	s := r.Get(context.Background())
	if s.Status != StatusOK {
		t.Fatalf("Status: got %q want %q", s.Status, StatusOK)
	}
	if s.Filename != "061_forwarder_sent_delivery.sql" {
		t.Fatalf("Filename: got %q", s.Filename)
	}
	if s.Count != 61 {
		t.Fatalf("Count: got %d want 61", s.Count)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet: %v", err)
	}
}

// TestReader_Get_DBError surfaces StatusUnknown without panicking.
func TestReader_Get_DBError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT filename FROM schema_migrations`).
		WillReturnError(errors.New("connection refused"))

	r := NewReader(db, 100*time.Millisecond, nil)
	s := r.Get(context.Background())
	if s.Status != StatusUnknown {
		t.Fatalf("Status: got %q want %q", s.Status, StatusUnknown)
	}
	if s.Filename != "" || s.Count != 0 {
		t.Fatalf("expected empty State on DB error, got %+v", s)
	}
}

// TestReader_Get_CachesWithinTTL verifies a single DB hit per cache window.
func TestReader_Get_CachesWithinTTL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Only ONE pair of queries expected — the second Get serves cache.
	mock.ExpectQuery(`SELECT filename FROM schema_migrations`).
		WillReturnRows(sqlmock.NewRows([]string{"filename"}).AddRow("042_x.sql"))
	mock.ExpectQuery(`SELECT COUNT\(\*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(42))

	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }
	r := NewReader(db, 60*time.Second, clock)

	a := r.Get(context.Background())
	b := r.Get(context.Background()) // cached — no new DB hit
	if a != b {
		t.Fatalf("expected cache hit: %+v vs %+v", a, b)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet: %v", err)
	}
}

// TestReader_NilDB returns StatusUnknown without panicking. /healthz must
// never crash because the platform DB was never wired up.
func TestReader_NilDB(t *testing.T) {
	r := NewReader(nil, time.Second, nil)
	s := r.Get(context.Background())
	if s.Status != StatusUnknown {
		t.Fatalf("nil DB: got %q want %q", s.Status, StatusUnknown)
	}
}

// TestQueryState_NoRows surfaces StatusOK with empty filename. A fresh DB
// where schema_migrations exists but is empty (boot-time race) is valid.
func TestQueryState_NoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT filename FROM schema_migrations`).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`SELECT COUNT\(\*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	s, err := queryState(context.Background(), db)
	if err != nil {
		t.Fatalf("queryState err: %v", err)
	}
	if s.Status != StatusOK || s.Count != 0 || s.Filename != "" {
		t.Fatalf("got %+v", s)
	}
}
