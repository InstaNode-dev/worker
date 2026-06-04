package jobs

// test_cohort_test.go — unit coverage for the shared cohort skip-guard helpers
// (test_cohort.go). sqlmock-driven so it runs in the no-DB `make gate` / -short
// lane and covers every branch: flagged, not-flagged, row-vanished, and the
// DB-error fail-safe-for-real-teams path.

import (
	"context"
	"errors"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

func TestTestCohortNotExistsClause_FormatsAlias(t *testing.T) {
	got := testCohortNotExistsClause("r.team_id")
	// Must correlate on the caller-supplied team-id expression, use a fixed
	// `tc` subselect alias (so it never collides with the caller's aliases),
	// and key on is_test_cohort.
	for _, want := range []string{"NOT EXISTS", "FROM teams tc", "tc.id = r.team_id", "tc.is_test_cohort"} {
		if !strings.Contains(got, want) {
			t.Errorf("clause %q missing %q", got, want)
		}
	}
}

func TestIsTestCohort_Flagged(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	id := uuid.New()
	mock.ExpectQuery("SELECT is_test_cohort FROM teams WHERE id = ").
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"is_test_cohort"}).AddRow(true))

	flagged, err := isTestCohort(context.Background(), db, id)
	if err != nil {
		t.Fatalf("isTestCohort: %v", err)
	}
	if !flagged {
		t.Error("flagged = false, want true for a is_test_cohort=true team")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestIsTestCohort_NotFlagged(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	id := uuid.New()
	mock.ExpectQuery("SELECT is_test_cohort FROM teams WHERE id = ").
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"is_test_cohort"}).AddRow(false))

	flagged, err := isTestCohort(context.Background(), db, id)
	if err != nil {
		t.Fatalf("isTestCohort: %v", err)
	}
	if flagged {
		t.Error("flagged = true, want false for a normal team")
	}
}

func TestIsTestCohort_RowVanished(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	id := uuid.New()
	// No rows → team deleted between candidate SELECT and this check. Not
	// synthetic; (false, nil) so the caller's own row-vanished handling runs.
	mock.ExpectQuery("SELECT is_test_cohort FROM teams WHERE id = ").
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"is_test_cohort"}))

	flagged, err := isTestCohort(context.Background(), db, id)
	if err != nil {
		t.Fatalf("isTestCohort on vanished row should be (false,nil), got err %v", err)
	}
	if flagged {
		t.Error("flagged = true for a vanished team row, want false")
	}
}

func TestIsTestCohort_DBError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	id := uuid.New()
	boom := errors.New("platform-db brownout")
	mock.ExpectQuery("SELECT is_test_cohort FROM teams WHERE id = ").
		WithArgs(id).
		WillReturnError(boom)

	flagged, err := isTestCohort(context.Background(), db, id)
	if err == nil {
		t.Fatal("isTestCohort should propagate the DB error")
	}
	if !errors.Is(err, boom) {
		t.Errorf("error chain lost the cause: %v", err)
	}
	if flagged {
		t.Error("flagged = true on a DB error; must be false (fail-safe-for-real-teams)")
	}
}

func TestSkipIfTestCohort_SkipsFlagged(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	id := uuid.New()
	mock.ExpectQuery("SELECT is_test_cohort FROM teams WHERE id = ").
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"is_test_cohort"}).AddRow(true))

	if !skipIfTestCohort(context.Background(), db, id, "jobs.unit_test") {
		t.Error("skipIfTestCohort = false for a synthetic team, want true (must skip)")
	}
}

func TestSkipIfTestCohort_ProcessesNormal(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	id := uuid.New()
	mock.ExpectQuery("SELECT is_test_cohort FROM teams WHERE id = ").
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"is_test_cohort"}).AddRow(false))

	if skipIfTestCohort(context.Background(), db, id, "jobs.unit_test") {
		t.Error("skipIfTestCohort = true for a normal team, want false (must process)")
	}
}

// TestSkipIfTestCohort_DBErrorProcesses pins the fail-safe-for-real-teams
// contract: on a DB error the team is PROCESSED (returns false), never silently
// skipped — a platform-DB blip must not suppress a real customer's money/email.
func TestSkipIfTestCohort_DBErrorProcesses(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	id := uuid.New()
	mock.ExpectQuery("SELECT is_test_cohort FROM teams WHERE id = ").
		WithArgs(id).
		WillReturnError(errors.New("conn reset"))

	if skipIfTestCohort(context.Background(), db, id, "jobs.unit_test") {
		t.Error("skipIfTestCohort = true on DB error; must be false so a real team is processed during a blip")
	}
}
