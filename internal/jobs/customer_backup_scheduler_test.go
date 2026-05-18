package jobs

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

// errDBInPkg is the local twin of jobs_test.errDB — this test file lives
// in `package jobs` (so it can poke unexported worker fields like `now`)
// and can't reach the package-test fixture.
var errDBInPkg = errors.New("db error (in-package fixture)")

// fakeSchedulerJob is the in-package twin of jobs_test.fakeJob — needed
// because this test file lives in `package jobs` so it can poke `now` on
// the unexported worker fields without going through a constructor knob.
func fakeSchedulerJob() *river.Job[CustomerBackupSchedulerArgs] {
	return &river.Job[CustomerBackupSchedulerArgs]{JobRow: &rivertype.JobRow{ID: 1}}
}

// TestHobbyDailySlot_Deterministic — the cadence-spread function must be
// deterministic per team (same UUID always yields same slot) and bounded.
func TestHobbyDailySlot_Deterministic(t *testing.T) {
	teamA := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	teamB := uuid.MustParse("11111111-2222-3333-4444-555555555555")

	slotA1 := hobbyDailySlot(teamA)
	slotA2 := hobbyDailySlot(teamA)
	slotB := hobbyDailySlot(teamB)

	if slotA1 != slotA2 {
		t.Errorf("non-deterministic: %d vs %d", slotA1, slotA2)
	}
	for _, s := range []int{slotA1, slotB} {
		if s < 0 || s >= 24 {
			t.Errorf("slot %d out of bounds [0,24)", s)
		}
	}
}

// TestScheduler_InsertsForProTierEveryHour — happy path. A single pro
// postgres resource yields one INSERT regardless of current hour.
func TestScheduler_InsertsForProTierEveryHour(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	resID := "fffffff0-1111-2222-3333-444444444444"

	mock.ExpectQuery(`SELECT r.id::text, r.tier, r.team_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}).
			AddRow(resID, "pro", teamID))

	// P2-W4: the dedupe is now folded into the INSERT as an atomic
	// `INSERT … SELECT … WHERE NOT EXISTS (…)`. RowsAffected=1 means the
	// NOT EXISTS arm passed and a row was scheduled.
	mock.ExpectExec(`INSERT INTO resource_backups`).
		WithArgs(uuid.MustParse(resID), "pro").
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := NewCustomerBackupSchedulerWorker(db)
	// Pin time to 14:00 UTC — for pro this is irrelevant (always inserts).
	w.now = func() time.Time { return time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestScheduler_HobbyOffSlot_Skips — hobby tier should NOT insert when
// the current hour-of-day != its daily slot. We construct a team whose
// slot is 5 and run the scheduler at hour 14 — expect no INSERT.
func TestScheduler_HobbyOffSlot_Skips(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Pick a team UUID whose first byte mod 24 = 5 (i.e. byte 5, 29, 53, ...).
	teamID := uuid.UUID{5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if hobbyDailySlot(teamID) != 5 {
		t.Fatalf("test fixture wrong: hobbyDailySlot(teamID)=%d, want 5", hobbyDailySlot(teamID))
	}

	resID := "fffffff0-1111-2222-3333-444444444444"
	mock.ExpectQuery(`SELECT r.id::text, r.tier, r.team_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}).
			AddRow(resID, "hobby", teamID))
	// No EXISTS, no INSERT.

	w := NewCustomerBackupSchedulerWorker(db)
	w.now = func() time.Time { return time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestScheduler_HobbyOnSlot_Inserts — when the current hour matches the
// team's daily slot, the hobby row gets inserted.
func TestScheduler_HobbyOnSlot_Inserts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.UUID{5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	resID := "fffffff0-1111-2222-3333-444444444444"

	mock.ExpectQuery(`SELECT r.id::text`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}).
			AddRow(resID, "hobby", teamID))
	mock.ExpectExec(`INSERT INTO resource_backups`).
		WithArgs(uuid.MustParse(resID), "hobby").
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := NewCustomerBackupSchedulerWorker(db)
	// Hour 5 = the team's slot.
	w.now = func() time.Time { return time.Date(2026, 5, 13, 5, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestScheduler_DedupExists_Skips — a recent row inside the 50min lookback
// should suppress the INSERT. P2-W4: the dedupe is now atomic inside the
// INSERT statement, so the worker always issues the INSERT but a recent
// row makes the `WHERE NOT EXISTS` arm match → RowsAffected=0 → the worker
// counts it as a deduped skip rather than an inserted row.
func TestScheduler_DedupExists_Skips(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	resID := "fffffff0-1111-2222-3333-444444444444"

	mock.ExpectQuery(`SELECT r.id::text`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}).
			AddRow(resID, "pro", teamID))
	// INSERT runs but the NOT EXISTS arm matches the recent row → 0 rows.
	mock.ExpectExec(`INSERT INTO resource_backups`).
		WithArgs(uuid.MustParse(resID), "pro").
		WillReturnResult(sqlmock.NewResult(0, 0))

	w := NewCustomerBackupSchedulerWorker(db)
	w.now = func() time.Time { return time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestScheduler_DedupeIsAtomicInsert pins BugBash P2-W4: the dedupe MUST
// be a single atomic `INSERT … SELECT … WHERE NOT EXISTS (…)` statement,
// NOT a separate SELECT EXISTS check followed by an unconditional INSERT.
// The old check-then-act shape let two concurrent ticks both observe
// existed=false and both INSERT, double-scheduling a backup.
//
// sqlmock's QueryMatcherRegexp asserts the worker issues exactly one
// statement carrying both `INSERT INTO resource_backups` and the
// `WHERE NOT EXISTS` guard — a regression to the two-statement shape
// fails this expectation.
func TestScheduler_DedupeIsAtomicInsert(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	resID := "fffffff0-1111-2222-3333-444444444444"

	mock.ExpectQuery(`SELECT r\.id::text, r\.tier, r\.team_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}).
			AddRow(resID, "pro", teamID))
	// The single statement must contain BOTH the INSERT and the NOT EXISTS
	// dedupe guard — proves the dedupe is folded in, not check-then-act.
	mock.ExpectExec(`INSERT INTO resource_backups[\s\S]+WHERE NOT EXISTS`).
		WithArgs(uuid.MustParse(resID), "pro").
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := NewCustomerBackupSchedulerWorker(db)
	w.now = func() time.Time { return time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("dedupe is not an atomic INSERT … WHERE NOT EXISTS — TOCTOU regressed: %v", err)
	}
}

// TestScheduler_HobbyPlus_OnSlotInserts — FIX-H regression. Hobby Plus
// (the $19/mo mid-tier) MUST be in the scheduled-backup set. Pre-fix the
// scheduler hardcoded `tier IN ('hobby','pro','growth','team')` and any
// hobby_plus / hobby_plus_yearly / pro_yearly customer received zero
// scheduled backups despite paying for them.
func TestScheduler_HobbyPlus_OnSlotInserts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Pick a team UUID whose slot = 5; run scheduler at hour 5.
	teamID := uuid.UUID{5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if hobbyDailySlot(teamID) != 5 {
		t.Fatalf("test fixture wrong: hobbyDailySlot(teamID)=%d, want 5", hobbyDailySlot(teamID))
	}
	resID := "fffffff0-1111-2222-3333-444444444444"

	mock.ExpectQuery(`SELECT r.id::text`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}).
			AddRow(resID, "hobby_plus", teamID))
	mock.ExpectExec(`INSERT INTO resource_backups`).
		WithArgs(uuid.MustParse(resID), "hobby_plus").
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := NewCustomerBackupSchedulerWorker(db)
	w.now = func() time.Time { return time.Date(2026, 5, 14, 5, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestScheduler_YearlyVariants_BackupHourly — pro_yearly and team_yearly
// (and any other _yearly tier with hourly cadence) must back up every
// hour just like their canonical monthly counterpart. Regression guard
// for the FIX-H widened tier set.
func TestScheduler_YearlyVariants_BackupHourly(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	resID := "fffffff0-1111-2222-3333-444444444444"

	mock.ExpectQuery(`SELECT r.id::text, r.tier, r.team_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}).
			AddRow(resID, "pro_yearly", teamID))
	mock.ExpectExec(`INSERT INTO resource_backups`).
		WithArgs(uuid.MustParse(resID), "pro_yearly").
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := NewCustomerBackupSchedulerWorker(db)
	// Hour 14 — pro_yearly should fire regardless (hourly cadence).
	w.now = func() time.Time { return time.Date(2026, 5, 14, 14, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestCanonicalTier — sanity: _yearly strips, others pass through.
func TestCanonicalTier(t *testing.T) {
	cases := map[string]string{
		"hobby":              "hobby",
		"hobby_yearly":       "hobby",
		"hobby_plus":         "hobby_plus",
		"hobby_plus_yearly":  "hobby_plus",
		"pro":                "pro",
		"pro_yearly":         "pro",
		"team":               "team",
		"team_yearly":        "team",
		"growth":             "growth",
		"growth_yearly":      "growth",
		"anonymous":          "anonymous",
		"":                   "",
		"_yearly":            "_yearly", // not stripped — guard: too short
	}
	for in, want := range cases {
		got := canonicalTier(in)
		if got != want {
			t.Errorf("canonicalTier(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestScheduler_DBSelectError_ReturnsError — bad SELECT bubbles up.
func TestScheduler_DBSelectError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT r.id::text`).WillReturnError(errDBInPkg)

	w := NewCustomerBackupSchedulerWorker(db)
	if err := w.Work(context.Background(), fakeSchedulerJob()); err == nil {
		t.Fatal("expected error from SELECT failure, got nil")
	}
}
