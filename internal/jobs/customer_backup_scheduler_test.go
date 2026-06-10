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

// schedulerPlans returns a BackupPlanRegistry whose rpo_minutes +
// backup_retention_days mirror the real api/plans.yaml values the scheduler
// reads in prod. Pinning the values here (rather than loading the embedded
// YAML) means a future plans.yaml edit that breaks the cadence contract
// fails an explicit assertion, not a moved goalpost — and the cadence logic
// is exercised against the exact promise numbers.
//
//	pro / pro_yearly / growth / team / team_yearly → rpo 60   (HOURLY)
//	hobby / hobby_yearly / hobby_plus*             → rpo 1440 (DAILY)
//	anonymous / free                               → rpo 0    (NEVER)
func schedulerPlans() *fakeBackupPlanRegistry {
	return &fakeBackupPlanRegistry{
		rpo: map[string]int{
			"pro":               60,
			"pro_yearly":        60,
			"growth":            60,
			"growth_yearly":     60,
			"team":              60,
			"team_yearly":       60,
			"hobby":             1440,
			"hobby_yearly":      1440,
			"hobby_plus":        1440,
			"hobby_plus_yearly": 1440,
			"anonymous":         0,
			"free":              0,
		},
		days: map[string]int{
			"pro":               30,
			"pro_yearly":        30,
			"growth":            30,
			"growth_yearly":     30,
			"team":              90,
			"team_yearly":       90,
			"hobby":             7,
			"hobby_yearly":      7,
			"hobby_plus":        14,
			"hobby_plus_yearly": 14,
			"anonymous":         0,
			"free":              0,
		},
	}
}

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

// TestCadenceForTier_RPODriven is the keystone R1 assertion: the cadence is
// derived from plans.yaml rpo_minutes, NOT a hardcoded tier list. Every paid
// tier promising rpo<=60 must be HOURLY; coarser-RPO tiers DAILY; rpo==0
// (anonymous/free) NEVER. This test iterates the registry's declared map so
// adding a new tier with the wrong cadence trips here (root rule 18 — no
// hand-typed single-site list).
func TestCadenceForTier_RPODriven(t *testing.T) {
	plans := schedulerPlans()
	want := map[string]cadence{
		"pro":               cadenceHourly,
		"pro_yearly":        cadenceHourly,
		"growth":            cadenceHourly,
		"growth_yearly":     cadenceHourly,
		"team":              cadenceHourly,
		"team_yearly":       cadenceHourly,
		"hobby":             cadenceDaily,
		"hobby_yearly":      cadenceDaily,
		"hobby_plus":        cadenceDaily,
		"hobby_plus_yearly": cadenceDaily,
		"anonymous":         cadenceNever,
		"free":              cadenceNever,
	}
	// Iterate the registry's declared tiers — a tier present in plans but
	// missing from `want` is an un-asserted cadence and fails loudly.
	for tier := range plans.rpo {
		exp, ok := want[tier]
		if !ok {
			t.Fatalf("tier %q present in registry but not asserted — add it to want", tier)
		}
		if got := cadenceForTier(plans, tier); got != exp {
			t.Errorf("cadenceForTier(%q) = %d, want %d (rpo=%d)", tier, got, exp, plans.RPOMinutes(tier))
		}
	}
}

// TestCadenceForTier_HourlyCutoffBoundary pins the [1,60]→hourly,
// >60→daily boundary so a plans.yaml edit nudging rpo_minutes across 60
// flips the cadence as intended (and rpo==0 is always never).
func TestCadenceForTier_HourlyCutoffBoundary(t *testing.T) {
	cases := []struct {
		rpo  int
		days int
		want cadence
	}{
		{0, 0, cadenceNever},    // no promise (anonymous/free)
		{0, 7, cadenceNever},    // rpo unset but retention set → still never
		{1, 7, cadenceHourly},   // tightest non-zero RPO
		{60, 30, cadenceHourly}, // exactly the pro/growth/team promise
		{61, 30, cadenceDaily},  // just over the line
		{1440, 7, cadenceDaily}, // hobby daily
		{-5, 7, cadenceNever},   // defensive: negative treated as no-promise
	}
	for _, c := range cases {
		plans := &fakeBackupPlanRegistry{
			rpo:  map[string]int{"x": c.rpo},
			days: map[string]int{"x": c.days},
		}
		if got := cadenceForTier(plans, "x"); got != c.want {
			t.Errorf("rpo=%d days=%d: cadence=%d, want %d", c.rpo, c.days, got, c.want)
		}
	}
}

// TestCadenceForTier_ZeroRetentionNeverBacksUp — independent of rpo_minutes,
// a tier with backup_retention_days<=0 must NEVER be enqueued (R1: never back
// up anonymous/free). Even a stray hourly rpo_minutes on a zero-retention
// tier is overridden to "never".
func TestCadenceForTier_ZeroRetentionNeverBacksUp(t *testing.T) {
	plans := &fakeBackupPlanRegistry{
		rpo:  map[string]int{"weird": 60}, // would be hourly on rpo alone…
		days: map[string]int{"weird": 0},  // …but 0-day retention vetoes it
	}
	if got := cadenceForTier(plans, "weird"); got != cadenceNever {
		t.Errorf("zero-retention tier cadence=%d, want cadenceNever", got)
	}
}

// TestCadenceForTier_NilRegistryLegacyFallback — when no registry is wired
// (boot misconfigured) the cadence falls back to the legacy hardcoded
// mapping so paid backups never silently stop.
func TestCadenceForTier_NilRegistryLegacyFallback(t *testing.T) {
	cases := map[string]cadence{
		"pro":          cadenceHourly,
		"pro_yearly":   cadenceHourly,
		"growth":       cadenceHourly,
		"team":         cadenceHourly,
		"team_yearly":  cadenceHourly,
		"hobby":        cadenceDaily,
		"hobby_plus":   cadenceDaily,
		"hobby_yearly": cadenceDaily,
		"anonymous":    cadenceNever,
		"free":         cadenceNever,
		"":             cadenceNever,
	}
	for tier, want := range cases {
		if got := cadenceForTier(nil, tier); got != want {
			t.Errorf("legacy cadenceForTier(nil, %q) = %d, want %d", tier, got, want)
		}
	}
}

// TestScheduler_InsertsForProTierEveryHour — happy path / R1 core ask: a
// single pro postgres resource (rpo_minutes:60) yields one INSERT regardless
// of the current hour, i.e. it becomes due HOURLY.
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

	w := NewCustomerBackupSchedulerWorker(db, schedulerPlans())
	// Pin time to 14:00 UTC — for pro this is irrelevant (always inserts).
	w.now = func() time.Time { return time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestScheduler_ProDueEveryHour_AcrossAllHours — stronger R1 proof: a pro
// resource is enqueued at EVERY hour of the day, never skipped. Catches a
// regression where an off-by-one or stray slot gate accidentally throttled
// an hourly tier to daily.
func TestScheduler_ProDueEveryHour_AcrossAllHours(t *testing.T) {
	teamID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	resID := "fffffff0-1111-2222-3333-444444444444"

	for hour := 0; hour < 24; hour++ {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("hour %d: sqlmock.New: %v", hour, err)
		}
		mock.ExpectQuery(`SELECT r.id::text, r.tier, r.team_id`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}).
				AddRow(resID, "pro", teamID))
		// MUST insert at every hour — RowsAffected=1.
		mock.ExpectExec(`INSERT INTO resource_backups`).
			WithArgs(uuid.MustParse(resID), "pro").
			WillReturnResult(sqlmock.NewResult(1, 1))

		w := NewCustomerBackupSchedulerWorker(db, schedulerPlans())
		w.now = func() time.Time { return time.Date(2026, 5, 13, hour, 0, 0, 0, time.UTC) }

		if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
			t.Fatalf("hour %d: Work: %v", hour, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("hour %d: pro must be due hourly but was not enqueued: %v", hour, err)
		}
		db.Close()
	}
}

// TestScheduler_FreeTierNeverEnqueued — R1 hard requirement: a `free` tier
// resource (rpo_minutes:0, backup_retention_days:0) must NEVER be enqueued.
// The SQL filter excludes 'free' up front, so the candidate set is empty and
// NO INSERT is ever issued.
func TestScheduler_FreeTierNeverEnqueued(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// The SQL excludes anonymous/free, so the SELECT returns no free rows.
	// We simulate the real query returning empty (free filtered out in SQL).
	mock.ExpectQuery(`SELECT r.id::text, r.tier, r.team_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}))
	// NO INSERT expected — any ExpectExec on resource_backups would be unmet.

	w := NewCustomerBackupSchedulerWorker(db, schedulerPlans())
	w.now = func() time.Time { return time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestScheduler_FreeTierGate_SkipsEvenIfRowLeaksThrough — defence-in-depth
// for R1. Even if a `free` row somehow leaks past the SQL filter (e.g. a
// race that flips tier after the WHERE evaluates), the per-row registry
// gate (cadenceForTier → cadenceNever) MUST skip it: no INSERT.
func TestScheduler_FreeTierGate_SkipsEvenIfRowLeaksThrough(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	resID := "fffffff0-1111-2222-3333-444444444444"

	// The mock returns a 'free' row as if it leaked past the SQL filter.
	mock.ExpectQuery(`SELECT r.id::text, r.tier, r.team_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}).
			AddRow(resID, "free", teamID))
	// No INSERT — the registry cadence gate must veto the free row.

	w := NewCustomerBackupSchedulerWorker(db, schedulerPlans())
	w.now = func() time.Time { return time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("free row was not skipped by the registry gate: %v", err)
	}
}

// TestScheduler_HobbyOffSlot_Skips — hobby tier (daily cadence) should NOT
// insert when the current hour-of-day != its daily slot. We construct a team
// whose slot is 5 and run the scheduler at hour 14 — expect no INSERT.
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

	w := NewCustomerBackupSchedulerWorker(db, schedulerPlans())
	w.now = func() time.Time { return time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestScheduler_HobbyOnSlot_Inserts — when the current hour matches the
// team's daily slot, the hobby (daily cadence) row gets inserted.
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

	w := NewCustomerBackupSchedulerWorker(db, schedulerPlans())
	// Hour 5 = the team's slot.
	w.now = func() time.Time { return time.Date(2026, 5, 13, 5, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestScheduler_NoDuplicateEnqueueWithinHour — R1 idempotency requirement:
// a recent scheduled row inside the 50min lookback must suppress a second
// INSERT in the same hour. The dedupe is atomic inside the INSERT statement
// (P2-W4), so the worker always issues the INSERT but a recent row makes the
// `WHERE NOT EXISTS` arm match → RowsAffected=0 → no duplicate backup.
//
// This proves "no duplicate enqueue within an hour": even though the
// scheduler runs and re-evaluates the pro resource (hourly cadence), the DB
// dedupe arm collapses the second attempt to a no-op.
func TestScheduler_NoDuplicateEnqueueWithinHour(t *testing.T) {
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
	// INSERT runs but the NOT EXISTS arm matches the recent (<50min) row → 0 rows.
	mock.ExpectExec(`INSERT INTO resource_backups`).
		WithArgs(uuid.MustParse(resID), "pro").
		WillReturnResult(sqlmock.NewResult(0, 0))

	w := NewCustomerBackupSchedulerWorker(db, schedulerPlans())
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
// Combined with the 50-minute DB lookback, this is what survives the
// WeeklyDigest-fired-daily failure mode (River periodic + RunOnStart can
// fire two ticks back-to-back on restart): dedupe lives in the DB, not in
// River's UniqueOpts.
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

	w := NewCustomerBackupSchedulerWorker(db, schedulerPlans())
	w.now = func() time.Time { return time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("dedupe is not an atomic INSERT … WHERE NOT EXISTS — TOCTOU regressed: %v", err)
	}
}

// TestScheduler_DedupeLookbackIsUnderOneHour pins the 50-minute lookback:
// the dedupe window must be < 60 minutes so that the SAME hour-bucket is
// always covered by the previous tick's row, yet the NEXT hour's tick (an
// hourly-cadence pro tier) is NOT suppressed. The INSERT SQL must carry the
// `INTERVAL '50 minutes'` guard.
func TestScheduler_DedupeLookbackIsUnderOneHour(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	resID := "fffffff0-1111-2222-3333-444444444444"

	mock.ExpectQuery(`SELECT r\.id::text`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tier", "team_id"}).
			AddRow(resID, "pro", teamID))
	// Assert the dedupe interval is 50 minutes (< 1h) — a regression to e.g.
	// 60+ minutes would suppress the legitimate next-hour backup.
	mock.ExpectExec(`INTERVAL '50 minutes'`).
		WithArgs(uuid.MustParse(resID), "pro").
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := NewCustomerBackupSchedulerWorker(db, schedulerPlans())
	w.now = func() time.Time { return time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("dedupe lookback is not '50 minutes' (<1h): %v", err)
	}
}

// TestScheduler_HobbyPlus_OnSlotInserts — FIX-H regression. Hobby Plus
// (the $19/mo mid-tier, rpo_minutes:1440 → daily) MUST be in the
// scheduled-backup set. Pre-fix the scheduler hardcoded
// `tier IN ('hobby','pro','growth','team')` and any hobby_plus customer
// received zero scheduled backups despite paying for them. Now driven by
// the registry, so any paid tier with retention>0 is covered.
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

	w := NewCustomerBackupSchedulerWorker(db, schedulerPlans())
	w.now = func() time.Time { return time.Date(2026, 5, 14, 5, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestScheduler_YearlyVariants_BackupHourly — pro_yearly and team_yearly
// (rpo_minutes:60 → hourly) must back up every hour just like their canonical
// monthly counterpart. Regression guard: the registry resolves the variant's
// own rpo_minutes, so no _yearly special-casing is needed in the scheduler.
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

	w := NewCustomerBackupSchedulerWorker(db, schedulerPlans())
	// Hour 14 — pro_yearly should fire regardless (hourly cadence).
	w.now = func() time.Time { return time.Date(2026, 5, 14, 14, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestScheduler_NilRegistry_LegacyFallbackInserts — when no registry is
// wired the scheduler falls back to the legacy hardcoded cadence (pro hourly)
// so paid backups never silently stop on a misconfigured boot.
func TestScheduler_NilRegistry_LegacyFallbackInserts(t *testing.T) {
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
	mock.ExpectExec(`INSERT INTO resource_backups`).
		WithArgs(uuid.MustParse(resID), "pro").
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := NewCustomerBackupSchedulerWorker(db, nil) // nil registry → legacy path
	w.now = func() time.Time { return time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC) }

	if err := w.Work(context.Background(), fakeSchedulerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("nil-registry legacy fallback did not enqueue pro: %v", err)
	}
}

// TestCanonicalTier — sanity: _yearly strips, others pass through.
func TestCanonicalTier(t *testing.T) {
	cases := map[string]string{
		"hobby":             "hobby",
		"hobby_yearly":      "hobby",
		"hobby_plus":        "hobby_plus",
		"hobby_plus_yearly": "hobby_plus",
		"pro":               "pro",
		"pro_yearly":        "pro",
		"team":              "team",
		"team_yearly":       "team",
		"growth":            "growth",
		"growth_yearly":     "growth",
		"anonymous":         "anonymous",
		"":                  "",
		"_yearly":           "_yearly", // not stripped — guard: too short
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

	w := NewCustomerBackupSchedulerWorker(db, schedulerPlans())
	if err := w.Work(context.Background(), fakeSchedulerJob()); err == nil {
		t.Fatal("expected error from SELECT failure, got nil")
	}
}
