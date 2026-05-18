package jobs_test

// quota_wall_nudge_test.go — Track U1 worker job tests.
//
// Covers the three lifecycle paths the PR-brief calls out:
//   1. Above-80% on storage axis → writes one audit_log row.
//   2. Same team within 24h → skipped (idempotency).
//   3. team tier → never scanned (unlimited has no walls).
//
// All tests use sqlmock so they're hermetic and run in <100ms each.

import (
	"context"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	"instant.dev/worker/internal/jobs"
)

// mockWallPlanRegistry stubs the QuotaWallPlanRegistry surface. Each field
// becomes the canned response for its method — keeps the table simple.
type mockWallPlanRegistry struct {
	storageMB   int
	connections int
	provisions  int
}

func (m *mockWallPlanRegistry) StorageLimitMB(tier, service string) int   { return m.storageMB }
func (m *mockWallPlanRegistry) ConnectionsLimit(tier, service string) int { return m.connections }
func (m *mockWallPlanRegistry) ProvisionLimit(tier string) int            { return m.provisions }

// TestQuotaWallNudge_WritesAuditRowAtStorage80Percent verifies the
// headline guarantee: a team at ≥80% storage on a tier-limited axis
// produces exactly one INSERT into audit_log with kind=near_quota_wall.
//
// Storage math: storageMB=10 (per-resource), one active postgres row of
// 9 MB. Total limit = 10 MB × 1 = 10 MB. 9/10 = 90%. ≥80% triggers.
func TestQuotaWallNudge_WritesAuditRowAtStorage80Percent(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()

	// 1) team list — one hobby team.
	mock.ExpectQuery(`SELECT id, plan_tier\s+FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "plan_tier"}).
			AddRow(teamID.String(), "hobby"))

	// 2) dedupe lookup — no recent near_quota_wall row.
	mock.ExpectQuery(`SELECT 1\s+FROM audit_log`).
		WithArgs(teamID, "near_quota_wall", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"n"}))

	// 3-5) storage axis per service: postgres has 9 MB (90% of 10),
	// redis + mongodb empty.
	nineMB := int64(9 * 1024 * 1024)
	mock.ExpectQuery(`SELECT COALESCE\(SUM\(storage_bytes\)`).
		WithArgs(teamID, "postgres").
		WillReturnRows(sqlmock.NewRows([]string{"sum", "count"}).AddRow(nineMB, 1))
	mock.ExpectQuery(`SELECT COALESCE\(SUM\(storage_bytes\)`).
		WithArgs(teamID, "redis").
		WillReturnRows(sqlmock.NewRows([]string{"sum", "count"}).AddRow(int64(0), 0))
	mock.ExpectQuery(`SELECT COALESCE\(SUM\(storage_bytes\)`).
		WithArgs(teamID, "mongodb").
		WillReturnRows(sqlmock.NewRows([]string{"sum", "count"}).AddRow(int64(0), 0))

	// Connections axis disabled (-1 = unlimited in our stub) so the
	// worker should not query those rows. Provisions disabled too.

	// 6) audit_log INSERT — the exact write we're verifying happens.
	// P2-W2-09: the row MUST carry resource_type. The storage axis fired
	// on postgres, so the 6th arg (resource_type) is "postgres" — NULLIF
	// keeps a real value here; only the service-agnostic provisions axis
	// would pass "" and land NULL.
	mock.ExpectExec(`INSERT INTO audit_log[\s\S]+resource_type`).
		WithArgs(teamID, "system", "near_quota_wall", sqlmock.AnyArg(), sqlmock.AnyArg(), "postgres").
		WillReturnResult(sqlmock.NewResult(1, 1))

	plans := &mockWallPlanRegistry{
		storageMB:   10,
		connections: -1,
		provisions:  -1,
	}
	w := jobs.NewQuotaWallNudgeWorker(db, plans)
	if err := w.Work(context.Background(), fakeJob[jobs.QuotaWallNudgeArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestQuotaWallNudge_SkipsWhenRecentlyNudged verifies the 24h dedupe:
// if the team already has a near_quota_wall row in the last 24h, the
// worker MUST NOT insert another one and MUST NOT run any axis queries.
func TestQuotaWallNudge_SkipsWhenRecentlyNudged(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()

	mock.ExpectQuery(`SELECT id, plan_tier\s+FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "plan_tier"}).
			AddRow(teamID.String(), "hobby"))

	// Dedupe lookup returns a row → we MUST short-circuit. No further
	// queries are expected; sqlmock strict mode will fail if any fire.
	mock.ExpectQuery(`SELECT 1\s+FROM audit_log`).
		WithArgs(teamID, "near_quota_wall", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))

	plans := &mockWallPlanRegistry{storageMB: 10, connections: -1, provisions: -1}
	w := jobs.NewQuotaWallNudgeWorker(db, plans)
	if err := w.Work(context.Background(), fakeJob[jobs.QuotaWallNudgeArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestQuotaWallNudge_SkipsTeamTier verifies that the team-list query
// filters out tiers with no walls (team, anonymous, free) at the SQL
// level — no per-team scan happens for those rows. The test asserts
// the team-list query itself contains the right WHERE clause.
func TestQuotaWallNudge_SkipsTeamTier(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// The query MUST contain "NOT IN ('team', 'anonymous', 'free')" so
	// unlimited / pre-conversion teams never get scanned. sqlmock's
	// regexp matcher gives us a precise check on the actual SQL.
	teamListPattern := regexp.MustCompile(`NOT IN \('team', 'anonymous', 'free'\)`)
	mock.ExpectQuery(teamListPattern.String()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "plan_tier"}))

	plans := &mockWallPlanRegistry{storageMB: 10, connections: -1, provisions: -1}
	w := jobs.NewQuotaWallNudgeWorker(db, plans)
	if err := w.Work(context.Background(), fakeJob[jobs.QuotaWallNudgeArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestQuotaWallNudge_NoNudgeBelowThreshold verifies the floor: a team
// at 70% on every axis must NOT get an audit row.
func TestQuotaWallNudge_NoNudgeBelowThreshold(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()

	mock.ExpectQuery(`SELECT id, plan_tier\s+FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "plan_tier"}).
			AddRow(teamID.String(), "hobby"))

	mock.ExpectQuery(`SELECT 1\s+FROM audit_log`).
		WithArgs(teamID, "near_quota_wall", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"n"}))

	// 7 MB on a 10 MB cap is 70% → below the 80% floor.
	sevenMB := int64(7 * 1024 * 1024)
	mock.ExpectQuery(`SELECT COALESCE\(SUM\(storage_bytes\)`).
		WithArgs(teamID, "postgres").
		WillReturnRows(sqlmock.NewRows([]string{"sum", "count"}).AddRow(sevenMB, 1))
	mock.ExpectQuery(`SELECT COALESCE\(SUM\(storage_bytes\)`).
		WithArgs(teamID, "redis").
		WillReturnRows(sqlmock.NewRows([]string{"sum", "count"}).AddRow(int64(0), 0))
	mock.ExpectQuery(`SELECT COALESCE\(SUM\(storage_bytes\)`).
		WithArgs(teamID, "mongodb").
		WillReturnRows(sqlmock.NewRows([]string{"sum", "count"}).AddRow(int64(0), 0))

	// No INSERT expected. sqlmock strict mode fails if one fires.

	plans := &mockWallPlanRegistry{storageMB: 10, connections: -1, provisions: -1}
	w := jobs.NewQuotaWallNudgeWorker(db, plans)
	if err := w.Work(context.Background(), fakeJob[jobs.QuotaWallNudgeArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
