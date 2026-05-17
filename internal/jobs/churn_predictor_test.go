package jobs_test

// churn_predictor_test.go — hermetic tests for ChurnPredictorWorker.
//
// The candidate SQL is treated as a black box: each test seeds the SELECT
// result via sqlmock and asserts on whether the worker issues an INSERT
// with the right shape. The query's filters (plan_tier != 'team',
// inactivity window, no-recent-flag dedupe, COUNT(resources) > 0) are
// documented behaviour rather than re-tested here — the DB will not
// return rows that don't match, so the worker logic the test exercises
// is "given a candidate row, do we flag it?".
//
// Scenarios covered (numbering matches the brief):
//   1. Team inactive 8d + active resources → flagged.
//   2. Team inactive 2d → not flagged (SELECT excludes it).
//   3. Team with no resources → not flagged (SELECT excludes it).
//   4. Team flagged 15d ago → not re-flagged (SELECT excludes it).
//   5. Team flagged 35d ago → re-flagged (SELECT returns it again).
//   6. Team tier=team → never flagged (SELECT excludes it).
//   7. Metadata includes email + tier + days_since + active_resource_count.

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	"instant.dev/worker/internal/jobs"
)

// auditKindChurnRiskFlaggedLiteral is the literal kind string the producer
// writes. We compare against this in test assertions but never reference
// the unexported package constant — the literal IS the contract Brevo
// reads, so duplicating it here is intentional.
const auditKindChurnRiskFlaggedLiteral = "churn.risk_flagged"

// churnRowCols is the column order the worker's SELECT returns.
// Keep in sync with churn_predictor.go::Work's SELECT projection.
var churnRowCols = []string{
	"team_id", "plan_tier", "owner_email", "last_activity", "active_resource_count",
}

// TestChurnPredictor_FlagsInactive8dTeam covers scenarios 1 + 7: a team
// that's been silent for 8 days with active resources, never previously
// flagged, yields exactly one INSERT into audit_log whose metadata JSONB
// carries tier, last_activity_days_ago, active_resource_count, and email.
func TestChurnPredictor_FlagsInactive8dTeam(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()
	lastActivity := time.Now().UTC().Add(-8 * 24 * time.Hour)

	mock.ExpectQuery(`FROM teams t`).
		WillReturnRows(sqlmock.NewRows(churnRowCols).
			AddRow(teamID, "hobby", "owner@example.com", lastActivity, int64(3)))

	// Capture metadata via a custom argument matcher so we can decode +
	// assert on the JSONB body.
	var capturedMeta []byte
	metaMatcher := &captureChurnBytesArg{out: &capturedMeta}

	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(
			teamID,
			"system",
			auditKindChurnRiskFlaggedLiteral,
			sqlmock.AnyArg(), // summary
			metaMatcher,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := jobs.NewChurnPredictorWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ChurnPredictorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}

	// Scenario 7: metadata payload contract.
	var meta map[string]any
	if err := json.Unmarshal(capturedMeta, &meta); err != nil {
		t.Fatalf("metadata JSON unmarshal: %v (raw: %s)", err, capturedMeta)
	}
	if got := meta["email"]; got != "owner@example.com" {
		t.Errorf("metadata.email = %v, want owner@example.com", got)
	}
	if got := meta["tier"]; got != "hobby" {
		t.Errorf("metadata.tier = %v, want hobby", got)
	}
	// last_activity_days_ago is float (rounded to nearest 0.1). ~8 days.
	days, ok := meta["last_activity_days_ago"].(float64)
	if !ok {
		t.Errorf("metadata.last_activity_days_ago missing or wrong type: %T %v", meta["last_activity_days_ago"], meta["last_activity_days_ago"])
	} else if days < 7.9 || days > 8.1 {
		t.Errorf("metadata.last_activity_days_ago = %v, want ~8.0", days)
	}
	// active_resource_count comes through as float64 from JSON decode.
	count, ok := meta["active_resource_count"].(float64)
	if !ok {
		t.Errorf("metadata.active_resource_count missing or wrong type: %T %v", meta["active_resource_count"], meta["active_resource_count"])
	} else if count != 3 {
		t.Errorf("metadata.active_resource_count = %v, want 3", count)
	}
}

// TestChurnPredictor_NoCandidates_2dActiveTeam covers scenario 2: a team
// with activity 2d ago doesn't satisfy the 7d inactivity HAVING clause,
// so the SELECT returns nothing. From the worker's point of view, the
// rowset is empty → no INSERT fires.
func TestChurnPredictor_NoCandidates_2dActiveTeam(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// 2d-ago activity → SQL HAVING excludes it → empty rowset.
	mock.ExpectQuery(`FROM teams t`).
		WillReturnRows(sqlmock.NewRows(churnRowCols))

	w := jobs.NewChurnPredictorWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ChurnPredictorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestChurnPredictor_NoCandidates_TeamWithNoResources covers scenario 3:
// a team that's been silent 10d but has zero active resources is past
// the "we miss you" window — already churned to nothing. The SQL HAVING
// COUNT(r.id) > 0 clause excludes it; the worker sees an empty rowset.
func TestChurnPredictor_NoCandidates_TeamWithNoResources(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`FROM teams t`).
		WillReturnRows(sqlmock.NewRows(churnRowCols))

	w := jobs.NewChurnPredictorWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ChurnPredictorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestChurnPredictor_NoCandidates_TeamFlagged15dAgo covers scenario 4: a
// team flagged 15d ago is inside the 30-day dedupe window, so the SQL
// NOT EXISTS clause excludes it. Empty rowset → no INSERT.
func TestChurnPredictor_NoCandidates_TeamFlagged15dAgo(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`FROM teams t`).
		WillReturnRows(sqlmock.NewRows(churnRowCols))

	w := jobs.NewChurnPredictorWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ChurnPredictorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestChurnPredictor_ReflagsAfter35dDedupePassed covers scenario 5: the
// prior flag was 35d ago, outside the 30d dedupe window, so the SQL NOT
// EXISTS clause no longer excludes the team. Behaviour is identical to
// scenario 1 — we emit a fresh row. The dedupe math itself lives in the
// SQL predicate (created_at > now() - 30d).
func TestChurnPredictor_ReflagsAfter35dDedupePassed(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()
	lastActivity := time.Now().UTC().Add(-9 * 24 * time.Hour)

	mock.ExpectQuery(`FROM teams t`).
		WillReturnRows(sqlmock.NewRows(churnRowCols).
			AddRow(teamID, "pro", "owner@example.com", lastActivity, int64(7)))

	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(
			teamID,
			"system",
			auditKindChurnRiskFlaggedLiteral,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := jobs.NewChurnPredictorWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ChurnPredictorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestChurnPredictor_NoCandidates_TeamTierExcluded covers scenario 6: a
// team with plan_tier='team' is never returned by the SELECT (the
// WHERE plan_tier != 'team' clause filters it). The empty rowset →
// no INSERT fires.
func TestChurnPredictor_NoCandidates_TeamTierExcluded(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`FROM teams t`).
		WillReturnRows(sqlmock.NewRows(churnRowCols))

	w := jobs.NewChurnPredictorWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ChurnPredictorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestChurnPredictor_SkipsWhenNoOwnerEmail verifies the orphan-team path:
// a candidate with no email cannot be reached by Brevo, so the worker
// logs a warn and does NOT INSERT — an audit row with no addressable
// email is dead weight for the email pipeline.
func TestChurnPredictor_SkipsWhenNoOwnerEmail(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	teamID := uuid.New()
	lastActivity := time.Now().UTC().Add(-10 * 24 * time.Hour)

	// owner_email returns "" (LEFT JOIN with no user match → COALESCE
	// emits empty string per the worker's SELECT projection).
	mock.ExpectQuery(`FROM teams t`).
		WillReturnRows(sqlmock.NewRows(churnRowCols).
			AddRow(teamID, "hobby", "", lastActivity, int64(2)))

	// No INSERT expected — sqlmock strict mode fails if one fires.

	w := jobs.NewChurnPredictorWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ChurnPredictorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestChurnPredictor_TopLevelQueryError_ReturnsError verifies that a
// fatal SELECT failure propagates so River retries the job. Per-row
// errors are fail-open (logged) per the worker's contract; the
// top-level query is the one River-visible error path.
func TestChurnPredictor_TopLevelQueryError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`FROM teams t`).WillReturnError(errDB)

	w := jobs.NewChurnPredictorWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ChurnPredictorArgs]()); err == nil {
		t.Fatal("expected error from top-level SELECT failure, got nil")
	}
}

// TestChurnPredictor_FailOpenOnInsertError covers the per-row insert
// failure path: one bad INSERT does NOT stop the worker from processing
// the rest of the batch and does NOT propagate. The test gives the
// worker two candidates, fails the first INSERT, and asserts the second
// INSERT still fires.
func TestChurnPredictor_FailOpenOnInsertError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	team1, team2 := uuid.New(), uuid.New()
	la1 := time.Now().UTC().Add(-9 * 24 * time.Hour)
	la2 := time.Now().UTC().Add(-12 * 24 * time.Hour)

	mock.ExpectQuery(`FROM teams t`).
		WillReturnRows(sqlmock.NewRows(churnRowCols).
			AddRow(team1, "hobby", "a@example.com", la1, int64(1)).
			AddRow(team2, "pro", "b@example.com", la2, int64(4)))

	// First INSERT fails — the second one must still happen.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(team1, "system", auditKindChurnRiskFlaggedLiteral, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(errDB)
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(team2, "system", auditKindChurnRiskFlaggedLiteral, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := jobs.NewChurnPredictorWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ChurnPredictorArgs]()); err != nil {
		t.Fatalf("expected nil (fail-open) on per-row INSERT error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// captureChurnBytesArg is a sqlmock.Argument that captures the raw bytes
// the worker passed for a JSONB column. We use it to introspect the
// audit_log.metadata payload after the worker writes it. Mirrors
// captureBytesArg in expire_imminent_test.go.
type captureChurnBytesArg struct {
	out *[]byte
}

// Match satisfies the sqlmock.Argument interface.
func (c *captureChurnBytesArg) Match(v driver.Value) bool {
	switch b := v.(type) {
	case []byte:
		*c.out = append((*c.out)[:0], b...)
		return true
	case string:
		*c.out = append((*c.out)[:0], []byte(b)...)
		return true
	}
	return false
}

// TestChurnPredictor_JoinsOnlyPrimaryUser pins fix #2: the candidate query's
// users join MUST be `LEFT JOIN users u ON u.team_id = t.id AND u.is_primary
// = true` — the team's canonical primary user. The old code used a
// LEFT JOIN LATERAL … ORDER BY created_at ASC LIMIT 1 (oldest user) under a
// stale comment that falsely claimed the users table had no is_primary
// column; migration 029 added it (and uq_users_one_primary_per_team
// guarantees exactly one match).
//
// sqlmock's QueryMatcherRegexp matches the expected query as a regex against
// the SQL the worker actually issues; a query missing the predicate fails
// ExpectationsWereMet. Mirrors TestExpiryReminderWorker_JoinsOnlyPrimaryUser.
func TestChurnPredictor_JoinsOnlyPrimaryUser(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`LEFT JOIN users u ON u\.team_id = t\.id AND u\.is_primary = true`).
		WillReturnRows(sqlmock.NewRows(churnRowCols))

	w := jobs.NewChurnPredictorWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ChurnPredictorArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("query did not include the `AND u.is_primary = true` join predicate: %v", err)
	}
}
