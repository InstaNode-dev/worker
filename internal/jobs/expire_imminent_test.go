package jobs_test

// expire_imminent_test.go — hermetic tests for ExpireImminentWorker.
//
// The candidate SQL is treated as a black box: each test seeds the SELECT
// result via sqlmock and asserts on whether the worker issues an INSERT
// with the right shape. The query's filters (expires_at window, dedupe
// NOT IN subquery, team_id NOT NULL) are documented behaviour rather than
// re-tested here — the DB will not return rows that don't match, so the
// worker logic the test exercises is "given a candidate row, do we emit?".
//
// Scenarios covered (numbering matches the brief):
//   1. Resource expires in 30min, no prior audit row → emits row.
//   2. Resource expires in 30min, audit row written 6h ago → skipped
//      (the candidate SELECT excludes it; we model this as "row not
//      returned by SELECT" and assert no INSERT fires).
//   3. Resource expires in 30min, audit row written 13h ago → emits
//      (same modeling — the SELECT returns it because the dedupe
//      window has passed).
//   4. Resource expires in 2h → not emitted (SELECT excludes it).
//   5. Resource already expired → not emitted (SELECT excludes it).
//   6. Anonymous resource (team_id NULL) → SELECT excludes it.
//   7. Authenticated resource but team has no users → in-worker skip
//      with a warn log, no INSERT.
//   8. Metadata payload contains hours_remaining, email, resource_id.

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

// auditKindExpiryImminentLiteral is the literal kind string the producer
// writes. We compare against this in test assertions but never reference
// the unexported package constant — the literal IS the contract Brevo
// reads, so duplicating it here is intentional.
const auditKindExpiryImminentLiteral = "resource.expiry_imminent"

// imminentRowCols is the column order the worker's SELECT returns.
// Keep in sync with expire_imminent.go::Work's SELECT projection.
var imminentRowCols = []string{
	"id", "token", "team_id", "resource_type", "expires_at", "owner_email",
}

// TestExpireImminent_EmitsForResourceWithin1h covers scenarios 1 + 8: a
// freshly-eligible resource (30min to expiry, no prior audit row in the
// last 12h) yields exactly one INSERT into audit_log whose metadata JSONB
// carries resource_id, email, hours_remaining, expires_at, and token.
func TestExpireImminent_EmitsForResourceWithin1h(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resourceID := uuid.New()
	token := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(30 * time.Minute)

	mock.ExpectQuery(`FROM resources r`).
		WillReturnRows(sqlmock.NewRows(imminentRowCols).
			AddRow(resourceID, token, teamID, "postgres", expires, "owner@example.com"))

	// The INSERT we're verifying. Capture metadata via a custom
	// argument matcher so we can decode + assert on the JSONB body.
	var capturedMeta []byte
	metaMatcher := &captureBytesArg{out: &capturedMeta}

	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(
			teamID,
			"system",
			auditKindExpiryImminentLiteral,
			sqlmock.AnyArg(), // summary — checked below via the kind
			metaMatcher,
			"postgres",
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := jobs.NewExpireImminentWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireImminentArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}

	// Scenario 8: metadata payload contract.
	var meta map[string]any
	if err := json.Unmarshal(capturedMeta, &meta); err != nil {
		t.Fatalf("metadata JSON unmarshal: %v (raw: %s)", err, capturedMeta)
	}
	if got := meta["resource_id"]; got != resourceID.String() {
		t.Errorf("metadata.resource_id = %v, want %s", got, resourceID)
	}
	if got := meta["email"]; got != "owner@example.com" {
		t.Errorf("metadata.email = %v, want owner@example.com", got)
	}
	if got := meta["resource_type"]; got != "postgres" {
		t.Errorf("metadata.resource_type = %v, want postgres", got)
	}
	if got := meta["token"]; got != token.String() {
		t.Errorf("metadata.token = %v, want %s", got, token)
	}
	// hours_remaining is float (rounded to nearest 0.1). 30min → 0.5h.
	hrs, ok := meta["hours_remaining"].(float64)
	if !ok {
		t.Errorf("metadata.hours_remaining missing or wrong type: %T %v", meta["hours_remaining"], meta["hours_remaining"])
	} else if hrs < 0.4 || hrs > 0.6 {
		t.Errorf("metadata.hours_remaining = %v, want ~0.5", hrs)
	}
	if _, ok := meta["expires_at"].(string); !ok {
		t.Errorf("metadata.expires_at missing or wrong type: %T", meta["expires_at"])
	}
}

// TestExpireImminent_NoCandidates covers scenarios 2, 4, 5, 6: in each of
// those cases the candidate SELECT excludes the row at the SQL layer (the
// dedupe NOT IN subquery, the 1h window predicate, the team_id NOT NULL
// filter). From the worker's point of view, the rowset is empty → no
// INSERT fires.
func TestExpireImminent_NoCandidates(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`FROM resources r`).
		WillReturnRows(sqlmock.NewRows(imminentRowCols))

	w := jobs.NewExpireImminentWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireImminentArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestExpireImminent_EmitsAfterDedupeWindowPassed covers scenario 3: the
// prior audit row was written 13h ago, outside the 12h dedupe window, so
// the candidate SELECT now returns the row again and we emit a fresh
// audit row. The worker's behaviour is identical to scenario 1; the dedupe
// math itself lives in the SQL predicate (created_at > now() - 12h).
func TestExpireImminent_EmitsAfterDedupeWindowPassed(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resourceID := uuid.New()
	token := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(45 * time.Minute)

	mock.ExpectQuery(`FROM resources r`).
		WillReturnRows(sqlmock.NewRows(imminentRowCols).
			AddRow(resourceID, token, teamID, "redis", expires, "owner@example.com"))

	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(
			teamID,
			"system",
			auditKindExpiryImminentLiteral,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			"redis",
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := jobs.NewExpireImminentWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireImminentArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestExpireImminent_SkipsWhenNoOwnerEmail covers scenario 7: an
// authenticated resource whose team has zero users (orphan team / stuck
// signup) → owner_email is "". The worker logs a warn and does NOT
// INSERT — an audit row with no addressable email is dead weight for the
// Loops/Brevo path.
func TestExpireImminent_SkipsWhenNoOwnerEmail(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resourceID := uuid.New()
	token := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(20 * time.Minute)

	// owner_email returns "" (LEFT JOIN with no user match → COALESCE
	// emits empty string per the worker's SELECT projection).
	mock.ExpectQuery(`FROM resources r`).
		WillReturnRows(sqlmock.NewRows(imminentRowCols).
			AddRow(resourceID, token, teamID, "mongodb", expires, ""))

	// No INSERT expected — sqlmock strict mode fails if one fires.

	w := jobs.NewExpireImminentWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireImminentArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestExpireImminent_TopLevelQueryError_ReturnsError verifies that a
// fatal SELECT failure propagates so River retries the job. Per-row
// errors are fail-open (logged) per the file's contract, but the
// top-level query is the one River-visible error path.
func TestExpireImminent_TopLevelQueryError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`FROM resources r`).WillReturnError(errDB)

	w := jobs.NewExpireImminentWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireImminentArgs]()); err == nil {
		t.Fatal("expected error from top-level SELECT failure, got nil")
	}
}

// TestExpireImminent_FailOpenOnInsertError covers the per-row insert
// failure path: one bad INSERT does NOT stop the worker from processing
// the rest of the batch and does NOT propagate. The test gives the
// worker two candidates, fails the first INSERT, and asserts the second
// INSERT still fires.
func TestExpireImminent_FailOpenOnInsertError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id1, id2 := uuid.New(), uuid.New()
	tok1, tok2 := uuid.New(), uuid.New()
	team1, team2 := uuid.New(), uuid.New()
	expires := time.Now().UTC().Add(40 * time.Minute)

	mock.ExpectQuery(`FROM resources r`).
		WillReturnRows(sqlmock.NewRows(imminentRowCols).
			AddRow(id1, tok1, team1, "postgres", expires, "a@example.com").
			AddRow(id2, tok2, team2, "redis", expires, "b@example.com"))

	// First INSERT fails — the second one must still happen.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(team1, "system", auditKindExpiryImminentLiteral, sqlmock.AnyArg(), sqlmock.AnyArg(), "postgres").
		WillReturnError(errDB)
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(team2, "system", auditKindExpiryImminentLiteral, sqlmock.AnyArg(), sqlmock.AnyArg(), "redis").
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := jobs.NewExpireImminentWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireImminentArgs]()); err != nil {
		t.Fatalf("expected nil (fail-open) on per-row INSERT error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// captureBytesArg is a sqlmock.Argument that captures the raw bytes the
// worker passed for a JSONB column. We use it to introspect the
// audit_log.metadata payload after the worker writes it.
//
// Implements sqlmock.Argument: Match(driver.Value) bool.
type captureBytesArg struct {
	out *[]byte
}

// Match satisfies the sqlmock.Argument interface.
func (c *captureBytesArg) Match(v driver.Value) bool {
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

// TestExpireImminent_JoinsOnlyPrimaryUser pins fix #1: the candidate query's
// LEFT JOIN users MUST carry `AND u.is_primary = true` so the owner email is
// the team's canonical primary user — not the oldest user (the old
// LEFT JOIN LATERAL … ORDER BY created_at ASC LIMIT 1 picked whoever signed
// up first, which is not necessarily the primary). Migration 029
// (uq_users_one_primary_per_team) guarantees exactly one match.
//
// sqlmock's QueryMatcherRegexp matches the expected query as a regex against
// the SQL the worker actually issues; a query missing the predicate fails
// ExpectationsWereMet. Mirrors TestExpiryReminderWorker_JoinsOnlyPrimaryUser.
func TestExpireImminent_JoinsOnlyPrimaryUser(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`LEFT JOIN users u ON u\.team_id = r\.team_id AND u\.is_primary = true`).
		WillReturnRows(sqlmock.NewRows(imminentRowCols))

	w := jobs.NewExpireImminentWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireImminentArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("query did not include the `AND u.is_primary = true` join predicate: %v", err)
	}
}

// TestExpireImminent_DedupeUsesNullSafeNotExists pins fix #3: the 12h dedupe
// MUST be a correlated NOT EXISTS, not a `NOT IN (subquery)`. A NOT IN against
// a subquery that can project a NULL uuid — an audit_log row whose
// metadata->>'resource_id' is JSON null → (null)::uuid — evaluates NULL for
// EVERY candidate row, so the whole query returns nothing and the worker
// silently idles. NOT EXISTS is NULL-safe.
//
// This test fails if the query ever reverts to `NOT IN`: the regex requires
// the NOT EXISTS form to be present.
func TestExpireImminent_DedupeUsesNullSafeNotExists(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Require the NULL-safe correlated NOT EXISTS dedupe clause.
	mock.ExpectQuery(`NOT EXISTS \(\s*SELECT 1\s*FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(imminentRowCols))

	w := jobs.NewExpireImminentWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireImminentArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("dedupe clause is not a NULL-safe NOT EXISTS (likely reverted to NOT IN): %v", err)
	}
}
