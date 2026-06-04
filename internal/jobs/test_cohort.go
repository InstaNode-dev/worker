package jobs

// test_cohort.go — shared skip-guards for synthetic test-cohort teams.
//
// teams.is_test_cohort (api migration 067) tags durable per-tier teams the
// continuous synthetic-monitoring program seeds (synthetic+hobby@instanode.dev,
// etc.). Those teams provision real resources on a continuous cadence so the
// synthetic runner can assert the live funnel — but to every background job
// they would otherwise look like real customers: the billing reconciler would
// flag them as drift (no real Razorpay subscription), the quota/expiry/churn/
// digest jobs would email synthetic addresses (Brevo-rejected noise + ledger
// pollution) and skew the real 2%/20% funnel.
//
// This file is the worker-side follow-up to api PR #246 (W0): every
// team-iterating job that charges, churns, emails, or quota-nudges a team MUST
// no-op for is_test_cohort teams. The plan enumerating the jobs is
// docs/sessions/2026-06-04/TEST-ACCOUNTS-AND-NR-SYNTHETICS-PLAN.md §1.6.
//
// # The two skip mechanisms
//
//  1. SQL-driven scans (a SELECT over teams / resources / a team-keyed table)
//     add the cohort filter inline so a test-cohort team never enters the
//     candidate set. SQL-driven jobs join teams and add `AND NOT t.is_test_cohort`,
//     or — when the candidate row carries a team_id but does not already join
//     teams — gate via a NOT EXISTS subselect (testCohortNotExistsClause).
//
//  2. Per-team Go checks (a job that already holds a team_id mid-loop) call
//     isTestCohort(ctx, db, teamID) and `continue` on true.
//
// # Inert by default
//
// No real team carries is_test_cohort=true today (migration 067 defaults every
// row to false and only a future seeder flips the synthetic teams). So every
// guard added here is a pure no-op for all real teams — a safety guard, not a
// feature, and it deliberately does NOT depend on FLOW_SYNTHETIC_ENABLED (that
// flag gates the future synthetic RUNNER, not these guards).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
)

// testCohortNotExistsClause is the SQL fragment that excludes synthetic
// test-cohort teams from a candidate scan whose driving table carries a
// `team_id` but does not already join teams. Drop it into the WHERE clause of
// such a scan; it filters out any row whose owning team is flagged
// is_test_cohort=true.
//
// It is a correlated NOT EXISTS (rather than `team_id NOT IN (SELECT …)`)
// because NOT IN is NULL-unsafe: an anonymous resource carries team_id IS NULL,
// and `NULL NOT IN (…)` evaluates to NULL (treated as false), which would
// silently drop every anonymous row from the scan. NOT EXISTS keeps NULL-
// team_id rows in the result set — they cannot be a synthetic team (synthetic
// teams are always real, seeded teams), so an anonymous row must never be
// filtered by the cohort guard. The correlation alias is the literal `r` so the
// fragment slots into the expiry reaper's existing `FROM resources r`; jobs
// whose driving alias differs format the team-id column in via %s.
//
// Usage (resources reaper, alias r):
//
//	WHERE … AND NOT EXISTS (
//	    SELECT 1 FROM teams tc WHERE tc.id = r.team_id AND tc.is_test_cohort
//	)
//
// Kept as a formatting helper so the (single, fixed) `tc` subselect alias can
// never collide with the caller's own table aliases.
func testCohortNotExistsClause(teamIDExpr string) string {
	return fmt.Sprintf(
		"NOT EXISTS (SELECT 1 FROM teams tc WHERE tc.id = %s AND tc.is_test_cohort)",
		teamIDExpr,
	)
}

// isTestCohort reports whether the given team is a synthetic test-cohort team
// (teams.is_test_cohort = true). It is the per-team Go guard for jobs that
// already hold a team_id mid-loop and cannot push the filter into their
// candidate SELECT (e.g. the payment-grace and checkout reconcilers, which scan
// payment_grace_periods / pending_checkouts and only carry the team_id forward).
//
// Fail-CLOSED is wrong here and fail-OPEN is wrong too — instead it fails
// SAFE-FOR-REAL-TEAMS: on any DB error (or a team row that has vanished) it
// returns (false, err). The caller treats a non-nil err as "could not confirm
// synthetic — process the team normally", so a transient platform-DB blip never
// silently suppresses a REAL customer's charge / dunning email. Because no real
// team is ever is_test_cohort=true, treating "unknown" as "not synthetic" can
// only ever mis-handle a synthetic team during a DB outage (a tolerable,
// self-correcting miss), never a real one.
func isTestCohort(ctx context.Context, db *sql.DB, teamID uuid.UUID) (bool, error) {
	var flagged bool
	err := db.QueryRowContext(ctx,
		`SELECT is_test_cohort FROM teams WHERE id = $1`, teamID,
	).Scan(&flagged)
	if errors.Is(err, sql.ErrNoRows) {
		// Team row gone (deleted between the candidate SELECT and this check).
		// Not synthetic by any meaningful definition; let the caller's own
		// row-vanished handling take over.
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("isTestCohort: %w", err)
	}
	return flagged, nil
}

// skipIfTestCohort is the convenience wrapper the per-team-loop jobs use. It
// returns true when the team should be SKIPPED (it is synthetic). On a DB error
// it logs at WARN under jobKind and returns false — "could not confirm, so
// process normally" — so a platform-DB blip never suppresses a real customer's
// money/email path (see isTestCohort's fail-safe-for-real-teams rationale).
func skipIfTestCohort(ctx context.Context, db *sql.DB, teamID uuid.UUID, jobKind string) bool {
	flagged, err := isTestCohort(ctx, db, teamID)
	if err != nil {
		slog.Warn(jobKind+".test_cohort_check_failed",
			"team_id", teamID.String(),
			"error", err,
			"note", "could not confirm is_test_cohort — processing team normally (no real team is synthetic, so this only ever risks one synthetic tick during a DB blip)",
		)
		return false
	}
	if flagged {
		slog.Debug(jobKind+".test_cohort_skipped",
			"team_id", teamID.String(),
			"note", "synthetic test-cohort team — no-op (no charge/churn/email/quota effect)",
		)
		return true
	}
	return false
}
