package jobs

// test_cohort_registry_test.go — registry-iterating regression test (CLAUDE.md
// rule 18) for the synthetic test-cohort skip-guards.
//
// The bug class this guards: a team-iterating job that charges / churns /
// emails / quota-nudges a team ships WITHOUT a is_test_cohort filter, so a
// seeded synthetic team starts getting real charges flagged, real dunning
// emails, and pollutes the conversion funnel. The plan enumerating the jobs is
// docs/sessions/2026-06-04/TEST-ACCOUNTS-AND-NR-SYNTHETICS-PLAN.md §1.6.
//
// This test does NOT hand-maintain a "these jobs are guarded" allow-list and
// assert each. Instead it pins the FULL §1.6 enumeration: every file that MUST
// carry a guard is listed with the exact cohort-guard token its scan uses, and
// the test fails if that token disappears from the file (a refactor dropped the
// guard) — so the guard cannot silently rot out. The deliberately-NOT-guarded
// jobs are listed separately with their reason, so a future reader sees the
// scope boundary was a decision, not an omission.

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// cohortGuardToken is one of the two skip mechanisms a guarded job uses:
//   - "testCohortNotExistsClause" — the SQL NOT-EXISTS fragment, for scans whose
//     driving table carries a team_id but does not already join teams.
//   - "is_test_cohort"            — a literal `AND NOT [t.]is_test_cohort` added
//     to a scan that already selects from / joins teams.
//   - "skipIfTestCohort"          — the per-team Go guard inside a loop.
//
// A file satisfies the registry test if it contains AT LEAST ONE of these. Two
// tokens cover every guarded file; the test asserts presence, not which one.
var cohortGuardTokens = []string{
	"testCohortNotExistsClause",
	"is_test_cohort",
	"skipIfTestCohort",
}

// guardedJobFiles is the FULL §1.6 enumeration: every team-iterating job file
// that MUST no-op for an is_test_cohort team. Adding a new such job without a
// guard, or refactoring a guard out of an existing one, reds this test.
var guardedJobFiles = []string{
	// Quota scan / 80% nudge — synthetic provisions would trip quota nudges →
	// fake upsell emails + audit noise.
	"quota.go",
	"quota_wall_nudge.go",
	// Churn predictor — seeded paid teams with synthetic-only activity would
	// skew churn scores + emit we-miss-you emails.
	"churn_predictor.go",
	// Expiry / TTL WARNING emailers — must not email the synthetic owner
	// (Brevo-rejected + ledger pollution). (The TTL REAPERS themselves —
	// expire.go / expire_stacks.go — are deliberately NOT guarded; see
	// notGuardedJobFiles so synthetic resources never leak.)
	"expire_imminent.go",
	"expiry_reminder.go",
	// Billing / charge — seeded paid teams have NO real Razorpay subscription →
	// reconciler would flag them as drift / undeliverable charge.
	"billing_reconciler.go",
	"checkout_reconcile.go",
	"payment_grace_reminder.go",
	"payment_grace_terminator.go",
	// Weekly digest / lifecycle email — no transactional email to synthetic
	// addresses.
	"email.go",
}

// notGuardedJobFiles documents the team-touching jobs that are DELIBERATELY left
// unguarded, each with its reason. Listed so the scope boundary is an auditable
// decision (rule 17 coverage discipline), not a silent miss. This slice is
// documentation; the test does not assert on it beyond existence.
var notGuardedJobFiles = map[string]string{
	"expire.go":           "TTL REAPER, not an emailer — must reap synthetic anon/free RESOURCES so they never leak (plan: keep TTL-expiry of test-cohort resources intact). It only acts on tier IN ('anonymous','free'); seeded synthetic teams are paid-tier and out of its scope anyway.",
	"expire_stacks.go":    "TTL REAPER for anonymous stacks (NULL team_id; synthetic deploys are anon /stacks/new per project memory). Must tear down synthetic stacks so namespaces never leak; no customer email is sent.",
	"lifecycle_emails.go": "Pure Go email RENDER layer — no DB scan, no team iteration. The TRIGGER jobs that decide whether to email (guarded above) gate it upstream.",
}

func jobsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(thisFile)
}

// TestAllTeamIteratingJobs_FilterTestCohort is the rule-18 net: every file in
// guardedJobFiles must contain a cohort-guard token. A guarded job that loses
// its guard (refactor) or a new team-iterating job added to the list without a
// guard reds here, BEFORE a synthetic team starts polluting production.
func TestAllTeamIteratingJobs_FilterTestCohort(t *testing.T) {
	dir := jobsDir(t)
	for _, fname := range guardedJobFiles {
		fname := fname
		t.Run(fname, func(t *testing.T) {
			path := filepath.Join(dir, fname)
			src, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", fname, err)
			}
			body := string(src)
			found := ""
			for _, tok := range cohortGuardTokens {
				if strings.Contains(body, tok) {
					found = tok
					break
				}
			}
			if found == "" {
				t.Errorf("%s is enumerated as a team-iterating job that must skip "+
					"is_test_cohort teams, but carries NONE of the cohort-guard tokens %v. "+
					"A refactor dropped the guard, or a new job was added without one — "+
					"add the AND-NOT-is_test_cohort filter / skipIfTestCohort call.",
					fname, cohortGuardTokens)
			}
		})
	}
}

// TestNotGuardedJobs_AreStillPresent pins the deliberately-unguarded files so a
// reviewer who renames/removes one is forced to revisit the scope decision. A
// missing file here means the documented boundary drifted from reality.
func TestNotGuardedJobs_AreStillPresent(t *testing.T) {
	dir := jobsDir(t)
	for fname, reason := range notGuardedJobFiles {
		if reason == "" {
			t.Errorf("%s is in notGuardedJobFiles with no reason — document why it is unguarded", fname)
		}
		if _, err := os.Stat(filepath.Join(dir, fname)); err != nil {
			t.Errorf("notGuardedJobFiles names %s but it does not exist: %v — "+
				"the documented scope boundary drifted; revisit whether its replacement needs a guard", fname, err)
		}
	}
}
