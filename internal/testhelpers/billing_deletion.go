package testhelpers

// billing_deletion.go — harness extensions for the Tier-1 (data-loss / money-
// adjacent) worker jobs covered by INTEGRATION-COVERAGE-PLAN-2026-06-04.md
// §5 #1: billing_reconciler (terminal downgrade) and team_deletion_executor
// (purge cascade). Kept in a separate file from testhelpers.go so the Wave-2
// T2 additions are reviewable on their own and merge cleanly behind the
// Wave-1 harness PR.
//
// Like testhelpers.go, every seed/read here targets the FULL api-migrated
// platform schema (the integration job applies all api migrations before
// running these tests; a developer box has the same). The columns these
// helpers touch beyond the testhelpers.go subset — teams.stripe_customer_id /
// deletion_requested_at / tombstoned_at, resources.connection_url /
// key_prefix, the users table — exist in that migrated schema. Where a column
// is absent on a bare harness DB, the helper degrades via isUndefinedColumn
// (the same fall-back testhelpers.SeedDeployment uses) so the harness still
// loads against a non-migrated DB even though these particular Tier-1 tests
// require the migrated one.

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
)

// lowSortingTeamID returns a UUID that sorts at the very front of an
// `ORDER BY id` scan (it begins with all-zero high bytes) while remaining
// unique per call (random low 48 bits). The billing reconciler's candidate
// query is `... ORDER BY id LIMIT 100`; against a heavily-polluted shared test
// DB (a developer box can accumulate tens of thousands of stray teams) a
// random-UUID seed sorts past position 100 and is never processed, making the
// assertion flake on the environment rather than the code. Seeding the target
// team with a guaranteed-first id makes the test deterministic on ANY DB
// state — fresh CI or polluted local — without a production-code scope hook.
func lowSortingTeamID() uuid.UUID {
	r := uuid.New()
	// Zero the first 10 bytes; keep the last 6 random for uniqueness. The
	// result is a valid UUID that lexically precedes any organically-generated
	// (v4) id, whose first bytes are effectively never all zero.
	for i := 0; i < 10; i++ {
		r[i] = 0
	}
	return r
}

// SeedTeamWithSubscription inserts an active paid team that sorts FIRST in the
// billing reconciler's `ORDER BY id LIMIT 100` candidate scan (see
// lowSortingTeamID) and stamps its Razorpay subscription id, returning both.
// Use this for billing-reconciler integration tests so the seeded team is
// always within the batch regardless of how many other subscription-bearing
// teams the shared DB carries. Cleaned up after the test.
func SeedTeamWithSubscription(t *testing.T, db *sql.DB, planTier, subscriptionID string) uuid.UUID {
	t.Helper()
	if planTier == "" {
		planTier = "pro"
	}
	id := lowSortingTeamID()
	if _, err := db.Exec(
		`INSERT INTO teams (id, name, plan_tier, status, stripe_customer_id)
		 VALUES ($1, $2, $3, 'active', $4)`,
		id, "itest-bill-"+id.String()[24:], planTier, subscriptionID,
	); err != nil {
		tFatalf(t, "SeedTeamWithSubscription: %v", err)
		return uuid.Nil
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM teams WHERE id = $1`, id)
	})
	return id
}

// SetTeamSubscription stamps a team's Razorpay subscription id (stored in the
// legacy-named teams.stripe_customer_id column) so the billing reconciler's
// candidate query (WHERE stripe_customer_id IS NOT NULL) selects it. The
// reconciler keys its per-team Razorpay fetch on this value, so a stub
// fetcher can branch on it to return a terminal/active/grace status for ONLY
// the seeded team — which is what makes the assertion deterministic against a
// shared local DB that may carry other subscription-bearing teams.
func SetTeamSubscription(t *testing.T, db *sql.DB, teamID uuid.UUID, subscriptionID string) {
	t.Helper()
	if _, err := db.Exec(
		`UPDATE teams SET stripe_customer_id = $1 WHERE id = $2`,
		subscriptionID, teamID,
	); err != nil {
		tFatalf(t, "SetTeamSubscription: %v", err)
		return
	}
}

// TeamPlanTier reads back teams.plan_tier for a row — the integration
// assertion target for the billing reconciler's terminal downgrade
// (plan_tier flips pro → hobby) and for the deletion executor's tombstone.
func TeamPlanTier(t *testing.T, db *sql.DB, teamID uuid.UUID) string {
	t.Helper()
	var tier string
	if err := db.QueryRow(
		`SELECT plan_tier FROM teams WHERE id = $1`, teamID,
	).Scan(&tier); err != nil {
		tFatalf(t, "TeamPlanTier: %v", err)
		return ""
	}
	return tier
}

// TeamStatus reads back teams.status — the integration assertion target for
// the deletion executor (deletion_requested → tombstoned).
func TeamStatus(t *testing.T, db *sql.DB, teamID uuid.UUID) string {
	t.Helper()
	var status string
	if err := db.QueryRow(
		`SELECT status FROM teams WHERE id = $1`, teamID,
	).Scan(&status); err != nil {
		tFatalf(t, "TeamStatus: %v", err)
		return ""
	}
	return status
}

// CountAuditLogByTeam returns how many audit_log rows of the given kind
// reference the given team id. Used to assert the billing reconciler's
// subscription.canceled emit and the deletion executor's team.tombstoned emit
// — both write team-scoped audit rows the event-email forwarder later
// dispatches from. Counting by (team_id, kind) — rather than the
// metadata->>'deploy_id' path CountAuditLog uses — is the right key for these
// team-lifecycle events.
func CountAuditLogByTeam(t *testing.T, db *sql.DB, teamID uuid.UUID, kind string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(
		`SELECT count(*) FROM audit_log WHERE team_id = $1 AND kind = $2`,
		teamID, kind,
	).Scan(&n); err != nil {
		tFatalf(t, "CountAuditLogByTeam: %v", err)
		return 0
	}
	return n
}

// SeedTeamPendingDeletion inserts a team already in status='deletion_requested'
// with deletion_requested_at set graceElapsedDays in the PAST, so the
// team_deletion_executor's candidate query (deletion_requested_at + 30d <
// now()) selects it. Pass a value > 30 to model a team whose grace window has
// elapsed. The team carries a non-empty stripe_customer_id + name so the
// tombstone step's NULL-out of those PII columns is observable. Cleaned up
// after the test.
func SeedTeamPendingDeletion(t *testing.T, db *sql.DB, planTier string, graceElapsedDays int) uuid.UUID {
	t.Helper()
	if planTier == "" {
		planTier = "pro"
	}
	id := uuid.New()
	requestedAt := time.Now().UTC().Add(-time.Duration(graceElapsedDays) * 24 * time.Hour)
	if _, err := db.Exec(`
		INSERT INTO teams (id, name, plan_tier, status, stripe_customer_id, deletion_requested_at)
		VALUES ($1, $2, $3, 'deletion_requested', $4, $5)
	`, id, "itest-del-"+id.String()[:8], planTier, "sub_"+id.String()[:12], requestedAt); err != nil {
		tFatalf(t, "SeedTeamPendingDeletion: %v", err)
		return uuid.Nil
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM teams WHERE id = $1`, id)
	})
	return id
}

// SeedResourceWithSecret inserts a resources row carrying a non-empty
// connection_url + key_prefix — the customer-data fields the deletion
// executor NULLs/blanks in its tombstone transaction. Returns the row id so
// the test can read those fields back and assert they were scrubbed. Cleaned
// up after the test.
func SeedResourceWithSecret(t *testing.T, db *sql.DB, teamID uuid.UUID, resourceType string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	token := uuid.New()
	if _, err := db.Exec(`
		INSERT INTO resources
			(id, team_id, token, resource_type, tier, status, connection_url, key_prefix, created_at)
		VALUES ($1, $2, $3, $4, 'pro', 'active', $5, $6, now())
	`, id, teamID, token, resourceType,
		"postgres://itest-secret@localhost:5432/db_"+id.String()[:8],
		"prefix-"+id.String()[:8]+"/",
	); err != nil {
		tFatalf(t, "SeedResourceWithSecret: %v", err)
		return uuid.Nil
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM resources WHERE id = $1`, id)
	})
	return id
}

// ResourceSecretFields reads back resources.connection_url + key_prefix for a
// row — the deletion-executor assertion target (connection_url → NULL,
// key_prefix → ”).
func ResourceSecretFields(t *testing.T, db *sql.DB, id uuid.UUID) (connURL sql.NullString, keyPrefix string) {
	t.Helper()
	if err := db.QueryRow(
		`SELECT connection_url, COALESCE(key_prefix, '') FROM resources WHERE id = $1`, id,
	).Scan(&connURL, &keyPrefix); err != nil {
		tFatalf(t, "ResourceSecretFields: %v", err)
		return sql.NullString{}, ""
	}
	return connURL, keyPrefix
}

// SeedUser inserts a users row owned by the team with the given email so the
// deletion executor's PII-scrub step (email → deleted-<id>@tombstoned.invalid,
// github_id/google_id → NULL) is observable. Returns the user id. Cleaned up
// after the test (the team-cascade also tidies it; this is belt-and-braces for
// a test that seeds a user without a cascading team delete).
func SeedUser(t *testing.T, db *sql.DB, teamID uuid.UUID, email string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	// github_id is seeded non-NULL so the scrub-to-NULL is observable. The
	// prod users table carries more NOT NULL columns than the bare harness
	// shape; this INSERT targets the api-migrated schema (the only schema
	// these Tier-1 tests run against). github_id is a TEXT/bigint depending on
	// migration; pass a string-shaped id which both accept via implicit cast,
	// falling back to a NULL github_id if the column rejects it.
	_, err := db.Exec(`
		INSERT INTO users (id, team_id, email, github_id)
		VALUES ($1, $2, $3, $4)
	`, id, teamID, email, "gh-"+id.String()[:8])
	if err != nil {
		// Fall back without github_id (bare schema / type mismatch): the
		// email scrub is the load-bearing assertion regardless.
		_, err = db.Exec(`
			INSERT INTO users (id, team_id, email)
			VALUES ($1, $2, $3)
		`, id, teamID, email)
	}
	if err != nil {
		tFatalf(t, "SeedUser: %v", err)
		return uuid.Nil
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM users WHERE id = $1`, id)
	})
	return id
}

// UserEmail reads back users.email for a row — the deletion-executor PII-scrub
// assertion target.
func UserEmail(t *testing.T, db *sql.DB, id uuid.UUID) string {
	t.Helper()
	var email string
	if err := db.QueryRow(
		`SELECT email FROM users WHERE id = $1`, id,
	).Scan(&email); err != nil {
		tFatalf(t, "UserEmail: %v", err)
		return ""
	}
	return email
}
