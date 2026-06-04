package testhelpers

// cohort.go — seed/read helpers for the synthetic test-cohort skip-guard tests.
//
// These back the worker-side cohort skip-guards (test_cohort.go +
// docs/sessions/2026-06-04/TEST-ACCOUNTS-AND-NR-SYNTHETICS-PLAN.md §1.6): every
// team-iterating job that charges / churns / emails / quota-nudges a team must
// no-op for a team flagged teams.is_test_cohort=true. The integration tests
// seed one normal team and one cohort team, run the job, and assert the cohort
// team produced no artifact while the normal team did.

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
)

// SetTeamTestCohort flips teams.is_test_cohort for a seeded team. Used to turn a
// freshly-seeded normal team into a synthetic test-cohort team so a guard test
// can assert the job skips it.
func SetTeamTestCohort(t *testing.T, db *sql.DB, teamID uuid.UUID, flagged bool) {
	t.Helper()
	if _, err := db.Exec(
		`UPDATE teams SET is_test_cohort = $1 WHERE id = $2`, flagged, teamID,
	); err != nil {
		tFatalf(t, "SetTeamTestCohort: %v", err)
	}
}

// SeedPrimaryUser inserts a primary (is_primary=true) user for a team — the
// recipient the expiry-warning and weekly-digest scans join on. Distinct from
// SeedUser, which leaves is_primary at its default (false).
//
// emailLocalPart is a hint; the actual address is made globally unique by
// embedding the new user id, so two tests passing the same hint cannot collide
// on the prod users_email_key unique constraint on a shared local DB.
func SeedPrimaryUser(t *testing.T, db *sql.DB, teamID uuid.UUID, emailLocalPart string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	email := emailLocalPart + "+" + id.String()[:12] + "@example.test"
	if _, err := db.Exec(
		`INSERT INTO users (id, team_id, email, is_primary) VALUES ($1, $2, $3, true)`,
		id, teamID, email,
	); err != nil {
		tFatalf(t, "SeedPrimaryUser: %v", err)
		return uuid.Nil
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM users WHERE id = $1`, id) })
	return id
}

// SeedExpiringFreeResource inserts an active free-tier resource whose
// expires_at falls inside the given duration from now — a candidate for the
// expiry-warning sweeps (expire_imminent / expiry_reminder). Returns the id.
func SeedExpiringFreeResource(t *testing.T, db *sql.DB, teamID uuid.UUID, resourceType string, expiresIn time.Duration) uuid.UUID {
	t.Helper()
	id := uuid.New()
	token := uuid.New()
	if _, err := db.Exec(`
		INSERT INTO resources
			(id, team_id, token, resource_type, tier, status, expires_at, reminders_sent, created_at)
		VALUES ($1, $2, $3, $4, 'free', 'active', $5, 0, now())
	`, id, teamID, token, resourceType, time.Now().UTC().Add(expiresIn)); err != nil {
		tFatalf(t, "SeedExpiringFreeResource: %v", err)
		return uuid.Nil
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM resources WHERE id = $1`, id) })
	return id
}

// SeedOverQuotaResource inserts an active resource whose storage_bytes exceeds
// the given limit — a candidate for the quota-wall nudge / suspend sweep. tier
// drives which plan limit the scan compares against. Returns the id.
func SeedOverQuotaResource(t *testing.T, db *sql.DB, teamID uuid.UUID, resourceType, tier string, storageBytes int64) uuid.UUID {
	t.Helper()
	id := uuid.New()
	token := uuid.New()
	if _, err := db.Exec(`
		INSERT INTO resources
			(id, team_id, token, resource_type, tier, status, storage_bytes, name, created_at)
		VALUES ($1, $2, $3, $4, $5, 'active', $6, $7, now())
	`, id, teamID, token, resourceType, tier, storageBytes, "itest-quota-"+id.String()[:8]); err != nil {
		tFatalf(t, "SeedOverQuotaResource: %v", err)
		return uuid.Nil
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM resources WHERE id = $1`, id) })
	return id
}

// SeedAbandonedCheckout inserts a pending_checkouts row old enough to be an
// abandoned-checkout candidate (created beyond the grace window, unresolved,
// un-notified) for the given team. Returns the subscription_id.
//
// created_at is set FAR in the past (10 years) so the row reliably sorts within
// the checkout reconciler's `ORDER BY created_at ASC LIMIT 200` batch even on a
// shared local DB that already carries hundreds of accumulated unresolved
// checkouts — otherwise a freshly "1h ago" row could fall outside the
// oldest-200 window and the test would flake.
func SeedAbandonedCheckout(t *testing.T, db *sql.DB, teamID uuid.UUID, email string) string {
	t.Helper()
	subID := "sub_ckab_" + uuid.New().String()[:12]
	if _, err := db.Exec(`
		INSERT INTO pending_checkouts
			(subscription_id, team_id, customer_email, plan_tier, created_at)
		VALUES ($1, $2, $3, 'pro', now() - interval '3650 days')
	`, subID, teamID, email); err != nil {
		tFatalf(t, "SeedAbandonedCheckout: %v", err)
		return ""
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM pending_checkouts WHERE subscription_id = $1`, subID) })
	return subID
}

// SeedActiveGracePeriod inserts an 'active' payment_grace_periods row for the
// team. expiresIn > 0 makes it a reminder candidate (clock still running);
// expiresIn < 0 makes it a terminator candidate (clock elapsed). Returns id.
func SeedActiveGracePeriod(t *testing.T, db *sql.DB, teamID uuid.UUID, expiresIn time.Duration) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := db.Exec(`
		INSERT INTO payment_grace_periods
			(id, team_id, subscription_id, status, started_at, expires_at, reminders_sent)
		VALUES ($1, $2, $3, 'active', now() - interval '1 day', $4, 0)
	`, id, teamID, "sub_grace_"+id.String()[:12], time.Now().UTC().Add(expiresIn)); err != nil {
		tFatalf(t, "SeedActiveGracePeriod: %v", err)
		return uuid.Nil
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM payment_grace_periods WHERE id = $1`, id) })
	return id
}

// GraceRemindersSent reads back payment_grace_periods.reminders_sent — the
// reminder-sweep assertion target (a cohort team's counter must stay 0).
func GraceRemindersSent(t *testing.T, db *sql.DB, id uuid.UUID) int {
	t.Helper()
	var n int
	if err := db.QueryRow(
		`SELECT reminders_sent FROM payment_grace_periods WHERE id = $1`, id,
	).Scan(&n); err != nil {
		tFatalf(t, "GraceRemindersSent: %v", err)
		return -1
	}
	return n
}

// CheckoutNotified reports whether a pending_checkouts row has been stamped
// failure_notified_at — the checkout-reconcile assertion target (a cohort
// team's checkout must never be notified).
func CheckoutNotified(t *testing.T, db *sql.DB, subscriptionID string) bool {
	t.Helper()
	var notified sql.NullTime
	if err := db.QueryRow(
		`SELECT failure_notified_at FROM pending_checkouts WHERE subscription_id = $1`, subscriptionID,
	).Scan(&notified); err != nil {
		tFatalf(t, "CheckoutNotified: %v", err)
		return false
	}
	return notified.Valid
}
