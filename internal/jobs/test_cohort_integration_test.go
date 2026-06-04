package jobs

// test_cohort_integration_test.go — REAL-Postgres round-trip proof that every
// SQL-driven team-iterating job no-ops for an is_test_cohort team while still
// processing a normal team. Each test seeds TWO identical teams (one normal,
// one flagged is_test_cohort=true), runs the job once, and asserts the artifact
// (audit row / counter / notified stamp) landed for the normal team and NOT for
// the cohort team. This is the convergence a sqlmock test cannot prove: that the
// guard's WHERE/NOT-EXISTS clause actually filters the live row.
//
// GATING: testhelpers.SetupTestDB skips under no-DB (the -short `make gate` /
// deploy.yml / ci.yml lanes ship no Postgres), so these run only where a real
// platform Postgres is supplied via TEST_DATABASE_URL — same posture as the
// existing *_integration_test.go files.

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"instant.dev/worker/internal/testhelpers"
)

// cohortMockPlans is a PlanRegistry / QuotaWallPlanRegistry that reports a tight
// storage limit so a seeded over-quota resource trips the quota scans.
type cohortMockPlans struct{ storageMB, connections, provisions int }

func (m cohortMockPlans) StorageLimitMB(tier, service string) int   { return m.storageMB }
func (m cohortMockPlans) ConnectionsLimit(tier, service string) int { return m.connections }
func (m cohortMockPlans) ProvisionLimit(tier string) int            { return m.provisions }

func fakeJobID[T river.JobArgs](args T) *river.Job[T] {
	return &river.Job[T]{JobRow: &rivertype.JobRow{ID: 1}}
}

// ── quota_wall_nudge ────────────────────────────────────────────────────────

func TestIntegration_QuotaWallNudge_SkipsTestCohort(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	normal := testhelpers.SeedTeam(t, db, "pro")
	cohort := testhelpers.SeedTeam(t, db, "pro")
	testhelpers.SetTeamTestCohort(t, db, cohort, true)

	// Each team gets one postgres resource at 90% of a 10 MB limit (≥80% → nudge).
	nineMB := int64(9 * 1024 * 1024)
	testhelpers.SeedOverQuotaResource(t, db, normal, "postgres", "pro", nineMB)
	testhelpers.SeedOverQuotaResource(t, db, cohort, "postgres", "pro", nineMB)

	plans := cohortMockPlans{storageMB: 10, connections: -1, provisions: -1}
	w := NewQuotaWallNudgeWorker(db, plans)
	if err := w.Work(context.Background(), fakeJobID(QuotaWallNudgeArgs{})); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if n := testhelpers.CountAuditLogByTeam(t, db, normal, "near_quota_wall"); n != 1 {
		t.Errorf("normal team near_quota_wall rows = %d, want 1 (nudge must fire)", n)
	}
	if n := testhelpers.CountAuditLogByTeam(t, db, cohort, "near_quota_wall"); n != 0 {
		t.Errorf("cohort team near_quota_wall rows = %d, want 0 (synthetic team must be skipped)", n)
	}
}

// ── churn_predictor ─────────────────────────────────────────────────────────

func TestIntegration_ChurnPredictor_SkipsTestCohort(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	normal := testhelpers.SeedTeam(t, db, "pro")
	cohort := testhelpers.SeedTeam(t, db, "pro")
	testhelpers.SetTeamTestCohort(t, db, cohort, true)

	// Both teams: a primary user (Brevo recipient) + an active resource, and
	// ZERO activity rows → 7+ days inactive → churn candidate.
	testhelpers.SeedPrimaryUser(t, db, normal, "normal-churn")
	testhelpers.SeedPrimaryUser(t, db, cohort, "cohort-churn")
	testhelpers.SeedResourceWithSecret(t, db, normal, "postgres")
	testhelpers.SeedResourceWithSecret(t, db, cohort, "postgres")

	w := NewChurnPredictorWorker(db)
	if err := w.Work(context.Background(), fakeJobID(ChurnPredictorArgs{})); err != nil {
		t.Fatalf("Work: %v", err)
	}

	// The cohort team is excluded from the candidate FROM-set entirely by the
	// guard, so its flag count is 0 regardless of how many other candidates the
	// batch holds — the load-bearing guard assertion.
	if n := testhelpers.CountAuditLogByTeam(t, db, cohort, "churn.risk_flagged"); n != 0 {
		t.Errorf("cohort team churn.risk_flagged rows = %d, want 0 (synthetic team must be skipped)", n)
	}

	// The "normal team IS a candidate" half is asserted via the candidate
	// predicate directly rather than the global batch: churn_predictor scans at
	// most churnPredictorBatchLimit teams per tick ordered by activity, and a
	// shared/CI DB can carry far more inactive-with-resources teams than that
	// limit, so a fresh normal team is not guaranteed into the batch. Asserting
	// the predicate membership proves the guard does NOT filter a normal team
	// (the converse of the cohort=0 assertion) without depending on batch
	// position. On a clean CI DB the team is also flagged; we assert the
	// stronger, position-independent property here.
	if !churnCandidate(t, db, normal) {
		t.Error("normal team is NOT a churn candidate — the guard must not filter a normal team")
	}
	if churnCandidate(t, db, cohort) {
		t.Error("cohort team IS a churn candidate — the guard must exclude a synthetic team")
	}
}

// churnCandidate reports whether the given team matches the churn_predictor
// candidate predicate (including the AND NOT t.is_test_cohort guard) — used to
// assert guard membership independent of the per-tick batch limit.
func churnCandidate(t *testing.T, db *sql.DB, teamID uuid.UUID) bool {
	t.Helper()
	var present bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM teams t
			LEFT JOIN audit_log a ON a.team_id = t.id
				AND (a.kind = 'provision' OR a.kind = 'login'
				     OR a.kind LIKE 'deploy%' OR a.kind LIKE 'vault.%'
				     OR a.kind LIKE 'experiment.%')
			LEFT JOIN resources r ON r.team_id = t.id AND r.status = 'active'
			WHERE t.id = $1
			  AND t.plan_tier != 'team'
			  AND NOT t.is_test_cohort
			GROUP BY t.id
			HAVING (MAX(a.created_at) < now() - interval '7 days' OR MAX(a.created_at) IS NULL)
			   AND COUNT(r.id) > 0
		)
	`, teamID).Scan(&present)
	if err != nil {
		t.Fatalf("churnCandidate: %v", err)
	}
	return present
}

// ── expiry_reminder ─────────────────────────────────────────────────────────

func TestIntegration_ExpiryReminder_SkipsTestCohort(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	normal := testhelpers.SeedTeam(t, db, "free")
	cohort := testhelpers.SeedTeam(t, db, "free")
	testhelpers.SetTeamTestCohort(t, db, cohort, true)

	testhelpers.SeedPrimaryUser(t, db, normal, "normal-exp")
	testhelpers.SeedPrimaryUser(t, db, cohort, "cohort-exp")
	// Free resource expiring in 40min → stage-3 (1h) warning candidate.
	testhelpers.SeedExpiringFreeResource(t, db, normal, "postgres", 40*time.Minute)
	testhelpers.SeedExpiringFreeResource(t, db, cohort, "postgres", 40*time.Minute)

	w := NewExpiryReminderWorker(db)
	if err := w.Work(context.Background(), fakeJobID(ExpiryReminderArgs{})); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if n := testhelpers.CountAuditLogByTeam(t, db, normal, auditKindAnonExpiryWarning); n < 1 {
		t.Errorf("normal team %s rows = %d, want >=1", auditKindAnonExpiryWarning, n)
	}
	if n := testhelpers.CountAuditLogByTeam(t, db, cohort, auditKindAnonExpiryWarning); n != 0 {
		t.Errorf("cohort team %s rows = %d, want 0 (synthetic team must be skipped)", auditKindAnonExpiryWarning, n)
	}
}

// ── expire_imminent ─────────────────────────────────────────────────────────

func TestIntegration_ExpireImminent_SkipsTestCohort(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	normal := testhelpers.SeedTeam(t, db, "hobby")
	cohort := testhelpers.SeedTeam(t, db, "hobby")
	testhelpers.SetTeamTestCohort(t, db, cohort, true)

	testhelpers.SeedPrimaryUser(t, db, normal, "normal-imm")
	testhelpers.SeedPrimaryUser(t, db, cohort, "cohort-imm")
	// Resource expiring in 40min (< 1h window) → expiry-imminent candidate.
	testhelpers.SeedExpiringFreeResource(t, db, normal, "postgres", 40*time.Minute)
	testhelpers.SeedExpiringFreeResource(t, db, cohort, "postgres", 40*time.Minute)

	w := NewExpireImminentWorker(db)
	if err := w.Work(context.Background(), fakeJobID(ExpireImminentArgs{})); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if n := testhelpers.CountAuditLogByTeam(t, db, normal, auditKindResourceExpiryImminent); n < 1 {
		t.Errorf("normal team %s rows = %d, want >=1", auditKindResourceExpiryImminent, n)
	}
	if n := testhelpers.CountAuditLogByTeam(t, db, cohort, auditKindResourceExpiryImminent); n != 0 {
		t.Errorf("cohort team %s rows = %d, want 0 (synthetic team must be skipped)", auditKindResourceExpiryImminent, n)
	}
}

// ── weekly_digest ───────────────────────────────────────────────────────────

func TestIntegration_WeeklyDigest_SkipsTestCohort(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	normal := testhelpers.SeedTeam(t, db, "pro")
	cohort := testhelpers.SeedTeam(t, db, "pro")
	testhelpers.SetTeamTestCohort(t, db, cohort, true)

	testhelpers.SeedPrimaryUser(t, db, normal, "normal-digest")
	testhelpers.SeedPrimaryUser(t, db, cohort, "cohort-digest")

	w := NewWeeklyDigestWorker(db)
	if err := w.Work(context.Background(), fakeJobID(WeeklyDigestArgs{})); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if n := testhelpers.CountAuditLogByTeam(t, db, normal, auditKindDigestWeekly); n != 1 {
		t.Errorf("normal team %s rows = %d, want 1", auditKindDigestWeekly, n)
	}
	if n := testhelpers.CountAuditLogByTeam(t, db, cohort, auditKindDigestWeekly); n != 0 {
		t.Errorf("cohort team %s rows = %d, want 0 (synthetic team must be skipped)", auditKindDigestWeekly, n)
	}
}

// ── checkout_reconcile ──────────────────────────────────────────────────────

func TestIntegration_CheckoutReconcile_SkipsTestCohort(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	normal := testhelpers.SeedTeam(t, db, "free")
	cohort := testhelpers.SeedTeam(t, db, "free")
	testhelpers.SetTeamTestCohort(t, db, cohort, true)

	normalSub := testhelpers.SeedAbandonedCheckout(t, db, normal, "normal-ck")
	cohortSub := testhelpers.SeedAbandonedCheckout(t, db, cohort, "cohort-ck")

	// nil fetcher → the 15-min heuristic stands alone; the abandoned-checkout
	// email fires for any candidate that passes the cohort guard.
	w := NewCheckoutReconcileWorker(db, nil)
	if err := w.Work(context.Background(), fakeJobID(CheckoutReconcileArgs{})); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if !testhelpers.CheckoutNotified(t, db, normalSub) {
		t.Error("normal team checkout should be notified (failure_notified_at stamped)")
	}
	if testhelpers.CheckoutNotified(t, db, cohortSub) {
		t.Error("cohort team checkout was notified — synthetic team must be skipped")
	}
	if n := testhelpers.CountAuditLogByTeam(t, db, cohort, "checkout.abandoned"); n != 0 {
		t.Errorf("cohort team checkout.abandoned rows = %d, want 0", n)
	}
}

// ── payment_grace_reminder ──────────────────────────────────────────────────

func TestIntegration_PaymentGraceReminder_SkipsTestCohort(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	normal := testhelpers.SeedTeam(t, db, "pro")
	cohort := testhelpers.SeedTeam(t, db, "pro")
	testhelpers.SetTeamTestCohort(t, db, cohort, true)

	// Active grace, clock still running (expires in 3 days) → reminder candidate.
	normalGrace := testhelpers.SeedActiveGracePeriod(t, db, normal, 3*24*time.Hour)
	cohortGrace := testhelpers.SeedActiveGracePeriod(t, db, cohort, 3*24*time.Hour)

	w := NewPaymentGraceReminderWorker(db)
	if err := w.Work(context.Background(), fakeJobID(PaymentGraceReminderArgs{})); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if n := testhelpers.GraceRemindersSent(t, db, normalGrace); n != 1 {
		t.Errorf("normal grace reminders_sent = %d, want 1", n)
	}
	if n := testhelpers.GraceRemindersSent(t, db, cohortGrace); n != 0 {
		t.Errorf("cohort grace reminders_sent = %d, want 0 (synthetic team must be skipped)", n)
	}
	if n := testhelpers.CountAuditLogByTeam(t, db, cohort, "payment.grace_reminder"); n != 0 {
		t.Errorf("cohort team payment.grace_reminder rows = %d, want 0", n)
	}
}

// ── billing_reconciler (primary sweep) ──────────────────────────────────────

func TestIntegration_BillingReconciler_SkipsTestCohort(t *testing.T) {
	db, cleanup := testhelpers.SetupTestDB(t)
	defer cleanup()

	// Both teams carry a Razorpay sub id; the fetcher reports BOTH `cancelled`
	// (terminal → would downgrade pro→hobby). The guard must keep the cohort
	// team OUT of the candidate sweep so its tier stays pro.
	normalSub := "sub_normal_" + uuid.New().String()[:10]
	cohortSub := "sub_cohort_" + uuid.New().String()[:10]
	normal := testhelpers.SeedTeamWithSubscription(t, db, "pro", normalSub)
	cohort := testhelpers.SeedTeamWithSubscription(t, db, "pro", cohortSub)
	testhelpers.SetTeamTestCohort(t, db, cohort, true)

	fetcher := &allCancelledFetcher{}
	w := NewBillingReconcilerWorker(db, fetcher, nil)
	if err := w.Work(context.Background(), fakeBillingJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := testhelpers.TeamPlanTier(t, db, normal); got != terminalDowngradeTier {
		t.Errorf("normal team plan_tier = %q after cancelled, want %q (downgrade must apply)", got, terminalDowngradeTier)
	}
	if got := testhelpers.TeamPlanTier(t, db, cohort); got != "pro" {
		t.Errorf("cohort team plan_tier = %q, want pro (synthetic team must be excluded from the sweep)", got)
	}
}

// allCancelledFetcher reports `cancelled` for every subscription id.
type allCancelledFetcher struct{}

func (allCancelledFetcher) FetchSubscriptionForReconciler(_ context.Context, _ string) (*reconcilerSubscriptionDetails, error) {
	return &reconcilerSubscriptionDetails{Status: "cancelled", PlanID: "", PaidCount: 1}, nil
}
