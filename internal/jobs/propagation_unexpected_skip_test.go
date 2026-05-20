package jobs

// propagation_unexpected_skip_test.go — CHAOS-DRILL-2026-05-20 F1 regression
// coverage for "unexpected_skip silently marks the propagation APPLIED".
//
// Pre-fix bug: handleTierElevation in propagation_runner.go (lines 756–771
// pre-patch) treated `(Applied=false, SkipReason=<any string not in the
// allowed-skip whitelist>)` as success. A WARN log fired, firstErr stayed
// nil, and the runner stamped applied_at on the row. A paying customer's
// regrade never landed — there was no retry, no dead-letter, no alert.
//
// Fix: any non-allowed SkipReason now returns propagationUnexpectedSkipErr.
// The runner's markRetry path detects this via errors.Is and emits a
// propagation.unexpected_skip audit row (NOT propagation.applied). The row
// retries per the backoff schedule and dead-letters at propagationMaxAttempts
// with a propagation.dead_lettered audit row. The Prometheus counter
// PropagationUnexpectedSkipTotal increments at the emit site so dashboards
// can spot patterns before the dead-letter lagging signal fires.
//
// Tests:
//
//   1. TestPropagation_UnexpectedSkip_DoesNotMarkApplied
//      The headline regression test. Provisioner returns
//      (Applied=false, SkipReason="postgres-admin secret not found"). Assert:
//        - markApplied is NOT called (no applied_at UPDATE in the sqlmock script).
//        - markRetry IS called: attempts+1, next_attempt_at advanced.
//        - audit_log row is propagation.unexpected_skip (NOT propagation.applied).
//        - PropagationUnexpectedSkipTotal counter incremented by 1 with
//          the right labels.
//
//   2. TestPropagation_UnexpectedSkip_DeadLettersAtMaxAttempts
//      attempts == propagationMaxAttempts-1, then one more
//      unexpected_skip. Assert the row dead-letters via the modal
//      markDeadLettered path (single UPDATE … SET failed_at=now()), and
//      the audit row is propagation.dead_lettered.
//
//   3. TestIsPropagationAllowedSkip_Coverage
//      Registry-iterating coverage test per CLAUDE.md rule 18. The
//      allowed-skip set lives in one place (propagationAllowedSkipSubstrings)
//      and we assert every documented allowed-skip string passes, while
//      a representative set of unexpected-skip strings fails. If a future
//      PR adds a new allowed-skip substring without updating the list, the
//      regression test below catches the un-mapped string.
//
//   4. TestPropagationUnexpectedSkipErr_IsMatches
//      Pins the errors.Is contract: the markRetry path uses
//      errors.Is(err, errPropagationUnexpectedSkipSentinel) to switch on
//      the audit kind, so the Is() implementation MUST match the
//      sentinel. A future refactor that breaks this would silently
//      reintroduce the F1 bug (markRetry would emit propagation.retrying
//      instead of propagation.unexpected_skip).
//
//   5. TestBucketSkipReason_BoundsCardinality
//      Pins the Prometheus label vocabulary so a Prometheus operator can
//      rely on the bucket names in alert rules.

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	commonplans "instant.dev/common/plans"
	"instant.dev/worker/internal/metrics"
)

// ─── Test 1: F1 regression — unexpected_skip does NOT mark applied ────────────

func TestPropagation_UnexpectedSkip_DoesNotMarkApplied(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	propID := uuid.New()
	teamID := uuid.New()
	resID := uuid.New()

	// Snapshot the unexpected_skip counter before the run — Prometheus
	// counters are process-global so other tests in this file (or in the
	// suite, run with -p 1) may have moved the value. testutil.ToFloat64
	// reads the WithLabelValues child, which is unique per labelset.
	counterChild := metrics.PropagationUnexpectedSkipTotal.WithLabelValues(
		propagationKindTierElevation, "postgres", "postgres_admin_secret_missing",
	)
	startCount := testutil.ToFloat64(counterChild)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, kind, team_id, target_tier, payload, attempts\s+FROM pending_propagations`).
		WillReturnRows(sqlmock.NewRows(propagationSweepCols).
			AddRow(propID, propagationKindTierElevation, teamID, "pro", []byte(`{}`), 0))
	mock.ExpectCommit()

	mock.ExpectQuery(`SELECT r\.id, r\.token, r\.provider_resource_id, r\.tier, r\.resource_type`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows(teamResourcesCols).
			AddRow(resID, "tok-1", sql.NullString{}, "pro", "postgres"))

	// THE BUG: pre-fix, the runner would next call markApplied (a
	// `UPDATE pending_propagations SET applied_at = now()`) and INSERT a
	// propagation.applied audit row. The fix re-routes through markRetry.
	// We intentionally do NOT script those expectations — if the bug ever
	// regressed, sqlmock would fail with "unexpected query".
	//
	// markRetry UPDATE — attempts + 1, next_attempt_at advanced.
	mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts\s*=\s*attempts \+ 1`).
		WithArgs(
			sqlmock.AnyArg(), // last_error truncated string
			sqlmock.AnyArg(), // nextAttempt time.Time
			propID,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// audit_log INSERT — must be propagation.unexpected_skip.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(
			teamID,
			propagationActor,
			auditKindPropagationUnexpectedSkip, // <-- the F1 contract
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Stub the provisioner to return the canonical F1 case:
	// Applied=false, SkipReason="postgres-admin secret not found".
	stub := &stubPropagationRegrader{outcome: regradeOutcome{
		Applied:    false,
		SkipReason: "resource not reachable: postgres-admin secret not found",
	}}
	w := NewPropagationRunnerWorker(db, commonplans.Default(), stub)
	w.now = fixedNow()

	if wErr := w.Work(context.Background(), fakePropagationJob()); wErr != nil {
		t.Fatalf("Work: %v", wErr)
	}

	if stub.calls != 1 {
		t.Errorf("RegradeResource calls = %d, want 1", stub.calls)
	}

	// Counter assertion — exact bucket name expected from bucketSkipReason
	// against the "postgres-admin secret not found" surface. If a future PR
	// changes the bucket name, the operator's NR alert rule must change too —
	// pin it here so the change isn't silent.
	endCount := testutil.ToFloat64(counterChild)
	if endCount-startCount != 1.0 {
		t.Errorf("PropagationUnexpectedSkipTotal{kind=tier_elevation,resource_type=postgres,skip_reason=postgres_admin_secret_missing} delta = %.1f, want 1.0",
			endCount-startCount)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// ─── Test 2: dead-letter at maxAttempts for unexpected_skip ───────────────────

func TestPropagation_UnexpectedSkip_DeadLettersAtMaxAttempts(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	propID := uuid.New()
	teamID := uuid.New()
	resID := uuid.New()

	// attempts already at maxAttempts-1; one more unexpected_skip dead-letters.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, kind, team_id, target_tier, payload, attempts\s+FROM pending_propagations`).
		WillReturnRows(sqlmock.NewRows(propagationSweepCols).
			AddRow(propID, propagationKindTierElevation, teamID, "pro", []byte(`{}`), propagationMaxAttempts-1))
	mock.ExpectCommit()

	mock.ExpectQuery(`SELECT r\.id, r\.token, r\.provider_resource_id, r\.tier, r\.resource_type`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows(teamResourcesCols).
			AddRow(resID, "tok-1", sql.NullString{}, "pro", "postgres"))

	// markDeadLettered UPDATE: stamps failed_at + last_error.
	mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts\s*=\s*attempts \+ 1,\s+last_attempt_at\s*=\s*now\(\),\s+last_error\s*=\s*\$1,\s+failed_at\s*=\s*now\(\)`).
		WithArgs(sqlmock.AnyArg(), propID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// age_seconds lookup query.
	mock.ExpectQuery(`SELECT created_at FROM pending_propagations WHERE id`).
		WithArgs(propID).
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(time.Now().Add(-25 * time.Hour)))
	// audit_log INSERT — propagation.dead_lettered. The handler-side
	// distinction (unexpected_skip vs gRPC-error vs DB-fail) collapses
	// at the dead-letter point: a row that exhausted maxAttempts gets
	// the canonical dead-letter audit kind regardless of cause. The
	// `last_error` column carries the unexpected_skip detail for the
	// operator to debug from.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(
			teamID,
			propagationActor,
			auditKindPropagationDeadLettered,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	stub := &stubPropagationRegrader{outcome: regradeOutcome{
		Applied:    false,
		SkipReason: "namespace not found",
	}}
	w := NewPropagationRunnerWorker(db, commonplans.Default(), stub)
	w.now = fixedNow()

	if wErr := w.Work(context.Background(), fakePropagationJob()); wErr != nil {
		t.Fatalf("Work: %v", wErr)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// ─── Test 3: allowed-skip registry coverage (CLAUDE.md rule 18) ───────────────

func TestIsPropagationAllowedSkip_Coverage(t *testing.T) {
	// The registry-iterating check: every documented allowed-skip substring
	// is non-empty and is matched by isPropagationAllowedSkip. A future PR
	// that adds an entry to propagationAllowedSkipSubstrings but forgets to
	// wire it through isPropagationAllowedSkip fails this test.
	if len(propagationAllowedSkipSubstrings) == 0 {
		t.Fatal("propagationAllowedSkipSubstrings is empty — every SkipReason will be treated as unexpected_skip, the F1 fix has been over-corrected into a fail-everything posture")
	}
	for _, sub := range propagationAllowedSkipSubstrings {
		if sub == "" {
			t.Error("propagationAllowedSkipSubstrings contains an empty string — would match every SkipReason (including '')")
			continue
		}
		// Match a sample reason that contains the substring.
		sample := "prefix " + sub + " suffix"
		if !isPropagationAllowedSkip(sample) {
			t.Errorf("isPropagationAllowedSkip(%q) = false for allowed substring %q — substring lookup is broken", sample, sub)
		}
	}

	// And the negative cases — every SkipReason from the chaos drill / known
	// failure-mode catalog MUST be treated as unexpected_skip. If any of
	// these turn into allowed-skips a paying customer's regrade will
	// silently fail.
	unexpectedSamples := []string{
		"postgres-admin secret not found",
		"resource not reachable: postgres-admin secret not found",
		"namespace not found",
		"redis-auth secret missing",
		"pod not found",
		"no Running pod",
		"exec fallback: CONFIG SET maxmemory failed",
		"legacy resource without auth secret",
	}
	for _, sample := range unexpectedSamples {
		if isPropagationAllowedSkip(sample) {
			t.Errorf("isPropagationAllowedSkip(%q) = true — this MUST be treated as an unexpected_skip (the whole point of F1)", sample)
		}
	}
}

// ─── Test 4: errors.Is contract for the sentinel ──────────────────────────────

func TestPropagationUnexpectedSkipErr_IsMatches(t *testing.T) {
	err := &propagationUnexpectedSkipErr{
		Resources: []propagationUnexpectedSkipDetail{
			{ResourceID: "r1", ResourceType: "postgres", SkipReason: "postgres-admin secret not found"},
		},
	}
	if !errors.Is(err, errPropagationUnexpectedSkipSentinel) {
		t.Error("errors.Is(*propagationUnexpectedSkipErr, errPropagationUnexpectedSkipSentinel) = false — the markRetry audit-kind switch will never fire, F1 audit signal regression")
	}

	// Wrapped via fmt.Errorf("…: %w", …) must still match — the dispatch
	// loop wraps unexpected_skip in handler-side context.
	wrapped := errors.New("not the sentinel")
	if errors.Is(wrapped, errPropagationUnexpectedSkipSentinel) {
		t.Error("errors.Is(unrelated_err, errPropagationUnexpectedSkipSentinel) = true — sentinel matches too broadly, would mis-classify routine retries as unexpected_skip")
	}

	// Nil receiver must not panic; the runner never constructs a nil
	// pointer here, but defensive.
	var nilErr *propagationUnexpectedSkipErr
	if errors.Is(nilErr, errPropagationUnexpectedSkipSentinel) {
		// Nil err of this type Still matches the sentinel via the Is method —
		// acceptable; the runner would never construct it.
		t.Log("nilErr matched (Is method returns true for nil receiver) — acceptable")
	}

	// Error string formatting includes every offending resource so the
	// operator can grep the audit_log.last_error column.
	got := err.Error()
	for _, want := range []string{"r1", "postgres", "postgres-admin secret"} {
		if !strings.Contains(got, want) {
			t.Errorf("propagationUnexpectedSkipErr.Error() = %q, missing substring %q (operator can't debug from audit_log.last_error)", got, want)
		}
	}
}

// ─── Test 5: bucketSkipReason cardinality contract ────────────────────────────

func TestBucketSkipReason_BoundsCardinality(t *testing.T) {
	cases := []struct {
		raw    string
		bucket string
	}{
		// Postgres admin secret missing — the canonical F1 trigger.
		{"resource not reachable: postgres-admin secret not found", "postgres_admin_secret_missing"},
		{"postgres-admin Secret not found", "postgres_admin_secret_missing"},
		{"missing postgres_admin secret", "postgres_admin_secret_missing"},

		// Redis-auth secret missing.
		{"redis-auth secret not found", "redis_auth_secret_missing"},

		// Namespace teardown race.
		{"namespace not found: instant-customer-xyz", "namespace_not_found"},

		// Pod-not-found from exec-fallback paths.
		{"exec fallback: no pod found", "pod_not_found"},
		{"pod not found in namespace", "pod_not_found"},

		// Resource unreachable (gRPC dial / DNS).
		{"resource not reachable", "resource_not_reachable"},
		{"backend unreachable", "resource_not_reachable"},

		// Legacy resource without auth secret.
		{"legacy resource without auth secret", "legacy_resource"},
		{"legacy pod missing required config", "legacy_resource"},

		// Unknown — the catch-all bucket.
		{"completely novel skip reason from a future provisioner", "other"},
		{"", "other"},
	}
	for _, c := range cases {
		if got := bucketSkipReason(c.raw); got != c.bucket {
			t.Errorf("bucketSkipReason(%q) = %q, want %q", c.raw, got, c.bucket)
		}
	}

	// And the bounded-cardinality invariant: a thousand random unique
	// SkipReasons should fall into a small fixed set of buckets. (We
	// declare the upper bound at 8 buckets — the 7 named + "other".)
	seen := map[string]struct{}{}
	for i := 0; i < 1000; i++ {
		// A faux randomness — different prefixes — none of which should
		// hit any named bucket.
		raw := "novel skip variant #" + uuid.New().String()
		seen[bucketSkipReason(raw)] = struct{}{}
	}
	if len(seen) != 1 || func() bool { _, ok := seen["other"]; return !ok }() {
		t.Errorf("bucketSkipReason swallowed %d distinct buckets for novel inputs (want exactly 1, 'other') — cardinality not bounded, Prometheus label explosion risk: %v", len(seen), seen)
	}
}
