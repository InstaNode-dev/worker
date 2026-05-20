package jobs

// propagation_runner_integration_test.go — Track 3 of the
// reliability-integration suite (2026-05-20).
//
// Adds the next layer up from propagation_runner_test.go (sqlmock unit
// drift guards). These tests exercise the runner's persistence + dispatch
// paths against a slightly broader surface: end-to-end markRetry +
// markDeadLettered + the unknown_kind bounded-retry path; plus live-DB
// gated tests for FOR UPDATE SKIP LOCKED concurrency and the enum ↔
// handler-map registry walk.
//
//   1. TestPropagation_BackoffIntegration_ExactScheduleViaMarkRetry —
//      drives w.markRetry directly with a mock clock and asserts the
//      persisted next_attempt_at SQL UPDATE arg matches
//      propagationBackoffSchedule[attempts] for every position +
//      clamp behaviour. The existing
//      TestPropagationBackoffSchedule_IsMonotonicAndClamps pins the
//      propagationBackoffFor helper directly; THIS test pins the
//      end-to-end markRetry persistence path so a regression in
//      markRetry that doesn't pass through propagationBackoffFor
//      (e.g. a constant-30s retry refactor) is also caught.
//
//   2. TestPropagation_DeadLetterIntegration_AtMaxAttempts — drives
//      w.markDeadLettered directly and asserts the SQL update sets
//      failed_at AND the propagation.dead_lettered audit row is
//      emitted.
//
//   3. TestPropagation_UnknownKindIntegration_BoundedRetries — F2 P1
//      guard: insert a pending_propagation with kind='garbage', runner
//      doesn't dispatch (no handler), markRetry fires (attempts++).
//      The bounded-retry guarantee comes from attempts++ hitting
//      maxAttempts — a future refactor that bypassed the increment
//      would loop forever.
//
//   4. TestPropagation_ForUpdateSkipLockedIntegration — two concurrent
//      runners against a REAL Postgres (gated on TEST_DATABASE_URL).
//      Verifies at most one runner picks the row.
//
//   5. TestPropagation_RegistryWalkIntegration_EnumVsHandlerMap — the
//      rule 18 registry test against the actual PG enum. The unit
//      tests cover the slice; this covers the DB-side enum.
//
// COVERAGE BLOCKS per CLAUDE.md rule 17 — see per-test docstrings.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	commonv1 "instant.dev/proto/common/v1"

	commonplans "instant.dev/common/plans"

	_ "github.com/lib/pq"
)

// ─── Test 1: backoff exact schedule via markRetry ─────────────────────────────

// TestPropagation_BackoffIntegration_ExactScheduleViaMarkRetry drives
// markRetry directly with a fixed clock and pins the next_attempt_at
// UPDATE arg for every position in the schedule + the clamp.
//
// COVERAGE BLOCK (rule 17):
//   Symptom:       a PR rewrites markRetry to use a different formula
//                  (e.g. constant 30s retries) without updating the
//                  schedule — silently bumps retry rate and pages NR
//                  with retry storm noise during a real outage.
//   Enumeration:   `rg -F 'propagationBackoffFor\|propagationBackoffSchedule' worker/`
//   Sites found:   2 (the schedule slice + the function).
//   Sites touched: 2 (this test pins both via the SQL UPDATE arg).
//   Coverage test: a divergence between markRetry's UPDATE arg and
//                  propagationBackoffSchedule[attempts] FAILS here.
//   Live verified: worker chaos drill 2026-05-20.
func TestPropagation_BackoffIntegration_ExactScheduleViaMarkRetry(t *testing.T) {
	clock := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	for i := 0; i < len(propagationBackoffSchedule); i++ {
		i := i
		t.Run(fmt.Sprintf("attempts=%d", i), func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()

			propID := uuid.New()
			teamID := uuid.New()

			expectedDelay := propagationBackoffFor(i)
			expectedNext := clock.Add(expectedDelay)

			mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts`).
				WithArgs(sqlmock.AnyArg(), expectedNext, propID).
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(`INSERT INTO audit_log`).
				WillReturnResult(sqlmock.NewResult(0, 1))

			w := NewPropagationRunnerWorker(db, commonplans.Default(), &stubPropagationRegrader{})
			w.now = func() time.Time { return clock }

			row := propagationRow{
				id:         propID,
				kind:       propagationKindTierElevation,
				teamID:     teamID,
				targetTier: sql.NullString{String: "pro", Valid: true},
				attempts:   i,
			}
			w.markRetry(context.Background(), row, errors.New("synthetic failure"))

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("attempts=%d: unmet sqlmock expectations: %v", i, err)
			}
		})
	}

	// Clamp: attempts beyond len(schedule)-1 stays at the final entry.
	t.Run("attempts_beyond_schedule_clamps", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		propID := uuid.New()
		teamID := uuid.New()
		finalDelay := propagationBackoffSchedule[len(propagationBackoffSchedule)-1]
		expectedNext := clock.Add(finalDelay)
		mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts`).
			WithArgs(sqlmock.AnyArg(), expectedNext, propID).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(`INSERT INTO audit_log`).
			WillReturnResult(sqlmock.NewResult(0, 1))

		w := NewPropagationRunnerWorker(db, commonplans.Default(), &stubPropagationRegrader{})
		w.now = func() time.Time { return clock }
		row := propagationRow{
			id:       propID,
			kind:     propagationKindTierElevation,
			teamID:   teamID,
			attempts: len(propagationBackoffSchedule) + 5,
		}
		w.markRetry(context.Background(), row, errors.New("late"))
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("clamp arm: unmet sqlmock expectations: %v", err)
		}
	})
}

// ─── Test 2: dead-letter integration ──────────────────────────────────────────

// TestPropagation_DeadLetterIntegration_AtMaxAttempts drives
// w.markDeadLettered directly. Pins the SQL UPDATE setting failed_at
// + the audit_log INSERT emitting the propagation.dead_lettered kind.
// The slog ERROR line is the alert-key; the audit row is the
// operator-visible ledger.
//
// COVERAGE BLOCK (rule 17):
//   Symptom:       a future refactor moves the dead_lettered audit
//                  emission conditionally → the customer's broken-tier
//                  infra stops paging NR on dead-letter.
//   Enumeration:   `rg -F 'auditKindPropagationDeadLettered' worker/`
//   Sites found:   2 (constant declaration + emission).
//   Sites touched: 1 (the emission via this test).
//   Coverage test: removing the audit insert from markDeadLettered
//                  unmets the sqlmock expectation here.
func TestPropagation_DeadLetterIntegration_AtMaxAttempts(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	propID := uuid.New()
	teamID := uuid.New()

	mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts\s*=\s*attempts \+ 1,\s+last_attempt_at\s*=\s*now\(\),\s+last_error\s*=\s*\$1,\s+failed_at\s*=\s*now\(\)`).
		WithArgs(sqlmock.AnyArg(), propID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT created_at FROM pending_propagations WHERE id`).
		WithArgs(propID).
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(time.Now().Add(-26 * time.Hour)))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := NewPropagationRunnerWorker(db, commonplans.Default(), &stubPropagationRegrader{})
	row := propagationRow{
		id:       propID,
		kind:     propagationKindTierElevation,
		teamID:   teamID,
		attempts: propagationMaxAttempts - 1,
	}
	w.markDeadLettered(context.Background(), row, errors.New("final failure"))

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// ─── Test 3: F2 P1 guard — unknown_kind bounded retries ───────────────────────

// TestPropagation_UnknownKindIntegration_BoundedRetries verifies a row
// with a kind absent from propagationHandlers is treated as a
// retryable failure (markRetry, NOT silent skip). The bounded-retry
// guarantee comes from markRetry incrementing attempts: after
// propagationMaxAttempts invocations, the row dead-letters per the
// standard path.
//
// COVERAGE BLOCK (rule 17):
//   Symptom:       F2 P1 — old-worker / new-api skew. api enqueues a
//                  new kind, worker doesn't know it → loops forever
//                  consuming worker tick capacity.
//   Enumeration:   `rg -F 'no handler registered for kind' worker/`
//   Sites found:   1 (propagation_runner.go).
//   Sites touched: 1 (this test).
//   Coverage test: removing attempts++ in the unknown_kind branch
//                  fails this test (the markRetry UPDATE is unmet).
//   Live verified: chaos drill 2026-05-20 (synthetic kind='garbage').
func TestPropagation_UnknownKindIntegration_BoundedRetries(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	propID := uuid.New()
	teamID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, kind, team_id, target_tier, payload, attempts\s+FROM pending_propagations`).
		WillReturnRows(sqlmock.NewRows(propagationSweepCols).
			AddRow(propID, "garbage_kind_nobody_handles", teamID, "pro", []byte(`{}`), 0))
	mock.ExpectCommit()

	// Expected: markRetry fires (attempts++ + audit row). On master
	// today, the unknown_kind branch routes to markRetry (NOT
	// markDeadLettered immediately) per propagation_runner.go.
	mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts\s*=\s*attempts \+ 1`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), propID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	stub := &stubPropagationRegrader{}
	w := NewPropagationRunnerWorker(db, commonplans.Default(), stub)
	if wErr := w.Work(context.Background(), fakePropagationJob()); wErr != nil {
		t.Fatalf("Work: %v", wErr)
	}

	if stub.calls != 0 {
		t.Errorf("regrader.calls = %d, want 0 — unknown_kind must NOT dispatch any provisioner RPC", stub.calls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// ─── Test 4: FOR UPDATE SKIP LOCKED concurrency ───────────────────────────────

// TestPropagation_ForUpdateSkipLockedIntegration runs two concurrent
// pickers against a real Postgres + a single eligible row, asserts at
// most one picks it up. Gated on TEST_DATABASE_URL + absence of
// -short.
//
// COVERAGE BLOCK (rule 17):
//   Symptom:       a future refactor drops FOR UPDATE SKIP LOCKED →
//                  two concurrent runner pods double-dispatch the
//                  same row → 2x noise + duplicate audit_log entries.
//   Enumeration:   `rg -F 'FOR UPDATE SKIP LOCKED' worker/`
//   Sites found:   1 (pickEligible in propagation_runner.go).
//   Sites touched: 1.
//   Coverage test: removing the SKIP LOCKED clause makes both pickers
//                  see the row → total picks = 2 → test fails.
//   Live verified: chaos drill 2026-05-20.
func TestPropagation_ForUpdateSkipLockedIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skip live-DB test under -short (regular gate)")
	}
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run the live-DB SKIP LOCKED test")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skipf("ping TEST_DATABASE_URL: %v — DB not reachable", err)
	}

	var exists bool
	if err := db.QueryRowContext(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			 WHERE table_name = 'pending_propagations'
		)
	`).Scan(&exists); err != nil {
		t.Fatalf("check table exists: %v", err)
	}
	if !exists {
		t.Skip("pending_propagations table absent — run api migrations against TEST_DATABASE_URL first")
	}

	ctx := context.Background()
	teamID := uuid.New()
	propID := uuid.New()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO pending_propagations
			(id, kind, team_id, target_tier, payload, attempts, next_attempt_at, created_at)
		VALUES ($1, $2, $3, $4, '{}'::jsonb, 0, now() - interval '1 minute', now())
	`, propID, propagationKindTierElevation, teamID, "pro"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM pending_propagations WHERE id = $1`, propID)
	})

	var (
		pickedByA, pickedByB int64
		wg                   sync.WaitGroup
	)

	wA := NewPropagationRunnerWorker(db, commonplans.Default(), &stubPropagationRegrader{})
	wB := NewPropagationRunnerWorker(db, commonplans.Default(), &stubPropagationRegrader{})

	startGate := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-startGate
		rows, err := wA.pickEligible(context.Background())
		if err != nil {
			t.Errorf("wA.pickEligible: %v", err)
			return
		}
		atomic.AddInt64(&pickedByA, int64(len(rows)))
	}()
	go func() {
		defer wg.Done()
		<-startGate
		rows, err := wB.pickEligible(context.Background())
		if err != nil {
			t.Errorf("wB.pickEligible: %v", err)
			return
		}
		atomic.AddInt64(&pickedByB, int64(len(rows)))
	}()

	close(startGate)
	wg.Wait()

	total := atomic.LoadInt64(&pickedByA) + atomic.LoadInt64(&pickedByB)
	if total > 1 {
		t.Errorf("total picks = %d, want <= 1 (FOR UPDATE SKIP LOCKED is broken)", total)
	}
	if total == 0 {
		t.Logf("total picks = 0 (both pickers raced past) — not a SKIP-LOCKED failure; what we guard against is total > 1.")
	}
}

// ─── Test 5: registry-walk against the PG enum ────────────────────────────────

// TestPropagation_RegistryWalkIntegration_EnumVsHandlerMap iterates
// the pending_propagations.kind PostgreSQL enum values + asserts
// every one has a propagationHandlers entry. The slice-based
// TestPropagationRunner_EveryKindHasAHandler covers the worker-side
// constants; THIS test covers the DB-side enum which the api
// enqueues against. Drift between the two registries IS the failure
// mode.
//
// Gated on TEST_DATABASE_URL.
//
// COVERAGE BLOCK (rule 17):
//   Symptom:       a migration adds a new value to the
//                  pending_propagations.kind enum but the worker
//                  release doesn't ship the handler. api enqueues
//                  the new kind; worker logs WARN ("no handler
//                  registered") and retry-loops until dead-letter.
//   Enumeration:   `psql -c "SELECT enum_range(NULL::propagation_kind)"`
//                  ↔ propagationHandlers map keys.
//   Sites found:   N (the enum values).
//   Sites touched: N (this test iterates ALL).
//   Coverage test: divergence between enum and handler map fails.
func TestPropagation_RegistryWalkIntegration_EnumVsHandlerMap(t *testing.T) {
	if testing.Short() {
		t.Skip("skip live-DB registry walk under -short (regular gate)")
	}
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to walk the pending_propagations.kind enum")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skipf("ping TEST_DATABASE_URL: %v", err)
	}

	var udtName sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT udt_name
		  FROM information_schema.columns
		 WHERE table_name = 'pending_propagations'
		   AND column_name = 'kind'
		 LIMIT 1
	`).Scan(&udtName); err != nil {
		t.Skipf("inspect pending_propagations.kind: %v — table may not be migrated", err)
	}
	if !udtName.Valid {
		t.Skip("pending_propagations.kind column has no udt_name — schema mismatch")
	}
	if udtName.String == "text" || udtName.String == "varchar" {
		t.Skipf("pending_propagations.kind is %s (not an enum) — rule-18 guarantee delegated to TestPropagationRunner_EveryKindHasAHandler", udtName.String)
	}

	rows, err := db.QueryContext(context.Background(),
		fmt.Sprintf(`SELECT unnest(enum_range(NULL::%s))::text`, udtName.String))
	if err != nil {
		t.Skipf("read enum %q values: %v", udtName.String, err)
	}
	defer rows.Close()
	var enumValues []string
	for rows.Next() {
		var v string
		if scanErr := rows.Scan(&v); scanErr != nil {
			t.Errorf("scan enum value: %v", scanErr)
			continue
		}
		enumValues = append(enumValues, v)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		t.Fatalf("enum rows iter: %v", rowsErr)
	}
	if len(enumValues) == 0 {
		t.Skip("enum has zero values — schema not seeded")
	}

	for _, v := range enumValues {
		if _, ok := propagationHandlers[v]; !ok {
			t.Errorf("pending_propagations.kind enum value %q has NO handler in propagationHandlers — a row of this kind enqueued by the api would loop forever on no-handler retries until it dead-letters",
				v)
		}
	}
}

// preventImportUnused stays in case ctx-based helpers are later added.
var _ = commonv1.ResourceType_RESOURCE_TYPE_POSTGRES
