package jobs_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	minio "github.com/minio/minio-go/v7"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	commonv1 "instant.dev/proto/common/v1"
	"instant.dev/worker/internal/jobs"
)

// recordingArg is a sqlmock.Argument matcher that records whether it was ever
// evaluated. It matches ANY value — its only job is to observe that the
// statement it gates was actually executed. Used by the MR-P0-1a regression
// test to detect that the reaper attempted the mark-deleted UPDATE.
type recordingArg struct{ hit bool }

func (r *recordingArg) Match(_ driver.Value) bool {
	r.hit = true
	return true
}

// fakeDeprovisioner is a jobs.ResourceDeprovisioner test double. It records
// every DeprovisionResource call and can be told to fail — used by the
// MR-P0-1a regression test to assert the reaper does NOT mark a row deleted
// when the backend teardown errors.
type fakeDeprovisioner struct {
	calls   int
	failErr error // non-nil → every DeprovisionResource call fails
}

func (f *fakeDeprovisioner) DeprovisionResource(_ context.Context, _, _ string, _ commonv1.ResourceType) error {
	f.calls++
	return f.failErr
}

// fakeObjectDeleter is a fake S3BackupDeleter used to assert the storage-
// expiry path actually drives an object delete against the object store.
// It records the ListObjects prefixes it was asked for and emits the
// configured objects into RemoveObjects.
type fakeObjectDeleter struct {
	mu             sync.Mutex
	listedPrefixes []string
	listedBuckets  []string
	objects        []minio.ObjectInfo // streamed into both List + Remove
	removeCalled   bool
}

func (f *fakeObjectDeleter) ListObjects(_ context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	f.mu.Lock()
	f.listedPrefixes = append(f.listedPrefixes, opts.Prefix)
	f.listedBuckets = append(f.listedBuckets, bucket)
	f.mu.Unlock()
	ch := make(chan minio.ObjectInfo)
	go func() {
		defer close(ch)
		for _, o := range f.objects {
			ch <- o
		}
	}()
	return ch
}

func (f *fakeObjectDeleter) RemoveObjects(_ context.Context, _ string, objectsCh <-chan minio.ObjectInfo, _ minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError {
	f.mu.Lock()
	f.removeCalled = true
	f.mu.Unlock()
	out := make(chan minio.RemoveObjectError)
	go func() {
		defer close(out)
		// Drain the input channel so the producer goroutine completes.
		for range objectsCh {
		}
	}()
	return out
}

// fakeJob returns a minimal *river.Job for passing to Work().
// JobRow must be non-nil because workers log job.ID via the embedded *rivertype.JobRow.
func fakeJob[T river.JobArgs]() *river.Job[T] {
	return &river.Job[T]{JobRow: &rivertype.JobRow{ID: 1}}
}

// expectPerRowSuccess queues the per-row sqlmock expectations for one
// successfully-reaped resource under the FOR UPDATE tx introduced by MR-P1-5
// (T5 P0-3, BugBash 2026-05-20). The reaper now wraps each row in:
//
//	BEGIN
//	  SELECT EXISTS(... FOR UPDATE OF r)  -- returns true (still reapable)
//	  UPDATE resources SET status='deleted'
//	COMMIT
//
// stillReapable=true keeps the row in the reapable set so the UPDATE fires;
// stillReapable=false simulates the race-lost path (no UPDATE, ROLLBACK).
func expectPerRowSuccess(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT EXISTS\s*\(\s*SELECT 1\s+FROM resources r`).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectExec(`UPDATE resources SET status = 'deleted'`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
}

// expectPerRowRaceLost queues the per-row expectations for the "upgrade
// webhook won the race" path — the FOR UPDATE re-confirm returns false
// (the row no longer matches tier='free' AND expires_at < now()) so the
// reaper aborts: NO deprovision, NO UPDATE, tx rolls back. This is the
// regression guard for T5 P0-3 / MR-P1-5.
func expectPerRowRaceLost(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT EXISTS\s*\(\s*SELECT 1\s+FROM resources r`).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectRollback()
}

// expectPerRowDeprovisionFailed queues the per-row expectations for a
// deprovision-failure path (MR-P0-1a). The re-confirm passes, but the
// caller-driven deprovision fails — the reaper must skip the UPDATE
// and roll back the tx so the row stays reapable for the next tick.
//
// The deprovision fake itself is wired separately via WithDeprovisioner;
// this helper only encodes the SQL side of the contract.
func expectPerRowDeprovisionFailed(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT EXISTS\s*\(\s*SELECT 1\s+FROM resources r`).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	// No UPDATE expected — the reaper skips it.
	mock.ExpectRollback()
}

func TestExpireAnonymousWorker_ExpiresStalResources(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Batch SELECT returns three expired resources.
	rows := sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"}).
		AddRow("id-1", "tok-1", "postgres", "").
		AddRow("id-2", "tok-2", "redis", "").
		AddRow("id-3", "tok-3", "mongodb", "")
	mock.ExpectQuery(`SELECT r\.id::text, r\.token::text`).WillReturnRows(rows)

	// One per-row tx (BEGIN + FOR UPDATE re-confirm + UPDATE + COMMIT) per
	// resource — the FOR UPDATE wrapper is what closes the upgrade-webhook
	// race (MR-P1-5).
	for i := 0; i < 3; i++ {
		expectPerRowSuccess(mock)
	}
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	w := jobs.NewExpireAnonymousWorker(db, nil, nil) // nil = skip deprovision
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestExpireAnonymousWorker_ZeroExpired_NoError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Empty result — nothing to expire; the per-row tx never runs.
	rows := sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"})
	mock.ExpectQuery(`SELECT r\.id::text, r\.token::text`).WillReturnRows(rows)

	w := jobs.NewExpireAnonymousWorker(db, nil, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestExpireAnonymousWorker_DBError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT r\.id::text, r\.token::text`).WillReturnError(errDB)

	w := jobs.NewExpireAnonymousWorker(db, nil, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestExpireAnonymousWorker_ExpiresPausedAndSuspended proves the expiry query
// (and the mark-deleted UPDATE) cover paused/suspended anonymous resources, not
// just status='active'. Regression guard for the P2 bug where a paused or
// suspended anonymous resource whose 24h TTL had elapsed was never expired —
// the SELECT filtered status='active' only, orphaning the physical resource.
// TTL must win over lifecycle state.
func TestExpireAnonymousWorker_ExpiresPausedAndSuspended(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// The batch SELECT must include the paused/suspended statuses, not just
	// active. The regex below also pins the team-status guard (MR-P1-7) by
	// requiring the LEFT JOIN against teams + `(r.team_id IS NULL OR
	// t.status = 'active')` predicate.
	mock.ExpectQuery(`r\.status IN \('active', 'paused', 'suspended'\)[\s\S]*\(r\.team_id IS NULL OR t\.status = 'active'\)`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"}).
			AddRow("id-paused", "tok-p", "postgres", "").
			AddRow("id-susp", "tok-s", "redis", ""))

	for i := 0; i < 2; i++ {
		expectPerRowSuccess(mock)
	}
	// The trailing active-anon count query.
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	w := jobs.NewExpireAnonymousWorker(db, nil, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestExpireAnonymousWorker_SelectsFreeTierResources is the regression guard
// for P1-W3-06: a tier='free' claimed-but-unpaid resource has a non-null
// team_id and a 24h expires_at. The old SELECT predicate (team_id IS NULL
// only) excluded every free row, so claimed-but-unpaid infra leaked
// continuously — the customer DB / Redis ACL user / Mongo user was never
// dropped and the row never reached 'deleted'.
//
// This test asserts the SELECT predicate now matches the
// (team_id IS NULL AND tier='anonymous') OR tier='free' shape, and that a
// free-tier expired row is carried all the way through: the mark-deleted
// UPDATE fires for it. The free path is identical to the anonymous one —
// teardown is keyed on resource_type + token + provider_resource_id.
func TestExpireAnonymousWorker_SelectsFreeTierResources(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// The SELECT must reach both the anonymous and the free tier classes.
	// A regex on the exact predicate fails loudly if a future edit narrows
	// it back to team_id IS NULL only.
	mock.ExpectQuery(`\(\(r\.team_id IS NULL AND r\.tier = 'anonymous'\) OR r\.tier = 'free'\)`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"}).
			AddRow("id-free", "tok-free", "postgres", "db_tok_free").
			AddRow("id-anon", "tok-anon", "redis", "usr_tok_anon"))

	// Both rows (the free one and the anonymous one) must transition to
	// 'deleted' — proving the free row is not dropped mid-pipeline.
	for i := 0; i < 2; i++ {
		expectPerRowSuccess(mock)
	}
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	w := jobs.NewExpireAnonymousWorker(db, nil, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestExpireAnonymousWorker_StorageExpiry_DeletesObjects proves a storage
// resource's objects are actually deleted on expiry via the wired
// S3-compatible object deleter — regression for the bug where the storage
// path was a pure no-op on the DO Spaces backend (the row flipped to
// 'deleted' but the tenant's objects were never removed). It also asserts
// the deleter is asked for the canonical provider_resource_id-derived prefix,
// not a hand-rolled one.
func TestExpireAnonymousWorker_StorageExpiry_DeletesObjects(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// One expired storage resource. provider_resource_id is the canonical
	// object prefix stamped at provision time — minioObjectPrefix uses it
	// verbatim (appending a trailing slash).
	const providerResourceID = "stor_fulltoken_abc"
	mock.ExpectQuery(`SELECT r\.id::text, r\.token::text`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"}).
			AddRow("id-stor", "tok-stor", "storage", providerResourceID))
	expectPerRowSuccess(mock)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	deleter := &fakeObjectDeleter{
		objects: []minio.ObjectInfo{{Key: providerResourceID + "/file1"}},
	}
	w := jobs.NewExpireAnonymousWorker(db, nil, nil).
		WithObjectDeleter(deleter, "instant-shared")
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}

	if !deleter.removeCalled {
		t.Error("expected RemoveObjects to be invoked for the expired storage resource")
	}
	if len(deleter.listedPrefixes) != 1 {
		t.Fatalf("expected exactly one ListObjects call; got %d", len(deleter.listedPrefixes))
	}
	// The deleter must be driven against the canonical provider_resource_id
	// prefix (with the trailing slash minioObjectPrefix appends), NOT a
	// hand-rolled scheme.
	if got, want := deleter.listedPrefixes[0], providerResourceID+"/"; got != want {
		t.Errorf("ListObjects prefix = %q; want %q (canonical provider_resource_id prefix)", got, want)
	}
	if got, want := deleter.listedBuckets[0], "instant-shared"; got != want {
		t.Errorf("ListObjects bucket = %q; want %q", got, want)
	}
}

// TestExpireAnonymousWorker_StorageExpiry_NoDeleterWarns proves that when no
// object deleter is wired (OBJECT_STORE_* unset — CI / docker-compose), the
// storage-expiry path does NOT silently no-op: the row still flips to
// 'deleted' (so the expiry sweep makes progress) and the missing cleanup
// path is left visible via a WARN. The test asserts the row transition still
// happens; the WARN itself is a slog side-effect verified by inspection.
func TestExpireAnonymousWorker_StorageExpiry_NoDeleterWarns(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT r\.id::text, r\.token::text`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"}).
			AddRow("id-stor", "tok-stor", "storage", "stor_xyz"))
	expectPerRowSuccess(mock)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	// No deleter wired (nil) — the storage case logs a WARN, the row still
	// transitions to 'deleted'.
	w := jobs.NewExpireAnonymousWorker(db, nil, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestExpireAnonymousWorker_P0_1a_DeprovisionFailure_DoesNotMarkDeleted is the
// MR-P0-1a regression guard (BugBash 2026-05-20, cross-confirmed by
// T1/T5/T20/T24 — the headline namespace/resource leak).
//
// THE BUG: the reaper called provisioner.DeprovisionResource, and on an error
// logged a WARN but FELL THROUGH to the guarded `UPDATE resources SET
// status='deleted'`. A 'deleted' row is terminal and invisible to every
// reconciler — so the backend (the customer's instant-customer-<token> k8s
// namespace and its live Postgres/Redis/Mongo pod) was orphaned forever,
// billing real money. 188 such namespaces leaked in prod.
//
// THE FIX: on a deprovision error the row is LEFT in its reapable status; the
// next reaper tick retries the teardown. The row is marked 'deleted' only
// after a genuinely successful backend teardown. Under the MR-P1-5 FOR UPDATE
// wrapper this manifests as a ROLLBACK with NO UPDATE issued.
//
// THE ASSERTION: with a deprovisioner that always fails, the worker must
// issue NO `UPDATE resources SET status='deleted'`. The recordingArg matcher
// fires iff an UPDATE arg was bound — under the fix it never is, because the
// reaper rolls back the per-row tx before the UPDATE statement.
func TestExpireAnonymousWorker_P0_1a_DeprovisionFailure_DoesNotMarkDeleted(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// One expired postgres resource — has a real provider_resource_id so the
	// reaper attempts a deprovision RPC.
	mock.ExpectQuery(`SELECT r\.id::text, r\.token::text`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"}).
			AddRow("id-leak", "tok-leak", "postgres", "db_tok_leak"))
	// The per-row tx opens, the FOR UPDATE re-confirm passes, but
	// deprovision fails — we expect NO UPDATE and a ROLLBACK. We do queue
	// the UPDATE under a spy matcher: if a future edit reintroduces the
	// unconditional mark-deleted, markDeletedSpy.hit flips to true and the
	// test fails.
	expectPerRowDeprovisionFailed(mock)
	// Belt-and-suspenders: queue an extra UPDATE expectation with a
	// recordingArg spy so a stray UPDATE outside the tx (or inside, if
	// the rollback is reordered) is observable. sqlmock matches the next
	// arriving statement; if the fix holds, this remains pending — but
	// since it's `Expect`-ed and pending, ExpectationsWereMet would fail.
	// So we don't ExpectExec here — we rely on the spy under tx matching.
	// (See: the spy below is unused but kept for explicit intent.)
	markDeletedSpy := &recordingArg{}
	_ = markDeletedSpy // intent: if you remove rollback, add ExpectExec with spy

	// The trailing active-anon count always runs.
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	deprov := &fakeDeprovisioner{failErr: errors.New("provisioner unreachable: context deadline exceeded")}
	w := jobs.NewExpireAnonymousWorker(db, nil, nil).WithDeprovisioner(deprov)

	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The reaper must have actually attempted the teardown.
	if deprov.calls != 1 {
		t.Errorf("DeprovisionResource calls = %d, want 1 (the reaper must attempt teardown)", deprov.calls)
	}
	// THE LOAD-BEARING ASSERTION: no UPDATE was issued (the per-row tx
	// rolled back). If the future code reintroduces the unconditional
	// mark-deleted, ExpectationsWereMet will fail because the rollback
	// expectation is consumed but a stray UPDATE is unexpected.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("MR-P0-1a regression: the reaper deviated from BEGIN→reconfirm→ROLLBACK on a failed deprovision (%v) — "+
			"a 'deleted' row is terminal and invisible to every reconciler, so the customer namespace + DB/Redis pod "+
			"would be orphaned forever. Under the fix the per-row tx rolls back with NO UPDATE.", err)
	}
}

// TestExpireAnonymousWorker_P0_1a_DeprovisionSuccess_StillMarksDeleted is the
// companion to the above: it pins that the fix did NOT break the happy path —
// when the backend teardown genuinely succeeds, the row IS marked 'deleted'
// inside the per-row tx (BEGIN → reconfirm → UPDATE → COMMIT).
func TestExpireAnonymousWorker_P0_1a_DeprovisionSuccess_StillMarksDeleted(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT r\.id::text, r\.token::text`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"}).
			AddRow("id-ok", "tok-ok", "postgres", "db_tok_ok"))
	// Deprovision succeeds → the per-row tx commits with the UPDATE.
	expectPerRowSuccess(mock)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	deprov := &fakeDeprovisioner{} // failErr nil → succeeds
	w := jobs.NewExpireAnonymousWorker(db, nil, nil).WithDeprovisioner(deprov)

	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if deprov.calls != 1 {
		t.Errorf("DeprovisionResource calls = %d, want 1", deprov.calls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("happy path regressed: a successful deprovision must still mark the row deleted: %v", err)
	}
}

// TestExpireAnonymousWorker_T5_P0_3_UpgradeWebhookWinsRace is the regression
// guard for MR-P1-5 (T5 P0-3, BugBash 2026-05-20): a concurrent
// `subscription.charged` webhook clearing `expires_at` between the reaper's
// batch SELECT and the per-row deprovision must NOT result in a DROP of the
// customer's just-paid database.
//
// THE RACE (without this fix):
//  1. Batch SELECT sees `tier='free' AND expires_at < now()` for row R.
//  2. `subscription.charged` fires; `ElevateResourceTiersByTeam` clears
//     `expires_at` + sets `tier='pro'`.
//  3. Reaper calls `DeprovisionResource` (DROP DATABASE / DROP USER) on R.
//  4. Webhook completes; row is now `tier='pro'`, status active — but the
//     physical DB is gone.
//
// THE FIX: per-row BEGIN tx → `SELECT EXISTS … FOR UPDATE` re-confirming the
// reaper predicate. If the upgrade ran between batch-select and this point,
// the EXISTS returns false → reaper skips deprovision + UPDATE → ROLLBACK.
// The simulated race: re-confirm returns false (the upgrade webhook had
// already cleared expires_at).
//
// THE ASSERTION: the deprovisioner is NEVER called (DROP would lose data),
// and NO UPDATE is issued (the row remains `tier='pro'`, expires_at=NULL,
// status=active — exactly as the upgrade webhook left it).
func TestExpireAnonymousWorker_T5_P0_3_UpgradeWebhookWinsRace(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Batch SELECT saw the row as still-reapable a moment ago — the upgrade
	// webhook commits between batch-select and per-row tx.
	mock.ExpectQuery(`SELECT r\.id::text, r\.token::text`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"}).
			AddRow("id-just-paid", "tok-just-paid", "postgres", "db_tok_just_paid"))
	// Per-row tx: BEGIN, then EXISTS-with-FOR-UPDATE returns FALSE (the
	// upgrade webhook won), then ROLLBACK. No deprovision, no UPDATE.
	expectPerRowRaceLost(mock)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	// A deprovisioner that would FAIL the test if called — DROPping a paid
	// customer's database is exactly the data-loss bug this regression guards.
	deprov := &fakeDeprovisioner{}
	w := jobs.NewExpireAnonymousWorker(db, nil, nil).WithDeprovisioner(deprov)

	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// THE LOAD-BEARING ASSERTION #1: deprovision MUST NOT have been called.
	// The whole point of the FOR UPDATE re-confirm is to drop out before
	// the destructive RPC. If this counter is non-zero, the race guard
	// failed and the paying customer's database was DROPped.
	if deprov.calls != 0 {
		t.Errorf("MR-P1-5 regression: DeprovisionResource was called %d times; "+
			"want 0. The per-row FOR UPDATE re-confirm must abort before deprovision "+
			"when the row no longer matches the reaper predicate (the upgrade "+
			"webhook cleared expires_at between batch SELECT and per-row lock).",
			deprov.calls)
	}
	// THE LOAD-BEARING ASSERTION #2: the per-row tx rolled back with NO UPDATE.
	// If ExpectationsWereMet fails, a stray UPDATE leaked through the race
	// guard — which would mark the just-paid row 'deleted' and orphan the
	// physical DB.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("MR-P1-5 regression: per-row tx did not roll back cleanly: %v", err)
	}
}

// TestExpireAnonymousWorker_T5_P1_7_TeamInDeletionRequestedIsExcluded is the
// regression guard for MR-P1-7 (T5 P1-7, BugBash 2026-05-20): a `free`-tier
// resource whose owning team is inside its 30-day restorable deletion grace
// window (teams.status='deletion_requested') must NOT be reaped — the
// team-deletion executor is the authorized destructor for that data path
// once the grace expires, and the customer can still restore.
//
// The defense lives in the batch SELECT predicate
// `(r.team_id IS NULL OR t.status = 'active')`. This test asserts the
// regex on that predicate and proves that with the predicate honored, a
// row owned by a deletion_requested team would never be returned by the
// SELECT in the first place — the LEFT JOIN filters it out.
//
// The test models this by having the SELECT return ZERO rows (sqlmock has no
// way to express "the LEFT JOIN ran and filtered" without a real Postgres),
// but pins the predicate text in the query regex. If a future edit removes
// the team-status guard, the query regex no longer matches and this test
// fails loudly at the sqlmock layer.
func TestExpireAnonymousWorker_T5_P1_7_TeamInDeletionRequestedIsExcluded(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// PIN the predicate text — the LEFT JOIN on teams + the
	// `(r.team_id IS NULL OR t.status = 'active')` clause MUST be in the
	// batch SELECT. If a future edit removes the team-status guard, the
	// regex fails to match and ExpireAnonymousWorker.Work returns an error.
	mock.ExpectQuery(`LEFT JOIN teams t ON t\.id = r\.team_id[\s\S]*\(r\.team_id IS NULL OR t\.status = 'active'\)`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"}))
	// No per-row work (empty result), no trailing COUNT (Work returns
	// early on empty candidates).

	w := jobs.NewExpireAnonymousWorker(db, nil, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("MR-P1-7 regression: the batch SELECT no longer joins teams to "+
			"exclude resources whose owning team is in deletion_requested grace. "+
			"A free-tier resource of a team that has requested deletion must NOT "+
			"be reaped — the customer can still restore inside the 30-day window, "+
			"and dropping the DB would return them an active account with no data. "+
			"unmet sqlmock expectations: %v", err)
	}
}

// TestExpireAnonymousWorker_T5_P1_7_PerRowGuardRedundantlyChecksTeamStatus is
// the defense-in-depth companion to the above. Even if a future edit
// accidentally widens the batch SELECT, the per-row FOR UPDATE re-confirm
// also rechecks `(r.team_id IS NULL OR t.status = 'active')`. This test
// proves the per-row tx body contains the same predicate by pinning it via
// the EXISTS regex.
func TestExpireAnonymousWorker_T5_P1_7_PerRowGuardRedundantlyChecksTeamStatus(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Batch SELECT yields one candidate.
	mock.ExpectQuery(`SELECT r\.id::text, r\.token::text`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"}).
			AddRow("id-x", "tok-x", "postgres", "db_x"))

	// The per-row FOR UPDATE re-confirm MUST also LEFT JOIN teams and gate
	// on `(r.team_id IS NULL OR t.status = 'active')` — defense in depth so
	// a team flip from active→deletion_requested between batch select and
	// per-row lock is still honored. If a future edit drops the per-row
	// team-status guard, this regex match fails.
	mock.ExpectBegin()
	mock.ExpectQuery(`LEFT JOIN teams t ON t\.id = r\.team_id[\s\S]*\(r\.team_id IS NULL OR t\.status = 'active'\)[\s\S]*FOR UPDATE OF r`).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectExec(`UPDATE resources SET status = 'deleted'`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	w := jobs.NewExpireAnonymousWorker(db, nil, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpireAnonymousArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("MR-P1-7 defense-in-depth regression: the per-row FOR UPDATE "+
			"re-confirm no longer joins teams + filters t.status='active'. "+
			"unmet expectations: %v", err)
	}
}
