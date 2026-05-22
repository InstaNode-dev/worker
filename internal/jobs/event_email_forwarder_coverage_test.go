package jobs

// event_email_forwarder_coverage_test.go — coverage-lifting tests for the
// production-backed seams (sqlSentLedger, sqlSuppressionChecker,
// redisEventCursorStore) plus the Work() error branches not exercised by
// the headline forwarder tests. Goal: drive event_email_forwarder.go
// statement coverage to ≥95%.
//
// Hermetic — uses sqlmock for the DB seams and miniredis for the Redis
// cursor store. No live network, no Docker required.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"instant.dev/worker/internal/email"
)

// ── maskRecipientForLedger defensive branches ──────────────────────────────

func TestForwarder_MaskRecipientForLedger_DefensivePaths(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", ""},                              // empty stays empty
		{"no-at-sign", "no-at-sign"},          // no '@' → unchanged
		{"@only", "@only"},                    // leading '@' (at==0) → unchanged
		{"a@example.com", "a@example.com"},    // single-char local kept
		{"ab@example.com", "a***@example.com"}, // multi-char local masked
		{"alice@example.com", "a***@example.com"},
	}
	for _, tc := range cases {
		if got := maskRecipientForLedger(tc.in); got != tc.want {
			t.Errorf("maskRecipientForLedger(%q) = %q; want %q", tc.in, got, tc.want)
		}
	}
}

// ── EventEmailForwarderArgs.Kind / WeeklyDigestArgs.Kind ───────────────────

func TestForwarder_EventEmailForwarderArgs_Kind(t *testing.T) {
	k := EventEmailForwarderArgs{}.Kind()
	if k != "event_email_forwarder" {
		t.Errorf("Kind() = %q; want event_email_forwarder", k)
	}
}

func TestEventEmail_WeeklyDigestArgs_Kind(t *testing.T) {
	k := WeeklyDigestArgs{}.Kind()
	if k != "weekly_digest" {
		t.Errorf("Kind() = %q; want weekly_digest", k)
	}
}

// ── sqlSentLedger ─────────────────────────────────────────────────────────

func TestForwarder_SqlSentLedger_IsSent_True(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectQuery(`SELECT EXISTS\(SELECT 1 FROM forwarder_sent`).
		WithArgs("a1").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	l := &sqlSentLedger{db: db}
	ok, err := l.isSent(context.Background(), "a1")
	if err != nil {
		t.Fatalf("isSent: %v", err)
	}
	if !ok {
		t.Errorf("isSent should be true when row exists")
	}
}

func TestForwarder_SqlSentLedger_IsSent_False(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectQuery(`SELECT EXISTS\(SELECT 1 FROM forwarder_sent`).
		WithArgs("a2").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	l := &sqlSentLedger{db: db}
	ok, err := l.isSent(context.Background(), "a2")
	if err != nil {
		t.Fatalf("isSent: %v", err)
	}
	if ok {
		t.Errorf("isSent should be false when no row")
	}
}

func TestForwarder_SqlSentLedger_IsSent_DBError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectQuery(`SELECT EXISTS\(SELECT 1 FROM forwarder_sent`).
		WithArgs("aerr").
		WillReturnError(fmt.Errorf("conn closed"))

	l := &sqlSentLedger{db: db}
	if _, err := l.isSent(context.Background(), "aerr"); err == nil {
		t.Fatalf("isSent expected error on DB failure")
	}
}

func TestForwarder_SqlSentLedger_MarkSent_Claimed(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectExec(`INSERT INTO forwarder_sent`).
		WithArgs("a1", "brevo", "msg-1", "a***@example.com", "onboarding.claimed", "success").
		WillReturnResult(sqlmock.NewResult(0, 1))

	l := &sqlSentLedger{db: db}
	claimed, err := l.markSent(context.Background(), ledgerClaim{
		AuditID: "a1", Provider: "brevo", ProviderID: "msg-1",
		Recipient: "a***@example.com", TemplateKind: "onboarding.claimed",
		Classification: ledgerClassSuccess,
	})
	if err != nil {
		t.Fatalf("markSent: %v", err)
	}
	if !claimed {
		t.Errorf("expected claimed=true when RowsAffected==1")
	}
}

func TestForwarder_SqlSentLedger_MarkSent_Conflict(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectExec(`INSERT INTO forwarder_sent`).
		WillReturnResult(sqlmock.NewResult(0, 0)) // ON CONFLICT DO NOTHING → 0 rows

	l := &sqlSentLedger{db: db}
	claimed, err := l.markSent(context.Background(), ledgerClaim{
		AuditID: "a1", Provider: "brevo", ProviderID: "x",
		Recipient: "r", TemplateKind: "k", Classification: ledgerClassSuccess,
	})
	if err != nil {
		t.Fatalf("markSent: %v", err)
	}
	if claimed {
		t.Errorf("expected claimed=false on conflict")
	}
}

func TestForwarder_SqlSentLedger_MarkSent_InsertError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectExec(`INSERT INTO forwarder_sent`).
		WillReturnError(fmt.Errorf("conn dead"))

	l := &sqlSentLedger{db: db}
	if _, err := l.markSent(context.Background(), ledgerClaim{AuditID: "x"}); err == nil {
		t.Errorf("expected error on insert failure")
	}
}

// rowsAffectedErr is a driver.Result whose RowsAffected returns an error.
type rowsAffectedErr struct{}

func (rowsAffectedErr) LastInsertId() (int64, error) { return 0, nil }
func (rowsAffectedErr) RowsAffected() (int64, error) { return 0, fmt.Errorf("driver said no") }

func TestForwarder_SqlSentLedger_MarkSent_RowsAffectedError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectExec(`INSERT INTO forwarder_sent`).
		WillReturnResult(rowsAffectedErr{})

	l := &sqlSentLedger{db: db}
	if _, err := l.markSent(context.Background(), ledgerClaim{AuditID: "x"}); err == nil {
		t.Errorf("expected error on RowsAffected failure")
	}
}

func TestForwarder_SqlSentLedger_Release_OK(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectExec(`DELETE FROM forwarder_sent`).
		WithArgs("a1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	l := &sqlSentLedger{db: db}
	if err := l.release(context.Background(), "a1"); err != nil {
		t.Errorf("release: %v", err)
	}
}

func TestForwarder_SqlSentLedger_Release_DBError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectExec(`DELETE FROM forwarder_sent`).WillReturnError(fmt.Errorf("nope"))

	l := &sqlSentLedger{db: db}
	if err := l.release(context.Background(), "a1"); err == nil {
		t.Errorf("expected error on delete failure")
	}
}

// ── noopSentLedger.release (the empty noop path) ──────────────────────────

func TestForwarder_NoopSentLedger_Release_Noop(t *testing.T) {
	if err := (noopSentLedger{}).release(context.Background(), "a"); err != nil {
		t.Errorf("noop release should be nil, got %v", err)
	}
}

// ── sqlSuppressionChecker — all four paths ────────────────────────────────

func TestForwarder_SqlSuppression_EmptyEmail_NoQuery(t *testing.T) {
	db, _, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	s := &sqlSuppressionChecker{db: db}
	got, err := s.hasSuppression(context.Background(), "")
	if err != nil {
		t.Fatalf("hasSuppression(\"\"): %v", err)
	}
	if got {
		t.Errorf("empty email must not be suppressed")
	}
}

func TestForwarder_SqlSuppression_UnsubscribeFound(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectQuery(`FROM email_events.*event_type = \$2`).
		WithArgs("u@example.com", suppressionEventTypeUnsubscribe).
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))

	s := &sqlSuppressionChecker{db: db}
	got, err := s.hasSuppression(context.Background(), "u@example.com")
	if err != nil {
		t.Fatalf("hasSuppression: %v", err)
	}
	if !got {
		t.Errorf("unsubscribed email must be suppressed")
	}
}

func TestForwarder_SqlSuppression_UnsubscribeDBError_WrapsSentinel(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectQuery(`FROM email_events.*event_type = \$2`).
		WithArgs("u@example.com", suppressionEventTypeUnsubscribe).
		WillReturnError(fmt.Errorf("brownout"))

	s := &sqlSuppressionChecker{db: db}
	_, err := s.hasSuppression(context.Background(), "u@example.com")
	if err == nil {
		t.Fatalf("expected an error on DB failure")
	}
	if !errors.Is(err, errUnsubscribeLookupFailed) {
		t.Errorf("expected error to wrap errUnsubscribeLookupFailed; got %v", err)
	}
}

func TestForwarder_SqlSuppression_BounceFound(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	// Path 1: unsubscribe returns no rows
	mock.ExpectQuery(`FROM email_events.*event_type = \$2`).
		WithArgs("b@example.com", suppressionEventTypeUnsubscribe).
		WillReturnError(sql.ErrNoRows)
	// Path 2: bounce returns a row
	mock.ExpectQuery(`FROM email_events.*event_type = ANY`).
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))

	s := &sqlSuppressionChecker{db: db}
	got, err := s.hasSuppression(context.Background(), "b@example.com")
	if err != nil {
		t.Fatalf("hasSuppression: %v", err)
	}
	if !got {
		t.Errorf("bounce-within-window email must be suppressed")
	}
}

func TestForwarder_SqlSuppression_NoMatchFound(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectQuery(`FROM email_events.*event_type = \$2`).
		WithArgs("ok@example.com", suppressionEventTypeUnsubscribe).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`FROM email_events.*event_type = ANY`).
		WillReturnError(sql.ErrNoRows)

	s := &sqlSuppressionChecker{db: db}
	got, err := s.hasSuppression(context.Background(), "ok@example.com")
	if err != nil {
		t.Fatalf("hasSuppression: %v", err)
	}
	if got {
		t.Errorf("non-suppressed email must not be suppressed")
	}
}

func TestForwarder_SqlSuppression_BounceDBError_PlainErr(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectQuery(`FROM email_events.*event_type = \$2`).
		WithArgs("x@example.com", suppressionEventTypeUnsubscribe).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`FROM email_events.*event_type = ANY`).
		WillReturnError(fmt.Errorf("decay table busy"))

	s := &sqlSuppressionChecker{db: db}
	_, err := s.hasSuppression(context.Background(), "x@example.com")
	if err == nil {
		t.Fatalf("expected an error on decay-path DB failure")
	}
	if errors.Is(err, errUnsubscribeLookupFailed) {
		t.Errorf("bounce-path error must NOT wrap unsubscribe sentinel — it's the fail-open class")
	}
}

// ── redisEventCursorStore — round-trip + missing + corrupt ────────────────

func newTestRedisClient(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

func TestForwarder_RedisCursorStore_Roundtrip(t *testing.T) {
	_, rdb := newTestRedisClient(t)
	s := &redisEventCursorStore{rdb: rdb}

	c0, missing, err := s.read(context.Background())
	if err != nil {
		t.Fatalf("read empty: %v", err)
	}
	if !missing {
		t.Errorf("fresh store must report missing=true")
	}
	if !c0.zero() {
		t.Errorf("fresh store returned non-zero cursor: %+v", c0)
	}

	want := eventCursor{CreatedAt: time.Date(2026, 5, 22, 9, 0, 0, 0, time.UTC), ID: "row-7"}
	if err := s.write(context.Background(), want); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, missing, err := s.read(context.Background())
	if err != nil {
		t.Fatalf("read after write: %v", err)
	}
	if missing {
		t.Errorf("read after write must report missing=false")
	}
	if got.ID != want.ID || !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("roundtrip mismatch: got %+v want %+v", got, want)
	}
}

func TestForwarder_RedisCursorStore_CorruptBlob_TreatedAsMissing(t *testing.T) {
	mr, rdb := newTestRedisClient(t)
	// Stash a corrupt JSON value at the cursor key.
	if err := mr.Set(eventEmailCursorKey, "{not-json"); err != nil {
		t.Fatalf("mr.Set: %v", err)
	}

	s := &redisEventCursorStore{rdb: rdb}
	got, missing, err := s.read(context.Background())
	if err != nil {
		t.Fatalf("read corrupt: %v", err)
	}
	if !missing {
		t.Errorf("corrupt blob must be treated as missing")
	}
	if !got.zero() {
		t.Errorf("corrupt blob must yield zero cursor")
	}
}

func TestForwarder_RedisCursorStore_ReadError(t *testing.T) {
	mr, rdb := newTestRedisClient(t)
	mr.Close() // force connection error
	s := &redisEventCursorStore{rdb: rdb}
	if _, _, err := s.read(context.Background()); err == nil {
		t.Errorf("expected error when Redis is unreachable")
	}
}

func TestForwarder_RedisCursorStore_WriteError(t *testing.T) {
	mr, rdb := newTestRedisClient(t)
	mr.Close()
	s := &redisEventCursorStore{rdb: rdb}
	if err := s.write(context.Background(), eventCursor{ID: "x", CreatedAt: time.Now()}); err == nil {
		t.Errorf("expected error when Redis is unreachable")
	}
}

// ── NewEventEmailForwarderWorker constructor ──────────────────────────────

func TestForwarder_NewEventEmailForwarderWorker_ProductionConstructorWiresProvidedSeams(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	_, rdb := newTestRedisClient(t)
	prov := &fakeProvider{name: "stub"}

	w := NewEventEmailForwarderWorker(db, rdb, prov)
	if w == nil {
		t.Fatal("constructor returned nil")
	}
	if w.db != db {
		t.Errorf("db not wired")
	}
	if w.provider != prov {
		t.Errorf("provider not wired")
	}
	if _, ok := w.cursor.(*redisEventCursorStore); !ok {
		t.Errorf("cursor is not redisEventCursorStore; got %T", w.cursor)
	}
	if _, ok := w.suppression.(*sqlSuppressionChecker); !ok {
		t.Errorf("suppression is not sqlSuppressionChecker; got %T", w.suppression)
	}
	if _, ok := w.ledger.(*sqlSentLedger); !ok {
		t.Errorf("ledger is not sqlSentLedger; got %T", w.ledger)
	}
}

// ── Work() error branches not yet exercised ───────────────────────────────

// flakyProvider returns a configurable messageId+err per call.
type flakyProvider struct{ fakeProvider }

// failingLedger captures call counts and lets the test fail any of the
// three operations. Used to drive Work() branches where the ledger probe
// errors or markSent fails post-2xx.
type failingLedger struct {
	isSentResp  bool
	isSentErr   error
	markErr     error
	markClaimed bool
	releaseErr  error
	markCalls   int
	relCalls    int
	probeCalls  int
}

func (f *failingLedger) isSent(_ context.Context, _ string) (bool, error) {
	f.probeCalls++
	return f.isSentResp, f.isSentErr
}
func (f *failingLedger) markSent(_ context.Context, _ ledgerClaim) (bool, error) {
	f.markCalls++
	if f.markErr != nil {
		return false, f.markErr
	}
	return f.markClaimed, nil
}
func (f *failingLedger) release(_ context.Context, _ string) error {
	f.relCalls++
	return f.releaseErr
}

func TestEventForwarder_LedgerProbeError_HoldsCursor(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	createdAt := time.Date(2026, 5, 22, 9, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("row-1", "team-1", auditKindOnboardingClaimed, "", "x",
				[]byte(`{"signup_source":"x"}`), createdAt, "owner@example.com"))

	prov := &fakeProvider{}
	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, prov)
	w.ledger = &failingLedger{isSentErr: fmt.Errorf("probe failure")}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 0 {
		t.Errorf("send must not happen when ledger probe errors")
	}
	if !cursor.c.zero() {
		t.Errorf("cursor must NOT advance on probe error; got %+v", cursor.c)
	}
}

func TestEventForwarder_LedgerProbeReportsAlreadySent_SkipsAndAdvances(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	createdAt := time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("row-dup", "team-1", auditKindOnboardingClaimed, "", "x",
				[]byte(`{"signup_source":"x"}`), createdAt, "owner@example.com"))

	prov := &fakeProvider{}
	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, prov)
	w.ledger = &failingLedger{isSentResp: true} // already sent

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 0 {
		t.Errorf("must skip send when ledger says already sent")
	}
	if cursor.c.ID != "row-dup" {
		t.Errorf("cursor must advance past already-sent row; got %+v", cursor.c)
	}
}

func TestEventForwarder_LedgerClaimErrorAfterSend_HoldsCursor(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	createdAt := time.Date(2026, 5, 22, 11, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("row-claim-fail", "team-1", auditKindOnboardingClaimed, "", "x",
				[]byte(`{"signup_source":"x"}`), createdAt, "owner@example.com"))

	prov := &fakeProvider{messageID: "msg-1"} // send returns OK
	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, prov)
	w.ledger = &failingLedger{markErr: fmt.Errorf("ledger insert failed post-2xx")}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 1 {
		t.Errorf("send must have been attempted")
	}
	if !cursor.c.zero() {
		t.Errorf("cursor must NOT advance when ledger claim fails post-send")
	}
}

func TestEventForwarder_LedgerClaimRace_AdvancesCursor(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	createdAt := time.Date(2026, 5, 22, 11, 30, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("row-race", "team-1", auditKindOnboardingClaimed, "", "x",
				[]byte(`{"signup_source":"x"}`), createdAt, "owner@example.com"))

	prov := &fakeProvider{messageID: "msg-1"}
	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, prov)
	// markClaimed=false simulates a concurrent forwarder having claimed the row.
	w.ledger = &failingLedger{markClaimed: false}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if cursor.c.ID != "row-race" {
		t.Errorf("cursor must still advance on claim race; got %+v", cursor.c)
	}
}

func TestEventForwarder_NoBuilder_AdvancesCursor(t *testing.T) {
	// Drive the no-builder branch: register a kind in supportedAuditKinds
	// implicit via fetchBatch ANY($1) — we can't easily inject one, so
	// instead we let the SQL return an unmapped kind by faking the row
	// scan to claim a kind that isn't in eventEmailBuilders.
	//
	// fetchBatch SELECTs WHERE kind = ANY($1) — but the SQL filter only
	// rejects kinds NOT in supportedAuditKinds at SQL time; if a row
	// somehow slipped through (or via a refactor), Work() must advance.
	// sqlmock lets us bypass the WHERE filter and return any kind we want.
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	createdAt := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("row-unmapped", "team-1", "totally.unmapped.kind", "", "x",
				[]byte(`{}`), createdAt, "owner@example.com"))

	prov := &fakeProvider{}
	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, prov)

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 0 {
		t.Errorf("must NOT send for unmapped kind")
	}
	if cursor.c.ID != "row-unmapped" {
		t.Errorf("cursor must advance past unmapped kind; got %+v", cursor.c)
	}
}

func TestEventForwarder_MissingRenderer_LedgerInsertFailsButCursorAdvances(t *testing.T) {
	// A kind with a builder but NO renderer — drives the F4 missing_renderer
	// path. We need a kind in eventEmailBuilders that's NOT in
	// eventEmailBodyRenderers. None exist today (the registry test forbids
	// it), so we synthesize one inline using a temp injection.
	origBuilders := eventEmailBuilders
	defer func() { eventEmailBuilders = origBuilders }()
	// Shallow copy + inject a synthetic kind whose renderer is absent.
	synthKind := "test.synthetic_no_renderer"
	newBuilders := make(map[string]eventEmailBuilder, len(origBuilders)+1)
	for k, v := range origBuilders {
		newBuilders[k] = v
	}
	newBuilders[synthKind] = func(row auditRow) (map[string]string, bool) {
		if !requireEmail(row) {
			return nil, false
		}
		return baseParams(row), true
	}
	eventEmailBuilders = newBuilders

	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	createdAt := time.Date(2026, 5, 22, 13, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("row-norend", "team-1", synthKind, "", "x", []byte(`{}`), createdAt, "owner@example.com"))

	prov := &fakeProvider{}
	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, prov)
	// markSent fails to drive the missing_renderer_ledger_failed branch.
	w.ledger = &failingLedger{markErr: fmt.Errorf("ledger down")}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 0 {
		t.Errorf("missing_renderer must not send")
	}
	if cursor.c.ID != "row-norend" {
		t.Errorf("cursor must advance past missing-renderer row even when ledger insert fails; got %+v", cursor.c)
	}
}

func TestEventForwarder_TerminalClass_LedgerInsertFails_StillAdvancesCursor(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	createdAt := time.Date(2026, 5, 22, 13, 30, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("row-perm", "team-1", auditKindOnboardingClaimed, "", "x",
				[]byte(`{"signup_source":"x"}`), createdAt, "owner@example.com"))

	prov := &fakeProvider{
		sendFn: func(_ context.Context, _ email.EventEmail) error {
			return &email.SendError{Class: email.SendClassPermanent, Message: "rejected"}
		},
	}
	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, prov)
	w.ledger = &failingLedger{markErr: fmt.Errorf("ledger down (terminal)")}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if cursor.c.ID != "row-perm" {
		t.Errorf("cursor must advance on Permanent even when terminal-ledger insert fails; got %+v", cursor.c)
	}
}

func TestEventForwarder_TerminalClass_LedgerAlreadyClaimed_NoOp(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	createdAt := time.Date(2026, 5, 22, 14, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("row-perm-2", "team-1", auditKindOnboardingClaimed, "", "x",
				[]byte(`{"signup_source":"x"}`), createdAt, "owner@example.com"))

	prov := &fakeProvider{
		sendFn: func(_ context.Context, _ email.EventEmail) error {
			return &email.SendError{Class: email.SendClassSkippedNoTemplate, Message: "no template"}
		},
	}
	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, prov)
	// markClaimed=false drives the "Already claimed by a prior attempt; benign" branch.
	w.ledger = &failingLedger{markClaimed: false}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if cursor.c.ID != "row-perm-2" {
		t.Errorf("cursor must advance even when terminal-ledger reports already-claimed; got %+v", cursor.c)
	}
}

func TestEventForwarder_CursorReadError_PropagatesAsJobError(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	prov := &fakeProvider{}
	w := newEventEmailForwarderWorkerForTest(db, &erroringCursor{readErr: fmt.Errorf("redis down")}, prov)
	err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]())
	if err == nil {
		t.Fatalf("expected error when cursor.read fails")
	}
	if !strings.Contains(err.Error(), "read cursor") {
		t.Errorf("error message must mention read cursor; got %v", err)
	}
}

func TestEventForwarder_FetchBatchError_PropagatesAsJobError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).WillReturnError(fmt.Errorf("query fail"))

	prov := &fakeProvider{}
	w := newEventEmailForwarderWorkerForTest(db, &memCursor{}, prov)
	err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]())
	if err == nil {
		t.Fatalf("expected error when fetchBatch fails")
	}
}

// erroringCursor returns programmable errors from read/write. Distinct from
// memCursor — memCursor never fails.
type erroringCursor struct {
	readErr  error
	writeErr error
	c        eventCursor
}

func (e *erroringCursor) read(_ context.Context) (eventCursor, bool, error) {
	return e.c, false, e.readErr
}
func (e *erroringCursor) write(_ context.Context, c eventCursor) error {
	e.c = c
	return e.writeErr
}

func TestEventForwarder_CursorWriteError_OnSuccess(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	createdAt := time.Date(2026, 5, 22, 14, 30, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("row-w", "team-1", auditKindOnboardingClaimed, "", "x",
				[]byte(`{"signup_source":"x"}`), createdAt, "owner@example.com"))

	prov := &fakeProvider{messageID: "ok"}
	cursor := &erroringCursor{writeErr: fmt.Errorf("redis write down")}
	w := newEventEmailForwarderWorkerForTest(db, cursor, prov)
	err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]())
	if err == nil {
		t.Fatalf("expected error when cursor.write fails after success")
	}
}

// ── fetchBatch error paths ───────────────────────────────────────────────

func TestForwarder_FetchBatch_ScanError_PropagatesAsError(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	// Return a row whose created_at column is the wrong shape so Scan fails.
	rows := sqlmock.NewRows(auditRowsCols).
		AddRow("id", "team", "kind", "rt", "sum", []byte(`{}`), "not-a-time", "owner@example.com")
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).WillReturnRows(rows)

	w := newEventEmailForwarderWorkerForTest(db, &memCursor{}, &fakeProvider{})
	if _, err := w.fetchBatch(context.Background(), eventCursor{}); err == nil {
		t.Errorf("expected scan error when created_at is malformed")
	}
}

func TestForwarder_FetchBatch_RowsErrAfterIteration(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()

	createdAt := time.Date(2026, 5, 22, 15, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows(auditRowsCols).
		AddRow("id", "team", auditKindOnboardingClaimed, "", "sum", []byte(`{}`), createdAt, "x@example.com").
		RowError(0, fmt.Errorf("rows err after Next"))
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).WillReturnRows(rows)

	w := newEventEmailForwarderWorkerForTest(db, &memCursor{}, &fakeProvider{})
	if _, err := w.fetchBatch(context.Background(), eventCursor{}); err == nil {
		t.Errorf("expected rows.Err to surface")
	}
}

// ── cursor.write error paths after various skip branches ─────────────────
//
// Each of these drives Work() through a different "advance cursor after X"
// failure path. Together they cover the defensive return-error rungs.

// failNthWriteCursor is a cursor whose Nth write returns an error.
type failNthWriteCursor struct {
	failOnNthCall int // 1 = fail first write, etc.
	calls         int
	c             eventCursor
}

func (f *failNthWriteCursor) read(_ context.Context) (eventCursor, bool, error) {
	return f.c, false, nil
}
func (f *failNthWriteCursor) write(_ context.Context, c eventCursor) error {
	f.calls++
	if f.calls == f.failOnNthCall {
		return fmt.Errorf("simulated cursor write failure on call %d", f.calls)
	}
	f.c = c
	return nil
}

func TestEventForwarder_CursorWriteErr_AfterMissingBuilder(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	createdAt := time.Date(2026, 5, 22, 15, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("r-mb", "team-1", "unmapped.kind.xxx", "", "x", []byte(`{}`), createdAt, "owner@example.com"))
	cursor := &failNthWriteCursor{failOnNthCall: 1}
	w := newEventEmailForwarderWorkerForTest(db, cursor, &fakeProvider{})
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err == nil {
		t.Errorf("expected error when cursor.write fails after no_builder_for_kind")
	}
}

func TestEventForwarder_CursorWriteErr_AfterBuilderSkip(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	createdAt := time.Date(2026, 5, 22, 15, 5, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("r-bs", "team-1", auditKindOnboardingClaimed, "", "x", []byte(`{}`), createdAt, "")) // no email
	cursor := &failNthWriteCursor{failOnNthCall: 1}
	w := newEventEmailForwarderWorkerForTest(db, cursor, &fakeProvider{})
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err == nil {
		t.Errorf("expected error when cursor.write fails after builder skip")
	}
}

func TestEventForwarder_CursorWriteErr_AfterSuppression(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	createdAt := time.Date(2026, 5, 22, 15, 10, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("r-sup", "team-1", auditKindOnboardingClaimed, "", "x",
				[]byte(`{"signup_source":"x"}`), createdAt, "supp@example.com"))
	cursor := &failNthWriteCursor{failOnNthCall: 1}
	w := newEventEmailForwarderWorkerForTest(db, cursor, &fakeProvider{})
	w.suppression = &memSuppression{suppressedEmails: map[string]bool{"supp@example.com": true}}
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err == nil {
		t.Errorf("expected error when cursor.write fails after suppression")
	}
}

func TestEventForwarder_CursorWriteErr_AfterMissingRenderer(t *testing.T) {
	// Inject a synthetic kind with builder but no renderer.
	origBuilders := eventEmailBuilders
	defer func() { eventEmailBuilders = origBuilders }()
	synthKind := "test.write_err_after_missing_renderer"
	newBuilders := make(map[string]eventEmailBuilder, len(origBuilders)+1)
	for k, v := range origBuilders {
		newBuilders[k] = v
	}
	newBuilders[synthKind] = func(row auditRow) (map[string]string, bool) {
		if !requireEmail(row) {
			return nil, false
		}
		return baseParams(row), true
	}
	eventEmailBuilders = newBuilders

	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	createdAt := time.Date(2026, 5, 22, 15, 15, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("r-mr", "team-1", synthKind, "", "x", []byte(`{}`), createdAt, "owner@example.com"))

	cursor := &failNthWriteCursor{failOnNthCall: 1}
	w := newEventEmailForwarderWorkerForTest(db, cursor, &fakeProvider{})
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err == nil {
		t.Errorf("expected error when cursor.write fails after missing_renderer")
	}
}

func TestEventForwarder_CursorWriteErr_AfterDuplicateSuppression(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	createdAt := time.Date(2026, 5, 22, 15, 20, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("r-dup", "team-1", auditKindOnboardingClaimed, "", "x",
				[]byte(`{"signup_source":"x"}`), createdAt, "owner@example.com"))
	cursor := &failNthWriteCursor{failOnNthCall: 1}
	w := newEventEmailForwarderWorkerForTest(db, cursor, &fakeProvider{})
	w.ledger = &failingLedger{isSentResp: true} // already sent → duplicate path
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err == nil {
		t.Errorf("expected error when cursor.write fails after duplicate-suppression")
	}
}

func TestEventForwarder_CursorWriteErr_AfterPermanent(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	createdAt := time.Date(2026, 5, 22, 15, 25, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("r-perm-wr", "team-1", auditKindOnboardingClaimed, "", "x",
				[]byte(`{"signup_source":"x"}`), createdAt, "owner@example.com"))
	prov := &fakeProvider{
		sendFn: func(_ context.Context, _ email.EventEmail) error {
			return &email.SendError{Class: email.SendClassPermanent, Message: "rejected"}
		},
	}
	cursor := &failNthWriteCursor{failOnNthCall: 1}
	w := newEventEmailForwarderWorkerForTest(db, cursor, prov)
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err == nil {
		t.Errorf("expected error when cursor.write fails after Permanent class")
	}
}

// ── ledgerClaim sanity — uses every column in the column list ─────────────

func TestForwarder_LedgerClaim_AllColumnsRoundtripThroughInsert(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close()
	mock.ExpectExec(`INSERT INTO forwarder_sent\s*\(audit_id, provider, provider_id, recipient, template_kind, classification\)`).
		WithArgs("a1", providerNoneMissingRenderer, providerIDMissingRenderer, "a***@example.com", "anon.expiry_warning", ledgerClassPermanentDrop).
		WillReturnResult(sqlmock.NewResult(0, 1))

	l := &sqlSentLedger{db: db}
	claimed, err := l.markSent(context.Background(), ledgerClaim{
		AuditID:        "a1",
		Provider:       providerNoneMissingRenderer,
		ProviderID:     providerIDMissingRenderer,
		Recipient:      "a***@example.com",
		TemplateKind:   "anon.expiry_warning",
		Classification: ledgerClassPermanentDrop,
	})
	if err != nil {
		t.Fatalf("markSent: %v", err)
	}
	if !claimed {
		t.Errorf("expected claimed=true")
	}
}
