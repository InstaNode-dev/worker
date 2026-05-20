package jobs

// event_email_forwarder_test.go — hermetic tests for the audit_log → email
// forwarder. Mocks the SQL layer via sqlmock, the cursor store via an
// in-memory implementation of eventCursorStore, and the email provider
// via a tiny fake that the test drives directly.
//
// In package `jobs` (not `jobs_test`) so it can construct the worker via
// newEventEmailForwarderWorkerForTest. The brief specifies the cursor +
// SendClass mapping — each test below pins one row of that table.

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"instant.dev/worker/internal/email"
)

// memCursor is an in-memory eventCursorStore for tests. Goroutine-safe.
//
// missing reports whether the store has no persisted cursor — read()
// returns it as the second value so tests can exercise the P1-2
// seed-to-now path. The zero value (missing=false) behaves like an
// existing zero cursor, preserving every pre-P1-2 test unchanged.
type memCursor struct {
	mu      sync.Mutex
	c       eventCursor
	missing bool
}

func (m *memCursor) read(_ context.Context) (eventCursor, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.c, m.missing, nil
}
func (m *memCursor) write(_ context.Context, c eventCursor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.c = c
	m.missing = false
	return nil
}

// fakeJobLocal mirrors expire_test.go's fakeJob but lives in package `jobs`
// so we don't have to cross the package boundary.
func fakeJobLocal[T river.JobArgs]() *river.Job[T] {
	return &river.Job[T]{JobRow: &rivertype.JobRow{ID: 1}}
}

// fakeProvider is the test double for email.EmailProvider. The test supplies
// a sendFn that decides what each call returns; the fake also records the
// last EventEmail handed to it for assertion.
//
// This is the heart of the provider-agnostic claim: the forwarder talks to
// this fake with no provider-specific code on either side.
type fakeProvider struct {
	name    string
	sendFn  func(ctx context.Context, evt email.EventEmail) error
	// sendFnWithID, when set, overrides sendFn and lets a test return a
	// per-send messageId alongside the error. Most tests just exercise
	// the error-classification path so they use sendFn (which yields
	// "" for the messageId); the messageId-capture tests use this.
	sendFnWithID func(ctx context.Context, evt email.EventEmail) (string, error)
	// messageID is the static value returned on SendEvent success when
	// neither sendFnWithID nor sendFn is set. "" by default — the
	// historical (pre-2026-05-20) behaviour.
	messageID string
	calls     int32
	lastEvt   email.EventEmail
	mu        sync.Mutex
}

func (f *fakeProvider) SendEvent(ctx context.Context, evt email.EventEmail) (string, error) {
	atomic.AddInt32(&f.calls, 1)
	f.mu.Lock()
	f.lastEvt = evt
	f.mu.Unlock()
	if f.sendFnWithID != nil {
		return f.sendFnWithID(ctx, evt)
	}
	if f.sendFn != nil {
		return "", f.sendFn(ctx, evt)
	}
	return f.messageID, nil
}

func (f *fakeProvider) Name() string {
	if f.name == "" {
		return "fake"
	}
	return f.name
}

func (f *fakeProvider) callCount() int32 { return atomic.LoadInt32(&f.calls) }

// auditRowsCols are the columns the forwarder's fetchBatch expects to scan.
var auditRowsCols = []string{"id", "team_id", "kind", "resource_type", "summary", "metadata", "created_at", "owner_email"}

// TestEventForwarder_SupportedKind_SendsAndAdvances verifies the headline
// guarantee: a supported audit_log row triggers a SendEvent AND the
// cursor advances past that row's (created_at, id).
func TestEventForwarder_SupportedKind_SendsAndAdvances(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Date(2026, 5, 13, 8, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("audit-id-1", "team-1", auditKindOnboardingClaimed, "", "team claimed", []byte(`{"signup_source":"github"}`), createdAt, "owner@example.com"))

	provider := &fakeProvider{} // default sendFn = nil → success

	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := provider.callCount(); got != 1 {
		t.Errorf("expected 1 SendEvent call, got %d", got)
	}
	if provider.lastEvt.Kind != auditKindOnboardingClaimed {
		t.Errorf("EventEmail.Kind = %q; want %q", provider.lastEvt.Kind, auditKindOnboardingClaimed)
	}
	if provider.lastEvt.Recipient != "owner@example.com" {
		t.Errorf("EventEmail.Recipient = %q; want owner@example.com", provider.lastEvt.Recipient)
	}
	if provider.lastEvt.IdempotencyKey != "audit-audit-id-1" {
		t.Errorf("EventEmail.IdempotencyKey = %q; want audit-audit-id-1 (prefix + row id)", provider.lastEvt.IdempotencyKey)
	}
	if provider.lastEvt.Params["signup_source"] != "github" {
		t.Errorf("EventEmail.Params[signup_source] = %q; want github", provider.lastEvt.Params["signup_source"])
	}
	if cursor.c.ID != "audit-id-1" {
		t.Errorf("cursor.ID = %q; want audit-id-1 (the row we processed)", cursor.c.ID)
	}
	if !cursor.c.CreatedAt.Equal(createdAt) {
		t.Errorf("cursor.CreatedAt = %v; want %v", cursor.c.CreatedAt, createdAt)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestEventForwarder_PermanentAdvancesCursor verifies the "don't get stuck
// on a poisoned row" contract: provider returns SendClassPermanent →
// advance cursor.
func TestEventForwarder_PermanentAdvancesCursor(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Date(2026, 5, 13, 9, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("audit-id-2", "team-2", auditKindSubscriptionUpgraded, "", "upgraded to pro", []byte(`{"to_tier":"pro"}`), createdAt, "owner@example.com"))

	provider := &fakeProvider{
		sendFn: func(_ context.Context, _ email.EventEmail) error {
			return &email.SendError{Class: email.SendClassPermanent, Message: "provider rejected"}
		},
	}

	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := provider.callCount(); got != 1 {
		t.Errorf("expected 1 SendEvent call, got %d", got)
	}
	// Despite the Permanent error, the cursor MUST advance — holding it
	// would pin the queue behind the poisoned row.
	if cursor.c.ID != "audit-id-2" {
		t.Errorf("cursor.ID = %q after Permanent; want audit-id-2 (advance past poisoned row)", cursor.c.ID)
	}
}

// TestEventForwarder_SkippedNoTemplateAdvancesCursor — the provider has no
// template for this kind. Cursor MUST advance (silently, at INFO).
func TestEventForwarder_SkippedNoTemplateAdvancesCursor(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Date(2026, 5, 13, 9, 30, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("audit-id-x", "team-x", auditKindOnboardingClaimed, "", "x", []byte(`{}`), createdAt, "owner@example.com"))

	provider := &fakeProvider{
		sendFn: func(_ context.Context, _ email.EventEmail) error {
			return &email.SendError{Class: email.SendClassSkippedNoTemplate, Message: "no template"}
		},
	}

	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if cursor.c.ID != "audit-id-x" {
		t.Errorf("cursor.ID = %q after SkippedNoTemplate; want audit-id-x (advance silently)", cursor.c.ID)
	}
}

// TestEventForwarder_TransientHoldsCursor verifies the retry contract:
// provider returns SendClassTransient → no cursor advance, batch halts.
func TestEventForwarder_TransientHoldsCursor(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Date(2026, 5, 13, 10, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("audit-id-3", "team-3", auditKindOnboardingClaimed, "", "claim", []byte(`{}`), createdAt, "owner@example.com"))

	provider := &fakeProvider{
		sendFn: func(_ context.Context, _ email.EventEmail) error {
			return &email.SendError{Class: email.SendClassTransient, Message: "503"}
		},
	}

	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := provider.callCount(); got != 1 {
		t.Errorf("expected 1 SendEvent call, got %d", got)
	}
	// Cursor MUST NOT advance — next tick retries.
	if !cursor.c.zero() {
		t.Errorf("cursor advanced on Transient (%+v); want zero so next tick retries", cursor.c)
	}
}

// TestEventForwarder_BatchHaltsOnTransient verifies that a Transient mid-batch
// stops processing the rest of the batch. We give the worker two rows,
// return Transient on the first call, and assert only one SendEvent was
// attempted.
func TestEventForwarder_BatchHaltsOnTransient(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	ca1 := time.Date(2026, 5, 13, 11, 0, 0, 0, time.UTC)
	ca2 := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("row-a", "team-a", auditKindOnboardingClaimed, "", "x", []byte(`{}`), ca1, "a@example.com").
			AddRow("row-b", "team-b", auditKindOnboardingClaimed, "", "y", []byte(`{}`), ca2, "b@example.com"))

	provider := &fakeProvider{
		sendFn: func(_ context.Context, _ email.EventEmail) error {
			return &email.SendError{Class: email.SendClassTransient, Message: "503"}
		},
	}

	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	// Only the first row should have been attempted — Transient halts the batch.
	if got := provider.callCount(); got != 1 {
		t.Errorf("expected exactly 1 SendEvent call (halt on Transient), got %d", got)
	}
}

// TestEventForwarder_NoOwnerEmailAdvances verifies the builder-skip path:
// a row without an owner email returns ok=false from its builder. The
// forwarder MUST advance the cursor AND MUST NOT call SendEvent.
func TestEventForwarder_NoOwnerEmailAdvances(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Date(2026, 5, 13, 13, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("orphan-row", "team-x", auditKindOnboardingClaimed, "", "x", []byte(`{}`), createdAt, "")) // no email

	provider := &fakeProvider{}

	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := provider.callCount(); got != 0 {
		t.Errorf("expected 0 SendEvent calls for orphan row, got %d", got)
	}
	if cursor.c.ID != "orphan-row" {
		t.Errorf("cursor.ID = %q; want orphan-row (must advance past unsendable row)", cursor.c.ID)
	}
}

// TestEventForwarder_AnonRowDeliversViaMetadataEmail is the W3 (P1-W3-10)
// regression guard. An anonymous team has no users row, so the LEFT JOIN
// yields a NULL owner_email; the recipient lives only in the audit row's
// metadata.email. Before the fix the builder saw an empty OwnerEmail,
// returned ok=false, and the forwarder advanced past the row — the
// highest-volume free-funnel email (anon.expiry_warning) was structurally
// undeliverable.
//
// Here the mocked row carries owner_email="" but a metadata.email — the
// shape an anonymous-tier row has. The forwarder MUST still SendEvent, and
// the EventEmail.Recipient MUST be the metadata address.
func TestEventForwarder_AnonRowDeliversViaMetadataEmail(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Date(2026, 5, 18, 9, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("anon-row-1", "anon-team", auditKindAnonExpiryWarning, "postgres", "anon resource expiring",
				[]byte(`{"email":"anon@example.com","resource_type":"postgres","hours_remaining":3}`),
				createdAt, "")) // owner_email empty — anonymous team, no users row

	provider := &fakeProvider{}
	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := provider.callCount(); got != 1 {
		t.Fatalf("W3: expected 1 SendEvent for an anon row with a metadata.email, got %d — the anon expiry email was dropped", got)
	}
	if provider.lastEvt.Recipient != "anon@example.com" {
		t.Errorf("W3: EventEmail.Recipient = %q; want anon@example.com (metadata.email fallback)", provider.lastEvt.Recipient)
	}
}

// TestEventForwarder_FetchBatchQueryWiring is the W4 (P1-W3-11) + W3
// guard on the fetchBatch SQL itself. sqlmock's regexp matcher does not
// verify the query body, so this test asserts the two structural fixes are
// present in the SQL string the forwarder runs:
//
//	W4: the users join filters on `is_primary = true` (migration 029),
//	    so a team with multiple users emails its PRIMARY user — not
//	    whoever signed up first.
//	W3: the recipient COALESCE includes `metadata->>'email'`, so an
//	    anonymous team with no users row still resolves a recipient.
//
// The query text is a private const inside fetchBatch; we capture it by
// running fetchBatch against a sqlmock that records the SQL it was asked
// to run.
func TestEventForwarder_FetchBatchQueryWiring(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// W4: assert the is_primary filter is in the join.
	mock.ExpectQuery(`is_primary\s*=\s*true`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols))
	w := newEventEmailForwarderWorkerForTest(db, &memCursor{}, &fakeProvider{})
	if _, err := w.fetchBatch(context.Background(), eventCursor{}); err != nil {
		t.Fatalf("W4: fetchBatch query does not contain `is_primary = true` — a multi-user team would email a non-primary user: %v", err)
	}

	// W3: assert the recipient COALESCE includes the metadata.email fallback.
	mock.ExpectQuery(`COALESCE\(u\.email,\s*a\.metadata->>'email'`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols))
	if _, err := w.fetchBatch(context.Background(), eventCursor{}); err != nil {
		t.Fatalf("W3: fetchBatch query does not COALESCE metadata->>'email' — anonymous-tier rows resolve no recipient: %v", err)
	}
}

// TestEventForwarder_BatchLimitIsHonored verifies the SQL passes the
// eventEmailBatchLimit constant — protects against a refactor that drops
// the LIMIT clause and tries to drain millions of rows per tick.
func TestEventForwarder_BatchLimitIsHonored(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Match the LIMIT clause literally — if a future refactor drops it,
	// this test fails first.
	mock.ExpectQuery(`LIMIT \$4`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols))

	provider := &fakeProvider{}
	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
	if eventEmailBatchLimit != 100 {
		t.Errorf("eventEmailBatchLimit = %d; want 100 (the brief specifies 100-row batches)", eventEmailBatchLimit)
	}
}

// TestEventForwarder_NoRowsExitsClean verifies that an empty batch is a
// no-op — no SendEvent calls, no cursor change, no error.
func TestEventForwarder_NoRowsExitsClean(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols))

	provider := &fakeProvider{
		sendFn: func(_ context.Context, _ email.EventEmail) error {
			t.Errorf("SendEvent attempted on empty batch")
			return nil
		},
	}

	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if !cursor.c.zero() {
		t.Errorf("cursor changed on empty batch: %+v", cursor.c)
	}
}

// ── Suppression tests ────────────────────────────────────────────────────────
//
// These pin the brief's three new contracts:
//   4. Recipient has bounce row in last 365d → skip + advance cursor.
//   5. Recipient has bounce row >366d ago    → still send (decay).
//   6. Recipient has unsubscribe row         → permanent skip (no decay).
//
// memSuppression is the in-memory checker used by these tests. It just
// returns whatever the test sets up — the decay rule is exercised by
// the production sqlSuppressionChecker, which is covered by the api
// repo's email_events_test.go (TestEmailEvents_HasSuppressionFor_*).

type memSuppression struct {
	suppressedEmails map[string]bool
	failNext         error
}

func (m *memSuppression) hasSuppression(_ context.Context, emailAddr string) (bool, error) {
	if m.failNext != nil {
		err := m.failNext
		m.failNext = nil
		return false, err
	}
	return m.suppressedEmails[emailAddr], nil
}

// TestEventForwarder_SuppressedRecipient_SkipsSend verifies that when the
// suppression checker reports a recipient is suppressed (bounce within
// window), the forwarder:
//   - DOES NOT call SendEvent
//   - DOES advance the cursor past the row
//   - Bumps the skipped counter
func TestEventForwarder_SuppressedRecipient_SkipsSend(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Date(2026, 5, 13, 15, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("supp-row-1", "team-s", auditKindOnboardingClaimed, "", "x", []byte(`{}`), createdAt, "bouncey@example.com"))

	provider := &fakeProvider{
		sendFn: func(_ context.Context, _ email.EventEmail) error {
			t.Errorf("SendEvent must NOT be called for a suppressed recipient")
			return nil
		},
	}
	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
	w.suppression = &memSuppression{
		suppressedEmails: map[string]bool{"bouncey@example.com": true},
	}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := provider.callCount(); got != 0 {
		t.Errorf("expected 0 SendEvent calls for suppressed recipient, got %d", got)
	}
	// Cursor MUST advance — holding it would re-attempt the same row
	// every 60s tick.
	if cursor.c.ID != "supp-row-1" {
		t.Errorf("cursor.ID = %q after suppression; want supp-row-1 (must advance)", cursor.c.ID)
	}
}

// TestEventForwarder_NonSuppressedRecipient_StillSends verifies the
// negative-space contract: when the suppression checker reports the
// recipient is NOT suppressed, the SendEvent fires normally. This is
// the "bounce >366d ago" / "address never bounced" path — the decay
// rule is enforced by the suppression checker, not the forwarder, so
// here we just check that a not-suppressed answer lets the send proceed.
func TestEventForwarder_NonSuppressedRecipient_StillSends(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Date(2026, 5, 13, 16, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("clean-row-1", "team-c", auditKindOnboardingClaimed, "", "x", []byte(`{}`), createdAt, "clean@example.com"))

	provider := &fakeProvider{} // success
	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
	w.suppression = &memSuppression{
		suppressedEmails: map[string]bool{}, // empty — nobody suppressed
	}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := provider.callCount(); got != 1 {
		t.Errorf("expected 1 SendEvent call for non-suppressed recipient, got %d", got)
	}
	if cursor.c.ID != "clean-row-1" {
		t.Errorf("cursor.ID = %q; want clean-row-1", cursor.c.ID)
	}
}

// TestEventForwarder_UnsubscribeIsPermanent verifies the brief's "no
// decay for unsubscribes" rule via the seam: the suppression checker
// can return true for an unsubscribe regardless of age, and the
// forwarder honors that without trying to second-guess the lookback
// window. The decay-vs-no-decay split is enforced in the checker
// (see api/internal/models/email_events.go HasSuppressionFor); the
// worker test pins the integration contract.
func TestEventForwarder_UnsubscribeIsPermanent(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Date(2026, 5, 13, 17, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("unsub-row-1", "team-u", auditKindSubscriptionUpgraded, "", "upgrade", []byte(`{"to_tier":"pro"}`), createdAt, "leaver@example.com"))

	provider := &fakeProvider{
		sendFn: func(_ context.Context, _ email.EventEmail) error {
			t.Errorf("SendEvent must NOT be called for an unsubscribed recipient (no decay)")
			return nil
		},
	}
	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
	w.suppression = &memSuppression{
		suppressedEmails: map[string]bool{"leaver@example.com": true},
	}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := provider.callCount(); got != 0 {
		t.Errorf("expected 0 SendEvent calls for unsubscribed recipient, got %d", got)
	}
	if cursor.c.ID != "unsub-row-1" {
		t.Errorf("cursor.ID = %q after unsubscribe-suppression; want unsub-row-1", cursor.c.ID)
	}
}

// TestEventForwarder_SuppressionCheckerError_FailsOpen verifies the
// fail-open contract: a DB error from the suppression checker is
// logged-and-swallowed, and the send proceeds. A Postgres blip MUST
// NOT pin the queue or block sends — duplicate-to-bouncer is
// preferable to no-sends-at-all.
func TestEventForwarder_SuppressionCheckerError_FailsOpen(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Date(2026, 5, 13, 18, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("err-row-1", "team-e", auditKindOnboardingClaimed, "", "x", []byte(`{}`), createdAt, "anyone@example.com"))

	provider := &fakeProvider{}
	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
	w.suppression = &memSuppression{
		suppressedEmails: map[string]bool{},
		failNext:         errFakeSuppression,
	}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := provider.callCount(); got != 1 {
		t.Errorf("expected 1 SendEvent call (fail-open), got %d", got)
	}
	if cursor.c.ID != "err-row-1" {
		t.Errorf("cursor.ID = %q; want err-row-1 (sent successfully despite suppression-check error)", cursor.c.ID)
	}
}

// errFakeSuppression is the sentinel returned by memSuppression.failNext
// to simulate a DB error from the suppression checker.
var errFakeSuppression = &fakeSuppressionError{}

type fakeSuppressionError struct{}

func (*fakeSuppressionError) Error() string { return "fake suppression DB error" }

// TestEventForwarder_UnsubscribeCheckError_FailsClosed verifies the
// SPLIT fail posture: a DB error specifically in the UNSUBSCRIBE lookup
// (wrapping errUnsubscribeLookupFailed) MUST fail CLOSED — the send
// is skipped AND the cursor is NOT advanced, so the row retries when the
// DB recovers. Emailing an unsubscribed user during a DB brownout is a
// CAN-SPAM / GDPR compliance violation, unlike a bounce-lookup error
// (covered by TestEventForwarder_SuppressionCheckerError_FailsOpen).
func TestEventForwarder_UnsubscribeCheckError_FailsClosed(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Date(2026, 5, 13, 19, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("unsub-err-1", "team-ue", auditKindOnboardingClaimed, "", "x", []byte(`{}`), createdAt, "maybe-unsub@example.com"))

	provider := &fakeProvider{
		sendFn: func(_ context.Context, _ email.EventEmail) error {
			t.Errorf("SendEvent must NOT be called when the unsubscribe lookup failed (fail-closed)")
			return nil
		},
	}
	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
	w.suppression = &memSuppression{
		suppressedEmails: map[string]bool{},
		// Wraps the sentinel so Work() takes the fail-CLOSED branch.
		failNext: fmt.Errorf("simulated unsubscribe DB error: %w", errUnsubscribeLookupFailed),
	}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	if got := provider.callCount(); got != 0 {
		t.Errorf("expected 0 SendEvent calls (fail-closed), got %d", got)
	}
	// Cursor MUST NOT advance — the row must be retried once the DB recovers.
	if cursor.c.ID != "" {
		t.Errorf("cursor.ID = %q; want \"\" (cursor MUST NOT advance on unsubscribe-lookup failure)", cursor.c.ID)
	}
}

// TestEventForwarder_NoopProvider_AdvancesCursor — wiring a real
// email.NoopProvider through the forwarder is the integration check that
// the SendClassSkippedNoTemplate path advances cursors. If this regresses,
// every operator who hasn't configured EMAIL_PROVIDER would silently
// re-fetch the same rows forever.
func TestEventForwarder_NoopProvider_AdvancesCursor(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Date(2026, 5, 13, 14, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("noop-row", "team-n", auditKindOnboardingClaimed, "", "claim", []byte(`{}`), createdAt, "u@example.com"))

	provider := &email.NoopProvider{}
	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if cursor.c.ID != "noop-row" {
		t.Errorf("cursor.ID = %q; want noop-row (NoopProvider must let the cursor advance)", cursor.c.ID)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// BugBash 2026-05-19 regression tests. Each FAILS against the pre-fix code
// and PASSES only with the corresponding fix in place.
// ─────────────────────────────────────────────────────────────────────────

// recentTimeArg is a sqlmock argument matcher that passes when the value
// is a time.Time within `window` of now — used to assert the P1-2 48h
// fetchBatch age floor is computed from the current time.
type recentTimeArg struct {
	window time.Duration
}

func (m recentTimeArg) Match(v driver.Value) bool {
	tv, ok := v.(time.Time)
	if !ok {
		return false
	}
	delta := time.Since(tv)
	return delta > 0 && delta < m.window
}

// fakeLedger is the test double for sentLedger. claimed controls what
// markSent returns; markCalls / releaseCalls / isSentCalls count
// invocations so a test can assert idempotency behavior. seen records
// claimed audit_ids so a re-run against the SAME fakeLedger behaves
// like the real ON CONFLICT. lastClaim captures the most recent
// ledgerClaim passed to markSent so a test can assert the 059 audit
// columns flow through correctly.
type fakeLedger struct {
	mu           sync.Mutex
	seen         map[string]bool
	claims       map[string]ledgerClaim
	isSentCalls  int
	markCalls    int
	releaseCalls int
	markErr      error
	isSentErr    error
	lastClaim    ledgerClaim
}

func newFakeLedger() *fakeLedger {
	return &fakeLedger{seen: map[string]bool{}, claims: map[string]ledgerClaim{}}
}

func (l *fakeLedger) isSent(_ context.Context, auditID string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.isSentCalls++
	if l.isSentErr != nil {
		return false, l.isSentErr
	}
	return l.seen[auditID], nil
}

func (l *fakeLedger) markSent(_ context.Context, c ledgerClaim) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.markCalls++
	l.lastClaim = c
	if l.markErr != nil {
		return false, l.markErr
	}
	if l.seen[c.AuditID] {
		return false, nil // already claimed — duplicate
	}
	l.seen[c.AuditID] = true
	l.claims[c.AuditID] = c
	return true, nil
}

func (l *fakeLedger) release(_ context.Context, auditID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.releaseCalls++
	delete(l.seen, auditID)
	delete(l.claims, auditID)
	return nil
}

// TestEventForwarder_FetchBatch_HasAgeFloor is the P1-2 regression test.
//
// BUG: a Redis wipe reset the cursor to the zero value and fetchBatch —
// which had NO age bound — re-scanned and re-emailed the ENTIRE audit_log
// history. This test runs Work with a zero cursor and asserts the SQL
// query carries a recent (within 48h) age-floor argument. It FAILS
// against the pre-fix query (which had only 4 args, no age floor).
func TestEventForwarder_FetchBatch_HasAgeFloor(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// The query MUST include `a.created_at > $5` and pass a now()-anchored
	// timestamp as the 5th arg. recentTimeArg asserts that arg is within
	// the 48h window — i.e. the floor exists and is freshly computed.
	mock.ExpectQuery(`a\.created_at > \$5`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			eventEmailBatchLimit, recentTimeArg{window: eventEmailMaxAge + time.Minute}).
		WillReturnRows(sqlmock.NewRows(auditRowsCols))

	w := newEventEmailForwarderWorkerForTest(db, &memCursor{}, &fakeProvider{})
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("fetchBatch did not apply the 48h age floor: %v", err)
	}
}

// TestEventForwarder_MissingCursor_SeedsToNow is the other half of P1-2.
//
// BUG: a missing cursor fell through as the zero value, so fetchBatch
// scanned from the beginning of time. With the fix, a missing cursor is
// seeded to now()-grace. This test sets the cursor store to missing=true
// and asserts the cursor predicate ($2) handed to the query is a recent
// timestamp, NOT the time.Time zero value.
func TestEventForwarder_MissingCursor_SeedsToNow(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// $2 is the cursor created_at. A seeded cursor → recent timestamp.
	// A pre-fix zero cursor → the time.Time zero value, which recentTimeArg
	// rejects (delta from now is centuries, far outside the window).
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WithArgs(sqlmock.AnyArg(),
			recentTimeArg{window: eventEmailCursorSeedGrace + time.Minute},
			sqlmock.AnyArg(), eventEmailBatchLimit, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows(auditRowsCols))

	w := newEventEmailForwarderWorkerForTest(db, &memCursor{missing: true}, &fakeProvider{})
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("missing cursor was not seeded to now()-grace (would re-scan ancient rows): %v", err)
	}
}

// TestEventForwarder_Ledger_SendsOnce is the P1-3 regression test.
//
// BUG: idempotency rested on a Brevo header that is not a delivery-dedup
// mechanism, so a cursor reset / crash recovery re-sent every email. With
// the forwarder_sent ledger, a second run carrying the SAME audit_id must
// NOT re-send. This test runs Work TWICE against the same fakeLedger (the
// 2nd run simulates a cursor reset — the cursor is reset to zero) and
// asserts the provider's SendEvent was called exactly once total.
func TestEventForwarder_Ledger_SendsOnce(t *testing.T) {
	createdAt := time.Now().UTC().Add(-time.Hour) // inside the 48h floor

	provider := &fakeProvider{} // default sendFn nil → success
	ledger := newFakeLedger()

	runOnce := func(cursor *memCursor) {
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
			WillReturnRows(sqlmock.NewRows(auditRowsCols).
				AddRow("dup-audit-id", "team-1", auditKindOnboardingClaimed, "",
					"team claimed", []byte(`{"signup_source":"github"}`), createdAt, "owner@example.com"))
		w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
		w.ledger = ledger
		if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
			t.Fatalf("Work: %v", err)
		}
	}

	// Run 1 — normal: claims the ledger, sends.
	runOnce(&memCursor{})
	// Run 2 — simulate a cursor reset (fresh zero cursor): the same
	// audit row is re-fetched. The ledger MUST suppress the re-send.
	runOnce(&memCursor{})

	if got := provider.callCount(); got != 1 {
		t.Errorf("SendEvent called %d times across two runs of the same audit_id; want 1 — the forwarder_sent ledger must make re-runs idempotent (P1-3)", got)
	}
	// MR-P1-16: under claim-after-2xx the BEFORE-send dedup is the
	// isSent probe, not markSent. Run 1 calls markSent once (after a
	// successful send); run 2's isSent probe returns true → markSent is
	// NEVER called on the duplicate. The total markCalls is 1, not 2.
	if ledger.markCalls != 1 {
		t.Errorf("ledger.markSent called %d times; want 1 (only the first successful send claims; the duplicate is suppressed via isSent BEFORE the send)", ledger.markCalls)
	}
	if ledger.isSentCalls != 2 {
		t.Errorf("ledger.isSent called %d times; want 2 (probe runs once per row per tick — both ticks should probe)", ledger.isSentCalls)
	}
}

// TestEventForwarder_TransientDoesNotClaim guards the MR-P1-16 ledger
// ordering: under claim-after-2xx, a Transient send MUST NOT have claimed
// the ledger (the claim is gated on a confirmed 2xx) — so there is
// nothing to release and the audit_id is NOT in `seen`. Next tick will
// re-fetch the row, the isSent probe will return false, and the send
// will retry. Pre-MR-P1-16 ordering claimed before the send and required
// release on Transient; that ordering opened a crash-loss window
// (see TestEventForwarder_CrashAfterSendBeforeClaim_NoLoss below).
func TestEventForwarder_TransientDoesNotClaim(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Now().UTC().Add(-time.Hour)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("transient-id", "team-1", auditKindOnboardingClaimed, "",
				"claim", []byte(`{}`), createdAt, "owner@example.com"))

	provider := &fakeProvider{
		sendFn: func(_ context.Context, _ email.EventEmail) error {
			return &email.SendError{Class: email.SendClassTransient, Message: "5xx"}
		},
	}
	ledger := newFakeLedger()
	w := newEventEmailForwarderWorkerForTest(db, &memCursor{}, provider)
	w.ledger = ledger
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if ledger.markCalls != 0 {
		t.Errorf("ledger.markSent called %d times after a Transient send; want 0 — under claim-after-2xx, Transient must NEVER claim", ledger.markCalls)
	}
	if ledger.releaseCalls != 0 {
		t.Errorf("ledger.release called %d times; want 0 — there is no claim to release under claim-after-2xx", ledger.releaseCalls)
	}
	if ledger.seen["transient-id"] {
		t.Errorf("transient-id appears in the ledger after a Transient send — the row must be retryable next tick")
	}
}

// TestEventForwarder_CrashAfterSendBeforeClaim_NoLoss is the MR-P1-16
// regression test.
//
// BUG (pre-MR-P1-16, the 2026-05-19 ordering): the forwarder claimed
// `forwarder_sent` BEFORE calling SendEvent. A pod-kill between the
// claim commit and the send returning left the row ledgered with no
// email actually delivered. On restart the next tick re-fetched the
// row, saw claimed=false → branched to "duplicate_suppressed" → cursor
// advanced → email permanently lost, no alert.
//
// FIX (MR-P1-16, this PR): the claim moved to AFTER a confirmed 2xx.
// A crash mid-POST now leaves the ledger un-claimed and the cursor
// un-advanced — restart re-sends the email (Brevo X-Mailin-Custom
// header absorbs the duplicate where honored). A duplicate is strictly
// safer than a silent loss.
//
// Repro: a fake provider whose SendEvent returns nil (success) on
// the first call, then SIGKILL is simulated by the test simply NOT
// crashing the process — but the test asserts the email IS attempted
// on a re-run. The pre-fix ordering would have skipped the re-run
// (the ledger row from the crashed run would have been present); the
// fix leaves the ledger un-claimed if the claim itself fails post-send.
//
// We exercise the precise loss-pathway by using a markErr on the FIRST
// run only (simulating "send succeeded but ledger write failed / pod
// died during ledger commit"). With the old ordering, the row would
// have been pre-claimed before the send. With the new ordering, the
// ledger only ever sees markSent AFTER a successful send, so a
// markSent error after a successful send is a "transient hold" (cursor
// not advanced) and the next tick retries the send. The email IS
// re-attempted instead of being silently lost.
func TestEventForwarder_CrashAfterSendBeforeClaim_NoLoss(t *testing.T) {
	createdAt := time.Now().UTC().Add(-time.Hour)

	provider := &fakeProvider{} // default success
	ledger := newFakeLedger()
	// First run: markSent errors after the send (simulating a crash
	// between SendEvent returning and the ledger insert committing,
	// or a transient DB error on the claim).
	ledger.markErr = errors.New("simulated crash: ledger insert failed after send")

	runOnce := func(cursor *memCursor) {
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
			WillReturnRows(sqlmock.NewRows(auditRowsCols).
				AddRow("crash-window-id", "team-1", auditKindOnboardingClaimed, "",
					"claim", []byte(`{"signup_source":"github"}`), createdAt, "owner@example.com"))
		w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
		w.ledger = ledger
		if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
			t.Fatalf("Work: %v", err)
		}
	}

	// Run 1: send succeeds, ledger insert errors → cursor NOT advanced,
	// transient_halt — row eligible for retry.
	cursor1 := &memCursor{}
	runOnce(cursor1)
	if got := provider.callCount(); got != 1 {
		t.Fatalf("Run 1: SendEvent called %d times; want 1 (send happens before claim)", got)
	}
	// Cursor must NOT have advanced past the zero value. If it advanced,
	// the next tick would skip this row, losing the email.
	if cursor1.c.ID != "" || !cursor1.c.CreatedAt.IsZero() {
		t.Errorf("Run 1: cursor advanced to id=%q created_at=%v after a failed post-send claim — next tick would skip this row, losing the email",
			cursor1.c.ID, cursor1.c.CreatedAt)
	}

	// Run 2: clear the markErr (simulating recovery from the crash)
	// and re-run with a fresh zero cursor (simulating cursor reset /
	// new pod start). The email MUST be re-attempted, NOT silently
	// suppressed. Under the OLD ordering this would be skipped (the
	// pre-claim from run 1 would have been written before the send
	// crashed) — a permanent loss. Under the NEW ordering, run 1's
	// failed claim left the ledger empty, so isSent returns false
	// and the send is re-tried.
	ledger.markErr = nil
	runOnce(&memCursor{})
	if got := provider.callCount(); got != 2 {
		t.Errorf("Run 2 after a crash-window failure: SendEvent called %d times total; want 2 — claim-after-2xx must allow re-send when a post-send claim fails (MR-P1-16)", got)
	}
	if !ledger.seen["crash-window-id"] {
		t.Errorf("After the recovery run the audit_id must be in the ledger so subsequent ticks won't re-send")
	}
}

// TestEventForwarder_SuppressionUsesSentRecipient is the F5 regression
// test.
//
// BUG: the forwarder checked suppression against row.OwnerEmail but sent
// to resolveRecipient(row). For an anonymous-tier row whose address lives
// in metadata.email and whose OwnerEmail column is empty, the suppression
// check ran against "" (always "not suppressed") while the email went to
// the real metadata address — so an unsubscribed anon user still got the
// email. This test feeds a row with EMPTY owner_email and the recipient
// only in metadata.email, marks that metadata address suppressed, and
// asserts NO send happens. Pre-fix it sends (checked "" → not suppressed).
func TestEventForwarder_SuppressionUsesSentRecipient(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Now().UTC().Add(-time.Hour)
	const anonAddr = "anon-unsubscribed@example.com"
	// owner_email column is EMPTY — the recipient is ONLY in metadata.email.
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("anon-row", "team-anon", auditKindAnonExpiryWarning, "redis",
				"anon expiry", []byte(`{"email":"`+anonAddr+`","hours_remaining":"6","resource_type":"redis"}`),
				createdAt, "" /* owner_email empty */))

	provider := &fakeProvider{
		sendFn: func(_ context.Context, _ email.EventEmail) error {
			t.Errorf("SendEvent called for an unsubscribed anon recipient — suppression checked the wrong (empty) address")
			return nil
		},
	}
	w := newEventEmailForwarderWorkerForTest(db, &memCursor{}, provider)
	// Suppress the metadata.email address — the one the email is sent to.
	w.suppression = &memSuppression{suppressedEmails: map[string]bool{anonAddr: true}}
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if got := provider.callCount(); got != 0 {
		t.Errorf("SendEvent called %d times; want 0 — suppression must be checked against resolveRecipient(row), not the empty owner_email column (F5)", got)
	}
}

// TestEventForwarder_MissingRenderer_LoudErrorDropAndAdvance is the F4
// regression test (BugBash 2026-05-20 reshape).
//
// BUG (pre-2026-05-19): a kind registered in eventEmailBuilders but
// missing from eventEmailBodyRenderers fell through to the dead Brevo
// dashboard-template path → SkippedNoTemplate → cursor advanced silently
// → zero email, zero error, audit row consumed forever.
//
// EARLIER FIX (2026-05-19): made a missing renderer a loud ERROR but
// HELD the cursor (would pin the queue behind a programming bug until
// the registry was repaired).
//
// CURRENT FIX (2026-05-20, F4 reshape): a missing renderer is now
// (a) a loud ERROR with the literal message "missing_email_renderer",
// (b) increments metrics.EmailMissingRendererTotal{kind},
// (c) inserts a forwarder_sent row with classification='permanent_drop'
//     / provider='none' / provider_id='missing_renderer' so support can
//     grep the ledger, and
// (d) ADVANCES the cursor — the row will never produce an email so
//     pinning the queue is worse than dropping it. The CI registry test
//     TestEventEmail_EverySupportedKindFullyWired catches the half-
//     registration at gate time; this is the runtime backstop.
//
// This test feeds the worker an orphan kind (builder, no renderer) and
// asserts: provider is NOT called, cursor IS advanced, and the
// fakeLedger sees one permanent_drop claim for the orphan row.
func TestEventForwarder_MissingRenderer_LoudErrorDropAndAdvance(t *testing.T) {
	const orphanKind = "test.builder_without_renderer"
	// Register a builder but deliberately NO renderer; clean up after.
	eventEmailBuilders[orphanKind] = func(row auditRow) (map[string]string, bool) {
		return map[string]string{"x": "y"}, true
	}
	supportedAuditKinds = append(supportedAuditKinds, orphanKind)
	t.Cleanup(func() {
		delete(eventEmailBuilders, orphanKind)
		supportedAuditKinds = supportedAuditKinds[:len(supportedAuditKinds)-1]
	})

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Now().UTC().Add(-time.Hour)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("orphan-id", "team-1", orphanKind, "", "no renderer",
				[]byte(`{}`), createdAt, "owner@example.com"))

	provider := &fakeProvider{
		sendFn: func(_ context.Context, _ email.EventEmail) error {
			t.Errorf("SendEvent called for a kind with no renderer — should have dropped before send")
			return nil
		},
	}
	cursor := &memCursor{}
	ledger := newFakeLedger()
	w := newEventEmailForwarderWorkerForTest(db, cursor, provider)
	w.ledger = ledger
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if cursor.c.ID != "orphan-id" {
		t.Errorf("cursor.ID = %q after missing-renderer drop; want orphan-id (cursor MUST advance — the row can never produce an email so pinning the queue is wrong) (F4)", cursor.c.ID)
	}
	if got := provider.callCount(); got != 0 {
		t.Errorf("SendEvent called %d times for a renderer-less kind; want 0", got)
	}
	if ledger.markCalls != 1 {
		t.Errorf("ledger.markSent called %d times; want 1 (the F4 path must write a permanent_drop row)", ledger.markCalls)
	}
	if ledger.lastClaim.Classification != ledgerClassPermanentDrop {
		t.Errorf("ledger.lastClaim.Classification = %q; want %q (F4 must classify as permanent_drop so support can grep)", ledger.lastClaim.Classification, ledgerClassPermanentDrop)
	}
	if ledger.lastClaim.Provider != providerNoneMissingRenderer {
		t.Errorf("ledger.lastClaim.Provider = %q; want %q (F4 path didn't call a provider)", ledger.lastClaim.Provider, providerNoneMissingRenderer)
	}
	if ledger.lastClaim.ProviderID != providerIDMissingRenderer {
		t.Errorf("ledger.lastClaim.ProviderID = %q; want %q (F4 sentinel)", ledger.lastClaim.ProviderID, providerIDMissingRenderer)
	}
	if ledger.lastClaim.TemplateKind != orphanKind {
		t.Errorf("ledger.lastClaim.TemplateKind = %q; want %q (the kind that hit the F4 path)", ledger.lastClaim.TemplateKind, orphanKind)
	}
}

// TestEventForwarder_IdleTick_NoInfoLog is the log-noise (P1-1)
// regression test.
//
// BUG: an idle forwarder tick (zero new rows) emitted the
// jobs.event_email_forwarder.no_new_rows line at INFO every 60s — pure
// heartbeat noise. With the fix it logs at DEBUG. This test installs a
// slog handler that records every record at INFO level or above and
// asserts an idle Work() produces NO such record from the forwarder.
func TestEventForwarder_IdleTick_NoInfoLog(t *testing.T) {
	rec := &levelRecorder{minLevel: slog.LevelInfo}
	prev := slog.Default()
	slog.SetDefault(slog.New(rec))
	t.Cleanup(func() { slog.SetDefault(prev) })

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols)) // zero rows → idle tick

	w := newEventEmailForwarderWorkerForTest(db, &memCursor{}, &fakeProvider{})
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	for _, msg := range rec.messages() {
		if msg == "jobs.event_email_forwarder.no_new_rows" {
			t.Errorf("idle tick emitted %q at INFO+ — idle ticks must log at DEBUG (P1-1 log noise)", msg)
		}
	}
}

// levelRecorder is a minimal slog.Handler that records the message of
// every record at or above minLevel. Used by TestEventForwarder_IdleTick.
type levelRecorder struct {
	mu       sync.Mutex
	minLevel slog.Level
	msgs     []string
}

func (r *levelRecorder) Enabled(_ context.Context, l slog.Level) bool { return l >= r.minLevel }
func (r *levelRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, rec.Message)
	return nil
}
func (r *levelRecorder) WithAttrs(_ []slog.Attr) slog.Handler { return r }
func (r *levelRecorder) WithGroup(_ string) slog.Handler      { return r }
func (r *levelRecorder) messages() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.msgs))
	copy(out, r.msgs)
	return out
}

// TestEventForwarder_BuilderSkippedRow_NotWarn is the P2-1 regression
// test (BugBash 2026-05-19).
//
// BUG: a row with no resolvable owner email (an expected, benign state
// for deleted / orphan / test teams) logged builder_skipped_row at WARN.
// A steady trickle of orphan rows erodes WARN's signal value. The fix
// demotes it to INFO. This test installs a slog handler recording only
// WARN+ records and asserts an owner-less row produces NO WARN-level
// builder_skipped_row line — while the cursor still advances (unchanged).
func TestEventForwarder_BuilderSkippedRow_NotWarn(t *testing.T) {
	rec := &levelRecorder{minLevel: slog.LevelWarn}
	prev := slog.Default()
	slog.SetDefault(slog.New(rec))
	t.Cleanup(func() { slog.SetDefault(prev) })

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	createdAt := time.Now().UTC().Add(-time.Hour)
	// owner_email empty AND no metadata.email → builder returns ok=false.
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow("orphan-team-row", "team-deleted", auditKindOnboardingClaimed, "",
				"claim", []byte(`{}`), createdAt, ""))

	cursor := &memCursor{}
	w := newEventEmailForwarderWorkerForTest(db, cursor, &fakeProvider{})
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	for _, msg := range rec.messages() {
		if msg == "jobs.event_email_forwarder.builder_skipped_row" {
			t.Errorf("builder_skipped_row logged at WARN+ for an expected no-owner-email row — must be INFO (P2-1)")
		}
	}
	// Cursor-advance behavior is unchanged — the orphan row is consumed.
	if cursor.c.ID != "orphan-team-row" {
		t.Errorf("cursor.ID = %q; want orphan-team-row — P2-1 keeps the cursor-advance behavior", cursor.c.ID)
	}
}

// TestEventForwarder_CursorConstants_Pinned pins the absolute values of the
// two cursor-defense constants so a future tightening/loosening is caught
// at review time, not in production. P1-2 follow-up (Wave 3, 2026-05-21).
//
// Rationale: TestEventForwarder_FetchBatch_HasAgeFloor and
// TestEventForwarder_MissingCursor_SeedsToNow already prove the BEHAVIOR is
// wired end-to-end. This test pins the SHAPE of the defense — a future PR
// that quietly drops eventEmailMaxAge to (say) 5 minutes (silently denies a
// real backlog drain) or balloons eventEmailCursorSeedGrace to 24h (mass-
// spams a wider window after a Redis wipe) trips this pinning test and
// forces an explicit conversation.
func TestEventForwarder_CursorConstants_Pinned(t *testing.T) {
	if eventEmailMaxAge != 48*time.Hour {
		t.Errorf("eventEmailMaxAge = %v; want 48h — see jobs.event_email_forwarder docstring before changing (P1-2)", eventEmailMaxAge)
	}
	if eventEmailCursorSeedGrace != 5*time.Minute {
		t.Errorf("eventEmailCursorSeedGrace = %v; want 5m — cursor-loss replay window (P1-2)", eventEmailCursorSeedGrace)
	}
	// Defense-in-depth invariant: the seed grace MUST be strictly smaller
	// than the absolute age floor. If grace ever exceeds the floor, a seeded
	// cursor could outscan the floor and the 48h cap stops protecting against
	// a Redis wipe.
	if eventEmailCursorSeedGrace >= eventEmailMaxAge {
		t.Fatalf("invariant violated: eventEmailCursorSeedGrace (%v) >= eventEmailMaxAge (%v); seed grace must replay strictly LESS than the absolute floor",
			eventEmailCursorSeedGrace, eventEmailMaxAge)
	}
}

// TestForwarderSent_AuditID_AlwaysUUID is the registry-iterating coverage
// test for the api migration 063 contract: forwarder_sent.audit_id must
// always be the canonical audit_log UUID (matching the soft-FK pattern),
// never the "audit-<id>" provider-idempotency-key prefix.
//
// Why this matters: a synthetic "audit-..." value in audit_id would break
// the soft FK and orphan the row from audit_log. The api migration 063
// adds a partial index on audit_id with a regex matching UUID format —
// any row that fails it is silently un-indexed (and un-findable from the
// receiver-side webhook). Worker side: every markSent call site MUST
// feed the canonical UUID.
//
// This test drives the only worker code path that synthesizes a
// non-UUID-looking ProviderID (the F4 missing_renderer path) and asserts
// that even there the AuditID column is still a UUID, and that
// ProviderID is "missing_renderer" — i.e. that the two fields don't
// leak into each other.
//
// Coverage block (CLAUDE.md rule 17):
//   Symptom:        non-UUID writes orphan rows from migration 063's partial-FK index
//   Enumeration:    rg -n 'AuditID:\\s*' internal/jobs/event_email_forwarder.go
//   Sites found:    3 markSent call sites (success, missing-renderer F4, permanent-drop)
//   Sites touched:  0 (all 3 already pass row.ID = audit_log.id::text)
//   Coverage test:  TestForwarderSent_AuditID_AlwaysUUID + TestEventForwarder_MissingRenderer_LoudErrorDropAndAdvance
//   Live verified:  deferred to deploy.yml verify-live step
func TestForwarderSent_AuditID_AlwaysUUID(t *testing.T) {
	// Drive the F4 path — the highest-risk call site because it ALSO writes
	// a synthetic value (providerIDMissingRenderer). If any field-mixup ever
	// occurred, this is where it would surface.
	const orphanKind = "test.builder_without_renderer_for_uuid_audit"
	eventEmailBuilders[orphanKind] = func(row auditRow) (map[string]string, bool) {
		return map[string]string{"x": "y"}, true
	}
	supportedAuditKinds = append(supportedAuditKinds, orphanKind)
	t.Cleanup(func() {
		delete(eventEmailBuilders, orphanKind)
		supportedAuditKinds = supportedAuditKinds[:len(supportedAuditKinds)-1]
	})

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Real-shaped UUID. Migration 063's partial-index regex MUST accept this.
	const realAuditUUID = "a1b2c3d4-e5f6-4789-9abc-def012345678"

	createdAt := time.Now().UTC().Add(-time.Hour)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(sqlmock.NewRows(auditRowsCols).
			AddRow(realAuditUUID, "team-1", orphanKind, "", "no renderer",
				[]byte(`{}`), createdAt, "owner@example.com"))

	cursor := &memCursor{}
	ledger := newFakeLedger()
	w := newEventEmailForwarderWorkerForTest(db, cursor, &fakeProvider{})
	w.ledger = ledger
	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}

	// Iterate every claim the ledger saw — assert AuditID is UUID-shaped
	// everywhere. The CURRENT code only writes one claim per Work() run
	// per row, but iterating all of ledger.claims is the registry-iterating
	// shape that CLAUDE.md rule 18 + the multi-path-coverage rules require:
	// future call sites added without rule 17 enumeration will surface
	// here automatically.
	if len(ledger.claims) == 0 {
		t.Fatalf("ledger.claims was empty — no markSent invocation reached the fake (this test pre-supposes the F4 path fires)")
	}
	uuidRE := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	for k, claim := range ledger.claims {
		if !uuidRE.MatchString(claim.AuditID) {
			t.Errorf("ledger.claims[%q].AuditID = %q; want UUID shape (migration 063 soft-FK contract). "+
				"Worker MUST pass audit_log.id::text (UUID), never the audit-<id> idempotency prefix.",
				k, claim.AuditID)
		}
		// Also pin: the AuditID column is NOT polluted with the
		// "audit-<id>" prefix even on the F4 path. The F4 path's only
		// permitted synthetic value is ProviderID="missing_renderer".
		if strings.HasPrefix(claim.AuditID, eventEmailIdempotencyPrefix) {
			t.Errorf("ledger.claims[%q].AuditID = %q starts with %q — that prefix is for the provider IdempotencyKey ONLY, NEVER for forwarder_sent.audit_id (would break migration 063 soft-FK)",
				k, claim.AuditID, eventEmailIdempotencyPrefix)
		}
	}
}
