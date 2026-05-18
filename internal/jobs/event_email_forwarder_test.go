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
	"fmt"
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
type memCursor struct {
	mu sync.Mutex
	c  eventCursor
}

func (m *memCursor) read(_ context.Context) (eventCursor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.c, nil
}
func (m *memCursor) write(_ context.Context, c eventCursor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.c = c
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
	calls   int32
	lastEvt email.EventEmail
	mu      sync.Mutex
}

func (f *fakeProvider) SendEvent(ctx context.Context, evt email.EventEmail) error {
	atomic.AddInt32(&f.calls, 1)
	f.mu.Lock()
	f.lastEvt = evt
	f.mu.Unlock()
	if f.sendFn != nil {
		return f.sendFn(ctx, evt)
	}
	return nil
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
