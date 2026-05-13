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
