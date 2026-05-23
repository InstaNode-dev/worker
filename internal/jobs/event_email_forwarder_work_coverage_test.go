package jobs

// event_email_forwarder_work_coverage_test.go — Work()-path coverage under the
// `TestForwarder` name prefix so the standard email-coverage gate
//
//	go test ./internal/jobs -run 'TestEventEmail|TestLifecycle|TestForwarder' ...
//
// reaches event_email_forwarder.go ≥95% on its own, without depending on the
// `TestEventForwarder`-prefixed headline tests (which that gate's regex does
// not match). Drives the main Work() branches plus the noopSentLedger seam.
//
// Hermetic — sqlmock for audit_log + memCursor for the watermark. No Docker,
// no live network.

import (
	"context"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"instant.dev/worker/internal/email"
)

// newWorkMock builds a forwarder wired to sqlmock'd audit_log returning the
// given rows, an in-memory cursor, and the supplied provider.
func newWorkMock(t *testing.T, provider email.EmailProvider, rows *sqlmock.Rows) (*EventEmailForwarderWorker, *memCursor) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).WillReturnRows(rows)
	cursor := &memCursor{}
	return newEventEmailForwarderWorkerForTest(db, cursor, provider), cursor
}

func oneAuditRow(id, kind, meta, email string, at time.Time) *sqlmock.Rows {
	return sqlmock.NewRows(auditRowsCols).
		AddRow(id, "team-1", kind, "", "summary", []byte(meta), at, email)
}

// ── Work happy path: supported kind sends + cursor advances ────────────────

func TestForwarder_Work_SupportedKind_SendsAndAdvances(t *testing.T) {
	at := time.Date(2026, 5, 22, 8, 0, 0, 0, time.UTC)
	prov := &fakeProvider{}
	w, cursor := newWorkMock(t, prov, oneAuditRow("a-1", auditKindOnboardingClaimed, `{"signup_source":"github"}`, "owner@example.com", at))

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 1 {
		t.Errorf("expected 1 send, got %d", prov.callCount())
	}
	if cursor.c.ID != "a-1" {
		t.Errorf("cursor must advance past sent row; got %+v", cursor.c)
	}
}

// ── Work: empty batch returns nil, no send ─────────────────────────────────

func TestForwarder_Work_NoRows_NoSend(t *testing.T) {
	prov := &fakeProvider{}
	w, cursor := newWorkMock(t, prov, sqlmock.NewRows(auditRowsCols))

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 0 {
		t.Errorf("no rows must yield no send")
	}
	if !cursor.c.zero() {
		t.Errorf("cursor must stay zero on empty batch")
	}
}

// ── Work: missing cursor seeds to now()-grace (P1-2) ───────────────────────

func TestForwarder_Work_MissingCursor_SeedsAndAdvances(t *testing.T) {
	at := time.Now().UTC().Add(-time.Minute)
	prov := &fakeProvider{}
	w, cursor := newWorkMock(t, prov, oneAuditRow("a-seed", auditKindOnboardingClaimed, `{"signup_source":"x"}`, "owner@example.com", at))
	cursor.missing = true // force the seed branch

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if cursor.missing {
		t.Errorf("cursor must no longer be missing after a write")
	}
}

// ── Work: builder produces no payload → skip + advance ─────────────────────

func TestForwarder_Work_NoRecipient_SkipsAndAdvances(t *testing.T) {
	at := time.Date(2026, 5, 22, 9, 0, 0, 0, time.UTC)
	prov := &fakeProvider{}
	// onboarding.claimed with an empty owner_email and no metadata.email →
	// resolveRecipient yields "" → builder returns payloadOK=false.
	w, cursor := newWorkMock(t, prov, oneAuditRow("a-norcpt", auditKindOnboardingClaimed, `{}`, "", at))

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 0 {
		t.Errorf("no-recipient row must not send")
	}
	if cursor.c.ID != "a-norcpt" {
		t.Errorf("no-recipient row must advance cursor; got %+v", cursor.c)
	}
}

// ── Work: provider send returns a transient (5xx-ish) error → holds cursor ─

func TestForwarder_Work_TransientSendError_HoldsCursor(t *testing.T) {
	at := time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
	prov := &fakeProvider{sendFn: func(context.Context, email.EventEmail) error {
		return &email.SendError{Class: email.SendClassTransient, Message: "503"}
	}}
	w, cursor := newWorkMock(t, prov, oneAuditRow("a-5xx", auditKindOnboardingClaimed, `{"signup_source":"x"}`, "owner@example.com", at))

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work returns nil even on transient (per-row logged): %v", err)
	}
	if !cursor.c.zero() {
		t.Errorf("transient send error must NOT advance cursor; got %+v", cursor.c)
	}
}

// ── Work: suppressed recipient → skip send, advance cursor ─────────────────

func TestForwarder_Work_SuppressedRecipient_SkipsAndAdvances(t *testing.T) {
	at := time.Date(2026, 5, 22, 11, 0, 0, 0, time.UTC)
	prov := &fakeProvider{}
	w, cursor := newWorkMock(t, prov, oneAuditRow("a-supp", auditKindOnboardingClaimed, `{"signup_source":"x"}`, "owner@example.com", at))
	w.suppression = suppressAll{}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 0 {
		t.Errorf("suppressed recipient must not receive a send")
	}
	if cursor.c.ID != "a-supp" {
		t.Errorf("suppressed row must advance cursor; got %+v", cursor.c)
	}
}

// suppressAll reports every recipient as suppressed.
type suppressAll struct{}

func (suppressAll) hasSuppression(context.Context, string) (bool, error) { return true, nil }

// ── noopSentLedger.isSent / markSent (the always-claim stub) ───────────────

func TestForwarder_NoopSentLedger_IsSentAndMarkSent(t *testing.T) {
	l := noopSentLedger{}
	sent, err := l.isSent(context.Background(), "any")
	if err != nil || sent {
		t.Errorf("noop isSent = (%v,%v); want (false,nil)", sent, err)
	}
	claimed, err := l.markSent(context.Background(), ledgerClaim{AuditID: "x"})
	if err != nil || !claimed {
		t.Errorf("noop markSent = (%v,%v); want (true,nil)", claimed, err)
	}
}

// ── Work: cursor read error propagates as a retryable job error ────────────

func TestForwarder_Work_CursorReadError_ReturnsError(t *testing.T) {
	db, _, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	w := newEventEmailForwarderWorkerForTest(db, &errReadCursor{}, &fakeProvider{})

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err == nil {
		t.Fatal("expected Work to return error on cursor read failure")
	}
}

// errReadCursor fails on read so Work's first error branch is exercised.
type errReadCursor struct{}

func (errReadCursor) read(context.Context) (eventCursor, bool, error) {
	return eventCursor{}, false, fmt.Errorf("redis down")
}
func (errReadCursor) write(context.Context, eventCursor) error { return nil }

// ── Work: fetchBatch error propagates as a retryable job error ─────────────

func TestForwarder_Work_FetchBatchError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).WillReturnError(fmt.Errorf("audit_log query exploded"))
	w := newEventEmailForwarderWorkerForTest(db, &memCursor{}, &fakeProvider{})

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err == nil {
		t.Fatal("expected Work to return error on fetchBatch failure")
	}
}

// ── Work: permanent send error → ledger permanent_drop + advance ───────────

func TestForwarder_Work_PermanentSendError_AdvancesCursor(t *testing.T) {
	at := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	prov := &fakeProvider{sendFn: func(context.Context, email.EventEmail) error {
		return &email.SendError{Class: email.SendClassPermanent, Message: "422 invalid recipient"}
	}}
	w, cursor := newWorkMock(t, prov, oneAuditRow("a-perm", auditKindOnboardingClaimed, `{"signup_source":"x"}`, "owner@example.com", at))
	led := &failingLedger{markClaimed: true}
	w.ledger = led

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 1 {
		t.Errorf("permanent class still attempts the send once; got %d", prov.callCount())
	}
	if cursor.c.ID != "a-perm" {
		t.Errorf("permanent send error must advance cursor; got %+v", cursor.c)
	}
	if led.markCalls == 0 {
		t.Errorf("permanent class must claim a permanent_drop ledger row")
	}
}

// ── Work: ledger probe reports already-sent → skip send, advance ───────────

func TestForwarder_Work_LedgerAlreadySent_SkipsAndAdvances(t *testing.T) {
	at := time.Date(2026, 5, 22, 13, 0, 0, 0, time.UTC)
	prov := &fakeProvider{}
	w, cursor := newWorkMock(t, prov, oneAuditRow("a-dup", auditKindOnboardingClaimed, `{"signup_source":"x"}`, "owner@example.com", at))
	w.ledger = &failingLedger{isSentResp: true}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 0 {
		t.Errorf("already-sent row must not re-send")
	}
	if cursor.c.ID != "a-dup" {
		t.Errorf("already-sent row must advance cursor; got %+v", cursor.c)
	}
}

// ── Work: ledger probe error → hold cursor, no send ────────────────────────

func TestForwarder_Work_LedgerProbeError_HoldsCursor(t *testing.T) {
	at := time.Date(2026, 5, 22, 14, 0, 0, 0, time.UTC)
	prov := &fakeProvider{}
	w, cursor := newWorkMock(t, prov, oneAuditRow("a-probe", auditKindOnboardingClaimed, `{"signup_source":"x"}`, "owner@example.com", at))
	w.ledger = &failingLedger{isSentErr: fmt.Errorf("ledger probe boom")}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 0 {
		t.Errorf("probe error must not send")
	}
	if !cursor.c.zero() {
		t.Errorf("probe error must hold cursor; got %+v", cursor.c)
	}
}

// ── Work: send OK but ledger claim fails post-2xx → hold cursor ────────────

func TestForwarder_Work_LedgerClaimFailsPostSend_HoldsCursor(t *testing.T) {
	at := time.Date(2026, 5, 22, 15, 0, 0, 0, time.UTC)
	prov := &fakeProvider{messageID: "msg-xyz"}
	w, cursor := newWorkMock(t, prov, oneAuditRow("a-claimfail", auditKindOnboardingClaimed, `{"signup_source":"x"}`, "owner@example.com", at))
	w.ledger = &failingLedger{markErr: fmt.Errorf("insert forwarder_sent failed")}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 1 {
		t.Errorf("send must have been attempted")
	}
	if !cursor.c.zero() {
		t.Errorf("ledger claim failure post-send must hold cursor; got %+v", cursor.c)
	}
}

// ── Work: missing renderer (F4) → permanent_drop ledger + advance ──────────
//
// Drives the no-renderer backstop by registering a kind that has a builder
// but no body renderer for the duration of the test, then restoring both
// maps. Exercises the metrics + ledger + cursor-advance branch.

func TestForwarder_Work_MissingRenderer_AdvancesCursor(t *testing.T) {
	const k = "test.synthetic.no_renderer_kind"
	// Register a builder but deliberately NO renderer for k.
	prevBuilder, hadBuilder := eventEmailBuilders[k]
	eventEmailBuilders[k] = func(auditRow) (map[string]string, bool) {
		return map[string]string{"x": "1"}, true
	}
	t.Cleanup(func() {
		if hadBuilder {
			eventEmailBuilders[k] = prevBuilder
		} else {
			delete(eventEmailBuilders, k)
		}
	})

	at := time.Date(2026, 5, 22, 16, 0, 0, 0, time.UTC)
	prov := &fakeProvider{}
	w, cursor := newWorkMock(t, prov, oneAuditRow("a-norenderer", k, `{"x":"1"}`, "owner@example.com", at))
	led := &failingLedger{markClaimed: true}
	w.ledger = led

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 0 {
		t.Errorf("missing-renderer kind must not send")
	}
	if cursor.c.ID != "a-norenderer" {
		t.Errorf("missing-renderer row must advance cursor; got %+v", cursor.c)
	}
	if led.markCalls == 0 {
		t.Errorf("missing-renderer must write a permanent_drop ledger row")
	}
}

// ── Work: unmapped kind (no builder) → skip + advance ──────────────────────

func TestForwarder_Work_NoBuilder_SkipsAndAdvances(t *testing.T) {
	at := time.Date(2026, 5, 22, 17, 0, 0, 0, time.UTC)
	prov := &fakeProvider{}
	// sqlmock returns a kind absent from eventEmailBuilders — exercises the
	// no_builder_for_kind backstop (the SQL ANY($1) filter is bypassed here
	// because we control the mocked rows directly).
	w, cursor := newWorkMock(t, prov, oneAuditRow("a-nobuilder", "totally.unmapped.kind", `{}`, "owner@example.com", at))

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 0 {
		t.Errorf("unmapped kind must not send")
	}
	if cursor.c.ID != "a-nobuilder" {
		t.Errorf("unmapped kind must advance cursor; got %+v", cursor.c)
	}
}

// configSuppression returns the configured (suppressed, err) for every call.
type configSuppression struct {
	suppressed bool
	err        error
}

func (c configSuppression) hasSuppression(context.Context, string) (bool, error) {
	return c.suppressed, c.err
}

// ── Work: unsubscribe lookup error → fail-CLOSED, hold cursor, no send ─────

func TestForwarder_Work_UnsubscribeLookupError_FailsClosed(t *testing.T) {
	at := time.Date(2026, 5, 22, 18, 0, 0, 0, time.UTC)
	prov := &fakeProvider{}
	w, cursor := newWorkMock(t, prov, oneAuditRow("a-failclosed", auditKindOnboardingClaimed, `{"signup_source":"x"}`, "owner@example.com", at))
	w.suppression = configSuppression{err: errUnsubscribeLookupFailed}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 0 {
		t.Errorf("fail-closed must not send")
	}
	if !cursor.c.zero() {
		t.Errorf("fail-closed must NOT advance cursor; got %+v", cursor.c)
	}
}

// ── Work: bounce/spam lookup error → fail-OPEN, send proceeds + advances ───

func TestForwarder_Work_BounceLookupError_FailsOpen(t *testing.T) {
	at := time.Date(2026, 5, 22, 19, 0, 0, 0, time.UTC)
	prov := &fakeProvider{}
	w, cursor := newWorkMock(t, prov, oneAuditRow("a-failopen", auditKindOnboardingClaimed, `{"signup_source":"x"}`, "owner@example.com", at))
	// A non-sentinel suppression error → fail-open: treated as sendable.
	w.suppression = configSuppression{err: fmt.Errorf("bounce table brownout")}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 1 {
		t.Errorf("fail-open must still attempt the send; got %d", prov.callCount())
	}
	if cursor.c.ID != "a-failopen" {
		t.Errorf("fail-open send must advance cursor; got %+v", cursor.c)
	}
}

// ── Work: send OK but ledger claim race (!claimed) → still advances ────────

func TestForwarder_Work_LedgerClaimRace_StillAdvances(t *testing.T) {
	at := time.Date(2026, 5, 22, 20, 0, 0, 0, time.UTC)
	prov := &fakeProvider{messageID: "msg-race"}
	w, cursor := newWorkMock(t, prov, oneAuditRow("a-race", auditKindOnboardingClaimed, `{"signup_source":"x"}`, "owner@example.com", at))
	// markClaimed=false → another forwarder won the claim; we still advance.
	w.ledger = &failingLedger{markClaimed: false}

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if cursor.c.ID != "a-race" {
		t.Errorf("claim race must still advance cursor; got %+v", cursor.c)
	}
}

// errWriteCursor reads zero/non-missing but fails every write, so the
// cursor-advance error branches in Work() return a propagated job error.
type errWriteCursor struct{}

func (errWriteCursor) read(context.Context) (eventCursor, bool, error) {
	return eventCursor{}, false, nil
}
func (errWriteCursor) write(context.Context, eventCursor) error {
	return fmt.Errorf("redis SET failed")
}

// ── Work: cursor write error on a successful send → propagated job error ───

func TestForwarder_Work_CursorWriteError_OnSuccess_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	at := time.Date(2026, 5, 22, 21, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT[\s\S]+FROM audit_log`).
		WillReturnRows(oneAuditRow("a-cwerr", auditKindOnboardingClaimed, `{"signup_source":"x"}`, "owner@example.com", at))
	w := newEventEmailForwarderWorkerForTest(db, errWriteCursor{}, &fakeProvider{})

	if err := w.Work(context.Background(), fakeJobLocal[EventEmailForwarderArgs]()); err == nil {
		t.Fatal("expected cursor-write error to propagate as a job error")
	}
}
