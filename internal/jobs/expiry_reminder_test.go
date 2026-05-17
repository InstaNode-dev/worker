package jobs_test

// expiry_reminder_test.go — covers the 2026-05-15 multi-stage rework
// of ExpiryReminderWorker. Three reminders per resource (12h / 6h / 1h),
// dedupe enforced via CAS on resources.reminders_sent, audit metadata
// carries reminder_index + upgrade_url + resource_url + token_prefix
// for the Brevo template.

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"testing"
	"time"

	"database/sql/driver"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"

	"instant.dev/worker/internal/jobs"
)

func TestExpiryReminderWorker_NoCandidates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "team_id", "resource_type", "expires_at", "reminders_sent", "key_prefix", "email"})
	mock.ExpectQuery(`SELECT r.id, r.team_id, r.resource_type, r.expires_at,`).WillReturnRows(rows)

	w := jobs.NewExpiryReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestAnonExpiryReminder_Stage1_WritesAuditWithFullMetadata covers the
// first reminder: resource expires in ~10h, reminders_sent=0, fires
// stage 1 (12h bucket). Metadata MUST carry reminder_index="1",
// upgrade_url, resource_url, token_prefix, hours_remaining.
func TestAnonExpiryReminder_Stage1_WritesAuditWithFullMetadata(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(10 * time.Hour)

	rows := sqlmock.NewRows([]string{"id", "team_id", "resource_type", "expires_at", "reminders_sent", "key_prefix", "email"}).
		AddRow(resID, teamID, "postgres", expires, 0, "abc12345", "owner@example.com")
	mock.ExpectQuery(`SELECT r.id, r.team_id`).WillReturnRows(rows)

	// CAS-stamp: target reminders_sent=1, predicate reminders_sent=0
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE resources`)).
		WithArgs(1, resID, 0).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Audit insert must include the resource_type column AND a metadata
	// JSON blob carrying every template param. We validate the metadata
	// shape by capturing the bytes argument.
	var captured []byte
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "anon.expiry_warning", "postgres", sqlmock.AnyArg(),
			argCaptureBytes(&captured)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := jobs.NewExpiryReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}

	meta := map[string]string{}
	if err := json.Unmarshal(captured, &meta); err != nil {
		t.Fatalf("metadata not valid JSON: %v (raw=%q)", err, string(captured))
	}
	mustEqual(t, meta, "resource_id", resID.String())
	mustEqual(t, meta, "resource_type", "postgres")
	mustEqual(t, meta, "reminder_index", "1")
	mustEqual(t, meta, "stage_label", "stage_12h")
	mustEqual(t, meta, "token_prefix", "abc12345")
	mustEqual(t, meta, "email", "owner@example.com")

	// upgrade_url + resource_url should be HTTPS and contain the resource id
	mustContain(t, meta["upgrade_url"], "https://instanode.dev/app/billing")
	mustContain(t, meta["upgrade_url"], "stage_12h")
	mustContain(t, meta["resource_url"], resID.String())
	// hours_remaining must be a non-zero positive integer string
	hrs, err := strconv.Atoi(meta["hours_remaining"])
	if err != nil || hrs <= 0 {
		t.Errorf("hours_remaining=%q want positive int", meta["hours_remaining"])
	}
}

// TestAnonExpiryReminder_Stage2_FiresWhenStage1AlreadySent: a resource
// already at reminders_sent=1 and inside the 6h window fires stage 2.
func TestAnonExpiryReminder_Stage2_FiresWhenStage1AlreadySent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(4 * time.Hour) // inside the 6h bucket

	rows := sqlmock.NewRows([]string{"id", "team_id", "resource_type", "expires_at", "reminders_sent", "key_prefix", "email"}).
		AddRow(resID, teamID, "redis", expires, 1, "pfx9", "owner@example.com")
	mock.ExpectQuery(`SELECT r.id, r.team_id`).WillReturnRows(rows)

	// CAS to 2 from 1.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE resources`)).
		WithArgs(2, resID, 1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	var captured []byte
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "anon.expiry_warning", "redis", sqlmock.AnyArg(),
			argCaptureBytes(&captured)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := jobs.NewExpiryReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}

	meta := map[string]string{}
	if err := json.Unmarshal(captured, &meta); err != nil {
		t.Fatalf("metadata not valid JSON: %v", err)
	}
	mustEqual(t, meta, "reminder_index", "2")
	mustEqual(t, meta, "stage_label", "stage_6h")
}

// TestAnonExpiryReminder_Stage3_Final: reminders_sent=2 and <=1h remaining
// fires the final (3rd) reminder, advancing reminders_sent to 3. After
// this no further reminders ever fire for this resource because the
// SQL filter is reminders_sent < 3.
func TestAnonExpiryReminder_Stage3_Final(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(45 * time.Minute) // inside the 1h bucket

	rows := sqlmock.NewRows([]string{"id", "team_id", "resource_type", "expires_at", "reminders_sent", "key_prefix", "email"}).
		AddRow(resID, teamID, "mongodb", expires, 2, "xyz", "owner@example.com")
	mock.ExpectQuery(`SELECT r.id, r.team_id`).WillReturnRows(rows)

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE resources`)).
		WithArgs(3, resID, 2).
		WillReturnResult(sqlmock.NewResult(0, 1))

	var captured []byte
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "anon.expiry_warning", "mongodb", sqlmock.AnyArg(),
			argCaptureBytes(&captured)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := jobs.NewExpiryReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}

	meta := map[string]string{}
	if err := json.Unmarshal(captured, &meta); err != nil {
		t.Fatalf("metadata not valid JSON: %v", err)
	}
	mustEqual(t, meta, "reminder_index", "3")
	mustEqual(t, meta, "stage_label", "stage_1h")
	// Final-stage hours_remaining floors at 1 even when remaining is 45 min.
	mustEqual(t, meta, "hours_remaining", "1")
}

// TestAnonExpiryReminder_NotEligibleYet: reminders_sent=0 but the
// resource is still inside the 12h window — and exactly at the
// boundary. Eligible for stage 1. Then a row at reminders_sent=1
// inside (12h, 6h] is NOT yet eligible for stage 2 — must wait.
func TestAnonExpiryReminder_NotEligibleYet(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(8 * time.Hour) // (6h, 12h] bucket

	rows := sqlmock.NewRows([]string{"id", "team_id", "resource_type", "expires_at", "reminders_sent", "key_prefix", "email"}).
		AddRow(resID, teamID, "postgres", expires, 1, "p", "owner@example.com")
	mock.ExpectQuery(`SELECT r.id, r.team_id`).WillReturnRows(rows)
	// No CAS UPDATE and no audit INSERT expected — the row is awaiting
	// the 6h bucket. The next sweep ~30 min from now will keep checking.

	w := jobs.NewExpiryReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestExpiryReminderWorker_StampsButSkipsAudit_WhenNoOwnerEmail: orphan
// team with NULL email. We still advance reminders_sent so the row
// doesn't keep getting re-evaluated, but skip the audit insert because
// there's no recipient.
func TestExpiryReminderWorker_StampsButSkipsAudit_WhenNoOwnerEmail(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(10 * time.Hour)
	rows := sqlmock.NewRows([]string{"id", "team_id", "resource_type", "expires_at", "reminders_sent", "key_prefix", "email"}).
		AddRow(resID, teamID, "postgres", expires, 0, "p", nil)
	mock.ExpectQuery(`SELECT r.id, r.team_id`).WillReturnRows(rows)
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE resources`)).
		WithArgs(1, resID, 0).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := jobs.NewExpiryReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestExpiryReminderWorker_FailOpenOnAuditInsertError: audit_log INSERT
// errors are logged + skipped (the row is already stamped).
func TestExpiryReminderWorker_FailOpenOnAuditInsertError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(1 * time.Hour)
	rows := sqlmock.NewRows([]string{"id", "team_id", "resource_type", "expires_at", "reminders_sent", "key_prefix", "email"}).
		AddRow(resID, teamID, "mongodb", expires, 2, "p", "x@example.com")
	mock.ExpectQuery(`SELECT r.id`).WillReturnRows(rows)
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE resources`)).
		WithArgs(3, resID, 2).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnError(errDB)

	w := jobs.NewExpiryReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("expected nil (fail-open) on audit insert error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestExpiryReminderWorker_TopLevelQueryError_ReturnsError: SELECT failure
// must propagate so River retries.
func TestExpiryReminderWorker_TopLevelQueryError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT r.id`).WillReturnError(errDB)

	w := jobs.NewExpiryReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err == nil {
		t.Fatal("expected error from top-level query failure")
	}
}

// TestExpiryReminderWorker_JoinsOnlyPrimaryUser is the P6 coverage test
// (BUGHUNT-REPORT-2026-05-17-round2.md): the LEFT JOIN users MUST carry
// `AND u.is_primary = true`. Without it, a team with N members fans the join
// out to N candidate rows per resource — the anon.expiry_warning audit row is
// written N times (every teammate emailed) and the per-tick LIMIT 500 budget
// is consumed by duplicates.
//
// sqlmock matches the expected query as a regular expression against the SQL
// the worker actually issues, so a query missing the predicate fails
// ExpectationsWereMet. This test fails if the is_primary predicate is ever
// dropped from the join.
func TestExpiryReminderWorker_JoinsOnlyPrimaryUser(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// The expected-query regex REQUIRES the is_primary predicate to be present
	// in the JOIN clause. A query without it will not match → ExpectationsWereMet
	// fails the test.
	mock.ExpectQuery(`LEFT JOIN users u ON u\.team_id = r\.team_id AND u\.is_primary = true`).
		WillReturnRows(sqlmock.NewRows(
			[]string{"id", "team_id", "resource_type", "expires_at", "reminders_sent", "key_prefix", "email"}))

	w := jobs.NewExpiryReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("query did not include the `AND u.is_primary = true` join predicate: %v", err)
	}
}

// TestExpiryReminderWorker_CASLoses_NoAudit: another worker advanced
// reminders_sent between SELECT and UPDATE — RowsAffected=0 → skip
// without writing the audit row.
func TestExpiryReminderWorker_CASLoses_NoAudit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resID := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(10 * time.Hour)
	rows := sqlmock.NewRows([]string{"id", "team_id", "resource_type", "expires_at", "reminders_sent", "key_prefix", "email"}).
		AddRow(resID, teamID, "postgres", expires, 0, "p", "owner@example.com")
	mock.ExpectQuery(`SELECT r.id`).WillReturnRows(rows)
	// CAS UPDATE matches nothing (predicate failed because another worker advanced).
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE resources`)).
		WithArgs(1, resID, 0).
		WillReturnResult(sqlmock.NewResult(0, 0))
	// No INSERT INTO audit_log expected.

	w := jobs.NewExpiryReminderWorker(db)
	if err := w.Work(context.Background(), fakeJob[jobs.ExpiryReminderArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// helpers --------------------------------------------------------------

// argCaptureBytes returns a sqlmock argument matcher that copies the
// underlying []byte value into dst so the test can introspect it after
// the call.
func argCaptureBytes(dst *[]byte) sqlmock.Argument {
	return capturingArg{dst: dst}
}

type capturingArg struct {
	dst *[]byte
}

func (c capturingArg) Match(v driver.Value) bool {
	switch b := v.(type) {
	case []byte:
		*c.dst = append((*c.dst)[:0], b...)
		return true
	case string:
		*c.dst = []byte(b)
		return true
	default:
		return false
	}
}

func mustEqual(t *testing.T, m map[string]string, k, want string) {
	t.Helper()
	if got := m[k]; got != want {
		t.Errorf("meta[%q]=%q want %q", k, got, want)
	}
}

func mustContain(t *testing.T, s, substr string) {
	t.Helper()
	if !contains(s, substr) {
		t.Errorf("got %q, want it to contain %q", s, substr)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || indexOf(s, substr) >= 0)
}

func indexOf(s, substr string) int {
	return stringIndex(s, substr)
}

// stringIndex mirrors strings.Index without importing the package, kept
// inline so the helper file has no extra deps beyond what's already here.
func stringIndex(s, substr string) int {
	if substr == "" {
		return 0
	}
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// keep fmt import alive for any future formatting in helpers
var _ = fmt.Sprintf
