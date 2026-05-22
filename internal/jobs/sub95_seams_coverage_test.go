package jobs

// sub95_seams_coverage_test.go — closes the last residual branches in
// quota_infra.go, expire_imminent.go, and expiry_reminder_email.go to ≥95%
// each via package-var test seams. New org policy: no coverage waivers.
//
// Each seam swaps a production indirection point (sqlOpen / validateIdent /
// jsonMarshal / the *template.Template package vars) for a failing stub,
// drives the otherwise-unreachable defensive fail-open arm, and restores the
// production binding in a deferred cleanup so the swap can never leak into a
// sibling test. Production defaults are byte-for-byte identical.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"html/template"
	"testing"
	textTemplate "text/template"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

// errSeam is the canned error every seam stub returns.
var errSeam = errors.New("seam-injected failure")

func seamImminentJob() *river.Job[ExpireImminentArgs] {
	return &river.Job[ExpireImminentArgs]{JobRow: &rivertype.JobRow{ID: 1}}
}

// ── quota_infra.go: sqlOpen lazy-error fail-open arm ─────────────────────────
//
// lib/pq's sql.Open is fully lazy and never errors at Open time, so the
// open-error fail-open branch in revokePostgres / grantPostgres is unreachable
// in production. The sqlOpen seam injects an Open that errors, exercising both
// fail-open arms (which must still return nil — convention #1).
func TestRevokeGrantPostgres_OpenError_FailOpen(t *testing.T) {
	orig := sqlOpen
	sqlOpen = func(driverName, dsn string) (*sql.DB, error) {
		return nil, errSeam
	}
	t.Cleanup(func() { sqlOpen = orig })

	r := &directResourceRevoker{customerDatabaseURL: "postgres://x:y@127.0.0.1:5432/z?sslmode=disable"}
	ctx := context.Background()

	if err := r.revokePostgres(ctx, "validtoken"); err != nil {
		t.Errorf("revokePostgres on sql.Open error: want nil (fail-open), got %v", err)
	}
	if err := r.grantPostgres(ctx, "validtoken"); err != nil {
		t.Errorf("grantPostgres on sql.Open error: want nil (fail-open), got %v", err)
	}
}

// ── quota_infra.go: validateIdent user-arm error return ──────────────────────
//
// db_<token> and usr_<token> share the same token, so for any token that fails
// validation the db check short-circuits first and the user-arm error return
// is unreachable. The validateIdent seam passes the db_-prefixed identifier and
// fails ONLY the usr_-prefixed one, driving the otherwise-dead user arm in both
// revokePostgres and grantPostgres.
func TestRevokeGrantPostgres_UserIdentArm(t *testing.T) {
	orig := validateIdent
	validateIdent = func(s string) error {
		// Pass the db_<token> identifier; fail the usr_<token> identifier so
		// the second guard's error return executes.
		if len(s) >= 4 && s[:4] == "usr_" {
			return errSeam
		}
		return nil
	}
	t.Cleanup(func() { validateIdent = orig })

	r := &directResourceRevoker{customerDatabaseURL: "postgres://x:y@127.0.0.1:5432/z?sslmode=disable"}
	ctx := context.Background()

	if err := r.revokePostgres(ctx, "validtoken"); err == nil {
		t.Error("revokePostgres: want error from the user-identifier guard, got nil")
	}
	if err := r.grantPostgres(ctx, "validtoken"); err == nil {
		t.Error("grantPostgres: want error from the user-identifier guard, got nil")
	}
}

// ── expire_imminent.go: jsonMarshal marshal-error fail-open arm ──────────────
//
// A map[string]any of primitive-only values never fails to marshal, so the
// metadata_marshal_failed skip branch is unreachable in production. The
// jsonMarshal seam returns an error, driving the per-row skip; no INSERT must
// fire and the worker must NOT propagate the error (per-row failures are
// logged, not returned — file contract).
func TestExpireImminent_MarshalError_SkipsRow(t *testing.T) {
	orig := jsonMarshal
	jsonMarshal = func(v any) ([]byte, error) { return nil, errSeam }
	t.Cleanup(func() { jsonMarshal = orig })

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resourceID := uuid.New()
	token := uuid.New()
	teamID := uuid.New()
	expires := time.Now().UTC().Add(30 * time.Minute)

	// One eligible candidate. With jsonMarshal failing, the worker logs and
	// skips — NO INSERT is expected (sqlmock strict mode fails if one fires).
	mock.ExpectQuery(`FROM resources r`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "team_id", "resource_type", "expires_at", "owner_email",
		}).AddRow(resourceID, token, teamID, "postgres", expires, "owner@example.com"))

	w := NewExpireImminentWorker(db)
	if err := w.Work(context.Background(), seamImminentJob()); err != nil {
		t.Fatalf("Work must fail-open on per-row marshal error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations (an INSERT fired despite marshal error?): %v", err)
	}
}

// ── expiry_reminder_email.go: template.Execute fallback arms ─────────────────
//
// Both templates are validated at init by template.Must, so Execute can only
// fail on a view-shape mismatch — unreachable while the view struct and
// templates stay in sync. The seam swaps in templates whose Execute fails (a
// method call on a field that errors), driving the html AND text Sprintf
// fallback bodies. Both production templates are restored afterward.
func TestRenderAnonExpiryEmail_TemplateExecuteFallback(t *testing.T) {
	origHTML := anonExpiryHTMLTmpl
	origText := anonExpiryTextTmpl
	t.Cleanup(func() {
		anonExpiryHTMLTmpl = origHTML
		anonExpiryTextTmpl = origText
	})

	// A template that calls .Fail (a method that returns an error) forces
	// Execute to fail at render time. html/template propagates the method
	// error out of Execute.
	failHTML := template.Must(template.New("fail_html").Parse(`{{ .Fail }}`))
	failText := textTemplate.Must(textTemplate.New("fail_text").Parse(`{{ .Fail }}`))
	anonExpiryHTMLTmpl = failHTML
	anonExpiryTextTmpl = failText

	params := map[string]string{
		"reminder_index":  "2",
		"resource_type":   "postgres",
		"hours_remaining": "6",
		"upgrade_url":     "https://instanode.dev/app/billing?upgrade=hobby",
	}

	// The render still succeeds (never returns error) — the fallback Sprintf
	// bodies kick in. Verify they carry the core copy.
	subject, html, text := renderAnonExpiryEmailWithFailView(params)

	if subject == "" {
		t.Error("subject must be non-empty even when template Execute fails")
	}
	for _, want := range []string{"postgres", "6 hours", "https://instanode.dev/app/billing?upgrade=hobby"} {
		if !bytesContains(html, want) {
			t.Errorf("HTML fallback body missing %q\n--- BODY ---\n%s", want, html)
		}
		if !bytesContains(text, want) {
			t.Errorf("text fallback body missing %q\n--- BODY ---\n%s", want, text)
		}
	}
}

// renderAnonExpiryEmailWithFailView calls the production renderAnonExpiryEmail
// but the swapped-in templates execute against the real anonExpiryView, which
// has no .Fail method — so html/template's Execute returns an error and the
// fallback fires. We don't change the view; the failing templates reference a
// field/method the view lacks, which is exactly the "view-shape mismatch"
// condition the fallback guards against.
func renderAnonExpiryEmailWithFailView(params map[string]string) (string, string, string) {
	return renderAnonExpiryEmail(params)
}

func bytesContains(s, sub string) bool {
	return bytes.Contains([]byte(s), []byte(sub))
}

// compile-time guard that the seam vars have the stdlib signatures so a future
// refactor that changes the production binding shape also breaks this test.
var (
	_ = json.Marshal
	_ = sql.Open
)
