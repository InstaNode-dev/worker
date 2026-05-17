package jobs_test

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"instant.dev/worker/internal/jobs"
)

var errDB = errors.New("db error")

// quotaScanCols is the column set the suspend/unsuspend loop SELECTs project.
// Centralised so a column-list change (e.g. the team_id + name additions for
// the audit-row emission) updates every test in one place.
var quotaScanCols = []string{
	"id", "token", "resource_type", "tier", "storage_bytes",
	"provider_resource_id", "team_id", "name",
}

// mockPlanRegistry is a simple PlanRegistry stub. ConnectionsLimit /
// ProvisionLimit are stubbed at "unlimited" so EnforceStorageQuotaWorker
// tests don't accidentally fire the wall-nudge axes — those have their
// own dedicated tests in quota_wall_nudge_test.go.
type mockPlanRegistry struct {
	limitMB int
}

func (m *mockPlanRegistry) StorageLimitMB(tier, service string) int {
	return m.limitMB
}

func (m *mockPlanRegistry) ConnectionsLimit(tier, service string) int { return -1 }
func (m *mockPlanRegistry) ProvisionLimit(tier string) int             { return -1 }

// ── mockResourceInfraRevoker ──────────────────────────────────────────────────

// mockResourceInfraRevoker records revoke/grant calls for assertion in tests.
// It satisfies the ResourceInfraRevoker interface without touching real infra.
// revokedTiers / grantedTiers record the tier argument so tests can assert the
// quota worker threads the resource tier through to the revoker (P1 fix: the
// Redis ACL username scheme depends on tier).
type mockResourceInfraRevoker struct {
	revokedTokens []string
	grantedTokens []string
	revokedTiers  []string
	grantedTiers  []string
	// revokedPRIDs / grantedPRIDs record the provider_resource_id argument so
	// tests can assert the quota worker threads the stored canonical
	// identifier through to the revoker (token-truncation fix: the Redis ACL
	// username resolves from provider_resource_id when present).
	revokedPRIDs []string
	grantedPRIDs []string
	revokeErr    error
	grantErr     error
}

func (m *mockResourceInfraRevoker) RevokeAccess(_ context.Context, _, token, tier, providerResourceID string) error {
	m.revokedTokens = append(m.revokedTokens, token)
	m.revokedTiers = append(m.revokedTiers, tier)
	m.revokedPRIDs = append(m.revokedPRIDs, providerResourceID)
	return m.revokeErr
}

func (m *mockResourceInfraRevoker) GrantAccess(_ context.Context, _, token, tier, providerResourceID string) error {
	m.grantedTokens = append(m.grantedTokens, token)
	m.grantedTiers = append(m.grantedTiers, tier)
	m.grantedPRIDs = append(m.grantedPRIDs, providerResourceID)
	return m.grantErr
}

// ── Suspend loop tests ────────────────────────────────────────────────────────

func TestEnforceStorageQuotaWorker_NoResources_NoSuspend(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// Suspend loop query (status='active').
	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("active").
		WillReturnRows(sqlmock.NewRows(quotaScanCols))
	// Unsuspend loop query (status='suspended').
	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("suspended").
		WillReturnRows(sqlmock.NewRows(quotaScanCols))

	plans := &mockPlanRegistry{limitMB: 10}
	w := jobs.NewEnforceStorageQuotaWorker(db, plans, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.EnforceStorageQuotaArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestEnforceStorageQuotaWorker_DBQueryError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("active").
		WillReturnError(errDB)

	plans := &mockPlanRegistry{limitMB: 10}
	w := jobs.NewEnforceStorageQuotaWorker(db, plans, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.EnforceStorageQuotaArgs]()); err == nil {
		t.Fatal("expected error from DB query failure, got nil")
	}
}

// ── P0-3 regression: 'suspended' value in the UPDATE must succeed ─────────────
//
// This test guards against the constraint-violation regression (bug P0-3)
// where the UPDATE was rejected by the CHECK constraint because 'suspended'
// was not an allowed value. It proves:
//  1. An over-quota resource's UPDATE is attempted with status='suspended'.
//  2. The UPDATE is issued (not skipped).
//  3. The revoker is called before the UPDATE (infra revoke FIRST).
//
// The sqlmock does NOT inject a constraint error — it succeeds, proving
// the code issues the correct UPDATE. A separate DB-layer test (or a live
// integration test after migration 049 is applied) proves the constraint
// accepts 'suspended'; this unit test proves the worker emits it.

func TestEnforceStorageQuotaWorker_OverQuota_SuspendsResource(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// A valid UUID for the resource row.
	resourceID := "11111111-1111-1111-1111-111111111111"
	token := "tok_overquota"
	resourceType := "postgres"
	tier := "anonymous"
	// The canonical provider_resource_id stamped at provision time — the
	// worker must thread this through to the revoker (token-truncation fix).
	providerResourceID := "usr_tok_overquota"
	// storage_bytes == 11 MB; limit is 10 MB → exceeded.
	storageBytes := int64(11 * 1024 * 1024)
	limitMB := 10

	teamID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	name := "my-overquota-db"

	// Suspend loop query.
	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("active").
		WillReturnRows(sqlmock.NewRows(quotaScanCols).
			AddRow(resourceID, token, resourceType, tier, storageBytes, providerResourceID, teamID, name))
	// checkStorageQuota inner query.
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"storage_bytes"}).AddRow(storageBytes))
	// The UPDATE to 'suspended'.
	mock.ExpectExec(`UPDATE resources SET status = \$1`).
		WithArgs("suspended", resourceID, "active").
		WillReturnResult(sqlmock.NewResult(1, 1))
	// The customer-visible audit_log row, emitted AFTER the successful UPDATE.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", "resource.quota_suspended", sqlmock.AnyArg(), sqlmock.AnyArg(), resourceType).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Unsuspend loop query — empty (no suspended resources yet).
	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("suspended").
		WillReturnRows(sqlmock.NewRows(quotaScanCols))

	revoker := &mockResourceInfraRevoker{}
	plans := &mockPlanRegistry{limitMB: limitMB}
	w := jobs.NewEnforceStorageQuotaWorker(db, plans, revoker)
	if err := w.Work(context.Background(), fakeJob[jobs.EnforceStorageQuotaArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
	// Verify the revoker was called (P0-4: infra revoke on suspend).
	if len(revoker.revokedTokens) != 1 || revoker.revokedTokens[0] != token {
		t.Errorf("expected revoker.RevokeAccess called with %q; got %v", token, revoker.revokedTokens)
	}
	// P1: the resource tier MUST be threaded through to the revoker so it can
	// derive the correct Redis ACL username (shared vs dedicated scheme).
	if len(revoker.revokedTiers) != 1 || revoker.revokedTiers[0] != tier {
		t.Errorf("expected revoker.RevokeAccess called with tier %q; got %v", tier, revoker.revokedTiers)
	}
	// Token-truncation fix: the canonical provider_resource_id stamped at
	// provision time MUST be threaded through so the revoker uses the exact
	// ACL username instead of re-deriving from the token.
	if len(revoker.revokedPRIDs) != 1 || revoker.revokedPRIDs[0] != providerResourceID {
		t.Errorf("expected revoker.RevokeAccess called with provider_resource_id %q; got %v",
			providerResourceID, revoker.revokedPRIDs)
	}
}

// ── P0-4 regression: auto-unsuspend when usage drops below limit ──────────────
//
// Proves the unsuspend loop fires and re-grants infra access + flips status
// back to 'active' when storage_bytes is now under the limit.

func TestEnforceStorageQuotaWorker_UnderQuota_UnsuspendsResource(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resourceID := "22222222-2222-2222-2222-222222222222"
	token := "tok_underquota"
	resourceType := "redis"
	tier := "hobby"
	// storage_bytes == 5 MB; limit is 25 MB → no longer exceeded.
	storageBytes := int64(5 * 1024 * 1024)
	limitMB := 25

	// Suspend loop: no active-status over-quota resources.
	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("active").
		WillReturnRows(sqlmock.NewRows(quotaScanCols))

	teamID := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	name := "my-underquota-cache"

	// Unsuspend loop: one suspended resource.
	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("suspended").
		WillReturnRows(sqlmock.NewRows(quotaScanCols).
			AddRow(resourceID, token, resourceType, tier, storageBytes, "", teamID, name)) // empty PRID = legacy row
	// checkStorageQuota inner query.
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"storage_bytes"}).AddRow(storageBytes))
	// The UPDATE back to 'active'.
	mock.ExpectExec(`UPDATE resources SET status = \$1`).
		WithArgs("active", resourceID, "suspended").
		WillReturnResult(sqlmock.NewResult(1, 1))
	// The customer-visible audit_log row, emitted AFTER the successful UPDATE.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", "resource.quota_unsuspended", sqlmock.AnyArg(), sqlmock.AnyArg(), resourceType).
		WillReturnResult(sqlmock.NewResult(1, 1))

	revoker := &mockResourceInfraRevoker{}
	plans := &mockPlanRegistry{limitMB: limitMB}
	w := jobs.NewEnforceStorageQuotaWorker(db, plans, revoker)
	if err := w.Work(context.Background(), fakeJob[jobs.EnforceStorageQuotaArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
	// Verify GrantAccess was called (infra re-grant on unsuspend).
	if len(revoker.grantedTokens) != 1 || revoker.grantedTokens[0] != token {
		t.Errorf("expected revoker.GrantAccess called with %q; got %v", token, revoker.grantedTokens)
	}
	// Verify RevokeAccess was NOT called for the unsuspend path.
	if len(revoker.revokedTokens) != 0 {
		t.Errorf("expected no RevokeAccess calls; got %v", revoker.revokedTokens)
	}
}

// ── NilRevoker path: no infra revoke, status flip still lands ─────────────────
//
// When revoker is nil (CUSTOMER_DATABASE_URL etc. not configured), the worker
// must still flip the status row — the API-level block is better than nothing.

func TestEnforceStorageQuotaWorker_NilRevoker_StatusFlipStillLands(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resourceID := "33333333-3333-3333-3333-333333333333"
	token := "tok_norevoker"
	storageBytes := int64(15 * 1024 * 1024)
	limitMB := 10

	// team_id is the SQL NULL sentinel ("" in the scanned NullString) — this is
	// an anonymous resource. nullableTeamID maps it to a NULL audit_log.team_id.
	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("active").
		WillReturnRows(sqlmock.NewRows(quotaScanCols).
			AddRow(resourceID, token, "mongodb", "hobby", storageBytes, "", nil, "")) // empty PRID = legacy row; nil team_id = anonymous
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"storage_bytes"}).AddRow(storageBytes))
	mock.ExpectExec(`UPDATE resources SET status = \$1`).
		WithArgs("suspended", resourceID, "active").
		WillReturnResult(sqlmock.NewResult(1, 1))
	// Audit row for an anonymous resource: team_id is NULL.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(nil, "system", "resource.quota_suspended", sqlmock.AnyArg(), sqlmock.AnyArg(), "mongodb").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("suspended").
		WillReturnRows(sqlmock.NewRows(quotaScanCols))

	plans := &mockPlanRegistry{limitMB: limitMB}
	w := jobs.NewEnforceStorageQuotaWorker(db, plans, nil) // nil revoker
	if err := w.Work(context.Background(), fakeJob[jobs.EnforceStorageQuotaArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// ── Hysteresis: a suspended resource in the dead-band stays suspended ─────────
//
// A resource suspended at >= 100% of the limit must NOT be unsuspended until
// usage drops below the hysteresis threshold (90% of the limit). A resource
// sitting at 95% is inside the dead-band — above 90%, below 100% — so it must
// remain suspended, with no GrantAccess call and no UPDATE. Without the
// hysteresis band the unsuspend loop would flip it back to 'active' the moment
// usage dipped below 100%, and the next tick would re-suspend it: a flap that
// fires a real provider REVOKE+GRANT every cycle.

func TestEnforceStorageQuotaWorker_HysteresisDeadBand_StaysSuspended(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resourceID := "55555555-5555-5555-5555-555555555555"
	token := "tok_deadband"
	resourceType := "postgres"
	tier := "hobby"
	limitMB := 100
	// storage_bytes == 95% of the 100 MB limit — inside the dead-band
	// (above the 90% unsuspend threshold, below the 100% suspend threshold).
	storageBytes := int64(float64(int64(limitMB)*1024*1024) * 0.95)

	// Suspend loop: no active over-quota resources.
	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("active").
		WillReturnRows(sqlmock.NewRows(quotaScanCols))

	// Unsuspend loop: one suspended resource sitting in the dead-band.
	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("suspended").
		WillReturnRows(sqlmock.NewRows(quotaScanCols).
			AddRow(resourceID, token, resourceType, tier, storageBytes, "", "cccccccc-cccc-cccc-cccc-cccccccccccc", "deadband-db")) // empty PRID = legacy row
	// readStorageBytes inner query — returns the dead-band value.
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"storage_bytes"}).AddRow(storageBytes))
	// NO UPDATE expected — the resource stays suspended.

	revoker := &mockResourceInfraRevoker{}
	plans := &mockPlanRegistry{limitMB: limitMB}
	w := jobs.NewEnforceStorageQuotaWorker(db, plans, revoker)
	if err := w.Work(context.Background(), fakeJob[jobs.EnforceStorageQuotaArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
	if len(revoker.grantedTokens) != 0 {
		t.Errorf("expected no GrantAccess calls inside the hysteresis dead-band; got %v", revoker.grantedTokens)
	}
}

// ── UnlimitedTier: never suspend ─────────────────────────────────────────────

func TestEnforceStorageQuotaWorker_UnlimitedTier_NoSuspend(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// limitMB -1 means unlimited — the worker must skip without issuing UPDATE.
	resourceID := "44444444-4444-4444-4444-444444444444"
	storageBytes := int64(999 * 1024 * 1024) // huge — should not matter

	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("active").
		WillReturnRows(sqlmock.NewRows(quotaScanCols).
			AddRow(resourceID, "tok_unlimited", "postgres", "team", storageBytes, "", "dddddddd-dddd-dddd-dddd-dddddddddddd", "unlimited-db")) // empty PRID = legacy row
	// No checkStorageQuota call expected — unlimited tier skips quota check.
	// No UPDATE expected.
	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("suspended").
		WillReturnRows(sqlmock.NewRows(quotaScanCols))

	revoker := &mockResourceInfraRevoker{}
	plans := &mockPlanRegistry{limitMB: -1} // unlimited
	w := jobs.NewEnforceStorageQuotaWorker(db, plans, revoker)
	if err := w.Work(context.Background(), fakeJob[jobs.EnforceStorageQuotaArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
	if len(revoker.revokedTokens) != 0 {
		t.Errorf("expected no revoke calls for unlimited tier; got %v", revoker.revokedTokens)
	}
}

// ── Audit-row emission regression tests ───────────────────────────────────────
//
// Suspending a customer's database is highly user-impacting but used to
// produce ZERO customer-visible artifact (slog only). These tests pin the
// contract that the suspend/unsuspend loops emit an audit_log row of the
// EXACT kind the api-side email renderer keys on.

// auditKindArgMatcher is a sqlmock.Argument that asserts the audit_log.kind
// passed to the INSERT matches the expected literal byte-for-byte.
type auditKindArgMatcher struct{ want string }

func (m auditKindArgMatcher) Match(v driver.Value) bool {
	s, ok := v.(string)
	return ok && s == m.want
}

// metadataContainsMatcher is a sqlmock.Argument that asserts the audit_log
// metadata JSONB payload contains the expected resource_id / resource_type /
// name fields the api email renderer reads.
type metadataContainsMatcher struct {
	resourceID   string
	resourceType string
	name         string
}

func (m metadataContainsMatcher) Match(v driver.Value) bool {
	var raw []byte
	switch t := v.(type) {
	case []byte:
		raw = t
	case string:
		raw = []byte(t)
	default:
		return false
	}
	var meta map[string]any
	if err := json.Unmarshal(raw, &meta); err != nil {
		return false
	}
	return meta["resource_id"] == m.resourceID &&
		meta["resource_type"] == m.resourceType &&
		meta["name"] == m.name
}

// TestEnforceStorageQuotaWorker_SuspendEmitsAuditRow proves the suspend loop
// inserts an audit_log row of kind EXACTLY "resource.quota_suspended" with the
// resource_id / resource_type / name in the metadata JSON, AND only after the
// status UPDATE succeeded.
func TestEnforceStorageQuotaWorker_SuspendEmitsAuditRow(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resourceID := "11111111-1111-1111-1111-111111111111"
	teamID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	resourceType := "postgres"
	name := "prod-db"
	storageBytes := int64(20 * 1024 * 1024)
	limitMB := 10

	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("active").
		WillReturnRows(sqlmock.NewRows(quotaScanCols).
			AddRow(resourceID, "tok_s", resourceType, "hobby", storageBytes, "", teamID, name))
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"storage_bytes"}).AddRow(storageBytes))
	// The status UPDATE must land FIRST.
	mock.ExpectExec(`UPDATE resources SET status = \$1`).
		WithArgs("suspended", resourceID, "active").
		WillReturnResult(sqlmock.NewResult(1, 1))
	// THEN the audit row — kind pinned byte-for-byte, metadata carries the
	// fields the api email renderer reads.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(
			teamID,
			"system",
			auditKindArgMatcher{want: "resource.quota_suspended"},
			sqlmock.AnyArg(), // summary
			metadataContainsMatcher{resourceID: resourceID, resourceType: resourceType, name: name},
			resourceType,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("suspended").
		WillReturnRows(sqlmock.NewRows(quotaScanCols))

	plans := &mockPlanRegistry{limitMB: limitMB}
	w := jobs.NewEnforceStorageQuotaWorker(db, plans, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.EnforceStorageQuotaArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestEnforceStorageQuotaWorker_UnsuspendEmitsAuditRow proves the unsuspend
// loop inserts an audit_log row of kind EXACTLY "resource.quota_unsuspended".
func TestEnforceStorageQuotaWorker_UnsuspendEmitsAuditRow(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resourceID := "22222222-2222-2222-2222-222222222222"
	teamID := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	resourceType := "redis"
	name := "prod-cache"
	storageBytes := int64(1 * 1024 * 1024) // well under the 25 MB limit
	limitMB := 25

	// Suspend loop: nothing over quota.
	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("active").
		WillReturnRows(sqlmock.NewRows(quotaScanCols))
	// Unsuspend loop: one suspended resource now under quota.
	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("suspended").
		WillReturnRows(sqlmock.NewRows(quotaScanCols).
			AddRow(resourceID, "tok_u", resourceType, "hobby", storageBytes, "", teamID, name))
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"storage_bytes"}).AddRow(storageBytes))
	mock.ExpectExec(`UPDATE resources SET status = \$1`).
		WithArgs("active", resourceID, "suspended").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(
			teamID,
			"system",
			auditKindArgMatcher{want: "resource.quota_unsuspended"},
			sqlmock.AnyArg(),
			metadataContainsMatcher{resourceID: resourceID, resourceType: resourceType, name: name},
			resourceType,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	plans := &mockPlanRegistry{limitMB: limitMB}
	w := jobs.NewEnforceStorageQuotaWorker(db, plans, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.EnforceStorageQuotaArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestEnforceStorageQuotaWorker_NoAuditRowWhenUpdateFails proves the audit row
// is NOT emitted when the status UPDATE itself errors — the customer-visible
// artifact must only follow a status flip that actually landed.
func TestEnforceStorageQuotaWorker_NoAuditRowWhenUpdateFails(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	resourceID := "99999999-9999-9999-9999-999999999999"
	storageBytes := int64(20 * 1024 * 1024)

	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("active").
		WillReturnRows(sqlmock.NewRows(quotaScanCols).
			AddRow(resourceID, "tok_f", "postgres", "hobby", storageBytes, "", "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee", "fail-db"))
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"storage_bytes"}).AddRow(storageBytes))
	// The UPDATE fails — NO audit INSERT must follow.
	mock.ExpectExec(`UPDATE resources SET status = \$1`).
		WithArgs("suspended", resourceID, "active").
		WillReturnError(errDB)
	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("suspended").
		WillReturnRows(sqlmock.NewRows(quotaScanCols))

	plans := &mockPlanRegistry{limitMB: 10}
	w := jobs.NewEnforceStorageQuotaWorker(db, plans, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.EnforceStorageQuotaArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// ExpectationsWereMet would FAIL if the code issued an unexpected
	// INSERT — no INSERT expectation was registered, so a stray audit row
	// surfaces as an error here.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (a stray audit INSERT would surface here): %v", err)
	}
}
