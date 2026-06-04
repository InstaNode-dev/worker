package jobs_test

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	commonv1 "instant.dev/proto/common/v1"

	"instant.dev/worker/internal/jobs"
)

const testResourceID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

// mockStorageBytesProvider implements StorageBytesProvider.
type mockStorageBytesProvider struct {
	storageBytes func(ctx context.Context, token, providerResourceID string, resType commonv1.ResourceType) (int64, error)
}

func (m *mockStorageBytesProvider) StorageBytes(ctx context.Context, token, provID string, rt commonv1.ResourceType) (int64, error) {
	if m.storageBytes != nil {
		return m.storageBytes(ctx, token, provID, rt)
	}
	return 2048, nil
}

func TestUpdateStorageBytesWorker_NilClient_NoOp(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// nil provClient — worker should no-op without querying DB.
	w := jobs.NewUpdateStorageBytesWorker(db, nil, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.UpdateStorageBytesArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUpdateStorageBytesWorker_UpdatesStorageBytes(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "token", "resource_type", "tier", "provider_resource_id"}).
		AddRow(testResourceID, "tok-1", "postgres", "anonymous", "")
	mock.ExpectQuery(`SELECT id, token`).WillReturnRows(rows)

	mock.ExpectExec(`UPDATE resources SET storage_bytes`).
		WithArgs(int64(2048), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	prov := &mockStorageBytesProvider{}
	w := jobs.NewUpdateStorageBytesWorker(db, prov, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.UpdateStorageBytesArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestUpdateStorageBytesWorker_RemeasuresSuspendedRow is the measurement half
// of the suspend→usage-drops→unsuspend lifecycle (SWEEP-BACKLOG #3). The
// scanner now selects status IN ('active','suspended'), so a quota-suspended
// resource keeps being re-measured. Without this, its storage_bytes would be
// frozen at the over-cap value and the EnforceStorageQuotaWorker unsuspend loop
// (which reads that column) would never see usage recede — leaving the row
// permanently suspended and breaking the suspend email's "access restored
// automatically once usage drops" promise. The unsuspend half is covered by
// TestEnforceStorageQuotaWorker_UnderQuota_UnsuspendsResource in quota_test.go;
// together they exercise the full lifecycle.
func TestUpdateStorageBytesWorker_RemeasuresSuspendedRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	suspendedID := "ffffffff-1111-2222-3333-444444444444"

	// The scanner must include suspended rows. Assert the status args are
	// exactly ('active','suspended') so a regression that drops 'suspended'
	// fails here. The freshly-measured value (300 bytes — well under cap) is the
	// value the unsuspend loop will later read to release the resource.
	rows := sqlmock.NewRows([]string{"id", "token", "resource_type", "tier", "provider_resource_id"}).
		AddRow(suspendedID, "tok-suspended", "postgres", "hobby", "")
	mock.ExpectQuery(`SELECT id, token`).
		WithArgs("active", "suspended", "", 1000).
		WillReturnRows(rows)

	mock.ExpectExec(`UPDATE resources SET storage_bytes`).
		WithArgs(int64(300), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	prov := &mockStorageBytesProvider{
		storageBytes: func(_ context.Context, _, _ string, _ commonv1.ResourceType) (int64, error) {
			return 300, nil // usage dropped well below cap
		},
	}
	w := jobs.NewUpdateStorageBytesWorker(db, prov, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.UpdateStorageBytesArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations (suspended row not re-measured?): %v", err)
	}
}

func TestUpdateStorageBytesWorker_DBQueryError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT id, token`).WillReturnError(errDB)

	prov := &mockStorageBytesProvider{}
	w := jobs.NewUpdateStorageBytesWorker(db, prov, nil)
	if err := w.Work(context.Background(), fakeJob[jobs.UpdateStorageBytesArgs]()); err == nil {
		t.Fatal("expected error from DB query failure")
	}
}

func TestUpdateStorageBytesWorker_ProviderError_FailOpen(t *testing.T) {
	// Provider errors should be fail-open: job succeeds, resource skipped.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "token", "resource_type", "tier", "provider_resource_id"}).
		AddRow(testResourceID, "tok-1", "postgres", "anonymous", "")
	mock.ExpectQuery(`SELECT id, token`).WillReturnRows(rows)
	// No UPDATE expected — provider error causes skip.

	prov := &mockStorageBytesProvider{
		storageBytes: func(_ context.Context, _, _ string, _ commonv1.ResourceType) (int64, error) {
			return 0, errDB
		},
	}
	w := jobs.NewUpdateStorageBytesWorker(db, prov, nil)
	// Should NOT return error (fail-open on provider error).
	if err := w.Work(context.Background(), fakeJob[jobs.UpdateStorageBytesArgs]()); err != nil {
		t.Fatalf("expected nil (fail-open), got %v", err)
	}
}
