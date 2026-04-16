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
	w := jobs.NewUpdateStorageBytesWorker(db, nil)
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
	w := jobs.NewUpdateStorageBytesWorker(db, prov)
	if err := w.Work(context.Background(), fakeJob[jobs.UpdateStorageBytesArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
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
	w := jobs.NewUpdateStorageBytesWorker(db, prov)
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
	w := jobs.NewUpdateStorageBytesWorker(db, prov)
	// Should NOT return error (fail-open on provider error).
	if err := w.Work(context.Background(), fakeJob[jobs.UpdateStorageBytesArgs]()); err != nil {
		t.Fatalf("expected nil (fail-open), got %v", err)
	}
}
