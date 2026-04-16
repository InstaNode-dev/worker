package jobs_test

import (
	"context"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"instant.dev/worker/internal/jobs"
)

var errDB = errors.New("db error")

// mockPlanRegistry is a simple PlanRegistry stub.
type mockPlanRegistry struct {
	limitMB int
}

func (m *mockPlanRegistry) StorageLimitMB(tier, service string) int {
	return m.limitMB
}

func TestEnforceStorageQuotaWorker_NoResources_NoSuspend(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "token", "resource_type", "tier", "storage_bytes"})
	mock.ExpectQuery(`SELECT id, token`).WillReturnRows(rows)

	plans := &mockPlanRegistry{limitMB: 10}
	w := jobs.NewEnforceStorageQuotaWorker(db, plans)
	if err := w.Work(context.Background(), fakeJob[jobs.EnforceStorageQuotaArgs]()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnforceStorageQuotaWorker_DBQueryError_ReturnsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT id, token`).WillReturnError(errDB)

	plans := &mockPlanRegistry{limitMB: 10}
	w := jobs.NewEnforceStorageQuotaWorker(db, plans)
	if err := w.Work(context.Background(), fakeJob[jobs.EnforceStorageQuotaArgs]()); err == nil {
		t.Fatal("expected error from DB query failure, got nil")
	}
}
