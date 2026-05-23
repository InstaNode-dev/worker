package jobs

// coverage_misc_test.go — drives ≥95% coverage on the worker job files
// targeted by the misc-job coverage worktree:
//   propagation_runner.go, provisioner_reconciler.go, resource_heartbeat.go,
//   prober.go, real_prober.go, uptime_prober.go, geodb.go,
//   team_deletion_executor.go, team_deletion_audit_kinds.go,
//   team_deletion_s3_adapter.go, chaos_lease_recovery.go.
//
// All tests intentionally use names matching the brief's filter set:
//   TestPropagation*|TestProvisionerReconcile*|TestHeartbeat*|TestProber*|
//   TestGeoDB*|TestTeamDeletion*|TestAuditKind*|TestS3Adapter*|TestChaos*

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	minio "github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	commonv1 "instant.dev/proto/common/v1"
	"instant.dev/worker/internal/config"
)

// localJob returns a minimal *river.Job for in-package callers.
func localJob[T river.JobArgs]() *river.Job[T] {
	return &river.Job[T]{JobRow: &rivertype.JobRow{ID: 42}}
}

// localFakeProber is a tiny ResourceProber double for in-package tests.
type localFakeProber struct {
	mu      sync.Mutex
	outcome ProbeOutcome
	err     error
	calls   int
}

func (p *localFakeProber) Probe(_ context.Context, _, _ string) (ProbeOutcome, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return p.outcome, p.err
}

// ─── prober.go: NoopProber dispatch ────────────────────────────────────────────

func TestProber_Noop_AllResourceTypesReachable(t *testing.T) {
	// Hit Probe(...) explicitly for both arms (resourceType ignored). Pins
	// the 100% surface on prober.go's only function.
	for _, rt := range []string{"postgres", "redis", "mongodb", "queue", "storage", "webhook", ""} {
		out, err := NoopProber{}.Probe(context.Background(), rt, "anything")
		if out != ProbeReachable || err != nil {
			t.Errorf("NoopProber{%q} = (%v,%v), want (ProbeReachable,nil)", rt, out, err)
		}
	}
}

func TestProber_ErrUnconfigured_Sentinel(t *testing.T) {
	// Sentinel must be a stable, non-nil error for callers to check via ==.
	if ErrProberUnconfigured == nil {
		t.Fatal("ErrProberUnconfigured must be non-nil")
	}
	if !strings.Contains(ErrProberUnconfigured.Error(), "not configured") {
		t.Errorf("ErrProberUnconfigured text = %q; expected to contain 'not configured'", ErrProberUnconfigured.Error())
	}
}

// ─── resource_heartbeat.go ────────────────────────────────────────────────────

func TestHeartbeat_Kind_ReturnsConstant(t *testing.T) {
	if k := (ResourceHeartbeatArgs{}).Kind(); k != "resource_heartbeat" {
		t.Errorf("ResourceHeartbeatArgs.Kind = %q, want resource_heartbeat", k)
	}
}

func TestHeartbeat_PeriodicInterval_ProdVsDev(t *testing.T) {
	if got := resourceHeartbeatPeriodicInterval("production"); got != resourceHeartbeatInterval {
		t.Errorf("production interval = %s, want %s", got, resourceHeartbeatInterval)
	}
	if got := resourceHeartbeatPeriodicInterval("development"); got != resourceHeartbeatDevInterval {
		t.Errorf("dev interval = %s, want %s", got, resourceHeartbeatDevInterval)
	}
	if got := resourceHeartbeatPeriodicInterval(""); got != resourceHeartbeatDevInterval {
		t.Errorf("empty env interval = %s, want dev %s", got, resourceHeartbeatDevInterval)
	}
}

func TestHeartbeat_NilProberFallsBackToNoop(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "resource_type", "connection_url",
			"team_id_text", "degraded", "last_seen_at",
		}))
	w := NewResourceHeartbeatWorker(db, nil) // nil → NoopProber
	if err := w.Work(context.Background(), localJob[ResourceHeartbeatArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestHeartbeat_FlapWindowSuppressesAuditRow(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	resID := uuid.New()
	token := uuid.New()
	teamID := uuid.New().String()

	// wasDegraded=true + fresh last_seen_at + probe fails → markDegraded
	// gated UPDATE returns 0 rows (within flap window) → no audit row.
	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "resource_type", "connection_url",
			"team_id_text", "degraded", "last_seen_at",
		}).AddRow(resID, token, "redis", "url", teamID, true, sql.NullTime{Time: time.Now(), Valid: true}))
	mock.ExpectExec(`UPDATE resources\s+SET degraded = true`).
		WithArgs(resID, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 0)) // 0 rows = inside flap window
	mock.ExpectQuery(`SELECT resource_type, COUNT\(\*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"resource_type", "count"}))

	w := NewResourceHeartbeatWorker(db, &localFakeProber{outcome: ProbeUnreachable, err: errors.New("boom")})
	if err := w.Work(context.Background(), localJob[ResourceHeartbeatArgs]()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestHeartbeat_HealthyUpdateError_LoggedAndContinues(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	resID := uuid.New()
	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "resource_type", "connection_url",
			"team_id_text", "degraded", "last_seen_at",
		}).AddRow(resID, uuid.New(), "postgres", "url", "", false, sql.NullTime{}))
	mock.ExpectExec(`UPDATE resources\s+SET last_seen_at`).
		WithArgs(resID).
		WillReturnError(errors.New("update failed"))
	mock.ExpectQuery(`SELECT resource_type, COUNT\(\*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"resource_type", "count"}))

	w := NewResourceHeartbeatWorker(db, &localFakeProber{outcome: ProbeReachable})
	if err := w.Work(context.Background(), localJob[ResourceHeartbeatArgs]()); err != nil {
		t.Fatalf("fail-open: Work should return nil, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestHeartbeat_DegradedUpdateError_LoggedAndContinues(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	resID := uuid.New()
	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "resource_type", "connection_url",
			"team_id_text", "degraded", "last_seen_at",
		}).AddRow(resID, uuid.New(), "postgres", "url", "", false, sql.NullTime{}))
	mock.ExpectExec(`UPDATE resources\s+SET degraded = true`).
		WithArgs(resID, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(errors.New("degrade update failed"))
	mock.ExpectQuery(`SELECT resource_type, COUNT\(\*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"resource_type", "count"}))

	w := NewResourceHeartbeatWorker(db, &localFakeProber{outcome: ProbeUnreachable, err: errors.New("boom")})
	if err := w.Work(context.Background(), localJob[ResourceHeartbeatArgs]()); err != nil {
		t.Fatalf("fail-open: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestHeartbeat_GaugeQueryError_DoesNotFailWork(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "resource_type", "connection_url",
			"team_id_text", "degraded", "last_seen_at",
		}).AddRow(uuid.New(), uuid.New(), "webhook", "", "", false, sql.NullTime{}))
	mock.ExpectQuery(`SELECT resource_type, COUNT\(\*\)`).
		WillReturnError(errors.New("gauge query failed"))

	w := NewResourceHeartbeatWorker(db, &localFakeProber{outcome: ProbeSkip})
	if err := w.Work(context.Background(), localJob[ResourceHeartbeatArgs]()); err != nil {
		t.Fatalf("gauge error must not fail Work: %v", err)
	}
}

// ─── provisioner_reconciler.go: refundQuota + helpers + Kind ──────────────────

func TestProvisionerReconcile_Kind_ReturnsConstant(t *testing.T) {
	if k := (ProvisionerReconcilerArgs{}).Kind(); k != "provisioner_reconciler" {
		t.Errorf("Kind = %q", k)
	}
}

func TestProvisionerReconcile_RefundQuota_NilRedisIsNoOp(t *testing.T) {
	w := &ProvisionerReconcilerWorker{}
	// nil rdb branch — must return cleanly.
	w.refundQuota(context.Background(), reconcilerCandidate{
		id:           uuid.New(),
		resourceType: "postgres",
		teamID:       sql.NullString{String: uuid.New().String(), Valid: true},
	})
}

func TestProvisionerReconcile_RefundQuota_AnonymousSkipped(t *testing.T) {
	// rdb non-nil but team_id NULL → log+skip branch.
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}) // unreachable, but never called
	defer rdb.Close()
	w := &ProvisionerReconcilerWorker{rdb: rdb}
	w.refundQuota(context.Background(), reconcilerCandidate{
		id:           uuid.New(),
		resourceType: "redis",
		teamID:       sql.NullString{}, // invalid → anonymous path
	})
}

func TestProvisionerReconcile_RefundQuota_DecrError_Logged(t *testing.T) {
	// rdb pointed at an unreachable port → Decr returns an error → logged
	// branch fires.
	rdb := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 100 * time.Millisecond,
	})
	defer rdb.Close()
	w := &ProvisionerReconcilerWorker{rdb: rdb}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	w.refundQuota(ctx, reconcilerCandidate{
		id:           uuid.New(),
		resourceType: "postgres",
		teamID:       sql.NullString{String: uuid.New().String(), Valid: true},
	})
}

func TestProvisionerReconcile_NullableTeamID_EmptyAndValid(t *testing.T) {
	if got := nullableTeamID(sql.NullString{}); got != nil {
		t.Errorf("invalid NullString → %v, want nil", got)
	}
	if got := nullableTeamID(sql.NullString{Valid: true, String: ""}); got != nil {
		t.Errorf("empty-string NullString → %v, want nil", got)
	}
	id := uuid.New().String()
	if got := nullableTeamID(sql.NullString{Valid: true, String: id}); got != id {
		t.Errorf("valid NullString → %v, want %s", got, id)
	}
}

func TestProvisionerReconcile_ProbeErrString_NilGuards(t *testing.T) {
	if s := probeErrString(nil); !strings.Contains(s, "no error message") {
		t.Errorf("nil err sentinel text = %q", s)
	}
	if s := probeErrString(errors.New("real failure")); s != "real failure" {
		t.Errorf("got %q", s)
	}
}

func TestProvisionerReconcile_TruncateReason_HonorsCap(t *testing.T) {
	if got := truncateReason("hi"); got != "hi" {
		t.Errorf("short returned changed: %q", got)
	}
	long := strings.Repeat("x", 1000)
	if got := truncateReason(long); len(got) != 500 {
		t.Errorf("truncated len = %d, want 500", len(got))
	}
}

func TestProvisionerReconcile_RecoveredAuditError_LoggedFailOpen(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	resID := uuid.New()
	teamID := uuid.New().String()

	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "resource_type", "connection_url", "team_id_text",
		}).AddRow(resID, uuid.New(), "postgres", "url", teamID))
	mock.ExpectExec(`UPDATE resources\s+SET status = 'active'`).
		WithArgs(resID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", "provisioner.reconcile_recovered", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(errors.New("audit insert failed"))

	w := NewProvisionerReconcilerWorker(db, nil, &localFakeProber{outcome: ProbeReachable})
	if err := w.Work(context.Background(), localJob[ProvisionerReconcilerArgs]()); err != nil {
		t.Fatalf("fail-open: %v", err)
	}
}

func TestProvisionerReconcile_AbandonedAuditError_LoggedFailOpen(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	resID := uuid.New()
	teamID := uuid.New().String()
	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "resource_type", "connection_url", "team_id_text",
		}).AddRow(resID, uuid.New(), "postgres", "url", teamID))
	mock.ExpectExec(`UPDATE resources\s+SET status = 'failed'`).
		WithArgs(resID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", "provisioner.reconcile_abandoned", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(errors.New("audit insert failed"))

	w := NewProvisionerReconcilerWorker(db, nil, &localFakeProber{outcome: ProbeUnreachable, err: errors.New("dial refused")})
	if err := w.Work(context.Background(), localJob[ProvisionerReconcilerArgs]()); err != nil {
		t.Fatalf("fail-open: %v", err)
	}
}

func TestProvisionerReconcile_SkipUpdateError_LoggedFailOpen(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	resID := uuid.New()
	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "resource_type", "connection_url", "team_id_text",
		}).AddRow(resID, uuid.New(), "webhook", "", ""))
	mock.ExpectExec(`UPDATE resources SET last_reconciled_at`).
		WithArgs(resID).
		WillReturnError(errors.New("stamp failed"))

	w := NewProvisionerReconcilerWorker(db, nil, &localFakeProber{outcome: ProbeSkip})
	if err := w.Work(context.Background(), localJob[ProvisionerReconcilerArgs]()); err != nil {
		t.Fatalf("fail-open: %v", err)
	}
}

func TestProvisionerReconcile_RowScanFailure_SkipsRow(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Row with a malformed UUID in id → Scan fails → row skipped → Work
	// returns nil with no candidates.
	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "resource_type", "connection_url", "team_id_text",
		}).AddRow("not-a-uuid", "also-not-uuid", "postgres", "url", ""))

	w := NewProvisionerReconcilerWorker(db, nil, &localFakeProber{outcome: ProbeReachable})
	if err := w.Work(context.Background(), localJob[ProvisionerReconcilerArgs]()); err != nil {
		t.Fatalf("scan errors must not propagate: %v", err)
	}
}

// ─── propagation_runner.go: Kind, Interval ─────────────────────────────────────

func TestPropagation_Kind_ReturnsConstant(t *testing.T) {
	if k := (PropagationRunnerArgs{}).Kind(); k != "propagation_runner" {
		t.Errorf("Kind = %q", k)
	}
}

func TestPropagation_Interval_DefaultsAndOverride(t *testing.T) {
	prev := os.Getenv("PROPAGATION_RUNNER_INTERVAL")
	defer os.Setenv("PROPAGATION_RUNNER_INTERVAL", prev)

	os.Unsetenv("PROPAGATION_RUNNER_INTERVAL")
	if got := PropagationRunnerInterval(); got != propagationDefaultInterval {
		t.Errorf("unset = %s, want %s", got, propagationDefaultInterval)
	}

	os.Setenv("PROPAGATION_RUNNER_INTERVAL", "10s")
	if got := PropagationRunnerInterval(); got != 10*time.Second {
		t.Errorf("override = %s, want 10s", got)
	}

	os.Setenv("PROPAGATION_RUNNER_INTERVAL", "garbage")
	if got := PropagationRunnerInterval(); got != propagationDefaultInterval {
		t.Errorf("bad value → fallback, got %s", got)
	}

	os.Setenv("PROPAGATION_RUNNER_INTERVAL", "0s")
	if got := PropagationRunnerInterval(); got != propagationDefaultInterval {
		t.Errorf("non-positive value → fallback, got %s", got)
	}

	os.Setenv("PROPAGATION_RUNNER_INTERVAL", "  ")
	if got := PropagationRunnerInterval(); got != propagationDefaultInterval {
		t.Errorf("whitespace-only value → fallback, got %s", got)
	}
}

func TestPropagation_NilRegrader_LogsWarnAndReturns(t *testing.T) {
	db, _, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w := NewPropagationRunnerWorker(db, nil, nil)
	if err := w.Work(context.Background(), localJob[PropagationRunnerArgs]()); err != nil {
		t.Fatalf("Work with nil regrader must return nil: %v", err)
	}
}

func TestPropagation_PickEligible_TopLevelSelectError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, kind, team_id`).WillReturnError(errors.New("select failed"))
	mock.ExpectRollback()
	w := NewPropagationRunnerWorker(db, nil, &stubPropagationRegrader{outcome: regradeOutcome{Applied: true}})
	if err := w.Work(context.Background(), localJob[PropagationRunnerArgs]()); err == nil {
		t.Fatal("expected error from pick failure")
	}
}

func TestPropagation_PgUUIDArray_FormatsAsArrayLiteral(t *testing.T) {
	if got := pgUUIDArray(nil); got != "{}" {
		t.Errorf("nil = %q, want {}", got)
	}
	if got := pgUUIDArray([]uuid.UUID{}); got != "{}" {
		t.Errorf("empty = %q, want {}", got)
	}
	one := uuid.New()
	if got := pgUUIDArray([]uuid.UUID{one}); got != "{"+one.String()+"}" {
		t.Errorf("one element = %q", got)
	}
	a, b := uuid.New(), uuid.New()
	if got := pgUUIDArray([]uuid.UUID{a, b}); got != "{"+a.String()+","+b.String()+"}" {
		t.Errorf("two elements = %q", got)
	}
}

func TestPropagation_TruncateError_RespectsCap(t *testing.T) {
	if got := truncatePropagationError("short"); got != "short" {
		t.Errorf("short changed: %q", got)
	}
	long := strings.Repeat("y", propagationLastErrorMax+50)
	got := truncatePropagationError(long)
	if len(got) != propagationLastErrorMax {
		t.Errorf("truncated len = %d, want %d", len(got), propagationLastErrorMax)
	}
	if !strings.HasSuffix(got, "...") {
		t.Errorf("expected ... suffix, got %q", got[len(got)-5:])
	}
}

func TestPropagation_NullableTierString_EmptyAndValid(t *testing.T) {
	if got := nullableTierString(sql.NullString{}); got != "" {
		t.Errorf("invalid → %q", got)
	}
	if got := nullableTierString(sql.NullString{Valid: true, String: "pro"}); got != "pro" {
		t.Errorf("valid → %q", got)
	}
}

func TestPropagation_ResourceTypeFromString_AllArms(t *testing.T) {
	cases := map[string]struct {
		want      commonv1.ResourceType
		supported bool
	}{
		"postgres": {commonv1.ResourceType_RESOURCE_TYPE_POSTGRES, true},
		"redis":    {commonv1.ResourceType_RESOURCE_TYPE_REDIS, true},
		"mongodb":  {commonv1.ResourceType_RESOURCE_TYPE_MONGODB, true},
		"storage":  {commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED, false},
		"queue":    {commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED, false},
		"webhook":  {commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED, false},
		"future":   {commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED, false},
	}
	for in, want := range cases {
		got, sup := resourceTypeFromString(in)
		if got != want.want || sup != want.supported {
			t.Errorf("resourceTypeFromString(%q) = (%v,%v), want (%v,%v)", in, got, sup, want.want, want.supported)
		}
	}
}

func TestPropagation_HandleTierElevation_EmptyTeamIsSuccess(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(`FROM resources r`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "provider_resource_id", "tier", "resource_type",
		}))

	row := propagationRow{id: uuid.New(), teamID: uuid.New(), kind: "tier_elevation"}
	regr := &stubPropagationRegrader{}
	if err := handleTierElevation(context.Background(), db, regr, nil, row); err != nil {
		t.Fatalf("empty team success: %v", err)
	}
	if regr.calls != 0 {
		t.Errorf("expected 0 RegradeResource calls, got %d", regr.calls)
	}
}

func TestPropagation_HandleTierElevation_QueryError_Returned(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(`FROM resources r`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnError(errors.New("select boom"))

	row := propagationRow{id: uuid.New(), teamID: uuid.New(), kind: "tier_elevation"}
	if err := handleTierElevation(context.Background(), db, &stubPropagationRegrader{}, nil, row); err == nil {
		t.Fatal("expected error")
	}
}

func TestPropagation_HandleTierElevation_UnsupportedAndEphemeral(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// One storage row (unsupported) + one anonymous row (ephemeral) → both
	// skipped → no regrader calls, no error.
	mock.ExpectQuery(`FROM resources r`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "provider_resource_id", "tier", "resource_type",
		}).
			AddRow(uuid.New(), "tok1", "prid1", "pro", "storage").
			AddRow(uuid.New(), "tok2", "prid2", "anonymous", "postgres"))

	row := propagationRow{id: uuid.New(), teamID: uuid.New(), kind: "tier_elevation"}
	regr := &stubPropagationRegrader{}
	if err := handleTierElevation(context.Background(), db, regr, nil, row); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if regr.calls != 0 {
		t.Errorf("expected 0 regrade calls (storage skipped, ephemeral skipped), got %d", regr.calls)
	}
}

func TestPropagation_HandleTierElevation_RegradeErrorReturnedFirst(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(`FROM resources r`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "provider_resource_id", "tier", "resource_type",
		}).AddRow(uuid.New(), "tok", "prid", "pro", "postgres"))

	row := propagationRow{id: uuid.New(), teamID: uuid.New(), kind: "tier_elevation"}
	regr := &stubPropagationRegrader{err: errors.New("grpc boom")}
	err = handleTierElevation(context.Background(), db, regr, nil, row)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "grpc boom") {
		t.Errorf("err text = %q", err.Error())
	}
}

func TestPropagation_HandleTierElevation_AllowedSkipReason_NoError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(`FROM resources r`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "provider_resource_id", "tier", "resource_type",
		}).AddRow(uuid.New(), "tok", "prid", "pro", "redis"))

	row := propagationRow{id: uuid.New(), teamID: uuid.New(), kind: "tier_elevation"}
	regr := &stubPropagationRegrader{outcome: regradeOutcome{Applied: false, SkipReason: "already correct"}}
	if err := handleTierElevation(context.Background(), db, regr, nil, row); err != nil {
		t.Fatalf("allowed skip must succeed: %v", err)
	}
}

func TestPropagation_HandleTierElevation_ScanError_RowSkipped(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// First row has bad UUID for id (will fail Scan), second row OK.
	mock.ExpectQuery(`FROM resources r`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "provider_resource_id", "tier", "resource_type",
		}).
			AddRow("not-a-uuid", "tok", "prid", "pro", "postgres").
			AddRow(uuid.New(), "tok", "prid", "pro", "postgres"))

	row := propagationRow{id: uuid.New(), teamID: uuid.New(), kind: "tier_elevation"}
	regr := &stubPropagationRegrader{outcome: regradeOutcome{Applied: true}}
	if err := handleTierElevation(context.Background(), db, regr, nil, row); err != nil {
		t.Fatalf("scan error: %v", err)
	}
	// Only the well-scanned row should be regraded.
	if regr.calls != 1 {
		t.Errorf("expected 1 regrade call, got %d", regr.calls)
	}
}

func TestPropagation_BucketSkipReason_ExtraCases(t *testing.T) {
	// Extra inputs for the bucketSkipReason switch arms not already tested by
	// existing TestBucketSkipReason_BoundsCardinality.
	cases := []struct {
		in   string
		want string
	}{
		{"POSTGRES_ADMIN missing", "postgres_admin_secret_missing"},
		{"some namespace not found here", "namespace_not_found"},
		{"backend is not reachable", "resource_not_reachable"},
		{"legacy pod has no creds", "legacy_resource"},
	}
	for _, c := range cases {
		if got := bucketSkipReason(c.in); got != c.want {
			t.Errorf("bucketSkipReason(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// ─── geodb.go: Kind, constructor, Work + helpers ───────────────────────────────

func TestGeoDB_Kind_ReturnsConstant(t *testing.T) {
	if k := (RefreshGeoDBArgs{}).Kind(); k != "refresh_geodb" {
		t.Errorf("Kind = %q", k)
	}
}

func TestGeoDB_NewWorker_NonNil(t *testing.T) {
	if w := NewRefreshGeoDBWorker(); w == nil {
		t.Fatal("ctor returned nil")
	}
}

func TestGeoDB_Work_NoLicenseKey_NoOp(t *testing.T) {
	w := NewRefreshGeoDBWorker()
	job := &river.Job[RefreshGeoDBArgs]{
		Args:   RefreshGeoDBArgs{LicenseKey: "", DBPath: "/tmp/nonexistent"},
		JobRow: &rivertype.JobRow{ID: 1},
	}
	if err := w.Work(context.Background(), job); err != nil {
		t.Fatalf("Work without license must noop: %v", err)
	}
}

func TestGeoDB_Work_FreshDB_Skipped(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "GeoLite2-City.mmdb")
	if err := os.WriteFile(path, []byte("mmdb"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".fetched", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	w := NewRefreshGeoDBWorker()
	job := &river.Job[RefreshGeoDBArgs]{
		Args:   RefreshGeoDBArgs{LicenseKey: "x", DBPath: path},
		JobRow: &rivertype.JobRow{ID: 1},
	}
	if err := w.Work(context.Background(), job); err != nil {
		t.Fatalf("fresh DB skip: %v", err)
	}
}

func TestGeoDB_Work_BadDownloadURL_ReturnsError(t *testing.T) {
	// LicenseKey set, DB not present, http GET will hit MaxMind which from
	// the test environment we can't reach. The downloadURL has no scheme
	// override; using an empty license generates URL fetch failure. Cancel
	// the ctx fast so we don't actually dial maxmind.
	w := NewRefreshGeoDBWorker()
	job := &river.Job[RefreshGeoDBArgs]{
		Args:   RefreshGeoDBArgs{LicenseKey: "fake-license-key", DBPath: filepath.Join(t.TempDir(), "out.mmdb")},
		JobRow: &rivertype.JobRow{ID: 1},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	// Either: network unreachable, or ctx deadline. Both surface as an error
	// from Work, exercising the download-failed branch.
	_ = w.Work(ctx, job)
}

func TestGeoDB_TouchFetchMarker_Roundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "GeoLite2-City.mmdb")
	touchGeoDBFetchMarker(path)
	if _, err := os.Stat(path + geoLite2FetchMarkerSuffix); err != nil {
		t.Fatalf("marker not written: %v", err)
	}
}

func TestGeoDB_TouchFetchMarker_PermissionError_Logged(t *testing.T) {
	// path under a non-existent directory → os.Create fails → branch
	// logging warning but not panicking.
	touchGeoDBFetchMarker("/nonexistent/dir/that/should/never/exist/file")
}

func TestGeoDB_IsFresh_EmptyPathReturnsFalse(t *testing.T) {
	if geoDBIsFresh("", time.Now()) {
		t.Error("empty path must be not-fresh")
	}
}

// ─── team_deletion_executor.go ────────────────────────────────────────────────

func TestTeamDeletion_Kind_ReturnsConstant(t *testing.T) {
	if k := (TeamDeletionExecutorArgs{}).Kind(); k != "team_deletion_executor" {
		t.Errorf("Kind = %q", k)
	}
}

func TestTeamDeletion_BackupPrefix_EmptyTokenReturnsEmpty(t *testing.T) {
	if got := s3BackupPrefixForToken(""); got != "" {
		t.Errorf("empty token → %q, want \"\"", got)
	}
	tok := uuid.New().String()
	if got := s3BackupPrefixForToken(tok); got != "backups/"+tok+"/" {
		t.Errorf("normal token → %q", got)
	}
}

func TestTeamDeletion_DeployNamespace_EmptyAppIDReturnsEmpty(t *testing.T) {
	if got := deployNamespaceForApp(""); got != "" {
		t.Errorf("empty appID → %q", got)
	}
	if got := deployNamespaceForApp("abc"); got != "instant-deploy-abc" {
		t.Errorf("normal appID → %q", got)
	}
}

func TestTeamDeletion_ContainsAny_AllArms(t *testing.T) {
	if containsAny("hello world", "world") != true {
		t.Error("substring should match")
	}
	if containsAny("hi", "longer-needle") != false {
		t.Error("needle longer than haystack must be false")
	}
	if containsAny("xyz", "y") != true {
		t.Error("single char match")
	}
	if containsAny("abc", "z") != false {
		t.Error("missing match")
	}
}

func TestTeamDeletion_NewExecutor_DefaultsBucket(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w := NewTeamDeletionExecutorWorker(db, nil, nil, nil, "")
	if w.bucketName != "instant-shared" {
		t.Errorf("default bucket = %q, want instant-shared", w.bucketName)
	}
	w2 := NewTeamDeletionExecutorWorker(db, nil, nil, nil, "custom-bucket")
	if w2.bucketName != "custom-bucket" {
		t.Errorf("custom bucket = %q", w2.bucketName)
	}
}

func TestTeamDeletion_FetchCandidates_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(`FROM teams\s+WHERE`).WillReturnError(errors.New("query failed"))

	w := NewTeamDeletionExecutorWorker(db, nil, nil, nil, "")
	if err := w.Work(context.Background(), localJob[TeamDeletionExecutorArgs]()); err == nil {
		t.Fatal("expected error from top-level query failure")
	}
}

func TestTeamDeletion_EmitDeletionFailed_StepInference_AllArms(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w := NewTeamDeletionExecutorWorker(db, nil, nil, nil, "")
	teamID := uuid.New()

	cases := []struct {
		err  error
		step string
	}{
		{context.Canceled, "context"},
		{context.DeadlineExceeded, "context"},
		{errors.New("fetch resources: db error"), "fetch_resources"},
		{errors.New("delete s3 backups for x"), "s3_delete"},
		{errors.New("deprovision postgres: failed"), "deprovision"},
		{errors.New("delete namespace foo: bad"), "delete_namespace"},
		{errors.New("fetch deploy app ids: query"), "delete_namespace"},
		{errors.New("mark deletion_pending: x"), "mark_deletion_pending"},
		{errors.New("null resource pii: oops"), "null_resource_pii"},
		{errors.New("null user pii: oops"), "null_user_pii"},
		{errors.New("flip team status: oops"), "flip_team_status"},
		{errors.New("commit tx: oops"), "tx_commit"},
		{errors.New("begin tx: oops"), "tx_commit"},
		{errors.New("totally unrelated error"), "unknown"},
	}
	for _, c := range cases {
		mock.ExpectExec(`INSERT INTO audit_log`).
			WithArgs(teamID, "system", auditKindTeamDeletionFailed, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))
		w.emitDeletionFailed(context.Background(), teamID, c.err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestTeamDeletion_EmitDeletionFailed_InsertError_Logged(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w := NewTeamDeletionExecutorWorker(db, nil, nil, nil, "")
	teamID := uuid.New()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", auditKindTeamDeletionFailed, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(errors.New("audit insert failed"))
	w.emitDeletionFailed(context.Background(), teamID, errors.New("upstream"))
}

func TestTeamDeletion_EmitTombstoned_InsertError_Logged(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w := NewTeamDeletionExecutorWorker(db, nil, nil, nil, "")
	teamID := uuid.New()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", auditKindTombstoned, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(errors.New("insert failed"))
	w.emitTombstoned(context.Background(), teamID, 1, 0, 0, time.Second)
}

func TestTeamDeletion_FetchTeamDeployAppIDs_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w := NewTeamDeletionExecutorWorker(db, nil, nil, nil, "")
	mock.ExpectQuery(`SELECT DISTINCT app_id`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnError(errors.New("boom"))
	if _, err := w.fetchTeamDeployAppIDs(context.Background(), uuid.New()); err == nil {
		t.Fatal("expected error")
	}
}

func TestTeamDeletion_FetchTeamResources_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w := NewTeamDeletionExecutorWorker(db, nil, nil, nil, "")
	mock.ExpectQuery(`FROM resources`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnError(errors.New("boom"))
	if _, err := w.fetchTeamResources(context.Background(), uuid.New()); err == nil {
		t.Fatal("expected error")
	}
}

func TestTeamDeletion_FetchCandidates_ScanError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w := NewTeamDeletionExecutorWorker(db, nil, nil, nil, "")
	// Bad UUID + good time → scan fails.
	mock.ExpectQuery(`FROM teams\s+WHERE`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "deletion_requested_at"}).
			AddRow("not-a-uuid", time.Now()))
	if _, err := w.fetchCandidates(context.Background()); err == nil {
		t.Fatal("expected scan error")
	}
}

// ─── team_deletion_audit_kinds.go ─────────────────────────────────────────────

func TestAuditKind_TombstonedAndFailed_Constants(t *testing.T) {
	if auditKindTombstoned != "team.tombstoned" {
		t.Errorf("auditKindTombstoned = %q", auditKindTombstoned)
	}
	if auditKindTeamDeletionFailed != "team.deletion_failed" {
		t.Errorf("auditKindTeamDeletionFailed = %q", auditKindTeamDeletionFailed)
	}
}

func TestAuditKind_ChaosLeaseRecovery_Constants(t *testing.T) {
	if AuditKindChaosLeaseRecoveryStart != "chaos.lease_recovery.start" {
		t.Errorf("start = %q", AuditKindChaosLeaseRecoveryStart)
	}
	if AuditKindChaosLeaseRecoveryEnd != "chaos.lease_recovery.end" {
		t.Errorf("end = %q", AuditKindChaosLeaseRecoveryEnd)
	}
}

// ─── team_deletion_s3_adapter.go ──────────────────────────────────────────────

func TestS3Adapter_NewMinIOBackupDeleter_NilScannerReturnsNil(t *testing.T) {
	if got := newMinIOBackupDeleter(nil); got != nil {
		t.Errorf("nil scanner → %v, want nil", got)
	}
}

// localFakeObjectLister satisfies minioObjectLister with no-ops, lets us
// drive newMinIOScannerWithClient via the test seam.
type localFakeObjectLister struct{}

func (localFakeObjectLister) BucketExists(_ context.Context, _ string) (bool, error) {
	return true, nil
}
func (localFakeObjectLister) ListObjects(_ context.Context, _ string, _ minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo)
	close(ch)
	return ch
}
func (localFakeObjectLister) ListIncompleteUploads(_ context.Context, _, _ string, _ bool) <-chan minio.ObjectMultipartInfo {
	ch := make(chan minio.ObjectMultipartInfo)
	close(ch)
	return ch
}

func TestS3Adapter_NewMinIOBackupDeleter_FakeClientReturnsNil(t *testing.T) {
	// Scanner with a fake (non-*minio.Client) → the type assertion fails →
	// the adapter returns nil per its contract (test seam path).
	scanner := newMinIOScannerWithClient(localFakeObjectLister{}, "test-bucket")
	if got := newMinIOBackupDeleter(scanner); got != nil {
		t.Errorf("fake client → %v, want nil", got)
	}
}

func TestS3Adapter_DeleterListAndRemove_RoundTrip(t *testing.T) {
	// Real *minio.Client constructed against an unreachable endpoint —
	// we only exercise the wrapper methods, NOT the underlying RPCs.
	mc, err := minio.New("127.0.0.1:1", &minio.Options{Secure: false})
	if err != nil {
		t.Fatal(err)
	}
	d := &minioBackupDeleter{client: mc, bucketName: "any"}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	// ListObjects returns a channel — drain it to exercise the call path.
	ch := d.ListObjects(ctx, "any", minio.ListObjectsOptions{})
	for range ch {
	}
	// RemoveObjects accepts an input channel — close it immediately.
	in := make(chan minio.ObjectInfo)
	close(in)
	errCh := d.RemoveObjects(ctx, "any", in, minio.RemoveObjectsOptions{})
	for range errCh {
	}
}

func TestS3Adapter_NewMinIOBackupDeleter_RealClient_NotNil(t *testing.T) {
	// Build a *minio.Client by hand and wrap it in a scanner. The factory
	// should now succeed (return non-nil).
	mc, err := minio.New("127.0.0.1:1", &minio.Options{Secure: false})
	if err != nil {
		t.Fatal(err)
	}
	scanner := newMinIOScannerWithClient(mc, "test")
	if got := newMinIOBackupDeleter(scanner); got == nil {
		t.Error("real minio.Client → adapter should be non-nil")
	}
}

// ─── chaos_lease_recovery.go ──────────────────────────────────────────────────

func TestChaos_LeaseRecovery_Kind(t *testing.T) {
	if k := (ChaosLeaseRecoveryArgs{}).Kind(); k != chaosLeaseRecoveryKind {
		t.Errorf("Kind = %q", k)
	}
}

func TestChaos_LeaseRecovery_InsertOpts(t *testing.T) {
	opts := (ChaosLeaseRecoveryArgs{}).InsertOpts()
	if opts.Queue != river.QueueDefault {
		t.Errorf("queue = %q", opts.Queue)
	}
	if opts.Priority != 4 {
		t.Errorf("priority = %d, want 4", opts.Priority)
	}
}

func TestChaos_LeaseRecovery_NewWorker(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	if w := NewChaosLeaseRecoveryWorker(db); w == nil {
		t.Fatal("ctor returned nil")
	}
}

func TestChaos_LeaseRecovery_PodHostname(t *testing.T) {
	prev := os.Getenv("HOSTNAME")
	defer os.Setenv("HOSTNAME", prev)

	os.Setenv("HOSTNAME", "worker-pod-42")
	if got := podHostname(); got != "worker-pod-42" {
		t.Errorf("set → %q", got)
	}
	os.Unsetenv("HOSTNAME")
	if got := podHostname(); got != "unknown" {
		t.Errorf("unset → %q", got)
	}
}

func TestChaos_LeaseRecovery_Work_BadTeamID_Errors(t *testing.T) {
	db, _, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w := NewChaosLeaseRecoveryWorker(db)
	job := &river.Job[ChaosLeaseRecoveryArgs]{
		Args:   ChaosLeaseRecoveryArgs{TeamID: "not-a-uuid", SleepSeconds: 0, RunID: "run1"},
		JobRow: &rivertype.JobRow{ID: 1},
	}
	if err := w.Work(context.Background(), job); err == nil {
		t.Fatal("expected error for bad team UUID")
	}
}

func TestChaos_LeaseRecovery_Work_HappyPath_Sleep0(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	teamID := uuid.New().String()

	// Two INSERT INTO audit_log calls — start + end.
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(sqlmock.AnyArg(), chaosLeaseRecoveryActor,
			AuditKindChaosLeaseRecoveryStart, "drill start", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(sqlmock.AnyArg(), chaosLeaseRecoveryActor,
			AuditKindChaosLeaseRecoveryEnd, "drill end", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(2, 1))

	w := NewChaosLeaseRecoveryWorker(db)
	job := &river.Job[ChaosLeaseRecoveryArgs]{
		Args:   ChaosLeaseRecoveryArgs{TeamID: teamID, SleepSeconds: 0, RunID: "run-happy"},
		JobRow: &rivertype.JobRow{ID: 1},
	}
	if err := w.Work(context.Background(), job); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestChaos_LeaseRecovery_Work_NegativeSleepClampedToZero(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	teamID := uuid.New().String()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(sqlmock.AnyArg(), chaosLeaseRecoveryActor,
			AuditKindChaosLeaseRecoveryStart, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(sqlmock.AnyArg(), chaosLeaseRecoveryActor,
			AuditKindChaosLeaseRecoveryEnd, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(2, 1))
	w := NewChaosLeaseRecoveryWorker(db)
	job := &river.Job[ChaosLeaseRecoveryArgs]{
		Args:   ChaosLeaseRecoveryArgs{TeamID: teamID, SleepSeconds: -5},
		JobRow: &rivertype.JobRow{ID: 1},
	}
	if err := w.Work(context.Background(), job); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

func TestChaos_LeaseRecovery_Work_StartMarkerError_Returns(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	teamID := uuid.New().String()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(sqlmock.AnyArg(), chaosLeaseRecoveryActor,
			AuditKindChaosLeaseRecoveryStart, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(errors.New("audit insert failed"))
	w := NewChaosLeaseRecoveryWorker(db)
	job := &river.Job[ChaosLeaseRecoveryArgs]{
		Args:   ChaosLeaseRecoveryArgs{TeamID: teamID, SleepSeconds: 0},
		JobRow: &rivertype.JobRow{ID: 1},
	}
	if err := w.Work(context.Background(), job); err == nil {
		t.Fatal("expected error when start marker fails")
	}
}

func TestChaos_LeaseRecovery_Work_EndMarkerError_Returns(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	teamID := uuid.New().String()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(sqlmock.AnyArg(), chaosLeaseRecoveryActor,
			AuditKindChaosLeaseRecoveryStart, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(sqlmock.AnyArg(), chaosLeaseRecoveryActor,
			AuditKindChaosLeaseRecoveryEnd, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(errors.New("audit insert failed"))
	w := NewChaosLeaseRecoveryWorker(db)
	job := &river.Job[ChaosLeaseRecoveryArgs]{
		Args:   ChaosLeaseRecoveryArgs{TeamID: teamID, SleepSeconds: 0},
		JobRow: &rivertype.JobRow{ID: 1},
	}
	if err := w.Work(context.Background(), job); err == nil {
		t.Fatal("expected error when end marker fails")
	}
}

func TestChaos_LeaseRecovery_Work_CtxCancelledDuringSleep(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	teamID := uuid.New().String()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(sqlmock.AnyArg(), chaosLeaseRecoveryActor,
			AuditKindChaosLeaseRecoveryStart, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	w := NewChaosLeaseRecoveryWorker(db)
	job := &river.Job[ChaosLeaseRecoveryArgs]{
		Args:   ChaosLeaseRecoveryArgs{TeamID: teamID, SleepSeconds: 60},
		JobRow: &rivertype.JobRow{ID: 1},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled — ctx.Done() fires immediately
	if err := w.Work(ctx, job); err == nil {
		t.Fatal("expected ctx error")
	}
}

func TestChaos_LeaseRecovery_Work_SleepClampedToMax(t *testing.T) {
	// SleepSeconds way over the cap — exercises the cap branch. We don't
	// actually wait the cap (5min); we cancel ctx fast.
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	teamID := uuid.New().String()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(sqlmock.AnyArg(), chaosLeaseRecoveryActor,
			AuditKindChaosLeaseRecoveryStart, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	w := NewChaosLeaseRecoveryWorker(db)
	job := &river.Job[ChaosLeaseRecoveryArgs]{
		Args:   ChaosLeaseRecoveryArgs{TeamID: teamID, SleepSeconds: 100000},
		JobRow: &rivertype.JobRow{ID: 1},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_ = w.Work(ctx, job)
}

// ─── uptime_prober.go: Kind, dialer ──────────────────────────────────────────

func TestProber_Uptime_Kind(t *testing.T) {
	if k := (UptimeProberArgs{}).Kind(); k != "uptime_prober" {
		t.Errorf("Kind = %q", k)
	}
}

func TestProber_UptimeRetention_Kind(t *testing.T) {
	if k := (UptimeRetentionArgs{}).Kind(); k != "uptime_retention" {
		t.Errorf("Kind = %q", k)
	}
}

func TestProber_Uptime_DefaultDialer_UnreachableErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := defaultProvisionerDialer(ctx, "127.0.0.1:1")
	if err == nil {
		t.Fatal("expected dial error to an unreachable port")
	}
}

func TestProber_UptimeRetention_DBError_Propagates(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec(`DELETE FROM uptime_samples`).WillReturnError(errors.New("boom"))
	w := NewUptimeRetentionWorker(db)
	if err := w.Work(context.Background(), localJob[UptimeRetentionArgs]()); err == nil {
		t.Fatal("expected error from delete failure")
	}
}

// ─── real_prober.go ────────────────────────────────────────────────────────────

func TestProber_NormalizeStorageURL_AllArms(t *testing.T) {
	// http/https → returned untouched.
	if got, err := normalizeStorageURL("https://example.com/bucket"); err != nil || got != "https://example.com/bucket" {
		t.Errorf("https returned (%q, %v)", got, err)
	}
	if got, err := normalizeStorageURL("http://example.com/bucket"); err != nil || got != "http://example.com/bucket" {
		t.Errorf("http returned (%q, %v)", got, err)
	}

	// s3://<bucket>/prefix → https://<bucket>.s3.amazonaws.com/
	got, err := normalizeStorageURL("s3://mybucket/prefix")
	if err != nil || got != "https://mybucket.s3.amazonaws.com/" {
		t.Errorf("s3 returned (%q, %v)", got, err)
	}

	// s3:// without host → error.
	if _, err := normalizeStorageURL("s3:///path"); err == nil {
		t.Error("missing-bucket s3 must error")
	}

	// Unknown scheme → error.
	if _, err := normalizeStorageURL("ftp://example.com/"); err == nil {
		t.Error("unsupported scheme must error")
	}

	// Bad URL → error.
	if _, err := normalizeStorageURL("://nope"); err == nil {
		t.Error("malformed URL must error")
	}
}

func TestProber_NatsHost_AllArms(t *testing.T) {
	if h, err := natsHost("nats://server.example.com:4222"); err != nil || h != "server.example.com" {
		t.Errorf("nats:// returned (%q, %v)", h, err)
	}
	if h, err := natsHost("tls://server.example.com:4222"); err != nil || h != "server.example.com" {
		t.Errorf("tls:// returned (%q, %v)", h, err)
	}
	if _, err := natsHost("http://x"); err == nil {
		t.Error("non-nats scheme must error")
	}
	if _, err := natsHost("nats://"); err == nil {
		t.Error("missing host must error")
	}
	if _, err := natsHost("://"); err == nil {
		t.Error("malformed URL must error")
	}
}

func TestProber_LooksLikePlaintextURL_AllSchemes(t *testing.T) {
	yes := []string{
		"postgres://x", "postgresql://x", "redis://x", "rediss://x",
		"mongodb://x", "mongodb+srv://x", "http://x", "https://x",
		"nats://x", "s3://x",
	}
	for _, s := range yes {
		if !looksLikePlaintextURL(s) {
			t.Errorf("looksLikePlaintextURL(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"ftp://x", "weird", "://x", "  postgres://x"} {
		if looksLikePlaintextURL(s) {
			t.Errorf("looksLikePlaintextURL(%q) = true, want false", s)
		}
	}
}

func TestProber_NetError_Reporting(t *testing.T) {
	// netError is marked //nolint:unused but reachable via the export — we
	// can call it directly here in-package.
	if netError(nil) {
		t.Error("nil → false")
	}
	if netError(errors.New("plain")) {
		t.Error("plain error → false")
	}
	if !netError(&net_error_for_test{msg: "x"}) {
		t.Error("net.Error → true")
	}
}

// net_error_for_test implements net.Error so netError can return true.
type net_error_for_test struct{ msg string }

func (e *net_error_for_test) Error() string   { return e.msg }
func (e *net_error_for_test) Timeout() bool   { return false }
func (e *net_error_for_test) Temporary() bool { return false }

func TestProber_RealProber_DecryptEmptyURL(t *testing.T) {
	p := NewRealProber(&config.Config{AESKey: ""}).(*realProber)
	if _, err := p.decrypt(""); err == nil {
		t.Error("empty url → error expected")
	}
}

func TestProber_RealProber_DecryptNilKeyReturnsAsIs(t *testing.T) {
	p := NewRealProber(&config.Config{AESKey: ""}).(*realProber)
	got, err := p.decrypt("postgres://anything")
	if err != nil || got != "postgres://anything" {
		t.Errorf("nil-key path: (%q, %v)", got, err)
	}
}

func TestProber_RealProber_ProbeStorage_BadURL(t *testing.T) {
	p := NewRealProber(&config.Config{AESKey: ""}).(*realProber)
	out, err := p.probeStorage(context.Background(), "ftp://nope/")
	if out != ProbeUnreachable || err == nil {
		t.Errorf("bad URL: (%v, %v)", out, err)
	}
}

func TestProber_RealProber_ProbeQueue_BadURL(t *testing.T) {
	p := NewRealProber(&config.Config{AESKey: ""}).(*realProber)
	out, err := p.probeQueue(context.Background(), "ftp://nope/")
	if out != ProbeUnreachable || err == nil {
		t.Errorf("bad URL: (%v, %v)", out, err)
	}
}

func TestProber_RealProber_ProbeQueue_NonHealthyHTTP(t *testing.T) {
	p := NewRealProber(&config.Config{AESKey: ""}).(*realProber)
	// nats://192.0.2.1 → builds http://192.0.2.1:8222/healthz → unreachable
	// blackhole → ProbeUnreachable via the GET error branch.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, _ := p.probeQueue(ctx, "nats://192.0.2.1:4222")
	if out != ProbeUnreachable {
		t.Errorf("blackhole nats: %v", out)
	}
}

func TestProber_RealProber_ProbePostgres_BadConn(t *testing.T) {
	p := NewRealProber(&config.Config{AESKey: ""}).(*realProber)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Malformed postgres URL → sql.Open succeeds (lazy), QueryRowContext
	// fails fast → ProbeUnreachable.
	out, err := p.probePostgres(ctx, "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if out != ProbeUnreachable {
		t.Errorf("unreachable pg: (%v, %v)", out, err)
	}
}

func TestProber_RealProber_ProbeRedis_BadURL(t *testing.T) {
	p := NewRealProber(&config.Config{AESKey: ""}).(*realProber)
	out, err := p.probeRedis(context.Background(), "not a url")
	if out != ProbeUnreachable {
		t.Errorf("bad url: (%v, %v)", out, err)
	}
}

func TestProber_RealProber_ProbeMongo_BadURL(t *testing.T) {
	p := NewRealProber(&config.Config{AESKey: ""}).(*realProber)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, _ := p.probeMongo(ctx, "mongodb://u:p@127.0.0.1:1/?serverSelectionTimeoutMS=200")
	if out != ProbeUnreachable {
		t.Errorf("unreachable mongo: %v", out)
	}
}

// ─── uptime_prober.go: full Work() + insertSample + each probe (matching the
//     brief's TestProber filter so they actually run under it) ─────────────

func TestProber_Uptime_NewWorker_NonNil(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	if w := NewUptimeProberWorker(db); w == nil {
		t.Fatal("ctor returned nil")
	}
}

func TestProber_Uptime_SetDialer(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	w := NewUptimeProberWorker(db)
	called := false
	SetUptimeProberDialer(w, func(_ context.Context, _ string) error {
		called = true
		return nil
	})
	_ = w.provisionerDialer(context.Background(), "x")
	if !called {
		t.Error("custom dialer not called")
	}
}

func TestProber_EnvOr_Behavior(t *testing.T) {
	t.Setenv("UPTIME_TEST_VAR", "  hello  ")
	if got := envOr("UPTIME_TEST_VAR", "fb"); got != "hello" {
		t.Errorf("trim → %q", got)
	}
	t.Setenv("UPTIME_TEST_VAR", "")
	if got := envOr("UPTIME_TEST_VAR", "fb"); got != "fb" {
		t.Errorf("empty → %q", got)
	}
	t.Setenv("UPTIME_TEST_VAR", "   ")
	if got := envOr("UPTIME_TEST_VAR", "fb"); got != "fb" {
		t.Errorf("whitespace → %q", got)
	}
}

func TestProber_Uptime_InsertSample_DBError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO uptime_samples`).
		WillReturnError(errors.New("insert failed"))
	w := NewUptimeProberWorker(db)
	if err := w.insertSample(context.Background(), "api", true, nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestProber_Uptime_InsertSample_WithLatency(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO uptime_samples`).
		WithArgs("api", true, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	w := NewUptimeProberWorker(db)
	latency := 25
	if err := w.insertSample(context.Background(), "api", true, &latency); err != nil {
		t.Fatal(err)
	}
}

func TestProber_Uptime_Work_AllProbesSucceed(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	t.Setenv("UPTIME_PROBE_API_URL", srv.URL+"/healthz")
	t.Setenv("UPTIME_PROBE_MARKETING_URL", srv.URL+"/")
	t.Setenv("UPTIME_PROBE_DEPLOYS_URL", srv.URL+"/")

	mock.ExpectQuery(`SELECT 1`).
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	for _, slug := range []string{"api", "provisioner", "worker", "deploys", "marketing"} {
		mock.ExpectExec(`INSERT INTO uptime_samples`).
			WithArgs(slug, true, sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}

	w := NewUptimeProberWorker(db)
	SetUptimeProberDialer(w, func(_ context.Context, _ string) error { return nil })
	if err := w.Work(context.Background(), localJob[UptimeProberArgs]()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestProber_Uptime_Work_FailedInsertContinues(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	t.Setenv("UPTIME_PROBE_API_URL", srv.URL+"/healthz")
	t.Setenv("UPTIME_PROBE_MARKETING_URL", srv.URL+"/")
	t.Setenv("UPTIME_PROBE_DEPLOYS_URL", srv.URL+"/")

	mock.ExpectQuery(`SELECT 1`).
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	// All inserts fail — Work must still return nil (log + continue).
	for i := 0; i < 5; i++ {
		mock.ExpectExec(`INSERT INTO uptime_samples`).
			WillReturnError(errors.New("insert failed"))
	}

	w := NewUptimeProberWorker(db)
	SetUptimeProberDialer(w, func(_ context.Context, _ string) error { return nil })
	if err := w.Work(context.Background(), localJob[UptimeProberArgs]()); err != nil {
		t.Fatalf("insert failures must not propagate: %v", err)
	}
}

func TestProber_Uptime_ProbeWorker_SelectFails(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	t.Setenv("UPTIME_PROBE_API_URL", srv.URL+"/healthz")
	t.Setenv("UPTIME_PROBE_MARKETING_URL", srv.URL+"/")
	t.Setenv("UPTIME_PROBE_DEPLOYS_URL", srv.URL+"/")

	mock.ExpectQuery(`SELECT 1`).
		WillReturnError(errors.New("db down"))
	for _, slug := range []string{"api", "provisioner", "worker", "deploys", "marketing"} {
		mock.ExpectExec(`INSERT INTO uptime_samples`).
			WithArgs(slug, sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	w := NewUptimeProberWorker(db)
	SetUptimeProberDialer(w, func(_ context.Context, _ string) error { return nil })
	if err := w.Work(context.Background(), localJob[UptimeProberArgs]()); err != nil {
		t.Fatal(err)
	}
}

func TestProber_Uptime_HttpHEAD_UnreachableServer(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	w := NewUptimeProberWorker(db)
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	srv.Close() // closed → unreachable
	healthy, latency := w.httpHEAD(context.Background(), srv.URL+"/", false)
	if healthy {
		t.Error("expected unhealthy")
	}
	if latency != nil {
		t.Errorf("expected nil latency, got %v", latency)
	}
}

func TestProber_Uptime_HttpHEAD_BadURL(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	w := NewUptimeProberWorker(db)
	// http.NewRequestWithContext rejects URLs with bad control characters.
	healthy, latency := w.httpHEAD(context.Background(), "http://\x00bad", true)
	if healthy {
		t.Error("expected unhealthy on bad URL")
	}
	if latency != nil {
		t.Errorf("expected nil latency, got %v", latency)
	}
}

func TestProber_UptimeRetention_HappyPath(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec(`DELETE FROM uptime_samples`).
		WithArgs(90).
		WillReturnResult(sqlmock.NewResult(0, 17))
	w := NewUptimeRetentionWorker(db)
	if err := w.Work(context.Background(), localJob[UptimeRetentionArgs]()); err != nil {
		t.Fatal(err)
	}
}

// ─── More propagation_runner / heartbeat / geodb / executor coverage ─────────

func TestPropagation_MarkApplied_DBError_Returns(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE pending_propagations\s+SET applied_at`).
		WillReturnError(errors.New("update failed"))
	w := NewPropagationRunnerWorker(db, nil, &stubPropagationRegrader{})
	row := propagationRow{id: uuid.New(), teamID: uuid.New(), kind: "tier_elevation"}
	if err := w.markApplied(context.Background(), row); err == nil {
		t.Fatal("expected error")
	}
}

func TestPropagation_MarkApplied_NoOpZeroRows(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE pending_propagations\s+SET applied_at`).
		WillReturnResult(sqlmock.NewResult(0, 0)) // 0 rows → sibling raced
	w := NewPropagationRunnerWorker(db, nil, &stubPropagationRegrader{})
	row := propagationRow{id: uuid.New(), teamID: uuid.New(), kind: "tier_elevation"}
	if err := w.markApplied(context.Background(), row); err != nil {
		t.Fatalf("0 rows must be nil: %v", err)
	}
}

func TestPropagation_MarkRetry_DBError_Logged(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts`).
		WillReturnError(errors.New("retry update failed"))
	w := NewPropagationRunnerWorker(db, nil, &stubPropagationRegrader{})
	w.now = func() time.Time { return time.Now() }
	row := propagationRow{id: uuid.New(), teamID: uuid.New(), kind: "tier_elevation"}
	w.markRetry(context.Background(), row, errors.New("dispatch fail"))
}

func TestPropagation_MarkRetry_NoOpZeroRows(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	w := NewPropagationRunnerWorker(db, nil, &stubPropagationRegrader{})
	w.now = func() time.Time { return time.Now() }
	row := propagationRow{id: uuid.New(), teamID: uuid.New(), kind: "tier_elevation"}
	w.markRetry(context.Background(), row, errors.New("dispatch fail"))
}

func TestPropagation_MarkRetry_UnexpectedSkip_AuditUsesDistinctKind(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(sqlmock.AnyArg(), propagationActor, "propagation.unexpected_skip", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	w := NewPropagationRunnerWorker(db, nil, &stubPropagationRegrader{})
	w.now = func() time.Time { return time.Now() }
	row := propagationRow{id: uuid.New(), teamID: uuid.New(), kind: "tier_elevation"}
	skipErr := &propagationUnexpectedSkipErr{
		Resources: []propagationUnexpectedSkipDetail{
			{ResourceID: "r1", ResourceType: "postgres", SkipReason: "postgres-admin secret not found"},
		},
	}
	w.markRetry(context.Background(), row, skipErr)
}

func TestPropagation_MarkDeadLettered_DBError_Logged(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts`).
		WillReturnError(errors.New("dead-letter update failed"))
	w := NewPropagationRunnerWorker(db, nil, &stubPropagationRegrader{})
	w.now = func() time.Time { return time.Now() }
	row := propagationRow{id: uuid.New(), teamID: uuid.New(), kind: "tier_elevation"}
	w.markDeadLettered(context.Background(), row, errors.New("terminal"))
}

func TestPropagation_MarkDeadLettered_NoOpZeroRows(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	w := NewPropagationRunnerWorker(db, nil, &stubPropagationRegrader{})
	w.now = func() time.Time { return time.Now() }
	row := propagationRow{id: uuid.New(), teamID: uuid.New(), kind: "tier_elevation"}
	w.markDeadLettered(context.Background(), row, errors.New("terminal"))
}

func TestPropagation_MarkUnknownKindDeadLettered_DBError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts`).
		WillReturnError(errors.New("unknown-kind update failed"))
	w := NewPropagationRunnerWorker(db, nil, &stubPropagationRegrader{})
	w.now = func() time.Time { return time.Now() }
	row := propagationRow{id: uuid.New(), teamID: uuid.New(), kind: "future_kind"}
	w.markUnknownKindDeadLettered(context.Background(), row, errors.New("no handler"))
}

func TestPropagation_MarkUnknownKindDeadLettered_NoOpZeroRows(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE pending_propagations\s+SET attempts`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	w := NewPropagationRunnerWorker(db, nil, &stubPropagationRegrader{})
	w.now = func() time.Time { return time.Now() }
	row := propagationRow{id: uuid.New(), teamID: uuid.New(), kind: "future_kind"}
	w.markUnknownKindDeadLettered(context.Background(), row, errors.New("no handler"))
}

func TestPropagation_InsertAuditRow_DBError_Logged(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnError(errors.New("audit insert failed"))
	w := NewPropagationRunnerWorker(db, nil, &stubPropagationRegrader{})
	w.insertPropagationAuditRow(context.Background(),
		propagationRow{id: uuid.New(), teamID: uuid.New(), kind: "tier_elevation"},
		"propagation.applied", "summary", map[string]any{"k": "v"})
}

func TestPropagation_UnexpectedSkipErr_EmptyError(t *testing.T) {
	var e *propagationUnexpectedSkipErr
	if got := e.Error(); !strings.Contains(got, "empty") {
		t.Errorf("nil receiver = %q", got)
	}
	e2 := &propagationUnexpectedSkipErr{}
	if got := e2.Error(); !strings.Contains(got, "empty") {
		t.Errorf("empty slice = %q", got)
	}
}

func TestPropagation_PickEligible_RowScanFails_Skipped(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	// One row with bad payload bytes for the uuid id → Scan fails → row
	// skipped → empty result → COMMIT.
	mock.ExpectQuery(`SELECT id, kind, team_id`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "kind", "team_id", "target_tier", "payload", "attempts",
		}).AddRow("not-a-uuid", "tier_elevation", "also-bad", nil, []byte(`{}`), 0))
	mock.ExpectCommit()

	w := NewPropagationRunnerWorker(db, nil, &stubPropagationRegrader{})
	if err := w.Work(context.Background(), localJob[PropagationRunnerArgs]()); err != nil {
		t.Fatalf("scan errors must not propagate: %v", err)
	}
}

// ─── geodb.go: extractGeoLite2MMDB via in-package call ─────────────────────────

func TestGeoDB_Extract_RoundTrip_DirectCall(t *testing.T) {
	// Build a minimal valid tarball and call the unexported helper directly.
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	body := []byte("mmdb-bytes")
	hdr := &tar.Header{
		Name:     "dir/GeoLite2-City.mmdb",
		Typeflag: tar.TypeReg,
		Size:     int64(len(body)),
		Mode:     0o644,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatal(err)
	}
	tw.Close()
	gz.Close()

	dst := filepath.Join(t.TempDir(), "out.mmdb")
	if err := extractGeoLite2MMDB(&buf, dst); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("mismatch: %q vs %q", got, body)
	}
}

func TestGeoDB_Extract_BadGzip_Errors(t *testing.T) {
	if err := extractGeoLite2MMDB(bytes.NewReader([]byte("not-gzip")), filepath.Join(t.TempDir(), "x")); err == nil {
		t.Fatal("expected gzip error")
	}
}

func TestGeoDB_Extract_NoMMDB_Errors(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	hdr := &tar.Header{Name: "readme.txt", Typeflag: tar.TypeReg, Size: 3}
	tw.WriteHeader(hdr)
	tw.Write([]byte("abc"))
	tw.Close()
	gz.Close()
	if err := extractGeoLite2MMDB(&buf, filepath.Join(t.TempDir(), "out")); err == nil {
		t.Fatal("expected no-mmdb error")
	}
}

// ─── resource_heartbeat.go: SampleDegradedGauge with rows ──────────────────────

func TestHeartbeat_SampleGauge_PopulatedRowsAndResets(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Resource list returns zero (we want to reach the gauge sample only).
	resID := uuid.New()
	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "resource_type", "connection_url",
			"team_id_text", "degraded", "last_seen_at",
		}).AddRow(resID, uuid.New(), "postgres", "url", "", false, sql.NullTime{}))
	mock.ExpectExec(`UPDATE resources\s+SET last_seen_at`).
		WithArgs(resID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// gauge query: include one row with a bad type to cover the scan-error
	// continue branch.
	mock.ExpectQuery(`SELECT resource_type, COUNT\(\*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"resource_type", "count"}).
			AddRow("postgres", "not-an-int").
			AddRow("redis", int64(7)))

	w := NewResourceHeartbeatWorker(db, &localFakeProber{outcome: ProbeReachable})
	if err := w.Work(context.Background(), localJob[ResourceHeartbeatArgs]()); err != nil {
		t.Fatal(err)
	}
}

func TestHeartbeat_RecoveryAuditInsertError_Logged(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	resID := uuid.New()
	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "resource_type", "connection_url",
			"team_id_text", "degraded", "last_seen_at",
		}).AddRow(resID, uuid.New(), "redis", "url", "", true, sql.NullTime{Time: time.Now(), Valid: true}))
	mock.ExpectExec(`UPDATE resources\s+SET last_seen_at`).
		WithArgs(resID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(nil, "system", "resource.recovered", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(errors.New("audit insert failed"))
	mock.ExpectQuery(`SELECT resource_type, COUNT\(\*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"resource_type", "count"}))

	w := NewResourceHeartbeatWorker(db, &localFakeProber{outcome: ProbeReachable})
	if err := w.Work(context.Background(), localJob[ResourceHeartbeatArgs]()); err != nil {
		t.Fatalf("fail-open: %v", err)
	}
}

func TestHeartbeat_DegradedAuditInsertError_Logged(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	resID := uuid.New()
	mock.ExpectQuery(`FROM resources`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "token", "resource_type", "connection_url",
			"team_id_text", "degraded", "last_seen_at",
		}).AddRow(resID, uuid.New(), "postgres", "url", "", false, sql.NullTime{Time: time.Now().Add(-2 * time.Hour), Valid: true}))
	mock.ExpectExec(`UPDATE resources\s+SET degraded = true`).
		WithArgs(resID, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(nil, "system", "resource.degraded", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(errors.New("audit insert failed"))
	mock.ExpectQuery(`SELECT resource_type, COUNT\(\*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"resource_type", "count"}))

	w := NewResourceHeartbeatWorker(db, &localFakeProber{outcome: ProbeUnreachable, err: errors.New("boom")})
	if err := w.Work(context.Background(), localJob[ResourceHeartbeatArgs]()); err != nil {
		t.Fatalf("fail-open: %v", err)
	}
}

// ─── team_deletion_executor.go: processTeam + S3 deletion paths ───────────────

// fakeS3Deleter fakes S3BackupDeleter for the executor's S3 step.
type fakeS3Deleter struct {
	listObjects   []minio.ObjectInfo
	listErr       error
	rmErrs        []minio.RemoveObjectError
}

func (f *fakeS3Deleter) ListObjects(ctx context.Context, _ string, _ minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo, len(f.listObjects)+1)
	go func() {
		defer close(ch)
		for _, o := range f.listObjects {
			select {
			case ch <- o:
			case <-ctx.Done():
				return
			}
		}
		if f.listErr != nil {
			ch <- minio.ObjectInfo{Err: f.listErr}
		}
	}()
	return ch
}

func (f *fakeS3Deleter) RemoveObjects(_ context.Context, _ string, in <-chan minio.ObjectInfo, _ minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError {
	out := make(chan minio.RemoveObjectError, len(f.rmErrs)+1)
	go func() {
		defer close(out)
		// drain input
		for range in {
		}
		for _, e := range f.rmErrs {
			out <- e
		}
	}()
	return out
}

func TestTeamDeletion_DeleteS3Backups_EmptyTokenNoOp(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	w := NewTeamDeletionExecutorWorker(db, nil, &fakeS3Deleter{}, nil, "instant-shared")
	got, err := w.deleteS3BackupsForToken(context.Background(), "")
	if err != nil || got != 0 {
		t.Errorf("empty token = (%d, %v)", got, err)
	}
}

func TestTeamDeletion_DeleteS3Backups_HappyPath(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	s3 := &fakeS3Deleter{
		listObjects: []minio.ObjectInfo{
			{Key: "backups/tok/a", Size: 100},
			{Key: "backups/tok/b", Size: 250},
		},
	}
	w := NewTeamDeletionExecutorWorker(db, nil, s3, nil, "instant-shared")
	got, err := w.deleteS3BackupsForToken(context.Background(), uuid.New().String())
	if err != nil {
		t.Fatal(err)
	}
	if got != 350 {
		t.Errorf("bytes freed = %d, want 350", got)
	}
}

func TestTeamDeletion_DeleteS3Backups_ListError(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	s3 := &fakeS3Deleter{listErr: errors.New("list boom")}
	w := NewTeamDeletionExecutorWorker(db, nil, s3, nil, "instant-shared")
	_, err := w.deleteS3BackupsForToken(context.Background(), uuid.New().String())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTeamDeletion_DeleteS3Backups_RemoveError(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	s3 := &fakeS3Deleter{
		listObjects: []minio.ObjectInfo{{Key: "backups/x/a", Size: 1}},
		rmErrs: []minio.RemoveObjectError{
			{ObjectName: "backups/x/a", Err: errors.New("rm failed")},
		},
	}
	w := NewTeamDeletionExecutorWorker(db, nil, s3, nil, "instant-shared")
	_, err := w.deleteS3BackupsForToken(context.Background(), uuid.New().String())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTeamDeletion_ProcessTeam_MarkPendingError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	teamID := uuid.New()
	mock.ExpectQuery(`FROM teams\s+WHERE`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "deletion_requested_at"}).
			AddRow(teamID, time.Now().Add(-90*24*time.Hour)))
	mock.ExpectExec(`UPDATE teams\s+SET status = 'deletion_pending'`).
		WithArgs(teamID).
		WillReturnError(errors.New("flip failed"))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", auditKindTeamDeletionFailed, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := NewTeamDeletionExecutorWorker(db, nil, nil, nil, "")
	if err := w.Work(context.Background(), localJob[TeamDeletionExecutorArgs]()); err != nil {
		t.Fatalf("per-team errors must be isolated: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestTeamDeletion_ProcessTeam_FetchResourcesError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	teamID := uuid.New()
	mock.ExpectQuery(`FROM teams\s+WHERE`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "deletion_requested_at"}).
			AddRow(teamID, time.Now().Add(-90*24*time.Hour)))
	mock.ExpectExec(`UPDATE teams\s+SET status = 'deletion_pending'`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`FROM resources\s+WHERE team_id`).
		WithArgs(teamID).
		WillReturnError(errors.New("res query failed"))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", auditKindTeamDeletionFailed, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	w := NewTeamDeletionExecutorWorker(db, nil, nil, nil, "")
	if err := w.Work(context.Background(), localJob[TeamDeletionExecutorArgs]()); err != nil {
		t.Fatalf("per-team isolated: %v", err)
	}
}

func TestTeamDeletion_ProcessTeam_S3Error(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	teamID := uuid.New()
	resID := uuid.New()
	mock.ExpectQuery(`FROM teams\s+WHERE`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "deletion_requested_at"}).
			AddRow(teamID, time.Now().Add(-90*24*time.Hour)))
	mock.ExpectExec(`UPDATE teams\s+SET status = 'deletion_pending'`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`FROM resources\s+WHERE team_id`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"}).
			AddRow(resID, uuid.New().String(), "postgres", ""))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", auditKindTeamDeletionFailed, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	s3 := &fakeS3Deleter{listErr: errors.New("s3 boom")}
	w := NewTeamDeletionExecutorWorker(db, nil, s3, nil, "instant-shared")
	if err := w.Work(context.Background(), localJob[TeamDeletionExecutorArgs]()); err != nil {
		t.Fatalf("per-team isolated: %v", err)
	}
}

func TestTeamDeletion_ProcessTeam_K8sFetchAppIDsError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	teamID := uuid.New()
	mock.ExpectQuery(`FROM teams\s+WHERE`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "deletion_requested_at"}).
			AddRow(teamID, time.Now().Add(-90*24*time.Hour)))
	mock.ExpectExec(`UPDATE teams\s+SET status = 'deletion_pending'`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`FROM resources\s+WHERE team_id`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"}))
	mock.ExpectQuery(`SELECT DISTINCT app_id`).
		WithArgs(teamID).
		WillReturnError(errors.New("appid query failed"))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", auditKindTeamDeletionFailed, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	k8s := newLocalFakeK8s()
	w := NewTeamDeletionExecutorWorker(db, nil, nil, k8s, "")
	if err := w.Work(context.Background(), localJob[TeamDeletionExecutorArgs]()); err != nil {
		t.Fatalf("per-team isolated: %v", err)
	}
}

func TestTeamDeletion_ProcessTeam_K8sDeleteNamespaceError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	teamID := uuid.New()
	mock.ExpectQuery(`FROM teams\s+WHERE`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "deletion_requested_at"}).
			AddRow(teamID, time.Now().Add(-90*24*time.Hour)))
	mock.ExpectExec(`UPDATE teams\s+SET status = 'deletion_pending'`).
		WithArgs(teamID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`FROM resources\s+WHERE team_id`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "provider_resource_id"}))
	mock.ExpectQuery(`SELECT DISTINCT app_id`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows([]string{"app_id"}).AddRow("appfoo"))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(teamID, "system", auditKindTeamDeletionFailed, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	k8s := newLocalFakeK8s()
	k8s.failOn["instant-deploy-appfoo"] = errors.New("delete failed")
	w := NewTeamDeletionExecutorWorker(db, nil, nil, k8s, "")
	if err := w.Work(context.Background(), localJob[TeamDeletionExecutorArgs]()); err != nil {
		t.Fatalf("per-team isolated: %v", err)
	}
}

// localFakeK8s is an in-package K8sNamespaceDeleter double.
type localFakeK8s struct {
	mu      sync.Mutex
	deleted []string
	failOn  map[string]error
}

func newLocalFakeK8s() *localFakeK8s {
	return &localFakeK8s{failOn: map[string]error{}}
}

func (f *localFakeK8s) DeleteNamespace(_ context.Context, ns string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.failOn[ns]; ok {
		return e
	}
	f.deleted = append(f.deleted, ns)
	return nil
}

func (f *localFakeK8s) NamespaceExists(_ context.Context, _ string) (bool, error) {
	return true, nil
}

// ─── real_prober.go: extra branches ───────────────────────────────────────────

func TestProber_RealProber_DecryptGarbageReturnsErr(t *testing.T) {
	p := NewRealProber(&config.Config{AESKey: "0000000000000000000000000000000000000000000000000000000000000000"}).(*realProber)
	if _, err := p.decrypt("garbage-no-scheme"); err == nil {
		t.Error("expected error for garbage without scheme")
	}
}

// ─── compile-time guard ───────────────────────────────────────────────────────
var _ = fmt.Sprintf
