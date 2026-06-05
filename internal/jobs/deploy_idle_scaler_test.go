package jobs

// deploy_idle_scaler_test.go — coverage for the scale-to-zero idle-scaler
// (Task #54). SQL via sqlmock; k8s via a recording fake scale provider.
//
// Properties pinned:
//   - flag OFF  → Work is a total no-op: NO SQL query is issued, NO scale call.
//   - k8s nil   → Work short-circuits with no SQL query (fail-open).
//   - happy path→ idle candidate is scaled to 0 (recorded) + DB CAS-flipped +
//                 the scaled_down counter + idle-apps gauge update.
//   - CAS race  → UPDATE returns 0 rows → counted as skipped, NOT scaled_down.
//   - NotFound  → torn-down Deployment is skipped (no DB flip, no failure).
//   - scale err → non-NotFound k8s error increments scale_failed, row untouched.
//   - constructor floors a sub-5 idle threshold to 30.
//   - namespaceAndNameFromProviderID derives ns/name and rejects bad shapes.

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/clientcmd"

	"instant.dev/worker/internal/metrics"
)

func idleScalerJob() *river.Job[DeployIdleScalerArgs] {
	return &river.Job[DeployIdleScalerArgs]{JobRow: &rivertype.JobRow{ID: 7}}
}

// recordingScaleProvider records every ScaleDeployment call and can be told to
// return a fixed error (e.g. a synthetic NotFound or transport failure).
type recordingScaleProvider struct {
	mu       sync.Mutex
	calls    []string // "ns/name=replicas"
	err      error
	notFound bool
}

func (r *recordingScaleProvider) ScaleDeployment(_ context.Context, ns, name string, replicas int32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, ns+"/"+name)
	if r.notFound {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "deployments"}, name)
	}
	return r.err
}

func (r *recordingScaleProvider) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

// TestDeployIdleScaler_FlagOffNoOp proves the job is fully inert when the flag
// is off: sqlmock asserts NO query is issued (any query would fail the
// ExpectationsWereMet check since none are registered), and the panicking
// provider would blow up if Work reached the scale layer.
func TestDeployIdleScaler_FlagOffNoOp(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	prov := &recordingScaleProvider{err: errors.New("must not be called when flag off")}
	w := NewDeployIdleScaler(db, prov, false /* enabled */, 30)

	if err := w.Work(context.Background(), idleScalerJob()); err != nil {
		t.Fatalf("flag-off Work should be nil, got: %v", err)
	}
	if prov.callCount() != 0 {
		t.Errorf("flag-off must not call ScaleDeployment; got %d calls", prov.callCount())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("flag-off must issue no SQL: %v", err)
	}
}

// TestDeployIdleScaler_NilK8sNoOp proves a nil k8s client short-circuits before
// any SQL is issued (fail-open at startup).
func TestDeployIdleScaler_NilK8sNoOp(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	w := NewDeployIdleScaler(db, nil /* k8s */, true /* enabled */, 30)
	if err := w.Work(context.Background(), idleScalerJob()); err != nil {
		t.Fatalf("nil-k8s Work should be nil, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("nil-k8s must issue no SQL: %v", err)
	}
}

// TestDeployIdleScaler_ScalesDownIdleApp covers the happy path: one idle
// candidate → scaled to 0 + DB CAS flip + gauge sample.
func TestDeployIdleScaler_ScalesDownIdleApp(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id := uuid.New()
	mock.ExpectQuery(`SELECT id, COALESCE\(provider_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "provider_id"}).
			AddRow(id, "app-abc123"))
	mock.ExpectExec(`UPDATE deployments`).
		WithArgs(id, "healthy").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT count\(\*\) FROM deployments WHERE scaled_to_zero = true`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	prov := &recordingScaleProvider{}
	w := NewDeployIdleScaler(db, prov, true, 30)

	before := testutil.ToFloat64(metrics.DeployScaledToZeroTotal.WithLabelValues("scaled_down"))
	if err := w.Work(context.Background(), idleScalerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 1 {
		t.Fatalf("expected 1 ScaleDeployment call, got %d", prov.callCount())
	}
	if prov.calls[0] != "instant-deploy-abc123/app-abc123" {
		t.Errorf("scaled wrong target: %q", prov.calls[0])
	}
	after := testutil.ToFloat64(metrics.DeployScaledToZeroTotal.WithLabelValues("scaled_down"))
	if after != before+1 {
		t.Errorf("scaled_down counter: before=%v after=%v", before, after)
	}
	if g := testutil.ToFloat64(metrics.DeployIdleApps); g != 1 {
		t.Errorf("idle-apps gauge = %v; want 1", g)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet SQL expectations: %v", err)
	}
}

// TestDeployIdleScaler_CASRaceSkips covers the case where the row was already
// woken/pinned/redeployed between SELECT and UPDATE — UPDATE returns 0 rows,
// so the scaled_down counter must NOT increment.
func TestDeployIdleScaler_CASRaceSkips(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id := uuid.New()
	mock.ExpectQuery(`SELECT id, COALESCE\(provider_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "provider_id"}).
			AddRow(id, "app-raced"))
	mock.ExpectExec(`UPDATE deployments`).
		WithArgs(id, "healthy").
		WillReturnResult(sqlmock.NewResult(0, 0)) // 0 rows = raced
	mock.ExpectQuery(`SELECT count\(\*\) FROM deployments WHERE scaled_to_zero = true`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	prov := &recordingScaleProvider{}
	w := NewDeployIdleScaler(db, prov, true, 30)

	before := testutil.ToFloat64(metrics.DeployScaledToZeroTotal.WithLabelValues("scaled_down"))
	if err := w.Work(context.Background(), idleScalerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	after := testutil.ToFloat64(metrics.DeployScaledToZeroTotal.WithLabelValues("scaled_down"))
	if after != before {
		t.Errorf("CAS-raced row must NOT increment scaled_down: before=%v after=%v", before, after)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet SQL expectations: %v", err)
	}
}

// TestDeployIdleScaler_NotFoundSkips: a torn-down Deployment (NotFound) is
// skipped — no DB flip, no failure counter.
func TestDeployIdleScaler_NotFoundSkips(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id := uuid.New()
	mock.ExpectQuery(`SELECT id, COALESCE\(provider_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "provider_id"}).
			AddRow(id, "app-gone"))
	// No UPDATE expected — NotFound short-circuits before the DB flip.
	mock.ExpectQuery(`SELECT count\(\*\) FROM deployments WHERE scaled_to_zero = true`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	prov := &recordingScaleProvider{notFound: true}
	w := NewDeployIdleScaler(db, prov, true, 30)

	beforeFail := testutil.ToFloat64(metrics.DeployScaledToZeroTotal.WithLabelValues("scale_failed"))
	if err := w.Work(context.Background(), idleScalerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	afterFail := testutil.ToFloat64(metrics.DeployScaledToZeroTotal.WithLabelValues("scale_failed"))
	if afterFail != beforeFail {
		t.Errorf("NotFound must NOT increment scale_failed: before=%v after=%v", beforeFail, afterFail)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet SQL expectations: %v", err)
	}
}

// TestDeployIdleScaler_ScaleErrorCounts: a non-NotFound k8s error increments
// scale_failed and leaves the row untouched (no UPDATE).
func TestDeployIdleScaler_ScaleErrorCounts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id := uuid.New()
	mock.ExpectQuery(`SELECT id, COALESCE\(provider_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "provider_id"}).
			AddRow(id, "app-boom"))
	mock.ExpectQuery(`SELECT count\(\*\) FROM deployments WHERE scaled_to_zero = true`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	prov := &recordingScaleProvider{err: errors.New("k8s boom")}
	w := NewDeployIdleScaler(db, prov, true, 30)

	before := testutil.ToFloat64(metrics.DeployScaledToZeroTotal.WithLabelValues("scale_failed"))
	if err := w.Work(context.Background(), idleScalerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	after := testutil.ToFloat64(metrics.DeployScaledToZeroTotal.WithLabelValues("scale_failed"))
	if after != before+1 {
		t.Errorf("k8s error must increment scale_failed: before=%v after=%v", before, after)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet SQL expectations: %v", err)
	}
}

// TestNewDeployIdleScaler_FloorsIdleMinutes: a sub-5 threshold floors to 30 so
// a misconfig can't make the scaler aggressively flap apps to sleep.
func TestNewDeployIdleScaler_FloorsIdleMinutes(t *testing.T) {
	w := NewDeployIdleScaler(nil, nil, true, 1)
	if w.idleMinutes != 30 {
		t.Errorf("sub-5 idleMinutes should floor to 30; got %d", w.idleMinutes)
	}
	w2 := NewDeployIdleScaler(nil, nil, true, 45)
	if w2.idleMinutes != 45 {
		t.Errorf("valid idleMinutes should pass through; got %d", w2.idleMinutes)
	}
}

// TestNamespaceAndNameFromProviderID covers the derivation + bad-shape rejection.
func TestNamespaceAndNameFromProviderID(t *testing.T) {
	cases := []struct {
		providerID string
		wantNS     string
		wantName   string
	}{
		{"app-abc", "instant-deploy-abc", "app-abc"},
		{"instant-stack-xyz", "", ""}, // stack row — not ours
		{"app-", "", ""},              // empty appID
		{"", "", ""},
	}
	for _, c := range cases {
		ns, name := namespaceAndNameFromProviderID(c.providerID)
		if ns != c.wantNS || name != c.wantName {
			t.Errorf("namespaceAndNameFromProviderID(%q) = (%q,%q); want (%q,%q)",
				c.providerID, ns, name, c.wantNS, c.wantName)
		}
	}
}

// TestDeployIdleScalerArgs_Kind pins the River job kind.
func TestDeployIdleScalerArgs_Kind(t *testing.T) {
	if (DeployIdleScalerArgs{}).Kind() != "deploy_idle_scaler" {
		t.Errorf("Kind() = %q; want deploy_idle_scaler", (DeployIdleScalerArgs{}).Kind())
	}
}

// TestNewK8sDeployScaleClientFromCluster_NoConfig exercises the cluster
// constructor's error path when neither in-cluster config nor a kubeconfig is
// reachable. Gated like TestNewDeployK8sClientset_NoConfig so it does not pick
// up a developer's ~/.kube/config.
func TestNewK8sDeployScaleClientFromCluster_NoConfig(t *testing.T) {
	if _, err := os.Stat(clientcmd.RecommendedHomeFile); err == nil {
		t.Skip("kubeconfig present on host — error path not reachable here")
	}
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		t.Skip("running in-cluster — in-cluster config will succeed")
	}
	if _, err := NewK8sDeployScaleClientFromCluster(); err == nil {
		t.Error("expected error with no in-cluster config and no kubeconfig")
	}
}

// TestDeployIdleScaler_ListQueryError: a failing candidate SELECT bubbles up as
// a job error (River retries).
func TestDeployIdleScaler_ListQueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT id, COALESCE\(provider_id`).
		WillReturnError(errors.New("db down"))
	w := NewDeployIdleScaler(db, &recordingScaleProvider{}, true, 30)
	if err := w.Work(context.Background(), idleScalerJob()); err == nil {
		t.Error("list query error should fail the job")
	}
}

// TestDeployIdleScaler_ScanError: a row whose id column is a non-UUID scrap
// forces a rows.Scan error inside listIdleCandidates → job error.
func TestDeployIdleScaler_ScanError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT id, COALESCE\(provider_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "provider_id"}).
			AddRow("not-a-uuid", "app-x"))
	w := NewDeployIdleScaler(db, &recordingScaleProvider{}, true, 30)
	if err := w.Work(context.Background(), idleScalerJob()); err == nil {
		t.Error("scan error should fail the job")
	}
}

// TestDeployIdleScaler_SkipsForeignProviderID: a candidate whose provider_id is
// not in app-<appID> shape (e.g. a stack row that slipped the SQL filter) is
// skipped without a scale call or DB flip.
func TestDeployIdleScaler_SkipsForeignProviderID(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT id, COALESCE\(provider_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "provider_id"}).
			AddRow(uuid.New(), "instant-stack-xyz"))
	mock.ExpectQuery(`SELECT count\(\*\) FROM deployments WHERE scaled_to_zero = true`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	prov := &recordingScaleProvider{}
	w := NewDeployIdleScaler(db, prov, true, 30)
	if err := w.Work(context.Background(), idleScalerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if prov.callCount() != 0 {
		t.Errorf("foreign provider_id must not be scaled; got %d calls", prov.callCount())
	}
}

// TestDeployIdleScaler_DBFlipError: a failing scaled_to_zero UPDATE after a
// successful scale increments scale_failed (the row is retried next tick).
func TestDeployIdleScaler_DBFlipError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	id := uuid.New()
	mock.ExpectQuery(`SELECT id, COALESCE\(provider_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "provider_id"}).AddRow(id, "app-dbflip"))
	mock.ExpectExec(`UPDATE deployments`).
		WithArgs(id, "healthy").
		WillReturnError(errors.New("update exploded"))
	mock.ExpectQuery(`SELECT count\(\*\) FROM deployments WHERE scaled_to_zero = true`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	prov := &recordingScaleProvider{}
	w := NewDeployIdleScaler(db, prov, true, 30)

	before := testutil.ToFloat64(metrics.DeployScaledToZeroTotal.WithLabelValues("scale_failed"))
	if err := w.Work(context.Background(), idleScalerJob()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	after := testutil.ToFloat64(metrics.DeployScaledToZeroTotal.WithLabelValues("scale_failed"))
	if after != before+1 {
		t.Errorf("db-flip error must increment scale_failed: before=%v after=%v", before, after)
	}
}

// TestDeployIdleScaler_GaugeSampleError: a failing countAsleep query is logged
// but does not fail the job (the scale-down already succeeded).
func TestDeployIdleScaler_GaugeSampleError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	id := uuid.New()
	mock.ExpectQuery(`SELECT id, COALESCE\(provider_id`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "provider_id"}).AddRow(id, "app-g"))
	mock.ExpectExec(`UPDATE deployments`).
		WithArgs(id, "healthy").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT count\(\*\) FROM deployments WHERE scaled_to_zero = true`).
		WillReturnError(errors.New("count failed"))
	w := NewDeployIdleScaler(db, &recordingScaleProvider{}, true, 30)
	if err := w.Work(context.Background(), idleScalerJob()); err != nil {
		t.Fatalf("gauge-sample error must not fail the job, got: %v", err)
	}
}

// TestK8sDeployScaleClient_ScaleDeployment covers the production scale client
// against a fake clientset: scale a seeded Deployment to 0, an already-at-target
// no-op, and a NotFound on a missing Deployment.
func TestK8sDeployScaleClient_ScaleDeployment(t *testing.T) {
	one := int32(1)
	cs := clientfake.NewSimpleClientset(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "app-x", Namespace: "instant-deploy-x"},
		Spec:       appsv1.DeploymentSpec{Replicas: &one},
	})
	c := NewK8sDeployScaleClient(cs)

	// Scale down to 0.
	if err := c.ScaleDeployment(context.Background(), "instant-deploy-x", "app-x", 0); err != nil {
		t.Fatalf("ScaleDeployment(0): %v", err)
	}
	got, _ := cs.AppsV1().Deployments("instant-deploy-x").Get(context.Background(), "app-x", metav1.GetOptions{})
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 0 {
		t.Errorf("replicas after scale-down = %v; want 0", got.Spec.Replicas)
	}

	// Already at 0 → idempotent no-op (no error).
	if err := c.ScaleDeployment(context.Background(), "instant-deploy-x", "app-x", 0); err != nil {
		t.Errorf("ScaleDeployment(0) idempotent should be nil: %v", err)
	}

	// Missing Deployment → NotFound surfaced (caller maps to skip).
	err := c.ScaleDeployment(context.Background(), "instant-deploy-missing", "app-missing", 0)
	if !apierrors.IsNotFound(err) {
		t.Errorf("ScaleDeployment on missing Deployment should return NotFound, got: %v", err)
	}
}
