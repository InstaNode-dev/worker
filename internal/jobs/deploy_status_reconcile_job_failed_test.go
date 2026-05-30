package jobs

// deploy_status_reconcile_job_failed_test.go — coverage for the silent-deploy-
// failure fix (2026-05-30 incident: a user's deploy sat at `building` forever
// because the kaniko build Job hit BackoffLimitExceeded and the build pod was
// GC'd before the pre-fix reconciler could observe a runtime Deployment).
//
// Two failure surfaces this file pins:
//
//   1. Reconciler MUST flip to `failed` when the build Job is Failed even
//      after the pod is GC'd. The Job object survives its
//      TTLSecondsAfterFinished window (5 min in the api) — long enough for
//      the 30s reconciler tick to read it.
//
//   2. Reconciler MUST NOT flip prematurely while the Job still has retries
//      left (Status.Failed <= BackoffLimit and no JobFailed condition). A
//      Failed-pod count during retries is normal — the user incident WAS a
//      terminal failure, not a transient retry.
//
// Mirrors fail-open posture: a transport-level Job-query error logs and
// falls through to the Deployment-based mapping, never blocks the row.

import (
	"context"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

// newHealthyDeployment is a tiny helper that returns an appsv1.Deployment
// in the "healthy" shape (AvailableReplicas=1) used by the
// JobQueryError fall-through test.
func newHealthyDeployment() *appsv1.Deployment {
	return &appsv1.Deployment{
		Status: appsv1.DeploymentStatus{AvailableReplicas: 1},
	}
}

// newFakeClientsetForBuildJob returns a real kubernetes.Interface (the
// k8s fake clientset). Used by TestK8sDeployStatusClient_GetBuildJob_NotFoundPath
// to exercise the production wrapper's BatchV1 dispatch.
func newFakeClientsetForBuildJob() kubernetes.Interface {
	return fake.NewSimpleClientset()
}

// helper: a Job with a Failed condition stamped — the modal BackoffLimit case.
func jobBackoffLimitExceeded() *batchv1.Job {
	bl := int32(2)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "build-gced"},
		Spec:       batchv1.JobSpec{BackoffLimit: &bl},
		Status: batchv1.JobStatus{
			Failed: 3, // BackoffLimit + 1 — k8s declared the Job dead.
			Conditions: []batchv1.JobCondition{{
				Type:    batchv1.JobFailed,
				Status:  corev1.ConditionTrue,
				Reason:  "BackoffLimitExceeded",
				Message: "Job has reached the specified backoff limit",
			}},
		},
	}
}

// helper: a Job actively retrying — Failed count > 0 but BackoffLimit not yet reached.
func jobActiveRetrying() *batchv1.Job {
	bl := int32(3)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "build-active"},
		Spec:       batchv1.JobSpec{BackoffLimit: &bl},
		Status: batchv1.JobStatus{
			Active: 1, // one pod currently running
			Failed: 2, // <= BackoffLimit, still has retries
			// No JobFailed condition yet.
		},
	}
}

// TestDeployStatusReconcile_JobFailedAfterPodGC is the PRIMARY guard for the
// silent-deploy-failure bug class. Setup mirrors the user's incident:
//
//   - deployments row at status='building' (api goroutine crashed mid-build
//     or never got to stamp the terminal status)
//   - The runtime Deployment was never created (typical of build-time failure)
//     → GetDeployment returns NotFound
//   - The build Job's pod has been GC'd by k8s, but the Job object remains
//     within its TTLSecondsAfterFinished window with Status.Failed=3 and a
//     JobFailed condition stamped
//
// The pre-fix reconciler mapped this to `stopped` (terminal, but looks like a
// teardown). The fixed reconciler MUST flip the row to `failed`, enqueue an
// autopsy upsert (the existing in-sweep capture path), and increment the
// instant_deploy_job_failed_detected_total counter.
func TestDeployStatusReconcile_JobFailedAfterPodGC(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id := uuid.New()
	mock.ExpectQuery(`FROM deployments\s+WHERE status IN`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "provider_id", "status"}).
			AddRow(id, "app-gced", "building"))

	k8s := newFakeDeployStatusK8s()
	// Deployment is missing (build never reached the apply step) — pre-fix
	// behaviour mapped this to "stopped".
	// Job exists with Failed condition — post-fix behaviour MUST detect
	// this as a terminal build failure.
	k8s.jobs["instant-deploy-gced|build-gced"] = jobBackoffLimitExceeded()

	// Expectations: autopsy upsert (kind=failure_autopsy) THEN status UPDATE to failed.
	mock.ExpectExec(`INSERT INTO deployment_events`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE deployments\s+SET status = \$1`).
		WithArgs(deployStatusFailed, id, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Pass an empty autopsy stub so the in-sweep capture writes an Unknown-
	// reason row (we only care about the status flip + autopsy fired).
	w := NewDeployStatusReconciler(db, k8s).WithAutopsyK8s(&fakeAutopsyK8sCov{})
	if err := w.Work(context.Background(), fakeRiverJob[DeployStatusReconcileArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestDeployStatusReconcile_JobActiveAndPodMissing_StaysBuilding is the
// complement: a build still has retries left (Status.Failed <= BackoffLimit,
// no JobFailed condition). The reconciler MUST NOT flip the row to failed —
// the build may yet recover. The runtime Deployment is also missing (the
// apply step hasn't run yet because the build is in flight). Expected
// outcome: row stays at `building`; no UPDATE happens (newStatus ==
// currentStatus).
func TestDeployStatusReconcile_JobActiveAndPodMissing_StaysBuilding(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id := uuid.New()
	mock.ExpectQuery(`FROM deployments\s+WHERE status IN`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "provider_id", "status"}).
			AddRow(id, "app-active", "building"))

	k8s := newFakeDeployStatusK8s()
	// Deployment missing — apply step hasn't run yet.
	// Job active — Failed=2, BackoffLimit=3 → still retrying.
	k8s.jobs["instant-deploy-active|build-active"] = jobActiveRetrying()

	// No autopsy upsert expected. No UPDATE expected (newStatus stays
	// "building" == currentStatus, so the sweep loop continues without
	// writing). sqlmock will fail if an unexpected exec arrives.

	w := NewDeployStatusReconciler(db, k8s).WithAutopsyK8s(&fakeAutopsyK8sCov{})
	if err := w.Work(context.Background(), fakeRiverJob[DeployStatusReconcileArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestDeployStatusReconcile_BothNotFound_StaysStopped covers the legacy
// path: a row whose Deployment AND build Job have both been reaped (real
// teardown). MUST stay mapped to `stopped`.
func TestDeployStatusReconcile_BothNotFound_StaysStopped(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id := uuid.New()
	mock.ExpectQuery(`FROM deployments\s+WHERE status IN`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "provider_id", "status"}).
			AddRow(id, "app-gone", "building"))

	// Both Deployment AND Job missing from the fake → NewNotFound errors.
	k8s := newFakeDeployStatusK8s()

	mock.ExpectExec(`UPDATE deployments\s+SET status = \$1`).
		WithArgs(deployStatusStopped, id, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := NewDeployStatusReconciler(db, k8s)
	if err := w.Work(context.Background(), fakeRiverJob[DeployStatusReconcileArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestDeployStatusReconcile_JobQueryError_FallsThroughToDeployment guards the
// fail-open posture: a transport-level error on the Job query MUST log+continue
// and let the Deployment-based mapping decide the row's status. The row in
// this test has a healthy Deployment so we should observe the legacy healthy
// transition even though the Job query failed.
func TestDeployStatusReconcile_JobQueryError_FallsThroughToDeployment(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id := uuid.New()
	mock.ExpectQuery(`FROM deployments\s+WHERE status IN`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "provider_id", "status"}).
			AddRow(id, "app-h1", "building"))

	k8s := newFakeDeployStatusK8s()
	// Healthy runtime Deployment — Deployment query MUST be authoritative.
	k8s.objs["instant-deploy-h1|app-h1"] = newHealthyDeployment()
	// Build Job query returns a non-NotFound transport error.
	k8s.jobErrOn["instant-deploy-h1|build-h1"] = errors.New("connection refused (mock)")

	mock.ExpectExec(`UPDATE deployments\s+SET status = \$1`).
		WithArgs(deployStatusHealthy, id, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := NewDeployStatusReconciler(db, k8s)
	if err := w.Work(context.Background(), fakeRiverJob[DeployStatusReconcileArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestJobIsFailed_Matrix covers the helper's predicate truth-table. We pin
// every branch (no conditions, Failed condition true/false, BackoffLimit
// exceeded via Status.Failed) so a future refactor that loosens the predicate
// (e.g. treats Active Failed-count as terminal) fails CI rather than
// reintroducing flapping false-positive flips.
func TestJobIsFailed_Matrix(t *testing.T) {
	one := int32(1)
	two := int32(2)
	cases := []struct {
		name string
		job  *batchv1.Job
		want bool
	}{
		{name: "nil job", job: nil, want: false},
		{name: "no conditions, no failed pods", job: &batchv1.Job{}, want: false},
		{
			name: "Failed condition stamped True is failed",
			job: &batchv1.Job{Status: batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"}},
			}},
			want: true,
		},
		{
			name: "Failed condition stamped False is not failed",
			job: &batchv1.Job{Status: batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionFalse}},
			}},
			want: false,
		},
		{
			name: "Status.Failed > BackoffLimit is failed (cluster-version backstop)",
			job: &batchv1.Job{
				Spec:   batchv1.JobSpec{BackoffLimit: &one},
				Status: batchv1.JobStatus{Failed: 2},
			},
			want: true,
		},
		{
			name: "Status.Failed == BackoffLimit is NOT failed (one retry left)",
			job: &batchv1.Job{
				Spec:   batchv1.JobSpec{BackoffLimit: &two},
				Status: batchv1.JobStatus{Failed: 2},
			},
			want: false,
		},
		{
			name: "Status.Failed > 0 with nil BackoffLimit is failed (default zero)",
			job: &batchv1.Job{
				Status: batchv1.JobStatus{Failed: 1},
			},
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := jobIsFailed(tc.job); got != tc.want {
				t.Errorf("jobIsFailed = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestJobFailureReason covers the metrics-label helper. Bounded label cardinality
// is enforced by jobFailureReason's small return set; this test pins each branch.
func TestJobFailureReason(t *testing.T) {
	cases := []struct {
		name string
		job  *batchv1.Job
		want string
	}{
		{name: "nil job", job: nil, want: "unknown"},
		{
			name: "Failed condition with reason",
			job: &batchv1.Job{Status: batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}},
			}},
			want: "BackoffLimitExceeded",
		},
		{
			name: "Failed condition without reason",
			job: &batchv1.Job{Status: batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}},
			}},
			want: "failed_no_reason",
		},
		{
			name: "no Failed condition (backstop bucket)",
			job:  &batchv1.Job{},
			want: "backoff_limit_exceeded",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := jobFailureReason(tc.job); got != tc.want {
				t.Errorf("jobFailureReason = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestK8sDeployStatusClient_GetBuildJob_NotFoundPath proves the production
// wrapper around BatchV1().Jobs().Get() forwards NotFound errors verbatim.
// Uses a fake.Clientset (already imported in deploy_lifecycle_coverage_test.go)
// so we exercise the real BatchV1 dispatch path without a live cluster.
func TestK8sDeployStatusClient_GetBuildJob_NotFoundPath(t *testing.T) {
	cs := newFakeClientsetForBuildJob() // empty fake → Get returns NotFound
	c := &k8sDeployStatusClient{cs: cs}
	_, err := c.GetBuildJob(context.Background(), "instant-deploy-x", "build-x")
	if err == nil {
		t.Fatal("expected NotFound error from empty fake clientset")
	}
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected apierrors.IsNotFound, got %v", err)
	}
}

// ensure helper imports are used.
var _ = schema.GroupResource{}
