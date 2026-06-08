package jobs

// deploy_runtime_failed_metric_test.go — covers the runtime rollout-failure
// detector wired into computeNewStatus (broken-image silent-failure fix,
// 2026-06-08). Asserts BOTH that a ProgressDeadlineExceeded rollout maps to
// "failed" AND that instant_deploy_runtime_failed_detected_total increments for
// that path only (and NOT for a generic DeploymentReplicaFailure, which is a
// distinct cause out of this counter's scope).

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus/testutil"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"instant.dev/worker/internal/metrics"
)

func TestComputeNewStatus_ProgressDeadlineExceeded_FailsAndCountsMetric(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	k8s := newFakeDeployStatusK8s()
	// Rollout exceeded its progress deadline with no available replica —
	// the broken-image runtime failure (container can't start).
	k8s.objs["instant-deploy-pde|app-pde"] = &appsv1.Deployment{
		Status: appsv1.DeploymentStatus{
			UnavailableReplicas: 1,
			Conditions: []appsv1.DeploymentCondition{{
				Type:   appsv1.DeploymentProgressing,
				Status: corev1.ConditionFalse,
				Reason: progressDeadlineExceededReason,
			}},
		},
	}
	w := NewDeployStatusReconciler(db, k8s)

	before := testutil.ToFloat64(
		metrics.DeployRuntimeFailedDetectedTotal.WithLabelValues(runtimeFailReasonProgressDeadline))

	status, err := w.computeNewStatus(context.Background(), "app-pde")
	if err != nil {
		t.Fatalf("computeNewStatus: %v", err)
	}
	if status != deployStatusFailed {
		t.Errorf("status = %q, want failed", status)
	}

	after := testutil.ToFloat64(
		metrics.DeployRuntimeFailedDetectedTotal.WithLabelValues(runtimeFailReasonProgressDeadline))
	if after-before != 1 {
		t.Errorf("DeployRuntimeFailedDetectedTotal delta = %v, want 1", after-before)
	}
}

// TestComputeNewStatus_ReplicaFailure_DoesNotCountRuntimeMetric pins the
// attribution boundary: a DeploymentReplicaFailure also maps to "failed" but
// must NOT increment the runtime-progress-deadline counter (distinct cause).
func TestComputeNewStatus_ReplicaFailure_DoesNotCountRuntimeMetric(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	k8s := newFakeDeployStatusK8s()
	k8s.objs["instant-deploy-rf|app-rf"] = &appsv1.Deployment{
		Status: appsv1.DeploymentStatus{
			Conditions: []appsv1.DeploymentCondition{{
				Type:   appsv1.DeploymentReplicaFailure,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	w := NewDeployStatusReconciler(db, k8s)

	before := testutil.ToFloat64(
		metrics.DeployRuntimeFailedDetectedTotal.WithLabelValues(runtimeFailReasonProgressDeadline))

	status, err := w.computeNewStatus(context.Background(), "app-rf")
	if err != nil {
		t.Fatalf("computeNewStatus: %v", err)
	}
	if status != deployStatusFailed {
		t.Errorf("status = %q, want failed", status)
	}

	after := testutil.ToFloat64(
		metrics.DeployRuntimeFailedDetectedTotal.WithLabelValues(runtimeFailReasonProgressDeadline))
	if after != before {
		t.Errorf("replica-failure must not bump runtime counter: delta = %v", after-before)
	}
}
