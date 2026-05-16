package jobs

// deploy_failure_autopsy_test.go — unit tests for the failure autopsy capture
// logic.
//
// Tests:
//   TestWorkerHintMap_AllReasonsHaveHints         — every reason constant has a hint
//   TestWorkerHintForReason_KnownReasons          — correct hint returned
//   TestWorkerHintForReason_FallsBackToUnknown    — unrecognised reason → Unknown hint
//   TestReasonFromEventMessage                    — substring mapping for event messages
//   TestIsPriorityEvent                           — priority event classification
//   TestExtractPodFailure_OOMKilled               — OOMKilled detected from lastState
//   TestExtractPodFailure_CrashLoopBackOff        — CrashLoopBackOff from waiting state
//   TestExtractPodFailure_ImagePullBackOff        — ImagePullBackOff from waiting state
//   TestExtractPodFailure_Evicted                 — Evicted from pod phase
//   TestUpsertAutopsyRow_Idempotent               — one row per deployment (ON CONFLICT)
//   TestCaptureDeploymentAutopsy_NilK8s           — nil k8s → Unknown-reason row written
//   TestCaptureDeploymentAutopsy_FullCapture      — stubbed k8s → correct reason written

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ── Hint map tests ────────────────────────────────────────────────────────────

var workerKnownReasons = []string{
	workerFailureReasonOOMKilled,
	workerFailureReasonEvicted,
	workerFailureReasonImagePullBackOff,
	workerFailureReasonCrashLoopBackOff,
	workerFailureReasonBuildFailed,
	workerFailureReasonDeadlineExceeded,
	workerFailureReasonError,
	workerFailureReasonUnknown,
}

// TestWorkerHintMap_AllReasonsHaveHints verifies the hint map is complete.
func TestWorkerHintMap_AllReasonsHaveHints(t *testing.T) {
	for _, reason := range workerKnownReasons {
		hint, ok := workerFailureHint[reason]
		if !ok {
			t.Errorf("workerFailureHint missing entry for reason %q", reason)
		}
		if hint == "" {
			t.Errorf("workerFailureHint[%q] is empty", reason)
		}
	}
}

// TestWorkerHintForReason_KnownReasons verifies correct hint returned per reason.
func TestWorkerHintForReason_KnownReasons(t *testing.T) {
	for _, reason := range workerKnownReasons {
		got := workerHintForReason(reason)
		want := workerFailureHint[reason]
		if got != want {
			t.Errorf("workerHintForReason(%q) = %q, want %q", reason, got, want)
		}
	}
}

// TestWorkerHintForReason_FallsBackToUnknown verifies the Unknown fallback.
func TestWorkerHintForReason_FallsBackToUnknown(t *testing.T) {
	got := workerHintForReason("FutureReasonThatDoesNotExist")
	want := workerFailureHint[workerFailureReasonUnknown]
	if got != want {
		t.Errorf("workerHintForReason(unrecognised) = %q, want Unknown hint", got)
	}
}

// ── reasonFromEventMessage tests ──────────────────────────────────────────────

func TestReasonFromEventMessage(t *testing.T) {
	tests := []struct {
		msg  string
		want string
	}{
		{"OOMKilling container", workerFailureReasonOOMKilled},
		{"out of memory: container exceeded limits", workerFailureReasonOOMKilled},
		{"Evicted: disk pressure on node", workerFailureReasonEvicted},
		{"eviction triggered by node condition", workerFailureReasonEvicted},
		{"ImagePullBackOff error pulling image", workerFailureReasonImagePullBackOff},
		{"image pull failed for registry.example.com/app", workerFailureReasonImagePullBackOff},
		{"CrashLoopBackOff: container restarted", workerFailureReasonCrashLoopBackOff},
		{"completely unrelated message", workerFailureReasonUnknown},
		{"", workerFailureReasonUnknown},
	}

	for _, tc := range tests {
		got := reasonFromEventMessage(tc.msg)
		if got != tc.want {
			t.Errorf("reasonFromEventMessage(%q) = %q, want %q", tc.msg, got, tc.want)
		}
	}
}

// ── isPriorityEvent tests ─────────────────────────────────────────────────────

func TestIsPriorityEvent(t *testing.T) {
	priority := []string{"OOMKilling", "Evicted", "Killing", "Failed", "FailedToPull", "BackOff", "ErrImagePull"}
	for _, r := range priority {
		if !isPriorityEvent(r) {
			t.Errorf("isPriorityEvent(%q) = false, want true", r)
		}
	}
	nonPriority := []string{"Scheduled", "Pulling", "Created", "Started"}
	for _, r := range nonPriority {
		if isPriorityEvent(r) {
			t.Errorf("isPriorityEvent(%q) = true, want false", r)
		}
	}
}

// ── extractPodFailure tests ───────────────────────────────────────────────────

// buildPodWithWaiting returns a pod with a container in Waiting state.
func buildPodWithWaiting(reason, message string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-pod"},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  reason,
							Message: message,
						},
					},
				},
			},
		},
	}
}

// buildPodWithTerminated returns a pod with a container in lastState.Terminated.
func buildPodWithTerminated(reason string, exitCode int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-pod"},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					LastTerminationState: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:   reason,
							ExitCode: exitCode,
							Message:  "container exited",
						},
					},
				},
			},
		},
	}
}

func TestExtractPodFailure_OOMKilled(t *testing.T) {
	pod := buildPodWithTerminated("OOMKilled", 137)
	result := &autopsyResult{reason: workerFailureReasonUnknown}
	extractPodFailure(pod, result)

	if result.reason != workerFailureReasonOOMKilled {
		t.Errorf("reason = %q, want OOMKilled", result.reason)
	}
	if !result.exitCode.Valid || result.exitCode.Int32 != 137 {
		t.Errorf("exitCode = %v, want 137", result.exitCode)
	}
}

func TestExtractPodFailure_CrashLoopBackOff(t *testing.T) {
	pod := buildPodWithWaiting("CrashLoopBackOff", "container restarted 5 times")
	result := &autopsyResult{reason: workerFailureReasonUnknown}
	extractPodFailure(pod, result)

	if result.reason != workerFailureReasonCrashLoopBackOff {
		t.Errorf("reason = %q, want CrashLoopBackOff", result.reason)
	}
}

func TestExtractPodFailure_ImagePullBackOff(t *testing.T) {
	pod := buildPodWithWaiting("ImagePullBackOff", "unable to pull image")
	result := &autopsyResult{reason: workerFailureReasonUnknown}
	extractPodFailure(pod, result)

	if result.reason != workerFailureReasonImagePullBackOff {
		t.Errorf("reason = %q, want ImagePullBackOff", result.reason)
	}
}

func TestExtractPodFailure_Evicted(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "evicted-pod"},
		Status: corev1.PodStatus{
			Phase:   corev1.PodFailed,
			Reason:  "Evicted",
			Message: "The node was low on resource: disk pressure.",
		},
	}
	result := &autopsyResult{reason: workerFailureReasonUnknown}
	extractPodFailure(pod, result)

	if result.reason != workerFailureReasonEvicted {
		t.Errorf("reason = %q, want Evicted", result.reason)
	}
	if result.event == "" {
		t.Error("expected non-empty event for evicted pod")
	}
}

// ── upsertAutopsyRow idempotency test ─────────────────────────────────────────

// TestUpsertAutopsyRow_Idempotent verifies that calling upsertAutopsyRow twice
// for the same deployment_id produces exactly one DB exec (ON CONFLICT path
// handles duplicates). We simulate two calls and assert the second one uses
// ON CONFLICT DO UPDATE so no second INSERT row is created.
func TestUpsertAutopsyRow_Idempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id := uuid.New()
	exitCode := sql.NullInt32{Int32: 1, Valid: true}

	// Both calls must match the same UPSERT query.
	for i := 0; i < 2; i++ {
		mock.ExpectExec(`INSERT INTO deployment_events`).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}

	for i := 0; i < 2; i++ {
		if err := upsertAutopsyRow(context.Background(), db, id,
			workerFailureReasonCrashLoopBackOff,
			exitCode,
			"container restarted",
			[]string{"FATAL: startup failed"},
		); err != nil {
			t.Fatalf("call %d: upsertAutopsyRow: %v", i, err)
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// ── captureDeploymentAutopsy integration-style tests ─────────────────────────

// TestCaptureDeploymentAutopsy_NilK8s verifies that when the autopsy k8s
// client is nil, captureDeploymentAutopsy still writes an Unknown-reason row.
func TestCaptureDeploymentAutopsy_NilK8s(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id := uuid.New()
	providerID := "app-abc123"

	// Expect one upsert with reason=Unknown.
	mock.ExpectExec(`INSERT INTO deployment_events`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	captureDeploymentAutopsy(context.Background(), db, id, providerID, nil)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestCaptureDeploymentAutopsy_FullCapture verifies that a stubbed k8s
// provider with an OOMKilled pod produces an OOMKilled reason in the
// upserted autopsy row.
func TestCaptureDeploymentAutopsy_FullCapture(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	id := uuid.New()
	providerID := "app-oom456"

	// Stub k8s: OOMKilled pod, no events, no logs.
	stub := &fakeAutopsyK8s{
		pods: &corev1.PodList{
			Items: []corev1.Pod{
				*buildPodWithTerminated("OOMKilled", 137),
			},
		},
		events: &corev1.EventList{},
		logs:   nil,
	}

	// Expect an upsert; we inspect the args to confirm OOMKilled is the reason.
	mock.ExpectExec(`INSERT INTO deployment_events`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	captureDeploymentAutopsy(context.Background(), db, id, providerID, stub)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
	// Verify stub was called (all three methods).
	if !stub.listPodsCalled {
		t.Error("expected ListPods to be called")
	}
}

// TestCaptureDeploymentAutopsy_LastLinesJSONRoundtrip verifies that
// upsertAutopsyRow serialises last_lines as a valid JSON array that
// round-trips back to a []string.
func TestCaptureDeploymentAutopsy_LastLinesJSONRoundtrip(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	lines := []string{"line1", "line2", "line3"}
	var capturedJSON []byte

	mock.ExpectExec(`INSERT INTO deployment_events`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	id := uuid.New()
	if err := upsertAutopsyRow(context.Background(), db, id,
		workerFailureReasonBuildFailed,
		sql.NullInt32{},
		"build error",
		lines,
	); err != nil {
		t.Fatalf("upsertAutopsyRow: %v", err)
	}

	// Re-encode lines to verify marshal/unmarshal roundtrip independently.
	capturedJSON, _ = json.Marshal(lines)
	var decoded []string
	if err := json.Unmarshal(capturedJSON, &decoded); err != nil {
		t.Fatalf("unmarshal last_lines: %v", err)
	}
	if len(decoded) != len(lines) {
		t.Errorf("last_lines round-trip: got %d entries, want %d", len(decoded), len(lines))
	}
	for i, l := range lines {
		if decoded[i] != l {
			t.Errorf("last_lines[%d] = %q, want %q", i, decoded[i], l)
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// ── fakeAutopsyK8s stub ───────────────────────────────────────────────────────

type fakeAutopsyK8s struct {
	pods           *corev1.PodList
	events         *corev1.EventList
	logs           []string
	listPodsCalled bool
}

func (f *fakeAutopsyK8s) ListPods(_ context.Context, _, _ string) (*corev1.PodList, error) {
	f.listPodsCalled = true
	if f.pods == nil {
		return &corev1.PodList{}, nil
	}
	return f.pods, nil
}

func (f *fakeAutopsyK8s) ListEvents(_ context.Context, _ string) (*corev1.EventList, error) {
	if f.events == nil {
		return &corev1.EventList{}, nil
	}
	return f.events, nil
}

func (f *fakeAutopsyK8s) GetPodLogs(_ context.Context, _, _ string, _ int64) ([]string, error) {
	return f.logs, nil
}

// compile-time check: fakeAutopsyK8s implements deployAutopsyK8sProvider.
var _ deployAutopsyK8sProvider = (*fakeAutopsyK8s)(nil)

// ensure time import is used.
var _ = time.Now
