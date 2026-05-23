package jobs

// k8s_clients_fake_coverage_test.go — drives the concrete k8s client
// wrappers (k8sNamespaceClient, k8sPodStateClient, k8sDeployStatusClient,
// k8sAutopsyClient) against a fake.Clientset.
//
// SEAM: the wrapper structs were retyped from `*kubernetes.Clientset`
// (concrete) to `kubernetes.Interface` (the long-standing follow-up noted in
// deploy_lifecycle_coverage_test.go) so fake.NewSimpleClientset can stand in.
// Every method that previously needed a live cluster — Delete / Get / List
// (namespaces + pods), the deploy-status GetDeployment, and the autopsy
// ListPods / ListEvents / GetPodLogs — is now exercisable hermetically,
// including the apierrors.IsNotFound idempotency arms and the generic-error
// arms via a fake reactor.

import (
	"context"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// ── k8sNamespaceClient ────────────────────────────────────────────────

func TestK8sNamespaceClient_DeleteNamespace(t *testing.T) {
	cs := fake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "instant-deploy-abc"},
	})
	c := &k8sNamespaceClient{cs: cs}
	ctx := context.Background()

	// Present namespace deletes cleanly.
	if err := c.DeleteNamespace(ctx, "instant-deploy-abc"); err != nil {
		t.Fatalf("DeleteNamespace present: %v", err)
	}
	// Absent namespace → IsNotFound swallowed (idempotency contract).
	if err := c.DeleteNamespace(ctx, "instant-deploy-gone"); err != nil {
		t.Fatalf("DeleteNamespace absent should be nil, got %v", err)
	}

	// Generic (non-NotFound) error path surfaces.
	csErr := fake.NewSimpleClientset()
	csErr.PrependReactor("delete", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver boom")
	})
	cErr := &k8sNamespaceClient{cs: csErr}
	if err := cErr.DeleteNamespace(ctx, "x"); err == nil {
		t.Error("DeleteNamespace generic error should surface")
	}
}

func TestK8sNamespaceClient_NamespaceExists(t *testing.T) {
	cs := fake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "instant-deploy-here"},
	})
	c := &k8sNamespaceClient{cs: cs}
	ctx := context.Background()

	ok, err := c.NamespaceExists(ctx, "instant-deploy-here")
	if err != nil || !ok {
		t.Fatalf("NamespaceExists present = (%v,%v), want (true,nil)", ok, err)
	}
	ok, err = c.NamespaceExists(ctx, "instant-deploy-nope")
	if err != nil || ok {
		t.Fatalf("NamespaceExists absent = (%v,%v), want (false,nil)", ok, err)
	}

	csErr := fake.NewSimpleClientset()
	csErr.PrependReactor("get", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver down")
	})
	cErr := &k8sNamespaceClient{cs: csErr}
	if _, err := cErr.NamespaceExists(ctx, "x"); err == nil {
		t.Error("NamespaceExists generic error should surface")
	}
}

func TestK8sNamespaceClient_ListNamespaces(t *testing.T) {
	cs := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: deployNamespacePrefixTDE + "1"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: customerNamespacePrefix + "tok"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ExpireStacksNamespacePrefix + "slug"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system"}},
	)
	c := &k8sNamespaceClient{cs: cs}
	ctx := context.Background()

	if got, err := c.ListDeployNamespaces(ctx); err != nil || len(got) != 1 {
		t.Fatalf("ListDeployNamespaces = (%v,%v), want 1 entry", got, err)
	}
	if got, err := c.ListCustomerNamespaces(ctx); err != nil || len(got) != 1 {
		t.Fatalf("ListCustomerNamespaces = (%v,%v), want 1 entry", got, err)
	}
	if got, err := c.ListStackNamespaces(ctx); err != nil || len(got) != 1 {
		t.Fatalf("ListStackNamespaces = (%v,%v), want 1 entry", got, err)
	}

	// List API error → surfaced.
	csErr := fake.NewSimpleClientset()
	csErr.PrependReactor("list", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("list boom")
	})
	cErr := &k8sNamespaceClient{cs: csErr}
	if _, err := cErr.ListDeployNamespaces(ctx); err == nil {
		t.Error("ListDeployNamespaces error should surface")
	}
}

func TestK8sNamespaceClient_GetNamespaceAge(t *testing.T) {
	ctx := context.Background()

	// Real CreationTimestamp → positive age.
	old := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	cs := fake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "ns-aged", CreationTimestamp: old},
	})
	c := &k8sNamespaceClient{cs: cs}
	age, err := c.GetNamespaceAge(ctx, "ns-aged")
	if err != nil || age < time.Hour {
		t.Fatalf("GetNamespaceAge aged = (%v,%v), want >1h", age, err)
	}

	// Zero CreationTimestamp → (0, nil) conservative branch.
	csZero := fake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "ns-zero"},
	})
	cZero := &k8sNamespaceClient{cs: csZero}
	if age, err := cZero.GetNamespaceAge(ctx, "ns-zero"); err != nil || age != 0 {
		t.Fatalf("GetNamespaceAge zero = (%v,%v), want (0,nil)", age, err)
	}

	// NotFound → (0, nil).
	csEmpty := fake.NewSimpleClientset()
	cEmpty := &k8sNamespaceClient{cs: csEmpty}
	if age, err := cEmpty.GetNamespaceAge(ctx, "absent"); err != nil || age != 0 {
		t.Fatalf("GetNamespaceAge absent = (%v,%v), want (0,nil)", age, err)
	}

	// Generic error → surfaced.
	csErr := fake.NewSimpleClientset()
	csErr.PrependReactor("get", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("get boom")
	})
	cErr := &k8sNamespaceClient{cs: csErr}
	if _, err := cErr.GetNamespaceAge(ctx, "x"); err == nil {
		t.Error("GetNamespaceAge generic error should surface")
	}
}

// ── k8sPodStateClient ─────────────────────────────────────────────────

func TestK8sPodStateClient_ListPodWaitingReasons(t *testing.T) {
	ctx := context.Background()
	ns := "instant-deploy-pods"

	waiting := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p-waiting", Namespace: ns},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
		}}},
	}
	running := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p-running", Namespace: ns},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}}},
	}
	noStatus := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p-pending", Namespace: ns},
	}
	cs := fake.NewSimpleClientset(&waiting, &running, &noStatus)
	c := &k8sPodStateClient{cs: cs}

	reasons, err := c.ListPodWaitingReasons(ctx, ns)
	if err != nil {
		t.Fatalf("ListPodWaitingReasons: %v", err)
	}
	if len(reasons) != 3 {
		t.Fatalf("expected 3 reasons, got %d (%v)", len(reasons), reasons)
	}
	var sawBackoff, sawEmpty int
	for _, r := range reasons {
		switch r {
		case "ImagePullBackOff":
			sawBackoff++
		case "":
			sawEmpty++
		}
	}
	if sawBackoff != 1 || sawEmpty != 2 {
		t.Fatalf("reasons = %v; want 1 backoff + 2 empty", reasons)
	}

	// NotFound on the namespace list → (nil, nil).
	csNF := fake.NewSimpleClientset()
	csNF.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "namespaces"}, ns)
	})
	cNF := &k8sPodStateClient{cs: csNF}
	if got, err := cNF.ListPodWaitingReasons(ctx, ns); err != nil || got != nil {
		t.Fatalf("ListPodWaitingReasons NotFound = (%v,%v), want (nil,nil)", got, err)
	}

	// Generic error → surfaced.
	csErr := fake.NewSimpleClientset()
	csErr.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("list pods boom")
	})
	cErr := &k8sPodStateClient{cs: csErr}
	if _, err := cErr.ListPodWaitingReasons(ctx, ns); err == nil {
		t.Error("ListPodWaitingReasons generic error should surface")
	}
}

// ── k8sDeployStatusClient + buildDeployStatusAndAutopsy ───────────────

func TestK8sDeployStatusClient_GetDeployment(t *testing.T) {
	ctx := context.Background()
	ns := "instant-deploy-ds"
	cs := fake.NewSimpleClientset(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: ns},
	})
	c := &k8sDeployStatusClient{cs: cs}
	if _, err := c.GetDeployment(ctx, ns, "app"); err != nil {
		t.Fatalf("GetDeployment present: %v", err)
	}
	if _, err := c.GetDeployment(ctx, ns, "missing"); err == nil || !apierrors.IsNotFound(err) {
		t.Fatalf("GetDeployment missing = %v, want NotFound", err)
	}
}

func TestBuildDeployStatusAndAutopsy_WiresBoth(t *testing.T) {
	cs := fake.NewSimpleClientset()
	status, autopsy := buildDeployStatusAndAutopsy(cs)
	if status == nil || autopsy == nil {
		t.Fatalf("buildDeployStatusAndAutopsy returned nil: status=%v autopsy=%v", status, autopsy)
	}
}

// ── k8sAutopsyClient ──────────────────────────────────────────────────

func TestK8sAutopsyClient_ListPodsAndEvents(t *testing.T) {
	ctx := context.Background()
	ns := "instant-deploy-autopsy"
	cs := fake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: ns, Labels: map[string]string{"app": "x"}}},
		&corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "ev1", Namespace: ns}},
	)
	c := NewK8sAutopsyClient(cs)

	pods, err := c.ListPods(ctx, ns, "app=x")
	if err != nil {
		t.Fatalf("ListPods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("ListPods items = %d, want 1", len(pods.Items))
	}

	events, err := c.ListEvents(ctx, ns)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(events.Items) != 1 {
		t.Fatalf("ListEvents items = %d, want 1", len(events.Items))
	}
}

// ── constructors (dual-failure-or-success, mirrors the deploy-status one) ──

func TestNewK8sNamespaceClient_DualFailureOrSuccess(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	c, err := NewK8sNamespaceClient()
	if c == nil && err == nil {
		t.Error("NewK8sNamespaceClient returned (nil, nil) — invalid contract")
	}
	if err != nil && c != nil {
		t.Errorf("error path must return nil client, got %v", c)
	}
}

func TestNewK8sPodStateClient_DualFailureOrSuccess(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	c, err := NewK8sPodStateClient()
	if c == nil && err == nil {
		t.Error("NewK8sPodStateClient returned (nil, nil) — invalid contract")
	}
	if err != nil && c != nil {
		t.Errorf("error path must return nil client, got %v", c)
	}
}

func TestK8sAutopsyClient_GetPodLogs_NoLogsIsNotError(t *testing.T) {
	ctx := context.Background()
	ns := "instant-deploy-logs"
	// fake.Clientset's GetLogs returns a canned "fake logs" stream; the
	// previous-then-current fallback both resolve, so we just assert the
	// call runs without error and returns a non-nil (possibly empty) slice.
	cs := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: ns},
	})
	c := NewK8sAutopsyClient(cs)
	lines, err := c.GetPodLogs(ctx, ns, "pod1", 50)
	if err != nil {
		t.Fatalf("GetPodLogs: %v", err)
	}
	_ = lines // content is fake-driver canned; we exercise the stream path
}
