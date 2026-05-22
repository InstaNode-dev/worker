package jobs

// k8s_namespace_client.go — concrete K8sNamespaceDeleter backed by a
// kubernetes.Clientset. Shared by the team_deletion_executor (which deletes
// a deleted team's deploy namespaces) and the orphan_sweep_reconciler
// (which detects + reclaims orphaned namespaces).
//
// WHY ITS OWN FILE
//
// deploy_status_reconcile.go already builds a kubernetes.Clientset for its
// read-only GetDeployment surface. The deletion path needs a *write*
// surface (Namespaces().Delete) plus a List surface for orphan detection.
// Keeping the deleter in its own file makes the blast radius of the write
// capability explicit and gives the orphan-sweep reconciler something to
// import without dragging in the status-reconciler's autopsy machinery.
//
// FAIL-OPEN POSTURE
//
// NewK8sNamespaceClient returns (nil, err) when no cluster is reachable
// (CI, docker-compose). Callers pass nil to the executor / reconciler,
// which then skip the k8s steps with a WARN log — the same posture as
// deployStatusK8sProvider. A worker that cannot reach k8s still runs every
// other periodic job.
//
// IDEMPOTENCY
//
// DeleteNamespace swallows apierrors.IsNotFound — a namespace that is
// already gone (a previous partially-failed teardown deleted it, or the
// deploy-expirer beat us to it) is success, not an error. This is the
// property that makes re-running a failed team deletion safe.

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// k8sNamespaceClient is the concrete K8sNamespaceDeleter. Wraps a
// kubernetes.Clientset.
type k8sNamespaceClient struct {
	cs kubernetes.Interface
}

// NewK8sNamespaceClient builds a K8sNamespaceDeleter from in-cluster config,
// falling back to the default kubeconfig for local dev. Returns (nil, err)
// when neither is reachable — the caller logs and passes nil to the
// executor / reconciler.
func NewK8sNamespaceClient() (K8sNamespaceDeleter, error) {
	cs, err := newDeployK8sClientset()
	if err != nil {
		return nil, err
	}
	return &k8sNamespaceClient{cs: cs}, nil
}

// DeleteNamespace removes the namespace and everything it contains. A
// NotFound namespace is treated as success — that is the idempotency
// contract the executor and the reconciler depend on for safe re-runs.
//
// The Delete call is asynchronous on the k8s side (the namespace enters
// Terminating and the control plane garbage-collects its contents); we do
// not block on completion. The orphan-sweep reconciler's NamespaceExists
// check will still report true for a Terminating namespace, so a namespace
// mid-termination is simply re-observed on the next sweep and re-Delete'd
// (also a no-op) — eventually consistent, never wrong.
func (c *k8sNamespaceClient) DeleteNamespace(ctx context.Context, namespace string) error {
	err := c.cs.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("k8sNamespaceClient.DeleteNamespace %q: %w", namespace, err)
	}
	return nil
}

// NamespaceExists reports whether the namespace is still present (including
// the Terminating phase). NotFound → (false, nil). Any other error is
// surfaced so the reconciler does not mistake an API outage for "orphan
// already cleaned".
func (c *k8sNamespaceClient) NamespaceExists(ctx context.Context, namespace string) (bool, error) {
	_, err := c.cs.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err == nil {
		return true, nil
	}
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return false, fmt.Errorf("k8sNamespaceClient.NamespaceExists %q: %w", namespace, err)
}

// ListDeployNamespaces returns the names of every instant-deploy-* namespace
// currently in the cluster. The orphan-sweep reconciler uses this to find
// namespaces whose owning deployment row / team is gone. Returns an error
// on any API failure so the reconciler skips the k8s-orphan phase rather
// than acting on a truncated list.
func (c *k8sNamespaceClient) ListDeployNamespaces(ctx context.Context) ([]string, error) {
	return c.listNamespacesWithPrefix(ctx, deployNamespacePrefixTDE)
}

// ListCustomerNamespaces returns the names of every instant-customer-*
// namespace currently in the cluster. The orphan-sweep reconciler's PASS 4
// uses this to find customer namespaces whose backing resources row is gone
// (the MR-P0-1b leak: a reaper that marked the row 'deleted' while the
// namespace's backend stayed live). Returns an error on any API failure so
// the reconciler skips the customer-orphan phase rather than acting on a
// truncated list — never delete a namespace off an incomplete picture.
func (c *k8sNamespaceClient) ListCustomerNamespaces(ctx context.Context) ([]string, error) {
	return c.listNamespacesWithPrefix(ctx, customerNamespacePrefix)
}

// ListStackNamespaces returns the names of every instant-stack-* namespace
// currently in the cluster. The orphan-sweep reconciler's PASS 5 (T6 P0-1,
// BugBash 2026-05-20) uses this to find stack namespaces whose backing
// `stacks` row is gone. The pre-fix ExpireStacksWorker carried the wrong
// prefix ("instant-apps-"), causing the safety guard to refuse every real
// stack-namespace delete while still hard-deleting the DB row → orphaned
// namespace + pods + ingress + TLS cert forever with no DB pointer. PASS 5
// is both the catch-up sweep for pre-fix orphans and the durable recurrence
// guard.
func (c *k8sNamespaceClient) ListStackNamespaces(ctx context.Context) ([]string, error) {
	return c.listNamespacesWithPrefix(ctx, ExpireStacksNamespacePrefix)
}

// listNamespacesWithPrefix is the shared cluster-scoped namespace List used
// by both ListDeployNamespaces and ListCustomerNamespaces.
func (c *k8sNamespaceClient) listNamespacesWithPrefix(ctx context.Context, prefix string) ([]string, error) {
	list, err := c.cs.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("k8sNamespaceClient.listNamespacesWithPrefix %q: %w", prefix, err)
	}
	var out []string
	for i := range list.Items {
		name := list.Items[i].Name
		if len(name) >= len(prefix) && name[:len(prefix)] == prefix {
			out = append(out, name)
		}
	}
	return out, nil
}

// GetNamespaceAge returns time.Since(namespace.CreationTimestamp). NotFound
// maps to (0, nil) — the orphan_sweep PASS 3 caller treats that as "the
// namespace was reaped by another path; nothing to do this tick".
func (c *k8sNamespaceClient) GetNamespaceAge(ctx context.Context, namespace string) (time.Duration, error) {
	ns, err := c.cs.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("k8sNamespaceClient.GetNamespaceAge %q: %w", namespace, err)
	}
	created := ns.CreationTimestamp.Time
	if created.IsZero() {
		// A namespace with no CreationTimestamp is anomalous — be
		// conservative and report it as freshly created so the
		// no_db_row reap path skips it this tick.
		return 0, nil
	}
	return time.Since(created), nil
}

// k8sPodStateClient is the production PodStateProvider used by PASS 6.
// Wraps the same kubernetes.Clientset used by k8sNamespaceClient — both
// are constructed in StartWorkers from a single newDeployK8sClientset()
// call so they share a TCP connection pool to the k8s API.
type k8sPodStateClient struct {
	cs kubernetes.Interface
}

// NewK8sPodStateClient builds the PASS 6 pod-state seam. Returns (nil, err)
// when no cluster is reachable — caller passes nil to
// (*OrphanSweepReconciler).WithPodStateProvider and PASS 6 stays disabled.
func NewK8sPodStateClient() (PodStateProvider, error) {
	cs, err := newDeployK8sClientset()
	if err != nil {
		return nil, err
	}
	return &k8sPodStateClient{cs: cs}, nil
}

// ListPodWaitingReasons returns the waiting-state reason of every pod's
// primary container in `namespace`. A pod whose primary container is NOT
// in Waiting (Running, ContainerCreating that has progressed, Terminated)
// contributes "" to the slice — the PASS 6 caller treats any "" as
// "build is progressing; leave alone".
//
// A NotFound on the namespace yields (nil, nil) — the namespace was reaped
// by another path before PASS 6 could check it.
func (c *k8sPodStateClient) ListPodWaitingReasons(ctx context.Context, namespace string) ([]string, error) {
	list, err := c.cs.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("k8sPodStateClient.ListPodWaitingReasons %q: %w", namespace, err)
	}
	out := make([]string, 0, len(list.Items))
	for i := range list.Items {
		pod := &list.Items[i]
		out = append(out, podPrimaryContainerWaitingReason(pod))
	}
	return out, nil
}

// podPrimaryContainerWaitingReason returns the Waiting.Reason of the
// pod's first container, or "" when not in Waiting state. Returning the
// first container's state is sufficient for PASS 6: instant-deploy-*
// pods are single-container by construction (the api's k8s provider
// creates exactly one app container per Deployment).
func podPrimaryContainerWaitingReason(pod *corev1.Pod) string {
	if len(pod.Status.ContainerStatuses) == 0 {
		// Pod hasn't reached the container status step yet (Pending +
		// scheduling). Report empty — caller treats as progressing.
		return ""
	}
	cs := pod.Status.ContainerStatuses[0]
	if cs.State.Waiting != nil {
		return cs.State.Waiting.Reason
	}
	return ""
}
