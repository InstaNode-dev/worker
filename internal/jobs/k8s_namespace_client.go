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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// k8sNamespaceClient is the concrete K8sNamespaceDeleter. Wraps a
// kubernetes.Clientset.
type k8sNamespaceClient struct {
	cs *kubernetes.Clientset
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
	list, err := c.cs.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("k8sNamespaceClient.ListDeployNamespaces: %w", err)
	}
	var out []string
	for i := range list.Items {
		name := list.Items[i].Name
		if len(name) >= len(deployNamespacePrefixTDE) &&
			name[:len(deployNamespacePrefixTDE)] == deployNamespacePrefixTDE {
			out = append(out, name)
		}
	}
	return out, nil
}
