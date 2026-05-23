package jobs

// deploy_failure_autopsy.go — Failure Autopsy Phase 0 capture logic.
//
// When DeployStatusReconciler detects a transition into "failed", it calls
// captureDeploymentAutopsy to collect the structured cause from the live k8s
// API and upsert a deployment_events row with kind='failure_autopsy'.
//
// The api's GET /deploy/:id and GET /api/v1/deployments/:id handlers read that
// row and surface it in the optional "failure" field of the response:
//
//   "failure": {
//     "reason":      "OOMKilled|Evicted|ImagePullBackOff|CrashLoopBackOff|...",
//     "exit_code":   <int|null>,
//     "event":       "<k8s event message>",
//     "last_lines":  ["<log line>", ...],  // up to 200, oldest-first
//     "hint":        "<plain-language cause + remedy>",
//     "occurred_at": "<RFC3339>"
//   }
//
// DESIGN
//
// The table lives in the api module's migration 050; the worker accesses it via
// plain database/sql without importing the api module (same pattern as
// deploy_status_reconcile.go duplicating its status strings). If the api's
// column layout ever changes, update upsertAutopsyRow here too.
//
// The capture is idempotent. The deployment_events_autopsy_uniq partial unique
// index (deployment_id, kind) WHERE kind='failure_autopsy' prevents duplicate
// rows; ON CONFLICT DO UPDATE makes repeated ticks overwrite the previous
// row rather than inserting a new one. A re-queued tick for the same failure
// is therefore a silent no-op at the DB level.
//
// RBAC
//
// The worker's extended k8s interface (deployStatusAutopsyK8sProvider) adds
// three methods beyond what deployStatusK8sProvider already has:
//   - GetPod       — read the pod's lastState.Terminated for exit code + reason
//   - ListEvents   — read Namespace-scoped events (OOMKilled, Evicted, ...)
//   - GetPodLogs   — tail the last ~200 lines for context
//
// All three read from the same per-deployment namespace ("instant-deploy-<appID>")
// that the existing deploy-status reconciler already queries. The RBAC
// ClusterRole "instant-worker-deploy-reader" (infra/k8s/worker-rbac.yaml) needs:
//   - pods/get, pods/list (new)
//   - events/get, events/list (new)
//   - pods/log (new)
//
// FAIL-OPEN POSTURE
//
// If the k8s call for pod details / events / logs fails, captureDeploymentAutopsy
// still writes an autopsy row with reason=Unknown and an empty last_lines slice.
// A DB write failure is logged and swallowed — other reconcile ticks keep running.

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ── Failure reason constants ──────────────────────────────────────────────────
//
// Mirror of api/internal/models/deployment_event.go constants. Duplicated
// because the worker module does not import the api module. Keep in sync.

const (
	workerFailureReasonOOMKilled        = "OOMKilled"
	workerFailureReasonEvicted          = "Evicted"
	workerFailureReasonImagePullBackOff = "ImagePullBackOff"
	workerFailureReasonCrashLoopBackOff = "CrashLoopBackOff"
	workerFailureReasonBuildFailed      = "BuildFailed"
	workerFailureReasonDeadlineExceeded = "DeadlineExceeded"
	workerFailureReasonError            = "Error"
	workerFailureReasonUnknown          = "Unknown"
)

// deploymentEventKindFailureAutopsy mirrors api/internal/models/deployment_event.go.
const deploymentEventKindFailureAutopsy = "failure_autopsy"

// ── Hint map ─────────────────────────────────────────────────────────────────
//
// Mirror of api/internal/models/deployment_failure_hints.go FailureHint.
// Duplicated (no shared import) — keep in sync.

var workerFailureHint = map[string]string{
	workerFailureReasonOOMKilled: "Your app exceeded its memory limit and was killed by the kernel. " +
		"Reduce memory usage, add GOMEMLIMIT / NODE_OPTIONS --max-old-space-size, " +
		"or upgrade to a tier with a higher memory cap.",

	workerFailureReasonEvicted: "Your app's pod was evicted from the node — this usually means the node " +
		"ran out of disk space or memory. Check for excessive logging or large temporary files. " +
		"Upgrade your tier for a dedicated node with more headroom.",

	workerFailureReasonImagePullBackOff: "Kubernetes could not pull your container image. " +
		"This is usually a registry authentication failure or a typo in the image reference. " +
		"Re-deploy with a fresh tarball to trigger a new build and push.",

	workerFailureReasonCrashLoopBackOff: "Your app container exited non-zero repeatedly. " +
		"Check the last_lines for stack traces or startup errors. " +
		"Common causes: missing environment variable, wrong PORT binding, or a top-level exception at startup.",

	workerFailureReasonBuildFailed: "The Kaniko image build failed before your app was deployed. " +
		"Check the event field for the build error. " +
		"Common causes: Dockerfile syntax error, missing COPY source file, or a failing RUN command.",

	workerFailureReasonDeadlineExceeded: "The build or rollout timed out after 10 minutes. " +
		"Large base images or slow package installs can cause this. " +
		"Try a smaller base image (e.g. alpine) and pre-install dependencies in the Dockerfile.",

	workerFailureReasonError: "A Kubernetes replica failure was detected. " +
		"This is often a transient scheduling or resource constraint. " +
		"Re-deploy to retry; if it persists, check your Dockerfile for correct CMD/ENTRYPOINT.",

	workerFailureReasonUnknown: "The failure cause could not be determined automatically. " +
		"Stream the pod logs via GET /deploy/:id/logs and check for error messages at the bottom.",
}

// workerHintForReason returns the plain-language hint for a FailureReason.
func workerHintForReason(reason string) string {
	if h, ok := workerFailureHint[reason]; ok {
		return h
	}
	return workerFailureHint[workerFailureReasonUnknown]
}

// ── Extended k8s interface ────────────────────────────────────────────────────

// deployAutopsyK8sProvider extends the narrow interface used by the status
// reconciler with the three additional methods needed for autopsy capture.
// Defined as an interface so tests can stub the k8s API without a real cluster.
type deployAutopsyK8sProvider interface {
	// ListPods returns pods in a namespace matching the label selector.
	ListPods(ctx context.Context, namespace, labelSelector string) (*corev1.PodList, error)
	// ListEvents returns events in a namespace, ordered by lastTimestamp DESC.
	ListEvents(ctx context.Context, namespace string) (*corev1.EventList, error)
	// GetPodLogs returns the last tailLines lines from a pod's main container.
	// Returns (nil, nil) when the pod has no logs (e.g. image-pull failure).
	GetPodLogs(ctx context.Context, namespace, podName string, tailLines int64) ([]string, error)
}

// k8sAutopsyClient is the production deployAutopsyK8sProvider backed by a
// real kubernetes.Clientset. The status reconciler's k8sDeployStatusClient
// wraps the same Clientset for GetDeployment; both can share a single
// kubernetes.Clientset instance at startup.
type k8sAutopsyClient struct {
	cs kubernetes.Interface
}

// ListPods implements deployAutopsyK8sProvider.
func (c *k8sAutopsyClient) ListPods(ctx context.Context, namespace, labelSelector string) (*corev1.PodList, error) {
	return c.cs.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
}

// ListEvents implements deployAutopsyK8sProvider.
func (c *k8sAutopsyClient) ListEvents(ctx context.Context, namespace string) (*corev1.EventList, error) {
	return c.cs.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
}

// GetPodLogs implements deployAutopsyK8sProvider.
func (c *k8sAutopsyClient) GetPodLogs(ctx context.Context, namespace, podName string, tailLines int64) ([]string, error) {
	req := c.cs.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		TailLines: &tailLines,
		// previous=true tries to get logs from the last terminated container
		// when the pod is restarting (CrashLoopBackOff). Fall back to current
		// when previous fails.
		Previous: true,
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		// Previous container may not exist yet; retry without Previous.
		req2 := c.cs.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
			TailLines: &tailLines,
		})
		stream, err = req2.Stream(ctx)
		if err != nil {
			// ImagePullBackOff pods have no logs — not an error.
			return nil, nil //nolint:nilerr
		}
	}
	defer stream.Close()
	return readLogLines(stream)
}

// NewK8sAutopsyClient constructs a k8sAutopsyClient from the same clientset
// used by the status reconciler. Call this in StartWorkers after constructing
// the status reconciler's client so both share one TCP connection pool.
func NewK8sAutopsyClient(cs kubernetes.Interface) deployAutopsyK8sProvider {
	return &k8sAutopsyClient{cs: cs}
}

// readLogLines reads a log stream line by line and returns the lines as a
// slice. Returns at most maxAutopsyLogLines lines (oldest-first).
func readLogLines(r io.Reader) ([]string, error) {
	var lines []string
	scanner := bufio.NewScanner(r)
	// Increase buffer to handle long log lines.
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		// Partial read is better than no read.
		slog.Warn("jobs.deploy_failure_autopsy.log_read_partial", "error", err)
	}
	return lines, nil
}

// maxAutopsyLogLines is the cap on last_lines per autopsy row.
const maxAutopsyLogLines = 200

// labelInstantAppID mirrors the constant in the api's k8s provider.
// "instant-app-id" is the label applied to all pods managed by the platform.
const labelInstantAppID = "instant-app-id"

// ── captureDeploymentAutopsy ──────────────────────────────────────────────────

// autopsyResult holds the collected information before it's written to the DB.
type autopsyResult struct {
	reason    string
	exitCode  sql.NullInt32
	event     string
	lastLines []string
	hint      string
}

// captureDeploymentAutopsy queries the k8s API for the root cause of a
// deployment failure and writes (or updates) a deployment_events row with
// kind='failure_autopsy'. Idempotent — safe to call on every reconcile tick
// because the ON CONFLICT DO UPDATE clause overwrites the previous row.
//
// autopsyK8s may be nil (the autopsy client was not initialised); in that
// case the function writes an Unknown row with an empty last_lines so the
// api can at least surface "failure" : { "reason": "Unknown" } rather than
// omitting the field entirely.
func captureDeploymentAutopsy(
	ctx context.Context,
	db *sql.DB,
	deploymentID uuid.UUID,
	providerID string,
	autopsyK8s deployAutopsyK8sProvider,
) {
	ns := deployNamespaceFromProviderID(providerID)
	if ns == "" {
		// Provider ID doesn't have the expected "app-<appID>" shape.
		// Write an Unknown autopsy so the api still surfaces the failure field.
		_ = upsertAutopsyRow(ctx, db, deploymentID, workerFailureReasonUnknown,
			sql.NullInt32{}, "provider_id did not match app-<appID> shape", nil)
		return
	}

	result := &autopsyResult{
		reason:    workerFailureReasonUnknown,
		lastLines: []string{},
	}

	if autopsyK8s != nil {
		result = collectAutopsyFromK8s(ctx, autopsyK8s, ns, providerID)
	}

	result.hint = workerHintForReason(result.reason)

	if err := upsertAutopsyRow(ctx, db, deploymentID, result.reason, result.exitCode, result.event, result.lastLines); err != nil {
		slog.Warn("jobs.deploy_failure_autopsy.upsert_failed",
			"deployment_id", deploymentID,
			"provider_id", providerID,
			"reason", result.reason,
			"error", err,
		)
	} else {
		slog.Info("jobs.deploy_failure_autopsy.captured",
			"deployment_id", deploymentID,
			"provider_id", providerID,
			"reason", result.reason,
		)
	}
}

// collectAutopsyFromK8s gathers pod lastState, namespace events, and log tail
// from the live k8s API. Fail-open on each step: if a step errors, the
// function continues with what it has.
func collectAutopsyFromK8s(
	ctx context.Context,
	k8s deployAutopsyK8sProvider,
	ns, providerID string,
) *autopsyResult {
	appID := strings.TrimPrefix(providerID, providerIDPrefix)

	result := &autopsyResult{
		reason:    workerFailureReasonUnknown,
		lastLines: []string{},
	}

	// Step 1: list pods in the namespace to find the failed container.
	podCtx, cancel := context.WithTimeout(ctx, k8sGetTimeout)
	defer cancel()

	podList, err := k8s.ListPods(podCtx, ns, labelInstantAppID+"="+appID)
	if err != nil && !apierrors.IsNotFound(err) {
		slog.Warn("jobs.deploy_failure_autopsy.list_pods_failed",
			"namespace", ns, "error", err)
	}

	var firstPodName string
	if podList != nil && len(podList.Items) > 0 {
		pod := &podList.Items[0]
		firstPodName = pod.Name
		// Extract reason + exit code from the pod's container statuses.
		extractPodFailure(pod, result)
	}

	// Step 2: namespace events — surface Evicted / OOMKilled messages that
	// are not visible in container status (e.g. the pod was evicted before
	// any container started).
	evCtx, evCancel := context.WithTimeout(ctx, k8sGetTimeout)
	defer evCancel()

	evList, err := k8s.ListEvents(evCtx, ns)
	if err != nil && !apierrors.IsNotFound(err) {
		slog.Warn("jobs.deploy_failure_autopsy.list_events_failed",
			"namespace", ns, "error", err)
	}
	if evList != nil {
		evMsg := extractRelevantEvent(evList)
		if evMsg != "" {
			result.event = evMsg
			// Override reason from event if we got an Unknown from pod status.
			if result.reason == workerFailureReasonUnknown {
				result.reason = reasonFromEventMessage(evMsg)
			}
		}
	}

	// Step 3: pod log tail. Skip when no pod was found (image pull failure
	// means the pod may never have started a container).
	if firstPodName != "" {
		logCtx, logCancel := context.WithTimeout(ctx, 10*time.Second)
		defer logCancel()

		lines, err := k8s.GetPodLogs(logCtx, ns, firstPodName, maxAutopsyLogLines)
		if err != nil {
			slog.Warn("jobs.deploy_failure_autopsy.get_logs_failed",
				"namespace", ns, "pod", firstPodName, "error", err)
		}
		if len(lines) > 0 {
			result.lastLines = lines
		}
	}

	return result
}

// extractPodFailure reads the container status of a pod and populates reason
// and exit code in the result. Checks waiting.reason first (ImagePullBackOff,
// CrashLoopBackOff), then terminated.reason (OOMKilled, Error).
func extractPodFailure(pod *corev1.Pod, result *autopsyResult) {
	for _, cs := range pod.Status.ContainerStatuses {
		if w := cs.State.Waiting; w != nil {
			switch w.Reason {
			case "ImagePullBackOff", "ErrImagePull":
				result.reason = workerFailureReasonImagePullBackOff
				result.event = fmt.Sprintf("ImagePullBackOff: %s", w.Message)
			case "CrashLoopBackOff":
				result.reason = workerFailureReasonCrashLoopBackOff
			}
		}
		// lastState gives us the terminated exit code even for CrashLoopBackOff.
		if t := cs.LastTerminationState.Terminated; t != nil {
			result.exitCode = sql.NullInt32{Int32: t.ExitCode, Valid: true}
			if t.Reason == "OOMKilled" {
				result.reason = workerFailureReasonOOMKilled
			} else if result.reason == workerFailureReasonUnknown && t.Reason != "" {
				result.reason = workerFailureReasonError
			}
			if result.event == "" && t.Message != "" {
				result.event = t.Message
			}
		}
		// Current terminated state (pod that didn't restart yet).
		if t := cs.State.Terminated; t != nil {
			if !result.exitCode.Valid {
				result.exitCode = sql.NullInt32{Int32: t.ExitCode, Valid: true}
			}
			if t.Reason == "OOMKilled" {
				result.reason = workerFailureReasonOOMKilled
			} else if result.reason == workerFailureReasonUnknown && t.Reason != "" {
				result.reason = workerFailureReasonError
			}
			if result.event == "" && t.Message != "" {
				result.event = t.Message
			}
		}
	}

	// Check pod-level status for eviction.
	if pod.Status.Phase == corev1.PodFailed && pod.Status.Reason == "Evicted" {
		result.reason = workerFailureReasonEvicted
		result.event = pod.Status.Message
	}
}

// extractRelevantEvent finds the most recent Warning event in the namespace
// that is likely related to a failure. Prefer OOMKilling / Eviction / image
// events, then fall back to the most recent Warning of any type.
func extractRelevantEvent(evList *corev1.EventList) string {
	var best string
	var bestTime time.Time

	for _, ev := range evList.Items {
		if ev.Type != corev1.EventTypeWarning {
			continue
		}
		t := ev.LastTimestamp.Time
		if t.IsZero() {
			t = ev.EventTime.Time
		}
		// Prefer OOM / eviction / image events.
		priority := isPriorityEvent(ev.Reason)
		if priority && (best == "" || t.After(bestTime)) {
			best = fmt.Sprintf("%s: %s", ev.Reason, ev.Message)
			bestTime = t
		} else if !priority && best == "" {
			best = fmt.Sprintf("%s: %s", ev.Reason, ev.Message)
			bestTime = t
		}
	}
	return best
}

// isPriorityEvent returns true for OOMKilling, Evicted, and image-pull events
// that should take precedence over generic Warning events in extractRelevantEvent.
func isPriorityEvent(reason string) bool {
	switch reason {
	case "OOMKilling", "Evicted", "Killing",
		"Failed", "FailedToPull", "BackOff", "ErrImagePull":
		return true
	}
	return false
}

// reasonFromEventMessage maps a k8s event message to a FailureReason constant
// using substring matching. Returns Unknown when no match is found.
func reasonFromEventMessage(msg string) string {
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "oomkill") || strings.Contains(lower, "out of memory"):
		return workerFailureReasonOOMKilled
	case strings.Contains(lower, "evict"):
		return workerFailureReasonEvicted
	case strings.Contains(lower, "imagepull") || strings.Contains(lower, "image pull"):
		return workerFailureReasonImagePullBackOff
	case strings.Contains(lower, "crashloop"):
		return workerFailureReasonCrashLoopBackOff
	default:
		return workerFailureReasonUnknown
	}
}

// ── SQL helpers ───────────────────────────────────────────────────────────────

// upsertAutopsyRow writes the autopsy to deployment_events. Idempotent via
// the partial unique index (deployment_id, kind) WHERE kind='failure_autopsy'.
// Mirror of models.UpsertDeploymentAutopsy in the api module — duplicated
// because the worker does not import the api.
func upsertAutopsyRow(
	ctx context.Context,
	db *sql.DB,
	deploymentID uuid.UUID,
	reason string,
	exitCode sql.NullInt32,
	event string,
	lastLines []string,
) error {
	if lastLines == nil {
		lastLines = []string{}
	}
	lastLinesJSON, err := json.Marshal(lastLines)
	if err != nil {
		return fmt.Errorf("upsertAutopsyRow: marshal last_lines: %w", err)
	}
	// Cap last_lines to maxAutopsyLogLines if the caller sent more.
	if len(lastLines) > maxAutopsyLogLines {
		lastLines = lastLines[len(lastLines)-maxAutopsyLogLines:]
		lastLinesJSON, _ = json.Marshal(lastLines)
	}

	var exitCodeArg interface{}
	if exitCode.Valid {
		exitCodeArg = exitCode.Int32
	}

	hint := workerHintForReason(reason)

	// Sanitise event: strip null bytes that Postgres TEXT rejects.
	event = strings.ReplaceAll(event, "\x00", "")

	// Limit event to 4096 chars so the column stays reasonable.
	const maxEventLen = 4096
	if len(event) > maxEventLen {
		// Use bytes.Count to avoid multi-byte boundary issues.
		event = string([]byte(event)[:maxEventLen])
	}

	_, err = db.ExecContext(ctx, `
		INSERT INTO deployment_events
			(deployment_id, kind, reason, exit_code, event, last_lines, hint)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (deployment_id, kind) WHERE kind = 'failure_autopsy'
		DO UPDATE SET
			reason     = EXCLUDED.reason,
			exit_code  = EXCLUDED.exit_code,
			event      = EXCLUDED.event,
			last_lines = EXCLUDED.last_lines,
			hint       = EXCLUDED.hint
	`,
		deploymentID,
		deploymentEventKindFailureAutopsy,
		reason,
		exitCodeArg,
		event,
		lastLinesJSON,
		hint,
	)
	if err != nil {
		return fmt.Errorf("upsertAutopsyRow: %w", err)
	}
	return nil
}

// ensure bytes import is used (for the event truncation above).
var _ = bytes.Count
