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
	"errors"
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

	"instant.dev/worker/internal/metrics"
)

// ── Autopsy metric outcome labels ─────────────────────────────────────────────
//
// Used as the `outcome` label on instant_deploy_autopsy_captured_total. Kept
// in a small bounded set so dashboard panels render predictable series.

const (
	// autopsyOutcomeLogsCaptured: at least one log line was captured from
	// either the app pod or the build pod — full autopsy succeeded.
	autopsyOutcomeLogsCaptured = "logs_captured"

	// autopsyOutcomeLogsUnavailable: the autopsy ran but the pod was already
	// GC'd (or never existed — image-pull failure) and no log lines were
	// captured. Reason and event fields are still populated from k8s state
	// + Job event fallback; lastLines is empty.
	autopsyOutcomeLogsUnavailable = "logs_unavailable"

	// autopsyOutcomeAlreadyPresent: the deployment_events row already had a
	// real (non-Unknown) reason from a prior autopsy and this tick added
	// nothing new — pure idempotent re-capture. Distinguishes "the autopsy
	// is doing useful work" from "the autopsy is just looping over old
	// state every 30s".
	autopsyOutcomeAlreadyPresent = "already_present"

	// autopsyOutcomeAuditEmitFailed: the autopsy row upsert succeeded but
	// the audit_log emit failed (Postgres brownout). Surfaces in the
	// dashboard so a missing failure email has a corresponding metric.
	autopsyOutcomeAuditEmitFailed = "audit_emit_failed"
)

// labelBuildJobName mirrors api/internal/providers/compute/k8s/client.go's
// build-Job pod label `job-name=build-<appID>`. Used by the autopsy log
// fallback path to fetch logs from the kaniko build pod when the runtime
// app pod was never created (BuildFailed / DeadlineExceeded modal case).
const labelBuildJobName = "job-name"

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
	defer func() { _ = stream.Close() }()
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
//
// SILENT-DEPLOY-FAILURE FIX (2026-05-30, PR 2):
//
// In addition to writing the deployment_events row, the autopsy now ALSO:
//
//  1. UPDATEs deployments.error_message with "<reason>: <hint snippet>" so
//     the api's GET /deploy/:id surfaces a one-line human-readable cause
//     even when the caller doesn't pull the structured deployment_events.
//
//  2. Emits an audit_log row with kind='deploy.failed' so the
//     event_email_forwarder dispatches the failure email (the api's runDeploy
//     normally emits this, but the user incident showed that when the api
//     goroutine crashes mid-build the audit row is never written — the
//     worker fills the gap so the user still gets the email).
//
//  3. Increments instant_deploy_autopsy_captured_total{outcome} so the NR
//     dashboard can chart logs_captured vs logs_unavailable vs already_present.
//
// The audit-emit is idempotent at the email layer (event_email_forwarder
// dedupes by audit_log.id), so re-running the autopsy on every tick
// re-emits the row but the user receives exactly one email.
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
		metrics.DeployAutopsyCapturedTotal.WithLabelValues(autopsyOutcomeLogsUnavailable).Inc()
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

	// PR 2 — fail-soft: was the row already populated with a real reason
	// from a prior autopsy? Used to label the metric "already_present" so
	// operators can distinguish first-capture from idempotent re-capture.
	preexisting := autopsyAlreadyPresentWithReason(ctx, db, deploymentID)

	if err := upsertAutopsyRow(ctx, db, deploymentID, result.reason, result.exitCode, result.event, result.lastLines); err != nil {
		slog.Warn("jobs.deploy_failure_autopsy.upsert_failed",
			"deployment_id", deploymentID,
			"provider_id", providerID,
			"reason", result.reason,
			"error", err,
		)
		// Emit the metric even on failure so the operator sees a non-zero
		// "logs_unavailable" rate when the DB is brown — pair with NR
		// alert on Postgres pool saturation.
		metrics.DeployAutopsyCapturedTotal.WithLabelValues(autopsyOutcomeLogsUnavailable).Inc()
		return
	}

	// PR 2 enhancement (rule 25 metric outcome label):
	outcome := autopsyOutcomeLogsCaptured
	if len(result.lastLines) == 0 {
		outcome = autopsyOutcomeLogsUnavailable
	}
	if preexisting && result.reason == workerFailureReasonUnknown {
		// Idempotent re-capture — the existing row had a real reason and
		// this tick added nothing new. Keep the dashboard signal honest.
		outcome = autopsyOutcomeAlreadyPresent
	}
	metrics.DeployAutopsyCapturedTotal.WithLabelValues(outcome).Inc()

	// PR 2: update deployments.error_message with "<reason>: <hint>" so the
	// api's row-only readers (CLI, dashboard list view) see a one-liner
	// cause without having to pull the deployment_events row.
	if err := updateDeploymentErrorMessage(ctx, db, deploymentID, result.reason, result.hint); err != nil {
		slog.Warn("jobs.deploy_failure_autopsy.error_message_update_failed",
			"deployment_id", deploymentID,
			"reason", result.reason,
			"error", err,
		)
	}

	// PR 2: emit audit_log kind='deploy.failed' so event_email_forwarder
	// dispatches the user-visible failure email. The api's runDeploy
	// normally emits this; the worker is the backstop for the
	// goroutine-crashed-mid-build case (which IS the 2026-05-30 incident).
	if err := emitDeployFailedAudit(ctx, db, deploymentID, result.reason, result.event); err != nil {
		// audit-emit failure is fail-soft — increment the audit_emit_failed
		// outcome counter so the operator sees missing failure emails on the
		// dashboard, and keep the rest of the sweep alive.
		metrics.DeployAutopsyCapturedTotal.WithLabelValues(autopsyOutcomeAuditEmitFailed).Inc()
		slog.Warn("jobs.deploy_failure_autopsy.audit_emit_failed",
			"deployment_id", deploymentID,
			"reason", result.reason,
			"error", err,
		)
	}

	slog.Info("jobs.deploy_failure_autopsy.captured",
		"deployment_id", deploymentID,
		"provider_id", providerID,
		"reason", result.reason,
		"outcome", outcome,
		"lines_captured", len(result.lastLines),
	)
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

	// PR 2: fall back to the BUILD pod's logs when the runtime app pod yielded
	// no log lines. The build pod runs as part of the kaniko Job
	// (label "job-name=build-<appID>") and is the typical source of failure
	// when the autopsy is triggered by a Job-failed state (PR 1) — the
	// runtime Deployment was never created so an app-pod log fetch returns
	// nothing useful. This call is best-effort; on success the kaniko
	// stderr tail surfaces the actual Dockerfile error in the api response.
	if len(result.lastLines) == 0 {
		buildPodName := findBuildPodName(ctx, k8s, ns, appID)
		if buildPodName != "" {
			buildLogCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			lines, err := k8s.GetPodLogs(buildLogCtx, ns, buildPodName, maxAutopsyLogLines)
			if err != nil {
				slog.Warn("jobs.deploy_failure_autopsy.get_build_logs_failed",
					"namespace", ns, "pod", buildPodName, "error", err)
			}
			if len(lines) > 0 {
				result.lastLines = lines
				if result.reason == workerFailureReasonUnknown {
					// We have logs from a build pod — classify as BuildFailed
					// unless something more specific was already set from
					// pod-status or event extraction.
					result.reason = workerFailureReasonBuildFailed
				}
			}
		}
	}

	return result
}

// findBuildPodName lists pods in the deploy namespace matching the kaniko
// Job's pod label (`job-name=build-<appID>`) and returns the first pod name,
// or "" when no build pod is reachable (already GC'd past
// TTLSecondsAfterFinished, or never created). Fail-soft: errors are logged
// and treated as "no pod found".
func findBuildPodName(ctx context.Context, k8s deployAutopsyK8sProvider, ns, appID string) string {
	listCtx, cancel := context.WithTimeout(ctx, k8sGetTimeout)
	defer cancel()
	selector := labelBuildJobName + "=" + buildJobNamePrefix + appID
	podList, err := k8s.ListPods(listCtx, ns, selector)
	if err != nil && !apierrors.IsNotFound(err) {
		slog.Warn("jobs.deploy_failure_autopsy.list_build_pods_failed",
			"namespace", ns, "selector", selector, "error", err)
		return ""
	}
	if podList == nil || len(podList.Items) == 0 {
		return ""
	}
	return podList.Items[0].Name
}

// buildJobNamePrefix mirrors api/internal/providers/compute/k8s/client.go's
// build Job naming convention: jobName = "build-" + sanitizeName(appID). Kept
// duplicated (no shared import — same pattern the rest of this file uses).
const buildJobNamePrefix = "build-"

// updateDeploymentErrorMessage stamps the deployments.error_message column
// with a "<reason>: <hint snippet>" one-liner so row-only readers see a
// human-readable cause without having to pull deployment_events.
//
// The snippet is the first sentence of the hint (up to the first period or
// 200 chars), keeping the column under the historical 2KB cap the api uses.
// We deliberately DO NOT clear a pre-existing error_message that doesn't
// match this format — the api may have already stamped a more specific
// build error (e.g. the kaniko stderr) and we should not clobber it. The
// UPDATE only runs when error_message IS NULL or empty.
func updateDeploymentErrorMessage(ctx context.Context, db *sql.DB, id uuid.UUID, reason, hint string) error {
	if reason == "" {
		reason = workerFailureReasonUnknown
	}
	snippet := firstSentence(hint, 200)
	combined := reason
	if snippet != "" {
		combined = reason + ": " + snippet
	}
	_, err := db.ExecContext(ctx, `
		UPDATE deployments
		SET error_message = $1
		WHERE id = $2
		  AND (error_message IS NULL OR error_message = '')
	`, combined, id)
	if err != nil {
		return fmt.Errorf("updateDeploymentErrorMessage: %w", err)
	}
	return nil
}

// firstSentence returns the leading portion of s up to the first period
// (inclusive) or up to maxLen chars, whichever is shorter. Used to derive
// a one-line snippet from the multi-sentence hint strings.
func firstSentence(s string, maxLen int) string {
	if s == "" {
		return ""
	}
	if i := strings.Index(s, "."); i >= 0 && i+1 <= maxLen {
		return s[:i+1]
	}
	if len(s) > maxLen {
		return s[:maxLen]
	}
	return s
}

// emitDeployFailedAudit inserts an audit_log row with kind='deploy.failed'
// so event_email_forwarder dispatches the user-visible failure email. The
// api's runDeploy normally emits this synchronously; the worker is the
// backstop for the case where the api goroutine crashed mid-build and
// never wrote the row (the 2026-05-30 silent-deploy-failure incident).
//
// team_id is required by the schema (NOT NULL). We resolve it by joining
// deployments → teams; on a missing deploy row (already deleted), the
// helper logs and returns nil — no audit emit possible without a team_id.
//
// Metadata mirrors the api's emitDeployAudit shape:
//
//	{
//	  "deploy_id":      "<uuid>",
//	  "team_id":        "<uuid>",
//	  "failure_stage":  "build",
//	  "error_summary":  "<reason>: <event truncated to 256 chars>",
//	  "source":         "worker_autopsy"
//	}
//
// The `source` field distinguishes the worker-emitted backstop from the
// api's synchronous emit so an operator triaging duplicate failure emails
// can see which path fired.
func emitDeployFailedAudit(ctx context.Context, db *sql.DB, deploymentID uuid.UUID, reason, event string) error {
	var teamID uuid.UUID
	err := db.QueryRowContext(ctx, `
		SELECT team_id FROM deployments WHERE id = $1
	`, deploymentID).Scan(&teamID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Row was already deleted between the autopsy capture and now.
			// Nothing to email — silently skip.
			return nil
		}
		return fmt.Errorf("emitDeployFailedAudit: lookup team_id: %w", err)
	}
	if teamID == uuid.Nil {
		// Deployment without a team_id should be impossible (schema NOT NULL)
		// but defend anyway — audit_log INSERT would itself fail.
		return nil
	}
	const maxErrorSummary = 256
	summary := reason
	if event != "" {
		summary = reason + ": " + event
	}
	if len(summary) > maxErrorSummary {
		summary = summary[:maxErrorSummary]
	}
	meta := map[string]any{
		"deploy_id":     deploymentID.String(),
		"team_id":       teamID.String(),
		"failure_stage": "build",
		"error_summary": summary,
		"source":        "worker_autopsy",
	}
	// json.Marshal of a map[string]any with string keys + string values is
	// total — unreachable error path. The orphan-sweep audit emit follows
	// the same _-ignore pattern (orphan_sweep_reconciler.go:emitOrphanAudit).
	metaBytes, _ := json.Marshal(meta)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES ($1, $2, $3, $4, $5)
	`, teamID, "worker.deploy_failure_autopsy", auditKindDeployFailed, summary, metaBytes); err != nil {
		return fmt.Errorf("emitDeployFailedAudit: insert: %w", err)
	}
	return nil
}

// (auditKindDeployFailed is declared in deploy_notify_webhook.go — re-used
// here to keep a single source of truth for the kind string. Changes to
// the kind value should land in that file.)

// autopsyAlreadyPresentWithReason returns true when the deployment_events
// row for this deployment already has a real (non-Unknown) reason captured.
// Used to label the metric outcome as "already_present" when an autopsy
// re-runs on the same row without adding new information.
//
// Fail-open: any error (including ErrNoRows) returns false so the metric
// labels the run as a real capture. The label-correctness is a
// dashboard-quality concern, not a safety concern.
func autopsyAlreadyPresentWithReason(ctx context.Context, db *sql.DB, deploymentID uuid.UUID) bool {
	var reason string
	err := db.QueryRowContext(ctx, `
		SELECT reason FROM deployment_events
		WHERE deployment_id = $1 AND kind = $2
	`, deploymentID, deploymentEventKindFailureAutopsy).Scan(&reason)
	if err != nil {
		return false
	}
	return reason != "" && reason != workerFailureReasonUnknown
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
