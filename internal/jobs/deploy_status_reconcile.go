package jobs

// deploy_status_reconcile.go — periodic reconciler for the deployments table.
//
// PROBLEM SHAPE
//
// The API's /deploy/new handler persists a deployments row with status="building"
// and kicks off runDeploy() in a goroutine. runDeploy() calls compute.Deploy()
// which, on a k8s backend, returns immediately after kicking off the kaniko
// build + creating the Deployment object. The handler then writes whatever
// status compute.Deploy() returned (typically still "building" if AvailableReplicas
// has not yet reached 1).
//
// After that single write, the deployments.status column is never touched again
// by the system. The k8s Deployment transitions to AvailableReplicas>=1 within
// ~30-90s on local clusters, but the DB row stays "building" forever — there
// is no background process that watches live k8s state and rolls it forward.
//
// Customers calling GET /deploy/:id to discover when their app is ready get
// the stale value. This worker fixes that by sweeping all non-terminal
// deployments every 30s and reconciling the column from the live k8s
// Deployment object's status.AvailableReplicas + conditions.
//
// LIFECYCLE (mirrors api/internal/providers/compute/k8s/client.go:deploymentStatus)
//
//   building ─→ deploying ─→ healthy
//                              │
//                              └→ failed (DeploymentReplicaFailure=True)
//
//   building ─→ stopped (k8s Deployment NotFound — namespace was deleted)
//
//   failed and stopped are TERMINAL — we never reconcile out of them. The
//   listActiveDeployments SQL filter already excludes them.
//
// SCOPE / FAIL-OPEN POSTURE
//
// The worker is constructed with a deployStatusK8sProvider interface. The
// concrete implementation lives in this file (k8sDeployStatusClient) and is
// built from rest.InClusterConfig() with a kubeconfig fallback for local
// dev. If client init fails (e.g. no kubeconfig in CI, no cluster reachable),
// the constructor returns a reconciler with k8s=nil and Work() short-circuits
// at the top with a warn log. This keeps the worker process alive — other
// periodic jobs (expire_anonymous, expire_stacks, custom_domain_reconcile)
// keep running. Same fail-open posture as ExpireAnonymousWorker on Redis errors.
//
// SQL SCHEMA
//
// The deployments table is owned by the api module. Columns referenced here:
//   - id           (uuid, PK)
//   - provider_id  (text, nullable until runDeploy completes — we skip those)
//   - status       (text)
//   - error_message (text, nullable)
//   - updated_at   (timestamptz)
// Status string set (kept in sync with api/internal/models/deployment.go):
//   building | deploying | healthy | failed | stopped
//
// NAMESPACE LAYOUT
//
// The api's k8s.K8sProvider derives namespace = "instant-deploy-<appID>" from
// the provider_id, where provider_id = "app-<appID>". This worker duplicates
// that derivation (deployNamespaceFromProviderID) rather than depending on
// the api package — same pattern custom_domain_reconcile.go uses for status
// strings.
//
// RBAC
//
// The worker ServiceAccount (infra/k8s/worker-rbac.yaml) gains a new ClusterRole
// "instant-worker-deploy-reader" with get on deployments.apps. We can't restrict
// it to namespace-by-namespace because instant-deploy-<appID> namespaces are
// created on demand by the api process; the deploy-reader ClusterRole is
// read-only and scoped to the deployments resource so the blast radius is tight.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"instant.dev/worker/internal/metrics"
)

// Reconciler tunables. The interval matches the periodic-job registration in
// workers.go.
const (
	deployStatusReconcileInterval = 30 * time.Second

	// k8sGetTimeout caps a single Get-Deployment call so one stuck namespace
	// doesn't stall the whole batch sweep. The control plane is normally
	// sub-second; 5s is generous.
	k8sGetTimeout = 5 * time.Second

	// maxAutopsiesPerTick caps how many failure-autopsy captures the
	// reconciler runs in a single sweep (BugBash 2026-05-18 W3 T3:
	// "autopsy synchronous in-sweep → tick overrun").
	//
	// captureDeploymentAutopsy runs synchronously and each one can do a
	// pod-log-tail call with its own multi-second timeout. A cluster with
	// a large batch of simultaneously-failed deployments would otherwise
	// chain N × (log-tail timeout) of synchronous work into one tick and
	// blow well past the 30s reconcile interval, starving the status
	// transitions that share the same loop.
	//
	// The capture is idempotent (deployment_events ON CONFLICT DO UPDATE),
	// so a failed deployment whose autopsy is deferred past the cap this
	// tick is simply captured on the next tick — at worst a 30s delay on
	// the "failure" object appearing in the api response. The status
	// transition itself is NOT capped: every row still gets its status
	// reconciled every tick; only the heavier autopsy side-effect is
	// rate-limited.
	maxAutopsiesPerTick = 8

	// autopsyBudgetPerTick bounds the total wall-clock the autopsy
	// captures may consume in one sweep. Once exceeded, remaining failed
	// rows defer their autopsy to the next tick even if the per-tick
	// count cap has not been hit — a belt-and-braces guard for the case
	// where individual log-tail calls run long. Sized to leave headroom
	// inside the 30s interval for the status-transition UPDATEs.
	autopsyBudgetPerTick = 20 * time.Second

	// Status strings — verbatim copies of the canonical set in
	// api/internal/models/deployment.go and the k8s compute provider's
	// deploymentStatus() helper. Duplicated here because the worker module
	// does not import the api module. If the api strings ever change,
	// update both places.
	deployStatusBuilding  = "building"
	deployStatusDeploying = "deploying"
	deployStatusHealthy   = "healthy"
	deployStatusFailed    = "failed"
	deployStatusStopped   = "stopped"

	// providerIDPrefix mirrors api/internal/providers/compute/k8s/client.go's
	// deploymentName(appID) = "app-" + appID.
	providerIDPrefix = "app-"

	// deployNamespacePrefix mirrors deployNamespace(appID) in the api's k8s
	// provider. The worker derives the namespace from provider_id rather than
	// storing it on the deployments row.
	deployNamespacePrefix = "instant-deploy-"

	// buildJobNamePrefix mirrors the api's k8s.buildImage() jobName format:
	//   jobName := "build-" + sanitizeName(appID)
	// (api/internal/providers/compute/k8s/client.go ~L1390 / L1217).
	//
	// The build runs as a `batchv1.Job` named `build-<appID>` in the same
	// per-deployment namespace (`instant-deploy-<appID>`) as the runtime
	// `appsv1.Deployment`. A Job that hits its BackoffLimit (kaniko Dockerfile
	// error) or its ActiveDeadlineSeconds (10 min wall-clock cap) marks itself
	// `Failed` in its status BUT the runtime Deployment object is NEVER
	// created — buildImage returns an error to runDeploy before the
	// apply/rollout step runs. The pre-fix reconciler only queried the
	// runtime Deployment, so a build-failed row reconciled to either
	// `stopped` (Deployment NotFound — wrong, looks like a teardown) or stayed
	// `building` forever (e.g. the api goroutine crashed mid-runDeploy and
	// the row's terminal status write never landed). This was the silent-
	// deploy-failure bug class (2026-05-30 user incident `truehomie-api-...`).
	//
	// The fix: after the Deployment query, ALWAYS query the build Job too. A
	// Failed Job is authoritative — flip the row to `failed` regardless of
	// what the runtime Deployment said. The Job's `TTLSecondsAfterFinished`
	// (5 min in the api) keeps the Job object readable for a window even
	// after k8s GCs the build pod, giving the reconciler a structured signal
	// the pre-fix code missed.
	buildJobNamePrefix = "build-"
)

// DeployStatusReconcileArgs is the periodic-job payload. Empty — every run is
// a full table sweep.
type DeployStatusReconcileArgs struct{}

// Kind implements river.JobArgs.
func (DeployStatusReconcileArgs) Kind() string { return "deploy_status_reconcile" }

// deployStatusK8sProvider is the slice of k8s API the reconciler uses. Defined
// as an interface so callers can pass nil when k8s isn't reachable (the
// reconciler then warn-logs and returns nil — same pattern as
// k8sCustomDomainProvider in custom_domain_reconcile.go).
type deployStatusK8sProvider interface {
	// GetDeployment returns the live k8s Deployment object, or apierrors.IsNotFound
	// when the namespace or Deployment has been deleted. The caller maps NotFound
	// to status="stopped".
	GetDeployment(ctx context.Context, namespace, name string) (*appsv1.Deployment, error)

	// GetBuildJob returns the live kaniko build Job, or apierrors.IsNotFound
	// when the Job has been GC'd (past its TTLSecondsAfterFinished — 5 min in
	// the api) or the namespace has been deleted. The caller inspects
	// Status.Failed + JobConditions to detect a terminal build failure that
	// the runtime-Deployment query alone cannot see.
	GetBuildJob(ctx context.Context, namespace, name string) (*batchv1.Job, error)
}

// k8sDeployStatusClient is the concrete deployStatusK8sProvider implementation.
// Wraps a kubernetes.Clientset so the reconciler doesn't import the full client
// surface at every callsite.
type k8sDeployStatusClient struct {
	cs kubernetes.Interface
}

// GetDeployment implements deployStatusK8sProvider.
func (c *k8sDeployStatusClient) GetDeployment(ctx context.Context, namespace, name string) (*appsv1.Deployment, error) {
	return c.cs.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
}

// GetBuildJob implements deployStatusK8sProvider.
func (c *k8sDeployStatusClient) GetBuildJob(ctx context.Context, namespace, name string) (*batchv1.Job, error) {
	return c.cs.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
}

// NewK8sDeployStatusClient builds a deployStatusK8sProvider from in-cluster
// config, falling back to the default kubeconfig for local dev. Returns nil
// (and a non-nil error) when neither is reachable — the caller logs and
// passes nil to NewDeployStatusReconciler.
//
// This mirrors api/internal/providers/compute/k8s/client.go:newClientset but
// duplicated here so the worker module does not depend on the api module.
func NewK8sDeployStatusClient() (deployStatusK8sProvider, error) {
	cs, err := newDeployK8sClientset()
	if err != nil {
		return nil, err
	}
	return &k8sDeployStatusClient{cs: cs}, nil
}

// NewK8sDeployStatusClientWithAutopsy is the same as NewK8sDeployStatusClient
// but also returns a deployAutopsyK8sProvider backed by the same underlying
// kubernetes.Clientset. Call this in StartWorkers so both the status reconciler
// and the autopsy capturer share a single TCP connection pool to the k8s API.
func NewK8sDeployStatusClientWithAutopsy() (deployStatusK8sProvider, deployAutopsyK8sProvider, error) {
	cs, err := newDeployK8sClientset()
	if err != nil {
		return nil, nil, err
	}
	status, autopsy := buildDeployStatusAndAutopsy(cs)
	return status, autopsy, nil
}

// buildDeployStatusAndAutopsy wires the status + autopsy providers around a
// single kubernetes.Interface. Split out from NewK8sDeployStatusClientWithAutopsy
// so the wiring is exercisable with a fake.Clientset without a live cluster.
func buildDeployStatusAndAutopsy(cs kubernetes.Interface) (deployStatusK8sProvider, deployAutopsyK8sProvider) {
	return &k8sDeployStatusClient{cs: cs}, NewK8sAutopsyClient(cs)
}

// newDeployK8sClientset builds a kubernetes.Clientset from in-cluster config,
// falling back to the default kubeconfig for local dev.
func newDeployK8sClientset() (*kubernetes.Clientset, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		cfg, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
		if err != nil {
			return nil, fmt.Errorf("k8s config (in-cluster + kubeconfig both failed): %w", err)
		}
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("k8s NewForConfig: %w", err)
	}
	return cs, nil
}

// DeployStatusReconciler is the River worker that sweeps non-terminal
// deployments and rolls status forward from live k8s state.
type DeployStatusReconciler struct {
	river.WorkerDefaults[DeployStatusReconcileArgs]
	db         *sql.DB
	k8s        deployStatusK8sProvider  // may be nil — the worker then warn-logs each run
	autopsyK8s deployAutopsyK8sProvider // may be nil — autopsy rows will use Unknown reason
}

// NewDeployStatusReconciler constructs the worker.
//
// Pass nil for k8sProvider in environments where the worker can't talk to
// the cluster API. Work() will short-circuit each run with a warn log so
// the rest of the periodic job lineup keeps functioning.
//
// Pass nil for autopsyK8s when the extended autopsy interface is not available;
// the reconciler will still write Unknown-reason autopsy rows on failure transitions
// so the api always surfaces a "failure" object on failed deployments.
func NewDeployStatusReconciler(db *sql.DB, k8sProvider deployStatusK8sProvider) *DeployStatusReconciler {
	return &DeployStatusReconciler{
		db:  db,
		k8s: k8sProvider,
	}
}

// WithAutopsyK8s wires the extended k8s client used for failure autopsy capture.
// Called by StartWorkers after constructing the reconciler when the cluster is
// reachable. Not called in CI / docker-compose where k8s is absent.
func (r *DeployStatusReconciler) WithAutopsyK8s(autopsyK8s deployAutopsyK8sProvider) *DeployStatusReconciler {
	r.autopsyK8s = autopsyK8s
	return r
}

// activeDeployment is the projection the reconciler reads — only the columns
// needed to drive transitions.
type activeDeployment struct {
	id         uuid.UUID
	providerID string
	status     string
}

// Work runs the full sweep. Errors on individual rows are logged and swallowed
// so one bad row never stops the rest — same fail-open posture as
// CustomDomainReconciler.
func (w *DeployStatusReconciler) Work(ctx context.Context, job *river.Job[DeployStatusReconcileArgs]) error {
	start := time.Now()

	if w.k8s == nil {
		// k8s wasn't reachable at startup — log once per tick at WARN so
		// operators notice, but don't fail the job (River would retry
		// forever). The rest of the worker process keeps running.
		slog.Warn("jobs.deploy_status_reconcile.skipped_no_k8s_client",
			"reason", "k8s client init failed at startup; deployments will stay stale until worker is restarted with reachable cluster",
			"job_id", job.ID,
		)
		return nil
	}

	deployments, err := w.listActiveDeployments(ctx)
	if err != nil {
		return fmt.Errorf("deploy_status_reconcile: list active: %w", err)
	}

	if len(deployments) == 0 {
		// T21 P1-1 (BugBash 2026-05-20): demote idle-tick INFO to DEBUG
		// (~5,760 lines/day from this 30s job alone). Same pattern as the
		// email-noise fix `7169493`. Operators see the non-zero ticks; the
		// idle steady state stays out of NR Logs.
		slog.Debug("jobs.deploy_status_reconcile.completed",
			"total", 0,
			"duration_ms", time.Since(start).Milliseconds(),
			"job_id", job.ID,
		)
		return nil
	}

	var (
		transitions int
		errors      int
		skipped     int
		// autopsiesThisTick counts failure-autopsy captures performed in
		// this sweep; deferred counts failed rows whose autopsy was
		// skipped because a per-tick cap was reached (BugBash 2026-05-18
		// W3 T3). A deferred autopsy is re-attempted on the next tick —
		// the capture is idempotent so nothing is lost, only delayed.
		autopsiesThisTick int
		autopsiesDeferred int
	)
	autopsyDeadline := start.Add(autopsyBudgetPerTick)

	for _, d := range deployments {
		if d.providerID == "" {
			// runDeploy() hasn't reached UpdateDeploymentProviderID yet
			// (kaniko build still in flight on the api side). Nothing to
			// poll — leave the row alone.
			skipped++
			continue
		}

		newStatus, err := w.computeNewStatus(ctx, d.providerID)
		if err == errSkipForeignProviderID {
			// Row was provisioned by a different backend (e.g. the stack
			// pipeline uses provider_id="instant-stack-*"). Not a fault.
			slog.Debug("jobs.deploy_status_reconcile.skip_foreign_provider_id",
				"id", d.id, "provider_id", d.providerID)
			skipped++
			continue
		}
		if err != nil {
			slog.Warn("jobs.deploy_status_reconcile.k8s_get_failed",
				"id", d.id, "provider_id", d.providerID, "error", err)
			errors++
			continue
		}

		// Phase 0 — Failure Autopsy: capture a deployment_events row whenever
		// the CURRENT k8s state is "failed" — decoupled from the status
		// transition. The api's GET /deploy/:id handler reads this row and
		// surfaces it as the optional "failure" object in the response.
		//
		// Capture is intentionally NOT gated behind `newStatus != d.status`.
		// A healthy→failed-on-redeploy deployment whose DB row the api already
		// flipped to "failed" (so the worker sees newStatus == d.status ==
		// "failed") would otherwise be skipped by the transition `continue`
		// below and never get a structured "failure" object. The upsert is
		// idempotent — the deployment_events partial-unique index +
		// ON CONFLICT DO UPDATE make re-capture on every tick a safe no-op
		// (re-overwrites the row with the latest pod state). We still bound
		// the cost: the capture only runs for rows listActiveDeployments
		// already returned (building|deploying|healthy), so the reconciler
		// does not start polling the whole failed-row history.
		//
		// This runs synchronously in the sweep loop because the log-tail call
		// has its own timeout. To keep a batch of simultaneously-failed
		// deployments from chaining N synchronous log-tail calls into one
		// tick and overrunning the 30s reconcile interval (BugBash
		// 2026-05-18 W3 T3), the capture is bounded two ways per tick:
		// a hard count cap (maxAutopsiesPerTick) and a wall-clock budget
		// (autopsyBudgetPerTick). Once either is hit, remaining failed rows
		// defer their autopsy to the next tick. The capture is idempotent
		// (deployment_events ON CONFLICT DO UPDATE), so a deferred autopsy
		// is simply re-attempted next sweep — at worst a one-interval delay
		// on the "failure" object surfacing in the api response. The status
		// transition below is NOT capped — every row still reconciles its
		// status every tick.
		if newStatus == deployStatusFailed {
			if autopsiesThisTick >= maxAutopsiesPerTick || time.Now().After(autopsyDeadline) {
				autopsiesDeferred++
			} else {
				captureDeploymentAutopsy(ctx, w.db, d.id, d.providerID, w.autopsyK8s)
				autopsiesThisTick++
			}
		}

		if newStatus == d.status {
			continue
		}

		if err := w.updateStatus(ctx, d.id, newStatus); err != nil {
			slog.Error("jobs.deploy_status_reconcile.update_failed",
				"id", d.id, "provider_id", d.providerID,
				"from", d.status, "to", newStatus, "error", err)
			errors++
			continue
		}

		slog.Info("jobs.deploy_status_reconcile.transition",
			"id", d.id, "provider_id", d.providerID,
			"from", d.status, "to", newStatus)
		transitions++
	}

	if autopsiesDeferred > 0 {
		slog.Warn("jobs.deploy_status_reconcile.autopsies_deferred",
			"deferred", autopsiesDeferred,
			"captured", autopsiesThisTick,
			"note", "per-tick autopsy cap/budget reached; deferred rows re-captured next tick (idempotent)",
		)
	}

	slog.Info("jobs.deploy_status_reconcile.completed",
		"total", len(deployments),
		"transitions", transitions,
		"errors", errors,
		"skipped", skipped,
		"autopsies", autopsiesThisTick,
		"autopsies_deferred", autopsiesDeferred,
		"duration_ms", time.Since(start).Milliseconds(),
		"job_id", job.ID,
	)
	return nil
}

// errSkipForeignProviderID is returned by computeNewStatus when a deployments
// row's provider_id does not match the "app-<appID>" shape (e.g. it was
// provisioned by the stack pipeline whose provider_id is "instant-stack-<id>").
// Sentinel error — Work() treats this as a skip, not an error.
var errSkipForeignProviderID = errors.New("provider_id not in app-<appID> shape; reconciler is single-app only")

// computeNewStatus performs a single Get against the per-deployment namespace
// and maps the result into the canonical status string set. NotFound (the
// namespace or Deployment has been deleted out from under us) maps to
// "stopped" — same as the api's k8s.Status() helper.
//
// JOB-FAILED OVERRIDE (silent-deploy-failure fix, 2026-05-30 incident):
//
// After the Deployment query, ALWAYS consult the kaniko build Job too.
// A Job in `Failed` phase (BackoffLimit exhausted, DeadlineExceeded, or any
// `Failed`-type condition) is authoritative — flip the row to `failed`
// regardless of what the runtime Deployment object reports.
//
// The pre-fix code only queried `appsv1.Deployments`; when the build Job
// crashed it did one of two equally-wrong things:
//
//  1. The runtime Deployment was never created (typical: buildImage errored
//     before applyDeployment ran) → GetDeployment returned NotFound → mapped
//     to `stopped`, a TERMINAL status that looks to the user like the deploy
//     was torn down on purpose, with no autopsy and no failure surface.
//
//  2. The api goroutine crashed mid-runDeploy (pod OOM, ctx kill, etc.) and
//     the row's terminal `failed` write never landed → the Deployment query
//     might return any in-flight state and the row sat at `building` forever.
//
// In both cases the build Job's `Status.Failed > 0` OR a `JobCondition` of
// type `Failed` is the unambiguous evidence that a terminal build failure
// occurred. The Job's `TTLSecondsAfterFinished` (5 min in the api) means we
// can read the Job for a window even after the build pod is GC'd — exactly
// the gap the pre-fix code missed.
//
// The Job's NotFound result is NOT treated as a build success: it just means
// the Job has been reaped or never existed (Deployment-status path remains
// authoritative). Only a `Failed` Job overrides — `Succeeded` and `Active`
// states fall through to the Deployment-based mapping.
func (w *DeployStatusReconciler) computeNewStatus(ctx context.Context, providerID string) (string, error) {
	ns := deployNamespaceFromProviderID(providerID)
	if ns == "" {
		// provider_id doesn't have the "app-" prefix — almost certainly a row
		// from a future or alternate compute backend. Skip cleanly.
		return "", errSkipForeignProviderID
	}

	getCtx, cancel := context.WithTimeout(ctx, k8sGetTimeout)
	defer cancel()

	deploy, deployErr := w.k8s.GetDeployment(getCtx, ns, providerID)
	if deployErr != nil && !apierrors.IsNotFound(deployErr) {
		// Transport-level k8s error on the Deployment query. Returning the
		// error skips the row this tick — the Job-failed override is not a
		// substitute for the row's healthy/deploying transitions, so a
		// brownout should bubble up.
		return "", deployErr
	}

	// Job-failed override: query the build Job before deciding the row's
	// new status. A terminal Job failure is authoritative over whatever
	// the runtime Deployment reports.
	jobCtx, jobCancel := context.WithTimeout(ctx, k8sGetTimeout)
	defer jobCancel()
	appID := strings.TrimPrefix(providerID, providerIDPrefix)
	jobName := buildJobNamePrefix + appID
	job, jobErr := w.k8s.GetBuildJob(jobCtx, ns, jobName)
	if jobErr != nil && !apierrors.IsNotFound(jobErr) {
		// Don't fail the whole row on a Job-query brownout — log and fall
		// through to the Deployment-based mapping. The next tick retries.
		slog.Warn("jobs.deploy_status_reconcile.job_query_failed",
			"namespace", ns, "job", jobName, "error", jobErr,
			"note", "falling through to Deployment-based status — silent build-failure detection skipped this tick")
	} else if jobErr == nil && jobIsFailed(job) {
		// Job-failed override — authoritative.
		metrics.DeployJobFailedDetectedTotal.WithLabelValues(jobFailureReason(job)).Inc()
		return deployStatusFailed, nil
	}

	if apierrors.IsNotFound(deployErr) {
		// Deployment query: NotFound + Job-not-failed (Job missing, Active, or
		// Succeeded). Two distinct cases:
		//   - Job NotFound + Deployment NotFound → namespace torn down → stopped.
		//   - Job Active + Deployment NotFound → build still running (pre-apply)
		//     → keep as "building" — Job-failed override caught the failure case.
		if apierrors.IsNotFound(jobErr) {
			return deployStatusStopped, nil
		}
		// Job exists and is not Failed — the build is still in flight or just
		// succeeded but the Deployment apply hasn't landed yet. Hold at the
		// row's current "building" status (deploymentStatusFromK8s returns
		// building for an all-zero status, which is what a missing Deployment
		// effectively represents at this stage).
		return deployStatusBuilding, nil
	}

	return deploymentStatusFromK8s(deploy), nil
}

// jobIsFailed reports whether a kaniko build Job has reached a terminal
// failure: either `Status.Failed > 0` (k8s incremented the failed-pod count
// past the BackoffLimit) OR a JobCondition of type `Failed` is present with
// status=True. Either condition is authoritative — the Job will not recover.
func jobIsFailed(job *batchv1.Job) bool {
	if job == nil {
		return false
	}
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	// Only treat Failed>0 as terminal if BackoffLimit has been reached, since
	// a transient pod failure during retries also bumps the counter. The
	// JobFailed condition above is the primary signal; this is the backstop
	// for cluster versions that surface Failed before stamping the condition.
	backoffLimit := int32(0)
	if job.Spec.BackoffLimit != nil {
		backoffLimit = *job.Spec.BackoffLimit
	}
	return job.Status.Failed > backoffLimit
}

// jobFailureReason picks a short bounded label for the
// instant_deploy_job_failed_detected_total counter from the Job's `Failed`
// condition. Falls back to "backoff_limit_exceeded" when the condition is
// absent but Status.Failed > BackoffLimit (the cluster-version backstop in
// jobIsFailed).
//
// Cardinality: k8s uses a small, stable set of Reason strings for JobFailed
// ("BackoffLimitExceeded", "DeadlineExceeded", "PodFailurePolicy"). We pass
// them through verbatim plus the fallback bucket. Bounded — safe to label.
func jobFailureReason(job *batchv1.Job) string {
	if job == nil {
		return "unknown"
	}
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			if cond.Reason != "" {
				return cond.Reason
			}
			return "failed_no_reason"
		}
	}
	return "backoff_limit_exceeded"
}

// deploymentStatusFromK8s mirrors api/internal/providers/compute/k8s/client.go's
// deploymentStatus() helper. Kept verbatim here to guarantee the worker's
// state machine matches what runDeploy() would write if it polled longer.
//
// Order matters: replica failure is checked first so a Deployment that has
// transient AvailableReplicas>=1 but a sticky failure condition (e.g. quota
// exceeded on a rolling update) does not flap into "healthy".
func deploymentStatusFromK8s(deploy *appsv1.Deployment) string {
	for _, cond := range deploy.Status.Conditions {
		if cond.Type == appsv1.DeploymentReplicaFailure && cond.Status == corev1.ConditionTrue {
			return deployStatusFailed
		}
	}
	if deploy.Status.AvailableReplicas >= 1 {
		return deployStatusHealthy
	}
	if deploy.Status.UpdatedReplicas > 0 || deploy.Status.UnavailableReplicas > 0 {
		return deployStatusDeploying
	}
	return deployStatusBuilding
}

// deployNamespaceFromProviderID derives the per-deployment namespace from the
// provider_id stored on a deployments row. provider_id = "app-<appID>";
// namespace = "instant-deploy-<appID>". Returns "" for rows whose provider_id
// doesn't match the expected shape (e.g. future Fly.io backend).
func deployNamespaceFromProviderID(providerID string) string {
	if !strings.HasPrefix(providerID, providerIDPrefix) {
		return ""
	}
	appID := strings.TrimPrefix(providerID, providerIDPrefix)
	if appID == "" {
		return ""
	}
	return deployNamespacePrefix + appID
}

// ── SQL helpers ───────────────────────────────────────────────────────────────
//
// These mirror the api's models.Deployment helpers but use only the columns
// the reconciler needs. Duplicated here intentionally — the worker module does
// not import the api module. The schema lives in the deployments migration in
// api/internal/db/migrations; if columns or status strings change, keep these
// in sync.

// listActiveDeployments returns every deployments row not in a terminal status.
// Terminal = failed | stopped. The api's runDeploy/Redeploy paths also write
// "deploying" and "building" transitively — both are picked up here.
func (w *DeployStatusReconciler) listActiveDeployments(ctx context.Context) ([]activeDeployment, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT id, COALESCE(provider_id, ''), status
		FROM deployments
		WHERE status IN ($1, $2, $3)
		ORDER BY updated_at ASC
	`, deployStatusBuilding, deployStatusDeploying, deployStatusHealthy)
	if err != nil {
		return nil, fmt.Errorf("listActiveDeployments: query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []activeDeployment
	for rows.Next() {
		var d activeDeployment
		if err := rows.Scan(&d.id, &d.providerID, &d.status); err != nil {
			return nil, fmt.Errorf("listActiveDeployments: scan: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// updateStatus advances the status column and bumps updated_at. We deliberately
// do NOT clear error_message — the api owns that field and we don't want to
// race with a concurrent runDeploy error write.
//
// We also do not transition out of "failed" or "stopped" — the SELECT above
// excludes those, so the UPDATE doesn't need a defensive WHERE clause for
// them. We do, however, add a WHERE status IN (...) guard so a row that
// concurrently transitioned to a terminal state between SELECT and UPDATE
// is left alone.
func (w *DeployStatusReconciler) updateStatus(ctx context.Context, id uuid.UUID, status string) error {
	_, err := w.db.ExecContext(ctx, `
		UPDATE deployments
		SET status = $1, updated_at = now()
		WHERE id = $2
		  AND status IN ($3, $4, $5)
	`, status, id, deployStatusBuilding, deployStatusDeploying, deployStatusHealthy)
	if err != nil {
		return fmt.Errorf("updateStatus: %w", err)
	}
	return nil
}
