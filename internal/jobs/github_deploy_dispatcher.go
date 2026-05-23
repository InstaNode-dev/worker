package jobs

// github_deploy_dispatcher.go — drains pending_github_deploys.
//
// Companion to api/internal/handlers/github_deploy.go (migration 035).
// The api inserts one pending_github_deploys row per accepted GitHub push.
// This worker:
//
//   1. Claims a queued row via SELECT ... FOR UPDATE SKIP LOCKED.
//   2. Marks status='in_progress' so a sibling worker doesn't double-fetch.
//   3. Downloads the source tarball from the GitHub archive URL for the
//      tracked repo + commit SHA:
//        https://api.github.com/repos/<owner>/<repo>/tarball/<sha>
//      (302 → codeload.github.com — http.Client follows redirects by default).
//   4. POSTs the tarball back to the api's /deploy/:id/redeploy endpoint
//      with the worker's internal JWT so the api treats it as a system
//      caller.
//   5. Marks status='completed' on success, 'failed' on a 4xx, retries up
//      to 3 attempts on transient 5xx / network errors.
//
// FAIL-OPEN POSTURE
//
// Mirrors deploy_status_reconcile.go: every error path is logged + swallowed
// so one bad row never stops the rest of the sweep. The Work() function
// returns nil unless the entire SELECT itself fails — River's retry policy
// would otherwise hammer this worker for hours on a single broken commit.
//
// REDEPLOY VS NEW
//
// The dispatcher only handles auto-deploy of EXISTING deployments. The api
// /deploy/:id/redeploy endpoint already drives the same compute.Redeploy
// path the dashboard's redeploy button uses, so we don't fork the build
// pipeline.

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"instant.dev/worker/internal/apiclient"
	"instant.dev/worker/internal/circuit"
)

const (
	// githubDispatcherInterval is the cadence of the periodic claim sweep.
	// 30s mirrors deploy_status_reconcile — the customer should not wait
	// minutes between push and first build log line.
	githubDispatcherInterval = 30 * time.Second

	// githubMaxAttempts caps retries for transient errors (5xx, network).
	// 4xx from the github archive endpoint is permanent (ref deleted /
	// permissions revoked / repo renamed); we don't retry those.
	githubMaxAttempts = 3

	// githubTarballTimeout caps the archive download. Large monorepos can
	// exceed default http.Client timeouts; 60s is generous for normal repos
	// but bounded enough that a stuck worker eventually frees its row.
	githubTarballTimeout = 60 * time.Second

	// githubMaxTarballBytes mirrors the api's POST /deploy/new 50 MB cap.
	// Larger archives surface as a "too_large" failure on the worker side
	// rather than blowing up the api's multipart reader.
	githubMaxTarballBytes = 50 << 20 // 50 MB

	// statusQueued / statusInProgress / statusCompleted / statusFailed
	// mirror the pending_github_deploys.status enum. Duplicated here
	// because this worker module does not import the api module.
	statusGitHubQueued     = "queued"
	statusGitHubInProgress = "in_progress"
	statusGitHubCompleted  = "completed"
	statusGitHubFailed     = "failed"
)

// GitHubDeployDispatcherArgs is the periodic-job payload. Empty — every
// run is a full claim sweep.
type GitHubDeployDispatcherArgs struct{}

// Kind implements river.JobArgs.
func (GitHubDeployDispatcherArgs) Kind() string { return "github_deploy_dispatcher" }

// GitHubDeployDispatcher drains pending_github_deploys rows and triggers a
// redeploy on the linked api deployment.
//
// apiCli is the circuit-breaker-wrapped HTTP client used for postRedeploy
// calls. Both httpClient (for the GitHub tarball fetch) and apiCli (for
// the api redeploy POST) share the same Timeout; only the api side is
// breaker-gated because that's the dependency that can go down and trap
// the worker in a retry storm.
type GitHubDeployDispatcher struct {
	river.WorkerDefaults[GitHubDeployDispatcherArgs]
	db          *sql.DB
	httpClient  *http.Client
	apiCli      *apiclient.Client
	apiBaseURL  string // api/internal/handlers — POST /deploy/:appID/redeploy lives here
	internalJWT string // worker's signed bearer for the api's internal endpoints
}

// NewGitHubDeployDispatcher constructs the worker. apiBaseURL is the api's
// in-cluster URL (e.g. http://instant-api.instant.svc.cluster.local:8080).
// internalJWT is the worker's per-restart-stable bearer token. Both are
// optional — when either is empty the worker logs a WARN and short-circuits
// each tick so a CI / docker-compose environment that doesn't set them
// keeps running.
func NewGitHubDeployDispatcher(db *sql.DB, apiBaseURL, internalJWT string) *GitHubDeployDispatcher {
	httpClient := &http.Client{Timeout: githubTarballTimeout}
	return &GitHubDeployDispatcher{
		db:          db,
		apiBaseURL:  apiBaseURL,
		internalJWT: internalJWT,
		httpClient:  httpClient,
		apiCli:      apiclient.New(httpClient),
	}
}

// pendingGitHubDeploy is the projection the worker reads per row.
type pendingGitHubDeploy struct {
	id           uuid.UUID
	connectionID uuid.UUID
	appUUID      uuid.UUID
	commitSHA    string
	attempts     int
	githubRepo   string
	branch       string
	appIDSlug    string // deployments.app_id (short slug used in the api redeploy URL)
}

// Work runs one drain pass. Errors on individual rows are logged + swallowed
// so a single bad commit can never block the queue.
func (w *GitHubDeployDispatcher) Work(ctx context.Context, job *river.Job[GitHubDeployDispatcherArgs]) error {
	if w.apiBaseURL == "" || w.internalJWT == "" {
		slog.Warn("jobs.github_deploy_dispatcher.skipped_no_api_config",
			"api_base_url_set", w.apiBaseURL != "",
			"internal_jwt_set", w.internalJWT != "",
			"job_id", job.ID)
		return nil
	}

	start := time.Now()
	rows, err := w.claimBatch(ctx)
	if err != nil {
		return fmt.Errorf("github_deploy_dispatcher: claim: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}

	var ok, failed int
	for _, r := range rows {
		if err := w.dispatch(ctx, r); err != nil {
			failed++
			w.markFailed(ctx, r.id, err)
			slog.Warn("jobs.github_deploy_dispatcher.row_failed",
				"id", r.id, "app_id", r.appIDSlug,
				"commit", r.commitSHA, "attempts", r.attempts+1,
				"error", err)
			continue
		}
		ok++
		w.markCompleted(ctx, r.id)
		slog.Info("jobs.github_deploy_dispatcher.row_completed",
			"id", r.id, "app_id", r.appIDSlug, "commit", r.commitSHA)
	}

	slog.Info("jobs.github_deploy_dispatcher.completed",
		"total", len(rows), "ok", ok, "failed", failed,
		"duration_ms", time.Since(start).Milliseconds(),
		"job_id", job.ID)
	return nil
}

// claimBatch grabs up to 10 queued rows, marks them in_progress, and joins
// the parent connection + deployment to learn the github repo + app slug.
// SKIP LOCKED is the standard race-free claim pattern.
func (w *GitHubDeployDispatcher) claimBatch(ctx context.Context) ([]pendingGitHubDeploy, error) {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	rs, err := tx.QueryContext(ctx, `
		WITH claimed AS (
			SELECT p.id
			  FROM pending_github_deploys p
			 WHERE p.status = $1 AND p.attempts < $2
			 ORDER BY p.enqueued_at
			 FOR UPDATE SKIP LOCKED
			 LIMIT 10
		)
		UPDATE pending_github_deploys p
		   SET status = $3,
		       attempts = attempts + 1
		  FROM claimed
		 WHERE p.id = claimed.id
		 RETURNING p.id, p.connection_id, p.app_id, p.commit_sha, p.attempts
	`, statusGitHubQueued, githubMaxAttempts, statusGitHubInProgress)
	if err != nil {
		return nil, err
	}

	var out []pendingGitHubDeploy
	for rs.Next() {
		var r pendingGitHubDeploy
		if err := rs.Scan(&r.id, &r.connectionID, &r.appUUID, &r.commitSHA, &r.attempts); err != nil {
			_ = rs.Close()
			return nil, err
		}
		out = append(out, r)
	}
	_ = rs.Close()

	// Enrich with parent metadata. Done one-by-one inside the same tx so
	// the claim and the read are atomic.
	for i := range out {
		row := tx.QueryRowContext(ctx, `
			SELECT c.github_repo, c.branch, d.app_id
			  FROM app_github_connections c
			  JOIN deployments d ON d.id = c.app_id
			 WHERE c.id = $1`,
			out[i].connectionID)
		if err := row.Scan(&out[i].githubRepo, &out[i].branch, &out[i].appIDSlug); err != nil {
			// Row is orphaned (connection got DELETE'd between push and
			// claim). Mark failed so the worker doesn't keep retrying.
			out[i].githubRepo = ""
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// dispatch handles one row: download tarball → POST to api redeploy.
func (w *GitHubDeployDispatcher) dispatch(ctx context.Context, r pendingGitHubDeploy) error {
	if r.githubRepo == "" {
		return errors.New("orphan_row: parent connection or deployment missing")
	}

	archiveURL := fmt.Sprintf("https://api.github.com/repos/%s/tarball/%s",
		r.githubRepo, r.commitSHA)
	tarball, err := w.fetchTarball(ctx, archiveURL)
	if err != nil {
		return fmt.Errorf("fetch tarball: %w", err)
	}
	if err := w.postRedeploy(ctx, r.appIDSlug, tarball); err != nil {
		return fmt.Errorf("post redeploy: %w", err)
	}
	return nil
}

// fetchTarball downloads the github archive. http.Client follows redirects
// to codeload.github.com automatically. The 50 MB cap is enforced via a
// LimitedReader so a malicious repo can't OOM the worker.
func (w *GitHubDeployDispatcher) fetchTarball(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "instanode-worker/github-dispatcher")
	resp, err := w.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		// Permanent — repo / ref doesn't exist (or is private + no auth).
		// Don't retry.
		return nil, &permanentError{Code: resp.StatusCode, Msg: "github archive 4xx"}
	}
	if resp.StatusCode >= 500 {
		return nil, fmt.Errorf("github archive %d", resp.StatusCode)
	}
	limited := io.LimitReader(resp.Body, githubMaxTarballBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(body) > githubMaxTarballBytes {
		return nil, &permanentError{Code: http.StatusRequestEntityTooLarge, Msg: "tarball exceeds 50 MB"}
	}
	return body, nil
}

// postRedeploy POSTs the tarball to /deploy/:appID/redeploy with the
// worker's internal JWT. Wrapped by the worker→api circuit breaker via
// apiCli — when the api is hosed the breaker short-circuits and the
// caller's "leave row queued; next tick re-tries" branch fires.
func (w *GitHubDeployDispatcher) postRedeploy(ctx context.Context, appIDSlug string, tarball []byte) error {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	part, err := mw.CreateFormFile("tarball", "src.tar.gz")
	if err != nil {
		return err
	}
	if _, err := part.Write(tarball); err != nil {
		return err
	}
	_ = mw.Close()

	url := w.apiBaseURL + "/deploy/" + appIDSlug + "/redeploy"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+w.internalJWT)
	req.Header.Set("User-Agent", "instanode-worker/github-dispatcher")

	cli := w.apiCli
	if cli == nil {
		// Test paths that build the dispatcher as a struct literal still
		// get the raw http.Client — but log a structured WARN so a
		// production miswire doesn't silently bypass the breaker.
		slog.Warn("github_deploy_dispatcher.no_apiclient",
			"note", "constructing apiclient inline — production should use NewGitHubDeployDispatcher",
		)
		cli = apiclient.New(w.httpClient)
		w.apiCli = cli
	}
	resp, err := cli.Do(req)
	if err != nil {
		if errors.Is(err, circuit.ErrOpen) {
			slog.Warn("github_deploy_dispatcher.api_circuit_open",
				"app_id", appIDSlug,
				"note", "leaving row queued; next tick will retry",
			)
		}
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body) // drain so keep-alive is happy
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return &permanentError{Code: resp.StatusCode, Msg: "api redeploy 4xx"}
	}
	if resp.StatusCode >= 500 {
		return fmt.Errorf("api redeploy %d", resp.StatusCode)
	}
	return nil
}

// markFailed sets status=failed when retries are exhausted OR error is
// permanent (4xx).
func (w *GitHubDeployDispatcher) markFailed(ctx context.Context, id uuid.UUID, cause error) {
	var perm *permanentError
	terminal := errors.As(cause, &perm)
	// Look up current attempts; if we've hit max OR error is permanent,
	// terminal-fail. Otherwise re-queue for the next tick.
	var attempts int
	_ = w.db.QueryRowContext(ctx, `SELECT attempts FROM pending_github_deploys WHERE id = $1`, id).Scan(&attempts)
	if terminal || attempts >= githubMaxAttempts {
		_, _ = w.db.ExecContext(ctx, `
			UPDATE pending_github_deploys
			   SET status = $2, error_message = $3, completed_at = now()
			 WHERE id = $1`,
			id, statusGitHubFailed, truncate(cause.Error(), 512))
		return
	}
	// Transient — flip back to queued so the next tick re-tries.
	_, _ = w.db.ExecContext(ctx, `
		UPDATE pending_github_deploys
		   SET status = $2, error_message = $3
		 WHERE id = $1`,
		id, statusGitHubQueued, truncate(cause.Error(), 512))
}

// markCompleted stamps completed_at and clears any prior error_message.
func (w *GitHubDeployDispatcher) markCompleted(ctx context.Context, id uuid.UUID) {
	_, _ = w.db.ExecContext(ctx, `
		UPDATE pending_github_deploys
		   SET status = $2, error_message = NULL, completed_at = now()
		 WHERE id = $1`,
		id, statusGitHubCompleted)
}

// permanentError marks a 4xx / hard-fail from github or the api as
// non-retryable. errors.As matches against this to terminal-fail the row.
type permanentError struct {
	Code int
	Msg  string
}

func (e *permanentError) Error() string {
	return fmt.Sprintf("%s (HTTP %d)", e.Msg, e.Code)
}

// truncate caps an error message so error_message doesn't bloat the table.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
