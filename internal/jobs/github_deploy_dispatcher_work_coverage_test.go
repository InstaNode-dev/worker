package jobs

// github_deploy_dispatcher_work_coverage_test.go — drives Work / claimBatch /
// dispatch / postRedeploy / markFailed / markCompleted to ≥95%.
//
// The original github_deploy_dispatcher_test.go only covered fetchTarball and
// the zero-config constructor; it explicitly punted on Work() because it
// believed a populated *river.Job was needed (it is, and we build one — every
// other coverage test in this package does the same).
//
// DB paths use sqlmock (default regexp matcher). The api redeploy POST uses an
// httptest server fronted by the real apiclient.Client so the breaker-gated
// path is exercised end-to-end.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"instant.dev/worker/internal/apiclient"
)

func githubDispatcherJob() *river.Job[GitHubDeployDispatcherArgs] {
	return &river.Job[GitHubDeployDispatcherArgs]{JobRow: &rivertype.JobRow{ID: 7}}
}

// ── Kind + permanentError.Error ───────────────────────────────────────

func TestGitHubDispatcher_Kind(t *testing.T) {
	if got := (GitHubDeployDispatcherArgs{}).Kind(); got != "github_deploy_dispatcher" {
		t.Errorf("Kind() = %q", got)
	}
}

func TestPermanentError_Error(t *testing.T) {
	e := &permanentError{Code: 404, Msg: "github archive 4xx"}
	if got := e.Error(); got != "github archive 4xx (HTTP 404)" {
		t.Errorf("permanentError.Error() = %q", got)
	}
}

// ── Work: zero-config short-circuit ───────────────────────────────────

func TestGitHubDispatcher_Work_ZeroConfig(t *testing.T) {
	d := NewGitHubDeployDispatcher(nil, "", "")
	if err := d.Work(context.Background(), githubDispatcherJob()); err != nil {
		t.Fatalf("Work zero-config: %v", err)
	}
}

// ── Work: claim error bubbles up ──────────────────────────────────────

func TestGitHubDispatcher_Work_ClaimError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectBegin().WillReturnError(errors.New("begin boom"))

	d := &GitHubDeployDispatcher{db: db, apiBaseURL: "http://api", internalJWT: "jwt"}
	if err := d.Work(context.Background(), githubDispatcherJob()); err == nil {
		t.Error("Work should surface claim error")
	}
}

// ── Work: no rows ─────────────────────────────────────────────────────

func TestGitHubDispatcher_Work_NoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery(`UPDATE pending_github_deploys`).
		WithArgs(statusGitHubQueued, githubMaxAttempts, statusGitHubInProgress).
		WillReturnRows(sqlmock.NewRows([]string{"id", "connection_id", "app_id", "commit_sha", "attempts"}))
	mock.ExpectCommit()

	d := &GitHubDeployDispatcher{db: db, apiBaseURL: "http://api", internalJWT: "jwt"}
	if err := d.Work(context.Background(), githubDispatcherJob()); err != nil {
		t.Fatalf("Work no-rows: %v", err)
	}
}

// ── Work: one orphan row → dispatch fails → markFailed ────────────────

func TestGitHubDispatcher_Work_OrphanRow_MarksFailed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rowID := uuid.New()
	connID := uuid.New()
	appUUID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(`UPDATE pending_github_deploys`).
		WithArgs(statusGitHubQueued, githubMaxAttempts, statusGitHubInProgress).
		WillReturnRows(sqlmock.NewRows([]string{"id", "connection_id", "app_id", "commit_sha", "attempts"}).
			AddRow(rowID, connID, appUUID, "deadbeef", 0))
	// Enrichment query errors → githubRepo stays "" → dispatch returns orphan error.
	mock.ExpectQuery(`SELECT c.github_repo, c.branch, d.app_id`).
		WithArgs(connID).
		WillReturnError(errors.New("orphaned connection"))
	mock.ExpectCommit()

	// markFailed: SELECT attempts, then terminal-fail UPDATE (orphan is a
	// non-permanent error, but attempts query returns 0 so it re-queues —
	// orphan_row is not a permanentError, so the transient branch fires).
	mock.ExpectQuery(`SELECT attempts FROM pending_github_deploys`).
		WithArgs(rowID).
		WillReturnRows(sqlmock.NewRows([]string{"attempts"}).AddRow(1))
	mock.ExpectExec(`UPDATE pending_github_deploys`).
		WithArgs(rowID, statusGitHubQueued, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	d := &GitHubDeployDispatcher{db: db, apiBaseURL: "http://api", internalJWT: "jwt"}
	if err := d.Work(context.Background(), githubDispatcherJob()); err != nil {
		t.Fatalf("Work orphan-row: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// ── Work: happy path through dispatch → postRedeploy → markCompleted ──

func TestGitHubDispatcher_Work_HappyPath(t *testing.T) {
	// GitHub tarball server.
	ghSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("tar-bytes"))
	}))
	defer ghSrv.Close()

	// api redeploy server (200 OK).
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer jwt" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(200)
	}))
	defer apiSrv.Close()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	rowID := uuid.New()
	connID := uuid.New()
	appUUID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery(`UPDATE pending_github_deploys`).
		WithArgs(statusGitHubQueued, githubMaxAttempts, statusGitHubInProgress).
		WillReturnRows(sqlmock.NewRows([]string{"id", "connection_id", "app_id", "commit_sha", "attempts"}).
			AddRow(rowID, connID, appUUID, "abc123", 0))
	mock.ExpectQuery(`SELECT c.github_repo, c.branch, d.app_id`).
		WithArgs(connID).
		WillReturnRows(sqlmock.NewRows([]string{"github_repo", "branch", "app_id"}).
			AddRow("owner/repo", "main", "app-slug"))
	mock.ExpectCommit()
	// markCompleted.
	mock.ExpectExec(`UPDATE pending_github_deploys`).
		WithArgs(rowID, statusGitHubCompleted).
		WillReturnResult(sqlmock.NewResult(0, 1))

	httpCli := &http.Client{}
	d := &GitHubDeployDispatcher{
		db:          db,
		apiBaseURL:  apiSrv.URL,
		internalJWT: "jwt",
		httpClient:  httpCli,
		apiCli:      apiclient.New(httpCli),
	}
	// Point fetchTarball at our github stub by overriding the archive host:
	// dispatch builds the URL from r.githubRepo via the github.com host, so
	// instead we test dispatch's two sub-steps through Work using a repo whose
	// tarball fetch we intercept via a custom RoundTripper.
	httpCli.Transport = &ghRedirectTransport{ghHost: ghSrv.URL}

	if err := d.Work(context.Background(), githubDispatcherJob()); err != nil {
		t.Fatalf("Work happy-path: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// ghRedirectTransport rewrites any api.github.com tarball request to the
// in-test github stub server, leaving every other request (the api redeploy
// POST) untouched.
type ghRedirectTransport struct {
	ghHost string
}

func (t *ghRedirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Host == "api.github.com" {
		stub, _ := http.NewRequestWithContext(req.Context(), req.Method, t.ghHost, req.Body)
		stub.Header = req.Header
		return http.DefaultTransport.RoundTrip(stub)
	}
	return http.DefaultTransport.RoundTrip(req)
}

// ── dispatch: orphan + fetch-error + post-error wraps ─────────────────

func TestGitHubDispatcher_Dispatch_Orphan(t *testing.T) {
	d := &GitHubDeployDispatcher{}
	err := d.dispatch(context.Background(), pendingGitHubDeploy{githubRepo: ""})
	if err == nil {
		t.Fatal("dispatch orphan: expected error")
	}
}

func TestGitHubDispatcher_Dispatch_FetchError(t *testing.T) {
	httpCli := &http.Client{Transport: &cannedRoundTripper{err: errors.New("net down")}}
	d := &GitHubDeployDispatcher{httpClient: httpCli, apiCli: apiclient.New(httpCli)}
	err := d.dispatch(context.Background(), pendingGitHubDeploy{githubRepo: "o/r", commitSHA: "sha"})
	if err == nil || !strings.Contains(err.Error(), "fetch tarball") {
		t.Fatalf("dispatch fetch-error = %v, want fetch tarball wrap", err)
	}
}

func TestGitHubDispatcher_Dispatch_PostError(t *testing.T) {
	// Tarball fetch returns 200 (any host), api redeploy returns 500.
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer apiSrv.Close()
	httpCli := &http.Client{Transport: &dispatchSplitTransport{apiHost: apiSrv.URL}}
	d := &GitHubDeployDispatcher{
		apiBaseURL:  apiSrv.URL,
		internalJWT: "jwt",
		httpClient:  httpCli,
		apiCli:      apiclient.New(httpCli),
	}
	err := d.dispatch(context.Background(), pendingGitHubDeploy{githubRepo: "o/r", commitSHA: "sha", appIDSlug: "slug"})
	if err == nil || !strings.Contains(err.Error(), "post redeploy") {
		t.Fatalf("dispatch post-error = %v, want post redeploy wrap", err)
	}
}

// dispatchSplitTransport answers github tarball requests with 200 and routes
// the api redeploy POST to apiHost.
type dispatchSplitTransport struct{ apiHost string }

func (t *dispatchSplitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Host == "api.github.com" {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("tar")), Header: make(http.Header), Request: req}, nil
	}
	return http.DefaultTransport.RoundTrip(req)
}

// ── fetchTarball: request-build error + 5xx transient ─────────────────

func TestGitHubDispatcher_FetchTarball_5xxTransient(t *testing.T) {
	httpCli := &http.Client{Transport: &cannedRoundTripper{status: 503}}
	d := &GitHubDeployDispatcher{httpClient: httpCli}
	_, err := d.fetchTarball(context.Background(), "https://api.github.com/repos/o/r/tarball/x")
	if err == nil {
		t.Fatal("fetchTarball 5xx should error")
	}
	var perm *permanentError
	if errors.As(err, &perm) {
		t.Error("5xx must be transient, not permanent")
	}
}

func TestGitHubDispatcher_FetchTarball_BadURL(t *testing.T) {
	d := &GitHubDeployDispatcher{httpClient: &http.Client{}}
	// Control character in URL → http.NewRequestWithContext fails.
	_, err := d.fetchTarball(context.Background(), "http://\x7f/bad")
	if err == nil {
		t.Error("fetchTarball bad-url should error on request build")
	}
}

func TestGitHubDispatcher_FetchTarball_DoError(t *testing.T) {
	httpCli := &http.Client{Transport: &cannedRoundTripper{err: errors.New("dial")}}
	d := &GitHubDeployDispatcher{httpClient: httpCli}
	if _, err := d.fetchTarball(context.Background(), "https://api.github.com/x"); err == nil {
		t.Error("fetchTarball Do-error should surface")
	}
}

// ── claimBatch: scan error + commit error ─────────────────────────────

func TestGitHubDispatcher_ClaimBatch_ScanError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery(`UPDATE pending_github_deploys`).
		WithArgs(statusGitHubQueued, githubMaxAttempts, statusGitHubInProgress).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("not-a-uuid")) // wrong shape → scan error
	mock.ExpectRollback()
	d := &GitHubDeployDispatcher{db: db}
	if _, err := d.claimBatch(context.Background()); err == nil {
		t.Error("claimBatch scan-error should surface")
	}
}

func TestGitHubDispatcher_ClaimBatch_CommitError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	rowID := uuid.New()
	connID := uuid.New()
	appUUID := uuid.New()
	mock.ExpectBegin()
	mock.ExpectQuery(`UPDATE pending_github_deploys`).
		WithArgs(statusGitHubQueued, githubMaxAttempts, statusGitHubInProgress).
		WillReturnRows(sqlmock.NewRows([]string{"id", "connection_id", "app_id", "commit_sha", "attempts"}).
			AddRow(rowID, connID, appUUID, "sha", 0))
	mock.ExpectQuery(`SELECT c.github_repo, c.branch, d.app_id`).
		WithArgs(connID).
		WillReturnRows(sqlmock.NewRows([]string{"github_repo", "branch", "app_id"}).AddRow("o/r", "main", "slug"))
	mock.ExpectCommit().WillReturnError(errors.New("commit boom"))
	d := &GitHubDeployDispatcher{db: db}
	if _, err := d.claimBatch(context.Background()); err == nil {
		t.Error("claimBatch commit-error should surface")
	}
}

// ── postRedeploy: 4xx is permanent, 5xx is transient ──────────────────

func TestGitHubDispatcher_PostRedeploy_StatusBranches(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		wantErr  bool
		wantPerm bool
	}{
		{"ok", 200, false, false},
		{"4xx", 403, true, true},
		{"5xx", 503, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()
			httpCli := &http.Client{}
			d := &GitHubDeployDispatcher{
				apiBaseURL:  srv.URL,
				internalJWT: "jwt",
				httpClient:  httpCli,
				apiCli:      apiclient.New(httpCli),
			}
			err := d.postRedeploy(context.Background(), "slug", []byte("tar"))
			if (err != nil) != tc.wantErr {
				t.Fatalf("postRedeploy %s: err=%v wantErr=%v", tc.name, err, tc.wantErr)
			}
			if tc.wantPerm {
				var perm *permanentError
				if !errors.As(err, &perm) {
					t.Errorf("postRedeploy %s: expected permanentError, got %v", tc.name, err)
				}
			}
		})
	}
}

// postRedeploy with a nil apiCli builds one inline (the struct-literal fallback
// WARN branch).
func TestGitHubDispatcher_PostRedeploy_NilApiCliFallback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()
	d := &GitHubDeployDispatcher{
		apiBaseURL:  srv.URL,
		internalJWT: "jwt",
		httpClient:  &http.Client{},
		apiCli:      nil, // forces the inline-construct WARN branch
	}
	if err := d.postRedeploy(context.Background(), "slug", []byte("tar")); err != nil {
		t.Fatalf("postRedeploy nil-apiCli: %v", err)
	}
	if d.apiCli == nil {
		t.Error("postRedeploy should have populated apiCli")
	}
}

// ── markFailed: terminal (permanent) vs transient ─────────────────────

func TestGitHubDispatcher_MarkFailed_Terminal(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	id := uuid.New()
	// Permanent error → terminal-fail branch (no SELECT-attempts dependency on
	// max, but the code always runs the SELECT first).
	mock.ExpectQuery(`SELECT attempts FROM pending_github_deploys`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"attempts"}).AddRow(1))
	mock.ExpectExec(`UPDATE pending_github_deploys`).
		WithArgs(id, statusGitHubFailed, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	d := &GitHubDeployDispatcher{db: db}
	d.markFailed(context.Background(), id, &permanentError{Code: 404, Msg: "gone"})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestGitHubDispatcher_MarkFailed_MaxAttempts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	id := uuid.New()
	// Transient error but attempts >= max → terminal-fail branch.
	mock.ExpectQuery(`SELECT attempts FROM pending_github_deploys`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"attempts"}).AddRow(githubMaxAttempts))
	mock.ExpectExec(`UPDATE pending_github_deploys`).
		WithArgs(id, statusGitHubFailed, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	d := &GitHubDeployDispatcher{db: db}
	d.markFailed(context.Background(), id, errors.New("transient 503"))
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestGitHubDispatcher_MarkCompleted(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	id := uuid.New()
	mock.ExpectExec(`UPDATE pending_github_deploys`).
		WithArgs(id, statusGitHubCompleted).
		WillReturnResult(sqlmock.NewResult(0, 1))
	d := &GitHubDeployDispatcher{db: db}
	d.markCompleted(context.Background(), id)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}
