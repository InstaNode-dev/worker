package jobs

// github_deploy_dispatcher_test.go — hermetic tests for the GitHub
// auto-deploy dispatcher. Lives in `package jobs` (not `jobs_test`) so it
// can reach the unexported fetchTarball + postRedeploy helpers without
// reopening their visibility.
//
// What we cover:
//
//   1. Work() with empty config is a no-op (fail-open posture mirrors
//      PaymentGraceTerminator). Critical because CI / docker-compose boots
//      the worker without INSTANT_API_INTERNAL_URL set.
//
//   2. fetchTarball happy-path: 200 OK with a body smaller than the 50 MB
//      cap returns the body bytes.
//
//   3. fetchTarball 4xx is a permanentError (don't retry).
//
//   4. fetchTarball 5xx is a transient error (do retry).
//
//   5. fetchTarball oversized body returns a permanentError so a malicious
//      repo cannot OOM the worker.

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestDispatcher_ZeroConfig verifies the constructor doesn't blow up
// when called without an api URL / JWT. We can't drive Work() directly
// without a populated *river.Job (its slog.* call sites read job.ID),
// so the no-config short-circuit is exercised end-to-end in CI / staging
// instead of in this unit test.
func TestDispatcher_ZeroConfig(t *testing.T) {
	d := NewGitHubDeployDispatcher(nil, "", "")
	if d == nil {
		t.Fatal("constructor returned nil")
	}
	if d.apiBaseURL != "" {
		t.Errorf("apiBaseURL: want empty, got %q", d.apiBaseURL)
	}
	if d.internalJWT != "" {
		t.Errorf("internalJWT: want empty, got %q", d.internalJWT)
	}
}

// TestFetchTarball_HappyPath: a 200 OK response with reasonable size body
// returns the body bytes verbatim.
func TestFetchTarball_HappyPath(t *testing.T) {
	want := []byte("fake-tar-bytes")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write(want)
	}))
	defer srv.Close()

	d := &GitHubDeployDispatcher{httpClient: srv.Client()}
	got, err := d.fetchTarball(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("fetchTarball: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("body mismatch: got %q want %q", got, want)
	}
}

// TestFetchTarball_4xxIsPermanent: a 404 from the github archive (ref
// deleted, repo gone) MUST surface as a *permanentError so markFailed
// doesn't keep retrying.
func TestFetchTarball_4xxIsPermanent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	}))
	defer srv.Close()

	d := &GitHubDeployDispatcher{httpClient: srv.Client()}
	_, err := d.fetchTarball(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
	var perm *permanentError
	if !errors.As(err, &perm) {
		t.Fatalf("expected *permanentError, got %T: %v", err, err)
	}
	if perm.Code != 404 {
		t.Errorf("expected code 404, got %d", perm.Code)
	}
}

// TestFetchTarball_5xxIsTransient: a 502 from github MUST NOT be a
// permanentError — the dispatcher will requeue the row for the next tick.
func TestFetchTarball_5xxIsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(502)
	}))
	defer srv.Close()

	d := &GitHubDeployDispatcher{httpClient: srv.Client()}
	_, err := d.fetchTarball(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("expected error on 502, got nil")
	}
	var perm *permanentError
	if errors.As(err, &perm) {
		t.Fatalf("5xx must NOT be permanentError; was %v", err)
	}
}

// TestFetchTarball_OversizedRejected: a 200 OK with body > 50 MB must
// surface as a permanentError so the dispatcher doesn't OOM and doesn't
// keep retrying forever.
func TestFetchTarball_OversizedRejected(t *testing.T) {
	// Build a body slightly over the cap.
	big := strings.Repeat("x", githubMaxTarballBytes+10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(big))
	}))
	defer srv.Close()

	d := &GitHubDeployDispatcher{httpClient: srv.Client()}
	_, err := d.fetchTarball(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("expected oversized rejection, got nil error")
	}
	var perm *permanentError
	if !errors.As(err, &perm) {
		t.Fatalf("oversized body must surface as permanentError, got %T: %v", err, err)
	}
}

// TestTruncate caps long messages so error_message column doesn't bloat.
func TestTruncate(t *testing.T) {
	short := "abc"
	if got := truncate(short, 10); got != short {
		t.Errorf("short truncate changed: %q", got)
	}
	long := strings.Repeat("a", 1000)
	out := truncate(long, 16)
	// 16 chars + "…" (3 bytes UTF-8) = 19. Anything bigger means cap leaked.
	if len(out) != 19 {
		t.Errorf("truncate did not cap to 16+ellipsis: len=%d, out=%q", len(out), out)
	}
}
