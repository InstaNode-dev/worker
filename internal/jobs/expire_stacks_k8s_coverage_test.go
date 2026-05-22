package jobs

// expire_stacks_k8s_coverage_test.go — in-package coverage for the
// in-cluster Kubernetes teardown path of expire_stacks.go that the
// black-box suite cannot reach: inClusterK8sClient (CA read + cert-pool
// build) and deleteK8sNamespace (SA-token read + DELETE round-trip,
// including 404/Accepted/error status handling and the in-cluster
// branch of ExpireStacksWorker.Work).
//
// These tests redirect the package-level saTokenFile / saCAFile /
// k8sAPIBaseURL vars (declared in expire_stacks.go solely for this
// purpose) at temp files + an httptest server, then restore them.

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

// k8sStacksRowCols mirrors the projection in expire_stacks.go::Work. The
// black-box stacksRowCols lives in package jobs_test and isn't visible to
// this in-package file, so we re-declare it here.
var k8sStacksRowCols = []string{"id", "slug", "namespace"}

// writeSelfSignedCA writes a valid PEM CA cert to dir/ca.crt and returns
// the path. A real PEM is required so x509.NewCertPool().AppendCertsFromPEM
// actually appends (an invalid blob is silently dropped).
func writeSelfSignedCA(t *testing.T, dir string) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	caPath := filepath.Join(dir, "ca.crt")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(caPath, pemBytes, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	return caPath
}

// withSAFiles points the package SA-path vars at a temp token + CA file
// and restores them on cleanup.
func withSAFiles(t *testing.T) (tokenPath, caPath string) {
	t.Helper()
	dir := t.TempDir()
	tokenPath = filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("fake-sa-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	caPath = writeSelfSignedCA(t, dir)

	origToken, origCA := saTokenFile, saCAFile
	saTokenFile, saCAFile = tokenPath, caPath
	t.Cleanup(func() { saTokenFile, saCAFile = origToken, origCA })
	return tokenPath, caPath
}

// TestInClusterK8sClient_BuildsWhenSATokenPresent covers the full
// happy path of inClusterK8sClient: Stat ok → ReadFile CA → cert pool →
// http.Client with a TLS transport.
func TestInClusterK8sClient_BuildsWhenSATokenPresent(t *testing.T) {
	withSAFiles(t)
	c := inClusterK8sClient()
	if c == nil {
		t.Fatal("expected a non-nil client when SA token + CA are present")
	}
	if c.Timeout != 30*time.Second {
		t.Errorf("client timeout = %v, want 30s", c.Timeout)
	}
	if _, ok := c.Transport.(*http.Transport); !ok {
		t.Errorf("expected an *http.Transport, got %T", c.Transport)
	}
}

// TestInClusterK8sClient_CAReadFails covers the branch where the token
// file exists but the CA file is unreadable → returns nil with a warn.
func TestInClusterK8sClient_CAReadFails(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("tok"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	origToken, origCA := saTokenFile, saCAFile
	saTokenFile = tokenPath
	saCAFile = filepath.Join(dir, "does-not-exist.crt")
	t.Cleanup(func() { saTokenFile, saCAFile = origToken, origCA })

	if c := inClusterK8sClient(); c != nil {
		t.Error("expected nil client when CA cert is unreadable")
	}
}

// TestDeleteK8sNamespace_StatusHandling exercises the DELETE round-trip
// for every status branch: 200/202/404 → nil, anything else → error.
func TestDeleteK8sNamespace_StatusHandling(t *testing.T) {
	withSAFiles(t)

	cases := []struct {
		name    string
		status  int
		wantErr bool
	}{
		{"ok", http.StatusOK, false},
		{"accepted", http.StatusAccepted, false},
		{"not_found", http.StatusNotFound, false},
		{"server_error", http.StatusInternalServerError, true},
		{"forbidden", http.StatusForbidden, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotAuth, gotMethod, gotPath string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotAuth = r.Header.Get("Authorization")
				gotMethod = r.Method
				gotPath = r.URL.Path
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			origBase := k8sAPIBaseURL
			k8sAPIBaseURL = srv.URL
			t.Cleanup(func() { k8sAPIBaseURL = origBase })

			err := deleteK8sNamespace(context.Background(), srv.Client(),
				"instant-stack-abc123", "instant-stack-")
			if tc.wantErr && err == nil {
				t.Errorf("status %d: expected error, got nil", tc.status)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("status %d: unexpected error: %v", tc.status, err)
			}
			if gotMethod != http.MethodDelete {
				t.Errorf("method = %q, want DELETE", gotMethod)
			}
			if gotAuth != "Bearer fake-sa-token" {
				t.Errorf("auth header = %q, want bearer with trimmed token", gotAuth)
			}
			if gotPath != "/api/v1/namespaces/instant-stack-abc123" {
				t.Errorf("path = %q", gotPath)
			}
		})
	}
}

// TestDeleteK8sNamespace_TransportError covers the client.Do error
// branch (server closed / unreachable).
func TestDeleteK8sNamespace_TransportError(t *testing.T) {
	withSAFiles(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // close immediately so Do fails

	origBase := k8sAPIBaseURL
	k8sAPIBaseURL = url
	t.Cleanup(func() { k8sAPIBaseURL = origBase })

	err := deleteK8sNamespace(context.Background(), &http.Client{Timeout: time.Second},
		"instant-stack-y", "instant-stack-")
	if err == nil {
		t.Error("expected transport error against a closed server")
	}
}

// TestExpireStacksWork_InClusterBranch covers ExpireStacksWorker.Work's
// in-cluster path (w.k8sClient != nil), which NewExpireStacksWorker never
// produces outside a real cluster. We set k8sClient directly (in-package
// access) and back it with an httptest server: one row's namespace tears
// down OK → its stacks row is DELETEd; a second row's teardown returns 500
// → it is skipped (continue) and NOT deleted. Uses sqlmock so no live
// stacks table is required.
func TestExpireStacksWork_InClusterBranch(t *testing.T) {
	withSAFiles(t)

	// Server returns 500 for the "fail" namespace, 200 otherwise.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/namespaces/instant-stack-fail" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	origBase := k8sAPIBaseURL
	k8sAPIBaseURL = srv.URL
	t.Cleanup(func() { k8sAPIBaseURL = origBase })

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	okID := uuid.New().String()
	failID := uuid.New().String()
	mock.ExpectQuery(`FROM stacks`).
		WillReturnRows(sqlmock.NewRows(k8sStacksRowCols).
			AddRow(okID, "ok-stack", "instant-stack-ok").
			AddRow(failID, "fail-stack", "instant-stack-fail"))
	// Only the OK row is deleted; the failed-teardown row issues no DELETE.
	mock.ExpectExec(`DELETE FROM stacks WHERE id = \$1`).
		WithArgs(okID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := &ExpireStacksWorker{db: db, k8sClient: srv.Client(), nsPrefix: "instant-stack-"}
	if err := w.Work(context.Background(), fakeJobLocal[ExpireStacksArgs]()); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations (the failed-teardown row must NOT delete): %v", err)
	}
}
