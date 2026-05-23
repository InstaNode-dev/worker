package jobs

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/riverqueue/river"
)

// ExpireStacksNamespacePrefix is the prefix used by the api stack
// provider (`compute.StackNamespace = "instant-stack-" + stackID`).
// EVERY namespace handled by this worker must start with this prefix —
// the `deleteK8sNamespace` safety guard refuses anything else.
//
// T6 P0-1 (BugBash 2026-05-20): before this constant, the prefix came
// from `cfg.KubeNamespaceApps+"-"` = "instant-apps-", which never
// matches a real stack namespace → the safety guard refused every
// delete and returned nil-success → the ExpireStacks Worker proceeded
// to DELETE the `stacks` row anyway, orphaning the namespace, pods,
// services, ingress, and TLS cert forever with no DB pointer.
const ExpireStacksNamespacePrefix = "instant-stack-"

// saTokenFile / saCAFile are the in-cluster ServiceAccount projected-volume
// paths. They are package vars (not consts) ONLY so tests can point them at
// a temp file to exercise the in-cluster HTTP teardown path; production never
// reassigns them.
var (
	saTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	saCAFile    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

// k8sAPIBaseURL is the in-cluster Kubernetes API base. Package var ONLY so
// tests can redirect the DELETE at an httptest server; production keeps the
// default in-cluster service DNS name.
var k8sAPIBaseURL = "https://kubernetes.default.svc"

// ExpireStacksArgs holds the arguments for the ExpireStacksJob.
// No fields are needed — it's a periodic maintenance job.
type ExpireStacksArgs struct{}

func (ExpireStacksArgs) Kind() string { return "expire_stacks" }

// inClusterK8sClient builds an HTTP client using the pod's projected ServiceAccount
// token and CA certificate. Returns nil (with a warning) if not running in-cluster.
func inClusterK8sClient() *http.Client {
	if _, err := os.Stat(saTokenFile); err != nil {
		return nil // not running in-cluster
	}
	ca, err := os.ReadFile(saCAFile)
	if err != nil {
		slog.Warn("expire_stacks: cannot read SA CA cert — namespace teardown disabled", "error", err)
		return nil
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}},
	}
}

// deleteK8sNamespace issues DELETE /api/v1/namespaces/{name} using the pod's SA token.
// It is safe to call when the namespace does not exist (404 is treated as success).
// The namespace name must start with nsPrefix as a safety guard.
func deleteK8sNamespace(ctx context.Context, client *http.Client, namespace, nsPrefix string) error {
	if !strings.HasPrefix(namespace, nsPrefix) {
		// Safety guard: never delete namespaces we didn't create.
		slog.Warn("expire_stacks: refusing to delete namespace — unexpected prefix",
			"namespace", namespace, "expected_prefix", nsPrefix)
		return nil
	}

	tokenBytes, err := os.ReadFile(saTokenFile)
	if err != nil {
		return fmt.Errorf("deleteK8sNamespace: read SA token: %w", err)
	}

	apiURL := k8sAPIBaseURL + "/api/v1/namespaces/" + namespace
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, apiURL, nil)
	if err != nil {
		return fmt.Errorf("deleteK8sNamespace: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tokenBytes)))

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("deleteK8sNamespace: DELETE %s: %w", namespace, err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted, http.StatusNotFound:
		return nil // success or already gone
	default:
		return fmt.Errorf("deleteK8sNamespace: k8s returned %d for namespace %s", resp.StatusCode, namespace)
	}
}

// ExpireStacksWorker hard-deletes anonymous stacks whose expires_at has passed,
// and tears down their k8s namespaces when running inside the cluster.
type ExpireStacksWorker struct {
	river.WorkerDefaults[ExpireStacksArgs]
	db           *sql.DB
	k8sClient    *http.Client // nil when not in-cluster; namespace teardown is skipped
	nsPrefix     string       // expected namespace prefix, e.g. "instant-apps-"
}

// NewExpireStacksWorker constructs an ExpireStacksWorker.
// nsPrefix is the namespace prefix used by the stack provider (e.g. "instant-apps-").
// Pass an empty string to disable namespace teardown (falls back to log-only).
func NewExpireStacksWorker(db *sql.DB, nsPrefix string) *ExpireStacksWorker {
	return &ExpireStacksWorker{
		db:        db,
		k8sClient: inClusterK8sClient(),
		nsPrefix:  nsPrefix,
	}
}

// Work queries for expired anonymous stacks and hard-deletes them, tearing down
// their k8s namespaces when running inside the cluster.
func (w *ExpireStacksWorker) Work(ctx context.Context, job *river.Job[ExpireStacksArgs]) error {
	start := time.Now()

	rows, err := w.db.QueryContext(ctx, `
		SELECT id::text, slug, namespace
		FROM stacks
		WHERE expires_at IS NOT NULL
		  AND expires_at < now()
		  AND status NOT IN ('deleted', 'deleting', 'failed', 'stopped')
	`)
	if err != nil {
		return fmt.Errorf("ExpireStacksWorker: query failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type expiredStack struct {
		id        string
		slug      string
		namespace string
	}
	var expired []expiredStack
	for rows.Next() {
		var s expiredStack
		if err := rows.Scan(&s.id, &s.slug, &s.namespace); err != nil {
			return fmt.Errorf("ExpireStacksWorker: scan failed: %w", err)
		}
		expired = append(expired, s)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("ExpireStacksWorker: rows error: %w", err)
	}
	_ = rows.Close()

	var deleted int
	for _, s := range expired {
		// Tear down k8s namespace first — if this fails, skip DB deletion so we
		// can retry next run.
		if w.k8sClient != nil && s.namespace != "" {
			if nsErr := deleteK8sNamespace(ctx, w.k8sClient, s.namespace, w.nsPrefix); nsErr != nil {
				slog.Error("jobs.expire_stacks.namespace_teardown_failed",
					"slug", s.slug, "namespace", s.namespace, "error", nsErr)
				continue // retry next hourly run
			}
			slog.Info("jobs.expire_stacks.namespace_deleted", "slug", s.slug, "namespace", s.namespace)
		} else if s.namespace != "" {
			// Not running in-cluster: the namespace is still live. Hard-deleting
			// the stacks row here would orphan the namespace with no DB pointer
			// — a later in-cluster worker run would never see it to tear it
			// down. Skip the DELETE and leave the row for the next in-cluster
			// run to expire properly. (TTL still wins eventually — the row's
			// expires_at remains in the past so a future in-cluster tick picks
			// it up.)
			slog.Warn("jobs.expire_stacks.delete_skipped",
				"slug", s.slug,
				"namespace", s.namespace,
				"note", "not running in-cluster — row left intact so a later in-cluster run can tear down the namespace before deleting the row",
			)
			continue
		}

		if _, err := w.db.ExecContext(ctx, `DELETE FROM stacks WHERE id = $1`, s.id); err != nil {
			slog.Error("jobs.expire_stacks.delete_failed",
				"slug", s.slug, "namespace", s.namespace, "error", err)
			continue
		}
		slog.Info("jobs.expire_stacks.deleted", "slug", s.slug, "namespace", s.namespace)
		deleted++
	}

	// Wave 3 / Worker T21 P1-1 follow-up (#146): demote idle-tick INFO →
	// DEBUG. expire_stacks runs every 1h; an idle tick (deleted==0) is
	// heartbeat noise. INFO retained when stacks actually expired so the
	// state-transition row appears in dashboards.
	if deleted == 0 {
		slog.Debug("jobs.expire_stacks.completed",
			"expired_count", 0,
			"duration_ms", time.Since(start).Milliseconds(),
			"job_id", job.ID,
		)
		return nil
	}
	slog.Info("jobs.expire_stacks.completed",
		"expired_count", deleted,
		"duration_ms", time.Since(start).Milliseconds(),
		"job_id", job.ID,
	)
	return nil
}
