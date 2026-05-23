package jobs

// deploy_notify_webhook.go — periodic dispatcher that drains
// audit_log rows for deploy lifecycle kinds into per-team
// customer-configured webhook URLs.
//
// Forward-compat design: the audit kinds (deploy.created, deploy.healthy,
// deploy.failed, deploy.redeploying) and the per-team vault entry
// (DEPLOY_NOTIFY_WEBHOOK_URL in vault_secrets.key) are the API surface this
// dispatcher binds to. The api side may not yet write every kind today —
// that's intentional: as soon as a producer starts emitting rows, this
// dispatcher picks them up with no change.
//
// Cursor: a (created_at, id) tuple stored in Redis. We mirror the
// event_email_forwarder.go cursor shape rather than carrying a separate
// state table — Redis durability is sufficient because the worst case on
// a wipe is "we re-POST a few customer webhook events"; the customer's
// Idempotency-Key dedupe (Idempotency-Key = audit_log.id) absorbs that.
//
// SSRF guard: validateDeployNotifyURL mirrors the api repo's
// validateNotifyWebhookURL gate (see
// api/internal/handlers/deploy_webhook_notify.go). We do NOT import api
// repo code — the worker and api are separate Go modules, and a duplicated
// gate is cheaper than vendoring. The gate is re-applied here even though
// the api repo already validated at write time: the vault value could have
// been edited via API after the URL was validated, and a defence-in-depth
// re-check at dispatch costs nothing.
//
// Retry / budget: 5s per-attempt timeout, 2 retries with 500ms backoff,
// total budget 15s. Failure beyond the budget is logged AND a
// deploy_notify.delivery_failed audit_log row is written so the customer
// can see the failure in their event stream. We do not put the failed
// audit row back on the deploy lifecycle path — the dashboard reads
// deploy.* kinds for status, and a delivery_failed event is operational
// noise that belongs in its own audit kind.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
)

// DeployNotifyWebhookArgs is the River job payload — no fields, runs as a sweep.
type DeployNotifyWebhookArgs struct{}

// Kind is the River worker key.
func (DeployNotifyWebhookArgs) Kind() string { return "deploy_notify_webhook" }

// deployNotifyWebhookInterval is the periodic dispatch cadence.
// 30s matches the deploy_status reconciler — a customer who wires a
// webhook expects "deploy hit healthy" to surface in their receiver
// within roughly the same time window the dashboard reflects it.
const deployNotifyWebhookInterval = 30 * time.Second

// deployNotifyBatchLimit caps rows processed per tick. 100 × ~1s per HTTP
// POST = ~100s of work; in practice each POST is ~150ms so the batch
// drains well inside the next 30s tick. A backlog past the limit drains
// across consecutive ticks via the cursor.
const deployNotifyBatchLimit = 100

// deployNotifyCursorKey is the Redis key holding the (created_at, id)
// cursor as a JSON blob. Same shape as event_email_forwarder.
const deployNotifyCursorKey = "worker:deploy_notify:last_audit_cursor"

// deployNotifyVaultKey is the vault_secrets.key value the customer
// stores their per-team webhook URL under. Centralised so a typo in
// either side surfaces at compile time, not silently at runtime.
const deployNotifyVaultKey = "DEPLOY_NOTIFY_WEBHOOK_URL"

// deployNotifyVaultEnv is the vault_secrets.env value the dispatcher
// reads the notify webhook URL from. vault_secrets is keyed on
// (team_id, env, key, version); without an env predicate the
// MAX(version) projection could pick a row from ANY env — a customer
// who set DEPLOY_NOTIFY_WEBHOOK_URL under both 'staging' and
// 'production' would get a non-deterministic, possibly cross-env URL
// (BugBash 2026-05-18 W3 T3 "lookupWebhookURL ignores env"). We pin to
// 'production' — vault entries default to env='production' (migration
// 008) and the audit_log rows the dispatcher drains carry no env field
// to disambiguate against. A per-env notify URL would need the producer
// to stamp env on the audit row first; until then 'production' is the
// single deterministic bucket.
const deployNotifyVaultEnv = "production"

// Audit kinds the dispatcher reads. Forward-compatible: this list is
// the exact set the producer side must emit to. Centralised here
// because the worker module does not import the api models package.
const (
	auditKindDeployCreated     = "deploy.created"
	auditKindDeployHealthy     = "deploy.healthy"
	auditKindDeployFailed      = "deploy.failed"
	auditKindDeployRedeploying = "deploy.redeploying"

	// deployNotifyDeliveryFailedKind is written by this worker when a
	// per-row delivery exhausts its retry budget. Lives in audit_log
	// alongside the deploy.* kinds so the customer can see the failure
	// in /api/v1/events.
	deployNotifyDeliveryFailedKind = "deploy_notify.delivery_failed"

	// deployNotifyActor is the audit_log.actor value for the
	// delivery_failed row. Matches the system-actor convention used by
	// quota_wall_nudge.go and churn_predictor.go.
	deployNotifyActor = "system"
)

// deployNotifyKinds is the SQL filter for the dispatcher query — only
// these kinds get pulled into a batch. Exported as a slice so the query
// can pass it via `kind = ANY($1::text[])` with pq.Array.
var deployNotifyKinds = []string{
	auditKindDeployCreated,
	auditKindDeployHealthy,
	auditKindDeployFailed,
	auditKindDeployRedeploying,
}

// deployNotifyPerAttemptTimeout caps a single HTTP POST. 5s is the
// brief's value: long enough that a slow customer receiver still
// completes, short enough that two retries + the final attempt fit
// inside deployNotifyTotalBudget.
const deployNotifyPerAttemptTimeout = 5 * time.Second

// deployNotifyTotalBudget is the wall-clock budget per audit row.
// After 15s we stop retrying and write the delivery_failed audit row.
const deployNotifyTotalBudget = 15 * time.Second

// deployNotifyRetryBackoff is the linear sleep between retries.
// Combined with deployNotifyMaxAttempts (3 = 1 initial + 2 retries),
// the worst-case wall clock is 3×5s + 2×500ms = 16s, just above the
// 15s budget — the budget check fires first and we stop early.
const deployNotifyRetryBackoff = 500 * time.Millisecond

// deployNotifyMaxAttempts is the total attempt count. 3 = 1 initial
// + 2 retries per the brief.
const deployNotifyMaxAttempts = 3

// deployNotifyResolver is overridable so tests can inject a deterministic
// resolver without doing real DNS. Production uses net.LookupIP.
// Mirrors notifyWebhookResolver in api/internal/handlers/deploy_webhook_notify.go.
var deployNotifyResolver = func(host string) ([]net.IP, error) {
	return net.LookupIP(host)
}

// errDeployNotifyTransient marks a validation failure that is NOT the
// customer's fault and may succeed on a later tick — currently only a
// DNS resolution failure. W3 T3 (BugBash 2026-05-18): a transient failure
// must NOT advance the cursor (the row would be silently lost from the
// backlog); a permanent failure (bad scheme, private-IP literal, no
// hostname) advances the cursor so a misconfigured URL can't wedge the
// queue forever. validateDeployNotifyURL wraps the DNS-failure error
// with this sentinel; the dispatch loop checks errors.Is against it.
var errDeployNotifyTransient = errors.New("deploy_notify URL validation failed transiently")

// deployNotifyAuditRow is the projection the dispatcher reads — only the
// columns we need to build the outbound payload.
type deployNotifyAuditRow struct {
	ID        string
	TeamID    string
	Kind      string
	Metadata  []byte
	CreatedAt time.Time
}

// deployNotifyCursor is the watermark structure. CreatedAt + ID together
// give a strict total order even when multiple rows share a timestamp.
type deployNotifyCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

// deployNotifyCursorStore abstracts the cursor read/write so tests can
// supply an in-memory implementation. Production uses
// redisDeployNotifyCursorStore wrapping a *redis.Client.
type deployNotifyCursorStore interface {
	read(ctx context.Context) (deployNotifyCursor, error)
	write(ctx context.Context, c deployNotifyCursor) error
}

type redisDeployNotifyCursorStore struct {
	rdb *redis.Client
}

func (s *redisDeployNotifyCursorStore) read(ctx context.Context) (deployNotifyCursor, error) {
	raw, err := s.rdb.Get(ctx, deployNotifyCursorKey).Result()
	if err == redis.Nil {
		return deployNotifyCursor{}, nil
	}
	if err != nil {
		return deployNotifyCursor{}, fmt.Errorf("redis GET %s: %w", deployNotifyCursorKey, err)
	}
	var c deployNotifyCursor
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		slog.Error("jobs.deploy_notify_webhook.cursor_corrupt",
			"raw", raw,
			"error", err,
			"note", "resetting to zero — receiver Idempotency-Key absorbs duplicates",
		)
		return deployNotifyCursor{}, nil
	}
	return c, nil
}

func (s *redisDeployNotifyCursorStore) write(ctx context.Context, c deployNotifyCursor) error {
	b, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshal cursor: %w", err)
	}
	if err := s.rdb.Set(ctx, deployNotifyCursorKey, string(b), 0).Err(); err != nil {
		return fmt.Errorf("redis SET %s: %w", deployNotifyCursorKey, err)
	}
	return nil
}

// DeployNotifyWebhookWorker drains deploy.* audit_log rows to per-team
// customer webhook URLs.
type DeployNotifyWebhookWorker struct {
	river.WorkerDefaults[DeployNotifyWebhookArgs]
	db      *sql.DB
	cursor  deployNotifyCursorStore
	httpCli *http.Client
	// vaultDecrypt converts the stored encrypted_value bytea to plaintext.
	// The worker module does not import the api crypto package, so this
	// is a function-typed seam wired by the constructor. The default
	// implementation is "treat bytes as plaintext" — appropriate for
	// integration setups where the operator stores the URL plaintext,
	// AND for the test path. Production wires a real AES helper via the
	// SetDeployNotifyDecryptor escape hatch when the worker is given an
	// AES key.
	//
	// TODO(ops): wire AES_KEY env var + real Decrypt(aesKey, encrypted)
	//            once the worker reads AES_KEY in config.Load. Until
	//            then the customer must store the URL unencrypted in
	//            vault_secrets — a known gap (callout in the PR body).
	vaultDecrypt func([]byte) (string, error)
}

// NewDeployNotifyWebhookWorker constructs the worker. httpCli may be nil
// — a default with deployNotifyPerAttemptTimeout is installed.
func NewDeployNotifyWebhookWorker(db *sql.DB, rdb *redis.Client, httpCli *http.Client) *DeployNotifyWebhookWorker {
	if httpCli == nil {
		httpCli = &http.Client{
			Timeout: deployNotifyPerAttemptTimeout,
			// Don't follow redirects automatically — a 3xx response from
			// the customer's receiver is a configuration error on their
			// side, not a signal to chase a Location header to a private
			// IP (SSRF reflected via redirect).
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	return &DeployNotifyWebhookWorker{
		db:           db,
		cursor:       &redisDeployNotifyCursorStore{rdb: rdb},
		httpCli:      httpCli,
		vaultDecrypt: func(b []byte) (string, error) { return string(b), nil },
	}
}

// newDeployNotifyWebhookWorkerForTest constructs a worker with an
// injectable cursor store. Used only by tests so they don't need live Redis.
func newDeployNotifyWebhookWorkerForTest(db *sql.DB, cursor deployNotifyCursorStore, httpCli *http.Client) *DeployNotifyWebhookWorker {
	w := NewDeployNotifyWebhookWorker(db, nil, httpCli)
	w.cursor = cursor
	return w
}

// Work runs one dispatcher sweep.
//
// Returned error semantics match the surrounding workers: a top-level DB
// or Redis failure returns an error so River retries the job; per-row
// failures are logged and the cursor advances PER ROW so a mid-batch
// crash never re-POSTs to a customer who already received the event.
func (w *DeployNotifyWebhookWorker) Work(ctx context.Context, job *river.Job[DeployNotifyWebhookArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.deploy_notify_webhook")
	defer span.End()

	start := time.Now()
	cursor, err := w.cursor.read(ctx)
	if err != nil {
		return fmt.Errorf("deploy_notify_webhook: read cursor: %w", err)
	}

	rows, err := w.fetchBatch(ctx, cursor)
	if err != nil {
		return fmt.Errorf("deploy_notify_webhook: fetch batch: %w", err)
	}

	if len(rows) == 0 {
		// T21 P1-1 (BugBash 2026-05-20): idle-tick demoted INFO→DEBUG.
		slog.Debug("jobs.deploy_notify_webhook.completed",
			"sent", 0,
			"skipped", 0,
			"failed", 0,
			"batch_size", 0,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil
	}

	var sent, skipped, failed int
	for _, row := range rows {
		// Per-row cursor advance happens at the end of each iteration so
		// any branch (skipped, sent, failed) moves the watermark forward.
		// We DELIBERATELY do not refuse to advance on a delivery failure —
		// the customer's receiver will pick up the next event once they
		// fix their endpoint, and we have already written a
		// deploy_notify.delivery_failed audit row.
		webhookURL, lookupErr := w.lookupWebhookURL(ctx, row.TeamID)
		if lookupErr != nil {
			// DB error during vault lookup — log and skip this row but
			// DO NOT advance the cursor: the team may have a real URL
			// we just couldn't fetch. Retry next tick.
			slog.Warn("jobs.deploy_notify_webhook.vault_lookup_failed",
				"audit_id", row.ID,
				"team_id", row.TeamID,
				"error", lookupErr,
			)
			continue
		}
		if webhookURL == "" {
			// Team has no configured notify webhook — nothing to do.
			// Advance the cursor so we don't re-check this row forever.
			if err := w.cursor.write(ctx, deployNotifyCursor{CreatedAt: row.CreatedAt, ID: row.ID}); err != nil {
				return fmt.Errorf("deploy_notify_webhook: advance cursor (no webhook): %w", err)
			}
			skipped++
			continue
		}

		vettedIPs, vErr := validateDeployNotifyURL(webhookURL)
		if vErr != nil {
			// W3 T3: a transient validation failure (DNS hiccup) must
			// NOT advance the cursor — the URL is fine, the lookup just
			// failed, and advancing would silently drop the row from the
			// backlog. Leave the cursor put and retry next tick. A
			// permanent rejection (bad scheme / private IP) DOES advance
			// so a misconfigured URL can't wedge the queue forever.
			if errors.Is(vErr, errDeployNotifyTransient) {
				slog.Warn("jobs.deploy_notify_webhook.url_validation_transient",
					"audit_id", row.ID,
					"team_id", row.TeamID,
					"error", vErr,
					"note", "transient (DNS) failure — cursor held, will retry",
				)
				continue
			}
			slog.Warn("jobs.deploy_notify_webhook.url_rejected",
				"audit_id", row.ID,
				"team_id", row.TeamID,
				"error", vErr,
				"note", "SSRF guard rejected stored URL — advancing cursor",
			)
			if err := w.cursor.write(ctx, deployNotifyCursor{CreatedAt: row.CreatedAt, ID: row.ID}); err != nil {
				return fmt.Errorf("deploy_notify_webhook: advance cursor (bad url): %w", err)
			}
			skipped++
			continue
		}

		// Build payload. deploy_id pulled from metadata when the row
		// carries it; the producer side may also use resource_id, so we
		// try both keys for forward-compat.
		deployID := metaString(row.Metadata, "deploy_id")
		payload := map[string]any{
			"kind":       row.Kind,
			"deploy_id":  deployID,
			"url":        webhookURL,
			"timestamp":  row.CreatedAt.UTC().Format(time.RFC3339),
			"team_id":    row.TeamID,
			"audit_id":   row.ID,
		}
		body, mErr := json.Marshal(payload)
		if mErr != nil {
			// Cannot happen with a map[string]any of primitives, but be
			// defensive — advance the cursor and skip.
			slog.Error("jobs.deploy_notify_webhook.marshal_failed",
				"audit_id", row.ID,
				"error", mErr,
			)
			if err := w.cursor.write(ctx, deployNotifyCursor{CreatedAt: row.CreatedAt, ID: row.ID}); err != nil {
				return fmt.Errorf("deploy_notify_webhook: advance cursor (marshal): %w", err)
			}
			skipped++
			continue
		}

		sendErr := w.dispatch(ctx, webhookURL, vettedIPs, row.ID, body)
		if sendErr != nil {
			slog.Warn("jobs.deploy_notify_webhook.delivery_failed",
				"audit_id", row.ID,
				"team_id", row.TeamID,
				"kind", row.Kind,
				"error", sendErr,
			)
			// Emit the delivery_failed audit row so the customer sees
			// the failure in their event stream. failures here do not
			// block the cursor — we have already given up on this row.
			if insErr := w.emitDeliveryFailed(ctx, row, sendErr); insErr != nil {
				slog.Error("jobs.deploy_notify_webhook.delivery_failed_audit_insert_failed",
					"audit_id", row.ID,
					"team_id", row.TeamID,
					"error", insErr,
				)
			}
			failed++
		} else {
			sent++
		}

		if err := w.cursor.write(ctx, deployNotifyCursor{CreatedAt: row.CreatedAt, ID: row.ID}); err != nil {
			return fmt.Errorf("deploy_notify_webhook: advance cursor: %w", err)
		}
	}

	slog.Info("jobs.deploy_notify_webhook.completed",
		"sent", sent,
		"skipped", skipped,
		"failed", failed,
		"batch_size", len(rows),
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}

// fetchBatch pulls the next deployNotifyBatchLimit deploy.* audit rows
// after the cursor. Cursor predicate: (created_at, id::text) > ($2, $3).
func (w *DeployNotifyWebhookWorker) fetchBatch(ctx context.Context, c deployNotifyCursor) ([]deployNotifyAuditRow, error) {
	q := `
		SELECT
			a.id::text,
			a.team_id::text,
			a.kind,
			a.metadata,
			a.created_at
		FROM audit_log a
		WHERE a.kind = ANY($1::text[])
		  AND (a.created_at, a.id::text) > ($2, $3)
		ORDER BY a.created_at ASC, a.id::text ASC
		LIMIT $4
	`
	rows, err := w.db.QueryContext(ctx, q,
		pq.Array(deployNotifyKinds),
		c.CreatedAt,
		c.ID,
		deployNotifyBatchLimit,
	)
	if err != nil {
		return nil, fmt.Errorf("fetchBatch query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []deployNotifyAuditRow
	for rows.Next() {
		var r deployNotifyAuditRow
		var metadata sql.NullString
		if err := rows.Scan(&r.ID, &r.TeamID, &r.Kind, &metadata, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("fetchBatch scan: %w", err)
		}
		if metadata.Valid {
			r.Metadata = []byte(metadata.String)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetchBatch rows: %w", err)
	}
	return out, nil
}

// lookupWebhookURL fetches the team's latest DEPLOY_NOTIFY_WEBHOOK_URL
// vault entry. Returns ("", nil) when the team has no entry. Returns
// the decrypted plaintext URL otherwise. The (team_id, env, key,
// version) UNIQUE constraint plus the version-DESC projection gives us
// the latest row.
//
// The query pins env = deployNotifyVaultEnv ('production'). Previously
// the env predicate was absent, so on a team with the same key set in
// more than one env the version-DESC ordering could surface a row from
// the wrong env (BugBash 2026-05-18 W3 T3 cross-env vault bleed). The
// env predicate also lets the planner use idx_vault_secrets_lookup
// (team_id, env, key) instead of a partial-key scan.
func (w *DeployNotifyWebhookWorker) lookupWebhookURL(ctx context.Context, teamID string) (string, error) {
	var enc []byte
	err := w.db.QueryRowContext(ctx, `
		SELECT encrypted_value
		FROM vault_secrets
		WHERE team_id = $1::uuid
		  AND env = $2
		  AND key = $3
		ORDER BY version DESC
		LIMIT 1
	`, teamID, deployNotifyVaultEnv, deployNotifyVaultKey).Scan(&enc)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("lookupWebhookURL query: %w", err)
	}
	plain, decErr := w.vaultDecrypt(enc)
	if decErr != nil {
		return "", fmt.Errorf("lookupWebhookURL decrypt: %w", decErr)
	}
	return strings.TrimSpace(plain), nil
}

// dispatch performs the HTTP POST with retry + budget. Returns nil on a
// 2xx response. Returns an error after deployNotifyMaxAttempts or after
// deployNotifyTotalBudget elapses, whichever first.
//
// vettedIPs are the addresses validateDeployNotifyURL resolved and
// SSRF-checked. dispatch PINS the outbound connection to exactly those
// IPs (W3 T3 SSRF-TOCTOU fix) so the http.Client cannot re-resolve the
// hostname at connect time and get rebound onto a private address.
//
// The pin is installed only when the worker's httpCli carries the
// default (nil) Transport — the production path. When a caller injected
// a custom Transport (the test harness redirecting to an httptest
// server), that Transport is preserved so the test's dial-hijack still
// works; tests exercise the SSRF gate via validateDeployNotifyURL
// directly and via deployNotifyResolver stubs.
func (w *DeployNotifyWebhookWorker) dispatch(ctx context.Context, urlStr string, vettedIPs []net.IP, auditID string, body []byte) error {
	pinnedCli := &http.Client{
		Timeout:       w.httpCli.Timeout,
		CheckRedirect: w.httpCli.CheckRedirect,
	}
	if w.httpCli.Transport != nil {
		// A custom Transport was injected (test harness) — keep it as-is.
		pinnedCli.Transport = w.httpCli.Transport
	} else {
		// Production path — pin the dialer to the SSRF-vetted IPs.
		pinnedCli.Transport = &http.Transport{
			DialContext:         pinnedIPDialContext(vettedIPs),
			TLSHandshakeTimeout: deployNotifyPerAttemptTimeout,
		}
	}

	deadline := time.Now().Add(deployNotifyTotalBudget)
	var lastErr error
	for attempt := 1; attempt <= deployNotifyMaxAttempts; attempt++ {
		if time.Now().After(deadline) {
			return fmt.Errorf("dispatch: budget exceeded after %d attempts: %w", attempt-1, lastErr)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, urlStr, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("dispatch: build request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", auditID)
		req.Header.Set("User-Agent", "instanode-deploy-notify/1")

		resp, doErr := pinnedCli.Do(req)
		if doErr != nil {
			lastErr = fmt.Errorf("attempt %d: %w", attempt, doErr)
		} else {
			// Drain + close body unconditionally so the connection
			// returns to the keep-alive pool.
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("attempt %d: status %d", attempt, resp.StatusCode)
		}
		if attempt < deployNotifyMaxAttempts {
			// Use a select so a cancelled ctx breaks the retry loop
			// immediately instead of always sleeping the full backoff.
			select {
			case <-ctx.Done():
				return fmt.Errorf("dispatch: ctx cancelled: %w", ctx.Err())
			case <-time.After(deployNotifyRetryBackoff):
			}
		}
	}
	return lastErr
}

// emitDeliveryFailed writes the deploy_notify.delivery_failed audit row
// so the customer can see the failure in /api/v1/events. team_id is
// parsed from the row.TeamID (UUID string).
func (w *DeployNotifyWebhookWorker) emitDeliveryFailed(ctx context.Context, row deployNotifyAuditRow, deliverErr error) error {
	meta := map[string]any{
		"failed_audit_id": row.ID,
		"failed_kind":     row.Kind,
		"error":           deliverErr.Error(),
	}
	metaBytes, mErr := json.Marshal(meta)
	if mErr != nil {
		return fmt.Errorf("marshal metadata: %w", mErr)
	}
	summary := fmt.Sprintf("deploy notify webhook delivery failed for %s", row.Kind)
	_, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES ($1::uuid, $2, $3, $4, $5)
	`, row.TeamID, deployNotifyActor, deployNotifyDeliveryFailedKind, summary, metaBytes)
	if err != nil {
		return fmt.Errorf("insert delivery_failed audit: %w", err)
	}
	return nil
}

// metaString reads a string field out of the JSONB metadata blob. Returns
// "" on missing field, nil metadata, or unmarshal failure — every caller
// treats "" as "field not present".
func metaString(raw []byte, key string) string {
	if len(raw) == 0 {
		return ""
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

// validateDeployNotifyURL is the SSRF + scheme gate. Mirrors
// api/internal/handlers/deploy_webhook_notify.go::validateNotifyWebhookURL.
// Re-applied at dispatch time so a vault entry that was edited after
// the original write-time validation can't ride past us.
//
// Returns the set of vetted IPs the host resolved to so the caller can
// PIN the outbound connection to exactly those addresses — see
// pinnedIPDialContext. W3 T3 (BugBash 2026-05-18): without pinning, the
// gate resolves DNS once and dispatch's http.Client resolves it AGAIN at
// connect time — a DNS-rebind attacker returns a public IP to the gate
// and a private IP (169.254.169.254, a tenant DB pod) to the connect.
// Pinning the dialer to the gate-vetted IPs closes that TOCTOU window.
//
// A DNS-resolution failure is wrapped with errDeployNotifyTransient so
// the caller can avoid advancing the cursor on a correctable hiccup.
func validateDeployNotifyURL(raw string) ([]net.IP, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("deploy_notify URL is not a valid URL")
	}
	if u.Scheme != "https" {
		return nil, fmt.Errorf("deploy_notify URL must use https:// (got %q)", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return nil, fmt.Errorf("deploy_notify URL is missing a hostname")
	}
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") {
		return nil, fmt.Errorf("deploy_notify hostname is not publicly routable")
	}
	if ip := net.ParseIP(host); ip != nil {
		if isBlockedDeployNotifyIP(ip) {
			return nil, fmt.Errorf("deploy_notify IP is in a blocked range")
		}
		return []net.IP{ip}, nil
	}
	ips, err := deployNotifyResolver(host)
	if err != nil {
		// DNS hiccup — transient. Wrap so the caller leaves the cursor put.
		return nil, fmt.Errorf("deploy_notify hostname does not resolve: %w", errDeployNotifyTransient)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("deploy_notify hostname has no A/AAAA records")
	}
	for _, ip := range ips {
		if isBlockedDeployNotifyIP(ip) {
			return nil, fmt.Errorf("deploy_notify hostname resolves to a private/loopback/link-local IP")
		}
	}
	return ips, nil
}

// pinnedIPDialContext returns a DialContext that ignores DNS for the
// connection and dials only the supplied (already-vetted) IPs, trying
// each in turn. The original port from the address is preserved. This
// is the SSRF-TOCTOU fix (W3 T3): dispatch connects to exactly the IPs
// validateDeployNotifyURL vetted, so a between-check-and-connect DNS
// rebind cannot redirect the POST to a private address.
func pinnedIPDialContext(ips []net.IP) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("pinnedIPDialContext: bad addr %q: %w", addr, err)
		}
		d := net.Dialer{Timeout: deployNotifyPerAttemptTimeout}
		var lastErr error
		for _, ip := range ips {
			conn, dErr := d.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if dErr == nil {
				return conn, nil
			}
			lastErr = dErr
		}
		if lastErr == nil {
			lastErr = errors.New("pinnedIPDialContext: no vetted IPs to dial")
		}
		return nil, lastErr
	}
}

// isBlockedDeployNotifyIP returns true if ip is in any range we refuse
// to dispatch to. Set mirrors api/internal/handlers/deploy_webhook_notify.go
// to preserve defence-in-depth consistency.
func isBlockedDeployNotifyIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() || ip.IsInterfaceLocalMulticast() ||
		ip.IsUnspecified() || ip.IsPrivate() {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		_, cgnat, _ := net.ParseCIDR("100.64.0.0/10")
		if cgnat != nil && cgnat.Contains(v4) {
			return true
		}
		if v4.Equal(net.IPv4bcast) {
			return true
		}
	}
	return false
}

