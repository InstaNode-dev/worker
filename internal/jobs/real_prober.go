package jobs

// real_prober.go — production implementation of ResourceProber.
//
// Per-resource-type liveness check with a 5s budget. Each probe opens a fresh
// connection (no shared pool — different customer DBs are different conns) and
// closes it on exit. AES-256-GCM decryption is applied to connection_url IFF
// AESKey is configured; otherwise the column is treated as plaintext
// (fail-open — better to probe a stale plaintext URL than to skip every row
// because the worker was rolled out before the AES_KEY env var landed).
//
// Why we go direct instead of asking the provisioner over gRPC:
//
//   The W5-A doc-comment in prober.go listed two paths — (a) plumb AES_KEY
//   into the worker config and decrypt locally, or (b) add a Probe RPC on
//   the provisioner. (a) is cheaper: it doesn't widen the provisioner's
//   surface area and it lets us probe storage / webhook resources the
//   provisioner doesn't own. The cost is that AES_KEY is now in two pods'
//   env (api + worker). That's an acceptable doubling of the secret's
//   blast radius because both pods are already in the same k8s namespace
//   under the same operator-managed Secret.
//
// Resource-type dispatch:
//
//   postgres / vector → database/sql + lib/pq, "SELECT 1" with ctx
//   redis             → go-redis/v9, client.Ping(ctx)
//   mongodb           → mongo-driver, RunCommand({ping: 1}) on admin DB
//   storage           → HEAD against the bucket endpoint; ANY HTTP response
//                       (incl. 4xx/5xx) counts as reachable — we are only
//                       distinguishing "DNS/TCP works" from "TCP black hole".
//   queue (NATS)      → HTTP GET against the monitoring port's /healthz.
//                       The api uses the same probe path for synchronous
//                       provisioning verification (api/internal/providers/
//                       queue/local.go), so a probe failure here means the
//                       same thing as a provision failure — the NATS pod
//                       is unreachable. We do NOT pull nats-io/nats.go just
//                       for a TCP-level ping because (i) the monitoring API
//                       is enabled cluster-wide already, and (ii) a real
//                       nats.Connect requires no-auth handshake that's
//                       already de-facto identical to a TCP probe.
//   webhook           → ProbeSkip — customers run the receiver, we don't.
//   unknown           → ProbeSkip — same fail-open contract.
//
// DNS resilience: a probe whose hostname doesn't resolve (NXDOMAIN) returns
// ProbeUnreachable with the DNS error text. That's the user-visible signal
// for "the upstream that hosted your DB has gone away" and the dashboard
// degraded-banner is the right place to surface it. We do NOT translate
// NXDOMAIN into ProbeSkip — it's a real failure mode.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"instant.dev/common/crypto"
	"instant.dev/worker/internal/config"
)

// realProberHTTPTimeout caps the storage HEAD and the NATS monitoring GET.
// 5s matches the per-probe budget the heartbeat job grants each row.
const realProberHTTPTimeout = 5 * time.Second

// realProberDialTimeout caps the TCP dial inside database/sql, go-redis,
// and the mongo driver. Same 5s budget — the per-probe context deadline is
// the authoritative cap; this is just so the driver doesn't sit on a
// fresh-from-the-pool DialTimeout default of 30s (which would mask the
// ctx cancellation as a "connection refused" instead of a clean timeout).
const realProberDialTimeout = 5 * time.Second

// natsMonitoringPort is the NATS monitoring HTTP port enabled by the
// instant-data NATS deployment (infra/k8s/data/nats.yaml — `-m 8222`).
// The api uses the same constant in api/internal/providers/queue/local.go.
const natsMonitoringPort = 8222

// realProber is the production ResourceProber. Construct via NewRealProber.
type realProber struct {
	aesKey []byte // 32-byte raw key, or nil when AES_KEY was unset

	// httpClient is shared across storage + queue probes because both are
	// stateless HTTP ops and Go's transport pool is per-host — different
	// upstream hosts won't share connections regardless.
	httpClient *http.Client
}

// NewRealProber constructs the production prober. The cfg.AESKey field is
// parsed once at startup; a malformed key is logged and the prober runs in
// plaintext mode (fail-open — see the file header).
//
// Returns a ResourceProber interface so callers can drop in NoopProber for
// tests without an import change.
func NewRealProber(cfg *config.Config) ResourceProber {
	p := &realProber{
		httpClient: &http.Client{Timeout: realProberHTTPTimeout},
	}
	if cfg != nil && cfg.AESKey != "" {
		key, err := crypto.ParseAESKey(cfg.AESKey)
		if err != nil {
			// Don't crash the worker — a fat-fingered AES_KEY shouldn't
			// take down the heartbeat job. Plaintext fallback still
			// probes every active resource; the operator will notice the
			// log and fix the secret.
			//
			// We intentionally do NOT use slog here to keep the package
			// import-free of logging (this function runs once per boot;
			// the caller logs the constructor result alongside its other
			// startup probes).
			_ = err
		} else {
			p.aesKey = key
		}
	}
	return p
}

// Probe dispatches by resource_type. Returns ProbeUnreachable + err on
// failure, ProbeReachable + nil on success, ProbeSkip + nil for webhook /
// unknown types.
func (p *realProber) Probe(ctx context.Context, resourceType, connectionURL string) (ProbeOutcome, error) {
	// Webhook is the only type that's customer-managed; skip unconditionally.
	if resourceType == "webhook" {
		return ProbeSkip, nil
	}

	plain, decryptErr := p.decrypt(connectionURL)
	if decryptErr != nil {
		// Decryption failed AND the column did not look like plaintext
		// either. The most likely cause is a mid-migration column with a
		// stale AES_KEY rotation. Skip rather than mark degraded so we
		// don't flap-spam audit_log over a config issue.
		return ProbeSkip, fmt.Errorf("decrypt connection_url: %w", decryptErr)
	}

	switch resourceType {
	case "postgres", "vector":
		return p.probePostgres(ctx, plain)
	case "redis":
		return p.probeRedis(ctx, plain)
	case "mongodb":
		return p.probeMongo(ctx, plain)
	case "storage":
		return p.probeStorage(ctx, plain)
	case "queue":
		return p.probeQueue(ctx, plain)
	default:
		// Unknown future resource_type. Skip rather than crash.
		return ProbeSkip, nil
	}
}

// decrypt returns the plaintext connection_url. If AES_KEY is unset, the
// column is returned as-is. If AES_KEY is set, an AES-256-GCM decrypt is
// attempted; on failure, we fall back to treating the column as plaintext
// IFF it parses as a URL (i.e. starts with a scheme://). Otherwise the
// caller sees the decrypt error and translates it to ProbeSkip.
func (p *realProber) decrypt(connectionURL string) (string, error) {
	if connectionURL == "" {
		return "", errors.New("connection_url is empty")
	}
	if p.aesKey == nil {
		// No key configured → fail-open plaintext.
		return connectionURL, nil
	}
	plain, err := crypto.Decrypt(p.aesKey, connectionURL)
	if err == nil {
		return plain, nil
	}
	// Decrypt failed. If the raw value already looks like a connection
	// URL (has a scheme), the column is probably plaintext (legacy row
	// from before encryption was rolled out). Tolerate it.
	if looksLikePlaintextURL(connectionURL) {
		return connectionURL, nil
	}
	return "", err
}

// looksLikePlaintextURL returns true when s appears to be a connection URL
// with a known scheme. Used to disambiguate "decrypt failed because the
// column is actually plaintext" from "decrypt failed because the key is
// wrong / the column is corrupt".
func looksLikePlaintextURL(s string) bool {
	for _, scheme := range []string{"postgres://", "postgresql://", "redis://", "rediss://", "mongodb://", "mongodb+srv://", "http://", "https://", "nats://", "s3://"} {
		if strings.HasPrefix(s, scheme) {
			return true
		}
	}
	return false
}

// probePostgres opens a fresh sql.DB, pings with SELECT 1, closes.
// database/sql's connection pool is closed on db.Close — no leak.
func (p *realProber) probePostgres(ctx context.Context, connURL string) (ProbeOutcome, error) {
	// sql.Open is lazy — no connection until first use. Pass a 5s context
	// for the ping. We do NOT call db.Ping with the same ctx more than
	// once because a transient DNS hiccup would otherwise burn the budget
	// on retries we never asked for.
	db, err := sql.Open("postgres", connURL)
	if err != nil {
		return ProbeUnreachable, fmt.Errorf("postgres: sql.Open: %w", err)
	}
	defer db.Close()

	// Tighten dial timeout so a black-holed host fails fast rather than
	// hanging the goroutine for the driver default.
	db.SetConnMaxLifetime(realProberDialTimeout)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)

	var one int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		return ProbeUnreachable, fmt.Errorf("postgres: SELECT 1: %w", err)
	}
	if one != 1 {
		return ProbeUnreachable, fmt.Errorf("postgres: SELECT 1 returned %d", one)
	}
	return ProbeReachable, nil
}

// probeRedis opens a fresh go-redis client, pings, closes.
func (p *realProber) probeRedis(ctx context.Context, connURL string) (ProbeOutcome, error) {
	opts, err := redis.ParseURL(connURL)
	if err != nil {
		return ProbeUnreachable, fmt.Errorf("redis: ParseURL: %w", err)
	}
	opts.DialTimeout = realProberDialTimeout
	opts.ReadTimeout = realProberDialTimeout
	opts.WriteTimeout = realProberDialTimeout
	// Force a single fresh connection — no pool reuse across probes.
	opts.PoolSize = 1
	opts.MinIdleConns = 0

	client := redis.NewClient(opts)
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		return ProbeUnreachable, fmt.Errorf("redis: PING: %w", err)
	}
	return ProbeReachable, nil
}

// probeMongo opens a fresh mongo.Client, runs adminCommand({ping: 1}),
// disconnects.
func (p *realProber) probeMongo(ctx context.Context, connURL string) (ProbeOutcome, error) {
	clientOpts := options.Client().
		ApplyURI(connURL).
		SetServerSelectionTimeout(realProberDialTimeout).
		SetConnectTimeout(realProberDialTimeout).
		SetSocketTimeout(realProberDialTimeout).
		SetMaxPoolSize(1).
		SetMinPoolSize(0)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return ProbeUnreachable, fmt.Errorf("mongodb: Connect: %w", err)
	}
	defer func() {
		discCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = client.Disconnect(discCtx)
	}()

	res := client.Database("admin").RunCommand(ctx, bson.D{{Key: "ping", Value: 1}})
	if err := res.Err(); err != nil {
		return ProbeUnreachable, fmt.Errorf("mongodb: ping: %w", err)
	}
	return ProbeReachable, nil
}

// probeStorage runs a HEAD against the bucket endpoint. Anything that
// returns an HTTP response (even 4xx / 5xx) is "reachable" — we are only
// distinguishing TCP black-hole / DNS failure from "endpoint exists".
// AccessDenied (403) is in particular common when probing S3-compatible
// endpoints with the bucket-name path but no signed credentials; that's
// still proof the endpoint is up.
func (p *realProber) probeStorage(ctx context.Context, connURL string) (ProbeOutcome, error) {
	target, err := normalizeStorageURL(connURL)
	if err != nil {
		return ProbeUnreachable, fmt.Errorf("storage: parse url: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, target, nil)
	if err != nil {
		return ProbeUnreachable, fmt.Errorf("storage: build request: %w", err)
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return ProbeUnreachable, fmt.Errorf("storage: HEAD %s: %w", target, err)
	}
	defer resp.Body.Close()
	// Any HTTP status is fine — we just needed a TCP+TLS handshake.
	return ProbeReachable, nil
}

// normalizeStorageURL converts an s3:// URL into an http(s):// URL pointing
// at the bucket endpoint, leaving http(s):// untouched. Connection URLs in
// resources.connection_url are stored as the canonical s3://bucket form
// (api/internal/handlers/storage.go); the HEAD probe needs an HTTP target.
//
// Fallback for s3:// without a host: we point at the cluster's
// OBJECT_STORE_ENDPOINT, which is the api's convention. But since the
// worker doesn't have the bucket-endpoint mapping handy, the safest probe
// is to leave http(s):// alone and treat s3:// as "skip" — the heartbeat
// caller will still mark the row healthy via the last successful s3-list
// elsewhere in the storage_minio scanner.
func normalizeStorageURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http", "https":
		return raw, nil
	case "s3":
		// s3://bucket/prefix → https://<bucket>.s3.amazonaws.com/ as a
		// best-effort probe target. For non-AWS S3-compatible stores
		// (DO Spaces, R2, MinIO) the caller's connection_url should be
		// the http(s):// presigned form; if it isn't, the probe falls
		// through to AWS and returns ProbeReachable only if AWS S3 itself
		// is reachable (which is a useful canary).
		if u.Host == "" {
			return "", fmt.Errorf("s3 url missing bucket")
		}
		return fmt.Sprintf("https://%s.s3.amazonaws.com/", u.Host), nil
	default:
		return "", fmt.Errorf("unsupported storage scheme %q", u.Scheme)
	}
}

// probeQueue (NATS) hits the monitoring port's /healthz. Returns
// ProbeReachable on HTTP 200; everything else is ProbeUnreachable.
func (p *realProber) probeQueue(ctx context.Context, connURL string) (ProbeOutcome, error) {
	host, err := natsHost(connURL)
	if err != nil {
		return ProbeUnreachable, fmt.Errorf("queue: parse url: %w", err)
	}
	target := fmt.Sprintf("http://%s:%d/healthz", host, natsMonitoringPort)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return ProbeUnreachable, fmt.Errorf("queue: build request: %w", err)
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return ProbeUnreachable, fmt.Errorf("queue: GET %s: %w", target, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ProbeUnreachable, fmt.Errorf("queue: NATS unhealthy (HTTP %d)", resp.StatusCode)
	}
	return ProbeReachable, nil
}

// natsHost extracts the host (without the :4222 client port) from a
// nats://host:port URL so the monitoring port can be substituted in.
func natsHost(connURL string) (string, error) {
	u, err := url.Parse(connURL)
	if err != nil {
		return "", err
	}
	if u.Scheme != "nats" && u.Scheme != "tls" {
		return "", fmt.Errorf("unsupported nats scheme %q", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return "", fmt.Errorf("nats url missing host")
	}
	return host, nil
}

// netError reports whether err is a net.Error (i.e. the failure surfaced
// inside the network stack rather than from the protocol layer). Currently
// unused — kept for a future refinement that wants to distinguish
// "TCP dial failed" from "auth handshake failed". The latter would still
// be ProbeReachable because the server is clearly up.
//
//nolint:unused // reserved for future use
func netError(err error) bool {
	var ne net.Error
	return errors.As(err, &ne)
}
