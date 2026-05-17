package jobs

// uptime_prober.go — per-minute liveness probe of every public component.
//
// Companion to the api's GET /api/v1/status (W11). The endpoint reads
// uptime_samples; this worker writes them. One probe per minute per
// component is enough to give the dashboard's 96-slot 24h bar a
// well-populated signal (≥1 sample per 15-minute bucket on average).
//
// Components today: api, provisioner, worker, deploys, marketing.
// Each has its own probe function below so we can fail one without
// poisoning the others. Probe failures DO insert a row (healthy=false)
// — that's the whole point of an uptime tracker. Probe-system errors
// (DB write failed, etc.) are logged and the row is skipped.
//
// Why a periodic insert instead of an external prober (pingdom etc.):
//   - We already have a worker fleet and a Postgres for cheap appends.
//   - External probers run from a single region and miss instanode's
//     own-edge failure mode (the one persona-3 caught).
//   - This worker runs INSIDE the cluster, so the worker → API probe
//     is real intra-cluster traffic — the same path an agent uses.
//
// Failure modes:
//   - api probe fails: the worker is itself still up, so the row is
//     written. The status page renders "api degraded/down" and the
//     worker (which we still trust because we just wrote a row from
//     it) gets a healthy=true row of its own.
//   - worker probe: trivially returns healthy (we ARE the worker — if
//     this code is running, the worker is up). Not strictly accurate
//     for the OTHER worker pod in a multi-pod deploy, but the row
//     count over a minute will show parity between pods.
//   - DB unreachable: we can't write the row. Log + skip. Status page
//     will inherit the previous slot value.

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
)

// UptimeProberArgs is the River job payload — no fields.
type UptimeProberArgs struct{}

// Kind is the River worker key.
func (UptimeProberArgs) Kind() string { return "uptime_prober" }

// uptimeProberInterval is the production cadence — once per minute.
// One sample/min × 5 components × 60min × 24h = 7200 rows/day, ~650k
// in the 90-day retention window. Linear, easy to prune.
const uptimeProberInterval = 1 * time.Minute

// uptimeProbeTimeout caps any single probe so the whole tick never
// runs longer than the cadence. 5s matches the resource_heartbeat
// budget — same "fast HTTP/TCP probe" semantics.
const uptimeProbeTimeout = 5 * time.Second

// componentSlug values must match service_components.slug rows seeded
// by migration 035. The worker inserts on the slug column — a typo
// here would foreign-key-violate at insert time, which is the
// intentional canary for a future migration that renames a slug.
const (
	componentAPI         = "api"
	componentProvisioner = "provisioner"
	componentWorker      = "worker"
	componentDeploys     = "deploys"
	componentMarketing   = "marketing"
)

// defaultAPIHealthURL is the production probe target for the api
// component. Overridable via UPTIME_PROBE_API_URL so a dev/staging
// worker probes its own cluster's API rather than prod. The bare
// "/healthz" path is what the api router exposes — we don't need
// /api/v1/status to bootstrap, that path would create a circular
// trust loop (the status page validating itself).
const defaultAPIHealthURL = "https://api.instanode.dev/healthz"

// defaultMarketingURL is the public marketing site. Same override
// convention as defaultAPIHealthURL via UPTIME_PROBE_MARKETING_URL.
const defaultMarketingURL = "https://instanode.dev/"

// defaultProvisionerAddr is the in-cluster gRPC address. Override via
// UPTIME_PROBE_PROVISIONER_ADDR. In dev/CI where the provisioner
// service isn't reachable the worker just records healthy=false
// instead of failing the whole tick.
const defaultProvisionerAddr = "instant-provisioner.instant-infra.svc.cluster.local:50051"

// UptimeProberWorker writes one uptime_samples row per component per
// tick. State is held entirely in DB rows — the worker carries no
// per-pod memory of previous probes (the api computes uptime % from
// the row history).
type UptimeProberWorker struct {
	river.WorkerDefaults[UptimeProberArgs]
	db         *sql.DB
	httpClient *http.Client
	// provisionerDialer is broken out so tests can inject a stub.
	// Production points at a 2-second TCP dial via net.Dialer (we
	// don't need to ride the full gRPC stack to know the listener is
	// alive — a clean TCP handshake is the strict subset of "service
	// is up").
	provisionerDialer func(ctx context.Context, addr string) error
}

// NewUptimeProberWorker constructs the worker. The HTTP client carries
// the per-probe timeout AND a single redirect (so a 30x from the
// marketing site still records healthy). TLS verification stays on —
// a cert misconfiguration is itself a status event we want to surface.
func NewUptimeProberWorker(db *sql.DB) *UptimeProberWorker {
	return &UptimeProberWorker{
		db: db,
		httpClient: &http.Client{
			Timeout: uptimeProbeTimeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
				// Disable connection reuse across ticks — a stale
				// connection to a flapping origin would mask the
				// outage we're trying to detect.
				DisableKeepAlives: true,
			},
		},
		provisionerDialer: defaultProvisionerDialer,
	}
}

// SetUptimeProberDialer overrides the provisioner dialer on an existing
// worker. Exposed as a package-level helper rather than mutating a
// public field so test code stays separated from the production
// construction path — this is the only legitimate call site outside
// the constructor.
func SetUptimeProberDialer(w *UptimeProberWorker, fn func(ctx context.Context, addr string) error) {
	w.provisionerDialer = fn
}

// defaultProvisionerDialer opens a TCP connection to the provisioner
// gRPC address with the per-probe timeout. Returns nil on success;
// any net error means the listener isn't reachable from this pod.
func defaultProvisionerDialer(ctx context.Context, addr string) error {
	d := &net.Dialer{Timeout: uptimeProbeTimeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	_ = conn.Close()
	return nil
}

// Work executes one tick. Probes each component in parallel via simple
// goroutines (5 is tiny — no semaphore needed). Each goroutine writes
// its own row; one slow probe doesn't delay the others.
func (w *UptimeProberWorker) Work(ctx context.Context, _ *river.Job[UptimeProberArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.uptime_prober")
	defer span.End()

	type result struct {
		slug      string
		healthy   bool
		latencyMs *int
	}

	probes := []func(ctx context.Context) result{
		func(ctx context.Context) result { return w.probeAPI(ctx) },
		func(ctx context.Context) result { return w.probeProvisioner(ctx) },
		func(ctx context.Context) result { return w.probeWorker(ctx) },
		func(ctx context.Context) result { return w.probeDeploys(ctx) },
		func(ctx context.Context) result { return w.probeMarketing(ctx) },
	}

	resCh := make(chan result, len(probes))
	for _, fn := range probes {
		fn := fn
		go func() {
			// Panic boundary (P1-B): a panic in a probe would otherwise
			// crash the worker pod AND leave the collector below blocked
			// waiting on resCh. On panic emit a sentinel unhealthy result
			// so every collector iteration still receives a value.
			defer func() {
				if r := recover(); r != nil {
					resCh <- result{slug: "", healthy: false, latencyMs: nil}
					LogRecoveredPanic("uptime_prober.probe", r)
				}
			}()
			resCh <- fn(ctx)
		}()
	}

	// Collect every result. Each probe carries its own deadline so a
	// stuck dial won't hang here past uptimeProbeTimeout.
	for i := 0; i < len(probes); i++ {
		r := <-resCh
		if err := w.insertSample(ctx, r.slug, r.healthy, r.latencyMs); err != nil {
			// Log but continue — losing one row this tick is
			// recoverable; the next tick will write again.
			slog.Warn("jobs.uptime_prober.insert_failed",
				"slug", r.slug, "error", err.Error())
		}
	}
	return nil
}

// insertSample writes one row. latencyMs is optional — nil for failed
// probes where no meaningful RTT was measured.
func (w *UptimeProberWorker) insertSample(ctx context.Context, slug string, healthy bool, latencyMs *int) error {
	var latency sql.NullInt32
	if latencyMs != nil {
		latency = sql.NullInt32{Int32: int32(*latencyMs), Valid: true}
	}
	_, err := w.db.ExecContext(ctx, `
		INSERT INTO uptime_samples(component_slug, sampled_at, healthy, latency_ms)
		VALUES ($1, now(), $2, $3)
	`, slug, healthy, latency)
	return err
}

// probeAPI hits GET /healthz on the configured api URL.
func (w *UptimeProberWorker) probeAPI(ctx context.Context) (r struct {
	slug      string
	healthy   bool
	latencyMs *int
}) {
	r.slug = componentAPI
	url := envOr("UPTIME_PROBE_API_URL", defaultAPIHealthURL)
	healthy, ms := w.httpHEAD(ctx, url, false /* useHead */)
	r.healthy = healthy
	r.latencyMs = ms
	return
}

// probeProvisioner attempts a TCP dial against the provisioner gRPC
// address. Healthy iff the dial succeeds within the budget.
func (w *UptimeProberWorker) probeProvisioner(ctx context.Context) (r struct {
	slug      string
	healthy   bool
	latencyMs *int
}) {
	r.slug = componentProvisioner
	addr := envOr("UPTIME_PROBE_PROVISIONER_ADDR", defaultProvisionerAddr)
	dialCtx, cancel := context.WithTimeout(ctx, uptimeProbeTimeout)
	defer cancel()
	start := time.Now()
	if err := w.provisionerDialer(dialCtx, addr); err != nil {
		r.healthy = false
		return
	}
	ms := int(time.Since(start) / time.Millisecond)
	r.healthy = true
	r.latencyMs = &ms
	return
}

// probeWorker is trivially healthy: this code is running, therefore
// the worker is running. SELECT 1 against the platform DB gives us a
// secondary "I can talk to my own state store" signal — useful
// because a worker that can't write rows is functionally down even
// while its process is alive.
func (w *UptimeProberWorker) probeWorker(ctx context.Context) (r struct {
	slug      string
	healthy   bool
	latencyMs *int
}) {
	r.slug = componentWorker
	dbCtx, cancel := context.WithTimeout(ctx, uptimeProbeTimeout)
	defer cancel()
	start := time.Now()
	var one int
	if err := w.db.QueryRowContext(dbCtx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
		r.healthy = false
		return
	}
	ms := int(time.Since(start) / time.Millisecond)
	r.healthy = true
	r.latencyMs = &ms
	return
}

// probeDeploys probes the wildcard deployment ingress. We HEAD a
// known-public sentinel URL: any 2xx/3xx/4xx response is "TLS terminated,
// ingress is routing" and counts as healthy. Only network errors /
// 5xx count as failure.
func (w *UptimeProberWorker) probeDeploys(ctx context.Context) (r struct {
	slug      string
	healthy   bool
	latencyMs *int
}) {
	r.slug = componentDeploys
	url := envOr("UPTIME_PROBE_DEPLOYS_URL", "https://probe.deployment.instanode.dev/")
	healthy, ms := w.httpHEAD(ctx, url, true /* useHead */)
	r.healthy = healthy
	r.latencyMs = ms
	return
}

// probeMarketing GETs the marketing site root.
func (w *UptimeProberWorker) probeMarketing(ctx context.Context) (r struct {
	slug      string
	healthy   bool
	latencyMs *int
}) {
	r.slug = componentMarketing
	url := envOr("UPTIME_PROBE_MARKETING_URL", defaultMarketingURL)
	healthy, ms := w.httpHEAD(ctx, url, false /* useHead */)
	r.healthy = healthy
	r.latencyMs = ms
	return
}

// httpHEAD issues a single HTTP request and reports (healthy, latencyMs).
// useHead picks the method: HEAD for the deploy ingress (don't want a
// body), GET for /healthz which expects a tiny JSON. A non-nil error
// or a 5xx response is unhealthy; 2xx/3xx/4xx are healthy because the
// ingress / TLS chain answered at all.
func (w *UptimeProberWorker) httpHEAD(ctx context.Context, url string, useHead bool) (bool, *int) {
	method := http.MethodGet
	if useHead {
		method = http.MethodHead
	}
	reqCtx, cancel := context.WithTimeout(ctx, uptimeProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, method, url, nil)
	if err != nil {
		return false, nil
	}
	req.Header.Set("User-Agent", "instanode-uptime-prober/1")
	start := time.Now()
	resp, err := w.httpClient.Do(req)
	ms := int(time.Since(start) / time.Millisecond)
	if err != nil {
		return false, nil
	}
	defer resp.Body.Close()
	// 5xx → unhealthy; everything else → healthy (TLS+routing+upstream
	// returned a response). 4xx is healthy because the dispatcher /
	// ingress is functional even if the route doesn't exist on the
	// probed sentinel hostname.
	if resp.StatusCode >= 500 {
		return false, &ms
	}
	return true, &ms
}

// envOr returns the env var value if set, else fallback. Trims
// whitespace so a stray newline in a k8s ConfigMap doesn't break the
// URL parser.
func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// UptimeRetentionArgs is the River job payload for the daily prune.
type UptimeRetentionArgs struct{}

// Kind is the River worker key.
func (UptimeRetentionArgs) Kind() string { return "uptime_retention" }

// uptimeRetentionDays is how long we keep uptime_samples. 90d matches
// the longest window the api computes ("uptime_30d_pct" + a buffer for
// the next window's prior-month read). Older rows have no consumer.
const uptimeRetentionDays = 90

// UptimeRetentionWorker prunes uptime_samples older than 90 days. Runs
// daily — the table grows ~7200 rows/day so the prune is cheap.
type UptimeRetentionWorker struct {
	river.WorkerDefaults[UptimeRetentionArgs]
	db *sql.DB
}

// NewUptimeRetentionWorker constructs the prune worker.
func NewUptimeRetentionWorker(db *sql.DB) *UptimeRetentionWorker {
	return &UptimeRetentionWorker{db: db}
}

// Work executes one prune. DELETE WHERE sampled_at < now() - INTERVAL
// '90 days'. Uses the (component_slug, sampled_at DESC) index so the
// table scan stays bounded.
func (w *UptimeRetentionWorker) Work(ctx context.Context, _ *river.Job[UptimeRetentionArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.uptime_retention")
	defer span.End()

	res, err := w.db.ExecContext(ctx, `
		DELETE FROM uptime_samples
		WHERE sampled_at < now() - INTERVAL '`+strconv.Itoa(uptimeRetentionDays)+` days'
	`)
	if err != nil {
		return fmt.Errorf("UptimeRetentionWorker: delete failed: %w", err)
	}
	n, _ := res.RowsAffected()
	slog.Info("jobs.uptime_retention.swept", "deleted_rows", n)
	return nil
}
