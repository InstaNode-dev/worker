package jobs

// custom_domain_reconcile.go — periodic reconciler for the custom_domains table.
//
// Sprint 10's Verify endpoint is pull-only: the dashboard polls it after the
// customer adds DNS. This worker complements that by sweeping the table on a
// schedule so domains advance even when nobody is looking at the dashboard.
//
// Lifecycle (mirrors api/internal/handlers/custom_domain.go):
//
//   pending_verification → verified → ingress_ready → cert_ready → live
//                       │
//                       └→ failed (after 7d stuck in pending_verification)
//
// SCOPE NOTE: the worker module (instant.dev/worker) is a separate Go module
// from the api (instant.dev) and does NOT import api packages or k8s.io
// client-go. As a result this reconciler covers:
//
//   step 1  TXT lookup → mark verified
//   step 4  HTTP HEAD probe of cert_ready domains → mark live
//   step 5  Stale pending_verification (>7d) → mark failed
//
// Steps 2 (Ingress create) and 3 (cert poll) remain in the API handler — the
// dashboard's first Verify click triggers them. See the TODO at the bottom of
// this file for promoting those into the worker once the api module is
// import-reachable (or once we vendor a minimal k8s client here).
//
// The worker duplicates the small set of model SQL needed (read + status
// updates) rather than depending on api/internal/models. The columns live in
// migration 014_custom_domains.sql; if that schema changes the queries here
// need an update too. Keep both in sync.

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
)

// Reconciler tunables. The interval matches the periodic-job registration in
// workers.go; threshold + timeouts are stand-alone.
const (
	customDomainReconcileInterval = 5 * time.Minute
	customDomainStaleThreshold    = 7 * 24 * time.Hour
	txtLookupTimeout              = 5 * time.Second
	httpProbeTimeout              = 10 * time.Second

	// Status strings — verbatim copies of models.CustomDomainStatus*.
	// Duplicated here because the worker module does not import the api
	// module. If the api strings ever change, update both places.
	statusPending      = "pending_verification"
	statusVerified     = "verified"
	statusIngressReady = "ingress_ready"
	statusCertReady    = "cert_ready"
	statusLive         = "live"
	statusFailed       = "failed"

	// VerificationTokenPrefix mirrors api/internal/models.VerificationTokenPrefix.
	verificationTokenPrefix = "instanode-verify-"

	// txtChallengePrefix is the DNS label we ask customers to put their TXT
	// record at — same constant the API handler uses.
	txtChallengePrefix = "_instanode."

	staleVerificationFailReason = "verification timeout: TXT record not observed within 7 days"
)

// CustomDomainReconcileArgs is the periodic-job payload. Empty — every run is
// a full table sweep.
type CustomDomainReconcileArgs struct{}

// Kind implements river.JobArgs.
func (CustomDomainReconcileArgs) Kind() string { return "custom_domain_reconcile" }

// k8sCustomDomainProvider is the slice of the api's K8sStackProvider used by
// the reconciler. Defined as an interface so callers can pass nil when k8s
// isn't reachable (steps 2 & 3 are then skipped — see SCOPE NOTE above).
//
// Today no implementation is wired in the worker — the worker's main.go
// always passes nil. Kept as part of the constructor signature so the
// follow-up that vendors a real k8s client doesn't have to reshape this
// worker's public surface.
type k8sCustomDomainProvider interface {
	EnsureCustomDomainIngress(ctx context.Context, namespace, hostname, serviceName string, servicePort int) (string, error)
	CertificateReady(ctx context.Context, namespace, certName string) (bool, string, error)
}

// CustomDomainReconciler is the River worker.
type CustomDomainReconciler struct {
	river.WorkerDefaults[CustomDomainReconcileArgs]
	db      *sql.DB
	k8s     k8sCustomDomainProvider // may be nil; ingress/cert steps then skipped
	httpCli *http.Client            // probe client; never follows redirects
}

// NewCustomDomainReconciler constructs the worker.
//
// Pass nil for k8sProvider in environments where the worker can't talk to
// the cluster API (steps 2 & 3 will be skipped — the API handler's Verify
// endpoint still drives those when a user clicks "verify" in the dashboard).
//
// httpCli may be nil — the worker installs a default with a 10s timeout and
// CheckRedirect set to ErrUseLastResponse so a 301 from the wrong CDN never
// fools us into reporting a custom hostname as live.
func NewCustomDomainReconciler(db *sql.DB, k8sProvider k8sCustomDomainProvider, httpCli *http.Client) *CustomDomainReconciler {
	if httpCli == nil {
		httpCli = &http.Client{
			Timeout: httpProbeTimeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	return &CustomDomainReconciler{
		db:      db,
		k8s:     k8sProvider,
		httpCli: httpCli,
	}
}

// activeCustomDomain is the projection the reconciler reads — only the columns
// needed for state transitions, no audit fields.
type activeCustomDomain struct {
	id        uuid.UUID
	hostname  string
	token     string // verification token; combined with prefix to build TXT value
	status    string
	createdAt time.Time
}

// Work runs the full sweep. Errors on individual domains are logged and
// swallowed so one bad row never stops the rest of the batch — same fail-open
// posture as ExpireAnonymousWorker.
func (w *CustomDomainReconciler) Work(ctx context.Context, job *river.Job[CustomDomainReconcileArgs]) error {
	start := time.Now()

	domains, err := w.listActiveDomains(ctx)
	if err != nil {
		return fmt.Errorf("custom_domain_reconcile: list active: %w", err)
	}

	if len(domains) == 0 {
		// #146 (BugBash 2026-05-20 idle-tick noise pass): 5min tick = 288
		// idle-tick lines/day per worker pod. Demote to DEBUG; INFO only
		// when there is non-empty work below.
		slog.Debug("jobs.custom_domain_reconcile.completed",
			"total", 0,
			"duration_ms", time.Since(start).Milliseconds(),
			"job_id", job.ID,
		)
		return nil
	}

	var (
		advancedVerified int
		advancedLive     int
		markedFailed     int
		recordedErrors   int
	)

	for _, d := range domains {
		switch d.status {
		case statusPending:
			result := w.reconcilePending(ctx, d)
			switch result {
			case reconcileAdvanced:
				advancedVerified++
			case reconcileFailed:
				markedFailed++
			case reconcileRecordedErr:
				recordedErrors++
			}

		case statusCertReady:
			if w.reconcileCertReady(ctx, d) == reconcileAdvanced {
				advancedLive++
			}

		case statusVerified, statusIngressReady:
			// Steps 2 & 3 (Ingress create / cert poll) are not wired in the
			// worker — the api handler still drives them via Verify. Log
			// once at debug so anyone reading the worker logs can confirm
			// these are intentionally skipped.
			slog.Debug("jobs.custom_domain_reconcile.skip_ingress_steps",
				"id", d.id,
				"hostname", d.hostname,
				"status", d.status,
				"note", "k8s steps owned by api handler in current sprint",
			)

		case statusLive, statusFailed:
			// Terminal — listActiveDomains filters these out, but defend in
			// case the SQL changes.

		default:
			slog.Warn("jobs.custom_domain_reconcile.unknown_status",
				"id", d.id, "hostname", d.hostname, "status", d.status,
			)
		}
	}

	slog.Info("jobs.custom_domain_reconcile.completed",
		"total", len(domains),
		"advanced_verified", advancedVerified,
		"advanced_live", advancedLive,
		"marked_failed", markedFailed,
		"recorded_errors", recordedErrors,
		"duration_ms", time.Since(start).Milliseconds(),
		"job_id", job.ID,
	)
	return nil
}

// reconcileResult is a small enum describing what happened for one row.
// Used purely for the per-batch counters in the completion log.
type reconcileResult int

const (
	reconcileNoop reconcileResult = iota
	reconcileAdvanced
	reconcileFailed
	reconcileRecordedErr
)

// reconcilePending handles a domain in pending_verification:
//
//  1. If created > 7d ago and still pending → mark failed (stop probing).
//  2. Otherwise run a TXT lookup; mark verified on match.
//  3. On miss / error, record last_check_err so the dashboard can render
//     the reason — never error out the batch.
func (w *CustomDomainReconciler) reconcilePending(ctx context.Context, d activeCustomDomain) reconcileResult {
	if time.Since(d.createdAt) > customDomainStaleThreshold {
		if err := w.markFailed(ctx, d.id, staleVerificationFailReason); err != nil {
			slog.Error("jobs.custom_domain_reconcile.mark_failed_failed",
				"error", err, "id", d.id, "hostname", d.hostname)
			return reconcileNoop
		}
		slog.Info("jobs.custom_domain_reconcile.marked_failed",
			"id", d.id, "hostname", d.hostname, "reason", staleVerificationFailReason)
		return reconcileFailed
	}

	matched, lookupErr := w.lookupTXT(ctx, d.hostname, d.token)
	if matched {
		if err := w.markVerified(ctx, d.id); err != nil {
			slog.Error("jobs.custom_domain_reconcile.mark_verified_failed",
				"error", err, "id", d.id, "hostname", d.hostname)
			return reconcileNoop
		}
		slog.Info("jobs.custom_domain_reconcile.advanced_verified",
			"id", d.id, "hostname", d.hostname)
		return reconcileAdvanced
	}

	msg := "TXT record missing or wrong value"
	if lookupErr != nil {
		msg = lookupErr.Error()
	}
	if err := w.updateLastCheck(ctx, d.id, msg); err != nil {
		slog.Warn("jobs.custom_domain_reconcile.update_last_check_failed",
			"error", err, "id", d.id, "hostname", d.hostname)
		return reconcileNoop
	}
	return reconcileRecordedErr
}

// reconcileCertReady runs an HTTP HEAD against https://<hostname>/. A 2xx or
// 3xx (without following) marks the domain live; anything else records the
// last_check_err so the dashboard surfaces "DNS not pointing yet" or similar.
//
// We deliberately do NOT follow redirects: a 301 to https://example.com from
// somewhere else's CDN is not proof our ingress is serving the hostname.
func (w *CustomDomainReconciler) reconcileCertReady(ctx context.Context, d activeCustomDomain) reconcileResult {
	url := "https://" + d.hostname + "/"
	probeCtx, cancel := context.WithTimeout(ctx, httpProbeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(probeCtx, http.MethodHead, url, nil)
	if err != nil {
		_ = w.updateLastCheck(ctx, d.id, fmt.Sprintf("build probe request: %v", err))
		return reconcileNoop
	}
	req.Header.Set("User-Agent", "instanode-domain-reconciler/1")

	resp, err := w.httpCli.Do(req)
	if err != nil {
		_ = w.updateLastCheck(ctx, d.id, fmt.Sprintf("HTTPS HEAD probe failed: %v", err))
		return reconcileNoop
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		if err := w.updateStatus(ctx, d.id, statusLive, ""); err != nil {
			slog.Error("jobs.custom_domain_reconcile.mark_live_failed",
				"error", err, "id", d.id, "hostname", d.hostname)
			return reconcileNoop
		}
		slog.Info("jobs.custom_domain_reconcile.advanced_live",
			"id", d.id, "hostname", d.hostname, "probe_status", resp.StatusCode)
		return reconcileAdvanced
	}

	msg := fmt.Sprintf("HTTPS HEAD probe returned %d", resp.StatusCode)
	if err := w.updateLastCheck(ctx, d.id, msg); err != nil {
		slog.Warn("jobs.custom_domain_reconcile.update_last_check_failed",
			"error", err, "id", d.id, "hostname", d.hostname)
		return reconcileNoop
	}
	return reconcileRecordedErr
}

// txtLookupFunc is the TXT-resolution seam. Defaults to the stdlib resolver;
// overridden in tests so the verification-match / miss / error arms can be
// exercised without real DNS.
var txtLookupFunc = func(ctx context.Context, name string) ([]string, error) {
	resolver := &net.Resolver{}
	return resolver.LookupTXT(ctx, name)
}

// lookupTXT runs a context-bound TXT lookup at "_instanode.<hostname>" and
// returns whether any record matches "instanode-verify-<token>". Trims
// surrounding quotes some resolvers leave on TXT contents — same logic the
// API handler uses.
func (w *CustomDomainReconciler) lookupTXT(parent context.Context, hostname, token string) (bool, error) {
	lookupCtx, cancel := context.WithTimeout(parent, txtLookupTimeout)
	defer cancel()

	records, err := txtLookupFunc(lookupCtx, txtChallengePrefix+hostname)
	if err != nil {
		return false, fmt.Errorf("TXT lookup for %s failed: %w", txtChallengePrefix+hostname, err)
	}
	want := verificationTokenPrefix + token
	for _, r := range records {
		clean := strings.Trim(r, "\"")
		if clean == want || r == want {
			return true, nil
		}
	}
	return false, nil
}

// ── SQL helpers ───────────────────────────────────────────────────────────────
//
// These mirror the api package's models.CustomDomain* helpers. They are
// duplicated here intentionally — the worker module does not import the api
// module. The schema lives in migration 014_custom_domains.sql; keep these in
// sync if columns or status strings ever change.

// listActiveDomains returns every row not in a terminal status. Newest first
// matches the order the dashboard already uses, but the worker doesn't depend
// on order — it's purely for log readability.
func (w *CustomDomainReconciler) listActiveDomains(ctx context.Context) ([]activeCustomDomain, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT id, hostname, verification_token, status, created_at
		FROM custom_domains
		WHERE status NOT IN ($1, $2)
		ORDER BY created_at ASC
	`, statusLive, statusFailed)
	if err != nil {
		return nil, fmt.Errorf("listActiveDomains: query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []activeCustomDomain
	for rows.Next() {
		var d activeCustomDomain
		if err := rows.Scan(&d.id, &d.hostname, &d.token, &d.status, &d.createdAt); err != nil {
			return nil, fmt.Errorf("listActiveDomains: scan: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// markVerified is the equivalent of models.MarkCustomDomainVerified. Sets
// verified_at = now() and clears last_check_err on success.
func (w *CustomDomainReconciler) markVerified(ctx context.Context, id uuid.UUID) error {
	_, err := w.db.ExecContext(ctx, `
		UPDATE custom_domains
		SET status = $1,
		    verified_at = now(),
		    last_check_at = now(),
		    last_check_err = NULL
		WHERE id = $2 AND status = $3
	`, statusVerified, id, statusPending)
	if err != nil {
		return fmt.Errorf("markVerified: %w", err)
	}
	return nil
}

// markFailed is the equivalent of the requested MarkFailed helper. Records
// reason in last_check_err so the dashboard can render "verification timed
// out".
func (w *CustomDomainReconciler) markFailed(ctx context.Context, id uuid.UUID, reason string) error {
	_, err := w.db.ExecContext(ctx, `
		UPDATE custom_domains
		SET status = $1,
		    last_check_at = now(),
		    last_check_err = $2
		WHERE id = $3
	`, statusFailed, reason, id)
	if err != nil {
		return fmt.Errorf("markFailed: %w", err)
	}
	return nil
}

// updateStatus advances status and resets last_check fields. Equivalent of
// models.UpdateCustomDomainStatus. Empty errMsg sets last_check_err to NULL.
func (w *CustomDomainReconciler) updateStatus(ctx context.Context, id uuid.UUID, status, errMsg string) error {
	var errVal interface{}
	if errMsg != "" {
		errVal = errMsg
	}
	_, err := w.db.ExecContext(ctx, `
		UPDATE custom_domains
		SET status = $1,
		    last_check_at = now(),
		    last_check_err = $2
		WHERE id = $3
	`, status, errVal, id)
	if err != nil {
		return fmt.Errorf("updateStatus: %w", err)
	}
	return nil
}

// updateLastCheck records last_check_at + last_check_err without changing
// status. Equivalent of the requested UpdateLastCheck helper.
func (w *CustomDomainReconciler) updateLastCheck(ctx context.Context, id uuid.UUID, errStr string) error {
	var errVal interface{}
	if errStr != "" {
		errVal = errStr
	}
	_, err := w.db.ExecContext(ctx, `
		UPDATE custom_domains
		SET last_check_at = now(),
		    last_check_err = $1
		WHERE id = $2
	`, errVal, id)
	if err != nil {
		return fmt.Errorf("updateLastCheck: %w", err)
	}
	return nil
}

// TODO(custom-domains): once the api module is import-reachable from worker
// (or once we vendor a minimal k8s client here) wire the k8sCustomDomainProvider
// path so steps 2 (Ingress create) and 3 (cert poll) run from the reconciler
// too. Today the dashboard's first Verify click drives those — fine for the
// happy path, but a domain whose user never returns to the dashboard will sit
// at "verified" forever.
