package jobs

// payment_grace_terminator.go — periodic job that walks the
// payment_grace_periods table looking for teams whose grace expired
// (expires_at < now()) and calls the api repo's
// POST /internal/teams/:id/terminate endpoint to (a) pause all resources,
// (b) downgrade tier to anonymous, (c) mark the dunning row terminated.
//
// Cadence: every 1h (per the brief). The 1h floor is fine because the
// grace clock is 7 days; a customer who recovers in the final hour
// transitions out of 'active' on their own webhook path before the next
// terminator tick.
//
// Why the destructive work lives on the api side, not here:
//   1. Resource pause needs the provisioner gRPC client + the API's
//      shared idempotency primitives (resource.status state machine).
//      Duplicating those here would re-implement half the api models
//      package.
//   2. The api's /internal/teams/:id/terminate endpoint is the single
//      audited write path — operators can drive the same destructive
//      flow from runbooks via curl. Having two implementations
//      (worker direct DB + api endpoint) would risk drift.
//
// JWT signing: the api expects a Bearer token signed with the shared
// WORKER_INTERNAL_JWT_SECRET. The worker reads the secret from its
// env. If the secret is absent we log a WARN per tick and short-circuit
// without action — better than booting the worker with no secret and
// later writing audit_log rows for non-terminations.
//
// TODO(ops): WORKER_INTERNAL_JWT_SECRET is a new env var. Operator must
// (1) generate a 32-byte random value, (2) write it into the worker's
// secrets, (3) write the same value into the api's secrets under
// WORKER_INTERNAL_JWT_SECRET so the api can verify the bearer token,
// (4) the api must expose POST /internal/teams/:id/terminate that
// accepts the bearer + does pause/downgrade/mark-terminated. The
// endpoint is referenced by this dispatcher but NOT yet implemented in
// the api repo — that side ships in a separate PR.

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
	"instant.dev/worker/internal/apiclient"
	"instant.dev/worker/internal/circuit"
)

// PaymentGraceTerminatorArgs is the River job payload — no fields.
type PaymentGraceTerminatorArgs struct{}

// Kind is the River worker key.
func (PaymentGraceTerminatorArgs) Kind() string { return "payment_grace_terminator" }

// paymentGraceTerminatorInterval is the dispatch cadence. 1h per the brief.
const paymentGraceTerminatorInterval = 1 * time.Hour

// paymentGraceTerminatorBatchLimit caps per-tick fan-out. Termination
// triggers a synchronous api call + provisioner cascade; we cap to keep
// a single tick under a sensible wall-clock so the next tick still has
// time to land if a backlog appears.
const paymentGraceTerminatorBatchLimit = 100

// paymentGraceTerminatorHTTPTimeout caps each api call. 30s is generous —
// the api endpoint walks all resources for the team and the worst case
// is a team with dozens of databases each requiring a provisioner RPC.
const paymentGraceTerminatorHTTPTimeout = 30 * time.Second

// auditKindPaymentGraceTerminated is the audit_log.kind value this job
// writes after a successful termination. Matches
// api/internal/models.AuditKindPaymentGraceTerminated.
const auditKindPaymentGraceTerminated = "payment.grace_terminated"

// paymentGraceTerminatorActor — system-actor convention.
const paymentGraceTerminatorActor = "system"

// PaymentGraceTerminatorWorker drives the terminator flow.
//
// The httpCli field is wrapped in apiclient.Client when the worker is
// constructed via NewPaymentGraceTerminatorWorker — every call to the
// api goes through that wrapper's circuit breaker. If you bypass the
// constructor in tests, the wrapped client falls back to the raw
// http.Client; the breaker is only attached via the apiCli field.
type PaymentGraceTerminatorWorker struct {
	river.WorkerDefaults[PaymentGraceTerminatorArgs]
	db        *sql.DB
	httpCli   *http.Client      // raw client retained for tests / back-compat
	apiCli    *apiclient.Client // circuit-breaker-wrapped wrapper, used by terminate()
	apiBase   string            // e.g. http://instant-api.instant.svc.cluster.local:8080
	jwtSecret string            // shared with the api side under WORKER_INTERNAL_JWT_SECRET
}

// NewPaymentGraceTerminatorWorker constructs the worker. apiBase and
// jwtSecret may be empty — the worker logs a WARN per tick and
// short-circuits with no DB writes in that case, so the worker boots
// cleanly even on a cluster that hasn't wired the env vars yet.
func NewPaymentGraceTerminatorWorker(db *sql.DB, apiBase, jwtSecret string, httpCli *http.Client) *PaymentGraceTerminatorWorker {
	if httpCli == nil {
		httpCli = &http.Client{Timeout: paymentGraceTerminatorHTTPTimeout}
	}
	return &PaymentGraceTerminatorWorker{
		db:        db,
		httpCli:   httpCli,
		apiCli:    apiclient.New(httpCli),
		apiBase:   strings.TrimRight(apiBase, "/"),
		jwtSecret: jwtSecret,
	}
}

// paymentGraceTerminatorRow is the projection the worker reads.
type paymentGraceTerminatorRow struct {
	id        uuid.UUID
	teamID    uuid.UUID
	expiresAt time.Time
}

// Work runs one sweep.
func (w *PaymentGraceTerminatorWorker) Work(ctx context.Context, job *river.Job[PaymentGraceTerminatorArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.payment_grace_terminator")
	defer span.End()

	start := time.Now()
	if w.apiBase == "" || w.jwtSecret == "" {
		// Don't error — that would make River retry forever. Log loudly
		// so the dashboard panel surfaces the misconfig.
		slog.Warn("jobs.payment_grace_terminator.misconfigured",
			"api_base_set", w.apiBase != "",
			"jwt_secret_set", w.jwtSecret != "",
			"note", "set INSTANT_API_INTERNAL_URL + WORKER_INTERNAL_JWT_SECRET to enable",
		)
		return nil
	}

	now := time.Now().UTC()

	// Candidate query: active grace rows whose clock has expired.
	rows, err := w.db.QueryContext(ctx, `
		SELECT id, team_id, expires_at
		FROM payment_grace_periods
		WHERE status = 'active'
		  AND expires_at < $1
		ORDER BY expires_at ASC
		LIMIT $2
	`, now, paymentGraceTerminatorBatchLimit)
	if err != nil {
		return fmt.Errorf("PaymentGraceTerminatorWorker: query failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var candidates []paymentGraceTerminatorRow
	for rows.Next() {
		var r paymentGraceTerminatorRow
		if scanErr := rows.Scan(&r.id, &r.teamID, &r.expiresAt); scanErr != nil {
			slog.Warn("jobs.payment_grace_terminator.scan_failed", "error", scanErr)
			continue
		}
		candidates = append(candidates, r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("PaymentGraceTerminatorWorker: rows error: %w", err)
	}
	_ = rows.Close()

	if len(candidates) == 0 {
		// P1-1 (BugBash 2026-05-19): idle tick — demoted INFO → DEBUG.
		// Liveness via jobs.middleware.work_ok; INFO only for a tick that
		// actually terminated a grace period.
		slog.Debug("jobs.payment_grace_terminator.completed",
			"terminated", 0,
			"candidates", 0,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return nil
	}

	var terminated, skipped int
	for _, r := range candidates {
		if termErr := w.terminate(ctx, r.teamID); termErr != nil {
			slog.Error("jobs.payment_grace_terminator.api_call_failed",
				"grace_id", r.id.String(),
				"team_id", r.teamID.String(),
				"error", termErr,
			)
			skipped++
			continue
		}
		// Audit emit happens AFTER the api confirms termination. The
		// api endpoint is responsible for the destructive work + the
		// payment_grace_periods row flip to status='terminated'; we
		// only mirror the lifecycle event into audit_log so the email
		// forwarder picks it up.
		summary := "payment grace expired — team terminated"
		meta := map[string]any{
			"grace_id":      r.id.String(),
			"grace_ends_at": r.expiresAt.UTC().Format(time.RFC3339),
		}
		metaBytes, mErr := json.Marshal(meta)
		if mErr != nil {
			slog.Error("jobs.payment_grace_terminator.metadata_marshal_failed",
				"grace_id", r.id.String(),
				"error", mErr,
			)
			skipped++
			continue
		}
		if _, insErr := w.db.ExecContext(ctx, `
			INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
			VALUES ($1, $2, $3, $4, $5)
		`, r.teamID, paymentGraceTerminatorActor, auditKindPaymentGraceTerminated, summary, metaBytes); insErr != nil {
			slog.Error("jobs.payment_grace_terminator.audit_insert_failed",
				"grace_id", r.id.String(),
				"team_id", r.teamID.String(),
				"error", insErr,
			)
			// The api side already did the destructive work; we just
			// failed to mirror the event. Operator can backfill via
			// the audit_log table directly if needed.
			skipped++
			continue
		}
		terminated++
		slog.Info("jobs.payment_grace_terminator.terminated",
			"grace_id", r.id.String(),
			"team_id", r.teamID.String(),
		)
	}

	slog.Info("jobs.payment_grace_terminator.completed",
		"terminated", terminated,
		"skipped", skipped,
		"candidates", len(candidates),
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return nil
}

// terminate POSTs to the api's /internal/teams/:id/terminate endpoint
// with a worker-signed bearer token. Returns nil on a 2xx response.
//
// We deliberately do NOT retry here: a 5xx from the api means the api
// is unhealthy, and the next 1h sweep tick will pick up the unterminated
// row naturally. A retry loop here would just stack up multiple failed
// attempts inside one River job.
func (w *PaymentGraceTerminatorWorker) terminate(ctx context.Context, teamID uuid.UUID) error {
	url := fmt.Sprintf("%s/internal/teams/%s/terminate", w.apiBase, teamID.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader([]byte("{}")))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	tok, tokErr := signWorkerInternalJWT(w.jwtSecret, teamID.String())
	if tokErr != nil {
		return fmt.Errorf("sign jwt: %w", tokErr)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "instanode-worker/grace-terminator")

	// Use the breaker-wrapped client. When the api is hosed and the
	// breaker is open, this returns circuit.ErrOpen immediately —
	// the caller's "log and continue" branch (next periodic tick will
	// re-process the row) takes over instead of burning the request
	// timeout per candidate.
	cli := w.apiCli
	if cli == nil {
		// Belt-and-braces: tests that construct the worker as a
		// literal struct (no constructor) still get raw http.Do.
		cli = apiclient.New(w.httpCli)
		w.apiCli = cli
	}
	resp, doErr := cli.Do(req)
	if doErr != nil {
		if errors.Is(doErr, circuit.ErrOpen) {
			slog.Warn("payment_grace_terminator.api_circuit_open",
				"team_id", teamID,
				"note", "leaving row 'active'; next tick will retry",
			)
			return doErr
		}
		return fmt.Errorf("api request: %w", doErr)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("api status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// signWorkerInternalJWT mints a short-lived HS256 JWT the api can
// verify against the shared WORKER_INTERNAL_JWT_SECRET. We hand-roll
// the JWT (header + claims + signature) so the worker doesn't add a
// dependency on a JWT library for this one path. The api side will
// use whatever JWT lib it already imports — both ends agree on the
// HS256 algorithm and the claim set below.
//
// Claims:
//   sub  — the team id being terminated (allows the api to assert the
//          path :id matches the JWT subject; prevents a stolen token
//          from terminating a different team)
//   iss  — "instanode-worker" so the api can route on issuer
//   iat  — issued-at (UTC seconds)
//   exp  — issued-at + 5 minutes (short window — single-shot use)
//   aud  — "internal-teams-terminate" so the same secret can't be
//          re-used by an attacker against a different internal route
func signWorkerInternalJWT(secret, teamID string) (string, error) {
	if secret == "" {
		return "", errors.New("empty secret")
	}
	header := base64URLEncode([]byte(`{"alg":"HS256","typ":"JWT"}`))
	now := time.Now().UTC().Unix()
	claims := map[string]any{
		"sub": teamID,
		"iss": "instanode-worker",
		"aud": "internal-teams-terminate",
		"iat": now,
		"exp": now + 5*60,
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("marshal claims: %w", err)
	}
	body := header + "." + base64URLEncode(claimsJSON)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	sig := base64URLEncode(mac.Sum(nil))
	return body + "." + sig, nil
}

// base64URLEncode is the unpadded URL-safe base64 encoding JWTs use.
func base64URLEncode(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}
