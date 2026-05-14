package jobs

// magic_link_reconciler.go — periodic job that drains the magic_links
// reconciliation queue (rows stuck at email_send_status IN ('pending',
// 'send_failed') inside the 15-minute TTL window).
//
// Live as of the 2026-05-14 RESEND_API_KEY=CHANGE_ME outage post-mortem:
// the api's POST /auth/email/start used to log "magic_link.start.sent"
// unconditionally, so NR saw every request as a successful send while no
// emails were actually going out. cd51ca7 fixed the log; this reconciler
// closes the loop by re-driving any row whose first send attempt failed.
//
// Cadence: every 60s. With a 15-minute TTL on magic_links the reconciler
// has roughly 14 windows to retry a single row; the 3-attempt cap in the
// api's resend handler bounds work-per-row so a permanently-degraded
// provider doesn't pile up.
//
// SCOPE NOTE: the worker module (instant.dev/worker) is a separate Go
// module from the api (instant.dev) so we don't import api/models. The
// reconciler reads the magic_links table directly via the duplicated
// shape below and writes status updates only via the api's
// POST /internal/email/resend-magic-link endpoint — the api owns the
// write path for email_send_status. We never UPDATE magic_links from
// here.

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
)

// magicLinkReconcilerInterval is the dispatch cadence. 60s per the brief
// is a sweet spot:
//   - short enough that a user whose first attempt failed gets the email
//     while they're still on the login page;
//   - long enough that a global provider outage doesn't generate a
//     per-second flood of retries.
const magicLinkReconcilerInterval = 60 * time.Second

// magicLinkReconcilerBatchLimit caps per-tick fan-out. 50 rows is enough
// to drain a moderate outage (typical magic-link volume is small) while
// keeping a single tick under a sensible wall-clock budget. The 3-attempt
// cap inside the api handler bounds per-row work.
const magicLinkReconcilerBatchLimit = 50

// magicLinkReconcilerTTL mirrors the api's magicLinkTTL constant. A row
// older than this is past consumption-eligibility regardless of send
// status — the consumer path rejects it — so the reconciler should not
// re-drive it. Duplicated rather than imported (worker module is
// independent of api); keep in sync if the api's TTL ever changes.
const magicLinkReconcilerTTL = 15 * time.Minute

// magicLinkResendHTTPTimeout is the per-resend HTTP timeout. The api
// endpoint synchronously dispatches the email send (which itself has a
// network call to Resend/Brevo), so 30s is generous.
const magicLinkResendHTTPTimeout = 30 * time.Second

// magicLinkReconcilerPurpose is the JWT `purpose` claim the api accepts
// on POST /internal/email/resend-magic-link. Distinct from the
// internal-terminate purpose so a captured terminate token can't be
// replayed to drive resends.
const magicLinkReconcilerPurpose = "resend_magic_link"

// MagicLinkReconcilerArgs is the River job payload. No fields — every run
// is a full table sweep within the TTL window.
type MagicLinkReconcilerArgs struct{}

// Kind is the River worker key.
func (MagicLinkReconcilerArgs) Kind() string { return "magic_link_reconciler" }

// MagicLinkReconcilerWorker is the River worker. apiBase and jwtSecret
// may be empty — the worker logs a WARN per tick and short-circuits with
// no DB or HTTP traffic in that case, so the worker boots cleanly on a
// cluster that hasn't wired the env vars yet (same fail-open posture as
// PaymentGraceTerminatorWorker).
type MagicLinkReconcilerWorker struct {
	river.WorkerDefaults[MagicLinkReconcilerArgs]
	db        *sql.DB
	httpCli   *http.Client
	apiBase   string
	jwtSecret string
}

// NewMagicLinkReconcilerWorker constructs the worker. httpCli may be nil
// — we install a default with the per-call timeout. The api base URL and
// JWT secret are passed in so main.go can wire them from config without
// importing config-shaped types here.
func NewMagicLinkReconcilerWorker(db *sql.DB, apiBase, jwtSecret string, httpCli *http.Client) *MagicLinkReconcilerWorker {
	if httpCli == nil {
		httpCli = &http.Client{Timeout: magicLinkResendHTTPTimeout}
	}
	return &MagicLinkReconcilerWorker{
		db:        db,
		httpCli:   httpCli,
		apiBase:   strings.TrimRight(apiBase, "/"),
		jwtSecret: jwtSecret,
	}
}

// magicLinkReconcileRow is the projection the worker reads from
// magic_links. Mirrors api/internal/models.MagicLinkReconcileRow shape;
// duplicated here because the worker module doesn't import api.
type magicLinkReconcileRow struct {
	id              uuid.UUID
	email           string
	emailSendStatus string
	attempts        int
	createdAt       time.Time
}

// Work runs one sweep.
//
// Returns nil on every "expected" outcome (misconfig, empty batch, api
// 503) so River doesn't retry the periodic job and pile work onto the
// next 60s tick. Only unexpected DB errors propagate (a transient query
// failure is fine to retry, but the next periodic tick handles that
// anyway — explicit error here just lets River surface the failure in
// its tracing).
func (w *MagicLinkReconcilerWorker) Work(ctx context.Context, job *river.Job[MagicLinkReconcilerArgs]) error {
	start := time.Now()

	if w.apiBase == "" || w.jwtSecret == "" {
		slog.Warn("jobs.magic_link_reconciler.misconfigured",
			"api_base_set", w.apiBase != "",
			"jwt_secret_set", w.jwtSecret != "",
			"note", "set INSTANT_API_INTERNAL_URL + WORKER_INTERNAL_JWT_SECRET to enable",
		)
		return nil
	}

	candidates, err := w.listReconcileCandidates(ctx)
	if err != nil {
		return fmt.Errorf("magic_link_reconciler: list candidates: %w", err)
	}

	if len(candidates) == 0 {
		slog.Info("jobs.magic_link_reconciler.completed",
			"candidates", 0,
			"resent", 0,
			"abandoned", 0,
			"skipped", 0,
			"duration_ms", time.Since(start).Milliseconds(),
			"job_id", job.ID,
		)
		return nil
	}

	var resent, abandoned, skipped int
	for _, r := range candidates {
		outcome := w.driveResend(ctx, r)
		switch outcome {
		case reconcileOutcomeResent:
			resent++
		case reconcileOutcomeAbandoned:
			abandoned++
		case reconcileOutcomeSkipped:
			skipped++
		}
	}

	slog.Info("jobs.magic_link_reconciler.completed",
		"candidates", len(candidates),
		"resent", resent,
		"abandoned", abandoned,
		"skipped", skipped,
		"duration_ms", time.Since(start).Milliseconds(),
		"job_id", job.ID,
	)
	return nil
}

// reconcileOutcome is a small enum describing what happened for one row.
// Used purely for per-batch counters in the completion log.
type reconcileOutcome int

const (
	reconcileOutcomeSkipped reconcileOutcome = iota
	reconcileOutcomeResent
	reconcileOutcomeAbandoned
)

// listReconcileCandidates returns rows that satisfy:
//
//   email_send_status IN ('pending', 'send_failed')
//   created_at > now() - 15min   (TTL gate)
//   email_send_attempts < 3      (3-attempt cap)
//
// Oldest-first so rows closest to expiry are retried before fresher ones.
// Backed by the partial index idx_magic_links_reconcile from migration
// 041.
func (w *MagicLinkReconcilerWorker) listReconcileCandidates(ctx context.Context) ([]magicLinkReconcileRow, error) {
	cutoff := time.Now().UTC().Add(-magicLinkReconcilerTTL)
	rows, err := w.db.QueryContext(ctx, `
		SELECT id, email, email_send_status, email_send_attempts, created_at
		FROM magic_links
		WHERE email_send_status IN ('pending', 'send_failed')
		  AND created_at > $1
		  AND email_send_attempts < 3
		ORDER BY created_at ASC
		LIMIT $2
	`, cutoff, magicLinkReconcilerBatchLimit)
	if err != nil {
		return nil, fmt.Errorf("query magic_links: %w", err)
	}
	defer rows.Close()

	var out []magicLinkReconcileRow
	for rows.Next() {
		var r magicLinkReconcileRow
		if err := rows.Scan(&r.id, &r.email, &r.emailSendStatus, &r.attempts, &r.createdAt); err != nil {
			slog.Warn("jobs.magic_link_reconciler.scan_failed", "error", err)
			continue
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return out, nil
}

// driveResend POSTs to the api's /internal/email/resend-magic-link
// endpoint with a worker-signed bearer token. Returns the outcome enum.
// The api is responsible for updating email_send_status; the worker
// reads the JSON response only to populate the per-batch counters.
func (w *MagicLinkReconcilerWorker) driveResend(ctx context.Context, r magicLinkReconcileRow) reconcileOutcome {
	url := w.apiBase + "/internal/email/resend-magic-link"
	body, err := json.Marshal(map[string]string{"link_id": r.id.String()})
	if err != nil {
		slog.Error("jobs.magic_link_reconciler.marshal_failed",
			"error", err, "link_id", r.id.String())
		return reconcileOutcomeSkipped
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		slog.Error("jobs.magic_link_reconciler.build_request_failed",
			"error", err, "link_id", r.id.String())
		return reconcileOutcomeSkipped
	}

	token, err := signMagicLinkResendJWT(w.jwtSecret, r.id.String())
	if err != nil {
		slog.Error("jobs.magic_link_reconciler.jwt_sign_failed",
			"error", err, "link_id", r.id.String())
		return reconcileOutcomeSkipped
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "instanode-worker/magic-link-reconciler")

	resp, err := w.httpCli.Do(req)
	if err != nil {
		slog.Warn("jobs.magic_link_reconciler.api_call_failed",
			"error", err,
			"link_id", r.id.String(),
		)
		return reconcileOutcomeSkipped
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		slog.Warn("jobs.magic_link_reconciler.api_non_2xx",
			"status", resp.StatusCode,
			"link_id", r.id.String(),
			"body", strings.TrimSpace(string(bodyBytes)),
		)
		return reconcileOutcomeSkipped
	}

	// Parse the JSON to decide between "resent" and "abandoned" for the
	// per-batch counter. Both are 200s — the api uses the response body
	// to distinguish outcomes.
	var respBody struct {
		OK     bool   `json:"ok"`
		Status string `json:"status"`
	}
	dec := json.NewDecoder(io.LimitReader(resp.Body, 4096))
	if err := dec.Decode(&respBody); err != nil {
		slog.Warn("jobs.magic_link_reconciler.api_response_parse_failed",
			"error", err, "link_id", r.id.String())
		return reconcileOutcomeSkipped
	}
	switch respBody.Status {
	case "sent":
		slog.Info("jobs.magic_link_reconciler.resent",
			"link_id", r.id.String(),
			"prior_attempts", r.attempts,
		)
		return reconcileOutcomeResent
	case "abandoned":
		// The api flipped the row to send_abandoned; an operator alert
		// log already fired from the api's
		// magic_link.resend.send_abandoned line.
		slog.Warn("jobs.magic_link_reconciler.abandoned",
			"link_id", r.id.String(),
			"prior_attempts", r.attempts,
		)
		return reconcileOutcomeAbandoned
	case "send_failed":
		// Transient — the row will be picked up again on the next tick
		// until it either succeeds or hits the abandonment cap.
		slog.Info("jobs.magic_link_reconciler.transient_failure",
			"link_id", r.id.String(),
			"prior_attempts", r.attempts,
		)
		return reconcileOutcomeSkipped
	case "expired":
		// The api saw the row had aged past TTL between our SELECT and
		// the resend request. Not an error; we just stop chasing it.
		slog.Info("jobs.magic_link_reconciler.expired_between_select_and_resend",
			"link_id", r.id.String(),
		)
		return reconcileOutcomeSkipped
	default:
		slog.Warn("jobs.magic_link_reconciler.unknown_api_status",
			"status", respBody.Status,
			"link_id", r.id.String(),
		)
		return reconcileOutcomeSkipped
	}
}

// signMagicLinkResendJWT mints a short-lived HS256 JWT the api can verify
// against WORKER_INTERNAL_JWT_SECRET. Claims match what the api's
// verifyInternalResendMagicLinkJWT expects:
//
//   purpose  — "resend_magic_link" (route discrimination)
//   link_id  — the row UUID we're asking the api to resend
//   iat      — issued-at (UTC seconds); within ±60s of api now
//
// Hand-rolled rather than pulling in a JWT library so the worker module
// stays dependency-light (the api side already imports
// github.com/golang-jwt/jwt for the matching verifier).
func signMagicLinkResendJWT(secret, linkID string) (string, error) {
	if secret == "" {
		return "", errors.New("empty secret")
	}
	header := magicLinkReconcilerBase64URLEncode([]byte(`{"alg":"HS256","typ":"JWT"}`))
	now := time.Now().UTC().Unix()
	claims := map[string]any{
		"purpose": magicLinkReconcilerPurpose,
		"link_id": linkID,
		"iat":     now,
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("marshal claims: %w", err)
	}
	body := header + "." + magicLinkReconcilerBase64URLEncode(claimsJSON)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	sig := magicLinkReconcilerBase64URLEncode(mac.Sum(nil))
	return body + "." + sig, nil
}

// magicLinkReconcilerBase64URLEncode is the unpadded URL-safe base64
// encoding JWTs use. Renamed (not just `base64URLEncode`) to avoid a
// collision with payment_grace_terminator.go's identically-named helper.
func magicLinkReconcilerBase64URLEncode(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}
