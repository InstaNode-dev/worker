package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/resend/resend-go/v2"
	"github.com/riverqueue/river"
)

// ---------------------------------------------------------------------------
// EmailClient — thin wrapper around resend used by email job workers
// ---------------------------------------------------------------------------

// EmailClient wraps the Resend API client for sending transactional emails.
type EmailClient struct {
	client *resend.Client
	from   string
	noop   bool
}

// DigestResourceCount holds per–resource-type counts for the weekly digest.
type DigestResourceCount struct {
	ResourceType string
	Count        int64
}

// NewEmailClient returns an EmailClient. A no-op client is returned when apiKey is empty.
func NewEmailClient(apiKey string) *EmailClient {
	if apiKey == "" {
		slog.Info("worker.email.noop", "reason", "no RESEND_API_KEY — emails will be logged only")
		return &EmailClient{noop: true, from: "Instant Dev <noreply@instant.dev>"}
	}
	return &EmailClient{
		client: resend.NewClient(apiKey),
		from:   "Instant Dev <noreply@instant.dev>",
	}
}

func (c *EmailClient) send(ctx context.Context, to, subject, plainText, htmlBody string) error {
	if c.noop {
		slog.Info("worker.email.skipped", "to", to, "subject", subject)
		return nil
	}
	params := &resend.SendEmailRequest{
		From:    c.from,
		To:      []string{to},
		Subject: subject,
		Text:    plainText,
		Html:    htmlBody,
	}
	_, err := c.client.Emails.SendWithContext(ctx, params)
	if err != nil {
		slog.Error("worker.email.send_failed", "to", to, "subject", subject, "error", err)
		return fmt.Errorf("email.send: %w", err)
	}
	return nil
}

func (c *EmailClient) SendTrialWarning(ctx context.Context, to string, resourceCount int, trialEndsAt time.Time) error {
	subject := "Your instant.dev trial ends in 2 days"
	endDate := trialEndsAt.UTC().Format("January 2, 2006")
	resWord := "resource"
	if resourceCount != 1 {
		resWord = "resources"
	}
	plain := fmt.Sprintf("Your instant.dev trial ends on %s.\n\nYou have %d active %s (databases, caches, storage, etc.). Add a payment method to keep them active after your trial ends.\n\nAdd payment method: https://instant.dev/billing/checkout\n\n— The instant.dev team\n", endDate, resourceCount, resWord)
	html := fmt.Sprintf(`<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body style="font-family:sans-serif;max-width:600px;margin:0 auto;padding:24px;color:#111;"><h2>Your instant.dev trial ends in 2 days</h2><p>Your trial ends on <strong>%s</strong>.</p><p>You have <strong>%d active %s</strong> (databases, caches, storage, etc.). Add a payment method to keep them active after your trial ends.</p><p style="margin-top:32px;"><a href="https://instant.dev/billing/checkout" style="background:#111;color:#fff;padding:12px 24px;text-decoration:none;border-radius:6px;font-weight:bold;">Add payment method &rarr;</a></p><p style="margin-top:40px;color:#666;font-size:13px;">— The instant.dev team</p></body></html>`, endDate, resourceCount, resWord)
	return c.send(ctx, to, subject, plain, html)
}

// SendExpiryReminder warns a claimed-but-unpaid user that one of their
// free-tier resources is about to expire. The reminder is dispatched by
// the hourly ExpiryReminderWorker (see expiry_reminder.go) once per
// resource — dedupe is enforced in the DB via resources.expiry_reminded_at.
//
// hoursRemaining is the integer hours until `expires_at`. We always render
// at least 1 to keep the copy honest ("expires in 0h" reads like it's
// already gone — the next reaper run will handle that case).
func (c *EmailClient) SendExpiryReminder(ctx context.Context, to, resourceType string, hoursRemaining int) error {
	if hoursRemaining < 1 {
		hoursRemaining = 1
	}

	subject := fmt.Sprintf("Your instanode %s expires in %dh", resourceType, hoursRemaining)

	plain := fmt.Sprintf(`Heads up — your instanode %s expires in about %d hour(s).

Free-tier resources are deleted 24h after you claim them unless you subscribe.

Keep it running for $9/mo (Hobby) — your data and connection string stay the same:
https://instanode.dev/app/billing

— The instanode.dev team
`, resourceType, hoursRemaining)

	safeType := htmlEscape(resourceType)
	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:sans-serif;max-width:600px;margin:0 auto;padding:24px;color:#111;">
  <h2>Your instanode %s expires in about %dh</h2>
  <p>Free-tier resources are deleted 24h after you claim them unless you subscribe.</p>
  <p>Keep it running for <strong>$9/mo (Hobby)</strong> — your data and connection string stay the same.</p>
  <p style="margin-top:32px;">
    <a href="https://instanode.dev/app/billing"
       style="background:#111;color:#fff;padding:12px 24px;text-decoration:none;border-radius:6px;font-weight:bold;">
      Keep my %s &rarr;
    </a>
  </p>
  <p style="margin-top:40px;color:#666;font-size:13px;">— The instanode.dev team</p>
</body>
</html>`, safeType, hoursRemaining, safeType)

	return c.send(ctx, to, subject, plain, html)
}

// SendDeployExpiring warns a user that a deployment is about to be auto-
// expired (Wave FIX-J). hoursRemaining is the integer hours until expires_at.
//
// The email body MUST tell the user EXACTLY what to do — the only acceptable
// next actions are (a) click the Make Permanent button to keep the deploy,
// or (b) let it expire. Anything fuzzier turns this into another support
// ticket. The make-permanent URL embeds the deployment id so the click is a
// single round-trip; no extra navigation needed.
func (c *EmailClient) SendDeployExpiring(ctx context.Context, to, deployName, deployURL string, hoursRemaining int, makePermanentURL string) error {
	if hoursRemaining < 1 {
		hoursRemaining = 1
	}
	subject := fmt.Sprintf("Your deployment %s expires in %dh — keep it or let it go", deployName, hoursRemaining)

	plain := fmt.Sprintf(`Heads up — your instanode deployment "%s" auto-expires in about %d hour(s).

Deploy URL: %s

To keep it permanently (no more reminders, no auto-delete):
%s

If you just wanted to test it out, do nothing — we'll clean it up automatically.

— The instanode.dev team
`, deployName, hoursRemaining, deployURL, makePermanentURL)

	safeName := htmlEscape(deployName)
	safeURL := htmlEscape(deployURL)
	safeMake := htmlEscape(makePermanentURL)
	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:sans-serif;max-width:600px;margin:0 auto;padding:24px;color:#111;">
  <h2>Your deployment %s expires in about %dh</h2>
  <p>Deploy URL: <a href="%s">%s</a></p>
  <p>To keep it permanently (no more reminders, no auto-delete):</p>
  <p style="margin-top:24px;">
    <a href="%s"
       style="background:#111;color:#fff;padding:12px 24px;text-decoration:none;border-radius:6px;font-weight:bold;">
      Keep this deployment &rarr;
    </a>
  </p>
  <p style="margin-top:24px;color:#666;">If you just wanted to test it out, do nothing — we'll clean it up automatically.</p>
  <p style="margin-top:40px;color:#666;font-size:13px;">— The instanode.dev team</p>
</body>
</html>`, safeName, hoursRemaining, safeURL, safeURL, safeMake)

	return c.send(ctx, to, subject, plain, html)
}

// SendDeployExpired notifies a user that their deployment was just removed.
// Sent ONCE after the deployment_expirer soft-deletes the row. No CTA to
// "restore" because the build artefact is gone — the user has to redeploy
// from source if they actually wanted to keep it.
func (c *EmailClient) SendDeployExpired(ctx context.Context, to, deployName string) error {
	subject := fmt.Sprintf("Your deployment %s has expired", deployName)
	plain := fmt.Sprintf(`Your instanode deployment "%s" reached its 24h TTL and was removed.

If this was a real deployment, you can redeploy from source and call POST /api/v1/deployments/<id>/make-permanent to keep it permanently — or flip your team's default at https://instanode.dev/app/settings/team.

— The instanode.dev team
`, deployName)
	safe := htmlEscape(deployName)
	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:sans-serif;max-width:600px;margin:0 auto;padding:24px;color:#111;">
  <h2>Your deployment %s has expired</h2>
  <p>This deployment reached its 24h TTL and was removed.</p>
  <p>If this was a real deployment, you can redeploy from source and call <code>POST /api/v1/deployments/&lt;id&gt;/make-permanent</code> to keep it permanently — or flip your team's default at <a href="https://instanode.dev/app/settings/team">instanode.dev/app/settings/team</a>.</p>
  <p style="margin-top:40px;color:#666;font-size:13px;">— The instanode.dev team</p>
</body>
</html>`, safe)
	return c.send(ctx, to, subject, plain, html)
}

func (c *EmailClient) SendTrialExpired(ctx context.Context, to string) error {
	subject := "Your instant.dev trial has ended"
	plain := "Your instant.dev trial has ended. Provisioned resources are suspended — your data is safe.\n\nReactivate your account for $12/mo to resume service.\n\nReactivate: https://instant.dev/billing/checkout\n\n— The instant.dev team\n"
	html := `<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body style="font-family:sans-serif;max-width:600px;margin:0 auto;padding:24px;color:#111;"><h2>Your instant.dev trial has ended</h2><p>Your trial ended. Provisioned resources are suspended &mdash; your data is safe.</p><p>Reactivate your account for <strong>$12/mo</strong> to resume service.</p><p style="margin-top:32px;"><a href="https://instant.dev/billing/checkout" style="background:#111;color:#fff;padding:12px 24px;text-decoration:none;border-radius:6px;font-weight:bold;">Reactivate for $12/mo &rarr;</a></p><p style="margin-top:40px;color:#666;font-size:13px;">— The instant.dev team</p></body></html>`
	return c.send(ctx, to, subject, plain, html)
}

func (c *EmailClient) SendWeeklyDigest(ctx context.Context, to string, stats []DigestResourceCount) error {
	subject := "Your instant.dev weekly summary"

	var sb strings.Builder
	sb.WriteString("Your instant.dev weekly summary\n\n")
	sb.WriteString(fmt.Sprintf("%-20s %10s\n", "Resource type", "Active"))
	for _, s := range stats {
		sb.WriteString(fmt.Sprintf("%-20s %10d\n", s.ResourceType, s.Count))
	}
	sb.WriteString("\nView your dashboard: https://instant.dev/dashboard\n")

	var htmlRows strings.Builder
	for _, s := range stats {
		htmlRows.WriteString(fmt.Sprintf(
			"<tr><td style=\"padding:8px 12px;border-bottom:1px solid #eee;\">%s</td><td style=\"padding:8px 12px;border-bottom:1px solid #eee;text-align:right;\">%d</td></tr>\n",
			htmlEscape(s.ResourceType), s.Count,
		))
	}
	html := fmt.Sprintf(`<!DOCTYPE html><html><head><meta charset="UTF-8"></head><body style="font-family:sans-serif;max-width:600px;margin:0 auto;padding:24px;color:#111;"><h2>Your instant.dev weekly summary</h2><table style="width:100%%;border-collapse:collapse;margin-top:16px;"><thead><tr style="background:#f5f5f5;"><th style="padding:8px 12px;text-align:left;border-bottom:2px solid #ddd;">Resource type</th><th style="padding:8px 12px;text-align:right;border-bottom:2px solid #ddd;">Active</th></tr></thead><tbody>%s</tbody></table><p style="margin-top:24px;color:#666;font-size:13px;">View your dashboard: <a href="https://instant.dev/dashboard">instant.dev/dashboard</a></p></body></html>`, htmlRows.String())

	return c.send(ctx, to, subject, sb.String(), html)
}

func htmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, `"`, "&quot;")
	return s
}

// ---------------------------------------------------------------------------
// TrialExpiryWorker — runs every 6 hours
// ---------------------------------------------------------------------------

// TrialExpiryArgs is the River job payload (empty — runs as a sweep).
type TrialExpiryArgs struct{}

func (TrialExpiryArgs) Kind() string { return "trial_expiry" }

// TrialExpiryWorker scans for expiring and expired hobby-tier teams.
type TrialExpiryWorker struct {
	river.WorkerDefaults[TrialExpiryArgs]
	db    *sql.DB
	email *EmailClient
}

// trialTeamRow is a lightweight projection of teams + user email for trial processing.
type trialTeamRow struct {
	teamID      uuid.UUID
	userEmail   string
	teamName    sql.NullString
	trialEndsAt time.Time
}

// Work scans teams and sends warning / expiry emails as appropriate.
func (w *TrialExpiryWorker) Work(ctx context.Context, job *river.Job[TrialExpiryArgs]) error {
	now := time.Now().UTC()

	if err := w.processWarnings(ctx, now); err != nil {
		slog.Error("jobs.trial_expiry.warnings_failed", "error", err)
	}

	if err := w.processExpired(ctx, now); err != nil {
		slog.Error("jobs.trial_expiry.expired_failed", "error", err)
	}

	return nil
}

func (w *TrialExpiryWorker) processWarnings(ctx context.Context, now time.Time) error {
	window := now.Add(48 * time.Hour)

	rows, err := w.db.QueryContext(ctx, `
		SELECT t.id, u.email, t.name, t.trial_ends_at
		FROM teams t
		JOIN users u ON u.team_id = t.id
		WHERE t.plan_tier = 'hobby'
		  AND t.trial_ends_at IS NOT NULL
		  AND t.trial_ends_at > $1
		  AND t.trial_ends_at <= $2
		ORDER BY t.trial_ends_at ASC
	`, now, window)
	if err != nil {
		return fmt.Errorf("trial_expiry.processWarnings query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var row trialTeamRow
		if err := rows.Scan(&row.teamID, &row.userEmail, &row.teamName, &row.trialEndsAt); err != nil {
			slog.Error("jobs.trial_expiry.warning_scan", "error", err)
			continue
		}

		var resourceCount int
		if err := w.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM resources
			WHERE team_id = $1
			  AND resource_type IN ('postgres', 'redis', 'mongodb', 'queue', 'storage', 'webhook')
			  AND status = 'active'
		`, row.teamID).Scan(&resourceCount); err != nil {
			slog.Error("jobs.trial_expiry.resource_count", "team_id", row.teamID, "error", err)
			resourceCount = 0
		}

		if err := w.email.SendTrialWarning(ctx, row.userEmail, resourceCount, row.trialEndsAt); err != nil {
			slog.Error("jobs.trial_expiry.send_warning_failed",
				"team_id", row.teamID,
				"email", row.userEmail,
				"error", err,
			)
		} else {
			slog.Info("jobs.trial_expiry.warning_sent",
				"team_id", row.teamID,
				"email", row.userEmail,
				"trial_ends_at", row.trialEndsAt,
			)
		}
	}
	return rows.Err()
}

func (w *TrialExpiryWorker) processExpired(ctx context.Context, now time.Time) error {
	rows, err := w.db.QueryContext(ctx, `
		SELECT t.id, u.email, t.name, t.trial_ends_at
		FROM teams t
		JOIN users u ON u.team_id = t.id
		WHERE t.plan_tier = 'hobby'
		  AND t.trial_ends_at IS NOT NULL
		  AND t.trial_ends_at < $1
		ORDER BY t.trial_ends_at ASC
	`, now)
	if err != nil {
		return fmt.Errorf("trial_expiry.processExpired query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var row trialTeamRow
		if err := rows.Scan(&row.teamID, &row.userEmail, &row.teamName, &row.trialEndsAt); err != nil {
			slog.Error("jobs.trial_expiry.expired_scan", "error", err)
			continue
		}

		if err := suspendTeamResources(ctx, w.db, row.teamID); err != nil {
			slog.Error("jobs.trial_expiry.suspend_failed",
				"team_id", row.teamID,
				"error", err,
			)
		}

		if _, err := w.db.ExecContext(ctx, `
			UPDATE teams SET trial_ends_at = NULL WHERE id = $1
		`, row.teamID); err != nil {
			slog.Error("jobs.trial_expiry.clear_trial_failed",
				"team_id", row.teamID,
				"error", err,
			)
		}

		if err := w.email.SendTrialExpired(ctx, row.userEmail); err != nil {
			slog.Error("jobs.trial_expiry.send_expired_failed",
				"team_id", row.teamID,
				"email", row.userEmail,
				"error", err,
			)
		} else {
			slog.Info("jobs.trial_expiry.expired_processed",
				"team_id", row.teamID,
				"email", row.userEmail,
			)
		}
	}
	return rows.Err()
}

// suspendTeamResources sets status='suspended' for provisioned resources belonging to a team.
func suspendTeamResources(ctx context.Context, db *sql.DB, teamID uuid.UUID) error {
	_, err := db.ExecContext(ctx, `
		UPDATE resources
		SET status = 'suspended'
		WHERE team_id = $1
		  AND resource_type IN ('postgres', 'redis', 'mongodb', 'queue', 'storage', 'webhook')
		  AND status = 'active'
	`, teamID)
	if err != nil {
		return fmt.Errorf("suspendTeamResources: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// WeeklyDigestWorker — runs every Monday at 08:00 UTC
// ---------------------------------------------------------------------------

// WeeklyDigestArgs is the River job payload (empty — runs as a sweep).
type WeeklyDigestArgs struct{}

func (WeeklyDigestArgs) Kind() string { return "weekly_digest" }

// WeeklyDigestWorker sends a weekly summary to all active team users.
type WeeklyDigestWorker struct {
	river.WorkerDefaults[WeeklyDigestArgs]
	db    *sql.DB
	email *EmailClient
}

// Work iterates over all non-anonymous teams and sends each user a weekly digest.
func (w *WeeklyDigestWorker) Work(ctx context.Context, job *river.Job[WeeklyDigestArgs]) error {
	rows, err := w.db.QueryContext(ctx, `
		SELECT u.email, t.id
		FROM users u
		JOIN teams t ON t.id = u.team_id
		WHERE t.plan_tier != 'anonymous'
		ORDER BY u.email
	`)
	if err != nil {
		return fmt.Errorf("weekly_digest.Work query users: %w", err)
	}
	defer rows.Close()

	type userTeam struct {
		email  string
		teamID uuid.UUID
	}
	var targets []userTeam

	for rows.Next() {
		var ut userTeam
		if err := rows.Scan(&ut.email, &ut.teamID); err != nil {
			slog.Error("jobs.weekly_digest.scan_users", "error", err)
			continue
		}
		targets = append(targets, ut)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("weekly_digest.Work rows: %w", err)
	}

	for _, ut := range targets {
		stats, err := w.buildResourceDigestCounts(ctx, ut.teamID)
		if err != nil {
			slog.Error("jobs.weekly_digest.build_stats",
				"team_id", ut.teamID,
				"email", ut.email,
				"error", err,
			)
			continue
		}

		if err := w.email.SendWeeklyDigest(ctx, ut.email, stats); err != nil {
			slog.Error("jobs.weekly_digest.send_failed",
				"team_id", ut.teamID,
				"email", ut.email,
				"error", err,
			)
		} else {
			slog.Info("jobs.weekly_digest.sent",
				"team_id", ut.teamID,
				"email", ut.email,
				"resource_type_rows", len(stats),
			)
		}
	}

	return nil
}

func (w *WeeklyDigestWorker) buildResourceDigestCounts(ctx context.Context, teamID uuid.UUID) ([]DigestResourceCount, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT resource_type, COUNT(*)::bigint
		FROM resources
		WHERE team_id = $1
		  AND resource_type IN ('postgres', 'redis', 'mongodb', 'queue', 'storage', 'webhook')
		  AND status != 'deleted'
		GROUP BY resource_type
		ORDER BY resource_type ASC
	`, teamID)
	if err != nil {
		return nil, fmt.Errorf("buildResourceDigestCounts query: %w", err)
	}
	defer rows.Close()

	var stats []DigestResourceCount
	for rows.Next() {
		var s DigestResourceCount
		if err := rows.Scan(&s.ResourceType, &s.Count); err != nil {
			return nil, fmt.Errorf("buildResourceDigestCounts scan: %w", err)
		}
		stats = append(stats, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("buildResourceDigestCounts rows: %w", err)
	}
	return stats, nil
}
