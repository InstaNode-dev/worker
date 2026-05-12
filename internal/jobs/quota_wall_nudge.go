package jobs

// quota_wall_nudge.go — Track U1.
//
// Periodic job that scans every team and writes a single `near_quota_wall`
// row to audit_log when the team is at or above 80% of any tier-limit axis
// (storage, connections, provisions). The API reads the latest row via
// GET /api/v1/usage/wall; the dashboard renders a yellow upgrade banner
// when the row is < 24h old.
//
// Idempotency contract: at most one near_quota_wall row per team per 24h.
// The scan skips a team whose most recent near_quota_wall row was written
// inside the last 24h, regardless of axis. This keeps the dashboard banner
// stable (it won't flicker between axes once it's earned a slot).
//
// Tier gating: teams on the "team" tier are skipped entirely — team tier
// is unlimited on every axis, so a wall nudge is incoherent. Teams on
// tier "free" (claimed-but-unpaid) and "anonymous" (unclaimed) are also
// skipped: they are pre-conversion and the conversion CTA is the claim
// banner, not an upgrade banner.
//
// Read path: the audit_log table already has
//   idx_audit_team_at (team_id, created_at DESC)
// (migration 012_audit_log.sql), which is the exact prefix our
// "most recent row for team where kind='near_quota_wall' in last 24h"
// query needs. No new index required.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
)

// QuotaWallNudgeArgs holds arguments for the quota-wall nudge scan.
type QuotaWallNudgeArgs struct{}

// Kind is the River worker key.
func (QuotaWallNudgeArgs) Kind() string { return "quota_wall_nudge" }

// quotaWallNudgeInterval is how often the periodic scan runs. 30 minutes
// is a balance between "fresh enough to catch a fast climber" and "not
// hammering the platform DB with full-team scans". The 24h dedupe makes
// the actual write rate ≤ 1 per team per day regardless of this cadence.
const quotaWallNudgeInterval = 30 * time.Minute

// quotaWallDedupeWindow is the minimum gap between two near_quota_wall
// rows for the same team. 24h matches the dashboard banner's natural
// lifetime — one nudge per day is enough to be noticed, not enough to
// be ignored.
const quotaWallDedupeWindow = 24 * time.Hour

// quotaWallThresholdPercent is the percent-used floor that triggers a
// nudge. 80% is the standard "you're approaching the wall" mark — early
// enough to upgrade before the user actually hits a 402 / quota error,
// late enough that the user has a real signal it matters.
const quotaWallThresholdPercent = 80

// quotaWallKind is the audit_log.kind value written by this job. The API
// endpoint filters audit_log on this exact string. Constant so a typo
// in either side surfaces at compile time, not silently at runtime.
const quotaWallKind = "near_quota_wall"

// quotaWallActor is the audit_log.actor value for system-written rows.
// Mirrors the convention used by other system-emitted audit rows (the
// human-readable activity feed renders "system …" for these).
const quotaWallActor = "system"

// QuotaWallPlanRegistry is the minimal plans.Registry surface needed by
// this job. Kept as a local interface so the worker doesn't have to take
// a hard dep on the full registry implementation in tests.
type QuotaWallPlanRegistry interface {
	StorageLimitMB(tier, service string) int
	ConnectionsLimit(tier, service string) int
	ProvisionLimit(tier string) int
}

// connectionStyleServices is the set of resource types where ConnectionsLimit
// is meaningful (each provisioned instance has a per-resource conn cap).
// Storage-only services (redis, queue, storage, webhook) don't count here.
var connectionStyleServices = []string{"postgres", "mongodb"}

// storageServices is the set of resource types where StorageLimitMB
// is meaningful and storage_bytes is tracked on the resource row.
var storageServices = []string{"postgres", "redis", "mongodb"}

// QuotaWallNudgeWorker scans every team for "approaching the wall"
// conditions and writes one audit_log row per team per 24h when found.
type QuotaWallNudgeWorker struct {
	river.WorkerDefaults[QuotaWallNudgeArgs]
	db    *sql.DB
	plans QuotaWallPlanRegistry
}

// NewQuotaWallNudgeWorker constructs a QuotaWallNudgeWorker.
func NewQuotaWallNudgeWorker(db *sql.DB, plans QuotaWallPlanRegistry) *QuotaWallNudgeWorker {
	return &QuotaWallNudgeWorker{db: db, plans: plans}
}

// Work runs one full scan: every team gets evaluated against the three
// axes. Teams already-nudged within quotaWallDedupeWindow are skipped.
// Returns an error only on a fatal DB failure — per-team scan errors are
// logged and the next team is processed (fail open: a stale banner is
// better than a stalled cron).
func (w *QuotaWallNudgeWorker) Work(ctx context.Context, job *river.Job[QuotaWallNudgeArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.quota_wall_nudge")
	defer span.End()

	rows, err := w.db.QueryContext(ctx, `
		SELECT id, plan_tier
		FROM teams
		WHERE plan_tier NOT IN ('team', 'anonymous', 'free')
		ORDER BY id
	`)
	if err != nil {
		return fmt.Errorf("QuotaWallNudgeWorker: list teams: %w", err)
	}
	defer rows.Close()

	scanned, nudged, skipped := 0, 0, 0

	for rows.Next() {
		var (
			teamIDStr string
			tier      string
		)
		if scanErr := rows.Scan(&teamIDStr, &tier); scanErr != nil {
			slog.Error("jobs.quota_wall_nudge.scan_error", "error", scanErr)
			continue
		}
		scanned++

		teamID, parseErr := uuid.Parse(teamIDStr)
		if parseErr != nil {
			slog.Error("jobs.quota_wall_nudge.invalid_uuid", "id", teamIDStr, "error", parseErr)
			continue
		}

		recentlyNudged, dedupeErr := w.teamRecentlyNudged(ctx, teamID)
		if dedupeErr != nil {
			slog.Error("jobs.quota_wall_nudge.dedupe_query_failed",
				"team_id", teamID, "error", dedupeErr)
			continue
		}
		if recentlyNudged {
			skipped++
			continue
		}

		hit, hitErr := w.evaluateTeam(ctx, teamID, tier)
		if hitErr != nil {
			slog.Error("jobs.quota_wall_nudge.evaluate_failed",
				"team_id", teamID, "tier", tier, "error", hitErr)
			continue
		}
		if hit == nil {
			continue
		}

		if insertErr := w.insertNearWallRow(ctx, teamID, hit); insertErr != nil {
			slog.Error("jobs.quota_wall_nudge.insert_failed",
				"team_id", teamID, "tier", tier, "error", insertErr)
			continue
		}
		nudged++
		slog.Info("jobs.quota_wall_nudge.wrote_row",
			"team_id", teamID,
			"tier", tier,
			"axis", hit.Axis,
			"percent_used", hit.PercentUsed,
		)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("QuotaWallNudgeWorker: rows error: %w", err)
	}

	var jobID int64
	if job.JobRow != nil {
		jobID = job.ID
	}
	slog.Info("jobs.quota_wall_nudge.completed",
		"scanned_teams", scanned,
		"nudged", nudged,
		"skipped_recent", skipped,
		"job_id", jobID,
	)
	return nil
}

// wallHit is the per-team scan result. Service is the resource type
// that produced the worst overrun on the storage / connections axes
// ("" for the provisions axis, which is service-agnostic).
type wallHit struct {
	Tier        string `json:"tier"`
	Axis        string `json:"axis"`
	Service     string `json:"service,omitempty"`
	Current     int64  `json:"current"`
	Limit       int64  `json:"limit"`
	PercentUsed int    `json:"percent_used"`
}

// teamRecentlyNudged returns true if any near_quota_wall row exists for
// this team within the dedupe window. Uses idx_audit_team_at (already
// shipped via migration 012) — the (team_id, created_at DESC) prefix is
// exactly the access pattern.
func (w *QuotaWallNudgeWorker) teamRecentlyNudged(ctx context.Context, teamID uuid.UUID) (bool, error) {
	cutoff := time.Now().Add(-quotaWallDedupeWindow)
	var n int
	err := w.db.QueryRowContext(ctx, `
		SELECT 1
		FROM audit_log
		WHERE team_id = $1
		  AND kind = $2
		  AND created_at >= $3
		ORDER BY created_at DESC
		LIMIT 1
	`, teamID, quotaWallKind, cutoff).Scan(&n)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("teamRecentlyNudged: %w", err)
	}
	return true, nil
}

// evaluateTeam runs the three-axis scan for one team. Returns the
// worst-overrun hit (highest percent_used) above the threshold, or nil
// if the team is below 80% on every axis.
//
// Axes:
//
//  1. storage — sum(storage_bytes) per service vs limit_mb * active_count
//  2. connections — count(active resources of conn-style type) vs
//     ConnectionsLimit(tier, type). With no live concurrent-conn metric in
//     the platform DB, the proxy is: how many resources of a conn-style
//     type are active relative to the per-resource conn cap; >= 80% of
//     that cap signals the user is materially using their conn budget.
//  3. provisions — count(all active resources for this team) vs
//     ProvisionLimit(tier). Daily limit; we use lifetime active count as
//     a conservative ceiling proxy (you can't have more concurrent
//     resources than you provisioned).
func (w *QuotaWallNudgeWorker) evaluateTeam(ctx context.Context, teamID uuid.UUID, tier string) (*wallHit, error) {
	var best *wallHit

	// ── Storage axis ────────────────────────────────────────────────
	for _, svc := range storageServices {
		var (
			sumBytes   int64
			countRows  int64
			countQuery = `
				SELECT COALESCE(SUM(storage_bytes), 0), COUNT(*)
				FROM resources
				WHERE team_id = $1
				  AND resource_type = $2
				  AND status = 'active'
			`
		)
		if err := w.db.QueryRowContext(ctx, countQuery, teamID, svc).Scan(&sumBytes, &countRows); err != nil {
			return nil, fmt.Errorf("evaluateTeam storage %s: %w", svc, err)
		}
		if countRows == 0 {
			continue
		}
		limitMB := w.plans.StorageLimitMB(tier, svc)
		if limitMB < 0 {
			continue // unlimited
		}
		totalLimitBytes := int64(limitMB) * 1024 * 1024 * countRows
		if totalLimitBytes <= 0 {
			continue
		}
		pct := int((sumBytes * 100) / totalLimitBytes)
		if pct < quotaWallThresholdPercent {
			continue
		}
		hit := &wallHit{
			Tier:        tier,
			Axis:        "storage",
			Service:     svc,
			Current:     sumBytes,
			Limit:       totalLimitBytes,
			PercentUsed: pct,
		}
		if best == nil || pct > best.PercentUsed {
			best = hit
		}
	}

	// ── Connections axis ───────────────────────────────────────────
	for _, svc := range connectionStyleServices {
		connLim := w.plans.ConnectionsLimit(tier, svc)
		if connLim < 0 {
			continue // unlimited
		}
		var countRows int64
		err := w.db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM resources
			WHERE team_id = $1
			  AND resource_type = $2
			  AND status = 'active'
		`, teamID, svc).Scan(&countRows)
		if err != nil {
			return nil, fmt.Errorf("evaluateTeam connections %s: %w", svc, err)
		}
		if countRows == 0 || connLim == 0 {
			continue
		}
		pct := int((countRows * 100) / int64(connLim))
		if pct < quotaWallThresholdPercent {
			continue
		}
		hit := &wallHit{
			Tier:        tier,
			Axis:        "connections",
			Service:     svc,
			Current:     countRows,
			Limit:       int64(connLim),
			PercentUsed: pct,
		}
		if best == nil || pct > best.PercentUsed {
			best = hit
		}
	}

	// ── Provisions axis ────────────────────────────────────────────
	provLim := w.plans.ProvisionLimit(tier)
	if provLim > 0 {
		var countRows int64
		err := w.db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM resources
			WHERE team_id = $1
			  AND status = 'active'
		`, teamID).Scan(&countRows)
		if err != nil {
			return nil, fmt.Errorf("evaluateTeam provisions: %w", err)
		}
		if countRows > 0 {
			pct := int((countRows * 100) / int64(provLim))
			if pct >= quotaWallThresholdPercent {
				hit := &wallHit{
					Tier:        tier,
					Axis:        "provisions",
					Current:     countRows,
					Limit:       int64(provLim),
					PercentUsed: pct,
				}
				if best == nil || pct > best.PercentUsed {
					best = hit
				}
			}
		}
	}

	return best, nil
}

// insertNearWallRow writes a single audit_log row with kind=near_quota_wall.
// Summary text is rendered verbatim by the dashboard (audit.go contract),
// so we keep it to known-safe values (tier, axis, integers).
func (w *QuotaWallNudgeWorker) insertNearWallRow(ctx context.Context, teamID uuid.UUID, hit *wallHit) error {
	metaBytes, err := json.Marshal(hit)
	if err != nil {
		return fmt.Errorf("insertNearWallRow marshal: %w", err)
	}
	summary := fmt.Sprintf(
		"You're at %d%% of your %s tier %s wall — upgrade for headroom.",
		hit.PercentUsed, hit.Tier, hit.Axis,
	)
	_, err = w.db.ExecContext(ctx, `
		INSERT INTO audit_log (team_id, actor, kind, summary, metadata)
		VALUES ($1, $2, $3, $4, $5)
	`, teamID, quotaWallActor, quotaWallKind, summary, metaBytes)
	if err != nil {
		return fmt.Errorf("insertNearWallRow exec: %w", err)
	}
	return nil
}
