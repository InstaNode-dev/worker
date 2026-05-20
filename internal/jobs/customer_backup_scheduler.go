// customer_backup_scheduler.go — periodic sweep that INSERTs a `pending`
// resource_backups row for every tier-eligible postgres/vector resource on
// the platform.
//
// Why a scheduler at all (vs. cron triggering the api directly): the worker
// already owns the DB connection, the audit_log writer, and the periodic-job
// runner. Adding "one INSERT per active resource every hour" here is one
// SQL roundtrip per resource and avoids a second cron service. The
// customer_backup_runner picks up the pending rows on its next 30s tick
// regardless of whether the row was inserted by this job or by the api
// (manual backup) — there's exactly one downstream code path.
//
// Cadence by tier (resource.tier, NOT team.plan_tier, so a row that was
// snapshotted at the higher tier keeps its cadence even after a downgrade):
//
//	team / pro / growth → every hour
//	hobby               → once per day, at the team's daily slot
//	anonymous           → never (24h TTL, the bundle assumes you re-claim)
//
// Hobby's "daily slot" is the lowest 4 bits of the team UUID mod 24, applied
// as the hour-of-day in UTC. This spreads daily backups across the 24 hours
// of the day deterministically per team — so a 2K-team customer base gets a
// flat ~83 backups/hour instead of a 2K-backup spike at midnight UTC.
//
// Dedupe is enforced by a 50-minute lookback per resource: the same hour-bucket
// won't double-insert if the scheduler runs twice (e.g. on a worker restart
// with RunOnStart=true). 50min < 60min so the same hour is always covered
// by the previous run's row.
package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
)

// CustomerBackupSchedulerArgs holds no fields — the job is periodic and
// self-contained.
type CustomerBackupSchedulerArgs struct{}

// Kind is the River-side identifier; matches snake_case of the worker name.
func (CustomerBackupSchedulerArgs) Kind() string { return "customer_backup_scheduler" }

// CustomerBackupSchedulerWorker scans the resources table once an hour and
// inserts a resource_backups row in the 'pending' state for every active
// postgres/vector resource whose tier is due for a backup this hour.
type CustomerBackupSchedulerWorker struct {
	river.WorkerDefaults[CustomerBackupSchedulerArgs]
	db  *sql.DB
	now func() time.Time // injectable for tests
}

// NewCustomerBackupSchedulerWorker constructs a CustomerBackupSchedulerWorker.
// now is injected so the daily-slot logic can be exercised at fixed times in
// tests; production callers pass nil and we default to time.Now.
func NewCustomerBackupSchedulerWorker(db *sql.DB) *CustomerBackupSchedulerWorker {
	return &CustomerBackupSchedulerWorker{db: db, now: time.Now}
}

// canonicalTier strips the "_yearly" suffix from a plan tier name so the
// cadence gate can treat e.g. "hobby_yearly" the same as "hobby". Kept
// local to this package so the scheduler doesn't need a hard dependency
// on common/plans just for one string strip. Mirrors
// instant.dev/common/plans.CanonicalTier.
func canonicalTier(tier string) string {
	const suffix = "_yearly"
	if len(tier) > len(suffix) && tier[len(tier)-len(suffix):] == suffix {
		return tier[:len(tier)-len(suffix)]
	}
	return tier
}

// hobbyDailySlot returns the hour-of-day [0,24) at which a given team should
// receive its single daily hobby-tier backup. Deterministic per team UUID:
// the high 4 bits of the first byte are used to spread teams across 24
// hours. Pure function — exported for the unit test.
func hobbyDailySlot(teamID uuid.UUID) int {
	// Use the first byte of the UUID to bucket. Modulo 24 gives flat
	// distribution because the first byte is uniformly random (UUID v4) or
	// monotonic (UUID v7 truncated — still uniform in the low bits). Mod by
	// 24 not 16 so we hit every clock hour, not just 0-15.
	return int(teamID[0]) % 24
}

// Work performs the per-tick sweep. Every step is fail-open at the
// per-resource granularity — a single bad row never blocks the rest of the
// sweep, matching the convention from expire.go / quota.go.
func (w *CustomerBackupSchedulerWorker) Work(ctx context.Context, job *river.Job[CustomerBackupSchedulerArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.customer_backup_scheduler")
	defer span.End()

	start := w.now().UTC()
	hourUTC := start.Hour()

	// One row per (resource_id, tier) snapshot. We deliberately read
	// resource.tier — not team.plan_tier — because the resource is what
	// carries the user-paid retention contract (mirrors ElevateResourceTiers
	// on the api side).
	//
	// FIX-H (#56/#R6 B36) — the prior hardcoded set
	// (hobby, pro, growth, team) silently excluded hobby_plus and every
	// _yearly variant, so paid hobby_plus / hobby_plus_yearly / pro_yearly /
	// growth_yearly / team_yearly customers received ZERO scheduled
	// backups. The fix lists every tier whose plans.yaml row has
	// backup_retention_days > 0. We keep the list inline rather than
	// querying plans.Registry here because the scheduler doesn't yet
	// take a Registry — adding a registry param would force a constructor
	// change across cmd/, deferred to a separate refactor.
	rows, err := w.db.QueryContext(ctx, `
		SELECT r.id::text, r.tier, r.team_id
		FROM resources r
		WHERE r.status = 'active'
		  AND r.resource_type IN ('postgres', 'vector')
		  AND r.tier IN (
		    'hobby', 'hobby_yearly',
		    'hobby_plus', 'hobby_plus_yearly',
		    'pro', 'pro_yearly',
		    'growth', 'growth_yearly',
		    'team', 'team_yearly'
		  )
	`)
	if err != nil {
		return fmt.Errorf("CustomerBackupSchedulerWorker: query failed: %w", err)
	}
	defer rows.Close()

	type cand struct {
		id     string
		tier   string
		teamID uuid.NullUUID
	}
	var candidates []cand
	for rows.Next() {
		var c cand
		if scanErr := rows.Scan(&c.id, &c.tier, &c.teamID); scanErr != nil {
			slog.Warn("jobs.customer_backup_scheduler.scan_failed", "error", scanErr)
			continue
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("CustomerBackupSchedulerWorker: rows error: %w", err)
	}

	inserted := 0
	skippedHobbyOffSlot := 0
	skippedDedup := 0
	for _, c := range candidates {
		// Cadence gate.
		//
		// FIX-H — hobby and hobby_plus (and their _yearly variants) run
		// at one daily slot per team. Pro / Growth / Team (and yearly
		// counterparts) back up every hour. canonicalTier strips the
		// _yearly suffix so hobby_yearly / hobby_plus_yearly share the
		// daily-slot policy with their monthly canonical tier.
		switch canonicalTier(c.tier) {
		case "hobby", "hobby_plus":
			if !c.teamID.Valid {
				// Defensive: a tier='hobby*' resource without a team_id is
				// nonsensical (only anonymous rows have NULL team) but if
				// it slips in we skip rather than panic-divide.
				continue
			}
			if hobbyDailySlot(c.teamID.UUID) != hourUTC {
				skippedHobbyOffSlot++
				continue
			}
		}
		// pro / growth / team (any variant) + hobby/hobby_plus-on-slot proceed.

		rid, err := uuid.Parse(c.id)
		if err != nil {
			slog.Warn("jobs.customer_backup_scheduler.bad_uuid", "id", c.id, "error", err)
			continue
		}

		// Dedupe: skip if a row already exists for this resource within
		// the last 50 minutes. The hourly cron tick PLUS RunOnStart=true
		// can fire two ticks back-to-back at worker startup — without this
		// guard a restart would double-insert.
		//
		// P2-W4 (BugBash 2026-05-18): the prior code did a separate
		// SELECT EXISTS … check followed by an unconditional INSERT — a
		// check-then-act TOCTOU. Two ticks (or two worker pods before the
		// River UniqueOpts guard, or RunOnStart racing the periodic tick)
		// could both observe existed=false and both INSERT, scheduling a
		// duplicate backup. The fix folds the dedupe into the INSERT
		// itself: `INSERT … SELECT … WHERE NOT EXISTS (…)`. The whole
		// statement is one atomic round-trip — Postgres evaluates the
		// NOT EXISTS and the INSERT under a single snapshot, so a losing
		// concurrent tick inserts 0 rows. RETURNING + RowsAffected tells
		// us which arm won. A query-level failure still fails open by
		// design: we log and skip the row (no insert) rather than risk an
		// unbounded retry pile-up.
		res, insErr := w.db.ExecContext(ctx, `
			INSERT INTO resource_backups (resource_id, status, backup_kind, tier_at_backup)
			SELECT $1, 'pending', 'scheduled', $2
			WHERE NOT EXISTS (
				SELECT 1 FROM resource_backups
				WHERE resource_id = $1
				  AND backup_kind = 'scheduled'
				  AND created_at > now() - INTERVAL '50 minutes'
			)
		`, rid, c.tier)
		if insErr != nil {
			slog.Error("jobs.customer_backup_scheduler.insert_failed",
				"resource_id", c.id,
				"tier", c.tier,
				"error", insErr,
			)
			continue
		}
		// RowsAffected == 0 means the NOT EXISTS arm matched — a recent
		// scheduled row already exists, so this tick is a deduped no-op.
		if n, raErr := res.RowsAffected(); raErr == nil && n == 0 {
			skippedDedup++
			continue
		}
		inserted++
	}

	// Wave 3 / Worker T21 P1-1 follow-up (#146): demote idle-tick INFO →
	// DEBUG. customer_backup_scheduler runs every 1h; an idle tick (no
	// candidates AND nothing inserted/skipped) is heartbeat noise. INFO
	// retained for any state-transitioning tick.
	if inserted == 0 && skippedHobbyOffSlot == 0 && skippedDedup == 0 && len(candidates) == 0 {
		slog.Debug("jobs.customer_backup_scheduler.completed",
			"candidates", 0,
			"inserted", 0,
			"skipped_hobby_off_slot", 0,
			"skipped_dedup", 0,
			"hour_utc", hourUTC,
			"duration_ms", time.Since(start).Milliseconds(),
			"job_id", job.ID,
		)
		return nil
	}
	slog.Info("jobs.customer_backup_scheduler.completed",
		"candidates", len(candidates),
		"inserted", inserted,
		"skipped_hobby_off_slot", skippedHobbyOffSlot,
		"skipped_dedup", skippedDedup,
		"hour_utc", hourUTC,
		"duration_ms", time.Since(start).Milliseconds(),
		"job_id", job.ID,
	)
	return nil
}
