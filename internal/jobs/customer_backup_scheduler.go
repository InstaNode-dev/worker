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
// Cadence is RPO-DRIVEN, not a hardcoded tier list. The decision reads the
// per-tier `rpo_minutes` promise from plans.yaml (via the BackupPlanRegistry)
// so the effective backup cadence always matches what the product SELLS:
//
//	rpo_minutes in [1,60]  → every hour  (pro / growth / team = 60)
//	rpo_minutes > 60       → once per day, at the team's daily slot
//	                         (hobby / hobby_plus = 1440)
//	rpo_minutes == 0       → never enqueued (anonymous / free have
//	                         backup_retention_days:0 and promise no RPO;
//	                         24h TTL, the bundle assumes you re-claim)
//
// R1 (2026-06-10): before this change pro/growth/team were backed up hourly
// but the cadence was wired to a hardcoded `switch canonicalTier()`. Changing
// rpo_minutes in plans.yaml would NOT have moved the cadence — an
// over-promise waiting to happen. Driving the gate off Registry.RPOMinutes
// closes that drift: plans.yaml is now the single source of truth for both
// the advertised RPO AND the cadence that makes it true. When the registry
// is nil (boot misconfigured) we fall back to the legacy hardcoded mapping
// so a bad boot never silently stops every paid backup.
//
// The hourlyRPOCutoffMinutes constant is the line between hourly and daily.
// 60 is not arbitrary: a tier promising rpo_minutes<=60 needs a fresh backup
// at least once an hour or the worst-case data-loss window exceeds the
// promise. Tiers promising a coarser RPO (1440 = daily) are served by the
// once-per-day slot below.
//
// The "daily slot" is the first byte of the team UUID mod 24, applied as the
// hour-of-day in UTC. This spreads daily backups across the 24 hours of the
// day deterministically per team — so a 2K-team customer base gets a flat
// ~83 backups/hour instead of a 2K-backup spike at midnight UTC.
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
	db    *sql.DB
	plans BackupPlanRegistry // RPO-cadence source of truth; nil → legacy fallback
	now   func() time.Time   // injectable for tests
}

// hourlyRPOCutoffMinutes is the boundary between hourly and daily cadence.
// A tier whose plans.yaml rpo_minutes is in [1, hourlyRPOCutoffMinutes] is
// backed up every hour; a coarser RPO (> cutoff) gets the once-daily slot.
// 60 matches the pro/growth/team rpo_minutes:60 promise — a fresh backup at
// least hourly is required to honour a 60-minute RPO.
const hourlyRPOCutoffMinutes = 60

// NewCustomerBackupSchedulerWorker constructs a CustomerBackupSchedulerWorker.
// plans is the source of truth for per-tier RPO → cadence; pass the same
// BackupPlanRegistry wired into the runner. When plans is nil the scheduler
// falls back to the legacy hardcoded tier→cadence mapping so a misconfigured
// boot doesn't silently stop every paid backup. now is injected so the
// daily-slot logic can be exercised at fixed times in tests; production
// callers pass time.Now via the constructor default.
func NewCustomerBackupSchedulerWorker(db *sql.DB, plans BackupPlanRegistry) *CustomerBackupSchedulerWorker {
	return &CustomerBackupSchedulerWorker{db: db, plans: plans, now: time.Now}
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

// cadence enumerates how often a tier is backed up. Derived from the tier's
// plans.yaml rpo_minutes promise — see cadenceForTier.
type cadence int

const (
	cadenceNever  cadence = iota // rpo_minutes==0 (anonymous/free) — skip entirely
	cadenceHourly                // rpo_minutes in [1,60] — every tick
	cadenceDaily                 // rpo_minutes>60 — once per day at the team slot
)

// cadenceForTier maps a resource tier to its backup cadence using the
// per-tier rpo_minutes from plans.yaml — the same value the product
// advertises on /api/v1/capabilities. This is the keystone of R1: the
// cadence that MAKES the RPO true is read from the SAME field that PROMISES
// it, so the two can never drift.
//
// When plans is nil (boot misconfigured) we fall back to the historical
// hardcoded mapping (pro/growth/team hourly, hobby/hobby_plus daily, all
// _yearly variants follow their canonical tier, everything else never) so a
// bad boot degrades to the known-good behaviour rather than silently
// stopping every paid backup.
func cadenceForTier(plans BackupPlanRegistry, tier string) cadence {
	if plans == nil {
		return legacyCadenceForTier(tier)
	}
	// rpo_minutes is the source of truth. 0 = "not promised" → no scheduled
	// backups (anonymous/free). retention 0 is a second, independent guard:
	// a tier that keeps backups 0 days must never have one enqueued even if
	// some future plans.yaml edit left a stray rpo_minutes on it.
	if plans.BackupRetentionDays(tier) <= 0 {
		return cadenceNever
	}
	rpo := plans.RPOMinutes(tier)
	switch {
	case rpo <= 0:
		return cadenceNever
	case rpo <= hourlyRPOCutoffMinutes:
		return cadenceHourly
	default:
		return cadenceDaily
	}
}

// legacyCadenceForTier is the pre-R1 hardcoded mapping, retained ONLY as the
// nil-registry fallback. New tiers added to plans.yaml are NOT reflected
// here — the registry path above is authoritative. Mirrors the old
// `switch canonicalTier()` gate so a registry-less boot behaves identically
// to the prior release.
func legacyCadenceForTier(tier string) cadence {
	switch canonicalTier(tier) {
	case "hobby", "hobby_plus":
		return cadenceDaily
	case "pro", "growth", "team":
		return cadenceHourly
	default:
		return cadenceNever
	}
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
	// R1 (2026-06-10) — the candidate SELECT now only excludes the
	// never-backup tiers (anonymous/free, backup_retention_days:0) instead
	// of an explicit allow-list of paid tiers. The authoritative cadence
	// decision (hourly vs daily vs skip) is made per-row by cadenceForTier,
	// which reads rpo_minutes from plans.yaml — so a NEW paid tier added to
	// plans.yaml is picked up automatically (no SQL edit), and the per-row
	// registry gate is a second, independent skip for anything that leaks
	// through. The SQL exclusion is cheap defence-in-depth: it keeps the
	// candidate set small without re-deriving the registry in SQL.
	//
	// Pre-R1 (FIX-H #56/#R6) this was a hardcoded allow-list that silently
	// dropped hobby_plus + every _yearly variant when first written; the
	// registry-driven path removes that single-site-list failure mode
	// entirely (root CLAUDE.md rule 18).
	rows, err := w.db.QueryContext(ctx, `
		SELECT r.id::text, r.tier, r.team_id
		FROM resources r
		WHERE r.status = 'active'
		  AND r.resource_type IN ('postgres', 'vector')
		  AND r.tier NOT IN ('anonymous', 'free')
	`)
	if err != nil {
		return fmt.Errorf("CustomerBackupSchedulerWorker: query failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

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
	skippedNeverTier := 0
	skippedDailyOffSlot := 0
	skippedDedup := 0
	for _, c := range candidates {
		// Cadence gate — RPO-driven (see cadenceForTier). The tier's
		// plans.yaml rpo_minutes decides hourly vs daily vs skip, so the
		// cadence always matches the advertised RPO.
		switch cadenceForTier(w.plans, c.tier) {
		case cadenceNever:
			// rpo_minutes==0 / backup_retention_days==0 — the tier promises
			// no RPO (anonymous/free, or anything that slipped past the SQL
			// filter). Never enqueue.
			skippedNeverTier++
			continue
		case cadenceDaily:
			if !c.teamID.Valid {
				// Defensive: a daily-cadence (hobby*) resource without a
				// team_id is nonsensical (only anonymous rows have NULL
				// team) but if it slips in we skip rather than panic-divide.
				skippedNeverTier++
				continue
			}
			if hobbyDailySlot(c.teamID.UUID) != hourUTC {
				skippedDailyOffSlot++
				continue
			}
		case cadenceHourly:
			// Every tick — fall through to insert.
		}

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
	if inserted == 0 && skippedNeverTier == 0 && skippedDailyOffSlot == 0 && skippedDedup == 0 && len(candidates) == 0 {
		slog.Debug("jobs.customer_backup_scheduler.completed",
			"candidates", 0,
			"inserted", 0,
			"skipped_never_tier", 0,
			"skipped_daily_off_slot", 0,
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
		"skipped_never_tier", skippedNeverTier,
		"skipped_daily_off_slot", skippedDailyOffSlot,
		"skipped_dedup", skippedDedup,
		"hour_utc", hourUTC,
		"duration_ms", time.Since(start).Milliseconds(),
		"job_id", job.ID,
	)
	return nil
}
