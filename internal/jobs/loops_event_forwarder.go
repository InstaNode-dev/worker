package jobs

// loops_event_forwarder.go — periodic job that drains audit_log rows into
// Loops.so so customer-facing lifecycle email is event-driven instead of
// requiring a HubSpot/Sales-Hub subscription before we have revenue.
//
// Cadence: every loopsForwarderInterval (60s). The cadence is conservative
// — Loops events drive email campaigns and a one-minute delay between
// "user did the thing" and "Loops fires the email" is well inside acceptable.
//
// Cursor: a (created_at, id) tuple stored in Redis under loopsCursorKey.
// We use a tuple, not a single id, because audit_log.id is UUID (NOT a
// bigserial — see migration 012_audit_log.sql) and UUID ordering is
// non-monotonic. The tuple gives us a stable, deterministic watermark.
//
// Idempotency: the cursor only advances after a confirmed Loops 2xx (or a
// permanent 4xx — see loops_client.go for the rationale). If the worker
// crashes mid-batch, the next start resumes from the last persisted
// cursor and re-sends only the unsent rows. Loops dedupes events per
// userId within a short window, so a duplicate send during a crash window
// won't fire two campaigns at the same user.
//
// Fail-open: a missing LOOPS_API_KEY at worker boot makes newLoopsClient
// return nil; this worker's Work then logs a warning and returns nil so
// River doesn't retry. The worker is still registered so the periodic job
// keeps firing — when the operator sets LOOPS_API_KEY and restarts the
// worker, forwarding begins automatically.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
)

// LoopsEventForwarderArgs is the River job payload — no fields, runs as a sweep.
type LoopsEventForwarderArgs struct{}

// Kind is the River worker key.
func (LoopsEventForwarderArgs) Kind() string { return "loops_event_forwarder" }

// loopsForwarderInterval is how often the periodic job fires. 60s is a
// balance between "fresh enough that users don't notice the delay" and
// "low enough churn on Redis + audit_log".
const loopsForwarderInterval = 60 * time.Second

// loopsBatchLimit caps rows processed per tick. With 100 rows × ~250ms
// per Loops POST = ~25s of work, well under the next 60s tick. A backlog
// drains over multiple ticks rather than blocking one giant transaction.
const loopsBatchLimit = 100

// loopsCursorKey is the Redis key holding the (created_at, id) cursor as
// a JSON blob. Single string key so it survives Redis restarts as long as
// the persistence policy allows; if Redis is wiped, the cursor resets and
// we re-process the audit_log tail (Loops dedupe absorbs the duplicates).
const loopsCursorKey = "loops:last_audit_cursor"

// loopsCursor is the watermark structure. CreatedAt + ID together give a
// strict total order even when multiple rows share a microsecond timestamp.
type loopsCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

// zero returns the lowest-possible cursor — used when Redis has no value yet.
// time.Time's zero value sorts before every real audit row's created_at.
func (c loopsCursor) zero() bool {
	return c.CreatedAt.IsZero() && c.ID == ""
}

// loopsCursorStore abstracts the cursor read/write so tests can supply an
// in-memory implementation. Production uses redisCursorStore, which wraps a
// *redis.Client. Single-method-per-direction surface keeps the seam tiny.
type loopsCursorStore interface {
	read(ctx context.Context) (loopsCursor, error)
	write(ctx context.Context, c loopsCursor) error
}

// redisCursorStore is the production implementation of loopsCursorStore.
// Backed by the platform Redis.
type redisCursorStore struct {
	rdb *redis.Client
}

func (s *redisCursorStore) read(ctx context.Context) (loopsCursor, error) {
	raw, err := s.rdb.Get(ctx, loopsCursorKey).Result()
	if err == redis.Nil {
		return loopsCursor{}, nil
	}
	if err != nil {
		return loopsCursor{}, fmt.Errorf("redis GET %s: %w", loopsCursorKey, err)
	}
	var c loopsCursor
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		// Corrupt cursor — start over. Log loudly so the operator can
		// investigate (and Loops dedupe will absorb duplicates).
		slog.Error("jobs.loops_event_forwarder.cursor_corrupt",
			"raw", raw,
			"error", err,
			"note", "resetting to zero — Loops dedupe absorbs duplicates",
		)
		return loopsCursor{}, nil
	}
	return c, nil
}

func (s *redisCursorStore) write(ctx context.Context, c loopsCursor) error {
	b, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshal cursor: %w", err)
	}
	if err := s.rdb.Set(ctx, loopsCursorKey, string(b), 0).Err(); err != nil {
		return fmt.Errorf("redis SET %s: %w", loopsCursorKey, err)
	}
	return nil
}

// LoopsEventForwarderWorker is the River worker. db is the platform Postgres,
// cursor is the watermark store (Redis in production), and loops is the HTTP
// client (nil iff LOOPS_API_KEY is unset, which makes Work a no-op).
type LoopsEventForwarderWorker struct {
	river.WorkerDefaults[LoopsEventForwarderArgs]
	db     *sql.DB
	cursor loopsCursorStore
	loops  *loopsClient
}

// NewLoopsEventForwarderWorker constructs the worker. Pass loops=nil when
// LOOPS_API_KEY is empty — Work then short-circuits with a warning each tick.
// The worker is always registered (vs. conditionally skipping AddWorker)
// because operators can rotate the secret without redeploying the binary.
func NewLoopsEventForwarderWorker(db *sql.DB, rdb *redis.Client, loops *loopsClient) *LoopsEventForwarderWorker {
	return &LoopsEventForwarderWorker{
		db:     db,
		cursor: &redisCursorStore{rdb: rdb},
		loops:  loops,
	}
}

// newLoopsEventForwarderWorkerForTest constructs a worker with an injectable
// cursor store. Used only by unit tests so they don't need a live Redis.
// Package-private so external callers must use NewLoopsEventForwarderWorker.
func newLoopsEventForwarderWorkerForTest(db *sql.DB, cursor loopsCursorStore, loops *loopsClient) *LoopsEventForwarderWorker {
	return &LoopsEventForwarderWorker{db: db, cursor: cursor, loops: loops}
}

// Work runs one sweep of audit_log → Loops.
//
// Returned error semantics match the surrounding workers (expire.go,
// quota_wall_nudge.go): a top-level DB or Redis failure returns an error
// so River retries the job; per-row failures are logged and the next row
// is processed. The cursor advances PER ROW after a successful send, so
// a mid-batch crash never re-sends rows that already made it to Loops.
func (w *LoopsEventForwarderWorker) Work(ctx context.Context, job *river.Job[LoopsEventForwarderArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.loops_event_forwarder")
	defer span.End()

	// Fail-open: no API key → log + exit clean. River doesn't retry, the
	// next tick fires the same path. Operators see one warning per minute
	// in logs which is loud enough to notice but quiet enough to be
	// filterable.
	if w.loops == nil {
		slog.Warn("jobs.loops_event_forwarder.disabled",
			"reason", "LOOPS_API_KEY not set — events will not be forwarded",
		)
		return nil
	}

	cursor, err := w.cursor.read(ctx)
	if err != nil {
		return fmt.Errorf("loops_event_forwarder: read cursor: %w", err)
	}

	rows, err := w.fetchBatch(ctx, cursor)
	if err != nil {
		return fmt.Errorf("loops_event_forwarder: fetch batch: %w", err)
	}

	if len(rows) == 0 {
		slog.Info("jobs.loops_event_forwarder.no_new_rows",
			"cursor_at", cursor.CreatedAt,
		)
		return nil
	}

	var sent, skipped, transient int
	// Labeled loop so a transient-halt break exits the for-range, not just
	// the switch case below — see the comment on the loopsResultTransient
	// branch. Without this label, a 5xx mid-batch would still try every
	// remaining row.
batchLoop:
	for _, row := range rows {
		// Build the payload for this row's kind. The mapping table
		// guarantees a builder exists for every kind in supportedAuditKinds
		// (TestLoops_AllSupportedKindsHaveBuilder enforces this), so a
		// missing builder here is a programming bug. We log and advance
		// the cursor so the queue doesn't stall.
		builder, ok := loopsEventBuilders[row.Kind]
		if !ok {
			slog.Error("jobs.loops_event_forwarder.no_builder_for_kind",
				"kind", row.Kind,
				"audit_id", row.ID,
				"note", "advancing cursor to avoid stall — fix the mapping table",
			)
			if advErr := w.cursor.write(ctx, loopsCursor{CreatedAt: row.CreatedAt, ID: row.ID}); advErr != nil {
				return fmt.Errorf("loops_event_forwarder: advance cursor after missing builder: %w", advErr)
			}
			skipped++
			continue
		}

		payload, payloadOK := builder(row)
		if !payloadOK {
			// Missing owner email or other required field. Advance the
			// cursor — a row that can't produce a valid payload now will
			// never be able to, and holding the cursor pins the queue.
			slog.Warn("jobs.loops_event_forwarder.builder_skipped_row",
				"kind", row.Kind,
				"audit_id", row.ID,
				"team_id", row.TeamID,
				"reason", "builder returned ok=false (likely no owner email)",
			)
			if advErr := w.cursor.write(ctx, loopsCursor{CreatedAt: row.CreatedAt, ID: row.ID}); advErr != nil {
				return fmt.Errorf("loops_event_forwarder: advance cursor after builder skip: %w", advErr)
			}
			skipped++
			continue
		}

		result := w.loops.sendEvent(ctx, payload)
		switch result {
		case loopsResultOK, loopsResultPermanent4xx:
			// Both advance the cursor. The 4xx case is already logged at
			// ERROR by the client so the operator can find the poisoned row.
			if advErr := w.cursor.write(ctx, loopsCursor{CreatedAt: row.CreatedAt, ID: row.ID}); advErr != nil {
				return fmt.Errorf("loops_event_forwarder: advance cursor: %w", advErr)
			}
			if result == loopsResultOK {
				sent++
			} else {
				skipped++
			}
		case loopsResultTransient:
			// DO NOT advance — retry next tick. We also bail out of the
			// rest of the batch because if Loops is throwing 5xx, the
			// remaining rows will hit the same wall. A labeled break is
			// required so we exit the for-range, not just the switch.
			slog.Warn("jobs.loops_event_forwarder.transient_halt",
				"kind", row.Kind,
				"audit_id", row.ID,
				"note", "halting batch — will retry next tick",
			)
			transient++
			break batchLoop
		}
	}

	slog.Info("jobs.loops_event_forwarder.completed",
		"sent", sent,
		"skipped", skipped,
		"transient", transient,
		"batch_size", len(rows),
	)
	return nil
}

// fetchBatch pulls the next loopsBatchLimit audit rows after the cursor whose
// kind matches the supported set. Joins users(team_id) to resolve the team's
// primary email for Loops.userId. The LEFT JOIN means rows without a
// registered email still surface — the builder returns ok=false and the
// forwarder advances past them.
//
// Cursor predicate: (created_at, id) > ($1, $2). On a fresh start (zero
// cursor) we pass the time.Time zero value + empty string, which sorts
// before every real row.
func (w *LoopsEventForwarderWorker) fetchBatch(ctx context.Context, c loopsCursor) ([]auditRow, error) {
	q := `
		SELECT
			a.id::text,
			a.team_id::text,
			a.kind,
			COALESCE(a.resource_type, ''),
			a.summary,
			a.metadata,
			a.created_at,
			COALESCE(u.email, '') AS owner_email
		FROM audit_log a
		LEFT JOIN LATERAL (
			SELECT email
			FROM users
			WHERE team_id = a.team_id
			ORDER BY created_at ASC
			LIMIT 1
		) u ON true
		WHERE a.kind = ANY($1::text[])
		  AND (a.created_at, a.id::text) > ($2, $3)
		ORDER BY a.created_at ASC, a.id::text ASC
		LIMIT $4
	`
	rows, err := w.db.QueryContext(ctx, q,
		pq.Array(supportedAuditKinds),
		c.CreatedAt,
		c.ID,
		loopsBatchLimit,
	)
	if err != nil {
		return nil, fmt.Errorf("fetchBatch query: %w", err)
	}
	defer rows.Close()

	var out []auditRow
	for rows.Next() {
		var r auditRow
		var metadata sql.NullString
		if err := rows.Scan(
			&r.ID, &r.TeamID, &r.Kind, &r.ResourceType,
			&r.Summary, &metadata, &r.CreatedAt, &r.OwnerEmail,
		); err != nil {
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
