package jobs

// event_email_forwarder.go — periodic job that drains audit_log rows
// into the configured email provider so customer-facing lifecycle email
// is event-driven instead of requiring a HubSpot/Sales-Hub subscription
// before we have revenue.
//
// PROVIDER AGNOSTIC: this file holds an email.EmailProvider and inspects
// only the typed SendError.Class on failure. It deliberately contains no
// provider identifiers — see internal/email/ for the seam definition and
// docs/email_providers.md for the rules. Swapping providers later = one
// new file under internal/email/ + one factory branch. Zero changes here.
//
// Cadence: every eventEmailForwarderInterval (60s). The cadence is conservative
// — provider events drive email campaigns and a one-minute delay between
// "user did the thing" and "the email fires" is well inside acceptable.
//
// Cursor: a (created_at, id) tuple stored in Redis under eventEmailCursorKey.
// We use a tuple, not a single id, because audit_log.id is UUID (NOT a
// bigserial — see migration 012_audit_log.sql) and UUID ordering is
// non-monotonic. The tuple gives us a stable, deterministic watermark.
//
// Idempotency: the cursor only advances after a confirmed success (or a
// permanent / skipped error class — see internal/email/provider.go for the
// rationale). If the worker crashes mid-batch, the next start resumes from
// the last persisted cursor and re-sends only the unsent rows. The
// EventEmail.IdempotencyKey ("audit-<row-id>") flows through to whatever
// dedupe header the configured provider supports, so duplicate sends
// during a crash window don't fire two campaigns at the same user.
//
// Fail-open: the factory always returns a working EmailProvider (a no-op
// implementation when EMAIL_PROVIDER is unset). Work logs at INFO/DEBUG;
// no boot crash.

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

	"instant.dev/worker/internal/email"
)

// EventEmailForwarderArgs is the River job payload — no fields, runs as a sweep.
type EventEmailForwarderArgs struct{}

// Kind is the River worker key. NOTE: this is the *River queue worker* name,
// NOT a provider name — kept distinct from any provider identifier.
func (EventEmailForwarderArgs) Kind() string { return "event_email_forwarder" }

// eventEmailForwarderInterval is how often the periodic job fires. 60s is a
// balance between "fresh enough that users don't notice the delay" and
// "low enough churn on Redis + audit_log".
const eventEmailForwarderInterval = 60 * time.Second

// eventEmailBatchLimit caps rows processed per tick. With 100 rows × ~250ms
// per provider POST = ~25s of work, well under the next 60s tick. A backlog
// drains over multiple ticks rather than blocking one giant transaction.
const eventEmailBatchLimit = 100

// eventEmailCursorKey is the Redis key holding the (created_at, id) cursor as
// a JSON blob. Single string key so it survives Redis restarts as long as
// the persistence policy allows; if Redis is wiped, the cursor resets and
// we re-process the audit_log tail (provider dedupe via IdempotencyKey
// absorbs the duplicates where supported).
const eventEmailCursorKey = "email:event_forwarder:last_audit_cursor"

// eventEmailIdempotencyPrefix is prepended to the audit row id to form the
// IdempotencyKey we hand the provider. The "audit-" prefix lets operators
// pattern-match dedupe headers in provider dashboards.
const eventEmailIdempotencyPrefix = "audit-"

// Suppression-related constants — mirrored from the api package's
// internal/models.email_events.go so the worker doesn't import the api
// module. Keep these values in sync across the two repos.
//
//   eventEmailSuppressionBounceDecay: how long a hard bounce / spam
//   complaint stays in the suppression set. After this window the
//   forwarder will attempt sends to the address again — bounces decay
//   because a previously-bouncing inbox may have been fixed.
//
//   Unsubscribes intentionally do NOT decay — see
//   suppressionUnsubscribeDecaysNever for the rationale.
const eventEmailSuppressionBounceDecay = 365 * 24 * time.Hour

// suppressionEventTypesDecaying is the set of event_type values that obey
// the eventEmailSuppressionBounceDecay window. Soft bounces deliberately
// omitted (transient — retry is the correct behavior).
var suppressionEventTypesDecaying = []string{"bounce", "spam_complaint"}

// suppressionEventTypeUnsubscribe is the SQL string for the unsubscribe
// event_type. Pulled to a constant so a typo in the suppression query
// can't silently keep nudging an unsubscribed user.
const suppressionEventTypeUnsubscribe = "unsubscribe"

// suppressionChecker is the seam the forwarder uses to ask "should I
// skip this recipient?". Production wires a real Postgres-backed
// implementation (sqlSuppressionChecker below); tests supply an
// in-memory map so the forwarder spec stays hermetic.
//
// Returns (true, nil) when the recipient has a suppression row, (false,
// nil) when they don't, and (false, err) on a DB error. The forwarder
// fail-OPENS on errors (treats them as "not suppressed") so a Redis or
// Postgres blip doesn't stall the queue. The downside — sending a
// duplicate to a bouncing inbox during a DB outage — is preferable to
// the alternative — pinning the queue behind a transient failure.
type suppressionChecker interface {
	hasSuppression(ctx context.Context, emailAddr string) (bool, error)
}

// sqlSuppressionChecker is the production implementation. Two queries
// fired in series — one for unsubscribes (no decay), one for bounces +
// spam complaints (365d decay). The composite index
// idx_email_events_email_type means each is a single range scan even
// when email_events grows to millions of rows.
type sqlSuppressionChecker struct {
	db *sql.DB
}

func (s *sqlSuppressionChecker) hasSuppression(ctx context.Context, emailAddr string) (bool, error) {
	if emailAddr == "" {
		return false, nil
	}

	// Path 1: unsubscribes. No decay window — once a user unsubscribes
	// we stay unsubscribed until they re-opt-in.
	var found int
	err := s.db.QueryRowContext(ctx, `
		SELECT 1
		FROM email_events
		WHERE email = $1 AND event_type = $2
		LIMIT 1
	`, emailAddr, suppressionEventTypeUnsubscribe).Scan(&found)
	if err == nil {
		return true, nil
	}
	if err != sql.ErrNoRows {
		return false, fmt.Errorf("hasSuppression unsubscribe: %w", err)
	}

	// Path 2: bounces + spam complaints with the 365d decay window.
	decayCutoff := time.Now().UTC().Add(-eventEmailSuppressionBounceDecay)
	err = s.db.QueryRowContext(ctx, `
		SELECT 1
		FROM email_events
		WHERE email = $1
		  AND event_type = ANY($2::text[])
		  AND created_at > $3
		LIMIT 1
	`, emailAddr, pq.Array(suppressionEventTypesDecaying), decayCutoff).Scan(&found)
	if err == nil {
		return true, nil
	}
	if err == sql.ErrNoRows {
		return false, nil
	}
	return false, fmt.Errorf("hasSuppression decay: %w", err)
}

// eventCursor is the watermark structure. CreatedAt + ID together give a
// strict total order even when multiple rows share a microsecond timestamp.
type eventCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

// zero returns the lowest-possible cursor — used when Redis has no value yet.
// time.Time's zero value sorts before every real audit row's created_at.
func (c eventCursor) zero() bool {
	return c.CreatedAt.IsZero() && c.ID == ""
}

// eventCursorStore abstracts the cursor read/write so tests can supply an
// in-memory implementation. Production uses redisEventCursorStore, which wraps
// a *redis.Client. Single-method-per-direction surface keeps the seam tiny.
type eventCursorStore interface {
	read(ctx context.Context) (eventCursor, error)
	write(ctx context.Context, c eventCursor) error
}

// redisEventCursorStore is the production implementation of eventCursorStore.
// Backed by the platform Redis.
type redisEventCursorStore struct {
	rdb *redis.Client
}

func (s *redisEventCursorStore) read(ctx context.Context) (eventCursor, error) {
	raw, err := s.rdb.Get(ctx, eventEmailCursorKey).Result()
	if err == redis.Nil {
		return eventCursor{}, nil
	}
	if err != nil {
		return eventCursor{}, fmt.Errorf("redis GET %s: %w", eventEmailCursorKey, err)
	}
	var c eventCursor
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		// Corrupt cursor — start over. Log loudly so the operator can
		// investigate (provider dedupe absorbs duplicates).
		slog.Error("jobs.event_email_forwarder.cursor_corrupt",
			"raw", raw,
			"error", err,
			"note", "resetting to zero — provider dedupe absorbs duplicates",
		)
		return eventCursor{}, nil
	}
	return c, nil
}

func (s *redisEventCursorStore) write(ctx context.Context, c eventCursor) error {
	b, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshal cursor: %w", err)
	}
	if err := s.rdb.Set(ctx, eventEmailCursorKey, string(b), 0).Err(); err != nil {
		return fmt.Errorf("redis SET %s: %w", eventEmailCursorKey, err)
	}
	return nil
}

// EventEmailForwarderWorker is the River worker. db is the platform Postgres,
// cursor is the watermark store (Redis in production), and provider is the
// configured email provider (NoopProvider when EMAIL_PROVIDER is unset).
// suppression decides "should we skip this recipient because they bounced
// or unsubscribed?" — fail-open on DB errors (see suppressionChecker doc).
type EventEmailForwarderWorker struct {
	river.WorkerDefaults[EventEmailForwarderArgs]
	db          *sql.DB
	cursor      eventCursorStore
	provider    email.EmailProvider
	suppression suppressionChecker
}

// NewEventEmailForwarderWorker constructs the worker. provider MUST be
// non-nil — the factory in internal/email returns NoopProvider rather than
// nil when no email provider is configured, so this constructor has no
// fail-open branch. The suppression checker is wired to the same *sql.DB
// the forwarder reads audit_log from; email_events lives in the platform
// Postgres alongside audit_log, so one connection serves both queries.
func NewEventEmailForwarderWorker(db *sql.DB, rdb *redis.Client, provider email.EmailProvider) *EventEmailForwarderWorker {
	return &EventEmailForwarderWorker{
		db:          db,
		cursor:      &redisEventCursorStore{rdb: rdb},
		provider:    provider,
		suppression: &sqlSuppressionChecker{db: db},
	}
}

// newEventEmailForwarderWorkerForTest constructs a worker with an injectable
// cursor store. Used only by unit tests so they don't need a live Redis.
// Package-private so external callers must use NewEventEmailForwarderWorker.
//
// Suppression defaults to a permissive (always-false) checker so existing
// tests that don't care about the suppression path keep working unchanged.
// Tests that DO want to exercise suppression should set `w.suppression`
// directly after construction.
func newEventEmailForwarderWorkerForTest(db *sql.DB, cursor eventCursorStore, provider email.EmailProvider) *EventEmailForwarderWorker {
	return &EventEmailForwarderWorker{
		db:          db,
		cursor:      cursor,
		provider:    provider,
		suppression: noopSuppressionChecker{},
	}
}

// noopSuppressionChecker is the "everyone is sendable" stub used by tests
// that don't care about suppression. Production NEVER uses this — the
// production constructor wires sqlSuppressionChecker.
type noopSuppressionChecker struct{}

func (noopSuppressionChecker) hasSuppression(context.Context, string) (bool, error) {
	return false, nil
}

// Work runs one sweep of audit_log → email provider.
//
// Returned error semantics match the surrounding workers (expire.go,
// quota_wall_nudge.go): a top-level DB or Redis failure returns an error
// so River retries the job; per-row failures are logged and the next row
// is processed. The cursor advances PER ROW after a successful send, so
// a mid-batch crash never re-sends rows that already made it through.
func (w *EventEmailForwarderWorker) Work(ctx context.Context, job *river.Job[EventEmailForwarderArgs]) error {
	ctx, span := otel.Tracer("instant.dev/worker").Start(ctx, "job.event_email_forwarder")
	defer span.End()

	cursor, err := w.cursor.read(ctx)
	if err != nil {
		return fmt.Errorf("event_email_forwarder: read cursor: %w", err)
	}

	rows, err := w.fetchBatch(ctx, cursor)
	if err != nil {
		return fmt.Errorf("event_email_forwarder: fetch batch: %w", err)
	}

	if len(rows) == 0 {
		slog.Info("jobs.event_email_forwarder.no_new_rows",
			"cursor_at", cursor.CreatedAt,
			"provider", w.provider.Name(),
		)
		return nil
	}

	var sent, skipped, transient int
	// Labeled loop so a transient-halt break exits the for-range, not just
	// the switch case below. Without this label, a 5xx mid-batch would
	// still try every remaining row.
batchLoop:
	for _, row := range rows {
		// Build the per-kind params. The mapping table guarantees a builder
		// exists for every kind in supportedAuditKinds (the
		// TestEventEmail_AllSupportedKindsHaveBuilder test enforces this),
		// so a missing builder here is a programming bug. We log and
		// advance the cursor so the queue doesn't stall.
		builder, ok := eventEmailBuilders[row.Kind]
		if !ok {
			slog.Error("jobs.event_email_forwarder.no_builder_for_kind",
				"kind", row.Kind,
				"audit_id", row.ID,
				"note", "advancing cursor to avoid stall — fix the mapping table",
			)
			if advErr := w.cursor.write(ctx, eventCursor{CreatedAt: row.CreatedAt, ID: row.ID}); advErr != nil {
				return fmt.Errorf("event_email_forwarder: advance cursor after missing builder: %w", advErr)
			}
			skipped++
			continue
		}

		params, payloadOK := builder(row)
		if !payloadOK {
			// Missing owner email or other required field. Advance the
			// cursor — a row that can't produce a valid payload now will
			// never be able to, and holding the cursor pins the queue.
			slog.Warn("jobs.event_email_forwarder.builder_skipped_row",
				"kind", row.Kind,
				"audit_id", row.ID,
				"team_id", row.TeamID,
				"reason", "builder returned ok=false (likely no owner email)",
			)
			if advErr := w.cursor.write(ctx, eventCursor{CreatedAt: row.CreatedAt, ID: row.ID}); advErr != nil {
				return fmt.Errorf("event_email_forwarder: advance cursor after builder skip: %w", advErr)
			}
			skipped++
			continue
		}

		// Suppression check — before any send, verify the recipient hasn't
		// already told us "stop". Bounces decay after 365d (the inbox may
		// have been fixed), unsubscribes never decay. See
		// sqlSuppressionChecker for the query shape; on DB errors the
		// checker fails-OPEN so we don't pin the queue.
		suppressed, supErr := w.suppression.hasSuppression(ctx, row.OwnerEmail)
		if supErr != nil {
			// Log + treat as "not suppressed" — see fail-open rationale.
			slog.Warn("jobs.event_email_forwarder.suppression_check_failed",
				"audit_id", row.ID,
				"error", supErr,
				"note", "fail-open: continuing as if recipient is sendable",
			)
		}
		if suppressed {
			// Skip + advance cursor. Logged at INFO with NO recipient
			// address — operators can see counts in the
			// jobs.event_email_forwarder.completed summary at the bottom
			// of the sweep. The audit_id is enough for forensic lookup
			// without leaking the suppressed email into log streams.
			slog.Info("jobs.event_email_forwarder.recipient_suppressed",
				"audit_id", row.ID,
				"kind", row.Kind,
				"note", "skip — recipient has bounce/unsubscribe/spam_complaint in suppression window",
			)
			if advErr := w.cursor.write(ctx, eventCursor{CreatedAt: row.CreatedAt, ID: row.ID}); advErr != nil {
				return fmt.Errorf("event_email_forwarder: advance cursor after suppression: %w", advErr)
			}
			skipped++
			continue
		}

		evt := email.EventEmail{
			Kind:           row.Kind,
			Recipient:      row.OwnerEmail,
			RecipientName:  "", // we don't store a display name today
			Params:         params,
			IdempotencyKey: eventEmailIdempotencyPrefix + row.ID,
		}
		sendErr := w.provider.SendEvent(ctx, evt)
		class := email.ClassOf(sendErr)
		switch {
		case sendErr == nil:
			// Success — advance cursor.
			if advErr := w.cursor.write(ctx, eventCursor{CreatedAt: row.CreatedAt, ID: row.ID}); advErr != nil {
				return fmt.Errorf("event_email_forwarder: advance cursor: %w", advErr)
			}
			sent++
		case class == email.SendClassPermanent, class == email.SendClassSkippedNoTemplate:
			// Both advance the cursor. Permanent is logged at ERROR by the
			// provider itself; SkippedNoTemplate is a configuration choice
			// (this kind isn't wired up in the provider's template map)
			// and stays at INFO here so dashboards don't light up.
			slog.Info("jobs.event_email_forwarder.row_skipped",
				"kind", row.Kind,
				"audit_id", row.ID,
				"provider", w.provider.Name(),
				"class", class.String(),
				"error", sendErr,
			)
			if advErr := w.cursor.write(ctx, eventCursor{CreatedAt: row.CreatedAt, ID: row.ID}); advErr != nil {
				return fmt.Errorf("event_email_forwarder: advance cursor: %w", advErr)
			}
			skipped++
		case class == email.SendClassTransient:
			// DO NOT advance — retry next tick. We also bail out of the
			// rest of the batch because if the provider is throwing 5xx,
			// the remaining rows will hit the same wall. A labeled break
			// is required so we exit the for-range, not just the switch.
			slog.Warn("jobs.event_email_forwarder.transient_halt",
				"kind", row.Kind,
				"audit_id", row.ID,
				"provider", w.provider.Name(),
				"note", "halting batch — will retry next tick",
				"error", sendErr,
			)
			transient++
			break batchLoop
		}
	}

	slog.Info("jobs.event_email_forwarder.completed",
		"provider", w.provider.Name(),
		"sent", sent,
		"skipped", skipped,
		"transient", transient,
		"batch_size", len(rows),
	)
	return nil
}

// fetchBatch pulls the next eventEmailBatchLimit audit rows after the cursor
// whose kind matches the supported set. Joins users(team_id) to resolve the
// team's primary email for the EventEmail recipient. The LEFT JOIN means
// rows without a registered email still surface — the builder returns
// ok=false and the forwarder advances past them.
//
// Cursor predicate: (created_at, id) > ($1, $2). On a fresh start (zero
// cursor) we pass the time.Time zero value + empty string, which sorts
// before every real row.
func (w *EventEmailForwarderWorker) fetchBatch(ctx context.Context, c eventCursor) ([]auditRow, error) {
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
		eventEmailBatchLimit,
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
