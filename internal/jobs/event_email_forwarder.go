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
	"errors"
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

// eventEmailMaxAge is the absolute age floor for fetchBatch (P1-2 fix,
// BugBash 2026-05-19). Any audit_log row older than this is considered
// stale and is never (re)forwarded — a lifecycle email that old has no
// value and re-firing it after a cursor reset is a mass-spam incident.
//
// 48h comfortably exceeds the forwarder's worst realistic backlog (the
// job runs every 60s and drains 100 rows/tick), so the floor never drops
// a row that legitimately should have been sent. Combined with seeding a
// missing cursor to now()-grace (see read()), a Redis wipe loses at most
// a few minutes of email instead of replaying all of audit_log history.
const eventEmailMaxAge = 48 * time.Hour

// eventEmailCursorSeedGrace is how far back a freshly-seeded cursor starts
// when Redis returns no value (a wipe / failover to an empty replica /
// first boot). We seed to now()-grace rather than the zero value so a
// cursor loss replays at most this much email instead of the entire
// audit_log tail. Small enough that almost nothing is re-sent, large
// enough to absorb the row that was in flight when Redis died.
const eventEmailCursorSeedGrace = 5 * time.Minute

// sentLedger is the worker-side idempotency seam (P1-3 fix, BugBash
// 2026-05-19). markSent attempts to claim an audit_id in the
// forwarder_sent table; it returns claimed=true when THIS call inserted
// the row and claimed=false when the row already existed (the audit_id
// was sent before — by an earlier tick, a pre-reset run, or a crash
// recovery). The forwarder skips the provider send when claimed=false.
//
// This makes the forwarder idempotent regardless of provider behavior:
// the Brevo X-Mailin-Custom header is NOT a delivery-dedup guarantee, so
// without this ledger every cursor reset / cursor_corrupt reset /
// crash-mid-batch re-sent real duplicate email.
//
// release un-claims an audit_id. It is called ONLY when a claim was made
// but the provider then returned a Transient failure (the cursor is not
// advanced, so the row will be retried next tick — and the retry must be
// able to re-claim). A claim followed by a confirmed 2xx / Permanent /
// SkippedNoTemplate is left in place: those advance the cursor and the
// ledger row is the permanent record that the audit_id is done.
type sentLedger interface {
	markSent(ctx context.Context, auditID string) (claimed bool, err error)
	release(ctx context.Context, auditID string) error
}

// sqlSentLedger is the production sentLedger backed by the forwarder_sent
// table in the platform Postgres (same DB as audit_log).
type sqlSentLedger struct {
	db *sql.DB
}

// markSent inserts (audit_id) ON CONFLICT DO NOTHING. RowsAffected==1
// means this call claimed the send; ==0 means the audit_id was already
// in the ledger and the send must be skipped.
func (l *sqlSentLedger) markSent(ctx context.Context, auditID string) (bool, error) {
	res, err := l.db.ExecContext(ctx, `
		INSERT INTO forwarder_sent (audit_id)
		VALUES ($1)
		ON CONFLICT (audit_id) DO NOTHING
	`, auditID)
	if err != nil {
		return false, fmt.Errorf("forwarder_sent insert: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("forwarder_sent rows affected: %w", err)
	}
	return n == 1, nil
}

// release deletes the audit_id row so a Transient-failed send can be
// re-claimed on the next tick.
func (l *sqlSentLedger) release(ctx context.Context, auditID string) error {
	if _, err := l.db.ExecContext(ctx, `
		DELETE FROM forwarder_sent WHERE audit_id = $1
	`, auditID); err != nil {
		return fmt.Errorf("forwarder_sent release: %w", err)
	}
	return nil
}

// noopSentLedger always claims the send — used by tests that don't care
// about the ledger path. Production NEVER uses this.
type noopSentLedger struct{}

func (noopSentLedger) markSent(context.Context, string) (bool, error) { return true, nil }
func (noopSentLedger) release(context.Context, string) error          { return nil }

// Suppression-related constants — mirrored from the api package's
// internal/models.email_events.go so the worker doesn't import the api
// module. Keep these values in sync across the two repos.
//
//	eventEmailSuppressionBounceDecay: how long a hard bounce / spam
//	complaint stays in the suppression set. After this window the
//	forwarder will attempt sends to the address again — bounces decay
//	because a previously-bouncing inbox may have been fixed.
//
//	Unsubscribes intentionally do NOT decay — see
//	suppressionUnsubscribeDecaysNever for the rationale.
const eventEmailSuppressionBounceDecay = 365 * 24 * time.Hour

// suppressionEventTypesDecaying is the set of event_type values that obey
// the eventEmailSuppressionBounceDecay window. Soft bounces deliberately
// omitted (transient — retry is the correct behavior).
var suppressionEventTypesDecaying = []string{"bounce", "spam_complaint"}

// suppressionEventTypeUnsubscribe is the SQL string for the unsubscribe
// event_type. Pulled to a constant so a typo in the suppression query
// can't silently keep nudging an unsubscribed user.
const suppressionEventTypeUnsubscribe = "unsubscribe"

// errUnsubscribeLookupFailed is returned by hasSuppression when the
// DB error occurred specifically in the UNSUBSCRIBE lookup (path 1).
//
// The fail posture is split by event class:
//
//   - Bounce / spam_complaint lookup failure → fail-OPEN. A duplicate
//     send to a bouncing inbox during a DB blip only costs sender
//     reputation; pinning the queue is worse.
//   - Unsubscribe lookup failure → fail-CLOSED. Emailing a user who
//     unsubscribed during a DB brownout is a CAN-SPAM / GDPR compliance
//     violation, not a reputation cost. We skip the send and DO NOT
//     advance the cursor, so the row is retried once the DB recovers.
//
// Work() distinguishes the two by checking errors.Is(err,
// errUnsubscribeLookupFailed).
var errUnsubscribeLookupFailed = errors.New("event_email_forwarder: unsubscribe suppression lookup failed (fail-closed)")

// suppressionChecker is the seam the forwarder uses to ask "should I
// skip this recipient?". Production wires a real Postgres-backed
// implementation (sqlSuppressionChecker below); tests supply an
// in-memory map so the forwarder spec stays hermetic.
//
// Returns (true, nil) when the recipient has a suppression row and
// (false, nil) when they don't. On a DB error the error is wrapped:
// an unsubscribe-lookup failure wraps errUnsubscribeLookupFailed (Work
// fails CLOSED — skips without advancing the cursor); a bounce/spam
// lookup failure is a plain error (Work fails OPEN — sends anyway).
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
	//
	// A DB error here wraps errUnsubscribeLookupFailed so Work() can
	// fail CLOSED — emailing an unsubscribed user during a DB brownout
	// is a CAN-SPAM / GDPR violation, not a recoverable reputation cost.
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
		return false, fmt.Errorf("hasSuppression unsubscribe: %w: %v", errUnsubscribeLookupFailed, err)
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
//
// read returns missing=true when the store has no persisted cursor (a
// fresh boot, a Redis wipe, a failover to an empty replica, or a corrupt
// blob that was discarded). Work uses that signal to seed the cursor to
// now()-grace rather than the zero value — see P1-2 (BugBash 2026-05-19).
type eventCursorStore interface {
	read(ctx context.Context) (c eventCursor, missing bool, err error)
	write(ctx context.Context, c eventCursor) error
}

// redisEventCursorStore is the production implementation of eventCursorStore.
// Backed by the platform Redis.
type redisEventCursorStore struct {
	rdb *redis.Client
}

func (s *redisEventCursorStore) read(ctx context.Context) (eventCursor, bool, error) {
	raw, err := s.rdb.Get(ctx, eventEmailCursorKey).Result()
	if err == redis.Nil {
		// No persisted cursor. missing=true tells Work to seed to
		// now()-grace instead of replaying audit_log history (P1-2).
		return eventCursor{}, true, nil
	}
	if err != nil {
		return eventCursor{}, false, fmt.Errorf("redis GET %s: %w", eventEmailCursorKey, err)
	}
	var c eventCursor
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		// Corrupt cursor — treat as missing. Log loudly so the operator
		// can investigate. missing=true → Work seeds to now()-grace; the
		// forwarder_sent ledger (P1-3) prevents any duplicate sends for
		// rows that were already forwarded, and the 48h fetchBatch floor
		// (P1-2) bounds the blast radius regardless.
		slog.Error("jobs.event_email_forwarder.cursor_corrupt",
			"raw", raw,
			"error", err,
			"note", "discarding corrupt cursor — reseeding to now()-grace; ledger + 48h floor prevent duplicate/stale sends",
		)
		return eventCursor{}, true, nil
	}
	return c, false, nil
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
	// ledger is the worker-side idempotency guard (P1-3). markSent is
	// called immediately before each provider send; a send is skipped
	// when the audit_id was already claimed. Production wires
	// sqlSentLedger; tests default to noopSentLedger.
	ledger sentLedger
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
		ledger:      &sqlSentLedger{db: db},
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
		ledger:      noopSentLedger{},
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

	cursor, missing, err := w.cursor.read(ctx)
	if err != nil {
		return fmt.Errorf("event_email_forwarder: read cursor: %w", err)
	}

	// P1-2 (BugBash 2026-05-19): a missing cursor (Redis wipe / failover to
	// an empty replica / corrupt blob / first boot) used to fall through as
	// the zero value, and fetchBatch then re-scanned the entire audit_log
	// history → mass-spam. Instead, seed the cursor to now()-grace so a
	// cursor loss replays at most a few minutes of email. The 48h fetchBatch
	// floor and the forwarder_sent ledger (P1-3) are the other two layers of
	// defense in depth — even with the seed, neither stale nor duplicate
	// rows can re-fire.
	if missing {
		seed := eventCursor{CreatedAt: time.Now().UTC().Add(-eventEmailCursorSeedGrace)}
		slog.Warn("jobs.event_email_forwarder.cursor_missing",
			"seeded_to", seed.CreatedAt,
			"grace", eventEmailCursorSeedGrace.String(),
			"note", "no persisted cursor — seeding to now()-grace instead of replaying audit_log history (P1-2)",
		)
		cursor = seed
	}

	rows, err := w.fetchBatch(ctx, cursor)
	if err != nil {
		return fmt.Errorf("event_email_forwarder: fetch batch: %w", err)
	}

	if len(rows) == 0 {
		// P1-1 (BugBash 2026-05-19): idle-tick log demoted INFO → DEBUG.
		// An idle sweep carries zero operational signal; emitting it at
		// INFO every 60s was ~1,440 noise lines/day. Liveness is covered
		// by jobs.middleware.work_ok. INFO is reserved for state changes.
		slog.Debug("jobs.event_email_forwarder.no_new_rows",
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

		// Resolve the recipient ONCE, up front. F5 (BugBash 2026-05-19):
		// the suppression check below MUST run against the exact address
		// the email is actually sent to. Previously suppression checked
		// row.OwnerEmail but the send used resolveRecipient(row) — for an
		// anonymous-tier row whose address lives in metadata.email the two
		// could diverge, letting an unsubscribed anon user still receive
		// the high-volume anon.expiry_warning / digest.weekly campaigns.
		recipient := resolveRecipient(row)

		params, payloadOK := builder(row)
		if !payloadOK {
			// No resolvable recipient (or other required field missing).
			// Advance the cursor — a row that can't produce a valid
			// payload now never will, and holding the cursor pins the
			// queue. P2-1 (BugBash 2026-05-19): a no-recipient row is an
			// EXPECTED, benign state (deleted / orphan / test teams), so
			// this logs at INFO, not WARN — a steady trickle of orphan
			// rows must not erode WARN's signal value.
			slog.Info("jobs.event_email_forwarder.builder_skipped_row",
				"kind", row.Kind,
				"audit_id", row.ID,
				"team_id", row.TeamID,
				"reason", "builder returned ok=false (no resolvable owner email — expected for deleted/orphan/test teams)",
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
		// sqlSuppressionChecker for the query shape.
		//
		// SPLIT FAIL POSTURE:
		//   - Unsubscribe lookup failure (errUnsubscribeLookupFailed) →
		//     fail-CLOSED. Skip the send and DO NOT advance the cursor, so
		//     the row is retried next tick once the DB recovers. Emailing
		//     an unsubscribed user during a DB brownout is a CAN-SPAM /
		//     GDPR compliance violation.
		//   - Bounce / spam_complaint lookup failure → fail-OPEN. A
		//     duplicate to a bouncing inbox only costs sender reputation;
		//     pinning the queue is worse.
		suppressed, supErr := w.suppression.hasSuppression(ctx, recipient)
		if supErr != nil && errors.Is(supErr, errUnsubscribeLookupFailed) {
			// Fail-CLOSED: can't prove the recipient is NOT unsubscribed.
			// Skip without advancing the cursor — the row retries when the
			// DB recovers. This intentionally halts forward progress on
			// this row rather than risk an illegal send.
			slog.Warn("jobs.event_email_forwarder.unsubscribe_check_failed_failclosed",
				"audit_id", row.ID,
				"kind", row.Kind,
				"error", supErr,
				"note", "fail-closed: skipping send, cursor NOT advanced — retries when DB recovers",
			)
			skipped++
			continue
		}
		if supErr != nil {
			// Bounce/spam lookup failure — fail-OPEN: treat as "not
			// suppressed" so a transient DB blip doesn't pin the queue.
			slog.Warn("jobs.event_email_forwarder.suppression_check_failed",
				"audit_id", row.ID,
				"error", supErr,
				"note", "fail-open: bounce/spam lookup error — continuing as if recipient is sendable",
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

		// F4 (BugBash 2026-05-19): a kind that has a builder but NO
		// registered renderer used to fall through to the dead Brevo
		// dashboard-template path → SkippedNoTemplate → cursor advanced
		// silently → zero email, zero error, audit row consumed forever.
		// AS OF 2026-05-15 every kind IS Go-rendered, so a missing
		// renderer is now unambiguously a programming bug (a 19th kind
		// added to eventEmailBuilders without a renderer). Treat it as a
		// loud ERROR and HOLD the cursor — never advance silently. The
		// TestEveryEmailKindHasAGoRenderer registry test catches this at
		// CI time; this is the runtime backstop if it ever ships anyway.
		renderer, hasRenderer := eventEmailBodyRenderers[row.Kind]
		if !hasRenderer {
			slog.Error("jobs.event_email_forwarder.missing_renderer",
				"kind", row.Kind,
				"audit_id", row.ID,
				"note", "kind has a builder but no Go renderer — holding cursor (NOT advancing). This is a registry bug; add a renderer to eventEmailBodyRenderers. Email NOT sent.",
			)
			transient++
			break batchLoop
		}

		// recipient was resolved once at the top of the loop (F5) — the
		// same address the suppression check ran against.
		evt := email.EventEmail{
			Kind:           row.Kind,
			Recipient:      recipient,
			RecipientName:  "", // we don't store a display name today
			Params:         params,
			IdempotencyKey: eventEmailIdempotencyPrefix + row.ID,
		}
		// Per-kind Go-rendered body — the provider takes the raw-HTML path
		// (no dashboard template lookup). Every supported kind has a
		// renderer (asserted above + by TestEveryEmailKindHasAGoRenderer).
		subject, htmlBody, textBody := renderer(params)
		evt.Subject = subject
		evt.HTMLBody = htmlBody
		evt.TextBody = textBody

		// P1-3 (BugBash 2026-05-19): claim the audit_id in the
		// forwarder_sent ledger BEFORE the send. markSent inserts ON
		// CONFLICT DO NOTHING — claimed=false means this audit_id was
		// already forwarded (by an earlier tick, a pre-cursor-reset run,
		// or a crash recovery), so we skip the provider POST and just
		// advance the cursor. This is the real idempotency guarantee:
		// the Brevo X-Mailin-Custom header is NOT a delivery dedup, so
		// without this ledger a cursor reset re-sent every email.
		//
		// A ledger DB error is treated as Transient (hold the cursor):
		// we cannot prove the row was not already sent, and sending into
		// an unknown state risks a duplicate — better to retry next tick.
		claimed, ledgerErr := w.ledger.markSent(ctx, row.ID)
		if ledgerErr != nil {
			slog.Warn("jobs.event_email_forwarder.ledger_error",
				"audit_id", row.ID,
				"kind", row.Kind,
				"error", ledgerErr,
				"note", "forwarder_sent claim failed — holding cursor, retry next tick (cannot prove not-already-sent)",
			)
			transient++
			break batchLoop
		}
		if !claimed {
			// Already in the ledger — duplicate-suppressed. Advance the
			// cursor (the row is genuinely done) without re-sending.
			slog.Info("jobs.event_email_forwarder.duplicate_suppressed",
				"audit_id", row.ID,
				"kind", row.Kind,
				"note", "audit_id already in forwarder_sent ledger — skipping re-send (cursor reset / crash recovery)",
			)
			if advErr := w.cursor.write(ctx, eventCursor{CreatedAt: row.CreatedAt, ID: row.ID}); advErr != nil {
				return fmt.Errorf("event_email_forwarder: advance cursor after duplicate suppression: %w", advErr)
			}
			skipped++
			continue
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
			//
			// P1-3: the send did NOT succeed, so RELEASE the ledger claim
			// — otherwise the retry next tick would see claimed=false and
			// skip the row forever (a never-sent email). A release error
			// is logged but not fatal: worst case the row is skipped, and
			// that is strictly safer than a re-send.
			if relErr := w.ledger.release(ctx, row.ID); relErr != nil {
				slog.Error("jobs.event_email_forwarder.ledger_release_failed",
					"audit_id", row.ID,
					"kind", row.Kind,
					"error", relErr,
					"note", "could not un-claim forwarder_sent after transient send — row may be skipped on retry",
				)
			}
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
// whose kind matches the supported set. Resolves the recipient email for the
// EventEmail. The recipient is COALESCEd in priority order:
//
//	1. u.email          — the team's primary user row (is_primary = true).
//	2. a.metadata->>'email' — W3 (P1-W3-10): anonymous teams have NO users
//	   row, so the LEFT JOIN yields NULL. The producers of the highest-volume
//	   free-funnel emails (anon.expiry_warning, digest.weekly, …) stash the
//	   recipient address in the audit row's metadata. Without this fallback
//	   every anonymous-tier email was structurally undeliverable — the
//	   builder saw an empty OwnerEmail and the forwarder advanced past it.
//	3. ''               — genuinely no recipient; the builder returns
//	   ok=false and the forwarder advances past the row.
//
// W4 (P1-W3-11): the users join is filtered to is_primary = true (migration
// 029 added the column for exactly this). The ORDER BY created_at ASC is
// kept only as a legacy tiebreaker for rows predating the is_primary
// backfill — without the is_primary filter a team with multiple users could
// send the lifecycle email to whoever happened to sign up first.
//
// Cursor predicate: (created_at, id) > ($1, $2). On a fresh start (zero
// cursor) we pass the time.Time zero value + empty string, which sorts
// before every real row.
func (w *EventEmailForwarderWorker) fetchBatch(ctx context.Context, c eventCursor) ([]auditRow, error) {
	// P1-2 (BugBash 2026-05-19): the `a.created_at > $5` floor caps the
	// blast radius of a cursor reset. A Redis wipe / corrupt cursor used
	// to drop the watermark to the zero value, and this query — with no
	// age bound — then re-scanned and re-emailed the ENTIRE audit_log
	// history. The 48h floor means even a fully-lost cursor only ever
	// re-scans the last 48h; a lifecycle email older than that is stale
	// and must never re-fire. (The forwarder_sent ledger is the other
	// guard — it stops re-sends even inside the 48h window.)
	ageFloor := time.Now().UTC().Add(-eventEmailMaxAge)
	q := `
		SELECT
			a.id::text,
			a.team_id::text,
			a.kind,
			COALESCE(a.resource_type, ''),
			a.summary,
			a.metadata,
			a.created_at,
			COALESCE(u.email, a.metadata->>'email', '') AS owner_email
		FROM audit_log a
		LEFT JOIN LATERAL (
			SELECT email
			FROM users
			WHERE team_id = a.team_id
			  AND is_primary = true
			ORDER BY created_at ASC
			LIMIT 1
		) u ON true
		WHERE a.kind = ANY($1::text[])
		  AND (a.created_at, a.id::text) > ($2, $3)
		  AND a.created_at > $5
		ORDER BY a.created_at ASC, a.id::text ASC
		LIMIT $4
	`
	rows, err := w.db.QueryContext(ctx, q,
		pq.Array(supportedAuditKinds),
		c.CreatedAt,
		c.ID,
		eventEmailBatchLimit,
		ageFloor,
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
