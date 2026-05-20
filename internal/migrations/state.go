// Package migrations exposes the platform DB's migration-tracking state to
// the worker's /healthz handler. Mirror of api/internal/migrations — kept
// independent so the worker doesn't import the api module (the api repo
// is a separate Go module). The source of truth is the schema_migrations
// table created by api migration 022_schema_migrations.sql and populated
// by the api binary's RunMigrations on every deploy.
//
// This package is READ-ONLY — the worker never writes schema_migrations.
//
// B14-F9 (BugBash 2026-05-20): /healthz JSON shape diverged across
// services — api emitted `migration_version`/`migration_count`/`migration_status`
// but worker emitted only ok/service/commit_id/build_time/version. A
// monitoring contract that requires a uniform health shape across the
// fleet (e.g. a single "all-services migration-aware" NR alert) couldn't
// fire — every probe had to know per-service shape. This package gives
// the worker the same three fields so the shape lines up.
//
// Caching: GET /healthz on the worker fires per liveness probe (~1/s/pod).
// A naïve "query the DB on every probe" would put one extra row read per
// second per pod on the platform DB. We cache the (filename, count) pair
// for cacheTTL (60s) per process — staleness window after an api migration
// is one minute, which is shorter than any meaningful operator alarm.
//
// Failure mode: when the DB is unreachable or schema_migrations is
// somehow missing, Get returns (StatusUnknown, "", 0). The /healthz
// handler converts that into migration_status: "unknown" while still
// returning 200 OK — worker liveness is independent of platform DB
// reachability (worker keeps River pumping; tracking read just lost
// signal).
package migrations

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// Status values surfaced on the /healthz response. Wire-stable strings —
// must match the api package's identical enum so a single NR alert can
// fire on `migration_status="unknown"` across both services.
const (
	StatusOK      = "ok"
	StatusUnknown = "unknown"
)

// defaultTTL is the per-process cache window. Same value as api (60s).
const defaultTTL = 60 * time.Second

// queryTimeout bounds the DB read so /healthz never stalls on a slow DB.
// Same value as api (2s).
const queryTimeout = 2 * time.Second

// State is the public-facing snapshot the /healthz handler emits. Wire
// shape is identical to api's migrations.State.
type State struct {
	Status   string // "ok" or "unknown"
	Filename string // highest-applied migration filename; "" when unknown
	Count    int    // total rows in schema_migrations; 0 when unknown
}

// Reader caches one State per process with a TTL. Safe for concurrent use.
// Clock is injectable so tests can advance time without sleeping.
type Reader struct {
	db    *sql.DB
	ttl   time.Duration
	clock func() time.Time

	mu      sync.Mutex
	cached  State
	expires time.Time
}

// NewReader builds a Reader backed by db. ttl <= 0 means use defaultTTL.
// clock nil means time.Now.
func NewReader(db *sql.DB, ttl time.Duration, clock func() time.Time) *Reader {
	if ttl <= 0 {
		ttl = defaultTTL
	}
	if clock == nil {
		clock = time.Now
	}
	return &Reader{db: db, ttl: ttl, clock: clock}
}

// Get returns the cached State, refreshing from the DB if the TTL has
// elapsed. On DB error returns StatusUnknown — the caller always gets a
// usable State.
//
// Mirrors the api package's lock discipline: the mutex is NEVER held
// across the DB call. A short window of N concurrent refreshes during a
// TTL expiry is acceptable (each probe is independent and the result is
// idempotent) and far cheaper than serializing every probe behind one
// lock.
func (r *Reader) Get(ctx context.Context) State {
	now := r.clock()

	// Fast path: serve the cached value under the lock if still fresh.
	r.mu.Lock()
	if !r.expires.IsZero() && now.Before(r.expires) {
		cached := r.cached
		r.mu.Unlock()
		return cached
	}
	r.mu.Unlock()

	// Refresh: DB IO happens WITHOUT the lock held.
	s, err := queryState(ctx, r.db)

	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		// DB unreachable / schema_migrations missing. Surface "unknown"
		// but keep the TTL — we don't want to hammer a sick DB on every
		// /healthz hit.
		r.cached = State{Status: StatusUnknown}
		r.expires = r.clock().Add(r.ttl)
		return r.cached
	}
	r.cached = s
	r.expires = r.clock().Add(r.ttl)
	return r.cached
}

// queryState reads the highest-filename row and the total count.
func queryState(ctx context.Context, db *sql.DB) (State, error) {
	if db == nil {
		return State{Status: StatusUnknown}, sql.ErrConnDone
	}

	qctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	var filename sql.NullString
	if err := db.QueryRowContext(qctx,
		`SELECT filename FROM schema_migrations ORDER BY filename DESC LIMIT 1`,
	).Scan(&filename); err != nil && err != sql.ErrNoRows {
		return State{Status: StatusUnknown}, err
	}

	var count int
	if err := db.QueryRowContext(qctx,
		`SELECT COUNT(*) FROM schema_migrations`,
	).Scan(&count); err != nil {
		return State{Status: StatusUnknown}, err
	}

	return State{
		Status:   StatusOK,
		Filename: filename.String,
		Count:    count,
	}, nil
}
