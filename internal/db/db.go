package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

// envInt reads a positive integer from an env var, falling back to def.
// Bad values fall back too — worker must not refuse to start on a typo.
func envInt(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

// envDuration reads a Go time.Duration from an env var (e.g. "5m", "90s"),
// falling back to def. Bad values fall back too.
func envDuration(name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

// Pool-size defaults. Wave-3 chaos verify (2026-05-21) found api+worker
// combined exhausting the DigitalOcean Managed Postgres connection pool
// under a 50-concurrent /db/new burst — event_email_forwarder failed
// with "remaining connection slots are reserved for non-replication
// superuser connections". Worker was at 10/5 with River + multiple
// reconciler jobs each occasionally grabbing a connection. Lowering
// the default ceiling buys headroom for api's burst against the same
// upstream; the operator can raise via env when DO Managed Postgres
// is bumped.
const (
	defaultWorkerPGMaxOpenConns = 8
	defaultWorkerPGMaxIdleConns = 3
	defaultWorkerPGConnMaxLife  = 4 * time.Minute
	defaultWorkerPGConnMaxIdle  = 90 * time.Second
)

// ErrDBConnect is returned when the Postgres connection cannot be established.
type ErrDBConnect struct {
	Cause error
}

func (e *ErrDBConnect) Error() string {
	return fmt.Sprintf("failed to connect to postgres: %v", e.Cause)
}

func (e *ErrDBConnect) Unwrap() error { return e.Cause }

// ErrRedisConnect is returned when the Redis connection cannot be established.
type ErrRedisConnect struct {
	Cause error
}

func (e *ErrRedisConnect) Error() string {
	return fmt.Sprintf("failed to connect to redis: %v", e.Cause)
}

func (e *ErrRedisConnect) Unwrap() error { return e.Cause }

// ConnectPostgres creates and verifies a *sql.DB connection pool using the lib/pq driver.
// It panics if the connection cannot be established.
//
// Pool sizing is tunable via env so the operator can raise the ceiling
// without a redeploy the moment the DO Managed Postgres tier is bumped:
//
//	WORKER_PG_MAX_OPEN_CONNS   (default 8)  — per-replica hard ceiling
//	WORKER_PG_MAX_IDLE_CONNS   (default 3)
//	WORKER_PG_CONN_MAX_LIFETIME (default 4m) — Go time.Duration
//	WORKER_PG_CONN_MAX_IDLE_TIME (default 90s)
func ConnectPostgres(databaseURL string) *sql.DB {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		panic(&ErrDBConnect{Cause: err})
	}

	maxOpen := envInt("WORKER_PG_MAX_OPEN_CONNS", defaultWorkerPGMaxOpenConns)
	maxIdle := envInt("WORKER_PG_MAX_IDLE_CONNS", defaultWorkerPGMaxIdleConns)
	connLife := envDuration("WORKER_PG_CONN_MAX_LIFETIME", defaultWorkerPGConnMaxLife)
	connIdle := envDuration("WORKER_PG_CONN_MAX_IDLE_TIME", defaultWorkerPGConnMaxIdle)

	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(connLife)
	db.SetConnMaxIdleTime(connIdle)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		panic(&ErrDBConnect{Cause: err})
	}

	slog.Info("worker.db.postgres.connected",
		"max_open_conns", maxOpen,
		"max_idle_conns", maxIdle,
		"conn_max_lifetime", connLife.String(),
		"conn_max_idle_time", connIdle.String(),
	)
	return db
}

// ConnectRedis creates and verifies a Redis client. Panics on failure.
func ConnectRedis(redisURL string) *redis.Client {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		panic(&ErrRedisConnect{Cause: fmt.Errorf("invalid redis URL: %w", err)})
	}

	opts.PoolSize = 10
	opts.MinIdleConns = 2
	opts.ConnMaxLifetime = 5 * time.Minute
	opts.ConnMaxIdleTime = 2 * time.Minute

	rdb := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(&ErrRedisConnect{Cause: err})
	}

	slog.Info("worker.db.redis.connected", "addr", opts.Addr, "pool_size", opts.PoolSize)
	return rdb
}
