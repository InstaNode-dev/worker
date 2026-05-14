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
func ConnectPostgres(databaseURL string) *sql.DB {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		panic(&ErrDBConnect{Cause: err})
	}

	maxOpen := envInt("WORKER_PG_MAX_OPEN_CONNS", 10)
	maxIdle := envInt("WORKER_PG_MAX_IDLE_CONNS", 5)
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(2 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		panic(&ErrDBConnect{Cause: err})
	}

	slog.Info("worker.db.postgres.connected",
		"max_open_conns", maxOpen,
		"max_idle_conns", maxIdle,
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
