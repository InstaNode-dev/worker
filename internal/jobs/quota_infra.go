package jobs

// quota_infra.go — Provider-side revoke/grant helpers for storage-quota suspension.
//
// These mirror the equivalent helpers in api/internal/handlers/resource.go
// (pauseProvider / resumeProvider and their sub-functions).  They are
// duplicated here because the worker module does not import the api and has
// no provisioner-side RPC for pause/resume — the provisioner exposes only
// Provision, Deprovision, StorageBytes, and Regrade.
//
// The worker DOES already import lib/pq, go-redis, and mongo-driver (see
// go.mod), so the revoke operations can be performed directly without a new
// gRPC contract.  All three functions are fail-open: a connectivity error
// is logged but does not block the status-row update, matching CLAUDE.md
// convention #1 ("Fail open on Redis errors").

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"

	goredis "github.com/redis/go-redis/v9"

	// lib/pq is already imported elsewhere in the package (event_email_forwarder.go,
	// deploy_notify_webhook.go) and registered as the "postgres" driver via its
	// blank-import init(). Importing it here would cause a duplicate blank-import
	// compile error, so we rely on database/sql.Open("postgres", …) being available
	// because pq is already registered by another file in this package.
	_ "github.com/lib/pq"
)

// ResourceInfraRevoker is a narrow interface for the worker's infra-revoke path.
// The production implementation is directResourceRevoker (below). Tests inject
// a mock so the quota worker can be tested without real DB/Redis/Mongo.
type ResourceInfraRevoker interface {
	// RevokeAccess disables connectivity to a resource at the infrastructure
	// layer (REVOKE CONNECT, ACL SETUSER off, revokeRolesFromUser).
	// Returns nil on success or when the resource type has no infra revoke
	// (queue/storage/webhook — a row status flip is the entire effect).
	// Logs and returns nil on connectivity failure (fail-open).
	RevokeAccess(ctx context.Context, resourceType, token, connectionURL string) error

	// GrantAccess re-enables connectivity (GRANT CONNECT, ACL SETUSER on,
	// grantRolesToUser). Same semantics as RevokeAccess.
	GrantAccess(ctx context.Context, resourceType, token, connectionURL string) error
}

// StatusOnly is the sentinel constant used as resourceType when a resource type
// has no provider-side effect for suspend/unsuspend and only the DB status row
// needs flipping (queue, storage, webhook).
const StatusOnly = "status_only"

// directResourceRevoker implements ResourceInfraRevoker using the customer
// database, Redis, and MongoDB admin credentials from the worker config.
// All three credential fields may be empty — when empty the corresponding
// revoke/grant is a no-op (fail-open, row flip still happens).
type directResourceRevoker struct {
	customerDatabaseURL string // admin DSN for shared Postgres cluster
	mongoAdminURI       string // admin URI for shared MongoDB cluster
	customerRedisURL    string // admin Redis URL for shared Redis cluster
}

// NewDirectResourceRevoker creates a directResourceRevoker from the given
// credential fields. Any field may be empty (fail-open for that resource type).
func NewDirectResourceRevoker(customerDatabaseURL, mongoAdminURI, customerRedisURL string) ResourceInfraRevoker {
	return &directResourceRevoker{
		customerDatabaseURL: customerDatabaseURL,
		mongoAdminURI:       mongoAdminURI,
		customerRedisURL:    customerRedisURL,
	}
}

// RevokeAccess implements ResourceInfraRevoker.
func (r *directResourceRevoker) RevokeAccess(ctx context.Context, resourceType, token, _ string) error {
	switch resourceType {
	case "postgres":
		return r.revokePostgres(ctx, token)
	case "redis":
		return r.revokeRedis(ctx, token)
	case "mongodb":
		return r.revokeMongo(ctx, token)
	default:
		// queue / storage / webhook: no infra revoke needed; status flip is the
		// entire suspend for these resource types.
		return nil
	}
}

// GrantAccess implements ResourceInfraRevoker.
func (r *directResourceRevoker) GrantAccess(ctx context.Context, resourceType, token, _ string) error {
	switch resourceType {
	case "postgres":
		return r.grantPostgres(ctx, token)
	case "redis":
		return r.grantRedis(ctx, token)
	case "mongodb":
		return r.grantMongo(ctx, token)
	default:
		return nil
	}
}

// ── Postgres ──────────────────────────────────────────────────────────────────

func (r *directResourceRevoker) revokePostgres(ctx context.Context, token string) error {
	if r.customerDatabaseURL == "" {
		slog.Warn("quota_infra.revokePostgres: CUSTOMER_DATABASE_URL not set — skipping infra revoke",
			"token", token)
		return nil
	}
	dbName := "db_" + token
	username := "usr_" + token
	if err := validateSuspendIdent(dbName); err != nil {
		return fmt.Errorf("revokePostgres: db: %w", err)
	}
	if err := validateSuspendIdent(username); err != nil {
		return fmt.Errorf("revokePostgres: user: %w", err)
	}

	conn, err := sql.Open("postgres", r.customerDatabaseURL)
	if err != nil {
		slog.Warn("quota_infra.revokePostgres: open failed (fail-open)", "token", token, "error", err)
		return nil
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx,
		fmt.Sprintf(`REVOKE CONNECT ON DATABASE %q FROM %q`, dbName, username)); err != nil {
		slog.Warn("quota_infra.revokePostgres: REVOKE failed (fail-open)", "token", token, "error", err)
		return nil
	}
	// Terminate live sessions — non-fatal on failure (REVOKE already prevents new connections).
	if _, err := conn.ExecContext(ctx,
		`SELECT pg_terminate_backend(pid)
		   FROM pg_stat_activity
		  WHERE datname = $1 AND usename = $2 AND pid <> pg_backend_pid()`,
		dbName, username); err != nil {
		slog.Warn("quota_infra.revokePostgres: pg_terminate_backend failed (non-fatal)", "token", token, "error", err)
	}
	slog.Info("quota_infra.revokePostgres: revoked", "token", token, "db", dbName, "user", username)
	return nil
}

func (r *directResourceRevoker) grantPostgres(ctx context.Context, token string) error {
	if r.customerDatabaseURL == "" {
		slog.Warn("quota_infra.grantPostgres: CUSTOMER_DATABASE_URL not set — skipping infra grant",
			"token", token)
		return nil
	}
	dbName := "db_" + token
	username := "usr_" + token
	if err := validateSuspendIdent(dbName); err != nil {
		return fmt.Errorf("grantPostgres: db: %w", err)
	}
	if err := validateSuspendIdent(username); err != nil {
		return fmt.Errorf("grantPostgres: user: %w", err)
	}

	conn, err := sql.Open("postgres", r.customerDatabaseURL)
	if err != nil {
		slog.Warn("quota_infra.grantPostgres: open failed (fail-open)", "token", token, "error", err)
		return nil
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx,
		fmt.Sprintf(`GRANT CONNECT ON DATABASE %q TO %q`, dbName, username)); err != nil {
		slog.Warn("quota_infra.grantPostgres: GRANT failed (fail-open)", "token", token, "error", err)
		return nil
	}
	slog.Info("quota_infra.grantPostgres: granted", "token", token, "db", dbName, "user", username)
	return nil
}

// ── Redis ─────────────────────────────────────────────────────────────────────

// redisUsernameForToken returns the ACL username the provisioner assigns to a
// Redis resource. Matches provisioner/internal/backend/redis/local.go convention.
func redisUsernameForToken(token string) string {
	// Token length >= 8 is guaranteed by the provisioner; short-token guard is
	// belt-and-suspenders only.
	if len(token) < 8 {
		return "usr_" + token
	}
	return "usr_" + token[:8]
}

func (r *directResourceRevoker) revokeRedis(ctx context.Context, token string) error {
	if r.customerRedisURL == "" {
		slog.Warn("quota_infra.revokeRedis: CUSTOMER_REDIS_URL not set — skipping infra revoke",
			"token", token)
		return nil
	}
	username := redisUsernameForToken(token)
	return setCustomerRedisACL(ctx, r.customerRedisURL, username, false, token)
}

func (r *directResourceRevoker) grantRedis(ctx context.Context, token string) error {
	if r.customerRedisURL == "" {
		slog.Warn("quota_infra.grantRedis: CUSTOMER_REDIS_URL not set — skipping infra grant",
			"token", token)
		return nil
	}
	username := redisUsernameForToken(token)
	return setCustomerRedisACL(ctx, r.customerRedisURL, username, true, token)
}

func setCustomerRedisACL(ctx context.Context, adminURL, username string, enable bool, token string) error {
	opts, err := goredis.ParseURL(adminURL)
	if err != nil {
		slog.Warn("quota_infra.setCustomerRedisACL: parse URL failed (fail-open)", "token", token, "error", err)
		return nil
	}
	client := goredis.NewClient(opts)
	defer client.Close()
	state := "off"
	if enable {
		state = "on"
	}
	if err := client.Do(ctx, "ACL", "SETUSER", username, state).Err(); err != nil {
		slog.Warn("quota_infra.setCustomerRedisACL: ACL SETUSER failed (fail-open)",
			"token", token, "username", username, "state", state, "error", err)
		return nil
	}
	action := "revoked"
	if enable {
		action = "granted"
	}
	slog.Info("quota_infra.setCustomerRedisACL: "+action, "token", token, "username", username)
	return nil
}

// ── MongoDB ───────────────────────────────────────────────────────────────────

func (r *directResourceRevoker) revokeMongo(ctx context.Context, token string) error {
	if r.mongoAdminURI == "" {
		slog.Warn("quota_infra.revokeMongo: MONGO_ADMIN_URI not set — skipping infra revoke",
			"token", token)
		return nil
	}
	username := "usr_" + token
	dbName := "db_" + token
	return runMongoRoleOp(ctx, r.mongoAdminURI, username, dbName, false, token)
}

func (r *directResourceRevoker) grantMongo(ctx context.Context, token string) error {
	if r.mongoAdminURI == "" {
		slog.Warn("quota_infra.grantMongo: MONGO_ADMIN_URI not set — skipping infra grant",
			"token", token)
		return nil
	}
	username := "usr_" + token
	dbName := "db_" + token
	return runMongoRoleOp(ctx, r.mongoAdminURI, username, dbName, true, token)
}

func runMongoRoleOp(ctx context.Context, adminURI, username, dbName string, grant bool, token string) error {
	client, err := mongo.Connect(ctx, mongooptions.Client().ApplyURI(adminURI).
		SetServerSelectionTimeout(3*time.Second))
	if err != nil {
		slog.Warn("quota_infra.runMongoRoleOp: connect failed (fail-open)", "token", token, "error", err)
		return nil
	}
	defer func() {
		if discErr := client.Disconnect(ctx); discErr != nil {
			slog.Warn("quota_infra.runMongoRoleOp: disconnect", "token", token, "error", discErr)
		}
	}()

	op := "revokeRolesFromUser"
	if grant {
		op = "grantRolesToUser"
	}

	result := client.Database("admin").RunCommand(ctx, bson.D{
		{Key: op, Value: username},
		{Key: "roles", Value: bson.A{
			bson.D{
				{Key: "role", Value: "readWrite"},
				{Key: "db", Value: dbName},
			},
		}},
	})
	if result.Err() != nil {
		slog.Warn("quota_infra.runMongoRoleOp: command failed (fail-open)",
			"op", op, "token", token, "error", result.Err())
		return nil
	}
	action := "revoked"
	if grant {
		action = "granted"
	}
	slog.Info("quota_infra.runMongoRoleOp: "+action, "token", token, "user", username, "db", dbName)
	return nil
}

// ── Validation ────────────────────────────────────────────────────────────────

// validateSuspendIdent rejects identifiers that would allow SQL injection through
// the quoted-identifier form. Only [a-z0-9_-] are allowed — matching the charset
// the provisioner uses for db / user names (db_<token> / usr_<token>).
func validateSuspendIdent(s string) error {
	if s == "" {
		return fmt.Errorf("empty identifier")
	}
	for _, ch := range s {
		if !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-') {
			return fmt.Errorf("unsafe identifier %q", s)
		}
	}
	return nil
}
