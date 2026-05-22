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

	"instant.dev/worker/internal/logsafe"
)

// sqlOpen is the indirection point for database/sql.Open. Production binds it
// to the stdlib func; tests override it to exercise the open-error fail-open
// arms in revokePostgres / grantPostgres. With lib/pq those arms are otherwise
// unreachable — its Open is fully lazy and surfaces connection errors only on
// first use (the already-covered Exec path). Reset to sql.Open after override.
var sqlOpen = sql.Open

// validateIdent is the indirection point for validateSuspendIdent. Production
// binds it to the real validator; tests override it to drive the user-arm
// error return in revokePostgres / grantPostgres, which is otherwise
// unreachable because db_<token> and usr_<token> share the same token (so the
// db check always fails first for any token that would fail validation).
// Reset to validateSuspendIdent after override.
var validateIdent = validateSuspendIdent

// ResourceInfraRevoker is a narrow interface for the worker's infra-revoke path.
// The production implementation is directResourceRevoker (below). Tests inject
// a mock so the quota worker can be tested without real DB/Redis/Mongo.
type ResourceInfraRevoker interface {
	// RevokeAccess disables connectivity to a resource at the infrastructure
	// layer (REVOKE CONNECT, ACL SETUSER off, revokeRolesFromUser).
	// Returns nil on success or when the resource type has no infra revoke
	// (queue/storage/webhook — a row status flip is the entire effect).
	// Logs and returns nil on connectivity failure (fail-open).
	//
	// tier is required for redis: the Redis ACL username scheme differs
	// between the shared backend (usr_<full-token>) and the dedicated
	// backend (ded_<full-token>). providerResourceID carries the canonical
	// username the provisioner stamped on the resource row at provision time;
	// when non-empty it is used verbatim, never re-derived. See
	// redisUsernameForToken.
	RevokeAccess(ctx context.Context, resourceType, token, tier, providerResourceID string) error

	// GrantAccess re-enables connectivity (GRANT CONNECT, ACL SETUSER on,
	// grantRolesToUser). Same semantics as RevokeAccess.
	GrantAccess(ctx context.Context, resourceType, token, tier, providerResourceID string) error
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
func (r *directResourceRevoker) RevokeAccess(ctx context.Context, resourceType, token, tier, providerResourceID string) error {
	switch resourceType {
	case "postgres":
		return r.revokePostgres(ctx, token)
	case "redis":
		return r.revokeRedis(ctx, token, tier, providerResourceID)
	case "mongodb":
		return r.revokeMongo(ctx, token)
	default:
		// queue / storage / webhook: no infra revoke needed; status flip is the
		// entire suspend for these resource types.
		return nil
	}
}

// GrantAccess implements ResourceInfraRevoker.
func (r *directResourceRevoker) GrantAccess(ctx context.Context, resourceType, token, tier, providerResourceID string) error {
	switch resourceType {
	case "postgres":
		return r.grantPostgres(ctx, token)
	case "redis":
		return r.grantRedis(ctx, token, tier, providerResourceID)
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
			"token", logsafe.Token(token))
		return nil
	}
	dbName := "db_" + token
	username := "usr_" + token
	if err := validateIdent(dbName); err != nil {
		return fmt.Errorf("revokePostgres: db: %w", err)
	}
	if err := validateIdent(username); err != nil {
		return fmt.Errorf("revokePostgres: user: %w", err)
	}

	conn, err := sqlOpen("postgres", r.customerDatabaseURL)
	if err != nil {
		slog.Warn("quota_infra.revokePostgres: open failed (fail-open)", "token", logsafe.Token(token), "error", err)
		return nil
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx,
		fmt.Sprintf(`REVOKE CONNECT ON DATABASE %q FROM %q`, dbName, username)); err != nil {
		slog.Warn("quota_infra.revokePostgres: REVOKE failed (fail-open)", "token", logsafe.Token(token), "error", err)
		return nil
	}
	// Terminate live sessions — non-fatal on failure (REVOKE already prevents new connections).
	if _, err := conn.ExecContext(ctx,
		`SELECT pg_terminate_backend(pid)
		   FROM pg_stat_activity
		  WHERE datname = $1 AND usename = $2 AND pid <> pg_backend_pid()`,
		dbName, username); err != nil {
		slog.Warn("quota_infra.revokePostgres: pg_terminate_backend failed (non-fatal)", "token", logsafe.Token(token), "error", err)
	}
	slog.Info("quota_infra.revokePostgres: revoked", "token", logsafe.Token(token), "db", dbName, "user", username)
	return nil
}

func (r *directResourceRevoker) grantPostgres(ctx context.Context, token string) error {
	if r.customerDatabaseURL == "" {
		slog.Warn("quota_infra.grantPostgres: CUSTOMER_DATABASE_URL not set — skipping infra grant",
			"token", logsafe.Token(token))
		return nil
	}
	dbName := "db_" + token
	username := "usr_" + token
	if err := validateIdent(dbName); err != nil {
		return fmt.Errorf("grantPostgres: db: %w", err)
	}
	if err := validateIdent(username); err != nil {
		return fmt.Errorf("grantPostgres: user: %w", err)
	}

	conn, err := sqlOpen("postgres", r.customerDatabaseURL)
	if err != nil {
		slog.Warn("quota_infra.grantPostgres: open failed (fail-open)", "token", logsafe.Token(token), "error", err)
		return nil
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx,
		fmt.Sprintf(`GRANT CONNECT ON DATABASE %q TO %q`, dbName, username)); err != nil {
		slog.Warn("quota_infra.grantPostgres: GRANT failed (fail-open)", "token", logsafe.Token(token), "error", err)
		return nil
	}
	slog.Info("quota_infra.grantPostgres: granted", "token", logsafe.Token(token), "db", dbName, "user", username)
	return nil
}

// ── Redis ─────────────────────────────────────────────────────────────────────

// Redis ACL username schemes. These MUST match — byte-for-byte — the
// usernames the provisioner and api create at provision time, or the worker's
// quota-suspend `ACL SETUSER <user> off` targets a user that does not exist
// (a silent no-op: the row flips to 'suspended' but the customer keeps full
// Redis access). This is the token-truncation class of bug — see P1 in
// BUGHUNT-REPORT-2026-05-17-round2.md.
//
// SHARED backend (anonymous / free tiers — the shared `redis-provision` pod):
//
//	usr_<FULL-token>
//
//	Verified against:
//	  - provisioner/internal/backend/redis/local.go  — aclUserPrefix = "usr_",
//	    aclUsername(token) = aclUserPrefix + token  (the FULL token, P1-D fix).
//	  - api/internal/providers/cache/redis.go        — aclUsernamePrefix = "usr_",
//	    aclUsername(token) = aclUsernamePrefix + token  (the FULL token, P1-E fix).
//
// DEDICATED backend (paid tiers — a per-tenant k8s Redis pod):
//
//	ded_<FULL-token>          (current scheme, P1 round-2 token-truncation fix)
//	ded_<token[:8]>           (legacy scheme — rows provisioned before the fix)
//
//	Verified against:
//	  - provisioner/internal/backend/redis/dedident.go:
//	    dedicatedACLUsername(token) = "ded_" + token  (the FULL token).
//	  - the provisioner stamps that canonical username into
//	    ProviderResourceID, the api persists it on resources.provider_resource_id.
//
// The two schemes are distinguished by tier: anonymous/free live on the shared
// backend, every paid tier gets a dedicated pod. isSharedRedisTier (defined in
// quota_redis_eviction.go) is the canonical tier→backend classifier and is
// reused here so the two call sites can never drift.
//
// Username resolution is store-at-provision, never re-derive (matches the
// provisioner's dedident.go and api's poolident.go pattern): the worker uses
// the provider_resource_id value stamped on the resource row when present, and
// falls back to a token derivation only for legacy rows (provider_resource_id
// NULL/empty). For dedicated Redis the legacy derivation must reproduce the
// old ded_<token[:8]> form — that is what a pre-fix dedicated pod's ACL user
// is actually called.
const (
	sharedRedisACLUserPrefix    = "usr_" // shared backend: usr_<full-token>
	dedicatedRedisACLUserPrefix = "ded_" // dedicated backend: ded_<full-token>
	// dedicatedRedisLegacyTokenLen is the token-prefix length the PRE-FIX
	// dedicated provisioner backend used for its ACL username
	// (ded_<token[:8]>). Retained as a named constant ONLY so the worker can
	// reconstruct the legacy username for a dedicated-Redis resource row that
	// was provisioned before the token-truncation fix and therefore has an
	// empty provider_resource_id. New rows never use it.
	dedicatedRedisLegacyTokenLen = 8
)

// redisUsernameForToken returns the EXACT ACL username the quota-suspend ACL
// op must target for a Redis resource. It MUST be byte-for-byte identical to
// the provision-time username or the op is a silent no-op (see the scheme
// documentation above).
//
// Resolution order:
//  1. providerResourceID — the canonical username the provisioner stamped on
//     the resource row at provision time. Used verbatim when non-empty. This
//     is the path every resource provisioned after the token-truncation fix
//     takes; no re-derivation, no drift.
//  2. shared tier  → usr_<full-token>  (the shared backend never truncated).
//  3. dedicated tier, empty providerResourceID → ded_<token[:8]>, the LEGACY
//     truncated form, because a dedicated-Redis row with no stored identifier
//     was provisioned before the fix and its ACL user really is under the old
//     8-char name.
func redisUsernameForToken(token, tier, providerResourceID string) string {
	// (1) Stored canonical identifier wins — it is the exact provisioned name.
	if providerResourceID != "" {
		return providerResourceID
	}
	if isSharedRedisTier(tier) {
		// (2) Shared backend: full token, never truncated (P1-D / P1-E).
		return sharedRedisACLUserPrefix + token
	}
	// (3) Legacy dedicated row (no stored PRID): the ACL user is under the
	// old truncated ded_<token[:8]> name.
	short := token
	if len(short) > dedicatedRedisLegacyTokenLen {
		short = short[:dedicatedRedisLegacyTokenLen]
	}
	return dedicatedRedisACLUserPrefix + short
}

func (r *directResourceRevoker) revokeRedis(ctx context.Context, token, tier, providerResourceID string) error {
	if r.customerRedisURL == "" {
		slog.Warn("quota_infra.revokeRedis: CUSTOMER_REDIS_URL not set — skipping infra revoke",
			"token", logsafe.Token(token))
		return nil
	}
	username := redisUsernameForToken(token, tier, providerResourceID)
	return setCustomerRedisACL(ctx, r.customerRedisURL, username, false, token)
}

func (r *directResourceRevoker) grantRedis(ctx context.Context, token, tier, providerResourceID string) error {
	if r.customerRedisURL == "" {
		slog.Warn("quota_infra.grantRedis: CUSTOMER_REDIS_URL not set — skipping infra grant",
			"token", logsafe.Token(token))
		return nil
	}
	username := redisUsernameForToken(token, tier, providerResourceID)
	return setCustomerRedisACL(ctx, r.customerRedisURL, username, true, token)
}

func setCustomerRedisACL(ctx context.Context, adminURL, username string, enable bool, token string) error {
	opts, err := goredis.ParseURL(adminURL)
	if err != nil {
		slog.Warn("quota_infra.setCustomerRedisACL: parse URL failed (fail-open)", "token", logsafe.Token(token), "error", err)
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
			"token", logsafe.Token(token), "username", username, "state", state, "error", err)
		return nil
	}
	action := "revoked"
	if enable {
		action = "granted"
	}
	slog.Info("quota_infra.setCustomerRedisACL: "+action, "token", logsafe.Token(token), "username", username)
	return nil
}

// ── MongoDB ───────────────────────────────────────────────────────────────────

func (r *directResourceRevoker) revokeMongo(ctx context.Context, token string) error {
	if r.mongoAdminURI == "" {
		slog.Warn("quota_infra.revokeMongo: MONGO_ADMIN_URI not set — skipping infra revoke",
			"token", logsafe.Token(token))
		return nil
	}
	username := "usr_" + token
	dbName := "db_" + token
	return runMongoRoleOp(ctx, r.mongoAdminURI, username, dbName, false, token)
}

func (r *directResourceRevoker) grantMongo(ctx context.Context, token string) error {
	if r.mongoAdminURI == "" {
		slog.Warn("quota_infra.grantMongo: MONGO_ADMIN_URI not set — skipping infra grant",
			"token", logsafe.Token(token))
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
		slog.Warn("quota_infra.runMongoRoleOp: connect failed (fail-open)", "token", logsafe.Token(token), "error", err)
		return nil
	}
	defer func() {
		if discErr := client.Disconnect(ctx); discErr != nil {
			slog.Warn("quota_infra.runMongoRoleOp: disconnect", "token", logsafe.Token(token), "error", discErr)
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
			"op", op, "token", logsafe.Token(token), "error", result.Err())
		return nil
	}
	action := "revoked"
	if grant {
		action = "granted"
	}
	slog.Info("quota_infra.runMongoRoleOp: "+action, "token", logsafe.Token(token), "user", username, "db", dbName)
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
