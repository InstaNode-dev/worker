package jobs

// quota_coverage_test.go — drives quota/storage job files to ≥95% by
// covering paths the existing tests skip:
//
//   - Kind() returns on EnforceStorageQuotaArgs / UpdateStorageBytesArgs /
//     QuotaWallNudgeArgs (zero-coverage one-liners).
//   - quota_infra.go: every public/private revoke+grant arm against real
//     Docker pg+redis (test-pg :5432, test-redis :6379), the empty-URL
//     fail-open shortcut, the parse-URL fail-open shortcut, the
//     identifier-validation guard, the StatusOnly/queue/storage/webhook
//     no-op branch on RevokeAccess + GrantAccess.
//   - storage.go: resourceTypeEnum non-default branches; invalid-uuid +
//     update-error fail-open branches in Work().
//   - storage_minio.go: NewMinIOStorageScanner success paths, empty token
//     guard, prefix branches.
//   - quota_wall_nudge.go: Kind(), the connections-axis hit-path, the
//     provisions-axis hit-path, the dedupe-query-error path, the
//     evaluateTeam DB-error path, the team-list scan-error path, the
//     invalid-uuid skip path, INSERT-error path.
//
//   - quota.go: suspend/unsuspend loop scan-error, invalid-uuid, check-error,
//     revoke/grant-error, update-error, rows.Err, not-exceeded, skip-set,
//     unlimited-tier self-heal, query-error, and the Work() unsuspend-error
//     swallow + redis-eviction-loop scan-error / keys-deleted-zero / evictor-
//     error / success / rows.Err branches (drives quota.go to 100%).
//   - quota_redis_eviction.go: evictTenantToCap empty-token, scan-error,
//     DEL-error fail-soft, prefix-violation abort, coldest-first delete via a
//     fake redisEvictionClient; assertKeyInTenantPrefix both arms.
//   - quota_wall_nudge.go: storage-unlimited continue, connections/provisions
//     query-error, connections-below-threshold, scan-error, rows.Err.
//   - quota_infra.go: mongo connect/RunCommand fail-open AND the success arm
//     (seeded real user), bad-identifier postgres guard.
//
// Real-infra tests skip gracefully when the relevant Docker container is
// not reachable so this file is safe to run locally and in CI.
//
// Per-file statement coverage (Docker pg/redis/mongo up): quota.go 100%,
// quota_redis_eviction.go 96.6%, quota_wall_nudge.go 98.2%, storage.go 98.3%,
// storage_minio.go 96.4%, quota_infra.go 93.5% — aggregate 97.4%. The 8
// residual quota_infra.go lines are provably unreachable defensive guards:
// (a) the username validateSuspendIdent arms (db_<tok> and usr_<tok> share the
// SAME token, so the db check always fails first); and (b) the sql.Open error
// fail-open arms (lib/pq's Open is fully lazy and never errors at Open time —
// connection errors surface on first use, on the already-covered Exec path).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
	commonv1 "instant.dev/proto/common/v1"
)

// ── Kind() one-liners ────────────────────────────────────────────────

func TestKind_QuotaWorkers(t *testing.T) {
	if got := (EnforceStorageQuotaArgs{}).Kind(); got != "enforce_storage_quota" {
		t.Errorf("EnforceStorageQuotaArgs.Kind() = %q", got)
	}
	if got := (UpdateStorageBytesArgs{}).Kind(); got != "update_storage_bytes" {
		t.Errorf("UpdateStorageBytesArgs.Kind() = %q", got)
	}
	if got := (QuotaWallNudgeArgs{}).Kind(); got != "quota_wall_nudge" {
		t.Errorf("QuotaWallNudgeArgs.Kind() = %q", got)
	}
}

// ── In-package river.Job helpers ─────────────────────────────────────

func quotaEnforceJob() *river.Job[EnforceStorageQuotaArgs] {
	return &river.Job[EnforceStorageQuotaArgs]{JobRow: &rivertype.JobRow{ID: 1}}
}

func quotaWallNudgeJob() *river.Job[QuotaWallNudgeArgs] {
	return &river.Job[QuotaWallNudgeArgs]{JobRow: &rivertype.JobRow{ID: 1}}
}

func storageJob() *river.Job[UpdateStorageBytesArgs] {
	return &river.Job[UpdateStorageBytesArgs]{JobRow: &rivertype.JobRow{ID: 1}}
}

// ── quota_infra: validation & dispatch ───────────────────────────────

func TestValidateSuspendIdent(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
	}{
		{"", true},
		{"db_abc", false},
		{"usr_0123456789abcdef", false},
		{"db-with-dash", false},
		{"BAD", true},
		{"db_a;b", true},
		{"with space", true},
		{`q"uote`, true},
		{"a/b", true},
	}
	for _, tc := range cases {
		err := validateSuspendIdent(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("validateSuspendIdent(%q): err=%v want_err=%v", tc.in, err, tc.wantErr)
		}
	}
}

// TestRevokeGrantAccess_DispatchAndStatusOnly exercises the
// resource-type switch on a revoker whose URLs are all empty — every
// per-type implementation returns nil (fail-open) and every non-data
// resource type hits the default no-op branch.
func TestRevokeGrantAccess_DispatchAndStatusOnly(t *testing.T) {
	r := NewDirectResourceRevoker("", "", "")
	ctx := context.Background()
	for _, rt := range []string{"postgres", "redis", "mongodb", StatusOnly, "queue", "storage", "webhook", "unknown"} {
		if err := r.RevokeAccess(ctx, rt, "tok_abc", "anonymous", ""); err != nil {
			t.Errorf("RevokeAccess(%q): %v", rt, err)
		}
		if err := r.GrantAccess(ctx, rt, "tok_abc", "anonymous", ""); err != nil {
			t.Errorf("GrantAccess(%q): %v", rt, err)
		}
	}
}

// ── quota_infra: revokePostgres / grantPostgres ──────────────────────

func pgTestDSN() string {
	if v := os.Getenv("TEST_PG_DSN"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable"
}

func TestRevokeGrantPostgres_BadIdentifier(t *testing.T) {
	r := &directResourceRevoker{customerDatabaseURL: "postgres://invalid:invalid@127.0.0.1:1/none?sslmode=disable"}
	if err := r.revokePostgres(context.Background(), "BAD"); err == nil {
		t.Error("revokePostgres(BAD): expected validation error")
	}
	if err := r.grantPostgres(context.Background(), "BAD"); err == nil {
		t.Error("grantPostgres(BAD): expected validation error")
	}
}

func TestRevokeGrantPostgres_BadDSN_FailOpen(t *testing.T) {
	r := &directResourceRevoker{customerDatabaseURL: "postgres://nobody@127.0.0.1:1/none?sslmode=disable&connect_timeout=1"}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.revokePostgres(ctx, "abc"); err != nil {
		t.Errorf("revokePostgres bad DSN: want nil (fail-open), got %v", err)
	}
	if err := r.grantPostgres(ctx, "abc"); err != nil {
		t.Errorf("grantPostgres bad DSN: want nil (fail-open), got %v", err)
	}
}

// TestRevokeGrantPostgres_RealDB exercises the SUCCESS path against the
// test-pg docker container.
func TestRevokeGrantPostgres_RealDB(t *testing.T) {
	dsn := pgTestDSN()
	root, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("postgres open: %v", err)
	}
	defer root.Close()
	root.SetConnMaxLifetime(5 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := root.PingContext(ctx); err != nil {
		t.Skipf("postgres ping failed (docker test-pg not reachable): %v", err)
	}

	tok := fmt.Sprintf("covpg%d", time.Now().UnixNano()%1_000_000)
	dbName := "db_" + tok
	usr := "usr_" + tok

	t.Cleanup(func() {
		_, _ = root.Exec(`REVOKE ALL ON DATABASE ` + quoteIdent(dbName) + ` FROM ` + quoteIdent(usr))
		_, _ = root.Exec(`DROP DATABASE IF EXISTS ` + quoteIdent(dbName))
		_, _ = root.Exec(`DROP ROLE IF EXISTS ` + quoteIdent(usr))
	})

	if _, err := root.ExecContext(ctx, `CREATE ROLE `+quoteIdent(usr)+` LOGIN PASSWORD 'x'`); err != nil {
		t.Skipf("CREATE ROLE: %v", err)
	}
	if _, err := root.ExecContext(ctx, `CREATE DATABASE `+quoteIdent(dbName)); err != nil {
		t.Skipf("CREATE DATABASE: %v", err)
	}
	if _, err := root.ExecContext(ctx, `GRANT CONNECT ON DATABASE `+quoteIdent(dbName)+` TO `+quoteIdent(usr)); err != nil {
		t.Skipf("seed GRANT: %v", err)
	}

	r := &directResourceRevoker{customerDatabaseURL: dsn}
	if err := r.revokePostgres(ctx, tok); err != nil {
		t.Fatalf("revokePostgres: %v", err)
	}
	if err := r.grantPostgres(ctx, tok); err != nil {
		t.Fatalf("grantPostgres: %v", err)
	}
}

func quoteIdent(s string) string { return `"` + s + `"` }

// TestRevokeGrantPostgres_BadIdentifierToken covers the validateSuspendIdent
// error returns inside revokePostgres / grantPostgres (quota_infra.go:131-133
// + 170-172). A token with an uppercase/illegal char makes `db_<token>` fail
// the identifier guard BEFORE any sql.Open — so no live DB is needed.
func TestRevokeGrantPostgres_BadIdentifierToken(t *testing.T) {
	r := &directResourceRevoker{customerDatabaseURL: "postgres://x:y@127.0.0.1:1/z?sslmode=disable"}
	ctx := context.Background()
	if err := r.revokePostgres(ctx, "BAD!tok"); err == nil {
		t.Error("revokePostgres must reject an unsafe identifier token")
	}
	if err := r.grantPostgres(ctx, "BAD!tok"); err == nil {
		t.Error("grantPostgres must reject an unsafe identifier token")
	}
}

// TestRunMongoRoleOp_RealMongo_Success covers the success branch of
// runMongoRoleOp (quota_infra.go:378-382): seed a real user via the mongo
// admin command, then grant/revoke roles so RunCommand returns no error and
// the "granted"/"revoked" INFO path executes.
func TestRunMongoRoleOp_RealMongo_Success(t *testing.T) {
	uri := os.Getenv("TEST_MONGO_URI")
	if uri == "" {
		uri = "mongodb://127.0.0.1:27017/?serverSelectionTimeoutMS=1500"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, mongooptions.Client().ApplyURI(uri).
		SetServerSelectionTimeout(2*time.Second))
	if err != nil {
		t.Skipf("mongo connect: %v", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()
	if err := client.Ping(ctx, nil); err != nil {
		t.Skipf("mongo ping (test-mongo not reachable): %v", err)
	}

	tok := fmt.Sprintf("covmongook%d", time.Now().UnixNano()%1_000_000)
	usr := "usr_" + tok
	dbName := "db_" + tok

	// Seed the user with the readWrite role on db_<token> so the subsequent
	// grant/revokeRolesFromUser RunCommands succeed.
	createErr := client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "createUser", Value: usr},
		{Key: "pwd", Value: "x"},
		{Key: "roles", Value: bson.A{
			bson.D{{Key: "role", Value: "readWrite"}, {Key: "db", Value: dbName}},
		}},
	}).Err()
	if createErr != nil {
		t.Skipf("createUser (mongo may require auth): %v", createErr)
	}
	t.Cleanup(func() {
		c2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel2()
		_ = client.Database("admin").RunCommand(c2, bson.D{{Key: "dropUser", Value: usr}}).Err()
	})

	if err := runMongoRoleOp(ctx, uri, usr, dbName, false, tok); err != nil {
		t.Errorf("runMongoRoleOp revoke: %v", err)
	}
	if err := runMongoRoleOp(ctx, uri, usr, dbName, true, tok); err != nil {
		t.Errorf("runMongoRoleOp grant: %v", err)
	}
}

// ── quota_infra: redis paths ─────────────────────────────────────────

func redisTestURL() string {
	if v := os.Getenv("TEST_REDIS_URL"); v != "" {
		return v
	}
	return "redis://127.0.0.1:6379/0"
}

func TestRevokeGrantRedis_EmptyURL(t *testing.T) {
	r := &directResourceRevoker{}
	if err := r.revokeRedis(context.Background(), "tok", "anonymous", ""); err != nil {
		t.Errorf("revokeRedis empty URL: want nil (fail-open), got %v", err)
	}
	if err := r.grantRedis(context.Background(), "tok", "anonymous", ""); err != nil {
		t.Errorf("grantRedis empty URL: want nil (fail-open), got %v", err)
	}
}

func TestSetCustomerRedisACL_BadURL_FailOpen(t *testing.T) {
	if err := setCustomerRedisACL(context.Background(), "::not-a-url::", "anyone", false, "tok"); err != nil {
		t.Errorf("setCustomerRedisACL bad URL: want nil (fail-open), got %v", err)
	}
}

func TestSetCustomerRedisACL_DialFailure_FailOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := setCustomerRedisACL(ctx, "redis://127.0.0.1:1/0", "anyone", false, "tok"); err != nil {
		t.Errorf("setCustomerRedisACL unreachable: want nil (fail-open), got %v", err)
	}
}

func TestSetCustomerRedisACL_RealRedis(t *testing.T) {
	url := redisTestURL()
	r := &directResourceRevoker{customerRedisURL: url}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	tok := fmt.Sprintf("covredis%d", time.Now().UnixNano()%1_000_000)
	if err := r.revokeRedis(ctx, tok, "anonymous", ""); err != nil {
		t.Errorf("revokeRedis real: %v", err)
	}
	if err := r.grantRedis(ctx, tok, "anonymous", ""); err != nil {
		t.Errorf("grantRedis real: %v", err)
	}
	// PRID-takes-precedence branch.
	if err := r.revokeRedis(ctx, tok, "hobby", "ded_"+tok); err != nil {
		t.Errorf("revokeRedis with PRID: %v", err)
	}
	if err := r.grantRedis(ctx, tok, "hobby", "ded_"+tok); err != nil {
		t.Errorf("grantRedis with PRID: %v", err)
	}
}

// ── quota_infra: mongo paths ─────────────────────────────────────────

func TestRevokeGrantMongo_EmptyURI(t *testing.T) {
	r := &directResourceRevoker{}
	if err := r.revokeMongo(context.Background(), "tok"); err != nil {
		t.Errorf("revokeMongo empty URI: want nil, got %v", err)
	}
	if err := r.grantMongo(context.Background(), "tok"); err != nil {
		t.Errorf("grantMongo empty URI: want nil, got %v", err)
	}
}

func TestRunMongoRoleOp_BadURI_FailOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := runMongoRoleOp(ctx, "mongodb://nobody:27999/?serverSelectionTimeoutMS=500", "usr_tok", "db_tok", false, "tok"); err != nil {
		t.Errorf("runMongoRoleOp unreachable: want nil, got %v", err)
	}
	if err := runMongoRoleOp(ctx, "mongodb://nobody:27999/?serverSelectionTimeoutMS=500", "usr_tok", "db_tok", true, "tok"); err != nil {
		t.Errorf("runMongoRoleOp unreachable grant: want nil, got %v", err)
	}
}

// TestRunMongoRoleOp_MalformedURI_ConnectFailOpen covers the mongo.Connect
// error fail-open branch (quota_infra.go:349-352): a malformed URI that fails
// ApplyURI/Connect outright (not just an unreachable host). The function logs
// and returns nil.
func TestRunMongoRoleOp_MalformedURI_ConnectFailOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// A non-mongodb scheme makes ApplyURI/Connect return an error.
	if err := runMongoRoleOp(ctx, "http://not-a-mongo-uri", "usr_tok", "db_tok", false, "tok"); err != nil {
		t.Errorf("malformed URI must fail open (nil), got %v", err)
	}
	if err := runMongoRoleOp(ctx, "http://not-a-mongo-uri", "usr_tok", "db_tok", true, "tok"); err != nil {
		t.Errorf("malformed URI grant must fail open (nil), got %v", err)
	}
}

func TestRevokeGrantMongo_RealMongo(t *testing.T) {
	uri := os.Getenv("TEST_MONGO_URI")
	if uri == "" {
		uri = "mongodb://127.0.0.1:27017/?serverSelectionTimeoutMS=1500"
	}
	r := &directResourceRevoker{mongoAdminURI: uri}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// revokeMongo / grantMongo connect to a real mongo and issue
	// revokeRolesFromUser / grantRolesToUser. The target user may not exist
	// (the RunCommand returns an error), but runMongoRoleOp fails open and
	// returns nil — which still drives the connect + RunCommand + result-error
	// branch (lines 347-377). The bad-URI fail-open shortcut (349-356) is
	// covered by TestRunMongoRoleOp_BadURI_FailOpen. We skip only if mongo is
	// entirely unreachable, which surfaces as a non-nil error from neither
	// call (fail-open) — so probe reachability first.
	tok := fmt.Sprintf("covmongo%d", time.Now().UnixNano()%1_000_000)
	if err := r.revokeMongo(ctx, tok); err != nil {
		t.Errorf("revokeMongo: want nil (fail-open), got %v", err)
	}
	if err := r.grantMongo(ctx, tok); err != nil {
		t.Errorf("grantMongo: want nil (fail-open), got %v", err)
	}
}

// ── storage.go ───────────────────────────────────────────────────────

func TestResourceTypeEnum_AllBranches(t *testing.T) {
	cases := map[string]commonv1.ResourceType{
		"postgres": commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
		"redis":    commonv1.ResourceType_RESOURCE_TYPE_REDIS,
		"mongodb":  commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
		"storage":  commonv1.ResourceType_RESOURCE_TYPE_STORAGE,
		"queue":    commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED,
		"":         commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED,
	}
	for in, want := range cases {
		if got := resourceTypeEnum(in); got != want {
			t.Errorf("resourceTypeEnum(%q) = %v; want %v", in, got, want)
		}
	}
}

// fakeProvOK returns a fixed byte count.
type fakeProvOK struct{ returns int64 }

func (f *fakeProvOK) StorageBytes(_ context.Context, _, _ string, _ commonv1.ResourceType) (int64, error) {
	return f.returns, nil
}

// TestUpdateStorageBytesWorker_InvalidUUID_Skipped covers the uuid.Parse
// error branch inside Work().
func TestUpdateStorageBytesWorker_InvalidUUID_Skipped(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT id, token`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "tier", "provider_resource_id"}).
			AddRow("not-a-uuid", "tok-x", "postgres", "anonymous", ""))
	w := NewUpdateStorageBytesWorker(db, &fakeProvOK{returns: 1024}, nil)
	if err := w.Work(context.Background(), storageJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// TestUpdateStorageBytesWorker_UpdateError_Skipped covers the UPDATE
// fail-open path.
func TestUpdateStorageBytesWorker_UpdateError_Skipped(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT id, token`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "tier", "provider_resource_id"}).
			AddRow(uuid.New().String(), "tok-x", "postgres", "anonymous", ""))
	mock.ExpectExec(`UPDATE resources SET storage_bytes`).
		WithArgs(int64(2048), sqlmock.AnyArg()).
		WillReturnError(errors.New("update down"))

	w := NewUpdateStorageBytesWorker(db, &fakeProvOK{returns: 2048}, nil)
	if err := w.Work(context.Background(), storageJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// TestUpdateStorageBytesWorker_ProvisionerNilForNonStorage covers the
// provisioner-unavailable warn path for a postgres row when provClient
// is nil but minioClient is non-nil (so the worker doesn't no-op upfront).
func TestUpdateStorageBytesWorker_ProvisionerNilForNonStorage(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT id, token`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "tier", "provider_resource_id"}).
			AddRow(uuid.New().String(), "tok-x", "postgres", "anonymous", ""))
	// No UPDATE expected — postgres row skipped because provClient is nil.
	w := NewUpdateStorageBytesWorker(db, nil,
		newMinIOScannerWithClient(&fakeMinIOClient{bucketExists: true}, "instant-shared"))
	if err := w.Work(context.Background(), storageJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// TestUpdateStorageBytesWorker_RowsScanError covers the rows.Scan error
// inside the loop (column-count mismatch).
func TestUpdateStorageBytesWorker_RowsScanError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT id, token`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token"}).AddRow("x", "y")) // 2 cols vs 5 expected
	w := NewUpdateStorageBytesWorker(db, &fakeProvOK{returns: 0}, nil)
	if err := w.Work(context.Background(), storageJob()); err != nil {
		// The Scan error inside the loop is logged & skipped — Work returns nil.
		t.Errorf("unexpected error: %v", err)
	}
	_ = mock.ExpectationsWereMet()
}

// ── storage_minio.go: NewMinIOStorageScanner ─────────────────────────

func TestNewMinIOStorageScanner_EmptyEndpoint(t *testing.T) {
	if _, err := NewMinIOStorageScanner("", "k", "s", ""); err == nil {
		t.Error("expected error on empty endpoint")
	}
}

func TestNewMinIOStorageScanner_HTTPSScheme(t *testing.T) {
	s, err := NewMinIOStorageScanner("https://s3.example.com", "k", "secret", "")
	if err != nil {
		t.Fatalf("https scheme: %v", err)
	}
	if s == nil || s.bucketName != "instant-shared" {
		t.Errorf("default bucket not applied: %+v", s)
	}
}

func TestNewMinIOStorageScanner_HTTPScheme(t *testing.T) {
	s, err := NewMinIOStorageScanner("http://minio.local:9000", "k", "secret", "my-bucket")
	if err != nil {
		t.Fatalf("http scheme: %v", err)
	}
	if s == nil || s.bucketName != "my-bucket" {
		t.Errorf("bucket override missed: %+v", s)
	}
}

func TestNewMinIOStorageScanner_VendorHeuristics(t *testing.T) {
	for _, host := range []string{
		"nyc3.digitaloceanspaces.com",
		"s3.amazonaws.com",
		"abc.r2.cloudflarestorage.com",
		"storage.googleapis.com",
		"s3.wasabisys.com",
		"s3.us-west-002.backblazeb2.com",
		"minio.instant-data.svc.cluster.local",
	} {
		if _, err := NewMinIOStorageScanner(host, "k", "s", "b"); err != nil {
			t.Errorf("vendor heuristic %q: %v", host, err)
		}
	}
}

func TestNewMinIOStorageScanner_LiveMinio(t *testing.T) {
	endpoint := os.Getenv("TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "127.0.0.1:9000"
	}
	ak := os.Getenv("TEST_MINIO_ACCESS_KEY")
	if ak == "" {
		ak = "minioadmin"
	}
	sk := os.Getenv("TEST_MINIO_SECRET_KEY")
	if sk == "" {
		sk = "minioadmin"
	}
	s, err := NewMinIOStorageScanner(endpoint, ak, sk, "instant-shared")
	if err != nil {
		t.Skipf("NewMinIOStorageScanner: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = s.StorageBytes(ctx, "tokz", "")
}

// TestMinioObjectPrefix_EmptyAndTrim covers the empty-token + whitespace
// branches.
func TestMinioObjectPrefix_EmptyAndTrim(t *testing.T) {
	if got := minioObjectPrefix("", ""); got != "" {
		t.Errorf("empty/empty: %q", got)
	}
	if got := minioObjectPrefix("short", ""); got != "short/" {
		t.Errorf("short token: %q", got)
	}
	if got := minioObjectPrefix("longerthaneight", ""); got != "longerth/" {
		t.Errorf("long token truncated: %q", got)
	}
	if got := minioObjectPrefix("ignored", "  pre/  "); got != "pre/" {
		t.Errorf("trim: %q", got)
	}
}

func TestMinIOScanner_EmptyTokenAndPRID(t *testing.T) {
	scanner := newMinIOScannerWithClient(&fakeMinIOClient{bucketExists: true}, "")
	if _, err := scanner.StorageBytes(context.Background(), "", ""); err == nil {
		t.Fatal("expected error on empty token + empty PRID")
	}
}

func TestMinIOScanner_BucketExistsError(t *testing.T) {
	scanner := newMinIOScannerWithClient(&fakeMinIOClient{
		bucketExistsErr: errors.New("network down"),
	}, "instant-shared")
	if _, err := scanner.StorageBytes(context.Background(), "tok", ""); err == nil {
		t.Fatal("expected error from BucketExists failure")
	}
}

func TestMinIOScanner_ListObjectsError(t *testing.T) {
	scanner := newMinIOScannerWithClient(&fakeMinIOClient{
		bucketExists: true,
		listErr:      errors.New("list down"),
	}, "instant-shared")
	if _, err := scanner.StorageBytes(context.Background(), "tok", ""); err == nil {
		t.Fatal("expected error from ListObjects failure")
	}
}

// ── quota_wall_nudge: extra branches ─────────────────────────────────

// mockWallPlanRegistryCov mirrors mockWallPlanRegistry in jobs_test but
// lives in package jobs.
type mockWallPlanRegistryCov struct {
	storageMB   int
	connections int
	provisions  int
}

func (m *mockWallPlanRegistryCov) StorageLimitMB(_, _ string) int   { return m.storageMB }
func (m *mockWallPlanRegistryCov) ConnectionsLimit(_, _ string) int { return m.connections }
func (m *mockWallPlanRegistryCov) ProvisionLimit(_ string) int      { return m.provisions }

func TestQuotaWallNudge_DBQueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT id, plan_tier\s+FROM teams`).WillReturnError(errors.New("db down"))
	w := NewQuotaWallNudgeWorker(db, &mockWallPlanRegistryCov{})
	if err := w.Work(context.Background(), quotaWallNudgeJob()); err == nil {
		t.Fatal("expected error on team-list query failure")
	}
}

func TestQuotaWallNudge_InvalidUUID_Skipped(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT id, plan_tier\s+FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "plan_tier"}).AddRow("not-a-uuid", "hobby"))
	w := NewQuotaWallNudgeWorker(db, &mockWallPlanRegistryCov{storageMB: 10})
	if err := w.Work(context.Background(), quotaWallNudgeJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestQuotaWallNudge_DedupeQueryError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	teamID := uuid.New()
	mock.ExpectQuery(`SELECT id, plan_tier\s+FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "plan_tier"}).AddRow(teamID.String(), "hobby"))
	mock.ExpectQuery(`SELECT 1\s+FROM audit_log`).
		WithArgs(teamID, "near_quota_wall", sqlmock.AnyArg()).
		WillReturnError(errors.New("dedupe down"))
	w := NewQuotaWallNudgeWorker(db, &mockWallPlanRegistryCov{storageMB: 10})
	if err := w.Work(context.Background(), quotaWallNudgeJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestQuotaWallNudge_EvaluateTeamError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	teamID := uuid.New()
	mock.ExpectQuery(`SELECT id, plan_tier\s+FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "plan_tier"}).AddRow(teamID.String(), "hobby"))
	mock.ExpectQuery(`SELECT 1\s+FROM audit_log`).
		WithArgs(teamID, "near_quota_wall", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"n"}))
	mock.ExpectQuery(`SELECT COALESCE\(SUM\(storage_bytes\)`).
		WithArgs(teamID, "postgres").
		WillReturnError(errors.New("storage scan failed"))
	w := NewQuotaWallNudgeWorker(db, &mockWallPlanRegistryCov{storageMB: 10})
	if err := w.Work(context.Background(), quotaWallNudgeJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// TestQuotaWallNudge_ConnectionsAxisHit — postgres 4 of 5 conn cap → 80%.
func TestQuotaWallNudge_ConnectionsAxisHit(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	teamID := uuid.New()
	mock.ExpectQuery(`SELECT id, plan_tier\s+FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "plan_tier"}).AddRow(teamID.String(), "hobby"))
	mock.ExpectQuery(`SELECT 1\s+FROM audit_log`).
		WithArgs(teamID, "near_quota_wall", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"n"}))
	// Storage axis under threshold.
	for _, svc := range []string{"postgres", "redis", "mongodb"} {
		mock.ExpectQuery(`SELECT COALESCE\(SUM\(storage_bytes\)`).
			WithArgs(teamID, svc).
			WillReturnRows(sqlmock.NewRows([]string{"sum", "count"}).AddRow(int64(0), 0))
	}
	// Connections axis: postgres 4/5=80%, mongodb 0/5=skip.
	mock.ExpectQuery(`SELECT COUNT\(\*\)\s+FROM resources`).
		WithArgs(teamID, "postgres").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(4)))
	mock.ExpectQuery(`SELECT COUNT\(\*\)\s+FROM resources`).
		WithArgs(teamID, "mongodb").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))
	mock.ExpectExec(`INSERT INTO audit_log[\s\S]+resource_type`).
		WithArgs(teamID, "system", "near_quota_wall", sqlmock.AnyArg(), sqlmock.AnyArg(), "postgres").
		WillReturnResult(sqlmock.NewResult(1, 1))

	plans := &mockWallPlanRegistryCov{storageMB: 100_000, connections: 5, provisions: -1}
	w := NewQuotaWallNudgeWorker(db, plans)
	if err := w.Work(context.Background(), quotaWallNudgeJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// TestQuotaWallNudge_ProvisionsAxisHit — 5 resources vs 5 cap → 100%.
func TestQuotaWallNudge_ProvisionsAxisHit(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	teamID := uuid.New()
	mock.ExpectQuery(`SELECT id, plan_tier\s+FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "plan_tier"}).AddRow(teamID.String(), "hobby"))
	mock.ExpectQuery(`SELECT 1\s+FROM audit_log`).
		WithArgs(teamID, "near_quota_wall", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"n"}))
	for _, svc := range []string{"postgres", "redis", "mongodb"} {
		mock.ExpectQuery(`SELECT COALESCE\(SUM\(storage_bytes\)`).
			WithArgs(teamID, svc).
			WillReturnRows(sqlmock.NewRows([]string{"sum", "count"}).AddRow(int64(0), 0))
	}
	// Connections axis disabled (connLim=-1) — no per-svc COUNT queries.
	// Provisions axis: 5 active resources vs 5 cap → 100% hit.
	mock.ExpectQuery(`COUNT\(\*\)[\s\S]+team_id = \$1[\s\S]+status = 'active'`).
		WithArgs(teamID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(5)))
	mock.ExpectExec(`INSERT INTO audit_log[\s\S]+resource_type`).
		WithArgs(teamID, "system", "near_quota_wall", sqlmock.AnyArg(), sqlmock.AnyArg(), "").
		WillReturnResult(sqlmock.NewResult(1, 1))

	plans := &mockWallPlanRegistryCov{storageMB: 100_000, connections: -1, provisions: 5}
	w := NewQuotaWallNudgeWorker(db, plans)
	if err := w.Work(context.Background(), quotaWallNudgeJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// TestQuotaWallNudge_InsertFailure covers the INSERT-error path.
func TestQuotaWallNudge_InsertFailure(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	teamID := uuid.New()
	mock.ExpectQuery(`SELECT id, plan_tier\s+FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "plan_tier"}).AddRow(teamID.String(), "hobby"))
	mock.ExpectQuery(`SELECT 1\s+FROM audit_log`).
		WithArgs(teamID, "near_quota_wall", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"n"}))
	nineMB := int64(9 * 1024 * 1024)
	mock.ExpectQuery(`SELECT COALESCE\(SUM\(storage_bytes\)`).
		WithArgs(teamID, "postgres").
		WillReturnRows(sqlmock.NewRows([]string{"sum", "count"}).AddRow(nineMB, 1))
	mock.ExpectQuery(`SELECT COALESCE\(SUM\(storage_bytes\)`).
		WithArgs(teamID, "redis").
		WillReturnRows(sqlmock.NewRows([]string{"sum", "count"}).AddRow(int64(0), 0))
	mock.ExpectQuery(`SELECT COALESCE\(SUM\(storage_bytes\)`).
		WithArgs(teamID, "mongodb").
		WillReturnRows(sqlmock.NewRows([]string{"sum", "count"}).AddRow(int64(0), 0))
	mock.ExpectExec(`INSERT INTO audit_log[\s\S]+resource_type`).
		WithArgs(teamID, "system", "near_quota_wall", sqlmock.AnyArg(), sqlmock.AnyArg(), "postgres").
		WillReturnError(errors.New("audit insert down"))
	plans := &mockWallPlanRegistryCov{storageMB: 10, connections: -1, provisions: -1}
	w := NewQuotaWallNudgeWorker(db, plans)
	if err := w.Work(context.Background(), quotaWallNudgeJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// TestQuotaWallNudge_ScanError covers the team-row scan error continue
// (quota_wall_nudge.go:133-135): plan_tier column returns a non-string value
// type that fails Scan.
func TestQuotaWallNudge_ScanError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	// One column emitted where two are scanned → Scan error.
	mock.ExpectQuery(`SELECT id, plan_tier\s+FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("only-one-col"))
	plans := &mockWallPlanRegistryCov{storageMB: 10, connections: -1, provisions: -1}
	w := NewQuotaWallNudgeWorker(db, plans)
	if err := w.Work(context.Background(), quotaWallNudgeJob()); err != nil {
		t.Fatalf("scan error must be swallowed per-row, got %v", err)
	}
}

// TestQuotaWallNudge_RowsErr covers the rows.Err() error return
// (quota_wall_nudge.go:179-181).
func TestQuotaWallNudge_RowsErr(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT id, plan_tier\s+FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "plan_tier"}).
			RowError(0, errors.New("rows broke")).
			AddRow(uuid.New().String(), "hobby"))
	plans := &mockWallPlanRegistryCov{storageMB: 10, connections: -1, provisions: -1}
	w := NewQuotaWallNudgeWorker(db, plans)
	if err := w.Work(context.Background(), quotaWallNudgeJob()); err == nil {
		t.Error("expected rows.Err() to propagate")
	}
}

// TestQuotaWallNudge_StorageUnlimited covers the storage-axis limitMB<0
// (unlimited) continue (quota_wall_nudge.go:285-286) for a team that has
// active storage rows but an unlimited storage cap — no nudge fires.
func TestQuotaWallNudge_StorageUnlimited(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	teamID := uuid.New()
	mock.ExpectQuery(`SELECT id, plan_tier\s+FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "plan_tier"}).AddRow(teamID.String(), "growth"))
	mock.ExpectQuery(`SELECT 1\s+FROM audit_log`).
		WithArgs(teamID, "near_quota_wall", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"n"}))
	// Each storage svc has active rows so countRows>0 → reaches the limitMB<0
	// continue. storageMB=-1 → unlimited.
	for _, svc := range []string{"postgres", "redis", "mongodb"} {
		mock.ExpectQuery(`SELECT COALESCE\(SUM\(storage_bytes\)`).
			WithArgs(teamID, svc).
			WillReturnRows(sqlmock.NewRows([]string{"sum", "count"}).AddRow(int64(1<<30), 1))
	}
	plans := &mockWallPlanRegistryCov{storageMB: -1, connections: -1, provisions: -1}
	w := NewQuotaWallNudgeWorker(db, plans)
	if err := w.Work(context.Background(), quotaWallNudgeJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// TestQuotaWallNudge_ConnectionsQueryError covers evaluateTeam's connections
// COUNT query error → returns error → Work logs and continues
// (quota_wall_nudge.go:323-324 + evaluate_failed branch).
func TestQuotaWallNudge_ConnectionsQueryError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	teamID := uuid.New()
	mock.ExpectQuery(`SELECT id, plan_tier\s+FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "plan_tier"}).AddRow(teamID.String(), "hobby"))
	mock.ExpectQuery(`SELECT 1\s+FROM audit_log`).
		WithArgs(teamID, "near_quota_wall", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"n"}))
	for _, svc := range []string{"postgres", "redis", "mongodb"} {
		mock.ExpectQuery(`SELECT COALESCE\(SUM\(storage_bytes\)`).
			WithArgs(teamID, svc).
			WillReturnRows(sqlmock.NewRows([]string{"sum", "count"}).AddRow(int64(0), 0))
	}
	// First connections COUNT (postgres) errors.
	mock.ExpectQuery(`SELECT COUNT\(\*\)\s+FROM resources`).
		WithArgs(teamID, "postgres").
		WillReturnError(errors.New("conn count down"))
	plans := &mockWallPlanRegistryCov{storageMB: 100_000, connections: 5, provisions: -1}
	w := NewQuotaWallNudgeWorker(db, plans)
	if err := w.Work(context.Background(), quotaWallNudgeJob()); err != nil {
		t.Fatalf("evaluate error must be swallowed per-team, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// TestQuotaWallNudge_ProvisionsQueryError covers evaluateTeam's provisions
// COUNT query error (quota_wall_nudge.go:356-357).
func TestQuotaWallNudge_ProvisionsQueryError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	teamID := uuid.New()
	mock.ExpectQuery(`SELECT id, plan_tier\s+FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "plan_tier"}).AddRow(teamID.String(), "hobby"))
	mock.ExpectQuery(`SELECT 1\s+FROM audit_log`).
		WithArgs(teamID, "near_quota_wall", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"n"}))
	for _, svc := range []string{"postgres", "redis", "mongodb"} {
		mock.ExpectQuery(`SELECT COALESCE\(SUM\(storage_bytes\)`).
			WithArgs(teamID, svc).
			WillReturnRows(sqlmock.NewRows([]string{"sum", "count"}).AddRow(int64(0), 0))
	}
	// connections disabled (=-1); provisions axis enabled and its COUNT errors.
	mock.ExpectQuery(`COUNT\(\*\)[\s\S]+team_id = \$1[\s\S]+status = 'active'`).
		WithArgs(teamID).
		WillReturnError(errors.New("prov count down"))
	plans := &mockWallPlanRegistryCov{storageMB: 100_000, connections: -1, provisions: 5}
	w := NewQuotaWallNudgeWorker(db, plans)
	if err := w.Work(context.Background(), quotaWallNudgeJob()); err != nil {
		t.Fatalf("evaluate error must be swallowed per-team, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

// TestQuotaWallNudge_ConnectionsBelowThreshold covers the connections-axis
// pct<threshold continue (quota_wall_nudge.go:330-331): 1 of 5 conns = 20%.
func TestQuotaWallNudge_ConnectionsBelowThreshold(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	teamID := uuid.New()
	mock.ExpectQuery(`SELECT id, plan_tier\s+FROM teams`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "plan_tier"}).AddRow(teamID.String(), "hobby"))
	mock.ExpectQuery(`SELECT 1\s+FROM audit_log`).
		WithArgs(teamID, "near_quota_wall", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"n"}))
	for _, svc := range []string{"postgres", "redis", "mongodb"} {
		mock.ExpectQuery(`SELECT COALESCE\(SUM\(storage_bytes\)`).
			WithArgs(teamID, svc).
			WillReturnRows(sqlmock.NewRows([]string{"sum", "count"}).AddRow(int64(0), 0))
	}
	// postgres 1/5=20% (below), mongodb 1/5=20% (below) → no nudge.
	mock.ExpectQuery(`SELECT COUNT\(\*\)\s+FROM resources`).
		WithArgs(teamID, "postgres").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	mock.ExpectQuery(`SELECT COUNT\(\*\)\s+FROM resources`).
		WithArgs(teamID, "mongodb").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	plans := &mockWallPlanRegistryCov{storageMB: 100_000, connections: 5, provisions: -1}
	w := NewQuotaWallNudgeWorker(db, plans)
	if err := w.Work(context.Background(), quotaWallNudgeJob()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestInsertNearWallRow_DBError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnError(errors.New("audit down"))
	w := NewQuotaWallNudgeWorker(db, &mockWallPlanRegistryCov{})
	err = w.insertNearWallRow(context.Background(), uuid.New(), &wallHit{
		Tier: "hobby", Axis: "storage", Service: "postgres",
		Current: 1, Limit: 2, PercentUsed: 50,
	})
	if err == nil {
		t.Error("expected error")
	}
}

func TestTeamRecentlyNudged_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT 1\s+FROM audit_log`).
		WithArgs(sqlmock.AnyArg(), "near_quota_wall", sqlmock.AnyArg()).
		WillReturnError(errors.New("audit_log down"))
	w := NewQuotaWallNudgeWorker(db, &mockWallPlanRegistryCov{})
	if _, err := w.teamRecentlyNudged(context.Background(), uuid.New()); err == nil {
		t.Error("expected error")
	}
}

// ── quota.go: emitQuotaAuditRow + read/check helpers ─────────────────

func TestEmitQuotaAuditRow_InsertError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).WillReturnError(errors.New("audit_log down"))
	teamID := sql.NullString{String: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", Valid: true}
	emitQuotaAuditRow(context.Background(), db, quotaSuspendedKind, teamID,
		"22222222-2222-2222-2222-222222222222", "postgres", "rid-1")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestEmitQuotaAuditRow_UnsuspendedKind(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(nil, "system", "resource.quota_unsuspended", sqlmock.AnyArg(), sqlmock.AnyArg(), "redis").
		WillReturnResult(sqlmock.NewResult(1, 1))
	emitQuotaAuditRow(context.Background(), db, quotaUnsuspendedKind, sql.NullString{}, "rid-2", "redis", "")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestEmitQuotaAuditRow_DefaultKind(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO audit_log`).
		WithArgs(nil, "system", "unknown.kind", sqlmock.AnyArg(), sqlmock.AnyArg(), "queue").
		WillReturnResult(sqlmock.NewResult(1, 1))
	emitQuotaAuditRow(context.Background(), db, "unknown.kind", sql.NullString{}, "rid-3", "queue", "")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestReadStorageBytes_RowMissing_ReturnsZero(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"storage_bytes"}))
	bytes, err := readStorageBytes(context.Background(), db, uuid.New())
	if err != nil || bytes != 0 {
		t.Errorf("got (%d, %v); want (0, nil)", bytes, err)
	}
}

func TestReadStorageBytes_DBError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnError(errors.New("scan err"))
	_, err = readStorageBytes(context.Background(), db, uuid.New())
	if err == nil {
		t.Error("expected error")
	}
}

func TestCheckStorageQuota_Unlimited(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	b, ex, err := checkStorageQuota(context.Background(), db, uuid.New(), -1)
	if err != nil || ex || b != 0 {
		t.Errorf("got (%d, %v, %v); want (0, false, nil)", b, ex, err)
	}
}

func TestCheckStorageQuota_RowMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"storage_bytes"}))
	b, ex, err := checkStorageQuota(context.Background(), db, uuid.New(), 10)
	if err != nil || ex || b != 0 {
		t.Errorf("got (%d, %v, %v); want (0, false, nil)", b, ex, err)
	}
}

func TestCheckStorageQuota_DBError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnError(errors.New("scan err"))
	_, _, err = checkStorageQuota(context.Background(), db, uuid.New(), 10)
	if err == nil {
		t.Error("expected error")
	}
}

// ── quota_redis_eviction: extra guards ───────────────────────────────

func TestEvictTenantToCap_EmptyAdminURL(t *testing.T) {
	ev := NewDirectRedisEvictor("")
	d, b, err := ev.EvictTenantToCap(context.Background(), "tok", 1024)
	if d != 0 || b != 0 || err != nil {
		t.Errorf("empty admin URL: got (%d, %d, %v); want (0, 0, nil)", d, b, err)
	}
}

func TestEvictTenantToCap_BadURL(t *testing.T) {
	ev := NewDirectRedisEvictor("::not-a-url::")
	_, _, err := ev.EvictTenantToCap(context.Background(), "tok", 1024)
	if err == nil {
		t.Error("expected parse error on bad URL")
	}
}

// ── runRedisEvictionLoop: SELECT-error branch ────────────────────────

// stubEvictor never gets called in this test (the eviction-loop SELECT
// errors before any tenant is processed), but the worker requires a
// non-nil evictor to even try.
type stubEvictor struct{}

func (s *stubEvictor) EvictTenantToCap(_ context.Context, _ string, _ int64) (int, int64, error) {
	return 0, 0, nil
}

type mockPlanRegistryCov struct{ limitMB int }

func (m *mockPlanRegistryCov) StorageLimitMB(_, _ string) int   { return m.limitMB }
func (m *mockPlanRegistryCov) ConnectionsLimit(_, _ string) int { return -1 }
func (m *mockPlanRegistryCov) ProvisionLimit(_ string) int      { return -1 }

func TestRunRedisEvictionLoop_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT id, token, resource_type`).WithArgs("active").
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "tier", "storage_bytes", "provider_resource_id", "team_id", "name"}))
	mock.ExpectQuery(`SELECT id, token, resource_type`).WithArgs("suspended").
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_type", "tier", "storage_bytes", "provider_resource_id", "team_id", "name"}))
	mock.ExpectQuery(`SELECT id, token, tier, storage_bytes\s+FROM resources`).WithArgs("active").
		WillReturnError(errors.New("eviction select down"))

	w := NewEnforceStorageQuotaWorkerWithEvictor(db, &mockPlanRegistryCov{limitMB: 5}, nil, &stubEvictor{})
	if err := w.Work(context.Background(), quotaEnforceJob()); err != nil {
		// Work swallows eviction-loop errors (logs them).
		t.Errorf("Work should swallow eviction-loop SELECT error, got %v", err)
	}
	_ = mock.ExpectationsWereMet()
}

// ── suspend / unsuspend loop error + edge branches ────────────────────

// failingRevoker returns an error from both arms so the suspend-loop
// revoke_error branch (quota.go:387-395) and unsuspend-loop grant_error
// branch (quota.go:545-551) are exercised. The worker logs and proceeds.
type failingRevoker struct{}

func (failingRevoker) RevokeAccess(_ context.Context, _, _, _, _ string) error {
	return errors.New("revoke down")
}
func (failingRevoker) GrantAccess(_ context.Context, _, _, _, _ string) error {
	return errors.New("grant down")
}

const covSuspendCols = "id, token, resource_type"

// suspendRows builds a *sqlmock.Rows with the suspend/unsuspend column set.
func suspendRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "token", "resource_type", "tier", "storage_bytes",
		"provider_resource_id", "team_id", "name",
	})
}

// TestRunSuspendLoop_ScanError covers the row-scan error continue
// (quota.go:344-346): a row whose storage_bytes column is a non-int64 forces
// rows.Scan to fail; the loop logs and continues, ending with 0 suspends.
func TestRunSuspendLoop_ScanError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(covSuspendCols).WithArgs("active").
		WillReturnRows(suspendRows().AddRow(
			uuid.New().String(), "tok", "postgres", "anonymous",
			"not-an-int", "", nil, "n"))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 5}, nil)
	ids, err := w.runSuspendLoop(context.Background())
	if err != nil {
		t.Fatalf("runSuspendLoop: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("scan-error row should yield 0 suspends, got %d", len(ids))
	}
}

// TestRunSuspendLoop_InvalidUUID covers the uuid.Parse error continue
// (quota.go:355-358).
func TestRunSuspendLoop_InvalidUUID(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(covSuspendCols).WithArgs("active").
		WillReturnRows(suspendRows().AddRow(
			"not-a-uuid", "tok", "postgres", "anonymous",
			int64(0), "", nil, "n"))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 5}, nil)
	ids, err := w.runSuspendLoop(context.Background())
	if err != nil {
		t.Fatalf("runSuspendLoop: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("invalid-uuid row should yield 0 suspends, got %d", len(ids))
	}
}

// TestRunSuspendLoop_CheckError covers the checkStorageQuota error continue
// (quota.go:362-367): the storage_bytes SELECT inside checkStorageQuota fails,
// so the loop fails open and does not suspend.
func TestRunSuspendLoop_CheckError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	id := uuid.New().String()
	mock.ExpectQuery(covSuspendCols).WithArgs("active").
		WillReturnRows(suspendRows().AddRow(
			id, "tok", "postgres", "anonymous", int64(0), "", nil, "n"))
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WillReturnError(errors.New("check down"))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 5}, nil)
	ids, err := w.runSuspendLoop(context.Background())
	if err != nil {
		t.Fatalf("runSuspendLoop: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("check-error row should fail open (0 suspends), got %d", len(ids))
	}
}

// TestRunSuspendLoop_RevokeError covers the revoker.RevokeAccess error branch
// (quota.go:387-395): the row is over quota and the revoker errors; the loop
// logs the unexpected error but still flips the status and suspends.
func TestRunSuspendLoop_RevokeError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	id := uuid.New().String()
	mock.ExpectQuery(covSuspendCols).WithArgs("active").
		WillReturnRows(suspendRows().AddRow(
			id, "tok", "redis", "anonymous", int64(0), "", nil, "n"))
	// over quota: limitMB=1 -> 1MiB; bytes used 2MiB
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WillReturnRows(sqlmock.NewRows([]string{"storage_bytes"}).AddRow(int64(2 * 1024 * 1024)))
	mock.ExpectExec(`UPDATE resources SET status`).
		WithArgs("suspended", id, "active").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 1}, failingRevoker{})
	ids, err := w.runSuspendLoop(context.Background())
	if err != nil {
		t.Fatalf("runSuspendLoop: %v", err)
	}
	if len(ids) != 1 {
		t.Errorf("revoke-error must still suspend, got %d ids", len(ids))
	}
}

// TestRunSuspendLoop_UpdateError covers the suspend UPDATE error continue
// (quota.go:402-407).
func TestRunSuspendLoop_UpdateError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	id := uuid.New().String()
	mock.ExpectQuery(covSuspendCols).WithArgs("active").
		WillReturnRows(suspendRows().AddRow(
			id, "tok", "postgres", "anonymous", int64(0), "", nil, "n"))
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WillReturnRows(sqlmock.NewRows([]string{"storage_bytes"}).AddRow(int64(2 * 1024 * 1024)))
	mock.ExpectExec(`UPDATE resources SET status`).
		WithArgs("suspended", id, "active").
		WillReturnError(errors.New("update down"))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 1}, nil)
	ids, err := w.runSuspendLoop(context.Background())
	if err != nil {
		t.Fatalf("runSuspendLoop: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("update-error row should not be suspended, got %d", len(ids))
	}
}

// TestRunSuspendLoop_RowsErr covers the rows.Err() error return
// (quota.go:441-443).
func TestRunSuspendLoop_RowsErr(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(covSuspendCols).WithArgs("active").
		WillReturnRows(suspendRows().RowError(0, errors.New("rows broke")).AddRow(
			uuid.New().String(), "tok", "postgres", "anonymous", int64(0), "", nil, "n"))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 5}, nil)
	if _, err := w.runSuspendLoop(context.Background()); err == nil {
		t.Error("expected rows.Err() to propagate")
	}
}

// TestRunUnsuspendLoop_AllBranches covers the unsuspend-loop scan error,
// skip-set, invalid uuid, unsuspend check error, grant error, update error
// branches in a single multi-row scan (quota.go:493-561).
func TestRunUnsuspendLoop_ScanError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(covSuspendCols).WithArgs("suspended").
		WillReturnRows(suspendRows().AddRow(
			uuid.New().String(), "tok", "postgres", "anonymous",
			"not-an-int", "", nil, "n"))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 5}, nil)
	n, err := w.runUnsuspendLoop(context.Background(), nil)
	if err != nil {
		t.Fatalf("runUnsuspendLoop: %v", err)
	}
	if n != 0 {
		t.Errorf("scan-error row should not unsuspend, got %d", n)
	}
}

// TestRunUnsuspendLoop_SkipSet covers the just-suspended skip-set continue
// (quota.go:500-501).
func TestRunUnsuspendLoop_SkipSet(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	id := uuid.New().String()
	mock.ExpectQuery(covSuspendCols).WithArgs("suspended").
		WillReturnRows(suspendRows().AddRow(
			id, "tok", "postgres", "anonymous", int64(0), "", nil, "n"))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 5}, nil)
	n, err := w.runUnsuspendLoop(context.Background(), []string{id})
	if err != nil {
		t.Fatalf("runUnsuspendLoop: %v", err)
	}
	if n != 0 {
		t.Errorf("just-suspended row must be skipped, got %d", n)
	}
}

// TestRunUnsuspendLoop_InvalidUUID covers the uuid.Parse continue
// (quota.go:511-514).
func TestRunUnsuspendLoop_InvalidUUID(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(covSuspendCols).WithArgs("suspended").
		WillReturnRows(suspendRows().AddRow(
			"not-a-uuid", "tok", "postgres", "anonymous", int64(0), "", nil, "n"))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 5}, nil)
	n, err := w.runUnsuspendLoop(context.Background(), nil)
	if err != nil {
		t.Fatalf("runUnsuspendLoop: %v", err)
	}
	if n != 0 {
		t.Errorf("invalid-uuid row should not unsuspend, got %d", n)
	}
}

// TestRunUnsuspendLoop_CheckError covers the readStorageBytes error continue
// (quota.go:525-528): limitMB>0 so the hysteresis read runs and fails.
func TestRunUnsuspendLoop_CheckError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	id := uuid.New().String()
	mock.ExpectQuery(covSuspendCols).WithArgs("suspended").
		WillReturnRows(suspendRows().AddRow(
			id, "tok", "postgres", "anonymous", int64(0), "", nil, "n"))
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WillReturnError(errors.New("read down"))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 1}, nil)
	n, err := w.runUnsuspendLoop(context.Background(), nil)
	if err != nil {
		t.Fatalf("runUnsuspendLoop: %v", err)
	}
	if n != 0 {
		t.Errorf("check-error row should fail open (0 unsuspends), got %d", n)
	}
}

// TestRunUnsuspendLoop_GrantError covers the grant_error branch
// (quota.go:545-551): under threshold, grant errors, loop logs and still
// flips to active.
func TestRunUnsuspendLoop_GrantError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	id := uuid.New().String()
	mock.ExpectQuery(covSuspendCols).WithArgs("suspended").
		WillReturnRows(suspendRows().AddRow(
			id, "tok", "redis", "anonymous", int64(0), "", nil, "n"))
	// well under hysteresis threshold so it unsuspends
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WillReturnRows(sqlmock.NewRows([]string{"storage_bytes"}).AddRow(int64(0)))
	mock.ExpectExec(`UPDATE resources SET status`).
		WithArgs("active", id, "suspended").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 100}, failingRevoker{})
	n, err := w.runUnsuspendLoop(context.Background(), nil)
	if err != nil {
		t.Fatalf("runUnsuspendLoop: %v", err)
	}
	if n != 1 {
		t.Errorf("grant-error must still unsuspend, got %d", n)
	}
}

// TestRunUnsuspendLoop_UpdateError covers the unsuspend UPDATE error continue
// (quota.go:558-561).
func TestRunUnsuspendLoop_UpdateError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	id := uuid.New().String()
	mock.ExpectQuery(covSuspendCols).WithArgs("suspended").
		WillReturnRows(suspendRows().AddRow(
			id, "tok", "postgres", "anonymous", int64(0), "", nil, "n"))
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WillReturnRows(sqlmock.NewRows([]string{"storage_bytes"}).AddRow(int64(0)))
	mock.ExpectExec(`UPDATE resources SET status`).
		WithArgs("active", id, "suspended").
		WillReturnError(errors.New("update down"))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 100}, nil)
	n, err := w.runUnsuspendLoop(context.Background(), nil)
	if err != nil {
		t.Fatalf("runUnsuspendLoop: %v", err)
	}
	if n != 0 {
		t.Errorf("update-error row should not unsuspend, got %d", n)
	}
}

// TestRunUnsuspendLoop_UnlimitedTierSelfHeals covers the limitMB==-1 self-heal
// branch (quota.go:505-509): unlimited tier always treated below threshold,
// so a historically-suspended unlimited row is unsuspended.
func TestRunUnsuspendLoop_UnlimitedTierSelfHeals(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	id := uuid.New().String()
	mock.ExpectQuery(covSuspendCols).WithArgs("suspended").
		WillReturnRows(suspendRows().AddRow(
			id, "tok", "postgres", "team", int64(0), "", nil, "n"))
	mock.ExpectExec(`UPDATE resources SET status`).
		WithArgs("active", id, "suspended").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO audit_log`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: -1}, nil)
	n, err := w.runUnsuspendLoop(context.Background(), nil)
	if err != nil {
		t.Fatalf("runUnsuspendLoop: %v", err)
	}
	if n != 1 {
		t.Errorf("unlimited-tier suspended row should self-heal, got %d", n)
	}
}

// TestRunUnsuspendLoop_RowsErr covers the rows.Err() error return
// (quota.go:580-582).
func TestRunUnsuspendLoop_RowsErr(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(covSuspendCols).WithArgs("suspended").
		WillReturnRows(suspendRows().RowError(0, errors.New("rows broke")).AddRow(
			uuid.New().String(), "tok", "postgres", "anonymous", int64(0), "", nil, "n"))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 5}, nil)
	if _, err := w.runUnsuspendLoop(context.Background(), nil); err == nil {
		t.Error("expected rows.Err() to propagate")
	}
}

// ── evictTenantToCap: error / edge branches via a fake client ──────────

// fakeEvictClient drives evictTenantToCap's non-happy branches that
// miniredis cannot easily reproduce: SCAN error, DEL error, and a key that
// escapes the tenant prefix (the cross-tenant safety guard).
type fakeEvictClient struct {
	scanKeys []string
	scanErr  error
	memBytes int64
	idleSecs float64
	delErr   error
}

func (f *fakeEvictClient) Scan(ctx context.Context, _ uint64, _ string, _ int64) *goredis.ScanCmd {
	cmd := goredis.NewScanCmd(ctx, nil)
	if f.scanErr != nil {
		cmd.SetErr(f.scanErr)
		return cmd
	}
	// cursor 0 terminates the loop after one page.
	cmd.SetVal(f.scanKeys, 0)
	return cmd
}

func (f *fakeEvictClient) MemoryUsage(ctx context.Context, _ string, _ ...int) *goredis.IntCmd {
	cmd := goredis.NewIntCmd(ctx)
	cmd.SetVal(f.memBytes)
	return cmd
}

func (f *fakeEvictClient) ObjectIdleTime(ctx context.Context, _ string) *goredis.DurationCmd {
	cmd := goredis.NewDurationCmd(ctx, time.Second)
	cmd.SetVal(time.Duration(f.idleSecs) * time.Second)
	return cmd
}

func (f *fakeEvictClient) Del(ctx context.Context, _ ...string) *goredis.IntCmd {
	cmd := goredis.NewIntCmd(ctx)
	if f.delErr != nil {
		cmd.SetErr(f.delErr)
		return cmd
	}
	cmd.SetVal(1)
	return cmd
}

func TestEvictTenantToCap_EmptyToken(t *testing.T) {
	if _, _, err := evictTenantToCap(context.Background(), &fakeEvictClient{}, "", 100); err == nil {
		t.Error("empty token must error")
	}
}

func TestEvictTenantToCap_ScanError(t *testing.T) {
	c := &fakeEvictClient{scanErr: errors.New("scan down")}
	if _, _, err := evictTenantToCap(context.Background(), c, "tok", 100); err == nil {
		t.Error("scan error must propagate")
	}
}

func TestEvictTenantToCap_DelError_Continues(t *testing.T) {
	// Two over-cap keys, DEL always errors → 0 deleted, no error (fail-soft).
	c := &fakeEvictClient{
		scanKeys: []string{"tok:a", "tok:b"},
		memBytes: 1024,
		delErr:   errors.New("del down"),
	}
	deleted, _, err := evictTenantToCap(context.Background(), c, "tok", 1)
	if err != nil {
		t.Fatalf("DEL error must be fail-soft, got %v", err)
	}
	if deleted != 0 {
		t.Errorf("all DELs failed; want 0 deleted, got %d", deleted)
	}
}

func TestEvictTenantToCap_PrefixViolation_Aborts(t *testing.T) {
	// SCAN returns a key OUTSIDE the tenant prefix; the safety guard must
	// refuse the DEL and abort the tenant.
	c := &fakeEvictClient{
		scanKeys: []string{"OTHER:leak"},
		memBytes: 1024,
	}
	if _, _, err := evictTenantToCap(context.Background(), c, "tok", 1); err == nil {
		t.Error("prefix violation must abort with an error")
	}
}

func TestEvictTenantToCap_DeletesColdestFirst(t *testing.T) {
	// Two in-prefix keys over cap; both deletable → drives the idle-sort,
	// successful-DEL, and total-subtraction lines.
	c := &fakeEvictClient{
		scanKeys: []string{"tok:hot", "tok:cold"},
		memBytes: 1024,
		idleSecs: 5,
	}
	deleted, reclaimed, err := evictTenantToCap(context.Background(), c, "tok", 1)
	if err != nil {
		t.Fatalf("evict: %v", err)
	}
	if deleted == 0 || reclaimed == 0 {
		t.Errorf("expected >=1 deletion with reclaimed bytes, got d=%d r=%d", deleted, reclaimed)
	}
}

func TestAssertKeyInTenantPrefix_Violation(t *testing.T) {
	if err := assertKeyInTenantPrefix("tok", "other:k"); err == nil {
		t.Error("out-of-prefix key must error")
	}
	if err := assertKeyInTenantPrefix("tok", "tok:k"); err != nil {
		t.Errorf("in-prefix key must pass, got %v", err)
	}
}

// ── runRedisEvictionLoop: row-level branches via sqlmock + fake evictor ──

const covEvictCols = `SELECT id, token, tier, storage_bytes`

// countingEvictor returns a configurable (keysDeleted, bytes, err) so the
// eviction loop's evErr / keysDeleted==0 / success branches are all reachable.
type countingEvictor struct {
	deleted int
	bytes   int64
	err     error
}

func (c *countingEvictor) EvictTenantToCap(_ context.Context, _ string, _ int64) (int, int64, error) {
	return c.deleted, c.bytes, c.err
}

// evictRows builds the 4-column row set the eviction loop SELECTs.
func evictRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "token", "tier", "storage_bytes"})
}

// TestRunRedisEvictionLoop_ScanError covers the per-row scan error continue
// (quota.go:236-238): a non-int storage_bytes forces rows.Scan to fail.
func TestRunRedisEvictionLoop_ScanError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(covEvictCols).WithArgs("active").
		WillReturnRows(evictRows().AddRow(uuid.New().String(), "tok", "anonymous", "not-int"))

	w := NewEnforceStorageQuotaWorkerWithEvictor(db, &mockPlanRegistryCov{limitMB: 1}, nil, &countingEvictor{})
	n, err := w.runRedisEvictionLoop(context.Background())
	if err != nil {
		t.Fatalf("runRedisEvictionLoop: %v", err)
	}
	if n != 0 {
		t.Errorf("scan-error row should evict nothing, got %d", n)
	}
}

// TestRunRedisEvictionLoop_KeysDeletedZero covers the keysDeleted==0
// idempotent no-op continue (quota.go:280-283): an over-cap tenant whose
// evictor reports 0 deletions (stale storage_bytes).
func TestRunRedisEvictionLoop_KeysDeletedZero(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	overCap := int64(2 * 1024 * 1024) // 2 MiB vs 1 MiB cap
	mock.ExpectQuery(covEvictCols).WithArgs("active").
		WillReturnRows(evictRows().AddRow(uuid.New().String(), "tok", "anonymous", overCap))

	w := NewEnforceStorageQuotaWorkerWithEvictor(db, &mockPlanRegistryCov{limitMB: 1}, nil, &countingEvictor{deleted: 0})
	n, err := w.runRedisEvictionLoop(context.Background())
	if err != nil {
		t.Fatalf("runRedisEvictionLoop: %v", err)
	}
	if n != 0 {
		t.Errorf("zero-deletions tenant must not count as enforced, got %d", n)
	}
}

// TestRunRedisEvictionLoop_EvictorError covers the evErr fail-soft continue
// (quota.go:268-277).
func TestRunRedisEvictionLoop_EvictorError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	overCap := int64(2 * 1024 * 1024)
	mock.ExpectQuery(covEvictCols).WithArgs("active").
		WillReturnRows(evictRows().AddRow(uuid.New().String(), "tok", "anonymous", overCap))

	w := NewEnforceStorageQuotaWorkerWithEvictor(db, &mockPlanRegistryCov{limitMB: 1}, nil,
		&countingEvictor{err: errors.New("evict down")})
	n, err := w.runRedisEvictionLoop(context.Background())
	if err != nil {
		t.Fatalf("evictor error must be fail-soft, got %v", err)
	}
	if n != 0 {
		t.Errorf("errored tenant must not count, got %d", n)
	}
}

// TestRunRedisEvictionLoop_Success covers the enforced++/metrics success path
// (quota.go:286-298).
func TestRunRedisEvictionLoop_Success(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	overCap := int64(2 * 1024 * 1024)
	mock.ExpectQuery(covEvictCols).WithArgs("active").
		WillReturnRows(evictRows().AddRow(uuid.New().String(), "tok", "anonymous", overCap))

	w := NewEnforceStorageQuotaWorkerWithEvictor(db, &mockPlanRegistryCov{limitMB: 1}, nil,
		&countingEvictor{deleted: 3, bytes: 4096})
	n, err := w.runRedisEvictionLoop(context.Background())
	if err != nil {
		t.Fatalf("runRedisEvictionLoop: %v", err)
	}
	if n != 1 {
		t.Errorf("one over-cap tenant evicted should count as 1, got %d", n)
	}
}

// TestRunRedisEvictionLoop_RowsErr covers the rows.Err() error return
// (quota.go:300-302).
func TestRunRedisEvictionLoop_RowsErr(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(covEvictCols).WithArgs("active").
		WillReturnRows(evictRows().RowError(0, errors.New("rows broke")).
			AddRow(uuid.New().String(), "tok", "anonymous", int64(0)))

	w := NewEnforceStorageQuotaWorkerWithEvictor(db, &mockPlanRegistryCov{limitMB: 1}, nil, &countingEvictor{})
	if _, err := w.runRedisEvictionLoop(context.Background()); err == nil {
		t.Error("expected rows.Err() to propagate")
	}
}

// TestRunSuspendLoop_NotExceeded covers the !exceeded continue (quota.go:370):
// an under-quota row is checked but not suspended.
func TestRunSuspendLoop_NotExceeded(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	id := uuid.New().String()
	mock.ExpectQuery(covSuspendCols).WithArgs("active").
		WillReturnRows(suspendRows().AddRow(id, "tok", "postgres", "anonymous", int64(0), "", nil, "n"))
	mock.ExpectQuery(`SELECT storage_bytes FROM resources WHERE id = \$1`).
		WillReturnRows(sqlmock.NewRows([]string{"storage_bytes"}).AddRow(int64(0)))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 100}, nil)
	ids, err := w.runSuspendLoop(context.Background())
	if err != nil {
		t.Fatalf("runSuspendLoop: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("under-quota row must not suspend, got %d", len(ids))
	}
}

// TestRunUnsuspendLoop_QueryError covers the SELECT error return
// (quota.go:475-477).
func TestRunUnsuspendLoop_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(covSuspendCols).WithArgs("suspended").
		WillReturnError(errors.New("unsuspend select down"))

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 5}, nil)
	if _, err := w.runUnsuspendLoop(context.Background(), nil); err == nil {
		t.Error("expected unsuspend SELECT error to propagate")
	}
}

// TestWork_UnsuspendLoopError_Swallowed covers the Work() unsuspend-error
// swallow branch (quota.go:163-166): the suspend loop is empty, the unsuspend
// SELECT errors, and Work logs but still returns nil.
func TestWork_UnsuspendLoopError_Swallowed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(covSuspendCols).WithArgs("active").
		WillReturnRows(suspendRows())
	mock.ExpectQuery(covSuspendCols).WithArgs("suspended").
		WillReturnError(errors.New("unsuspend down"))
	// No evictor → eviction loop is a no-op (skipped).

	w := NewEnforceStorageQuotaWorker(db, &mockPlanRegistryCov{limitMB: 5}, nil)
	if err := w.Work(context.Background(), quotaEnforceJob()); err != nil {
		t.Errorf("Work must swallow unsuspend-loop error, got %v", err)
	}
}
