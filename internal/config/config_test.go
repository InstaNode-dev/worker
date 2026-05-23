package config

import (
	"strings"
	"testing"
)

// clearEnv unsets every env var Load reads so each test starts from a known
// blank slate. t.Setenv restores values on cleanup, but it does not unset
// pre-existing process env, so we explicitly clear first.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"DATABASE_URL", "REDIS_URL", "PROVISIONER_ADDR", "PROVISIONER_SECRET",
		"EMAIL_PROVIDER", "BREVO_API_KEY", "BREVO_TEMPLATE_IDS", "BREVO_SENDER_EMAIL",
		"BREVO_SENDER_NAME", "SES_AWS_REGION", "SES_AWS_ACCESS_KEY_ID",
		"SES_AWS_SECRET_ACCESS_KEY", "SES_FROM_EMAIL", "SES_TEMPLATE_NAMES",
		"ENVIRONMENT", "MAXMIND_LICENSE_KEY", "GEOLITE2_DB_PATH", "PLANS_PATH",
		"OBJECT_STORE_ENDPOINT", "OBJECT_STORE_ACCESS_KEY", "OBJECT_STORE_SECRET_KEY",
		"OBJECT_STORE_BUCKET", "OBJECT_STORE_REGION", "OBJECT_STORE_SECURE",
		"MINIO_ENDPOINT", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_BUCKET_NAME",
		"KUBE_NAMESPACE_APPS", "INSTANT_API_INTERNAL_URL", "WORKER_INTERNAL_JWT_SECRET",
		"AES_KEY", "OBJECT_STORE_BACKEND", "BACKUP_S3_BUCKET", "BACKUP_S3_PATH_PREFIX",
		"PLATFORM_BACKUP_S3_PREFIX", "CUSTOMER_DATABASE_URL", "MONGO_ADMIN_URI",
		"CUSTOMER_REDIS_URL",
	} {
		// Setenv("") then unset semantics: t.Setenv records the original and
		// restores it; setting to "" is enough since Load treats "" as unset.
		t.Setenv(k, "")
	}
}

func TestErrMissingConfig_Error(t *testing.T) {
	err := &ErrMissingConfig{Key: "DATABASE_URL"}
	got := err.Error()
	if !strings.Contains(got, "DATABASE_URL") {
		t.Fatalf("error message missing key: %q", got)
	}
	if !strings.Contains(got, "not set") {
		t.Fatalf("unexpected error message: %q", got)
	}
}

func TestGetenv(t *testing.T) {
	t.Setenv("CFG_TEST_KEY", "value")
	if got := getenv("CFG_TEST_KEY", "fb"); got != "value" {
		t.Fatalf("getenv set: got %q want value", got)
	}
	t.Setenv("CFG_TEST_KEY", "")
	if got := getenv("CFG_TEST_KEY", "fallback"); got != "fallback" {
		t.Fatalf("getenv empty: got %q want fallback", got)
	}
}

func TestRequire_Panics(t *testing.T) {
	t.Setenv("CFG_REQ_KEY", "")
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("require did not panic on missing key")
		}
		e, ok := r.(*ErrMissingConfig)
		if !ok {
			t.Fatalf("panic value type = %T, want *ErrMissingConfig", r)
		}
		if e.Key != "CFG_REQ_KEY" {
			t.Fatalf("panic key = %q", e.Key)
		}
	}()
	require("CFG_REQ_KEY")
}

func TestRequire_Present(t *testing.T) {
	t.Setenv("CFG_REQ_KEY", "x")
	if got := require("CFG_REQ_KEY"); got != "x" {
		t.Fatalf("require present: got %q", got)
	}
}

func TestLoad_Defaults(t *testing.T) {
	clearEnv(t)
	t.Setenv("DATABASE_URL", "postgres://localhost/db")

	cfg := Load()

	if cfg.DatabaseURL != "postgres://localhost/db" {
		t.Errorf("DatabaseURL = %q", cfg.DatabaseURL)
	}
	if cfg.RedisURL != "redis://localhost:6379" {
		t.Errorf("RedisURL default = %q", cfg.RedisURL)
	}
	if cfg.Environment != "development" {
		t.Errorf("Environment default = %q", cfg.Environment)
	}
	if cfg.GeoLite2DBPath != "./GeoLite2-City.mmdb" {
		t.Errorf("GeoLite2DBPath default = %q", cfg.GeoLite2DBPath)
	}
	if cfg.ObjectStoreBucket != "instant-shared" {
		t.Errorf("ObjectStoreBucket default = %q", cfg.ObjectStoreBucket)
	}
	if cfg.KubeNamespaceApps != "instant-apps" {
		t.Errorf("KubeNamespaceApps default = %q", cfg.KubeNamespaceApps)
	}
	if cfg.ObjectStoreBackend != "minio" {
		t.Errorf("ObjectStoreBackend default = %q", cfg.ObjectStoreBackend)
	}
	if cfg.BackupS3PathPrefix != "backups/" {
		t.Errorf("BackupS3PathPrefix default = %q", cfg.BackupS3PathPrefix)
	}
	if cfg.PlatformBackupS3Prefix != "platform-backups/" {
		t.Errorf("PlatformBackupS3Prefix default = %q", cfg.PlatformBackupS3Prefix)
	}
	// BackupS3Bucket falls back to ObjectStoreBucket.
	if cfg.BackupS3Bucket != "instant-shared" {
		t.Errorf("BackupS3Bucket fallback = %q", cfg.BackupS3Bucket)
	}
	if cfg.ObjectStoreSecure {
		t.Error("ObjectStoreSecure should default false")
	}
	// Empty maps, not nil.
	if cfg.BrevoTemplateIDs == nil || len(cfg.BrevoTemplateIDs) != 0 {
		t.Errorf("BrevoTemplateIDs = %v", cfg.BrevoTemplateIDs)
	}
	if cfg.SESTemplateNames == nil || len(cfg.SESTemplateNames) != 0 {
		t.Errorf("SESTemplateNames = %v", cfg.SESTemplateNames)
	}
}

func TestLoad_PanicsWithoutDatabaseURL(t *testing.T) {
	clearEnv(t)
	defer func() {
		if recover() == nil {
			t.Fatal("Load did not panic without DATABASE_URL")
		}
	}()
	Load()
}

func TestLoad_AllOverrides(t *testing.T) {
	clearEnv(t)
	t.Setenv("DATABASE_URL", "postgres://db")
	t.Setenv("REDIS_URL", "redis://r:6379")
	t.Setenv("PROVISIONER_ADDR", "prov:50051")
	t.Setenv("PROVISIONER_SECRET", "psecret")
	t.Setenv("EMAIL_PROVIDER", "brevo")
	t.Setenv("BREVO_API_KEY", "bkey")
	t.Setenv("BREVO_TEMPLATE_IDS", `{"a.kind":12,"b.kind":7}`)
	t.Setenv("BREVO_SENDER_EMAIL", "no@x.dev")
	t.Setenv("BREVO_SENDER_NAME", "X")
	t.Setenv("SES_AWS_REGION", "us-east-1")
	t.Setenv("SES_AWS_ACCESS_KEY_ID", "AK")
	t.Setenv("SES_AWS_SECRET_ACCESS_KEY", "SK")
	t.Setenv("SES_FROM_EMAIL", "from@x.dev")
	t.Setenv("SES_TEMPLATE_NAMES", `{"a.kind":"tmpl-v1"}`)
	t.Setenv("ENVIRONMENT", "production")
	t.Setenv("MAXMIND_LICENSE_KEY", "mm")
	t.Setenv("GEOLITE2_DB_PATH", "/data/geo.mmdb")
	t.Setenv("PLANS_PATH", "/etc/plans.yaml")
	t.Setenv("OBJECT_STORE_ENDPOINT", "nyc3.do.com")
	t.Setenv("OBJECT_STORE_ACCESS_KEY", "oak")
	t.Setenv("OBJECT_STORE_SECRET_KEY", "osk")
	t.Setenv("OBJECT_STORE_BUCKET", "mybucket")
	t.Setenv("OBJECT_STORE_REGION", "nyc3")
	t.Setenv("OBJECT_STORE_SECURE", "true")
	t.Setenv("KUBE_NAMESPACE_APPS", "myapps")
	t.Setenv("INSTANT_API_INTERNAL_URL", "http://api")
	t.Setenv("WORKER_INTERNAL_JWT_SECRET", "wjwt")
	t.Setenv("AES_KEY", "deadbeef")
	t.Setenv("OBJECT_STORE_BACKEND", "do-spaces")
	t.Setenv("BACKUP_S3_BUCKET", "backups-bkt")
	t.Setenv("BACKUP_S3_PATH_PREFIX", "bk/")
	t.Setenv("PLATFORM_BACKUP_S3_PREFIX", "plat/")
	t.Setenv("CUSTOMER_DATABASE_URL", "postgres://cust")
	t.Setenv("MONGO_ADMIN_URI", "mongodb://admin")
	t.Setenv("CUSTOMER_REDIS_URL", "redis://cust")

	cfg := Load()

	if cfg.RedisURL != "redis://r:6379" {
		t.Errorf("RedisURL = %q", cfg.RedisURL)
	}
	if cfg.ProvisionerAddr != "prov:50051" || cfg.ProvisionerSecret != "psecret" {
		t.Errorf("provisioner = %q / %q", cfg.ProvisionerAddr, cfg.ProvisionerSecret)
	}
	if cfg.EmailProvider != "brevo" || cfg.BrevoAPIKey != "bkey" {
		t.Errorf("brevo = %q / %q", cfg.EmailProvider, cfg.BrevoAPIKey)
	}
	if cfg.BrevoTemplateIDs["a.kind"] != 12 || cfg.BrevoTemplateIDs["b.kind"] != 7 {
		t.Errorf("BrevoTemplateIDs = %v", cfg.BrevoTemplateIDs)
	}
	if cfg.SESTemplateNames["a.kind"] != "tmpl-v1" {
		t.Errorf("SESTemplateNames = %v", cfg.SESTemplateNames)
	}
	if cfg.Environment != "production" {
		t.Errorf("Environment = %q", cfg.Environment)
	}
	if cfg.GeoLite2DBPath != "/data/geo.mmdb" || cfg.PlansPath != "/etc/plans.yaml" {
		t.Errorf("geo/plans = %q / %q", cfg.GeoLite2DBPath, cfg.PlansPath)
	}
	if !cfg.ObjectStoreSecure {
		t.Error("ObjectStoreSecure should be true")
	}
	if cfg.ObjectStoreBucket != "mybucket" {
		t.Errorf("ObjectStoreBucket = %q", cfg.ObjectStoreBucket)
	}
	if cfg.ObjectStoreBackend != "do-spaces" {
		t.Errorf("ObjectStoreBackend = %q", cfg.ObjectStoreBackend)
	}
	if cfg.BackupS3Bucket != "backups-bkt" {
		t.Errorf("BackupS3Bucket = %q", cfg.BackupS3Bucket)
	}
	if cfg.BackupS3PathPrefix != "bk/" || cfg.PlatformBackupS3Prefix != "plat/" {
		t.Errorf("backup prefixes = %q / %q", cfg.BackupS3PathPrefix, cfg.PlatformBackupS3Prefix)
	}
	if cfg.AESKey != "deadbeef" {
		t.Errorf("AESKey = %q", cfg.AESKey)
	}
	if cfg.CustomerDatabaseURL != "postgres://cust" ||
		cfg.MongoAdminURI != "mongodb://admin" ||
		cfg.CustomerRedisURL != "redis://cust" {
		t.Errorf("customer infra = %q / %q / %q",
			cfg.CustomerDatabaseURL, cfg.MongoAdminURI, cfg.CustomerRedisURL)
	}
	if cfg.InstantAPIInternalURL != "http://api" || cfg.WorkerInternalJWTSecret != "wjwt" {
		t.Errorf("internal api = %q / %q", cfg.InstantAPIInternalURL, cfg.WorkerInternalJWTSecret)
	}
}

// TestLoad_LegacyMinioFallback exercises every MINIO_* fallback branch when
// the OBJECT_STORE_* equivalents are unset.
func TestLoad_LegacyMinioFallback(t *testing.T) {
	clearEnv(t)
	t.Setenv("DATABASE_URL", "postgres://db")
	t.Setenv("MINIO_ENDPOINT", "minio:9000")
	t.Setenv("MINIO_ROOT_USER", "minioadmin")
	t.Setenv("MINIO_ROOT_PASSWORD", "miniopass")
	t.Setenv("MINIO_BUCKET_NAME", "legacy-bucket")

	cfg := Load()

	if cfg.ObjectStoreEndpoint != "minio:9000" {
		t.Errorf("endpoint fallback = %q", cfg.ObjectStoreEndpoint)
	}
	if cfg.ObjectStoreAccessKey != "minioadmin" {
		t.Errorf("access key fallback = %q", cfg.ObjectStoreAccessKey)
	}
	if cfg.ObjectStoreSecretKey != "miniopass" {
		t.Errorf("secret key fallback = %q", cfg.ObjectStoreSecretKey)
	}
	// ObjectStoreBucket defaults to "instant-shared" then MINIO_BUCKET_NAME wins.
	if cfg.ObjectStoreBucket != "legacy-bucket" {
		t.Errorf("bucket fallback = %q", cfg.ObjectStoreBucket)
	}
}

// TestLoad_ObjectStoreWinsOverMinio confirms the OBJECT_STORE_* values are NOT
// overwritten by MINIO_* when both present (the fallback branches are skipped).
func TestLoad_ObjectStoreWinsOverMinio(t *testing.T) {
	clearEnv(t)
	t.Setenv("DATABASE_URL", "postgres://db")
	t.Setenv("OBJECT_STORE_ENDPOINT", "primary:9000")
	t.Setenv("OBJECT_STORE_ACCESS_KEY", "pak")
	t.Setenv("OBJECT_STORE_SECRET_KEY", "psk")
	t.Setenv("OBJECT_STORE_BUCKET", "primary-bucket")
	t.Setenv("MINIO_ENDPOINT", "minio:9000")
	t.Setenv("MINIO_ROOT_USER", "minioadmin")
	t.Setenv("MINIO_ROOT_PASSWORD", "miniopass")
	t.Setenv("MINIO_BUCKET_NAME", "legacy-bucket")

	cfg := Load()

	if cfg.ObjectStoreEndpoint != "primary:9000" {
		t.Errorf("endpoint = %q", cfg.ObjectStoreEndpoint)
	}
	if cfg.ObjectStoreAccessKey != "pak" || cfg.ObjectStoreSecretKey != "psk" {
		t.Errorf("keys = %q / %q", cfg.ObjectStoreAccessKey, cfg.ObjectStoreSecretKey)
	}
	// Bucket != "instant-shared" so the MINIO_BUCKET_NAME branch is skipped.
	if cfg.ObjectStoreBucket != "primary-bucket" {
		t.Errorf("bucket = %q", cfg.ObjectStoreBucket)
	}
}

func TestParseBrevoTemplateIDs(t *testing.T) {
	if m := parseBrevoTemplateIDs(""); len(m) != 0 || m == nil {
		t.Errorf("empty = %v", m)
	}
	m := parseBrevoTemplateIDs(`{"x":1,"y":2}`)
	if m["x"] != 1 || m["y"] != 2 {
		t.Errorf("valid = %v", m)
	}
	if bad := parseBrevoTemplateIDs(`{not json`); len(bad) != 0 || bad == nil {
		t.Errorf("malformed should be empty map: %v", bad)
	}
}

func TestParseSESTemplateNames(t *testing.T) {
	if m := parseSESTemplateNames(""); len(m) != 0 || m == nil {
		t.Errorf("empty = %v", m)
	}
	m := parseSESTemplateNames(`{"x":"tmpl"}`)
	if m["x"] != "tmpl" {
		t.Errorf("valid = %v", m)
	}
	if bad := parseSESTemplateNames(`[]invalid`); len(bad) != 0 || bad == nil {
		t.Errorf("malformed should be empty map: %v", bad)
	}
}
