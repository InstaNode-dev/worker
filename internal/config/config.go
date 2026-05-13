package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
)

// Config holds all runtime configuration for the worker service.
type Config struct {
	DatabaseURL       string // DATABASE_URL — platform postgres
	RedisURL          string // REDIS_URL — platform redis
	ProvisionerAddr   string // PROVISIONER_ADDR — gRPC addr of provisioner service
	ProvisionerSecret string // PROVISIONER_SECRET
	ResendAPIKey      string // RESEND_API_KEY — for digest/trial emails

	// Event-email provider — see internal/email/.
	//
	// EMAIL_PROVIDER selects the implementation:
	//   "brevo"     → POST to Brevo /v3/smtp/email with BREVO_TEMPLATE_IDS mapping
	//   "noop" / "" → silent no-op (NoopProvider; default — fail-open)
	//   "ses" / "sendgrid" → reserved for future implementations
	//
	// Swapping providers later = one line in internal/email/factory.go + one new
	// file under internal/email/. No forwarder change, no mapping change.
	EmailProvider    string            // EMAIL_PROVIDER
	BrevoAPIKey      string            // BREVO_API_KEY
	BrevoTemplateIDs map[string]int    // BREVO_TEMPLATE_IDS (JSON object: audit_log.kind → numeric template id)

	Environment       string // ENVIRONMENT
	MaxMindLicenseKey string // MAXMIND_LICENSE_KEY — for GeoLite2 refresh job
	GeoLite2DBPath    string // GEOLITE2_DB_PATH — local path to the GeoLite2 City MMDB
	PlansPath         string // PLANS_PATH — path to plans.yaml (optional; uses built-in defaults if empty)
	// Object storage backend for the storage_bytes scanner
	// (provider-agnostic — works against MinIO, DO Spaces, AWS S3,
	// GCS, R2, B2, Wasabi). The scanner uses the standard minio-go
	// SDK, which speaks plain S3 against any of these endpoints.
	ObjectStoreEndpoint  string // OBJECT_STORE_ENDPOINT — host:port (e.g. nyc3.digitaloceanspaces.com)
	ObjectStoreAccessKey string // OBJECT_STORE_ACCESS_KEY — master access key
	ObjectStoreSecretKey string // OBJECT_STORE_SECRET_KEY — master secret key
	ObjectStoreBucket    string // OBJECT_STORE_BUCKET — shared bucket (default: instant-shared)
	ObjectStoreRegion    string // OBJECT_STORE_REGION — e.g. "nyc3" for DO Spaces
	ObjectStoreSecure    bool   // OBJECT_STORE_SECURE — true for TLS-terminated endpoints

	// Legacy MINIO_* env vars — fallback for backward compat.
	MinioEndpoint     string
	MinioRootUser     string
	MinioRootPassword string
	MinioBucketName   string
	KubeNamespaceApps string // KUBE_NAMESPACE_APPS — stack namespace prefix (default: "instant-apps")
}

// ErrMissingConfig is returned when a required env var is absent.
type ErrMissingConfig struct {
	Key string
}

func (e *ErrMissingConfig) Error() string {
	return fmt.Sprintf("required environment variable %q is not set", e.Key)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func require(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(&ErrMissingConfig{Key: key})
	}
	return v
}

// Load reads configuration from environment variables. Panics on missing required fields.
func Load() *Config {
	cfg := &Config{
		DatabaseURL:       require("DATABASE_URL"),
		RedisURL:          getenv("REDIS_URL", "redis://localhost:6379"),
		ProvisionerAddr:   os.Getenv("PROVISIONER_ADDR"),
		ProvisionerSecret: os.Getenv("PROVISIONER_SECRET"),
		ResendAPIKey:      os.Getenv("RESEND_API_KEY"),
		EmailProvider:     os.Getenv("EMAIL_PROVIDER"),
		BrevoAPIKey:       os.Getenv("BREVO_API_KEY"),
		BrevoTemplateIDs:  parseBrevoTemplateIDs(os.Getenv("BREVO_TEMPLATE_IDS")),
		Environment:       getenv("ENVIRONMENT", "development"),
		MaxMindLicenseKey: os.Getenv("MAXMIND_LICENSE_KEY"),
		GeoLite2DBPath:    getenv("GEOLITE2_DB_PATH", "./GeoLite2-City.mmdb"),
		PlansPath:         os.Getenv("PLANS_PATH"),
		ObjectStoreEndpoint:  os.Getenv("OBJECT_STORE_ENDPOINT"),
		ObjectStoreAccessKey: os.Getenv("OBJECT_STORE_ACCESS_KEY"),
		ObjectStoreSecretKey: os.Getenv("OBJECT_STORE_SECRET_KEY"),
		ObjectStoreBucket:    getenv("OBJECT_STORE_BUCKET", "instant-shared"),
		ObjectStoreRegion:    os.Getenv("OBJECT_STORE_REGION"),
		ObjectStoreSecure:    os.Getenv("OBJECT_STORE_SECURE") == "true",

		MinioEndpoint:     os.Getenv("MINIO_ENDPOINT"),
		MinioRootUser:     os.Getenv("MINIO_ROOT_USER"),
		MinioRootPassword: os.Getenv("MINIO_ROOT_PASSWORD"),
		MinioBucketName:   getenv("MINIO_BUCKET_NAME", "instant-shared"),
		KubeNamespaceApps: getenv("KUBE_NAMESPACE_APPS", "instant-apps"),
	}

	// Fall back to legacy MINIO_* names so deployments that haven't
	// migrated env vars keep working.
	if cfg.ObjectStoreEndpoint == "" {
		cfg.ObjectStoreEndpoint = cfg.MinioEndpoint
	}
	if cfg.ObjectStoreAccessKey == "" {
		cfg.ObjectStoreAccessKey = cfg.MinioRootUser
	}
	if cfg.ObjectStoreSecretKey == "" {
		cfg.ObjectStoreSecretKey = cfg.MinioRootPassword
	}
	if cfg.ObjectStoreBucket == "instant-shared" && cfg.MinioBucketName != "" {
		cfg.ObjectStoreBucket = cfg.MinioBucketName
	}

	slog.Info("worker.config.loaded",
		"environment", cfg.Environment,
		"provisioner_addr_set", cfg.ProvisionerAddr != "",
		"resend_key_set", cfg.ResendAPIKey != "",
		"email_provider", cfg.EmailProvider,
		"brevo_key_set", cfg.BrevoAPIKey != "",
		"brevo_template_count", len(cfg.BrevoTemplateIDs),
	)
	return cfg
}

// parseBrevoTemplateIDs decodes the BREVO_TEMPLATE_IDS env var. The expected
// shape is a JSON object mapping audit_log.kind → numeric Brevo template id:
//
//   BREVO_TEMPLATE_IDS='{"subscription.upgraded": 12, "near_quota_wall": 7}'
//
// Empty string returns an empty map (Brevo will then SkipNoTemplate on every
// send — operator opted into "API key set, no templates yet"). A malformed
// value logs an ERROR and returns empty so the worker still boots — a
// templated message that drops back to SkipNoTemplate is easier to debug
// than a boot crash for a marketing operator who fat-fingered the JSON.
func parseBrevoTemplateIDs(raw string) map[string]int {
	if raw == "" {
		return map[string]int{}
	}
	m := map[string]int{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		slog.Error("worker.config.brevo_template_ids_invalid",
			"error", err,
			"note", "BREVO_TEMPLATE_IDS must be a JSON object of kind→int; falling back to empty map",
		)
		return map[string]int{}
	}
	return m
}
