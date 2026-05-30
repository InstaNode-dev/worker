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

	// FOLLOWUP-5 (2026-05-14): RESEND_API_KEY removed — the legacy Resend
	// EmailClient (worker/internal/jobs/email.go) was deleted as part of
	// finishing the Resend→Brevo migration. All lifecycle email is now
	// routed via the BrevoForwarder (event_email_forwarder.go). If you
	// still see RESEND_API_KEY in a k8s Secret, it's safe to remove.

	// Event-email provider — see internal/email/.
	//
	// EMAIL_PROVIDER selects the implementation:
	//   "brevo"     → POST to Brevo /v3/smtp/email with BREVO_TEMPLATE_IDS mapping
	//   "ses"       → AWS SES v2 SendEmail with SES_TEMPLATE_NAMES mapping
	//   "noop" / "" → silent no-op (NoopProvider; default — fail-open)
	//   "sendgrid"  → reserved for future implementations
	//
	// Swapping providers later = flip EMAIL_PROVIDER + populate that provider's
	// env vars. No forwarder change, no mapping change.
	EmailProvider    string         // EMAIL_PROVIDER
	BrevoAPIKey      string         // BREVO_API_KEY
	BrevoTemplateIDs map[string]int // BREVO_TEMPLATE_IDS (JSON object: audit_log.kind → numeric template id)
	// Sender identity for the raw-HTML send path (EventEmail.HTMLBody non-empty).
	// Defaults are applied inside email.NewBrevoProvider — env unset is safe.
	//   BREVO_SENDER_EMAIL — defaults to noreply@instanode.dev
	//   BREVO_SENDER_NAME  — defaults to "instanode"
	// These exist as code-controlled config so a rendered email cannot inherit
	// a personal address left in the Brevo dashboard sender field.
	BrevoSenderEmail string // BREVO_SENDER_EMAIL
	BrevoSenderName  string // BREVO_SENDER_NAME

	// SES_* env vars — populated only when EMAIL_PROVIDER=ses. SES_AWS_*
	// names are scoped (not bare AWS_*) so they can't be confused with
	// general-purpose AWS creds used elsewhere in the cluster (e.g. the
	// storage-bytes scanner, which uses OBJECT_STORE_* and may point at
	// a non-AWS S3-compatible backend).
	SESAWSRegion     string            // SES_AWS_REGION
	SESAWSAccessKey  string            // SES_AWS_ACCESS_KEY_ID
	SESAWSSecretKey  string            // SES_AWS_SECRET_ACCESS_KEY
	SESFromEmail     string            // SES_FROM_EMAIL (must be a verified SES identity)
	SESTemplateNames map[string]string // SES_TEMPLATE_NAMES (JSON object: audit_log.kind → SES template name)

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

	// Internal worker → api authenticated callbacks. The
	// PaymentGraceTerminatorWorker uses these to POST
	// /internal/teams/:id/terminate against the api repo. Both must be
	// set for the terminator to act — otherwise the job short-circuits
	// each tick with a WARN.
	InstantAPIInternalURL   string // INSTANT_API_INTERNAL_URL — base URL of api (cluster-local)
	WorkerInternalJWTSecret string // WORKER_INTERNAL_JWT_SECRET — HS256 shared with api

	// AES_KEY — 64-char hex key (32 bytes) used to decrypt
	// resources.connection_url for the customer-backup runner AND the real
	// resource-heartbeat prober (real_prober.go). Mirrors the api's
	// crypto.ParseAESKey/Decrypt usage. When unset both consumers fail-open
	// with a logged WARN — neither dumps plaintext nor crashes the worker.
	AESKey string

	// Backup-specific object-store settings. Default to the OBJECT_STORE_*
	// values above so a single-bucket dev cluster (MinIO) works out of the
	// box; production overrides BACKUP_S3_BUCKET to a separate, 90-day-
	// lifecycle-policied bucket so pg_dump tarballs never mix with the
	// /storage/new customer object data.
	//
	// OBJECT_STORE_BACKEND selects intent only: "minio" today, "do-spaces"
	// post-cutover. The actual SDK path is the same minio-go client either
	// way (DO Spaces speaks the S3 API natively) — the env var lets ops flip
	// behavior (e.g. retention windows, bucket names) without a code change.
	ObjectStoreBackend  string // OBJECT_STORE_BACKEND — "minio" (default) | "do-spaces"
	BackupS3Bucket      string // BACKUP_S3_BUCKET — defaults to ObjectStoreBucket if empty
	BackupS3PathPrefix  string // BACKUP_S3_PATH_PREFIX — defaults to "backups/"

	// PlatformBackupS3Prefix is the inner prefix under
	// BACKUP_S3_PATH_PREFIX for platform DB dumps. Distinct from the
	// customer-DB backup prefix so a single bucket-list can answer "show me
	// every platform dump in chronological order" without filtering customer
	// data.
	PlatformBackupS3Prefix string // PLATFORM_BACKUP_S3_PREFIX (default: "platform-backups/")

	// Storage-quota enforcement: infra revoke/grant on suspend/unsuspend (P0-3/P0-4 fix).
	// These mirror the api's config for the same operations — the worker
	// needs direct access to customer infrastructure to revoke connections
	// (ACL SETUSER off / REVOKE CONNECT / revokeRolesFromUser) when a
	// resource exceeds its storage quota and to re-grant on auto-unsuspend.
	//
	// All three are optional — when absent the worker skips the infra-revoke
	// step (fail-open) and only flips the status row. The row flip still
	// gates the user's provisioning API calls; the infra revoke terminates
	// live TCP connections.
	//
	// CUSTOMER_DATABASE_URL — admin DSN for shared Postgres cluster.
	// MONGO_ADMIN_URI       — admin URI for shared MongoDB cluster.
	// CUSTOMER_REDIS_URL    — admin Redis URL for shared Redis cluster.
	CustomerDatabaseURL string // CUSTOMER_DATABASE_URL
	MongoAdminURI       string // MONGO_ADMIN_URI
	CustomerRedisURL    string // CUSTOMER_REDIS_URL

	// AUTH-004 synthetic prober — drives a real-browser-shaped login probe
	// against the api every 5 minutes so the next regression in the
	// /auth/exchange CORS chain pages immediately. All five fields are
	// optional; AuthProbeBaseURL empty falls back to https://api.instanode.dev.
	// AuthProbeBearerToken empty causes leg 3 (/auth/me) to be skipped with
	// result="degraded" — operator hasn't wired a probe-account token.
	AuthProbeBaseURL     string // AUTH_PROBE_BASE_URL — default https://api.instanode.dev
	AuthProbeEmail       string // AUTH_PROBE_EMAIL — synthetic identity for /auth/email/start
	AuthProbeReturnTo    string // AUTH_PROBE_RETURN_TO — must be on api's allow-list
	AuthProbeOrigin      string // AUTH_PROBE_ORIGIN — must match api CORS allow-list
	AuthProbeBearerToken string // AUTH_PROBE_BEARER_TOKEN — probe-account session JWT

	// Hourly synthetic deploy prober — drives a real end-to-end deploy
	// against the prod /deploy/new pipeline every 60 minutes so the
	// next regression in Kaniko / k8s / Ingress / TLS pages within the
	// 30-minute alert window. BearerToken is required for the prober to
	// run; empty causes every leg to report result="degraded" (config
	// drift, not outage). BaseURL + DeployHost fall back to the production
	// hosts inside jobs.DeployProbeConfig.Defaults().
	DeployProbeBaseURL     string // DEPLOY_PROBE_BASE_URL — default https://api.instanode.dev
	DeployProbeDeployHost  string // DEPLOY_PROBE_DEPLOY_HOST — default deployment.instanode.dev
	DeployProbeBearerToken string // DEPLOY_PROBE_BEARER_TOKEN — probe-team session JWT (required)
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
		EmailProvider:     os.Getenv("EMAIL_PROVIDER"),
		BrevoAPIKey:       os.Getenv("BREVO_API_KEY"),
		BrevoTemplateIDs:  parseBrevoTemplateIDs(os.Getenv("BREVO_TEMPLATE_IDS")),
		BrevoSenderEmail:  os.Getenv("BREVO_SENDER_EMAIL"),
		BrevoSenderName:   os.Getenv("BREVO_SENDER_NAME"),
		SESAWSRegion:      os.Getenv("SES_AWS_REGION"),
		SESAWSAccessKey:   os.Getenv("SES_AWS_ACCESS_KEY_ID"),
		SESAWSSecretKey:   os.Getenv("SES_AWS_SECRET_ACCESS_KEY"),
		SESFromEmail:      os.Getenv("SES_FROM_EMAIL"),
		SESTemplateNames:  parseSESTemplateNames(os.Getenv("SES_TEMPLATE_NAMES")),
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

		InstantAPIInternalURL:   os.Getenv("INSTANT_API_INTERNAL_URL"),
		WorkerInternalJWTSecret: os.Getenv("WORKER_INTERNAL_JWT_SECRET"),

		// AES_KEY is shared with the api for connection_url decryption.
		// Consumed by both the customer-backup runner AND the real
		// resource-heartbeat prober (real_prober.go). Optional at boot
		// (fail-open: prober still runs against raw column, backup
		// runner WARN-skips each tick) so the rollout of W5-B's k8s
		// Secret patch doesn't hard-fail worker boot.
		AESKey:             os.Getenv("AES_KEY"),
		ObjectStoreBackend: getenv("OBJECT_STORE_BACKEND", "minio"),
		BackupS3Bucket:     os.Getenv("BACKUP_S3_BUCKET"),
		BackupS3PathPrefix: getenv("BACKUP_S3_PATH_PREFIX", "backups/"),

		// Platform-DB-specific sub-prefix. Trailing slash so concatenation
		// with a YYYY-MM-DD path segment yields well-formed S3 keys without
		// the producer needing to know whether the parent ends in a slash.
		PlatformBackupS3Prefix: getenv("PLATFORM_BACKUP_S3_PREFIX", "platform-backups/"),

		// Storage-quota suspend/unsuspend infra credentials (P0-3/P0-4 fix).
		// All three are optional — when absent the infra-revoke step is
		// skipped (fail-open) and only the status row is updated.
		CustomerDatabaseURL: os.Getenv("CUSTOMER_DATABASE_URL"),
		MongoAdminURI:       os.Getenv("MONGO_ADMIN_URI"),
		CustomerRedisURL:    os.Getenv("CUSTOMER_REDIS_URL"),

		// AUTH-004 synthetic prober. All optional — defaults applied
		// inside jobs.AuthProbeConfig.Defaults() so a missing env var
		// still runs the prober against prod.
		AuthProbeBaseURL:     os.Getenv("AUTH_PROBE_BASE_URL"),
		AuthProbeEmail:       os.Getenv("AUTH_PROBE_EMAIL"),
		AuthProbeReturnTo:    os.Getenv("AUTH_PROBE_RETURN_TO"),
		AuthProbeOrigin:      os.Getenv("AUTH_PROBE_ORIGIN"),
		AuthProbeBearerToken: os.Getenv("AUTH_PROBE_BEARER_TOKEN"),

		// Hourly synthetic deploy prober — all optional; defaults applied
		// inside jobs.DeployProbeConfig.Defaults() so a missing env var
		// still runs against prod. Empty BearerToken keeps the prober
		// configured-off (degraded outcomes only, no fail alerts).
		DeployProbeBaseURL:     os.Getenv("DEPLOY_PROBE_BASE_URL"),
		DeployProbeDeployHost:  os.Getenv("DEPLOY_PROBE_DEPLOY_HOST"),
		DeployProbeBearerToken: os.Getenv("DEPLOY_PROBE_BEARER_TOKEN"),
	}

	// Fall back to the shared object-store bucket when the operator hasn't
	// carved out a dedicated backup bucket yet.
	if cfg.BackupS3Bucket == "" {
		cfg.BackupS3Bucket = cfg.ObjectStoreBucket
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
		"email_provider", cfg.EmailProvider,
		"brevo_key_set", cfg.BrevoAPIKey != "",
		"brevo_template_count", len(cfg.BrevoTemplateIDs),
		"ses_region", cfg.SESAWSRegion,
		"ses_key_set", cfg.SESAWSAccessKey != "",
		"ses_from_set", cfg.SESFromEmail != "",
		"ses_template_count", len(cfg.SESTemplateNames),
		"aes_key_set", cfg.AESKey != "",
		"customer_db_set", cfg.CustomerDatabaseURL != "",
		"mongo_admin_set", cfg.MongoAdminURI != "",
		"customer_redis_set", cfg.CustomerRedisURL != "",
	)
	return cfg
}

// parseBrevoTemplateIDs decodes the BREVO_TEMPLATE_IDS env var. The expected
// shape is a JSON object mapping audit_log.kind → numeric Brevo template id:
//
//	BREVO_TEMPLATE_IDS='{"subscription.upgraded": 12, "near_quota_wall": 7}'
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

// parseSESTemplateNames decodes the SES_TEMPLATE_NAMES env var. The expected
// shape is a JSON object mapping audit_log.kind → SES template name (string,
// not numeric — SES references templates by name unlike Brevo):
//
//	SES_TEMPLATE_NAMES='{"subscription.upgraded": "tier-upgraded-v1", "near_quota_wall": "quota-wall-nudge-v1"}'
//
// Empty string returns an empty map (SES will then SkipNoTemplate on every
// send — operator opted into "AWS creds set, no templates yet"). A malformed
// value logs an ERROR and returns empty so the worker still boots — easier
// to debug a SkipNoTemplate stream than a boot crash from a fat-fingered JSON.
func parseSESTemplateNames(raw string) map[string]string {
	if raw == "" {
		return map[string]string{}
	}
	m := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		slog.Error("worker.config.ses_template_names_invalid",
			"error", err,
			"note", "SES_TEMPLATE_NAMES must be a JSON object of kind→string; falling back to empty map",
		)
		return map[string]string{}
	}
	return m
}
