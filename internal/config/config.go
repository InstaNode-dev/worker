package config

import (
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
	)
	return cfg
}
