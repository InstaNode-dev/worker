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
	MinioEndpoint     string // MINIO_ENDPOINT — host:port of MinIO server
	MinioRootUser     string // MINIO_ROOT_USER — MinIO admin credentials
	MinioRootPassword string // MINIO_ROOT_PASSWORD
	MinioBucketName   string // MINIO_BUCKET_NAME — shared bucket name (default: "instant-shared")
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
		MinioEndpoint:     os.Getenv("MINIO_ENDPOINT"),
		MinioRootUser:     os.Getenv("MINIO_ROOT_USER"),
		MinioRootPassword: os.Getenv("MINIO_ROOT_PASSWORD"),
		MinioBucketName:   getenv("MINIO_BUCKET_NAME", "instant-shared"),
		KubeNamespaceApps: getenv("KUBE_NAMESPACE_APPS", "instant-apps"),
	}

	slog.Info("worker.config.loaded",
		"environment", cfg.Environment,
		"provisioner_addr_set", cfg.ProvisionerAddr != "",
		"resend_key_set", cfg.ResendAPIKey != "",
	)
	return cfg
}
