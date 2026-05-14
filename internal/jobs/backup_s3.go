// backup_s3.go — narrow S3 surface used by the customer-backup runner +
// retention sweep + restore runner.
//
// We keep this as a tiny interface (Upload / Download / Delete / List) instead
// of passing *minio.Client around directly so:
//
//   1. The runner / restore-runner / retention sweep tests can use a fake that
//      exercises the exact streaming path without dialing a real S3.
//   2. A future cutover from MinIO to a real DO-Spaces SDK (or AWS SDK v2) is a
//      one-file change — every consumer of this interface stays the same.
//
// All four methods take the bucket as an explicit argument so the same client
// can serve a separate retention sweep on a different bucket later (e.g.
// "instant-backups-cold" for tier=team multi-year retention).
package jobs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	commonplans "instant.dev/common/plans"
)

// BackupPlanRegistry is the minimal plans.Registry surface needed by the
// customer-backup retention sweep. *commonplans.Registry satisfies it
// directly; tests pass a small in-memory stub.
//
// Why an interface here and not just *commonplans.Registry: the runner's
// unit tests assert per-tier retention against an explicit tier→days
// mapping rather than the embedded default YAML, so they pass a tiny
// fakeBackupPlanRegistry instead of constructing a full Registry.
type BackupPlanRegistry interface {
	// BackupRetentionDays returns the per-tier retention window in days
	// from plans.yaml. 0 means "this tier does not take backups"; see
	// retentionCutoff for how the sweep interprets that (delete-now
	// rather than leave-forever, so a row that leaked in from a previous
	// tier — e.g. a Pro→Free downgrade — cannot stick around past
	// policy).
	BackupRetentionDays(tier string) int
	// TierNames lists the tier names the sweep should iterate. We sweep
	// per-tier because the SQL hits a partial index on tier_at_backup;
	// iterating an explicit list (rather than scanning DISTINCT) keeps
	// the plan-shape stable and the retention horizon source-of-truth in
	// plans.yaml.
	TierNames() []string
}

// commonPlanRegistryAdapter wraps *commonplans.Registry into the
// BackupPlanRegistry surface. We need an adapter only because the
// common package's Registry.All() returns map[string]*commonplans.Plan
// whereas the interface above is jobs-package-local; the BackupRetentionDays
// call passes through unchanged.
type commonPlanRegistryAdapter struct {
	reg *commonplans.Registry
}

// NewBackupPlanRegistry wraps a *commonplans.Registry for use with the
// customer-backup retention sweep. Returns nil if reg is nil — the
// runner then falls back to the legacy 7-day defaults via
// retentionDaysForTier so a misconfigured boot doesn't accidentally
// nuke the entire bucket.
func NewBackupPlanRegistry(reg *commonplans.Registry) BackupPlanRegistry {
	if reg == nil {
		return nil
	}
	return &commonPlanRegistryAdapter{reg: reg}
}

// BackupRetentionDays delegates to the common Registry. Returns 0 for
// unknown tiers (common's Get falls back to "anonymous", whose policy
// is 0 / no backups).
func (a *commonPlanRegistryAdapter) BackupRetentionDays(tier string) int {
	return a.reg.BackupRetentionDays(tier)
}

// TierNames returns every tier name registered in plans.yaml. Sorted
// is unnecessary — the sweep is order-independent — but stable across
// process lifetime so log lines per tier read predictably.
func (a *commonPlanRegistryAdapter) TierNames() []string {
	all := a.reg.All()
	out := make([]string, 0, len(all))
	for name := range all {
		out = append(out, name)
	}
	return out
}

// BackupObjectStore is the surface every backup-pipeline consumer needs.
// Implementations live in backup_s3.go (real, minio-backed) and the test
// files (fakeBackupStore).
type BackupObjectStore interface {
	// Upload streams r into bucket/objectKey. Returns the number of bytes
	// written so the caller can persist size_bytes without re-stat'ing the
	// object after the fact.
	Upload(ctx context.Context, bucket, objectKey string, r io.Reader) (int64, error)
	// Download returns a ReadCloser the caller MUST close. The reader
	// blocks per-chunk so a 5GB restore doesn't pin 5GB of memory.
	Download(ctx context.Context, bucket, objectKey string) (io.ReadCloser, error)
	// DeleteObject is used by the retention sweep.
	DeleteObject(ctx context.Context, bucket, objectKey string) error
}

// minioBackupStore implements BackupObjectStore against any S3-compatible
// endpoint via minio-go. Mirrors NewMinIOStorageScanner's TLS heuristic so
// the same OBJECT_STORE_ENDPOINT works against in-cluster MinIO,
// DO Spaces, AWS S3, etc.
type minioBackupStore struct {
	client *minio.Client
}

// NewMinIOBackupStore constructs a BackupObjectStore. Returns nil + error
// when the endpoint is empty — callers should fail open and pass nil to
// NewCustomerBackupRunner.
func NewMinIOBackupStore(endpoint, accessKey, secretKey string) (*minioBackupStore, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("backup_s3: OBJECT_STORE_ENDPOINT is required")
	}

	secure := false
	if strings.HasPrefix(endpoint, "https://") {
		endpoint = strings.TrimPrefix(endpoint, "https://")
		secure = true
	} else if strings.HasPrefix(endpoint, "http://") {
		endpoint = strings.TrimPrefix(endpoint, "http://")
		secure = false
	} else {
		for _, vendor := range []string{
			"digitaloceanspaces.com",
			"amazonaws.com",
			"cloudflarestorage.com",
			"googleapis.com",
			"wasabisys.com",
			"backblazeb2.com",
		} {
			if strings.Contains(endpoint, vendor) {
				secure = true
				break
			}
		}
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, fmt.Errorf("backup_s3: new client for %s: %w", endpoint, err)
	}
	return &minioBackupStore{client: client}, nil
}

// Upload streams r into bucket/objectKey. We pass size=-1 so minio-go uses
// its built-in chunked multipart path — pg_dump output size isn't known
// ahead of time because we tee it through gzip.
func (s *minioBackupStore) Upload(ctx context.Context, bucket, objectKey string, r io.Reader) (int64, error) {
	info, err := s.client.PutObject(ctx, bucket, objectKey, r, -1, minio.PutObjectOptions{
		ContentType: "application/gzip",
		// PartSize defaults to 64MiB which is fine for the 5GB single-object
		// case (78 parts) and small backups (single part).
	})
	if err != nil {
		return 0, fmt.Errorf("backup_s3.Upload: %w", err)
	}
	return info.Size, nil
}

// Download returns the object as a streaming ReadCloser. The caller MUST
// close it (the restore runner does, via defer).
func (s *minioBackupStore) Download(ctx context.Context, bucket, objectKey string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("backup_s3.Download: %w", err)
	}
	// minio-go's *Object satisfies io.ReadCloser; the underlying HTTP body
	// isn't dialed until the first Read so we can't validate existence
	// here. The runner handles a Read-time 404 by treating the row as a
	// hard failure (no retry — backup is gone).
	return obj, nil
}

// DeleteObject hard-deletes the object. Used by the retention sweep.
func (s *minioBackupStore) DeleteObject(ctx context.Context, bucket, objectKey string) error {
	if err := s.client.RemoveObject(ctx, bucket, objectKey, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("backup_s3.DeleteObject: %w", err)
	}
	return nil
}

// backupObjectKey returns the S3 key for a given resource-token + backup-id.
// Layout: <prefix>/<resource-token>/<backup-id>.dump.gz
//
// prefix is the BACKUP_S3_PATH_PREFIX env var ("backups/" by default).
// Token-bucketing matches the public-facing structure documented in the
// task spec: instant-shared/backups/<resource-token>/<backup-id>.dump.gz
// — operators can list-by-token to scope a customer-support workflow.
func backupObjectKey(prefix, resourceToken, backupID string) string {
	p := strings.TrimSuffix(prefix, "/")
	if p == "" {
		p = "backups"
	}
	return fmt.Sprintf("%s/%s/%s.dump.gz", p, resourceToken, backupID)
}

// retentionDaysForTier returns the hard-delete cutoff for a backup row
// given its tier_at_backup, reading directly from plans.yaml via the
// supplied BackupPlanRegistry. This replaces the W12 hardcoded switch
// that silently dropped Hobby Plus to default-7 and ignored
// backup_retention_days=0 for anonymous/free.
//
// Semantics:
//   - registry returns N > 0 → keep backups for N days, sweep older
//   - registry returns 0     → this tier does not back up; sweep ALL
//     existing rows for the tier (handles leaks from prior tiers, e.g.
//     a Pro→Free downgrade left old pro-tier rows tagged 'free')
//   - registry returns < 0   → unexpected (plans.yaml uses -1 only for
//     "unlimited" counts, never for retention); WARN and fall back to a
//     safe 7-day window rather than nuke or persist forever
//   - registry nil           → boot misconfigured; WARN and fall back to
//     the legacy hardcoded 7-day default so a misconfigured worker
//     can't accidentally delete every backup it sees
//
// Pure-ish: emits slog.Warn for the unexpected-value cases. Tests pin
// the happy paths via a stub BackupPlanRegistry.
func retentionDaysForTier(reg BackupPlanRegistry, tier string) int {
	const legacyDefaultDays = 7
	if reg == nil {
		slog.Warn("jobs.customer_backup_runner.retention_registry_nil",
			"tier", tier, "fallback_days", legacyDefaultDays)
		return legacyDefaultDays
	}
	days := reg.BackupRetentionDays(tier)
	if days < 0 {
		slog.Warn("jobs.customer_backup_runner.retention_negative_days",
			"tier", tier, "registry_value", days,
			"fallback_days", legacyDefaultDays)
		return legacyDefaultDays
	}
	return days
}

// retentionCutoff returns the timestamp before which backups of the
// given tier should be hard-deleted. Returns now() (UTC) when the
// registry says retention=0, so the SQL "created_at < cutoff" predicate
// matches every row of that tier — see retentionDaysForTier for why
// this is the right semantic for retention=0 (delete-now, not
// leave-forever). Pure function, used only by the sweep loop.
func retentionCutoff(reg BackupPlanRegistry, tier string, now time.Time) time.Time {
	days := retentionDaysForTier(reg, tier)
	return now.UTC().Add(-time.Duration(days) * 24 * time.Hour)
}
