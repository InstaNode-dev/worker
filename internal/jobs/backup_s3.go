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
	"strings"
	"time"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

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

// retentionDaysForTier returns the hard-delete cutoff for a backup row given
// its tier_at_backup. anonymous never schedules backups (the scheduler skips
// it) but we still treat the tier as 7-day-equivalent here in case a manual
// row sneaks through from the api side — safer than panicking on an
// unrecognized tier.
func retentionDaysForTier(tier string) int {
	switch tier {
	case "hobby":
		return 7
	case "pro":
		return 30
	case "growth":
		return 30
	case "team":
		return 90
	case "anonymous":
		return 7
	default:
		return 7
	}
}

// retentionCutoff returns the timestamp before which backups of the given
// tier should be hard-deleted. Pure function, exported only for tests.
func retentionCutoff(tier string, now time.Time) time.Time {
	return now.UTC().Add(-time.Duration(retentionDaysForTier(tier)) * 24 * time.Hour)
}
