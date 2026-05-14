package jobs

// platform_db_backup_s3.go — concrete s3Client used by
// PlatformDBBackupWorker in production. Speaks plain S3 against any
// endpoint (DO Spaces / AWS / MinIO / R2 / GCS / Wasabi / B2) via the
// minio-go SDK we already depend on for the storage_bytes scanner.
//
// Kept in a separate file from platform_db_backup.go so the worker file
// stays focused on the job logic and so the test file can compile
// without minio-go in its import graph (the worker tests use a fakeS3).

import (
	"context"
	"fmt"
	"io"
	"strings"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// minioS3 is the production s3Client. It wraps *minio.Client behind the
// three narrow surfaces PlatformDBBackupWorker needs (upload, list,
// delete). minio.Client already satisfies plain S3 against managed
// providers; the wrapper does endpoint/scheme normalisation that matches
// storage_minio.go::NewMinIOStorageScanner so operators get one
// consistent set of OBJECT_STORE_* semantics across the worker.
type minioS3 struct {
	client *minio.Client
}

// NewBackupS3Client constructs the production s3Client for backup
// uploads. Endpoint + creds are sourced from the OBJECT_STORE_* env vars
// (see config.Load) — the SAME bucket/endpoint as the storage_bytes
// scanner. If endpoint is empty the function returns nil + an error and
// the worker is constructed in disabled mode (see Work's S3==nil branch).
//
// TLS handling mirrors NewMinIOStorageScanner so an operator who has
// already configured OBJECT_STORE_ENDPOINT for the storage scanner does
// not need to learn a second set of rules.
func NewBackupS3Client(endpoint, accessKey, secretKey string) (s3Client, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("platform_db_backup: OBJECT_STORE_ENDPOINT is required")
	}
	secure := false
	switch {
	case strings.HasPrefix(endpoint, "https://"):
		endpoint = strings.TrimPrefix(endpoint, "https://")
		secure = true
	case strings.HasPrefix(endpoint, "http://"):
		endpoint = strings.TrimPrefix(endpoint, "http://")
		secure = false
	default:
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
		return nil, fmt.Errorf("platform_db_backup: new minio client for %s: %w", endpoint, err)
	}
	return &minioS3{client: client}, nil
}

// Upload streams body to the (bucket, key). size=-1 → multipart.
func (m *minioS3) Upload(ctx context.Context, bucket, key string, body io.Reader, size int64) error {
	_, err := m.client.PutObject(ctx, bucket, key, body, size, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return fmt.Errorf("PutObject %s/%s: %w", bucket, key, err)
	}
	return nil
}

// List returns every object key under prefix.
func (m *minioS3) List(ctx context.Context, bucket, prefix string) ([]string, error) {
	var keys []string
	for obj := range m.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("list %s/%s: %w", bucket, prefix, obj.Err)
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

// Delete removes a single object. NotFound is NOT treated as an error —
// retention sweeps can race against an operator deletion or a previous
// failed sweep's partial completion.
func (m *minioS3) Delete(ctx context.Context, bucket, key string) error {
	if err := m.client.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("RemoveObject %s/%s: %w", bucket, key, err)
	}
	return nil
}
