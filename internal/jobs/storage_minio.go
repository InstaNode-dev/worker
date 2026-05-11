package jobs

import (
	"context"
	"fmt"
	"strings"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// minioObjectLister is the narrow surface of *minio.Client that the scanner
// needs. Lifted to an interface so tests can feed in a fake without standing
// up a real MinIO process — matches the storage_test.go pattern of mocking
// the StorageBytesProvider rather than the gRPC client.
//
// BucketExists / ListObjects / ListIncompleteUploads mirror exactly the
// admin-credential code path in
// provisioner/internal/backend/storage/minio.go, so usage numbers stay
// consistent whether the row is queried via gRPC or the new direct path.
type minioObjectLister interface {
	BucketExists(ctx context.Context, bucketName string) (bool, error)
	ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
	ListIncompleteUploads(ctx context.Context, bucketName, prefix string, recursive bool) <-chan minio.ObjectMultipartInfo
}

// minioStorageScanner implements MinIOStorageScanner.
//
// Object prefix derivation matches api/internal/providers/storage/local.go
// (first 8 chars of token + "/") and the provisioner-side scanner, so the
// worker reports identical numbers to what the API allocated.
type minioStorageScanner struct {
	client     minioObjectLister
	bucketName string
}

// NewMinIOStorageScanner constructs a scanner backed by github.com/minio/minio-go/v7
// using the same root/admin credentials the worker already loads from
// MINIO_ENDPOINT / MINIO_ROOT_USER / MINIO_ROOT_PASSWORD (see config.go).
//
// Returns nil + error when the endpoint is empty or the client can't be built;
// callers should fail open and pass nil to NewUpdateStorageBytesWorker.
func NewMinIOStorageScanner(endpoint, accessKey, secretKey, bucketName string) (*minioStorageScanner, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("storage_minio: MINIO_ENDPOINT is required")
	}
	if bucketName == "" {
		bucketName = "instant-shared"
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("storage_minio: new client for %s: %w", endpoint, err)
	}
	return &minioStorageScanner{client: client, bucketName: bucketName}, nil
}

// newMinIOScannerWithClient is a test seam: lets storage_minio_test.go inject
// a fake minioObjectLister without dialing a real MinIO server.
func newMinIOScannerWithClient(client minioObjectLister, bucketName string) *minioStorageScanner {
	if bucketName == "" {
		bucketName = "instant-shared"
	}
	return &minioStorageScanner{client: client, bucketName: bucketName}
}

// minioObjectPrefix returns the S3 key prefix for a storage resource.
// Matches api/internal/providers/storage/local.go (Provision) and the
// provisioner backend.
func minioObjectPrefix(token, providerResourceID string) string {
	p := strings.TrimSpace(providerResourceID)
	if p != "" {
		if !strings.HasSuffix(p, "/") {
			p += "/"
		}
		return p
	}
	if token == "" {
		return ""
	}
	pfx := token
	if len(pfx) > 8 {
		pfx = pfx[:8]
	}
	return pfx + "/"
}

// StorageBytes returns the total size in bytes under the tenant prefix:
// committed objects (all versions when versioning is enabled, excluding
// delete markers and zero-byte directory placeholders) plus incomplete
// multipart uploads.
func (s *minioStorageScanner) StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error) {
	prefix := minioObjectPrefix(token, providerResourceID)
	if prefix == "" {
		return 0, fmt.Errorf("storage_minio.StorageBytes: empty token and provider_resource_id")
	}

	exists, err := s.client.BucketExists(ctx, s.bucketName)
	if err != nil {
		return 0, fmt.Errorf("storage_minio.StorageBytes: bucket exists %q: %w", s.bucketName, err)
	}
	if !exists {
		return 0, fmt.Errorf("storage_minio.StorageBytes: bucket %q does not exist", s.bucketName)
	}

	var total int64
	for obj := range s.client.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
		Prefix:       prefix,
		Recursive:    true,
		WithVersions: true,
	}) {
		if obj.Err != nil {
			return 0, fmt.Errorf("storage_minio.StorageBytes: list objects under %q: %w", prefix, obj.Err)
		}
		if obj.IsDeleteMarker {
			continue
		}
		if strings.HasSuffix(obj.Key, "/") && obj.Size == 0 {
			continue
		}
		total += obj.Size
	}

	for part := range s.client.ListIncompleteUploads(ctx, s.bucketName, prefix, true) {
		if part.Err != nil {
			return 0, fmt.Errorf("storage_minio.StorageBytes: list multipart under %q: %w", prefix, part.Err)
		}
		total += part.Size
	}

	return total, nil
}
