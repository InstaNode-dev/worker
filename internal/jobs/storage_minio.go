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
// against any S3-compatible endpoint (self-hosted MinIO, DO Spaces, AWS S3,
// GCS, R2, B2, Wasabi). Callers source endpoint + creds + bucket from the
// OBJECT_STORE_* env vars in config.Load (which fall back to legacy MINIO_*
// names for backward compat).
//
// Auto-detects TLS: if `endpoint` is prefixed with "https://" the scanner uses
// TLS; "http://" forces plain. Without a scheme, a heuristic kicks in — a
// hostname containing "digitaloceanspaces.com", "amazonaws.com",
// "cloudflarestorage.com", "googleapis.com", "wasabisys.com", or
// "backblazeb2.com" is assumed TLS; everything else (e.g. an in-cluster
// minio.instant-data.svc.cluster.local) is plain. Callers that need explicit
// control should call NewS3Scanner directly.
//
// Returns nil + error when the endpoint is empty or the client can't be built;
// callers should fail open and pass nil to NewUpdateStorageBytesWorker.
func NewMinIOStorageScanner(endpoint, accessKey, secretKey, bucketName string) (*minioStorageScanner, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("storage_minio: OBJECT_STORE_ENDPOINT is required")
	}
	if bucketName == "" {
		bucketName = "instant-shared"
	}

	// Strip explicit scheme prefix and remember whether TLS was requested.
	secure := false
	if strings.HasPrefix(endpoint, "https://") {
		endpoint = strings.TrimPrefix(endpoint, "https://")
		secure = true
	} else if strings.HasPrefix(endpoint, "http://") {
		endpoint = strings.TrimPrefix(endpoint, "http://")
		secure = false
	} else {
		// Heuristic: managed S3-compatible vendors all serve TLS by default.
		// In-cluster MinIO uses plain HTTP. Misidentified endpoints can be
		// fixed by explicitly prefixing http:// or https:// in the env var.
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
