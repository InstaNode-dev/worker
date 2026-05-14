package jobs

// team_deletion_s3_adapter.go — bridges the existing minioStorageScanner
// (which exposes ListObjects via a private client field) to the
// S3BackupDeleter interface the executor needs.
//
// The scanner uses minioObjectLister, which carries ListObjects but NOT
// RemoveObjects. We extend the surface here so the executor can perform
// bulk deletes against the same S3 backend the scanner already reaches.
//
// Why an adapter file rather than expanding minioObjectLister: the
// existing scanner is read-only by design — its tests assert that no
// destructive method is ever called. Adding RemoveObjects to the
// scanner's interface would pollute the read-only contract. The
// adapter keeps deletion strictly opt-in for the executor wiring.

import (
	"context"

	minio "github.com/minio/minio-go/v7"
)

// minioBackupDeleter implements S3BackupDeleter by holding the same
// *minio.Client the scanner already constructed. We avoid reaching into
// the scanner's private field via reflection by exposing a tiny accessor
// (clientForDeletion) on the scanner type below.
type minioBackupDeleter struct {
	client     *minio.Client
	bucketName string
}

func (d *minioBackupDeleter) ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	return d.client.ListObjects(ctx, bucketName, opts)
}

func (d *minioBackupDeleter) RemoveObjects(ctx context.Context, bucketName string, objectsCh <-chan minio.ObjectInfo, opts minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError {
	return d.client.RemoveObjects(ctx, bucketName, objectsCh, opts)
}

// newMinIOBackupDeleter constructs an S3BackupDeleter from an existing
// minioStorageScanner. Returns nil when the scanner is nil (CI / no
// OBJECT_STORE_* env vars), matching the fail-open contract upstream
// in StartWorkers.
//
// The scanner's underlying client is a real *minio.Client (constructed
// in NewMinIOStorageScanner via minio.New). Tests that use the
// newMinIOScannerWithClient seam pass a fake — in that case this
// adapter returns nil because the fake doesn't implement RemoveObjects.
func newMinIOBackupDeleter(scanner *minioStorageScanner) S3BackupDeleter {
	if scanner == nil {
		return nil
	}
	mc, ok := scanner.client.(*minio.Client)
	if !ok || mc == nil {
		// Test seam path — the scanner holds a fake. Tombstone
		// destruction will fall through to "no s3 deleter wired" which
		// is the right behaviour under fakes.
		return nil
	}
	return &minioBackupDeleter{
		client:     mc,
		bucketName: scanner.bucketName,
	}
}
