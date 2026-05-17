package jobs_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"instant.dev/worker/internal/jobs"
)

// buildGeoLite2Tarball produces a gzipped tarball shaped like MaxMind's real
// GeoLite2-City download: a dated parent directory holding the .mmdb file plus
// COPYRIGHT.txt / LICENSE.txt text members. mmdbContent is written as the body
// of the .mmdb member.
func buildGeoLite2Tarball(t *testing.T, mmdbContent []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	const dir = "GeoLite2-City_20260517/"
	writeEntry := func(name string, typeflag byte, body []byte) {
		hdr := &tar.Header{
			Name:     name,
			Typeflag: typeflag,
			Size:     int64(len(body)),
			Mode:     0o644,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("WriteHeader %s: %v", name, err)
		}
		if len(body) > 0 {
			if _, err := tw.Write(body); err != nil {
				t.Fatalf("Write %s: %v", name, err)
			}
		}
	}

	// Dated parent directory entry.
	writeEntry(dir, tar.TypeDir, nil)
	// License/readme text files — must be skipped by the extractor.
	writeEntry(dir+"COPYRIGHT.txt", tar.TypeReg, []byte("copyright text"))
	writeEntry(dir+"LICENSE.txt", tar.TypeReg, []byte("license text"))
	// The actual database — the only member the extractor must write out.
	writeEntry(dir+"GeoLite2-City.mmdb", tar.TypeReg, mmdbContent)

	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

// TestExtractGeoLite2MMDB_ExtractsMMDBMember proves the refresh job gunzips +
// untars MaxMind's archive and writes the .mmdb member (NOT the raw .tar.gz)
// to the destination. This is the regression guard for the P2 bug where the
// raw tarball bytes were io.Copy'd straight to the .mmdb path, corrupting the
// database after every refresh.
func TestExtractGeoLite2MMDB_ExtractsMMDBMember(t *testing.T) {
	mmdbContent := []byte("FAKE-MMDB-DATABASE-BYTES")
	tarball := buildGeoLite2Tarball(t, mmdbContent)

	dst := filepath.Join(t.TempDir(), "GeoLite2-City.mmdb.tmp")
	if err := jobs.ExtractGeoLite2MMDB(bytes.NewReader(tarball), dst); err != nil {
		t.Fatalf("ExtractGeoLite2MMDB: %v", err)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if !bytes.Equal(got, mmdbContent) {
		t.Errorf("extracted content mismatch:\n  got  %q\n  want %q", got, mmdbContent)
	}
	// Sanity: the destination must NOT be the gzip stream itself. A gzip
	// stream starts with the magic bytes 0x1f 0x8b — the corrupt-DB bug wrote
	// exactly those.
	if len(got) >= 2 && got[0] == 0x1f && got[1] == 0x8b {
		t.Error("destination still holds gzip-magic bytes — tarball was not extracted")
	}
}

// TestExtractGeoLite2MMDB_NoMMDBMember proves the extractor errors (rather than
// silently producing an empty/garbage file) when the archive has no .mmdb.
func TestExtractGeoLite2MMDB_NoMMDBMember(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	hdr := &tar.Header{Name: "README.txt", Typeflag: tar.TypeReg, Size: 3, Mode: 0o644}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	if _, err := tw.Write([]byte("abc")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	tw.Close()
	gz.Close()

	dst := filepath.Join(t.TempDir(), "out.mmdb.tmp")
	err := jobs.ExtractGeoLite2MMDB(bytes.NewReader(buf.Bytes()), dst)
	if err == nil {
		t.Fatal("expected error for archive with no .mmdb member, got nil")
	}
	if !strings.Contains(err.Error(), "no .mmdb member") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestExtractGeoLite2MMDB_NotGzip proves a non-gzip stream (e.g. an HTML error
// page from MaxMind) is rejected rather than written as a corrupt database.
func TestExtractGeoLite2MMDB_NotGzip(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "out.mmdb.tmp")
	err := jobs.ExtractGeoLite2MMDB(strings.NewReader("<html>not a tarball</html>"), dst)
	if err == nil {
		t.Fatal("expected error for non-gzip input, got nil")
	}
}
