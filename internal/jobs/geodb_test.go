package jobs_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

// ── P1-D: GeoLite2 refresh freshness gate ─────────────────────────────────────

// TestGeoDBIsFresh_RecentMarkerIsFresh proves that when a fetch marker
// written within GeoLite2MaxAge sits beside the .mmdb, the DB is reported
// fresh — so Work() skips the MaxMind download on a routine worker restart
// instead of refetching every time.
//
// BugBash 2026-05-18 P3: freshness is now keyed on the worker-written fetch
// marker, not the .mmdb file's own mtime.
func TestGeoDBIsFresh_RecentMarkerIsFresh(t *testing.T) {
	path := filepath.Join(t.TempDir(), "GeoLite2-City.mmdb")
	if err := os.WriteFile(path, []byte("mmdb"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".fetched", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if !jobs.GeoDBIsFresh(path, time.Now()) {
		t.Error("a .mmdb with a just-written fetch marker must be reported fresh; otherwise every worker restart refetches from MaxMind")
	}
}

// TestGeoDBIsFresh_BakedInDBWithoutMarkerIsNotFresh is the core BugBash
// 2026-05-18 P3 guarantee: a recently-modified .mmdb with NO fetch marker
// (the copy baked into the worker image — its mtime is the image build
// time, not the MaxMind publish time) is reported NOT fresh, so the first
// run after a deploy actually refetches.
func TestGeoDBIsFresh_BakedInDBWithoutMarkerIsNotFresh(t *testing.T) {
	path := filepath.Join(t.TempDir(), "GeoLite2-City.mmdb")
	if err := os.WriteFile(path, []byte("mmdb"), 0o644); err != nil {
		t.Fatal(err)
	}
	// No marker file — this is exactly the baked-in-image state.
	if jobs.GeoDBIsFresh(path, time.Now()) {
		t.Error("a recently-written .mmdb with NO fetch marker is the baked-in image copy; it must NOT be reported fresh — the first refresh after deploy must run")
	}
}

// TestGeoDBIsFresh_StaleMarkerIsNotFresh proves a fetch marker older than
// GeoLite2MaxAge is reported NOT fresh — so Work() proceeds with the
// refresh. The geo DB does NOT stay stale forever on a long-lived worker.
func TestGeoDBIsFresh_StaleMarkerIsNotFresh(t *testing.T) {
	path := filepath.Join(t.TempDir(), "GeoLite2-City.mmdb")
	if err := os.WriteFile(path, []byte("mmdb"), 0o644); err != nil {
		t.Fatal(err)
	}
	marker := path + ".fetched"
	if err := os.WriteFile(marker, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	// Backdate the marker mtime well past the freshness window.
	old := time.Now().Add(-2 * jobs.GeoLite2MaxAge)
	if err := os.Chtimes(marker, old, old); err != nil {
		t.Fatal(err)
	}
	if jobs.GeoDBIsFresh(path, time.Now()) {
		t.Errorf("a fetch marker older than GeoLite2MaxAge (%s) must NOT be fresh — Work() must refresh it", jobs.GeoLite2MaxAge)
	}
}

// TestGeoDBIsFresh_MissingFileIsNotFresh proves a missing file is treated as
// not-fresh so the very first refresh always proceeds.
func TestGeoDBIsFresh_MissingFileIsNotFresh(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist.mmdb")
	if jobs.GeoDBIsFresh(missing, time.Now()) {
		t.Error("a missing geo DB file must NOT be reported fresh — the first refresh must run")
	}
	if jobs.GeoDBIsFresh("", time.Now()) {
		t.Error("an empty path must NOT be reported fresh")
	}
}

// TestGeoLite2RefreshInterval_AlignsWithMaxAge guards the P1-D contract: the
// periodic backstop interval must not exceed the freshness window, or a
// never-redeployed worker could let the geo DB drift past stale before the
// next periodic tick. Both are derived from the same 7-day window.
func TestGeoLite2RefreshInterval_AlignsWithMaxAge(t *testing.T) {
	if jobs.GeoLite2RefreshInterval > jobs.GeoLite2MaxAge {
		t.Errorf("GeoLite2RefreshInterval (%s) must be <= GeoLite2MaxAge (%s); "+
			"a longer interval lets the geo DB go stale on a long-lived worker before the backstop fires",
			jobs.GeoLite2RefreshInterval, jobs.GeoLite2MaxAge)
	}
}

// TestGeoDBPeriodicJob_RunsOnStart is the P1-D regression guard. The refresh
// periodic job MUST be registered with RunOnStart:true — with RunOnStart
// false (the original bug) a frequently-redeployed worker keeps resetting
// the long periodic timer and the job never fires, leaving the geo DB the
// stale baked-in copy forever. This scans workers.go for the geodb periodic
// job block and fails if it is not RunOnStart:true.
func TestGeoDBPeriodicJob_RunsOnStart(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("workers.go"))
	if err != nil {
		t.Fatalf("read workers.go: %v", err)
	}
	body := string(src)
	idx := strings.Index(body, "RefreshGeoDBArgs{")
	if idx < 0 {
		t.Fatal("workers.go no longer registers RefreshGeoDBArgs — geodb refresh job missing")
	}
	// Inspect the ~400 bytes following the geodb job builder — that window
	// covers the PeriodicJobOpts literal for this job.
	end := idx + 400
	if end > len(body) {
		end = len(body)
	}
	window := body[idx:end]
	if !strings.Contains(window, "RunOnStart: true") {
		t.Errorf("the GeoLite2 refresh periodic job must be registered with "+
			"RunOnStart: true (P1-D) — otherwise a redeployed worker never runs it. "+
			"Block inspected:\n%s", window)
	}
}
