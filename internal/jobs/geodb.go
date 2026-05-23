package jobs

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/riverqueue/river"
)

// RefreshGeoDBArgs holds arguments for the GeoLite2 refresh job.
type RefreshGeoDBArgs struct {
	LicenseKey string `json:"license_key"`
	DBPath     string `json:"db_path"`
}

func (RefreshGeoDBArgs) Kind() string { return "refresh_geodb" }

// RefreshGeoDBWorker downloads a fresh copy of the MaxMind GeoLite2 City database.
type RefreshGeoDBWorker struct {
	river.WorkerDefaults[RefreshGeoDBArgs]
}

// NewRefreshGeoDBWorker constructs a RefreshGeoDBWorker.
func NewRefreshGeoDBWorker() *RefreshGeoDBWorker {
	return &RefreshGeoDBWorker{}
}

// geoLite2DownloadURL is the MaxMind GeoLite2-City download template (the
// license key is interpolated in). It is a package var rather than a const
// so the happy-path download→extract→rename pipeline in Work can be driven
// against an httptest server in tests; production never reassigns it.
var geoLite2DownloadURL = "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key=%s&suffix=tar.gz"

// geoLite2MMDBSuffix is the file-name suffix of the MMDB member inside the
// MaxMind tarball. The archive contains a dated directory
// (GeoLite2-City_<date>/) holding GeoLite2-City.mmdb plus license/readme
// text files; we extract only the *.mmdb member by suffix-match.
const geoLite2MMDBSuffix = ".mmdb"

// geoLite2MaxAge is the freshness window for the on-disk MMDB (P1-D).
//
// The refresh job is registered with RunOnStart=true so a freshly-deployed
// worker always re-checks the geo DB — without this the periodic 30-day
// timer keeps resetting on every redeploy and the job effectively never
// runs, leaving the stale baked-in copy permanently in place. To stop a
// frequently-redeployed worker from hammering MaxMind on every restart,
// Work() skips the download when the existing file's mtime is newer than
// this window. MaxMind publishes GeoLite2-City twice weekly; 7 days means
// we are at most one update cycle behind while still skipping the refetch
// on routine restarts.
const geoLite2MaxAge = 7 * 24 * time.Hour

// geoLite2RefreshInterval is the periodic backstop cadence for the refresh
// job. With RunOnStart=true the job runs on every worker restart, so this
// interval only matters for a worker that is never redeployed. 7 days keeps
// it aligned with geoLite2MaxAge — a long-lived worker still re-checks once
// per freshness window.
const geoLite2RefreshInterval = 7 * 24 * time.Hour

// geoLite2FetchMarkerSuffix is appended to the DBPath to form the
// fetch-marker filename (e.g. /app/GeoLite2-City.mmdb.fetched). The marker
// is written ONLY by a successful download in Work(); its mtime records
// when this worker last actually pulled a fresh copy from MaxMind.
//
// BugBash 2026-05-18 P3 ("geodb freshness keyed on mtime"): the previous
// freshness gate stat'd the .mmdb file itself. The .mmdb shipped baked into
// the worker image has an mtime equal to the *image build time*, not the
// MaxMind publish time — so a freshly-deployed worker carrying a months-old
// baked-in DB sees a recent mtime and skips the refresh, leaving stale geo
// data permanently in place. Keying freshness on a marker the worker writes
// itself fixes that: a baked-in image has no marker, so the gate reports
// "not fresh" and the download proceeds on first run.
//
// A full content checksum (verifying the .mmdb bytes against MaxMind's
// published SHA256) needs a persistent record of the last-fetched digest —
// a fetch-state store this worker does not have. That remains a follow-up;
// the marker approach is the achievable mechanical fix and closes the
// "baked-in DB looks fresh" hole on its own.
const geoLite2FetchMarkerSuffix = ".fetched"

// geoDBIsFresh reports whether this worker has successfully fetched the
// GeoLite2 DB within geoLite2MaxAge. Freshness is keyed on the fetch-marker
// file (path + geoLite2FetchMarkerSuffix), NOT on the .mmdb file's own
// mtime — see geoLite2FetchMarkerSuffix for why. A missing marker, a
// missing .mmdb, or an unstattable file is treated as NOT fresh so the
// refresh proceeds. now is injected for deterministic tests.
func geoDBIsFresh(path string, now time.Time) bool {
	if path == "" {
		return false
	}
	// The .mmdb itself must exist — a fresh marker with no DB beside it is
	// meaningless (e.g. the DB was deleted out of band).
	if _, err := os.Stat(path); err != nil {
		return false
	}
	info, err := os.Stat(geoDBFetchMarkerPath(path))
	if err != nil {
		// No marker → this worker has never fetched; the .mmdb present is
		// the baked-in image copy. Treat as stale so the download runs.
		return false
	}
	return now.Sub(info.ModTime()) < geoLite2MaxAge
}

// geoDBFetchMarkerPath returns the fetch-marker path for a given DBPath.
func geoDBFetchMarkerPath(dbPath string) string {
	return dbPath + geoLite2FetchMarkerSuffix
}

// touchGeoDBFetchMarker creates/updates the fetch-marker file so its mtime
// records this successful fetch. Best-effort: a marker write failure is
// logged but does not fail the job — the worst case is the next run
// refetches an already-fresh DB, which is harmless.
func touchGeoDBFetchMarker(dbPath string) {
	markerPath := geoDBFetchMarkerPath(dbPath)
	f, err := os.Create(markerPath)
	if err != nil {
		slog.Warn("jobs.refresh_geodb.fetch_marker_write_failed",
			"path", markerPath, "error", err)
		return
	}
	_ = f.Close()
}

// Work downloads the latest GeoLite2 City MMDB and atomically replaces the existing file.
func (w *RefreshGeoDBWorker) Work(ctx context.Context, job *river.Job[RefreshGeoDBArgs]) error {
	if job.Args.LicenseKey == "" {
		slog.Warn("jobs.refresh_geodb.skipped: no license key configured")
		return nil
	}

	// Freshness gate (P1-D): with RunOnStart=true the job fires on every
	// worker restart; skip the MaxMind download when the on-disk copy is
	// still within geoLite2MaxAge so routine redeploys don't refetch.
	if geoDBIsFresh(job.Args.DBPath, time.Now()) {
		slog.Info("jobs.refresh_geodb.skipped_fresh",
			"path", job.Args.DBPath,
			"max_age", geoLite2MaxAge.String(),
		)
		return nil
	}

	downloadURL := fmt.Sprintf(geoLite2DownloadURL, job.Args.LicenseKey)

	httpClient := &http.Client{Timeout: 5 * time.Minute}
	req, err := http.NewRequestWithContext(ctx, "GET", downloadURL, nil)
	if err != nil {
		return fmt.Errorf("RefreshGeoDBWorker: build request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("RefreshGeoDBWorker: download failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("RefreshGeoDBWorker: unexpected status %d", resp.StatusCode)
	}

	// MaxMind serves a gzipped tarball (suffix=tar.gz), NOT a bare .mmdb file.
	// The archive holds a dated directory (GeoLite2-City_<date>/) containing
	// GeoLite2-City.mmdb plus license/readme text files. Writing the raw
	// .tar.gz bytes straight to the .mmdb path corrupts the database — the
	// maxminddb reader cannot parse a gzip stream. We must gunzip + untar and
	// extract only the *.mmdb member before the atomic rename.
	tmpPath := job.Args.DBPath + ".tmp"
	if err := extractGeoLite2MMDB(resp.Body, tmpPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("RefreshGeoDBWorker: extract failed: %w", err)
	}

	if err := os.Rename(tmpPath, job.Args.DBPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("RefreshGeoDBWorker: rename failed: %w", err)
	}

	// Stamp the fetch marker so geoDBIsFresh keys the next freshness check
	// on this actual download rather than the .mmdb file's mtime — see
	// geoLite2FetchMarkerSuffix (BugBash 2026-05-18 P3).
	touchGeoDBFetchMarker(job.Args.DBPath)

	slog.Info("jobs.refresh_geodb.completed",
		"path", job.Args.DBPath,
		"job_id", job.ID,
	)
	return nil
}

// extractGeoLite2MMDB gunzips + untars the MaxMind GeoLite2 archive stream and
// writes the single *.mmdb member to dstPath. It returns an error if the
// stream is not a valid gzip tarball or if no .mmdb member is present.
func extractGeoLite2MMDB(r io.Reader, dstPath string) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("extractGeoLite2MMDB: gzip reader: %w", err)
	}
	defer func() { _ = gz.Close() }()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("extractGeoLite2MMDB: read tar entry: %w", err)
		}
		// Only a regular file whose name ends in .mmdb is the database we want.
		// The archive also contains COPYRIGHT.txt / LICENSE.txt and the dated
		// parent directory entry — all skipped here.
		if hdr.Typeflag != tar.TypeReg || !strings.HasSuffix(hdr.Name, geoLite2MMDBSuffix) {
			continue
		}

		f, err := os.Create(dstPath)
		if err != nil {
			return fmt.Errorf("extractGeoLite2MMDB: create temp file: %w", err)
		}
		// Bound the copy to the declared tar-header size to avoid a
		// decompression-bomb writing unbounded bytes.
		if _, err := io.Copy(f, io.LimitReader(tr, hdr.Size)); err != nil {
			_ = f.Close()
			return fmt.Errorf("extractGeoLite2MMDB: write temp file: %w", err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("extractGeoLite2MMDB: close temp file: %w", err)
		}
		return nil
	}
	return fmt.Errorf("extractGeoLite2MMDB: no %s member found in archive", geoLite2MMDBSuffix)
}
