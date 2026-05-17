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

const geoLite2DownloadURL = "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key=%s&suffix=tar.gz"

// geoLite2MMDBSuffix is the file-name suffix of the MMDB member inside the
// MaxMind tarball. The archive contains a dated directory
// (GeoLite2-City_<date>/) holding GeoLite2-City.mmdb plus license/readme
// text files; we extract only the *.mmdb member by suffix-match.
const geoLite2MMDBSuffix = ".mmdb"

// Work downloads the latest GeoLite2 City MMDB and atomically replaces the existing file.
func (w *RefreshGeoDBWorker) Work(ctx context.Context, job *river.Job[RefreshGeoDBArgs]) error {
	if job.Args.LicenseKey == "" {
		slog.Warn("jobs.refresh_geodb.skipped: no license key configured")
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
	defer resp.Body.Close()

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
		os.Remove(tmpPath)
		return fmt.Errorf("RefreshGeoDBWorker: extract failed: %w", err)
	}

	if err := os.Rename(tmpPath, job.Args.DBPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("RefreshGeoDBWorker: rename failed: %w", err)
	}

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
	defer gz.Close()

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
			f.Close()
			return fmt.Errorf("extractGeoLite2MMDB: write temp file: %w", err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("extractGeoLite2MMDB: close temp file: %w", err)
		}
		return nil
	}
	return fmt.Errorf("extractGeoLite2MMDB: no %s member found in archive", geoLite2MMDBSuffix)
}
