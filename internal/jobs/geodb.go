package jobs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
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

	// Write to a temp file first, then atomically rename.
	tmpPath := job.Args.DBPath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("RefreshGeoDBWorker: create temp file: %w", err)
	}

	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("RefreshGeoDBWorker: write temp file: %w", err)
	}
	f.Close()

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
