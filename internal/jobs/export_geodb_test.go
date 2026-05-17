package jobs

// export_geodb_test.go — test-only exports for geodb.go internals. Compiled
// only under `go test` (filename ends in _test.go). The jobs_test external
// package uses these to exercise the GeoLite2 tarball-extraction path without
// hitting MaxMind's real download endpoint.

import (
	"io"
	"time"
)

// ExtractGeoLite2MMDB exports extractGeoLite2MMDB for the extraction test.
func ExtractGeoLite2MMDB(r io.Reader, dstPath string) error {
	return extractGeoLite2MMDB(r, dstPath)
}

// GeoDBIsFresh exports geoDBIsFresh for the P1-D freshness-gate test.
func GeoDBIsFresh(path string, now time.Time) bool {
	return geoDBIsFresh(path, now)
}

// GeoLite2MaxAge exports the freshness window constant for tests.
const GeoLite2MaxAge = geoLite2MaxAge

// GeoLite2RefreshInterval exports the periodic backstop interval for tests.
const GeoLite2RefreshInterval = geoLite2RefreshInterval
