package jobs

// export_geodb_test.go — test-only exports for geodb.go internals. Compiled
// only under `go test` (filename ends in _test.go). The jobs_test external
// package uses these to exercise the GeoLite2 tarball-extraction path without
// hitting MaxMind's real download endpoint.

import "io"

// ExtractGeoLite2MMDB exports extractGeoLite2MMDB for the extraction test.
func ExtractGeoLite2MMDB(r io.Reader, dstPath string) error {
	return extractGeoLite2MMDB(r, dstPath)
}
