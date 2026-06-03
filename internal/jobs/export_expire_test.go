package jobs

// export_expire_test.go — test-only exports for the expire*.go keyset batch
// limits so the external (jobs_test) test package can reference them in
// sqlmock WithArgs(cursor, limit) expectations without re-declaring the magic
// numbers (CLAUDE.md: "Use named constants, not inline strings"). Only visible
// to _test.go files because the file ends in _test.go.

// ExpireScanBatchLimit exports expireScanBatchLimit — the keyset page size the
// ExpireAnonymousWorker reaper batch SELECT uses.
const ExpireScanBatchLimit = expireScanBatchLimit

// ExpireStacksScanBatchLimit exports expireStacksScanBatchLimit — the keyset
// page size the ExpireStacksWorker reaper batch SELECT uses.
const ExpireStacksScanBatchLimit = expireStacksScanBatchLimit
