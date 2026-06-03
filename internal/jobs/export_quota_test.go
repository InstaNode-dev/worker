package jobs

// export_quota_test.go — test-only exports for quota.go internals so the
// external (jobs_test) test package can assert against the keyset-pagination
// batch limit without re-declaring the magic number (CLAUDE.md: "Use named
// constants, not inline strings"). Only visible to _test.go files because the
// file ends in _test.go.

// QuotaScanBatchLimit exports quotaScanBatchLimit — the keyset page size the
// suspend / unsuspend / redis-eviction loops use. External tests reference
// this in their sqlmock WithArgs(cursor, status, limit) expectations so the
// limit lives in exactly one place.
const QuotaScanBatchLimit = quotaScanBatchLimit
