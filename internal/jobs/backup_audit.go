package jobs

// Audit-log kinds emitted by the customer-backup jobs (scheduler + runner +
// restore runner). Kept in a dedicated file so the api side has one place to
// look when adding the matching reader (recent-activity feed, NR widgets,
// retention reports) and the event-email forwarder mapping if/when these
// graduate to lifecycle email triggers.
//
// The literal strings are the public contract — once a row is written the
// kind is queryable by external consumers, so renames here are breaking
// changes for any downstream that pinned the value.
const (
	auditKindBackupStarted   = "backup.started"
	auditKindBackupSucceeded = "backup.succeeded"
	auditKindBackupFailed    = "backup.failed"

	auditKindRestoreStarted   = "restore.started"
	auditKindRestoreSucceeded = "restore.succeeded"
	auditKindRestoreFailed    = "restore.failed"

	// Common actor for system-driven backup events. A manual backup (kicked
	// off by POST /api/v1/resources/:id/backup) still flows through the
	// worker runner, so the actor remains "system" on the worker side — the
	// api will have written its own kind=backup.requested row with
	// actor="user" at request time.
	backupActor = "system"
)
