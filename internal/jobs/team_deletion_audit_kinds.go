package jobs

// team_deletion_audit_kinds.go — duplicated copies of the api's
// audit-kind constants for the right-to-be-forgotten (GDPR Article 17)
// lifecycle. The api repo and the worker repo are separate Go modules
// per CLAUDE.md so a shared constants package is not in scope; the
// values MUST match the literal strings declared in
// api/internal/models/audit_kinds.go.
//
// Producer/consumer split:
//
//	team.deletion_requested  — API emits on DELETE /api/v1/team
//	team.deletion_canceled   — API emits on POST /api/v1/team/restore
//	team.tombstoned          — THIS WORKER emits on successful destruction
//	team.deletion_failed     — THIS WORKER emits on per-team error
//
// The worker only writes the last two; the API only writes the first two.
// Centralising the literal strings here (with a contract test) keeps the
// two sides in lockstep without a cross-module import.

const (
	// auditKindTombstoned is the audit_log.kind value the executor writes
	// after a successful per-team destruction pass. Metadata:
	// {resource_count_destroyed, s3_bytes_freed, duration_seconds}.
	auditKindTombstoned = "team.tombstoned"

	// auditKindTeamDeletionFailed is written when a per-team destruction
	// step errors mid-way. The team stays in deletion_requested state so
	// an operator can investigate and re-run the sweep. Metadata:
	// {error, failed_at_step, resource_id (when applicable)}.
	auditKindTeamDeletionFailed = "team.deletion_failed"
)
