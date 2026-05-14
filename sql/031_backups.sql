-- Migration: 031_backups — customer-facing pg_dump backup + restore tracking.
--
-- Owned by the api side (which will copy this file into its own migrations dir
-- as 031_backups.sql). The worker side ships the same SQL here so an operator
-- bringing up the worker on a fresh DB has a single source of truth and a copy
-- they can hand-apply if the api hasn't shipped yet.
--
-- resource_backups — one row per backup attempt. The api inserts pending rows
-- (either from a customer-triggered POST /api/v1/resources/:id/backup OR from
-- the worker's scheduler sweep). The worker's customer_backup_runner job
-- claims the pending row, runs pg_dump → S3, then updates status/finished_at/
-- s3_key/size_bytes.
--
-- tier_at_backup captures the resource's tier at backup time so the retention
-- sweep (worker side) hard-deletes S3 objects against the tier-at-write
-- retention window, NOT the team's current tier. This matches the user-benefit
-- rule from the api side (downgrades don't immediately shorten retention on
-- backups that were already paid for at the higher tier).
CREATE TABLE IF NOT EXISTS resource_backups (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    resource_id     UUID NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
    status          TEXT NOT NULL CHECK (status IN ('pending','running','ok','failed')) DEFAULT 'pending',
    backup_kind     TEXT NOT NULL CHECK (backup_kind IN ('scheduled','manual')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    s3_key          TEXT,
    size_bytes      BIGINT,
    tier_at_backup  TEXT,
    error_summary   TEXT,
    triggered_by    UUID REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_backups_resource ON resource_backups(resource_id);
-- Partial index — the runner sweep `WHERE status IN ('pending','running')` is
-- the hot path; we don't want a full-table index that wastes space on the
-- (eventually large) ok/failed historical rows.
CREATE INDEX IF NOT EXISTS idx_backups_pending  ON resource_backups(status) WHERE status IN ('pending','running');

-- resource_restores — one row per restore attempt. Customer triggers via
-- POST /api/v1/resources/:id/restore with a backup_id (the api inserts a
-- pending row); the worker's customer_restore_runner picks it up, downloads
-- from S3, and pg_restores into the SAME resource (backup objects are
-- immutable; restores always overwrite the current resource's data).
CREATE TABLE IF NOT EXISTS resource_restores (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    resource_id     UUID NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
    backup_id       UUID NOT NULL REFERENCES resource_backups(id),
    status          TEXT NOT NULL CHECK (status IN ('pending','running','ok','failed')) DEFAULT 'pending',
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    error_summary   TEXT,
    triggered_by    UUID NOT NULL REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_restores_resource ON resource_restores(resource_id);
CREATE INDEX IF NOT EXISTS idx_restores_pending  ON resource_restores(status) WHERE status IN ('pending','running');
