-- 055_forwarder_sent.sql — worker-side send ledger for the event-email
-- forwarder (event_email_forwarder.go).
--
-- The worker repo does not own a migration runner (see 030/031 headers).
-- This file is the canonical source; the api repo copies it verbatim into
-- api/internal/db/migrations/055_forwarder_sent.sql and the api testhelpers
-- mirror so a fresh test DB and the api auto-deploy gate both have the
-- table. Keep the three in sync.
--
-- WHY (BugBash 2026-05-19, P1-3):
-- The forwarder's only idempotency mechanism was the Brevo X-Mailin-Custom
-- header, which is free-form metadata — NOT a delivery-dedup guarantee.
-- Brevo does not suppress a second POST carrying the same value. So every
-- cursor-reset (Redis wipe — P1-2), every cursor_corrupt reset, and every
-- crash-mid-batch recovery re-sent real duplicate email.
--
-- forwarder_sent is a true worker-side ledger. The forwarder INSERTs
-- (audit_id) with ON CONFLICT DO NOTHING immediately before each send;
-- when the insert affects 0 rows the audit_id was already sent and the
-- forwarder skips. This makes the forwarder idempotent regardless of
-- provider behavior or cursor state.
--
-- Columns:
--   * audit_id — the audit_log.id (TEXT to match the forwarder's id::text
--     projection — audit_log.id is a UUID but the forwarder treats it as
--     an opaque string watermark). PRIMARY KEY gives the ON CONFLICT
--     target and the dedup uniqueness in one index.
--   * sent_at  — when the ledger row was written (≈ the send time).

CREATE TABLE IF NOT EXISTS forwarder_sent (
    audit_id TEXT PRIMARY KEY,
    sent_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
