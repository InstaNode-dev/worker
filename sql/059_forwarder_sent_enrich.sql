-- 059_forwarder_sent_enrich.sql — enrich the worker-side send ledger with
-- audit columns so support staff can answer "which audit_log row was
-- forwarded to which provider, when, to what masked recipient, with what
-- terminal classification" without grepping pod logs.
--
-- The worker repo does not own a migration runner (see 030/031/055 headers).
-- This file is the canonical source; the api repo copies it verbatim into
-- api/internal/db/migrations/059_forwarder_sent_enrich.sql and the api
-- testhelpers mirror so a fresh test DB and the api auto-deploy gate both
-- get the columns. Keep the three in sync.
--
-- WHY (BugBash 2026-05-20, P1-3 enrichment):
-- Migration 055 introduced forwarder_sent (audit_id, sent_at) as a minimal
-- idempotency ledger. That stopped duplicate sends across cursor resets,
-- but it did NOT give support a way to answer "what happened to email X?"
-- without log-spelunking — and the F4 missing-renderer path (next door
-- in this PR) needs a place to record permanent drops so an operator
-- can grep `classification='permanent_drop'` to find them.
--
-- The columns are appended via ALTER TABLE so a fresh deploy and an
-- already-populated prod DB both converge cleanly. Existing rows
-- backfill to provider='legacy' / classification='success' (the only
-- state a pre-059 row could have been in — markSent was only called on
-- a confirmed 2xx or terminal class).

ALTER TABLE forwarder_sent
    ADD COLUMN IF NOT EXISTS provider       TEXT NOT NULL DEFAULT 'legacy',
    ADD COLUMN IF NOT EXISTS provider_id    TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS recipient      TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS template_kind  TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS classification TEXT NOT NULL DEFAULT 'success';

CREATE INDEX IF NOT EXISTS idx_forwarder_sent_sent_at
    ON forwarder_sent (sent_at DESC);

CREATE INDEX IF NOT EXISTS idx_forwarder_sent_template_kind_sent_at
    ON forwarder_sent (template_kind, sent_at DESC);

CREATE INDEX IF NOT EXISTS idx_forwarder_sent_perm_drop
    ON forwarder_sent (sent_at DESC)
    WHERE classification = 'permanent_drop';
