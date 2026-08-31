-- Adds native-Twitch emote usage to bronze_live_chat (see ../ARCHITECTURE.md).
-- Applied on srv-db (PostgreSQL), same as 001-003.
--
-- emotes comes from the IRC message's own `emotes` tag, parsed in main.py
-- (_parse_emotes) into {emote_id: occurrence_count} — free, per-message, no
-- extra API calls. Third-party emotes (7TV/BetterTTV/FrankerFaceZ) aren't in
-- this column: Twitch's IRC has no knowledge of them at all. Those are
-- matched against bronze_emote_catalog (see emote_catalog.py /
-- 005_emote_catalog.sql) as a dbt-side join against message_text instead.
--
-- ADD COLUMN IF NOT EXISTS is additive and idempotent: existing rows get NULL,
-- nothing is backfilled.
--
-- IMPORTANT — same gotcha as 003 (see that file's header and
-- ARCHITECTURE.md step 6a): PutDatabaseRecord's Table Schema Cache Size means
-- this column will be silently dropped from every row until the
-- PutDatabaseRecord (INSERT) processor (or all of NiFi) is restarted after
-- applying this migration.

ALTER TABLE bronze_live_chat
ADD COLUMN IF NOT EXISTS emotes JSONB;
