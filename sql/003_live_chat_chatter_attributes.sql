-- Adds per-chatter role/identity columns to bronze_live_chat (see ../ARCHITECTURE.md).
-- Applied on srv-db (PostgreSQL), same as 001_bronze_schema.sql/002_donation_goals.sql.
--
-- badges/user_type come from the IRC message's own tags (free, per-message, always
-- current as of message_sent_at). account_created_at/broadcaster_type come from a
-- Helix Get Users lookup, cached per chatter_id in main.py (ChatterInfo/chatter_cache)
-- since they don't change message to message — see main.py's _chatter_lookup_loop.
--
-- ADD COLUMN IF NOT EXISTS is additive and idempotent: existing rows get NULL in all
-- four columns, nothing is backfilled.
--
-- IMPORTANT, unlike 001/002 (which only ever created new tables): PutDatabaseRecord's
-- "Table Schema Cache Size" means the live_chat/metadata/zevent_snapshot INSERT
-- processor already has bronze_live_chat's old column list cached in memory from
-- earlier pushes. Applying this ALTER alone does nothing visible — NiFi keeps using
-- its stale cached schema, treats these four columns as unmatched fields (Ignore
-- Unmatched Fields), and silently drops them from every row, no error, nothing in
-- dead-letter. After running this file, the PutDatabaseRecord (INSERT) processor (or
-- the whole NiFi instance, e.g. `docker compose restart nifi`) must also be
-- stopped/restarted so it re-reads the table's real column list. Confirmed by testing
-- on srv-dev on 2026-08-31: badges/user_type/account_created_at/broadcaster_type were
-- NULL on every row until the NiFi container was restarted.

ALTER TABLE bronze_live_chat
ADD COLUMN IF NOT EXISTS badges             JSONB,
ADD COLUMN IF NOT EXISTS user_type          TEXT,
ADD COLUMN IF NOT EXISTS account_created_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS broadcaster_type   TEXT;
