-- "Latest known state" table for emote_catalog.py (see ../ARCHITECTURE.md), same
-- shape of problem as sql/002_donation_goals.sql: NiFi's PutDatabaseRecord for this
-- stream must be configured with Statement Type = UPSERT and Update Keys =
-- (service, scope, channel, emote_id), so a re-poll overwrites each emote's row in
-- place instead of appending a new one. An emote later removed upstream (by Twitch,
-- 7TV, BetterTTV, or FrankerFaceZ) is not deleted here — same known limitation as
-- bronze_donation_goals, see README.md "Known limitations".
--
-- scope='global' rows use channel = '__global__' (Twitch/7TV/BetterTTV/FrankerFaceZ
-- all expose global emote sets usable in any channel, in addition to each channel's
-- own) — deliberately NOT NULL. A nullable channel here would break the UPSERT this
-- table depends on: Postgres's UNIQUE treats NULL as distinct from NULL, so ON
-- CONFLICT (service, scope, channel, emote_id) would never match two "global" rows
-- for the same emote, and every re-poll would insert a fresh duplicate forever
-- instead of overwriting in place. '__global__' can never collide with a real Twitch
-- login (Twitch logins are alphanumeric/underscore but this exact sentinel is
-- reserved here, not just assumed unclaimed).
--
-- Applied on srv-db (PostgreSQL), same as 001-004.
--
-- Run statement-by-statement (e.g. `psql -f`, not wrapped in one BEGIN/COMMIT): the
-- CREATE INDEX CONCURRENTLY statement below can't execute inside a transaction block.

CREATE TABLE IF NOT EXISTS bronze_emote_catalog (
    id           BIGSERIAL PRIMARY KEY,
    service      TEXT NOT NULL,
    scope        TEXT NOT NULL,
    channel      TEXT NOT NULL,
    emote_id     TEXT NOT NULL,
    emote_code   TEXT NOT NULL,
    fetched_at   TIMESTAMPTZ NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (service, scope, channel, emote_id)
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS bronze_emote_catalog_emote_code_idx ON bronze_emote_catalog (emote_code);
