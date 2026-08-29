-- "Latest known state" table for zevent_donation_goals.py (see ../ARCHITECTURE.md).
-- Applied on srv-db (PostgreSQL), same as 001_bronze_schema.sql.
--
-- Unlike the append-only bronze_* tables in 001_bronze_schema.sql, this table holds
-- one current row per (participation_id, goal_id): NiFi's PutDatabaseRecord for this
-- stream must be configured with Statement Type = UPSERT and Update Keys =
-- (participation_id, goal_id), so a re-poll of an existing goal overwrites it in place
-- instead of appending a new row. A goal that's later removed upstream is not deleted
-- here — see the "Known limitations" note in README.md.
--
-- Run statement-by-statement (e.g. `psql -f`, not wrapped in one BEGIN/COMMIT): the
-- CREATE INDEX CONCURRENTLY statement below can't execute inside a transaction block.

CREATE TABLE IF NOT EXISTS bronze_donation_goals (
    id               BIGSERIAL PRIMARY KEY,
    batch_id         UUID NOT NULL,
    row_number       INT NOT NULL,
    participation_id UUID NOT NULL,
    streamer_name    TEXT NOT NULL,
    twitch_login     TEXT,
    twitch_id        TEXT,
    goal_id          UUID NOT NULL,
    goal_name        TEXT NOT NULL,
    goal_amount_eur  NUMERIC(12, 2) NOT NULL,
    goal_category    TEXT,
    snapshot_at      TIMESTAMPTZ NOT NULL,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (participation_id, goal_id)
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS bronze_donation_goals_streamer_name_idx ON bronze_donation_goals (streamer_name);
