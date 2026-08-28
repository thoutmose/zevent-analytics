-- Bronze/raw tables for the Zevent ELT pipeline (see ../ARCHITECTURE.md).
-- Applied on srv-db (PostgreSQL). NiFi's PutDatabaseRecord processors write here through
-- PgBouncer. Each row carries the batch_id/row_number pair its source batch was tagged
-- with (see nifi_client.py), so a replayed flowfile is a no-op instead of a duplicate.

CREATE TABLE IF NOT EXISTS bronze_live_chat (
    id              BIGSERIAL PRIMARY KEY,
    batch_id        UUID NOT NULL,
    row_number      INT NOT NULL,
    channel         TEXT NOT NULL,
    chatter         TEXT,
    chatter_id      TEXT,
    text            TEXT,
    message_sent_at TIMESTAMPTZ,
    captured_at     TIMESTAMPTZ,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, row_number)
);

CREATE TABLE IF NOT EXISTS bronze_metadata_snapshots (
    id                BIGSERIAL PRIMARY KEY,
    batch_id          UUID NOT NULL,
    row_number        INT NOT NULL,
    channel           TEXT NOT NULL,
    broadcaster_id    TEXT,
    is_live           BOOLEAN,
    title             TEXT,
    category          TEXT,
    viewer_count      INT,
    duration_seconds  DOUBLE PRECISION,
    stream_started_at TIMESTAMPTZ,
    snapshot_at       TIMESTAMPTZ,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, row_number)
);

CREATE TABLE IF NOT EXISTS bronze_zevent_snapshots (
    id                         BIGSERIAL PRIMARY KEY,
    batch_id                   UUID NOT NULL,
    row_number                 INT NOT NULL,
    website_mode                TEXT,
    total_donation_amount_eur   DOUBLE PRECISION,
    total_viewer_count          INT,
    streamers                   JSONB,
    ingested_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, row_number)
);

CREATE INDEX IF NOT EXISTS bronze_live_chat_captured_at_idx ON bronze_live_chat (captured_at);
CREATE INDEX IF NOT EXISTS bronze_metadata_snapshots_snapshot_at_idx ON bronze_metadata_snapshots (snapshot_at);
CREATE INDEX IF NOT EXISTS bronze_zevent_snapshots_ingested_at_idx ON bronze_zevent_snapshots (ingested_at);
