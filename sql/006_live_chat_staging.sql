-- Staging-table ingest pattern for bronze_live_chat (see ../ARCHITECTURE.md, "Tuning
-- for higher throughput"). Applied on srv-db (PostgreSQL), same as 001-005.
--
-- bronze_live_chat is the highest-volume stream by a wide margin (every chat message,
-- ~300 channels, vs. per-channel polls for the other two INSERT-only tables) and the
-- one stress_test.py measures against. Writing every row straight into a WAL-logged,
-- indexed table one JDBC batch at a time is the ceiling "Tuning for higher throughput"
-- describes. PutDatabaseRecord (INSERT) now targets bronze_live_chat_staging (UNLOGGED
-- -- no WAL overhead) instead for this one stream, and a scheduled ExecuteSQL
-- processor (see ARCHITECTURE.md step 8) periodically calls
-- bronze_merge_live_chat_staging() to fold whatever's accumulated into the real table
-- with one bulk statement.
--
-- bronze_metadata_snapshots / bronze_zevent_snapshots don't get this treatment: their
-- volume (per-channel polls every 15-30s, one event-wide snapshot) is nowhere near a
-- bottleneck, and PutDatabaseRecord (INSERT)'s table-name expression still writes
-- those two straight to their real tables -- only the live_chat branch changes.
--
-- Run statement-by-statement (e.g. `psql -f`, not wrapped in one BEGIN/COMMIT): the
-- CREATE/DROP INDEX CONCURRENTLY statements below can't execute inside a transaction
-- block.

CREATE UNLOGGED TABLE IF NOT EXISTS bronze_live_chat_staging (
    batch_id           UUID NOT NULL,
    row_number         INT NOT NULL,
    channel            TEXT NOT NULL,
    chatter            TEXT,
    chatter_id         TEXT,
    message_text       TEXT,
    message_sent_at    TIMESTAMPTZ,
    captured_at        TIMESTAMPTZ,
    badges             JSONB,
    user_type          TEXT,
    account_created_at TIMESTAMPTZ,
    broadcaster_type   TEXT,
    emotes             JSONB
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS bronze_live_chat_staging_batch_id_idx ON bronze_live_chat_staging (batch_id);

-- Atomically drains everything currently in staging and folds it into
-- bronze_live_chat in one statement: the DELETE...RETURNING snapshot only sees rows
-- committed before this function starts, so concurrent PutDatabaseRecord writers
-- (Concurrent Tasks: 8, see ARCHITECTURE.md) inserting into staging mid-run are left
-- alone and picked up on the next scheduled call, not lost or double-merged.
-- ON CONFLICT DO NOTHING makes a replayed batch_id/row_number pair a no-op, same
-- idempotency guarantee 001_bronze_schema.sql's header describes for the old direct-
-- insert path.
CREATE OR REPLACE FUNCTION bronze_merge_live_chat_staging()
RETURNS TABLE (rows_merged BIGINT) AS $$
    WITH moved AS (
        DELETE FROM bronze_live_chat_staging
        RETURNING *
    ), ins AS (
        INSERT INTO bronze_live_chat (
            batch_id, row_number, channel, chatter, chatter_id, message_text,
            message_sent_at, captured_at, badges, user_type, account_created_at,
            broadcaster_type, emotes
        )
        SELECT
            batch_id, row_number, channel, chatter, chatter_id, message_text,
            message_sent_at, captured_at, badges, user_type, account_created_at,
            broadcaster_type, emotes
        FROM moved
        ON CONFLICT (batch_id, row_number) DO NOTHING
        RETURNING 1
    )
    SELECT count(*) FROM ins;
$$ LANGUAGE sql;

-- captured_at is time-ordered as chat is captured and this index only needs to
-- support range/time-window queries, not point lookups -- a BRIN index costs a few
-- bytes per page instead of per row, which matters at Zevent's insert volume.
-- Replaces the btree version from 001_bronze_schema.sql.
DROP INDEX CONCURRENTLY IF EXISTS bronze_live_chat_captured_at_idx;
CREATE INDEX CONCURRENTLY IF NOT EXISTS bronze_live_chat_captured_at_brin_idx ON bronze_live_chat USING brin (captured_at);
