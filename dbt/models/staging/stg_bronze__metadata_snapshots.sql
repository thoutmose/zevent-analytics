-- 1:1 typed view over bronze_metadata_snapshots.
--
-- materialized='table' (overriding staging's default view), same reasoning
-- as stg_bronze__live_chat: 9+ downstream models ref this one, so a view
-- meant a full scan of a 5.16M-row source per consumer. Also required for
-- the (channel, snapshot_at) index below — dbt-postgres only applies
-- `indexes` config to table/incremental materializations, not views.
{{ config(
    materialized='table',
    indexes=[{'columns': ['channel', 'snapshot_at']}]
) }}

select
    id,
    batch_id,
    row_number,
    channel,
    broadcaster_id,
    is_live,
    title,
    category,
    viewer_count,
    duration_seconds,
    stream_started_at,
    snapshot_at,
    ingested_at
from {{ source('bronze', 'bronze_metadata_snapshots') }}
