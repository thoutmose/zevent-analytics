-- 1:1 typed view over bronze_metadata_snapshots.
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
