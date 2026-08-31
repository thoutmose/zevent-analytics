-- One row per actual broadcast (not per poll): main.py already tracks
-- stream_started_at per snapshot while a channel is live, and that value is
-- constant for the whole duration of one continuous stream and changes the
-- next time the streamer goes live — so grouping on it directly gives real
-- sessions for free, instead of reconstructing them from is_live
-- true/false transitions.
select
    channel,
    broadcaster_id,
    stream_started_at,
    min(snapshot_at) as first_seen_at,
    max(snapshot_at) as last_seen_at,
    max(duration_seconds) as duration_seconds,
    avg(viewer_count) as avg_viewer_count,
    max(viewer_count) as peak_viewer_count
from {{ ref('stg_bronze__metadata_snapshots') }}
where is_live and stream_started_at is not null
group by 1, 2, 3
