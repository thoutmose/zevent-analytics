-- Viewer count per channel per hour — the per-channel counterpart to
-- mart_donations__timeseries's event-wide total_viewer_count, and the base
-- for both the viewership timeline and the rank/"bump chart" view (day/night
-- cycle) across channels.
select
    channel,
    date_trunc('hour', snapshot_at) as hour_bucket,
    avg(viewer_count) as avg_viewer_count,
    max(viewer_count) as peak_viewer_count,
    count(*) filter (where is_live) as live_snapshot_count,
    count(*) as total_snapshot_count
from {{ ref('stg_bronze__metadata_snapshots') }}
group by 1, 2
