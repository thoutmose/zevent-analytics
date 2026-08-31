-- Per-channel viewer count over time, plus its rank among all channels at
-- that same hour — the small-multiples/timeline view and the "bump chart"
-- (rank over time, handles the day/night cycle) from one table.
select
    channel,
    hour_bucket,
    avg_viewer_count,
    peak_viewer_count,
    live_snapshot_count,
    total_snapshot_count,
    rank() over (
        partition by hour_bucket order by avg_viewer_count desc
    ) as viewer_rank_at_hour
from {{ ref('int_streams__hourly_viewership') }}
where live_snapshot_count > 0
