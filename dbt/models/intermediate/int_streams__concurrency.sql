-- How many channels were live at each hour, event-wide — the denominator
-- for "does the event cannibalize itself at peak, or does a rising tide
-- lift all channels."
select
    hour_bucket,
    count(*) filter (where live_snapshot_count > 0) as live_channel_count,
    sum(avg_viewer_count) filter (where live_snapshot_count > 0) as total_avg_viewer_count
from {{ ref('int_streams__hourly_viewership') }}
group by 1
