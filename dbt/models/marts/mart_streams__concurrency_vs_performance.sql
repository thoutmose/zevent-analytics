-- Per-channel viewer count next to how many channels were live at the same
-- hour, event-wide — does the event cannibalize itself at peak concurrency,
-- or does a rising tide lift all channels?
select
    v.channel,
    v.hour_bucket,
    v.avg_viewer_count,
    c.live_channel_count,
    c.total_avg_viewer_count
from {{ ref('int_streams__hourly_viewership') }} as v
inner join {{ ref('int_streams__concurrency') }} as c on c.hour_bucket = v.hour_bucket
where v.live_snapshot_count > 0
