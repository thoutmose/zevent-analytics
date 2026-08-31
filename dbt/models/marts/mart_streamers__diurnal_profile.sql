-- Each streamer's viewer count at a given hour-of-day, indexed against the
-- event-wide average at that SAME hour-of-day. viewer_index_vs_event > 1
-- means genuinely outsized for that time slot; < 1 means "big because
-- everyone is big at 9pm," not because of anything this streamer did.
-- Fixes the time-of-day confound that sits underneath almost every raw
-- metric in the other marts.
with streamer_hourly as (
    select
        channel,
        extract(hour from hour_bucket) as hour_of_day,
        avg(avg_viewer_count) as streamer_avg_viewer_count
    from {{ ref('int_streams__hourly_viewership') }}
    where live_snapshot_count > 0
    group by 1, 2
),

event_hourly as (
    select
        extract(hour from hour_bucket) as hour_of_day,
        avg(avg_viewer_count) as event_avg_viewer_count
    from {{ ref('int_streams__hourly_viewership') }}
    where live_snapshot_count > 0
    group by 1
)

select
    s.channel,
    s.hour_of_day,
    s.streamer_avg_viewer_count,
    e.event_avg_viewer_count,
    s.streamer_avg_viewer_count / nullif(e.event_avg_viewer_count, 0) as viewer_index_vs_event
from streamer_hourly as s
inner join event_hourly as e on e.hour_of_day = s.hour_of_day
