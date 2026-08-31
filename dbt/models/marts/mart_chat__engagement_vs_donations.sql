-- The comparison this whole sizing model rests on: does chat volume move
-- with donation velocity for a given streamer/hour? Also the empirical
-- check on the "1-3 msg/min per 100 viewers" assumption behind this
-- project's own extraction capacity planning (README's Reliability
-- mechanisms) — msgs_per_min_per_100_viewers below is that exact ratio,
-- computed from real data instead of assumed.
with donations_hourly as (
    select
        twitch_login as channel,
        date_trunc('hour', ingested_at) as hour_bucket,
        sum(donation_delta_eur) as donation_delta_eur,
        avg(viewer_count) as avg_viewer_count
    from {{ ref('int_donations__streamer_deltas') }}
    group by 1, 2
)

select
    c.channel,
    c.hour_bucket,
    c.message_count,
    c.unique_chatter_count,
    d.donation_delta_eur,
    d.avg_viewer_count,
    case
        when d.avg_viewer_count > 0
            then (c.message_count / 60.0) / (d.avg_viewer_count / 100.0)
    end as msgs_per_min_per_100_viewers
from {{ ref('int_chat__hourly_channel_activity') }} as c
left join donations_hourly as d
    on c.channel = d.channel and c.hour_bucket = d.hour_bucket
