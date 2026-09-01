-- Which emotes spiked when, per channel per day — rolled up from
-- int_chat__emote_usage's hourly grain since day-level is the natural
-- resolution for "trending" rather than hour-by-hour noise.
select
    channel,
    service,
    emote_id,
    date_trunc('day', hour_bucket) as day_bucket,
    sum(usage_count) as usage_count,
    rank() over (
        partition by channel, date_trunc('day', hour_bucket)
        order by sum(usage_count) desc
    ) as rank_within_channel_day
from {{ ref('int_chat__emote_usage') }}
group by 1, 2, 3, 4
