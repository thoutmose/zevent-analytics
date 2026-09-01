-- Hours where a channel's message count is >=2 standard deviations above
-- ITS OWN average (not a global threshold, which would just always flag
-- naturally-huge channels) — turns the "sanity check on extraction
-- completeness" idea from README.md into a real signal, joinable to
-- int_streams__category_changes / int_donations__streamer_deltas to explain
-- what actually happened at that moment.
with stats as (
    select
        channel,
        hour_bucket,
        message_count,
        unique_chatter_count,
        avg(message_count) over (partition by channel) as channel_avg_message_count,
        stddev(message_count) over (partition by channel) as channel_stddev_message_count
    from {{ ref('int_chat__hourly_channel_activity') }}
)

select
    channel,
    hour_bucket,
    message_count,
    unique_chatter_count,
    channel_avg_message_count,
    channel_stddev_message_count,
    (message_count - channel_avg_message_count) / channel_stddev_message_count as message_count_zscore
from stats
where
    channel_stddev_message_count > 0
    and (message_count - channel_avg_message_count) / channel_stddev_message_count >= 2
