-- All-time (whole event) message count per (chatter, channel) — the grain
-- both the chatter leaderboard and the chatter profile classification
-- (sedentaire/multi_streamer/semi_nomade/nomade) are built from.
select
    chatter_id,
    channel,
    count(*) as message_count,
    min(message_sent_at) as first_message_at,
    max(message_sent_at) as last_message_at,
    count(distinct date_trunc('hour', message_sent_at)) as active_hours
from {{ ref('stg_bronze__live_chat') }}
where chatter_id is not null
group by 1, 2
