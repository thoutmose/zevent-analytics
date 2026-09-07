-- One row per (chatter, channel), with both per-channel and global rank
-- attached — covers all three leaderboard views asked for from one table:
--   - top chatters on one channel:    filter channel = X, order by rank_within_channel
--   - top chatters across channels:   distinct on chatter_id, order by distinct_channel_count
--   - top chatters event-wide:        distinct on chatter_id, order by rank_global
with per_chatter_totals as (
    select
        chatter_id,
        count(distinct channel) as distinct_channel_count,
        sum(message_count) as total_message_count
    from {{ ref('int_chat__chatter_channel_activity') }}
    group by 1
)

select
    a.chatter_id,
    ca.chatter,
    a.channel,
    a.message_count,
    a.active_hours,
    a.first_message_at,
    a.last_message_at,
    t.distinct_channel_count,
    t.total_message_count,
    rank() over (partition by a.channel order by a.message_count desc) as rank_within_channel,
    rank() over (order by t.total_message_count desc) as rank_global
from {{ ref('int_chat__chatter_channel_activity') }} as a
inner join per_chatter_totals as t on a.chatter_id = t.chatter_id
left join {{ ref('int_chat__chatter_activity') }} as ca on a.chatter_id = ca.chatter_id
