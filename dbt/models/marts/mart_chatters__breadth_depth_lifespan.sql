-- Breadth (distinct channels) vs. depth (messages per channel) vs. lifespan
-- (first-to-last message span) per chatter — the individual-level
-- counterpart to the community network marts, and distinct from
-- mart_chatters__retention (day-spread) or mart_chatters__profile
-- (channel-count bucket only).
select
    chatter_id,
    chatter,
    distinct_channel_count,
    total_message_count,
    first_message_at,
    last_message_at,
    lifespan_seconds,
    round(total_message_count::numeric / nullif(distinct_channel_count, 0), 1)
        as avg_messages_per_channel,
    round(lifespan_seconds / 3600.0, 1) as lifespan_hours
from {{ ref('int_chat__chatter_activity') }}
