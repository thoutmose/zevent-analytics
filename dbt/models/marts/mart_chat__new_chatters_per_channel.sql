-- New-to-this-channel chatters per day: a channel's chat-audience growth
-- signal, distinct from raw message volume (int_chat__hourly_channel_activity)
-- — first_message_at here is per (chatter, channel), not event-wide, so a
-- chatter already active elsewhere still counts as "new" the day they first
-- show up in a given channel's chat.
select
    channel,
    date_trunc('day', first_message_at) as day_bucket,
    count(*) as new_chatters_to_channel
from {{ ref('int_chat__chatter_channel_activity') }}
group by 1, 2
