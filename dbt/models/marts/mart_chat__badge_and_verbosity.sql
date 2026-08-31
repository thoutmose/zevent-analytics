-- Badge/subscriber population mix and message verbosity per channel.
-- is_plain_viewer means the badges tag was present but empty ('{}') or
-- absent entirely — an anonymous/no-badge chatter, not "unknown".
with badge_flags as (
    select
        channel,
        message_text,
        (badges ? 'subscriber') as is_subscriber,
        (badges ? 'vip') as is_vip,
        (badges ? 'moderator') as is_moderator,
        (badges is null or badges = '{}'::jsonb) as is_plain_viewer
    from {{ ref('stg_bronze__live_chat') }}
)

select
    channel,
    count(*) as message_count,
    count(*) filter (where is_subscriber) as subscriber_message_count,
    count(*) filter (where is_vip) as vip_message_count,
    count(*) filter (where is_moderator) as moderator_message_count,
    count(*) filter (where is_plain_viewer) as plain_viewer_message_count,
    avg(length(message_text)) as avg_message_length,
    avg(length(message_text)) filter (where is_subscriber) as avg_message_length_subscriber,
    avg(length(message_text)) filter (where is_plain_viewer) as avg_message_length_plain_viewer
from badge_flags
group by 1
