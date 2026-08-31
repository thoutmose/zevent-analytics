-- 1:1 typed view over bronze_live_chat. badges/emotes are kept as jsonb —
-- named flags (is_moderator, is_vip, ...) are derived downstream, at query
-- time (`badges ? 'vip'`), not baked in here: Twitch's badge set isn't fixed,
-- so this survives a new badge showing up without a model change.
select
    id,
    batch_id,
    row_number,
    channel,
    chatter,
    chatter_id,
    message_text,
    message_sent_at,
    captured_at,
    badges,
    nullif(user_type, '') as user_type,
    emotes,
    account_created_at,
    nullif(broadcaster_type, '') as broadcaster_type,
    ingested_at
from {{ source('bronze', 'bronze_live_chat') }}
