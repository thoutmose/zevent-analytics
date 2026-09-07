-- 1:1 typed table over bronze_live_chat. badges/emotes are kept as jsonb —
-- named flags (is_moderator, is_vip, ...) are derived downstream, at query
-- time (`badges ? 'vip'`), not baked in here: Twitch's badge set isn't fixed,
-- so this survives a new badge showing up without a model change.
--
-- materialized='table' (overriding staging's default view): 14 downstream
-- models ref this one, and as a view each ref re-scanned bronze_live_chat
-- from source — ~15 full seq scans of a 2GB table per dbt run, thrashing
-- shared_buffers. Table materialization makes it one scan (this model's
-- build) instead of one per consumer. See INCIDENT.md.
{{ config(
    materialized='table',
    indexes=[
        {'columns': ['message_sent_at'], 'type': 'brin'},
        {'columns': ['channel', 'message_sent_at']}
    ]
) }}

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
    emotes,
    account_created_at,
    ingested_at,
    nullif(user_type, '') as user_type,
    nullif(broadcaster_type, '') as broadcaster_type
from {{ source('bronze', 'bronze_live_chat') }}
