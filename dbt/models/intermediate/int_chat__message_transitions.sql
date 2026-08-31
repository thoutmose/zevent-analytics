-- Inferred channel-hops: one row per pair of consecutive messages from the
-- same chatter that landed in two different channels within
-- chatter_migration_max_gap_minutes (dbt_project.yml var) of each other.
-- The closest available proxy to the raid data dropped along with EventSub
-- (see README.md's Overview) — not an actual raid signal, just a
-- correlation inferred from chat behavior.
with ordered as (
    select
        chatter_id,
        channel,
        message_sent_at,
        lag(channel) over (
            partition by chatter_id order by message_sent_at
        ) as prev_channel,
        lag(message_sent_at) over (
            partition by chatter_id order by message_sent_at
        ) as prev_message_sent_at
    from {{ ref('stg_bronze__live_chat') }}
    where chatter_id is not null and message_sent_at is not null
)

select
    chatter_id,
    prev_channel as from_channel,
    channel as to_channel,
    prev_message_sent_at as departed_at,
    message_sent_at as arrived_at,
    extract(epoch from (message_sent_at - prev_message_sent_at)) as gap_seconds
from ordered
where channel != prev_channel
    and message_sent_at - prev_message_sent_at
        <= interval '{{ var("chatter_migration_max_gap_minutes") }} minutes'
