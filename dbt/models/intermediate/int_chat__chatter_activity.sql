-- One row per chatter, event-wide: identity (latest known display name/
-- account age/broadcaster_type — a chatter's display name can change, so
-- `latest_attrs` takes the value from their most recent message, not an
-- arbitrary one) plus the aggregates several marts key off (breadth vs.
-- depth, lifespan, verbosity, bot-signal heuristics).
--
-- has_long_digit_suffix is a WEAK bot/throwaway-account signal (Twitch
-- appends digits to a login when the desired name is taken), not a
-- classifier — cross-tab it with account age, don't treat it alone as proof
-- of anything.
with latest_attrs as (
    select distinct on (chatter_id)
        chatter_id,
        chatter,
        account_created_at,
        broadcaster_type
    from {{ ref('stg_bronze__live_chat') }}
    where chatter_id is not null
    order by chatter_id asc, message_sent_at desc
),

aggregates as (
    select
        chatter_id,
        count(distinct channel) as distinct_channel_count,
        count(*) as total_message_count,
        min(message_sent_at) as first_message_at,
        max(message_sent_at) as last_message_at,
        avg(length(message_text)) as avg_message_length
    from {{ ref('stg_bronze__live_chat') }}
    where chatter_id is not null
    group by chatter_id
)

select
    a.chatter_id,
    la.chatter,
    la.account_created_at,
    la.broadcaster_type,
    a.distinct_channel_count,
    a.total_message_count,
    a.first_message_at,
    a.last_message_at,
    a.avg_message_length,
    extract(epoch from (a.last_message_at - a.first_message_at)) as lifespan_seconds,
    (la.chatter ~ '[0-9]{4,}$') as has_long_digit_suffix
from aggregates as a
inner join latest_attrs as la on a.chatter_id = la.chatter_id
