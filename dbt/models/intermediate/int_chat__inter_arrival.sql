-- Gap between one chatter's consecutive messages, across any channel.
-- Suspiciously regular gaps are a bot signal — this is a data-quality tool
-- as much as an analytical one, deliberately kept at message grain (not
-- pre-aggregated) so a downstream mart can compute its own regularity
-- measure (stddev, coefficient of variation) per chatter.
select
    chatter_id,
    channel,
    message_sent_at,
    message_sent_at - lag(message_sent_at) over (
        partition by chatter_id order by message_sent_at
    ) as inter_arrival
from {{ ref('stg_bronze__live_chat') }}
where chatter_id is not null and message_sent_at is not null
