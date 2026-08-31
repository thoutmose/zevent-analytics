-- Chat volume per channel per hour — the base for both the msg/sec sanity
-- check against this project's sized throughput (README's Reliability
-- mechanisms) and the chat-vs-donations comparison downstream.
select
    channel,
    date_trunc('hour', message_sent_at) as hour_bucket,
    count(*) as message_count,
    count(distinct chatter_id) as unique_chatter_count
from {{ ref('stg_bronze__live_chat') }}
where message_sent_at is not null
group by 1, 2
