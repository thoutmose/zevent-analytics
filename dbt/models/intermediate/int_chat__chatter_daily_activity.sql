-- Chatter x day grain (distinct from int_chat__chatter_channel_activity's
-- chatter x channel, all-time grain) — feeds day-over-day retention and
-- new-vs-returning-chatter marts, both of which need "was this chatter
-- present on day N" as their own dimension.
select
    chatter_id,
    date_trunc('day', message_sent_at) as day_bucket,
    count(distinct channel) as distinct_channel_count,
    count(*) as message_count
from {{ ref('stg_bronze__live_chat') }}
where chatter_id is not null and message_sent_at is not null
group by 1, 2
