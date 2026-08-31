-- Day-over-day presence per chatter: is_new_today distinguishes a chatter's
-- first-ever day from a return visit, distinct from mart_chatters__profile
-- (which only looks at channel count, never at day spread).
select
    chatter_id,
    day_bucket,
    distinct_channel_count,
    message_count,
    min(day_bucket) over (partition by chatter_id) as first_active_day,
    (day_bucket = min(day_bucket) over (partition by chatter_id)) as is_new_today,
    count(*) over (partition by chatter_id) as total_days_active
from {{ ref('int_chat__chatter_daily_activity') }}
