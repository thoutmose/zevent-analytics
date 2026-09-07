-- 15-minute-bucket presence per chatter: is_new_this_quarter_hour distinguishes
-- a chatter's first-ever bucket from a return visit, distinct from
-- mart_chatters__profile (which only looks at channel count, never at time spread).
select
    d.chatter_id,
    ca.chatter,
    d.quarter_hour_bucket,
    d.distinct_channel_count,
    d.message_count,
    min(d.quarter_hour_bucket) over (partition by d.chatter_id) as first_active_quarter_hour,
    (d.quarter_hour_bucket = min(d.quarter_hour_bucket) over (partition by d.chatter_id)) as is_new_this_quarter_hour,
    count(*) over (partition by d.chatter_id) as total_quarter_hours_active
from {{ ref('int_chat__chatter_quarter_hourly_activity') }} as d
left join {{ ref('int_chat__chatter_activity') }} as ca on d.chatter_id = ca.chatter_id
