-- Account-age bucket per chatter, cross-tabbed against their channel-count
-- profile — account_created_at was captured (main.py's ChatterInfo cache)
-- but never analyzed anywhere until this model. "now()" as the reference
-- point is fine for a bucket this coarse (day-of-query doesn't change which
-- bucket a multi-month-old account falls into).
{{ config(indexes=[{'columns': ['chatter_id'], 'unique': True}]) }}

select
    ca.chatter_id,
    ca.chatter,
    ca.account_created_at,
    p.chatter_profile,
    p.distinct_channel_count,
    case
        when ca.account_created_at is null then null
        when ca.account_created_at >= now() - interval '30 days' then 'new (<30d)'
        when ca.account_created_at >= now() - interval '1 year' then '1mo-1yr'
        when ca.account_created_at >= now() - interval '3 years' then '1-3yr'
        else '3yr+'
    end as account_age_bucket
from {{ ref('int_chat__chatter_activity') }} as ca
left join {{ ref('mart_chatters__profile') }} as p on ca.chatter_id = p.chatter_id
