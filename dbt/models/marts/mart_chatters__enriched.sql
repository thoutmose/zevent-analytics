-- One row per chatter, joining profile + bot_signal + account_age_profile +
-- breadth_depth_lifespan once here instead of at every chatter_breakdown
-- page load (4.85s joining four 431k-row tables with no key). Columns
-- duplicated across those four sources (chatter, distinct_channel_count,
-- total_message_count, chatter_profile, account_created_at) are taken from
-- mart_chatters__profile only; the others contribute their additive columns.
{{ config(indexes=[{'columns': ['chatter_id'], 'unique': True}]) }}

select
    p.chatter_id,
    p.chatter,
    p.distinct_channel_count,
    p.total_message_count,
    p.max_channel_message_count,
    p.top_channel_share,
    p.chatter_profile,
    aap.account_created_at,
    aap.account_age_bucket,
    bs.has_long_digit_suffix,
    bs.gap_count,
    bs.avg_gap_seconds,
    bs.stddev_gap_seconds,
    bs.gap_coefficient_of_variation,
    bdl.first_message_at,
    bdl.last_message_at,
    bdl.lifespan_seconds,
    bdl.avg_messages_per_channel,
    bdl.lifespan_hours
from {{ ref('mart_chatters__profile') }} as p
left join {{ ref('mart_chatters__account_age_profile') }} as aap on p.chatter_id = aap.chatter_id
left join {{ ref('mart_chatters__bot_signal') }} as bs on p.chatter_id = bs.chatter_id
left join {{ ref('mart_chatters__breadth_depth_lifespan') }} as bdl on p.chatter_id = bdl.chatter_id
