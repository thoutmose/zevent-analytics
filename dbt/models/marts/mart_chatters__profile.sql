-- Chatter profile: sedentaire (exactly 1 channel all event) / multi_streamer
-- (a small, fixed handful) / semi_nomade / nomade (broad roaming across many
-- streamers) — bucketed purely on distinct_channel_count, thresholds in
-- dbt_project.yml `vars`. This is a judgment call, not a Twitch-defined
-- category — tune the vars, not this file, if the buckets feel wrong.
-- top_channel_share is included but NOT used to bucket (v1 keeps this
-- simple): a chatter with 4 channels but 95% of messages in one of them
-- currently lands in the same bucket as one spread evenly — worth
-- revisiting if that turns out to matter more than raw channel count.
with per_chatter_totals as (
    select
        chatter_id,
        count(distinct channel) as distinct_channel_count,
        sum(message_count) as total_message_count,
        max(message_count) as max_channel_message_count
    from {{ ref('int_chat__chatter_channel_activity') }}
    group by 1
)

select
    chatter_id,
    distinct_channel_count,
    total_message_count,
    max_channel_message_count,
    max_channel_message_count::numeric / nullif(total_message_count, 0) as top_channel_share,
    case
        when distinct_channel_count <= {{ var('chatter_profile_sedentaire_max_channels') }}
            then 'sedentaire'
        when distinct_channel_count <= {{ var('chatter_profile_multi_streamer_max_channels') }}
            then 'multi_streamer'
        when distinct_channel_count <= {{ var('chatter_profile_semi_nomade_max_channels') }}
            then 'semi_nomade'
        else 'nomade'
    end as chatter_profile
from per_chatter_totals
