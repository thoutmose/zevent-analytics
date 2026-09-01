-- One row per streamer: identity + donation/chat/viewership/community
-- summary in one place, for a streamer-level overview rather than always
-- slicing the fact marts by channel. Donations/chat/viewership are each
-- genuinely separate grains upstream (event-wide snapshot, per-message,
-- per-poll) — this is the join, not a source of new numbers.
with identity as (
    select distinct on (twitch_login)
        twitch_login,
        twitch_id,
        display_name
    from {{ ref('stg_bronze__zevent_snapshots') }}
    where twitch_login is not null
    order by twitch_login asc, snapshot_id desc
),

donations as (
    select
        twitch_login,
        max(donation_amount_eur) as latest_donation_amount_eur,
        sum(case when donation_delta_eur > 0 then donation_delta_eur else 0 end) as donation_delta_sum_eur
    from {{ ref('int_donations__streamer_deltas') }}
    group by twitch_login
),

chat_totals as (
    select
        channel,
        count(*) as unique_chatter_count,
        sum(message_count) as total_message_count
    from {{ ref('int_chat__chatter_channel_activity') }}
    group by channel
),

chatter_profile_mix as (
    select
        a.channel,
        p.chatter_profile,
        count(*) as chatter_count
    from {{ ref('int_chat__chatter_channel_activity') }} as a
    inner join {{ ref('mart_chatters__profile') }} as p on a.chatter_id = p.chatter_id
    group by a.channel, p.chatter_profile
),

chatter_profile_pivot as (
    select
        channel,
        sum(chatter_count) filter (where chatter_profile = 'sedentaire') as sedentaire_chatter_count,
        sum(chatter_count) filter (where chatter_profile = 'multi_streamer') as multi_streamer_chatter_count,
        sum(chatter_count) filter (where chatter_profile = 'semi_nomade') as semi_nomade_chatter_count,
        sum(chatter_count) filter (where chatter_profile = 'nomade') as nomade_chatter_count
    from chatter_profile_mix
    group by channel
),

viewership as (
    select
        channel,
        broadcaster_id,
        count(*) as session_count,
        sum(duration_seconds) as total_stream_duration_seconds,
        avg(avg_viewer_count) as avg_viewer_count,
        max(peak_viewer_count) as peak_viewer_count
    from {{ ref('int_streams__sessions') }}
    group by channel, broadcaster_id
),

top_category as (
    select distinct on (channel)
        channel,
        category as top_category
    from (
        select
            channel,
            category,
            count(*) as snapshot_count
        from {{ ref('stg_bronze__metadata_snapshots') }}
        where category is not null
        group by channel, category
    ) as ranked
    order by channel asc, snapshot_count desc
),

category_diversity as (
    select
        channel,
        count(distinct category) as category_diversity_score
    from {{ ref('stg_bronze__metadata_snapshots') }}
    where category is not null
    group by channel
),

-- Uptime denominator: the observed data's own time span, not a hardcoded
-- Zevent schedule — see mart_event__phase_segmentation's header for the
-- same caveat on a dev/test database that isn't one clean 55h event.
observed_window as (
    select extract(epoch from (max(snapshot_at) - min(snapshot_at))) as observed_seconds
    from {{ ref('stg_bronze__metadata_snapshots') }}
)

select
    i.twitch_login as channel,
    i.twitch_id,
    i.display_name,
    d.latest_donation_amount_eur,
    d.donation_delta_sum_eur,
    ct.unique_chatter_count,
    ct.total_message_count,
    cp.sedentaire_chatter_count,
    cp.multi_streamer_chatter_count,
    cp.semi_nomade_chatter_count,
    cp.nomade_chatter_count,
    v.session_count,
    v.total_stream_duration_seconds,
    v.avg_viewer_count,
    v.peak_viewer_count,
    tc.top_category,
    cd.category_diversity_score,
    v.total_stream_duration_seconds / nullif(ow.observed_seconds, 0) as uptime_pct
from identity as i
left join donations as d on i.twitch_login = d.twitch_login
left join chat_totals as ct on i.twitch_login = ct.channel
left join chatter_profile_pivot as cp on i.twitch_login = cp.channel
left join viewership as v on i.twitch_login = v.channel
left join top_category as tc on i.twitch_login = tc.channel
left join category_diversity as cd on i.twitch_login = cd.channel
cross join observed_window as ow
