-- Donation efficiency per average viewer / per unique chatter — the fair
-- comparison across streamers of very different audience sizes that a raw
-- donation total can't give you.
with streamer_totals as (
    select
        twitch_login,
        max(donation_amount_eur) as latest_donation_amount_eur,
        avg(viewer_count) as avg_viewer_count
    from {{ ref('int_donations__streamer_deltas') }}
    group by 1
),

chat_totals as (
    select
        channel,
        count(*) as unique_chatter_count
    from {{ ref('int_chat__chatter_channel_activity') }}
    group by 1
)

select
    st.twitch_login,
    st.latest_donation_amount_eur,
    st.avg_viewer_count,
    ct.unique_chatter_count,
    st.latest_donation_amount_eur / nullif(st.avg_viewer_count, 0)
        as donation_eur_per_avg_viewer,
    st.latest_donation_amount_eur / nullif(ct.unique_chatter_count, 0)
        as donation_eur_per_unique_chatter
from streamer_totals as st
left join chat_totals as ct on st.twitch_login = ct.channel
