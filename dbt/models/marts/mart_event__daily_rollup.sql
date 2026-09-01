-- Daily rollup (donations, viewers, chat volume) across the event's days —
-- the "analyse temporelle journaliere" view. Built from the intermediate
-- models directly, not other marts, to keep this a leaf output.
with chat_daily as (
    select
        date_trunc('day', hour_bucket) as day_bucket,
        sum(message_count) as message_count,
        count(distinct channel) as active_channels
    from {{ ref('int_chat__hourly_channel_activity') }}
    group by 1
),

donations_daily as (
    select
        date_trunc('day', ingested_at) as day_bucket,
        sum(donation_delta_eur) as donation_delta_eur,
        max(total_donation_amount_eur) as total_donation_amount_eur_end_of_day,
        avg(total_viewer_count) as avg_total_viewer_count
    from {{ ref('int_donations__streamer_deltas') }}
    group by 1
)

select
    c.message_count,
    c.active_channels,
    d.donation_delta_eur,
    d.total_donation_amount_eur_end_of_day,
    d.avg_total_viewer_count,
    coalesce(c.day_bucket, d.day_bucket) as day_bucket
from chat_daily as c
full outer join donations_daily as d on c.day_bucket = d.day_bucket
