-- Top 200 donation-velocity spikes, with the title/category live at that
-- moment and that channel's chat volume that hour — a highlight-reel mart
-- tying donations, streams, and chat together, not just a rename of
-- mart_donations__timeseries.
with spikes as (
    select
        twitch_login,
        display_name,
        ingested_at,
        donation_delta_eur
    from {{ ref('int_donations__streamer_deltas') }}
    where donation_delta_eur > 0
)

select
    s.twitch_login,
    s.display_name,
    s.ingested_at,
    s.donation_delta_eur,
    m.title,
    m.category,
    c.message_count as chat_messages_that_hour
from spikes as s
left join lateral (
    select ms.title, ms.category
    from {{ ref('stg_bronze__metadata_snapshots') }} as ms
    where ms.channel = s.twitch_login
        and ms.snapshot_at <= s.ingested_at
        and ms.snapshot_at >= s.ingested_at - interval '5 minutes'
    order by ms.snapshot_at desc
    limit 1
) as m on true
left join {{ ref('int_chat__hourly_channel_activity') }} as c
    on c.channel = s.twitch_login
    and c.hour_bucket = date_trunc('hour', s.ingested_at)
order by s.donation_delta_eur desc
limit 200
