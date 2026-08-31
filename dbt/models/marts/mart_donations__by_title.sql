-- Which stream titles generated the most donations: joins each donation
-- delta to that streamer's most recent known title at (or just before) the
-- moment the donation landed. Bounded to a 5-minute lookback (metadata polls
-- every ~15s, donation snapshots every ~20s — a title more than 5 minutes
-- stale isn't a meaningful "what was playing" association anymore) mainly so
-- the lateral join has a tight window to search instead of scanning
-- metadata_snapshots' full history per donation row.
with donations as (
    select *
    from {{ ref('int_donations__streamer_deltas') }}
    where donation_delta_eur > 0
)

select
    d.twitch_login,
    d.display_name,
    d.ingested_at,
    d.donation_delta_eur,
    m.title,
    m.category
from donations as d
left join lateral (
    select ms.title, ms.category
    from {{ ref('stg_bronze__metadata_snapshots') }} as ms
    where ms.channel = d.twitch_login
        and ms.snapshot_at <= d.ingested_at
        and ms.snapshot_at >= d.ingested_at - interval '5 minutes'
    order by ms.snapshot_at desc
    limit 1
) as m on true
