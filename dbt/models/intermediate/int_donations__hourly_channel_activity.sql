-- Donation total per streamer per hour, downsampled to the last snapshot
-- observed within that hour — the int_chat__hourly_channel_activity pattern
-- applied to donations. mart_donations__timeseries is a full per-snapshot
-- scan (3.98M rows); channel_donations_leaderboard_timeseries only ever
-- needs each streamer's running total at the end of each hour, so this
-- keeps ~22k rows (343 streamers x ~66h) instead.
with hourly as (
    select
        twitch_login,
        display_name,
        donation_amount_eur,
        date_trunc('hour', ingested_at) as hour_bucket,
        row_number() over (
            partition by twitch_login, date_trunc('hour', ingested_at)
            order by ingested_at desc
        ) as rn
    from {{ ref('int_donations__streamer_deltas') }}
)

select
    twitch_login,
    display_name,
    hour_bucket,
    donation_amount_eur
from hourly
where rn = 1
