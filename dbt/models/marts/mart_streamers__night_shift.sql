-- Donation-per-viewer efficiency by hour-of-day, flagging overnight hours
-- (0-5) specifically — is a 4am audience more generous per capita than a
-- 9pm one?
with hourly as (
    select
        twitch_login as channel,
        extract(hour from ingested_at) as hour_of_day,
        sum(donation_delta_eur) as donation_delta_eur,
        avg(viewer_count) as avg_viewer_count
    from {{ ref('int_donations__streamer_deltas') }}
    group by 1, 2
)

select
    channel,
    hour_of_day,
    donation_delta_eur,
    avg_viewer_count,
    donation_delta_eur / nullif(avg_viewer_count, 0) as donation_eur_per_viewer,
    (hour_of_day between 0 and 5) as is_overnight
from hourly
