-- Leaderboard rank and rank-change, downsampled to the last snapshot
-- observed within each hour — the hourly counterpart to
-- int_donations__hourly_channel_activity. mart_donations__rank_churn scans
-- every snapshot (3.4s for leaderboard_movers); this lets that query read
-- one row for the hour closest to :end instead.
with hourly as (
    select
        twitch_login,
        display_name,
        donation_rank,
        prev_donation_rank,
        rank_change,
        date_trunc('hour', ingested_at) as hour_bucket,
        row_number() over (
            partition by twitch_login, date_trunc('hour', ingested_at)
            order by ingested_at desc
        ) as rn
    from {{ ref('int_donations__leaderboard_rank') }}
)

select
    twitch_login,
    display_name,
    hour_bucket,
    donation_rank,
    prev_donation_rank,
    rank_change
from hourly
where rn = 1
