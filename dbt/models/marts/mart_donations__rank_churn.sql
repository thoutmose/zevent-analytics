-- Leaderboard volatility: who climbed, who dropped, and how often the top
-- 10 actually changed — a different (and more narratively useful) signal
-- than donation totals alone.
{{ config(indexes=[{'columns': ['twitch_login', 'ingested_at']}]) }}

select
    twitch_login,
    display_name,
    ingested_at,
    donation_rank,
    prev_donation_rank,
    rank_change,
    (donation_rank <= 10) as in_top_10
from {{ ref('int_donations__leaderboard_rank') }}
