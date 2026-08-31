-- Sum of a streamer's own goal targets vs. what they've actually raised —
-- a rough completion signal even though per-goal progress isn't trackable
-- (bronze_donation_goals is latest-state only, see README.md's Known
-- limitations).
select
    g.twitch_login,
    g.total_goal_amount_eur_for_streamer,
    d.latest_donation_amount_eur,
    d.latest_donation_amount_eur - g.total_goal_amount_eur_for_streamer as surplus_eur,
    d.latest_donation_amount_eur / nullif(g.total_goal_amount_eur_for_streamer, 0)
        as pct_of_goals_covered
from (
    select distinct twitch_login, total_goal_amount_eur_for_streamer
    from {{ ref('mart_donation_goals__current') }}
) as g
inner join (
    select twitch_login, max(donation_amount_eur) as latest_donation_amount_eur
    from {{ ref('int_donations__streamer_deltas') }}
    group by 1
) as d on d.twitch_login = g.twitch_login
