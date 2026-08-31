-- Current donation goals — a snapshot dimension, not a time series:
-- bronze_donation_goals is UPSERT/latest-state only (no "accomplished" flag,
-- see zevent_donation_goals.py's module docstring), so this is "what goals
-- exist and their targets right now", joinable onto the fact marts by
-- twitch_login — not "how close each one is to being met over time".
select
    participation_id,
    streamer_name,
    twitch_login,
    twitch_id,
    goal_id,
    goal_name,
    goal_amount_eur,
    goal_category,
    snapshot_at,
    count(*) over (partition by twitch_login) as goal_count_for_streamer,
    sum(goal_amount_eur) over (partition by twitch_login) as total_goal_amount_eur_for_streamer
from {{ ref('stg_bronze__donation_goals') }}
