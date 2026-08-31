-- Goal category breakdown (e.g. "recurent") — never analyzed on its own
-- before, only ever passed through as a column on the per-goal marts.
select
    goal_category,
    count(*) as goal_count,
    count(distinct twitch_login) as streamer_count,
    sum(goal_amount_eur) as total_goal_amount_eur,
    avg(goal_amount_eur) as avg_goal_amount_eur
from {{ ref('stg_bronze__donation_goals') }}
group by 1
