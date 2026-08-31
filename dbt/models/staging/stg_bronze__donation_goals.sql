-- 1:1 typed view over bronze_donation_goals. Latest-state (UPSERT) upstream
-- — this view has no history either, see README.md's "Known limitations".
select
    participation_id,
    streamer_name,
    lower(twitch_login) as twitch_login,
    twitch_id,
    goal_id,
    goal_name,
    goal_amount_eur,
    goal_category,
    snapshot_at,
    ingested_at
from {{ source('bronze', 'bronze_donation_goals') }}
