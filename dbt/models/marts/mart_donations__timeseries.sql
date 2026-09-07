-- The "donation total over time" hero chart, plus the far more interesting
-- velocity (€/min) that actually shows which moments moved money — per
-- streamer, per day, and event-wide (total_donation_amount_eur/
-- total_viewer_count, denormalized onto every row from the parent snapshot).
{{ config(indexes=[{'columns': ['twitch_login', 'ingested_at']}]) }}

select
    twitch_login,
    display_name,
    game,
    is_online,
    viewer_count,
    donation_amount_eur,
    donation_delta_eur,
    total_donation_amount_eur,
    total_viewer_count,
    ingested_at,
    seconds_since_prev_snapshot,
    date_trunc('day', ingested_at) as day_bucket,
    case
        when seconds_since_prev_snapshot > 0
            then donation_delta_eur / (seconds_since_prev_snapshot / 60.0)
    end as donation_velocity_eur_per_min
from {{ ref('int_donations__streamer_deltas') }}
