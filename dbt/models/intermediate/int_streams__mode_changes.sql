-- One row per detected change in zevent.fr's own reported website_mode,
-- event-wide — the site's own phase signal, as opposed to
-- mart_event__phase_segmentation's relative-time-based opening/middle/
-- final_push. Kept at snapshot grain (one row per snapshot_id, not per
-- streamer) since website_mode is the same for every streamer row in a
-- given poll — distinct so a single poll never produces duplicate "changes".
with snapshots as (
    select distinct
        snapshot_id,
        ingested_at,
        website_mode,
        total_donation_amount_eur,
        total_viewer_count
    from {{ ref('stg_bronze__zevent_snapshots') }}
),

ordered as (
    select
        snapshot_id,
        ingested_at,
        website_mode,
        total_donation_amount_eur,
        total_viewer_count,
        lag(website_mode) over (order by ingested_at) as prev_website_mode
    from snapshots
)

select
    snapshot_id,
    ingested_at as changed_at,
    prev_website_mode,
    website_mode as new_website_mode,
    total_donation_amount_eur,
    total_viewer_count
from ordered
where website_mode is distinct from prev_website_mode
