-- Phase-level donation totals — mart_event__phase_segmentation's own grain
-- is one row per (streamer, snapshot) (3.98M rows), but event_phase_breakdown
-- only ever needs the 3-4 phase totals it group-bys down to at query time.
-- Precomputed here instead.
select
    event_phase,
    count(distinct twitch_login) as streamer_count,
    sum(donation_delta_eur) as total_donation_delta_eur,
    sum(donation_delta_eur) / nullif(sum(sum(donation_delta_eur)) over (), 0)
        as pct_of_event_total
from {{ ref('mart_event__phase_segmentation') }}
group by 1
