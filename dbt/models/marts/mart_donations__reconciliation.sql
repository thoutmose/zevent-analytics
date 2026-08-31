-- Zevent API's event-wide total vs. the sum of per-streamer amounts, per
-- snapshot. Divergence is both a data-quality check and a real finding:
-- donations made to the event's global pot rather than attributed to any
-- one streamer.
select
    snapshot_id,
    ingested_at,
    max(total_donation_amount_eur) as total_donation_amount_eur,
    sum(donation_amount_eur) as sum_of_streamer_donations_eur,
    max(total_donation_amount_eur) - sum(donation_amount_eur) as divergence_eur
from {{ ref('stg_bronze__zevent_snapshots') }}
group by 1, 2
