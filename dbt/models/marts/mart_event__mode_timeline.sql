-- Duration and donation/viewer movement for each website_mode segment:
-- pairs each mode change from int_streams__mode_changes with the next one to
-- get "how long was each mode active, and what happened during it" without
-- assuming what mode values zevent.fr actually uses upstream.
select
    new_website_mode as website_mode,
    changed_at as segment_started_at,
    lead(changed_at) over (order by changed_at) as segment_ended_at,
    total_donation_amount_eur as donation_amount_eur_at_start,
    total_viewer_count as viewer_count_at_start,
    lead(total_donation_amount_eur) over (order by changed_at) - total_donation_amount_eur
        as donation_delta_during_segment
from {{ ref('int_streams__mode_changes') }}
