-- Opening (<20% through) / middle / final-push (>80% through) donation
-- velocity, by relative position in the observed data's own time span —
-- quantifies whether there's a real end-of-event surge, and each
-- streamer's share of it.
--
-- event_start/event_end are derived from the data itself (min/max
-- ingested_at), not a hardcoded Zevent schedule — on a dev/test database
-- spanning ongoing traffic rather than one clean 55h event, "opening" /
-- "final_push" describe the observed window, not necessarily the real
-- event's actual open/close.
with event_bounds as (
    select
        min(ingested_at) as event_start,
        max(ingested_at) as event_end
    from {{ ref('int_donations__streamer_deltas') }}
),

donations_with_phase as (
    select
        d.twitch_login,
        d.ingested_at,
        d.donation_delta_eur,
        extract(epoch from (d.ingested_at - eb.event_start))
        / nullif(extract(epoch from (eb.event_end - eb.event_start)), 0)
            as event_progress_pct
    from {{ ref('int_donations__streamer_deltas') }} as d
    cross join event_bounds as eb
)

select
    twitch_login,
    ingested_at,
    donation_delta_eur,
    event_progress_pct,
    case
        when event_progress_pct < 0.2 then 'opening'
        when event_progress_pct < 0.8 then 'middle'
        else 'final_push'
    end as event_phase
from donations_with_phase
