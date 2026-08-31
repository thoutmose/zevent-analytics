-- Per-streamer donation velocity: the cumulative donation_amount_eur each
-- zevent.fr/api/ snapshot carries is diffed against the previous snapshot for
-- that streamer, turning a running total into "how much came in between
-- these two polls" — the number that actually says which moments moved
-- money, not just the (much less interesting) ever-climbing total.
with ordered as (
    select
        twitch_login,
        display_name,
        game,
        online,
        viewer_count,
        donation_amount_eur,
        -- Event-wide totals, denormalized here from the parent snapshot
        -- (same value repeated across every streamer in that poll) so
        -- downstream marts can plot per-streamer and event-wide donation
        -- curves on the same timeline without a second join.
        total_donation_amount_eur,
        total_viewer_count,
        ingested_at,
        lag(donation_amount_eur) over (
            partition by twitch_login order by ingested_at
        ) as prev_donation_amount_eur,
        lag(ingested_at) over (
            partition by twitch_login order by ingested_at
        ) as prev_ingested_at
    from {{ ref('stg_bronze__zevent_snapshots') }}
    where twitch_login is not null
)

select
    twitch_login,
    display_name,
    game,
    online,
    viewer_count,
    donation_amount_eur,
    total_donation_amount_eur,
    total_viewer_count,
    ingested_at,
    prev_ingested_at,
    -- First snapshot per streamer has no predecessor: 0, not NULL — it's a
    -- starting point, not a missing observation.
    coalesce(donation_amount_eur - prev_donation_amount_eur, 0) as donation_delta_eur,
    extract(epoch from (ingested_at - prev_ingested_at)) as seconds_since_prev_snapshot
from ordered
