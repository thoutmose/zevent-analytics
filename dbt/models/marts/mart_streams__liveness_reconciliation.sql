-- Cross-checks Twitch Helix's own is_live (metadata_snapshots, main.py)
-- against zevent.fr's self-reported is_online (zevent_snapshots) for the
-- same streamer at close to the same moment — same reconciliation idea as
-- mart_donations__reconciliation, applied to liveness instead of amounts.
-- Bounded to a 5-minute lookback for the same reason mart_donations__by_title
-- is: metadata polls every ~15s, zevent polls every ~20s, so a Helix
-- snapshot more than 5 minutes stale isn't a meaningful "was this streamer
-- actually live right then" comparison anymore. A mismatch is only flagged
-- when a Helix snapshot was actually found in that window — a missing one
-- means no data to compare, not a contradiction.
select
    z.twitch_login,
    z.snapshot_id,
    z.ingested_at as zevent_snapshot_at,
    z.is_online as zevent_reported_online,
    m.is_live as helix_reported_live,
    m.snapshot_at as helix_snapshot_at,
    (m.snapshot_at is not null and z.is_online is distinct from m.is_live) as liveness_mismatch
from {{ ref('stg_bronze__zevent_snapshots') }} as z
left join lateral (
    select
        ms.is_live,
        ms.snapshot_at
    from {{ ref('stg_bronze__metadata_snapshots') }} as ms
    where
        ms.channel = z.twitch_login
        and ms.snapshot_at <= z.ingested_at
        and ms.snapshot_at >= z.ingested_at - interval '5 minutes'
    order by ms.snapshot_at desc
    limit 1
) as m on true
