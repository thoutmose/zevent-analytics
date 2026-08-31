-- Each streamer's donation-total rank at every snapshot, plus the rank
-- change from their own previous snapshot — reconstructable because
-- zevent_api.py polls the whole streamer array in one go, so every
-- streamer's rank at a given ingested_at is directly comparable (same
-- poll, not approximated across different poll times).
with ranked as (
    select
        twitch_login,
        display_name,
        ingested_at,
        donation_amount_eur,
        rank() over (
            partition by ingested_at order by donation_amount_eur desc
        ) as donation_rank
    from {{ ref('stg_bronze__zevent_snapshots') }}
    where twitch_login is not null
)

select
    twitch_login,
    display_name,
    ingested_at,
    donation_amount_eur,
    donation_rank,
    lag(donation_rank) over (
        partition by twitch_login order by ingested_at
    ) as prev_donation_rank,
    donation_rank - lag(donation_rank) over (
        partition by twitch_login order by ingested_at
    ) as rank_change
from ranked
