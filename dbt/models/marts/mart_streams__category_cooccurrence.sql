-- Which games were being played simultaneously across channels, per hour —
-- a per-category "share of event" timeseries, joinable to
-- mart_streams__viewership_timeseries / mart_donations__timeseries to check
-- whether a game trending across many channels at once coincided with a
-- viewer or donation lift.
select
    date_trunc('hour', snapshot_at) as hour_bucket,
    category,
    count(distinct channel) as channel_count_playing
from {{ ref('stg_bronze__metadata_snapshots') }}
where is_live and category is not null
group by 1, 2
