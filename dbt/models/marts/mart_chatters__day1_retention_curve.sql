-- Day-N retention curve: for each chatter, day_index counts days since their
-- own first active day (0 = the day they first appeared). Aggregating that
-- across chatters gives the classic retention curve, which only ever has a
-- handful of day_index values (the event spans a few days) — precomputed
-- here instead of joining int_chat__chatter_quarter_hourly_activity's
-- 15-minute-bucket grain against itself at every page load.
with chatter_days as (
    select distinct
        chatter_id,
        date_trunc('day', quarter_hour_bucket) as activity_day
    from {{ ref('int_chat__chatter_quarter_hourly_activity') }}
),

day_offsets as (
    select
        chatter_id,
        extract(
            day from activity_day - min(activity_day) over (partition by chatter_id)
        )::int as day_index
    from chatter_days
),

cohort as (
    select
        day_index,
        count(distinct chatter_id) as active_chatter_count
    from day_offsets
    group by 1
)

select
    day_index,
    active_chatter_count,
    first_value(active_chatter_count) over (order by day_index) as cohort_size,
    active_chatter_count::numeric
        / nullif(first_value(active_chatter_count) over (order by day_index), 0)
        as retention_pct
from cohort
