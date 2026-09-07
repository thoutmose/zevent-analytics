-- Consecutive (channel, title, category) snapshots collapsed into segments —
-- the same dedup stream_activity_log currently recomputes at query time with
-- LAG()/window functions over stg_bronze__metadata_snapshots' 5.16M rows.
-- Also feeds title_leaderboard.
with ordered as (
    select
        channel,
        broadcaster_id,
        title,
        category,
        viewer_count,
        snapshot_at,
        lag(title) over (partition by channel order by snapshot_at) as prev_title,
        lag(category) over (partition by channel order by snapshot_at) as prev_category
    from {{ ref('stg_bronze__metadata_snapshots') }}
    where is_live
),

flagged as (
    select
        *,
        case
            when title is distinct from prev_title or category is distinct from prev_category
                then 1
            else 0
        end as is_new_segment
    from ordered
),

segmented as (
    select
        *,
        sum(is_new_segment) over (partition by channel order by snapshot_at) as segment_id
    from flagged
)

select
    channel,
    broadcaster_id,
    segment_id,
    title,
    category,
    min(snapshot_at) as segment_started_at,
    max(snapshot_at) as segment_ended_at,
    count(*) as snapshot_count,
    avg(viewer_count) as avg_viewer_count,
    max(viewer_count) as max_viewer_count
from segmented
group by 1, 2, 3, 4, 5
