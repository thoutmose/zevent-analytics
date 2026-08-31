-- One row per detected category/title switch, while live: compares each
-- snapshot to the previous one for that channel (chronologically) and keeps
-- only the moments something actually changed — the "annotations on the
-- viewer timeline" marking exactly when a streamer switched games and what
-- it did to their numbers, joinable onto int_streams__hourly_viewership by
-- channel + a time range.
with ordered as (
    select
        channel,
        broadcaster_id,
        title,
        category,
        viewer_count,
        snapshot_at,
        lag(category) over (partition by channel order by snapshot_at) as prev_category,
        lag(title) over (partition by channel order by snapshot_at) as prev_title,
        lag(viewer_count) over (partition by channel order by snapshot_at) as prev_viewer_count
    from {{ ref('stg_bronze__metadata_snapshots') }}
    where is_live
)

select
    channel,
    broadcaster_id,
    snapshot_at as changed_at,
    prev_category,
    category as new_category,
    prev_title,
    title as new_title,
    prev_viewer_count,
    viewer_count as viewer_count_at_change
from ordered
where category is distinct from prev_category
    or title is distinct from prev_title
