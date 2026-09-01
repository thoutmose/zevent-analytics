-- Category/title changes as annotations for the viewer timeline: each
-- switch, plus the viewer count in the hour right after it, so a chart can
-- show "what did switching to X do to this streamer's numbers."
select
    c.channel,
    c.broadcaster_id,
    c.changed_at,
    c.prev_category,
    c.new_category,
    c.prev_title,
    c.new_title,
    c.viewer_count_at_change,
    v.avg_viewer_count as avg_viewer_count_hour_after
from {{ ref('int_streams__category_changes') }} as c
left join {{ ref('int_streams__hourly_viewership') }} as v
    on
        c.channel = v.channel
        and v.hour_bucket = date_trunc('hour', c.changed_at) + interval '1 hour'
