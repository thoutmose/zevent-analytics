-- Consolidates duration distribution, duration vs. donations, viewer decay,
-- and chat pace into one per-session mart rather than four near-duplicate
-- ones — all four are genuinely questions about the same row (one
-- broadcast), not different grains.
with session_snapshots as (
    select
        channel,
        stream_started_at,
        first_value(viewer_count) over (
            partition by channel, stream_started_at order by snapshot_at
        ) as viewer_count_at_start,
        last_value(viewer_count) over (
            partition by channel, stream_started_at order by snapshot_at
            rows between unbounded preceding and unbounded following
        ) as viewer_count_at_end
    from {{ ref('stg_bronze__metadata_snapshots') }}
    where is_live and stream_started_at is not null
),

session_snapshots_distinct as (
    select distinct
        channel,
        stream_started_at,
        viewer_count_at_start,
        viewer_count_at_end
    from session_snapshots
),

session_chat as (
    select
        s.channel,
        s.stream_started_at,
        count(*) filter (
            where lc.message_sent_at < s.first_seen_at + (s.last_seen_at - s.first_seen_at) / 2
        ) as message_count_first_half,
        count(*) filter (
            where lc.message_sent_at >= s.first_seen_at + (s.last_seen_at - s.first_seen_at) / 2
        ) as message_count_second_half
    from {{ ref('int_streams__sessions') }} as s
    inner join {{ ref('stg_bronze__live_chat') }} as lc
        on
            s.channel = lc.channel
            and lc.message_sent_at between s.first_seen_at and s.last_seen_at
    group by 1, 2
),

session_donations as (
    select
        s.channel,
        s.stream_started_at,
        sum(d.donation_delta_eur) as donation_delta_during_session
    from {{ ref('int_streams__sessions') }} as s
    inner join {{ ref('int_donations__streamer_deltas') }} as d
        on
            s.channel = d.twitch_login
            and d.ingested_at between s.first_seen_at and s.last_seen_at
    group by 1, 2
)

select
    s.channel,
    s.broadcaster_id,
    s.stream_started_at,
    s.duration_seconds,
    -- 12 buckets up to 12h; a longer session just lands in the last bucket.
    s.avg_viewer_count,
    s.peak_viewer_count,
    ss.viewer_count_at_start,
    ss.viewer_count_at_end,
    sc.message_count_first_half,
    sc.message_count_second_half,
    sd.donation_delta_during_session,
    width_bucket(s.duration_seconds, 0, 43200, 12) as duration_bucket_index,
    ss.viewer_count_at_end - ss.viewer_count_at_start as viewer_change
from {{ ref('int_streams__sessions') }} as s
left join session_snapshots_distinct as ss
    on s.channel = ss.channel and s.stream_started_at = ss.stream_started_at
left join session_chat as sc
    on s.channel = sc.channel and s.stream_started_at = sc.stream_started_at
left join session_donations as sd
    on s.channel = sd.channel and s.stream_started_at = sd.stream_started_at
