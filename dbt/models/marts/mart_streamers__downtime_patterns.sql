-- Break/downtime patterns per streamer — break count, average/longest/
-- shortest gap between sessions — a marathon-fatigue signal distinct from
-- mart_streamers__night_shift (donation efficiency by hour-of-day) and
-- mart_streams__session_analysis (per-session stats, not the gaps between
-- sessions).
select
    channel,
    broadcaster_id,
    count(gap_before_next_session) as break_count,
    avg(gap_before_next_session) as avg_gap,
    max(gap_before_next_session) as longest_gap,
    min(gap_before_next_session) as shortest_gap
from {{ ref('int_streams__session_gaps') }}
where gap_before_next_session is not null
group by 1, 2
