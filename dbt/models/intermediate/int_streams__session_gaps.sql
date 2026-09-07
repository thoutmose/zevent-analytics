-- Gap between one streamer's consecutive broadcast sessions — the
-- between-session counterpart to int_chat__inter_arrival (which does this
-- for chat messages): how long a streamer was offline before their next
-- session, surfacing rest/break patterns during the marathon.
select
    channel,
    broadcaster_id,
    stream_started_at,
    last_seen_at,
    lead(stream_started_at) over (
        partition by channel order by stream_started_at
    ) as next_session_started_at,
    lead(stream_started_at) over (
        partition by channel order by stream_started_at
    ) - last_seen_at as gap_before_next_session
from {{ ref('int_streams__sessions') }}
