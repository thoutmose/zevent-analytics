-- Directed flow graph aggregated from int_chat__message_transitions: who
-- feeds viewers to whom. The closest available proxy to the raid data
-- dropped along with EventSub (see README.md's Overview) — inferred from
-- chat behavior, not an actual raid event.
select
    from_channel,
    to_channel,
    count(*) as hop_count,
    count(distinct chatter_id) as distinct_chatters_hopping,
    avg(gap_seconds) as avg_gap_seconds
from {{ ref('int_chat__message_transitions') }}
group by 1, 2
