-- Streamer-to-streamer network graph feed: self-joins the bipartite
-- chatter<->streamer edges on (chatter_id, hour_bucket) to find channel
-- pairs with shared audience — the aggregated, ~n_channels-node view meant
-- for the animated network graph (a raw chatter-level graph would be
-- unreadable at this scale; this stays compact regardless of chatter count).
-- channel_a < channel_b avoids both self-pairs and double-counting each
-- pair in both directions.
select
    a.hour_bucket,
    a.channel as channel_a,
    b.channel as channel_b,
    count(distinct a.chatter_id) as shared_chatter_count,
    sum(least(a.message_count, b.message_count)) as min_message_overlap
from {{ ref('int_community__hourly_chatter_streamer_edges') }} as a
inner join {{ ref('int_community__hourly_chatter_streamer_edges') }} as b
    on a.chatter_id = b.chatter_id
    and a.hour_bucket = b.hour_bucket
    and a.channel < b.channel
group by 1, 2, 3
