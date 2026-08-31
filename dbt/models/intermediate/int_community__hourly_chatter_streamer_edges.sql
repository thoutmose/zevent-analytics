-- Bipartite chatter<->streamer edges, weighted by message count, bucketed
-- hourly — the raw material for the network graph (mart_community__
-- hourly_network self-joins this on chatter_id + hour_bucket to find
-- streamer pairs with shared audience). Same grain as
-- int_chat__hourly_channel_activity but per-chatter rather than aggregated,
-- since the network view needs the individual edges, not just the totals.
--
-- Represents chatters who wrote, not the full audience: Twitch exposes no
-- silent-viewer list, only active chatters — see README.md's "Known
-- limitations". A chatter only appears here for hours they actually posted
-- in.
select
    chatter_id,
    channel,
    date_trunc('hour', message_sent_at) as hour_bucket,
    count(*) as message_count
from {{ ref('stg_bronze__live_chat') }}
where chatter_id is not null
group by 1, 2, 3
