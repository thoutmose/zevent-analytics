-- Event-wide (not hourly, unlike int_community__hourly_chatter_streamer_edges)
-- channel-to-channel audience overlap: Jaccard index on each pair's shared
-- chatter sets. This is the adjacency-matrix input for audience
-- communities/clusters — which streamers effectively share one audience vs.
-- which are isolated islands — something only possible because this project
-- ingests all ~300+ channels at once rather than one.
with chatter_channels as (
    select distinct
        chatter_id,
        channel
    from {{ ref('stg_bronze__live_chat') }}
    where chatter_id is not null
),

pair_overlap as (
    select
        a.channel as channel_a,
        b.channel as channel_b,
        count(*) as shared_chatter_count
    from chatter_channels as a
    inner join chatter_channels as b
        on a.chatter_id = b.chatter_id and a.channel < b.channel
    group by 1, 2
),

channel_totals as (
    select
        channel,
        count(distinct chatter_id) as chatter_count
    from chatter_channels
    group by 1
)

select
    p.channel_a,
    p.channel_b,
    p.shared_chatter_count,
    ta.chatter_count as channel_a_chatter_count,
    tb.chatter_count as channel_b_chatter_count,
    p.shared_chatter_count::numeric
    / nullif(ta.chatter_count + tb.chatter_count - p.shared_chatter_count, 0)
        as jaccard_index
from pair_overlap as p
inner join channel_totals as ta on p.channel_a = ta.channel
inner join channel_totals as tb on p.channel_b = tb.channel
