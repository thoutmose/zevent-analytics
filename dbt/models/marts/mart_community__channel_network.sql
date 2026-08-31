-- Filtered view of int_community__channel_pair_jaccard (shared_chatter_count
-- >= 3, dropping single-shared-chatter noise) — the adjacency matrix input
-- for audience communities/clusters: which streamers effectively share one
-- audience vs. which are isolated islands.
select
    channel_a,
    channel_b,
    shared_chatter_count,
    channel_a_chatter_count,
    channel_b_chatter_count,
    jaccard_index
from {{ ref('int_community__channel_pair_jaccard') }}
where shared_chatter_count >= 3
