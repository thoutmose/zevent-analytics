-- Registered-vs-used emote inventory per (channel, service): what share of
-- a channel's addressable emote set (its own custom set plus the service's
-- global one) actually got typed in chat vs. sat unused — the catalog-side
-- counterpart to mart_chat__emote_trends, which only ever sees emotes that
-- were used at least once.
select
    channel,
    service,
    count(*) as catalog_size,
    count(*) filter (where was_used) as used_count,
    count(*) filter (where was_used)::numeric / nullif(count(*), 0) as utilization_pct,
    sum(total_usage_count) as total_usage_count
from {{ ref('int_chat__emote_catalog_coverage') }}
group by 1, 2
