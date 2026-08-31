-- 1:1 typed view over bronze_emote_catalog. Latest-state (UPSERT) upstream,
-- same as donation_goals. `channel` is the '__global__' sentinel for
-- scope='global' rows (see emote_catalog.py / sql/005_emote_catalog.sql) —
-- kept as-is here rather than nulled out, so a join on `channel` still needs
-- an explicit `union all`/`or channel = '__global__'` downstream instead of
-- silently matching nothing, which is the whole reason that sentinel exists.
select
    service,
    scope,
    channel,
    emote_id,
    emote_code,
    fetched_at,
    ingested_at
from {{ source('bronze', 'bronze_emote_catalog') }}
