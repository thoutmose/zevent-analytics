-- Per (channel, service, emote), whether a registered emote was ever used —
-- joins the emote catalog (what exists) against int_chat__emote_usage (what
-- got typed) so downstream can tell live inventory from dead inventory.
-- Global-scope entries are addressable from every channel with chat
-- activity, so they're cross-joined against distinct chatted-in channels
-- rather than deduplicated across channels — coverage is inherently a
-- per-channel question, same reasoning stg_bronze__emote_catalog's
-- '__global__' sentinel comment gives for not nulling scope out.
with channel_scoped as (
    select
        channel,
        service,
        emote_id
    from {{ ref('stg_bronze__emote_catalog') }}
    where scope = 'channel'
),

global_scoped as (
    select
        lc.channel,
        ec.service,
        ec.emote_id
    from {{ ref('stg_bronze__emote_catalog') }} as ec
    cross join (select distinct channel from {{ ref('stg_bronze__live_chat') }}) as lc
    where ec.scope = 'global'
),

catalog as (
    select * from channel_scoped
    union
    select * from global_scoped
),

usage as (
    select
        channel,
        service,
        emote_id,
        sum(usage_count) as total_usage_count
    from {{ ref('int_chat__emote_usage') }}
    group by 1, 2, 3
)

select
    c.channel,
    c.service,
    c.emote_id,
    coalesce(u.total_usage_count, 0) as total_usage_count,
    (u.total_usage_count is not null) as was_used
from catalog as c
left join usage as u
    on
        c.channel = u.channel
        and c.service = u.service
        and c.emote_id = u.emote_id
