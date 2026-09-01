-- Per (channel, hour, service, emote), how many times an emote was used.
-- Two entirely different detection mechanisms, unioned into one grain:
--   - native: bronze_live_chat.emotes is a real IRC-tag signal, already
--     counted per message (main.py's _parse_emotes) — just unnested here.
--   - 7tv/bttv/ffz: no tag exists at all (see main.py's module docstring),
--     so a message's whitespace-split tokens are matched against that
--     channel's + the service's global fetched catalog. This is a plain
--     token match, not word-boundary-aware — an emote code that's also a
--     common word (rare, by convention emote codes are CamelCase-distinct)
--     could false-positive; not corrected for here.
with native as (
    select
        lc.channel,
        date_trunc('hour', lc.message_sent_at) as hour_bucket,
        'twitch' as service,
        kv.key as emote_id,
        sum(kv.value::int) as usage_count
    from {{ ref('stg_bronze__live_chat') }} as lc
    cross join lateral jsonb_each_text(lc.emotes) as kv(key, value)
    where lc.emotes is not null and lc.emotes != '{}'::jsonb
    group by 1, 2, 3, 4
),

third_party_tokens as (
    select
        lc.channel,
        token.token,
        date_trunc('hour', lc.message_sent_at) as hour_bucket
    from {{ ref('stg_bronze__live_chat') }} as lc
    cross join lateral regexp_split_to_table(lc.message_text, '\s+') as token
    where lc.message_text is not null and lc.message_text != ''
),

third_party as (
    select
        t.channel,
        t.hour_bucket,
        ec.service,
        ec.emote_id,
        count(*) as usage_count
    from third_party_tokens as t
    inner join {{ ref('stg_bronze__emote_catalog') }} as ec
        on
            t.token = ec.emote_code
            and ec.service in ('7tv', 'bttv', 'ffz')
            and (t.channel = ec.channel or ec.channel = '__global__')
    group by 1, 2, 3, 4
)

select * from native
union all
select * from third_party
