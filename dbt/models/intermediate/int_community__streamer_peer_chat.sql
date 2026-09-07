-- Messages where the chatter is themself a Zevent-roster streamer, posted in
-- a channel that isn't their own — the participant-to-participant support
-- signal (peer shoutouts/chat visits), distinct from mart_community__*
-- (built on shared regular-viewer overlap, never on the participants
-- themselves). Matched on login, lowercased on both sides the same way
-- stg_bronze__zevent_snapshots already lowercases twitch_login.
select
    lc.chatter_id,
    lower(lc.chatter) as supporting_streamer_login,
    lc.channel as supported_channel,
    lc.message_sent_at
from {{ ref('stg_bronze__live_chat') }} as lc
inner join (select distinct twitch_login from {{ ref('stg_bronze__zevent_snapshots') }}) as roster
    on lower(lc.chatter) = roster.twitch_login
where lc.chatter_id is not null and lower(lc.chatter) != lc.channel
