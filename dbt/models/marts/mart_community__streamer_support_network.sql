-- Directed peer-support graph: which Zevent streamer chatted in which other
-- streamer's channel, and how often — the participant-to-participant
-- counterpart to mart_community__channel_network's shared-audience view.
select
    supporting_streamer_login,
    supported_channel,
    count(*) as message_count,
    min(message_sent_at) as first_message_at,
    max(message_sent_at) as last_message_at
from {{ ref('int_community__streamer_peer_chat') }}
group by 1, 2
