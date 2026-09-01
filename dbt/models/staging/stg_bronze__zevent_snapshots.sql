-- One row per (snapshot, streamer): unnests bronze_zevent_snapshots.streamers
-- (a jsonb array, one element per streamer at that poll) so every downstream
-- model can join/aggregate on twitch_login without repeating this lateral
-- join. Kept in staging, not intermediate, since it's a structural flatten
-- of one source table with no cross-source logic — see README.md's Data
-- model for the array's shape (twitch_id, twitch_login, display_name,
-- profile_url, online, game, viewer_count, donation_amount_eur). Renamed to
-- is_online here since "online" is a SQL keyword (RF04) that would need
-- quoting downstream otherwise.
select
    s.id as snapshot_id,
    s.batch_id,
    s.row_number,
    s.website_mode,
    s.total_donation_amount_eur,
    s.total_viewer_count,
    s.ingested_at,
    streamer.value ->> 'twitch_id' as twitch_id,
    lower(streamer.value ->> 'twitch_login') as twitch_login,
    streamer.value ->> 'display_name' as display_name,
    streamer.value ->> 'profile_url' as profile_url,
    (streamer.value ->> 'online')::boolean as is_online,
    streamer.value ->> 'game' as game,
    (streamer.value ->> 'viewer_count')::int as viewer_count,
    (streamer.value ->> 'donation_amount_eur')::numeric as donation_amount_eur
from {{ source('bronze', 'bronze_zevent_snapshots') }} as s
cross join lateral jsonb_array_elements(s.streamers) as streamer(value)
