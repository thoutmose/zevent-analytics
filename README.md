# twitch-analytics

Two same-day test scripts for Zevent-style aggregation. Each prints
aggregated stats as JSON on every poll so you can check the data is correct
before wiring it into Airflow/NiFi — they're independent extraction tasks,
run separately.

- `main.py` — deep-dive on **one Twitch channel** via TwitchIO: chat volume,
  subs, raids, viewer count over time, stream title, category, duration.
- `zevent_api.py` — **all Zevent streamers at once** via the official
  `zevent.fr/api/`: online/offline status, viewer count, donation amount.

## `main.py` setup

1. Register a Twitch application at https://dev.twitch.tv/console/apps.
   Set **Category** to `Application Integration` and **Client Type** to
   `Public` (Device Code Flow needs a public client — no client secret).
2. Copy `.env.example` to `.env` and fill in `TWITCH_CLIENT_ID` and
   `TWITCH_CHANNEL` (the channel's login name, e.g. `zerator`, not its
   display name).
3. Install dependencies: `uv sync`
4. Run: `uv run main.py`

On first run it prints a URL — open it, log in with a Twitch account, and
authorize the app (Device Code Flow). The authorized token is cached in
`.tio.tokens.json` so you won't have to re-authorize on later runs.

## What gets collected, and what each requires

| Data | Source | Auth requirement |
|---|---|---|
| Title, category, viewer count, live/offline, duration | Helix API poll + `channel.update`/`stream.online`/`stream.offline` EventSub | none beyond the app's own token — works for any public channel |
| Chat messages, per-chatter message counts | `channel.chat.message` EventSub | just `user:read:chat` scope from whoever authorizes via DCF — works for **any** channel, not just ones you own or moderate |
| Raids received | `channel.raid` EventSub | none — works for any channel |
| Subs | `channel.subscribe` EventSub | the authorized account must **be** the broadcaster — `channel:read:subscriptions` can only be granted by the channel owner, so this fails for a channel you don't own |

So everything works out of the box for any `TWITCH_CHANNEL` except subs,
which only works when you're testing against your own channel. The script
logs a warning and keeps running with everything else if a subscription is
rejected.

### `main.py` output

Every poll interval (`TWITCH_POLL_INTERVAL_SECONDS`, default 60s) and on
stream offline, the script logs a `STATS {...}` JSON line with the full
aggregate for the current session — that's the shape to hand to
NiFi/Airflow once you're happy with it.

## `zevent_api.py`

No Twitch auth needed — `zevent.fr/api/` is a public, unauthenticated JSON
endpoint listing every registered streamer at once. Run: `uv run zevent_api.py`

Every poll interval (`ZEVENT_API_POLL_INTERVAL_SECONDS`, default 20s — the
API itself caches for ~15s, so don't go much below that) it logs a
`STATS {...}` JSON line with:

- `website_mode` — `"offline"` outside the event, `"live"` during it
- `total_donation_amount_eur`, `total_viewer_count` — event-wide totals
- `streamers[]` — per streamer: `twitch_id`, `twitch_login`, `display_name`,
  `online`, `game` (category, or `"Offline"`), `viewer_count`,
  `donation_amount_eur`

**Not available from this API:** a donation *count* (number of individual
donations) — only the cumulative euro amount is exposed, per streamer and
globally. `stats.zevent.fr` looked like it might have more detail but sits
behind an interactive Cloudflare bot-challenge, so it isn't scraped here.

## Not implemented

"Viewer typology" (e.g. lurker vs. chatter, new vs. returning) — Twitch's
API doesn't expose a full viewer list, only active chatters via a
moderator-only endpoint. Flag if you want this added on top of a moderator
token.
