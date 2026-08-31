"""Fetches emote catalogs — native Twitch plus the three third-party services
every popular Twitch chat extension supports (7TV, BetterTTV, FrankerFaceZ) —
for every Zevent channel, into bronze_emote_catalog.

Why this exists as a separate poller rather than living in main.py: Twitch's
own IRC `emotes` tag (parsed by main.py's _parse_emotes into
bronze_live_chat.emotes) only covers native Twitch emotes. 7TV/BetterTTV/
FrankerFaceZ are client-side overlays Twitch's IRC/API has zero knowledge of —
a message using one is just plain text (e.g. "monkaS") with no tag at all.
Detecting those requires matching message_text tokens against each channel's
actual emote set, fetched from each service's own API — this module is that
fetch; the token-matching join against bronze_live_chat.message_text is a
dbt model, not something done here.

One row per (service, scope, channel, emote_id) in bronze_emote_catalog,
UPSERT'd — latest-state, not append-only, same as zevent_donation_goals.py
and for the same reason: none of the four sources below expose a "what
changed since X" endpoint, so a full re-fetch each poll is the only option,
and an emote removed from a set upstream is never deleted here (same
documented limitation as donation_goals — see README.md's "Known
limitations").

7TV/BetterTTV/FrankerFaceZ are unofficial services with no documented rate
limit — MAX_CONCURRENT_CATALOG_REQUESTS below is a "be a polite crawler"
bound, not a limit copied from a rate-limit doc (same caveat as
zevent_donation_goals.MAX_CONCURRENT_GOAL_REQUESTS). A 404 from any of the
three means the channel simply never linked that service — not an error;
most channels have zero rows from at least one of them.
"""

import asyncio
import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp
from dotenv import load_dotenv

import logging_setup
import nifi_client
import zevent_api
from http_config import USER_AGENT

_ = load_dotenv()

LOGGER: logging.Logger = logging.getLogger("emote_catalog")

TWITCH_CLIENT_ID: str = os.environ["TWITCH_CLIENT_ID"]
TWITCH_CLIENT_SECRET: str = os.environ["TWITCH_CLIENT_SECRET"]

# Emote sets are effectively static for the life of an event — nothing here
# needs main.py's ~15s cadence. Long default so a ~300-channel roster times
# ~4 API calls doesn't run back-to-back all day for no reason.
POLL_INTERVAL_SECONDS: int = int(
    os.environ.get("EMOTE_CATALOG_POLL_INTERVAL_SECONDS", "1800")
)
MAX_CONCURRENT_CATALOG_REQUESTS: int = int(
    os.environ.get("EMOTE_CATALOG_MAX_CONCURRENCY", "10")
)
CHECKPOINT_PATH: Path = Path(
    os.environ.get(
        "EMOTE_CATALOG_CHECKPOINT_PATH", "data/emote_catalog_checkpoint.json"
    )
)

HELIX_BATCH_SIZE: int = 100

# Sentinel for scope="global" rows in bronze_emote_catalog.channel — must stay
# NOT NULL there for the UPSERT's ON CONFLICT to work (see 005_emote_catalog.sql).
GLOBAL_CHANNEL: str = "__global__"


def _chunked(items: list[str], size: int) -> list[list[str]]:
    """Splits items into consecutive slices of at most `size` elements each."""
    return [items[i : i + size] for i in range(0, len(items), size)]


@dataclass
class EmoteCatalogEntry:
    service: str  # "twitch" | "7tv" | "bttv" | "ffz"
    scope: str  # "global" | "channel"
    channel: str  # twitch login, or GLOBAL_CHANNEL for scope="global"
    emote_id: str
    emote_code: str
    fetched_at: str


async def _fetch_roster() -> list[str]:
    """Fetches the current Zevent streamer roster (Twitch logins), same
    source as main.py's own _fetch_roster."""
    async with aiohttp.ClientSession() as session:
        snapshot = await zevent_api.fetch_snapshot(session)
    return [s.twitch_login for s in snapshot.streamers]


async def _get_twitch_app_token(session: aiohttp.ClientSession) -> str:
    """Client Credentials Grant — same auth style as main.py, but obtained
    directly here since this is a separate process with no twitchio Bot to
    borrow a token from."""
    async with session.post(
        "https://id.twitch.tv/oauth2/token",
        data={
            "client_id": TWITCH_CLIENT_ID,
            "client_secret": TWITCH_CLIENT_SECRET,
            "grant_type": "client_credentials",
        },
    ) as resp:
        resp.raise_for_status()
        body = await resp.json()
        return body["access_token"]


async def _resolve_broadcaster_ids(
    session: aiohttp.ClientSession, token: str, logins: list[str]
) -> dict[str, str]:
    """Batched Get Users login->id resolution (100/call, same limit as
    main.py's HELIX_BATCH_SIZE) — 7TV/BetterTTV key by broadcaster id, not
    login."""
    ids: dict[str, str] = {}
    headers = {"Client-Id": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"}
    for chunk in _chunked(logins, HELIX_BATCH_SIZE):
        async with session.get(
            "https://api.twitch.tv/helix/users",
            params=[("login", login) for login in chunk],
            headers=headers,
        ) as resp:
            resp.raise_for_status()
            body = await resp.json()
        for user in body["data"]:
            ids[user["login"].lower()] = user["id"]
    return ids


def _parse_twitch_emotes(
    data: list[dict[str, Any]], scope: str, channel: str, fetched_at: str
) -> list[EmoteCatalogEntry]:
    return [
        EmoteCatalogEntry(
            "twitch", scope, channel, str(e["id"]), str(e["name"]), fetched_at
        )
        for e in data
    ]


def _parse_7tv_emotes(
    emotes: list[dict[str, Any]], scope: str, channel: str, fetched_at: str
) -> list[EmoteCatalogEntry]:
    return [
        EmoteCatalogEntry(
            "7tv", scope, channel, str(e["id"]), str(e["name"]), fetched_at
        )
        for e in emotes
    ]


def _parse_bttv_emotes(
    emotes: list[dict[str, Any]], scope: str, channel: str, fetched_at: str
) -> list[EmoteCatalogEntry]:
    return [
        EmoteCatalogEntry(
            "bttv", scope, channel, str(e["id"]), str(e["code"]), fetched_at
        )
        for e in emotes
    ]


def _parse_ffz_emotes(
    emotes: list[dict[str, Any]], scope: str, channel: str, fetched_at: str
) -> list[EmoteCatalogEntry]:
    return [
        EmoteCatalogEntry(
            "ffz", scope, channel, str(e["id"]), str(e["name"]), fetched_at
        )
        for e in emotes
    ]


def _flatten_ffz_sets(body: dict[str, Any], set_ids: list[Any]) -> list[dict[str, Any]]:
    """FFZ nests emotes under sets[set_id]['emoticons'] rather than a flat
    list — this walks the given set ids (default_sets for the global
    endpoint, all of `sets` for a room) into one flat list."""
    sets = body.get("sets", {})
    return [
        emote
        for set_id in set_ids
        for emote in sets.get(str(set_id), {}).get("emoticons", [])
    ]


async def fetch_twitch_global_emotes(
    session: aiohttp.ClientSession, token: str, fetched_at: str
) -> list[EmoteCatalogEntry]:
    async with session.get(
        "https://api.twitch.tv/helix/chat/emotes/global",
        headers={"Client-Id": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"},
    ) as resp:
        resp.raise_for_status()
        body = await resp.json()
    return _parse_twitch_emotes(body["data"], "global", GLOBAL_CHANNEL, fetched_at)


async def fetch_twitch_channel_emotes(
    session: aiohttp.ClientSession,
    token: str,
    semaphore: asyncio.Semaphore,
    channel: str,
    broadcaster_id: str,
    fetched_at: str,
) -> list[EmoteCatalogEntry]:
    async with semaphore:
        async with session.get(
            "https://api.twitch.tv/helix/chat/emotes",
            params={"broadcaster_id": broadcaster_id},
            headers={"Client-Id": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"},
        ) as resp:
            resp.raise_for_status()
            body = await resp.json()
    return _parse_twitch_emotes(body["data"], "channel", channel, fetched_at)


async def fetch_7tv_global_emotes(
    session: aiohttp.ClientSession, fetched_at: str
) -> list[EmoteCatalogEntry]:
    async with session.get("https://7tv.io/v3/emote-sets/global") as resp:
        resp.raise_for_status()
        body = await resp.json()
    return _parse_7tv_emotes(
        body.get("emotes", []), "global", GLOBAL_CHANNEL, fetched_at
    )


async def fetch_7tv_channel_emotes(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    channel: str,
    broadcaster_id: str,
    fetched_at: str,
) -> list[EmoteCatalogEntry]:
    async with semaphore:
        async with session.get(
            f"https://7tv.io/v3/users/twitch/{broadcaster_id}"
        ) as resp:
            if resp.status == 404:
                return []
            resp.raise_for_status()
            body = await resp.json()
    emotes = (body.get("emote_set") or {}).get("emotes") or []
    return _parse_7tv_emotes(emotes, "channel", channel, fetched_at)


async def fetch_bttv_global_emotes(
    session: aiohttp.ClientSession, fetched_at: str
) -> list[EmoteCatalogEntry]:
    async with session.get("https://api.betterttv.net/3/cached/emotes/global") as resp:
        resp.raise_for_status()
        body = await resp.json()
    return _parse_bttv_emotes(body, "global", GLOBAL_CHANNEL, fetched_at)


async def fetch_bttv_channel_emotes(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    channel: str,
    broadcaster_id: str,
    fetched_at: str,
) -> list[EmoteCatalogEntry]:
    async with semaphore:
        async with session.get(
            f"https://api.betterttv.net/3/cached/users/twitch/{broadcaster_id}"
        ) as resp:
            if resp.status == 404:
                return []
            resp.raise_for_status()
            body = await resp.json()
    entries = body.get("channelEmotes", []) + body.get("sharedEmotes", [])
    return _parse_bttv_emotes(entries, "channel", channel, fetched_at)


async def fetch_ffz_global_emotes(
    session: aiohttp.ClientSession, fetched_at: str
) -> list[EmoteCatalogEntry]:
    async with session.get("https://api.frankerfacez.com/v1/set/global") as resp:
        resp.raise_for_status()
        body = await resp.json()
    entries = _flatten_ffz_sets(body, body.get("default_sets", []))
    return _parse_ffz_emotes(entries, "global", GLOBAL_CHANNEL, fetched_at)


async def fetch_ffz_channel_emotes(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    channel: str,
    fetched_at: str,
) -> list[EmoteCatalogEntry]:
    # FFZ's room endpoint keys by Twitch *login*, unlike 7TV/BetterTTV which
    # key by broadcaster id.
    async with semaphore:
        async with session.get(
            f"https://api.frankerfacez.com/v1/room/{channel}"
        ) as resp:
            if resp.status == 404:
                return []
            resp.raise_for_status()
            body = await resp.json()
    entries = _flatten_ffz_sets(body, list(body.get("sets", {}).keys()))
    return _parse_ffz_emotes(entries, "channel", channel, fetched_at)


async def _fetch_channel_catalog(
    session: aiohttp.ClientSession,
    token: str,
    semaphore: asyncio.Semaphore,
    channel: str,
    broadcaster_id: str,
    fetched_at: str,
) -> list[EmoteCatalogEntry]:
    """Fetches all four services for one channel; one service failing (a
    transient error, not a 404 — those are handled per-fetcher above) doesn't
    lose the other three."""
    results = await asyncio.gather(
        fetch_twitch_channel_emotes(
            session, token, semaphore, channel, broadcaster_id, fetched_at
        ),
        fetch_7tv_channel_emotes(
            session, semaphore, channel, broadcaster_id, fetched_at
        ),
        fetch_bttv_channel_emotes(
            session, semaphore, channel, broadcaster_id, fetched_at
        ),
        fetch_ffz_channel_emotes(session, semaphore, channel, fetched_at),
        return_exceptions=True,
    )
    entries: list[EmoteCatalogEntry] = []
    for result in results:
        if isinstance(result, BaseException):
            LOGGER.warning(
                "Emote catalog fetch failed for channel=%s: %r", channel, result
            )
            continue
        entries.extend(result)
    return entries


async def fetch_all_emote_catalogs(
    session: aiohttp.ClientSession,
) -> list[EmoteCatalogEntry]:
    """Global sets (once) + every channel's own sets (concurrency-bounded)."""
    token = await _get_twitch_app_token(session)
    roster = await _fetch_roster()
    fetched_at = datetime.now(UTC).isoformat(timespec="microseconds")

    broadcaster_ids = await _resolve_broadcaster_ids(session, token, roster)
    missing = [c for c in roster if c.lower() not in broadcaster_ids]
    if missing:
        LOGGER.warning(
            "%d channel(s) not found on Twitch, skipping emote catalog: %s",
            len(missing),
            missing,
        )

    global_groups = await asyncio.gather(
        fetch_twitch_global_emotes(session, token, fetched_at),
        fetch_7tv_global_emotes(session, fetched_at),
        fetch_bttv_global_emotes(session, fetched_at),
        fetch_ffz_global_emotes(session, fetched_at),
    )
    entries: list[EmoteCatalogEntry] = [e for group in global_groups for e in group]

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_CATALOG_REQUESTS)
    channel_groups = await asyncio.gather(
        *(
            _fetch_channel_catalog(
                session,
                token,
                semaphore,
                channel,
                broadcaster_ids[channel.lower()],
                fetched_at,
            )
            for channel in roster
            if channel.lower() in broadcaster_ids
        )
    )
    for group in channel_groups:
        entries.extend(group)

    return entries


def _write_checkpoint(entries: list[EmoteCatalogEntry]) -> None:
    """Same temp-file-then-rename pattern as zevent_api.py/
    zevent_donation_goals.py, so a crash mid-write can't corrupt the
    checkpoint."""
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = CHECKPOINT_PATH.with_suffix(".json.tmp")
    _ = tmp_path.write_text(
        json.dumps([asdict(e) for e in entries], ensure_ascii=False)
    )
    _ = tmp_path.replace(CHECKPOINT_PATH)


async def poll_forever() -> None:
    """Polls all four emote sources forever, logging and pushing one
    snapshot per interval.

    On exit (including Ctrl+C), waits for any NiFi push still in flight
    before returning — see nifi_client.wait_for_pending_pushes.
    """
    try:
        async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as session:
            while True:
                try:
                    entries = await fetch_all_emote_catalogs(session)
                except aiohttp.ClientError:
                    LOGGER.exception("Failed to fetch emote catalogs")
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    continue

                _write_checkpoint(entries)

                by_service = {
                    service: sum(1 for e in entries if e.service == service)
                    for service in ("twitch", "7tv", "bttv", "ffz")
                }
                LOGGER.info(
                    "[emote_catalog] %d emote(s) total (%s)",
                    len(entries),
                    ", ".join(f"{k}={v}" for k, v in by_service.items()),
                )

                # NiFi's PutDatabaseRecord for this stream must use Statement
                # Type = UPSERT (Update Keys = service, scope, channel,
                # emote_id) — see sql/005_emote_catalog.sql — so a re-poll
                # overwrites each row in place instead of appending a
                # duplicate.
                nifi_client.push_batch_background(
                    "emote_catalog.py", "emote_catalog", [asdict(e) for e in entries]
                )

                await asyncio.sleep(POLL_INTERVAL_SECONDS)
    finally:
        await nifi_client.wait_for_pending_pushes()


def main() -> None:
    """Entry point: runs poll_forever() until interrupted."""
    logging_setup.setup_logging()
    try:
        asyncio.run(poll_forever())
    except KeyboardInterrupt:
        LOGGER.warning("Shutting down due to KeyboardInterrupt.")


if __name__ == "__main__":
    main()
