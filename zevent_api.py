"""Zevent official API poller: https://zevent.fr/api/

Aggregates, across every streamer participating in Zevent: online/offline
status, viewer count, and donation amount in euros. No auth needed - it's
a public, unauthenticated JSON endpoint, refreshed server-side every ~15s.

Note: this endpoint exposes total donation *amount* per streamer and for
the whole event, but not a donation *count* - that field doesn't exist
here (nor anywhere else public; stats.zevent.fr has more detail but sits
behind an interactive bot-challenge that isn't something to script around).
"""

import asyncio
import json
import logging
import os
from dataclasses import asdict, dataclass

import aiohttp
from dotenv import load_dotenv

import logging_setup
import nifi_client

load_dotenv()

LOGGER = logging.getLogger("zevent_api")

API_URL = "https://zevent.fr/api/"
POLL_INTERVAL_SECONDS = int(os.environ.get("ZEVENT_API_POLL_INTERVAL_SECONDS", "20"))
USER_AGENT = "twitch-analytics/0.1 (https://github.com/thoutmose/twitch-analytics)"


@dataclass
class StreamerSnapshot:
    twitch_id: str
    twitch_login: str
    display_name: str
    profile_url: str
    online: bool
    game: str
    viewer_count: int
    donation_amount_eur: float


@dataclass
class ZeventSnapshot:
    website_mode: str
    total_donation_amount_eur: float
    total_viewer_count: int
    streamers: list[StreamerSnapshot]


def _parse_snapshot(data: dict) -> ZeventSnapshot:
    """Maps the raw zevent.fr/api/ JSON response onto ZeventSnapshot."""
    streamers = [
        StreamerSnapshot(
            twitch_id=s["twitch_id"],
            twitch_login=s["twitch"],
            display_name=s["display"],
            profile_url=s["profileUrl"],
            online=s["online"],
            game=s["game"],
            viewer_count=s["viewersAmount"]["number"],
            donation_amount_eur=s["donationAmount"]["number"],
        )
        for s in data["live"]
    ]
    return ZeventSnapshot(
        website_mode=data["websiteMode"],
        total_donation_amount_eur=data["donationAmount"]["number"],
        total_viewer_count=data["viewersCount"]["number"],
        streamers=streamers,
    )


async def fetch_snapshot(session: aiohttp.ClientSession) -> ZeventSnapshot:
    """Fetches and parses one snapshot from zevent.fr/api/."""
    async with session.get(API_URL, headers={"User-Agent": USER_AGENT}) as resp:
        resp.raise_for_status()
        data = await resp.json(content_type=None)
    return _parse_snapshot(data)


async def poll_forever() -> None:
    """Polls zevent.fr/api/ forever, logging and pushing one snapshot per interval.

    On exit (including Ctrl+C), waits for any NiFi push still in flight before
    returning, so a slow request isn't cancelled mid-send — see
    nifi_client.wait_for_pending_pushes.
    """
    try:
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    snapshot = await fetch_snapshot(session)
                except aiohttp.ClientError:
                    LOGGER.exception("Failed to fetch %s", API_URL)
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    continue

                online = [s for s in snapshot.streamers if s.online]
                LOGGER.info(
                    "mode=%s total_donations=%s€ total_viewers=%s online_streamers=%d/%d",
                    snapshot.website_mode,
                    snapshot.total_donation_amount_eur,
                    snapshot.total_viewer_count,
                    len(online),
                    len(snapshot.streamers),
                )
                LOGGER.info(
                    "STATS %s", json.dumps(asdict(snapshot), ensure_ascii=False)
                )

                # "streamers" is stringified to JSON text before pushing: NiFi's
                # PutDatabaseRecord can't write a nested array-of-records field
                # straight into a jsonb column (ClassCastException: MapRecord
                # cannot be cast to Byte) — it needs a plain string it can hand
                # to the JDBC driver, which `stringtype=unspecified` on the
                # connection then lets Postgres coerce into jsonb.
                row = asdict(snapshot)
                row["streamers"] = json.dumps(row["streamers"], ensure_ascii=False)
                nifi_client.push_batch_background(
                    "zevent_api.py", "zevent_snapshot", [row]
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
