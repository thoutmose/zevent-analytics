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
    streamers = [
        StreamerSnapshot(
            twitch_id=s["twitch_id"],
            twitch_login=s["twitch"],
            display_name=s["display"],
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
    async with session.get(API_URL, headers={"User-Agent": USER_AGENT}) as resp:
        resp.raise_for_status()
        data = await resp.json(content_type=None)
    return _parse_snapshot(data)


async def poll_forever() -> None:
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
            LOGGER.info("STATS %s", json.dumps(asdict(snapshot), ensure_ascii=False))

            await asyncio.sleep(POLL_INTERVAL_SECONDS)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(poll_forever())
    except KeyboardInterrupt:
        LOGGER.warning("Shutting down due to KeyboardInterrupt.")


if __name__ == "__main__":
    main()
