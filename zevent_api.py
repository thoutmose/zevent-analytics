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
from pathlib import Path
from typing import TypedDict

import aiohttp
from dotenv import load_dotenv

import logging_setup
import nifi_client
from http_config import USER_AGENT

_ = load_dotenv()

LOGGER: logging.Logger = logging.getLogger("zevent_api")

API_URL: str = "https://zevent.fr/api/"
POLL_INTERVAL_SECONDS: int = int(
    os.environ.get("ZEVENT_API_POLL_INTERVAL_SECONDS", "20")
)

# Unlike main.py, this poller has no Parquet dual-write — a snapshot only ever
# lives in memory before being pushed to NiFi. This checkpoint is the local
# fallback: the last successfully fetched snapshot, persisted so a restart (or
# a zevent.fr/api/ outage, which happened for ~17min during Zevent 2024) still
# leaves a recent, known-good snapshot on disk.
CHECKPOINT_PATH: Path = Path(
    os.environ.get("ZEVENT_CHECKPOINT_PATH", "data/zevent_checkpoint.json")
)


class _AmountDict(TypedDict):
    number: float


class RawStreamerDict(TypedDict):
    """One entry of the raw zevent.fr/api/ response's "live" array."""

    twitch_id: str
    twitch: str
    display: str
    profileUrl: str
    online: bool
    game: str
    viewersAmount: _AmountDict
    donationAmount: _AmountDict


class RawSnapshotDict(TypedDict):
    """
    The raw zevent.fr/api/ JSON response shape, before _parse_snapshot maps it.
    """

    websiteMode: str
    donationAmount: _AmountDict
    viewersCount: _AmountDict
    live: list[RawStreamerDict]


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


def _parse_snapshot(data: RawSnapshotDict) -> ZeventSnapshot:
    """Maps the raw zevent.fr/api/ JSON response onto ZeventSnapshot."""
    streamers = [
        StreamerSnapshot(
            twitch_id=s["twitch_id"],
            twitch_login=s["twitch"],
            display_name=s["display"],
            profile_url=s["profileUrl"],
            online=s["online"],
            game=s["game"],
            viewer_count=int(s["viewersAmount"]["number"]),
            donation_amount_eur=s["donationAmount"]["number"],
        )
        for s in data["live"]
    ]
    return ZeventSnapshot(
        website_mode=data["websiteMode"],
        total_donation_amount_eur=data["donationAmount"]["number"],
        total_viewer_count=int(data["viewersCount"]["number"]),
        streamers=streamers,
    )


async def fetch_snapshot(session: aiohttp.ClientSession) -> ZeventSnapshot:
    """Fetches and parses one snapshot from zevent.fr/api/."""
    async with session.get(API_URL, headers={"User-Agent": USER_AGENT}) as resp:
        resp.raise_for_status()
        data: RawSnapshotDict = await resp.json(content_type=None)
    return _parse_snapshot(data)


def _write_checkpoint(snapshot: ZeventSnapshot) -> None:
    """Persists `snapshot` to CHECKPOINT_PATH, overwriting the previous one.

    Written via a temp file + rename so a crash mid-write can't leave a
    truncated/corrupt checkpoint behind.
    """
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = CHECKPOINT_PATH.with_suffix(".json.tmp")
    _ = tmp_path.write_text(json.dumps(asdict(snapshot), ensure_ascii=False))
    _ = tmp_path.replace(CHECKPOINT_PATH)


async def poll_forever() -> None:
    """Polls zevent.fr/api/ forever, logging and pushing one snapshot per
    interval.

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

                try:
                    _write_checkpoint(snapshot)

                    online = [s for s in snapshot.streamers if s.online]
                    LOGGER.info(
                        "mode=%s total_donations=%s€ total_viewers=%s "
                        "online_streamers=%d/%d",
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
                except Exception:
                    # e.g. a full disk on the checkpoint write — don't let it end
                    # the whole poller for the rest of a 55-hour event.
                    LOGGER.exception("Failed to process/push snapshot")

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
