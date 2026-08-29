"""Zevent donation goals poller: https://zevent.gdoc.fr/participations

The page itself is a client-rendered Nuxt SPA with no server-rendered HTML to
scrape (see git history/PR description for how this was found) — it calls a
JSON backend at api.ppr.evenmorestats.fr, reverse-engineered from that SPA's
JS bundle. This module calls that backend directly instead, the same way
zevent_api.py calls zevent.fr/api/ directly instead of scraping zevent.fr.

Two endpoints, in order:
  - GET /events                                       -> resolve the current
    Zevent's event id (see _pick_latest_zevent) instead of hardcoding one,
    since it changes every year.
  - GET /events/{event_id}/donation_goals/overview     -> one entry per
    streamer/participation, with a donation_goals_count telling us whether
    it's worth fetching goal detail for that one.
  - GET /participations/{participation_id}/donation_goals -> that streamer's
    full goal list, fetched only for participations with donation_goals_count
    > 0, bounded by MAX_CONCURRENT_GOAL_REQUESTS so a ~300-streamer roster
    doesn't fire 300 simultaneous requests at a third-party API.

Each goal also carries an "accomplished" (completion) flag upstream, which is
intentionally dropped here — this module tracks what the goals *are*, not
how close to met they are; "amount_raised" (in bronze_zevent_snapshots,
zevent_api.py) already covers progress.
"""

import asyncio
import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict

import aiohttp
from dotenv import load_dotenv

import logging_setup
import nifi_client
from http_config import USER_AGENT

_ = load_dotenv()

LOGGER: logging.Logger = logging.getLogger("zevent_donation_goals")

API_BASE: str = "https://api.ppr.evenmorestats.fr"

POLL_INTERVAL_SECONDS: int = int(
    os.environ.get("DONATION_GOALS_POLL_INTERVAL_SECONDS", "300")
)
# Bounds how many /participations/{id}/donation_goals requests run at once —
# there's no documented rate limit for this API, so this is a "be a polite
# crawler" default, not a limit copied from a rate-limit doc (contrast with
# Twitch's documented limits in main.py).
MAX_CONCURRENT_GOAL_REQUESTS: int = int(
    os.environ.get("DONATION_GOALS_MAX_CONCURRENCY", "5")
)

# Same "last known good state on disk" role as zevent_api.CHECKPOINT_PATH:
# survives a restart or an API outage with a recent snapshot to fall back on.
CHECKPOINT_PATH: Path = Path(
    os.environ.get(
        "DONATION_GOALS_CHECKPOINT_PATH", "data/donation_goals_checkpoint.json"
    )
)


class ScheduleDict(TypedDict):
    start: str
    end: str


class EventDict(TypedDict):
    """One row of GET /events — most fields unused here, kept for documentation."""

    id: str
    name: str
    schedule: ScheduleDict


class TwitchSocialDict(TypedDict):
    id: str
    login: str


class SocialsDict(TypedDict, total=False):
    twitch: TwitchSocialDict


class ParticipationOverviewDict(TypedDict):
    """One row of GET /events/{id}/donation_goals/overview."""

    id: str  # this is the participation_id, despite the field name
    name: str
    donation_goals_count: int
    socials: SocialsDict


class DonationGoalDict(TypedDict):
    """One row of GET /participations/{id}/donation_goals.

    `accomplished` (whether the goal's been met) is deliberately never read
    — see the module docstring.
    """

    id: str
    name: str
    amount: int
    category: str
    accomplished: bool
    participation_id: str


@dataclass
class DonationGoal:
    participation_id: str
    streamer_name: str
    twitch_login: str | None
    twitch_id: str | None
    goal_id: str
    goal_name: str
    goal_amount_eur: float
    goal_category: str | None
    snapshot_at: str


async def _fetch_json(session: aiohttp.ClientSession, path: str) -> Any:
    """GETs `path` off API_BASE and returns the parsed JSON body.

    Returns `Any` because that's genuinely as far as this function alone can
    know the shape — every caller immediately assigns the result to a
    variable annotated with the real expected type (list[EventDict], etc.),
    which is what actually stops `Any` from propagating into the rest of the
    module (see e.g. fetch_all_donation_goals below).
    """
    async with session.get(
        f"{API_BASE}{path}", headers={"User-Agent": USER_AGENT}
    ) as resp:
        resp.raise_for_status()
        return await resp.json()


def _pick_latest_zevent(events: list[EventDict]) -> EventDict:
    """Picks the most recently-scheduled event named "ZEvent ...".

    The evenmorestats.fr backend tracks every event it's ever covered (going
    back to ZEvent 2017, plus unrelated events like "Birds Of Prey"), not just
    the current one — filtering by name and taking the latest schedule.start
    avoids hardcoding an event id that changes every year, the same idea as
    main.py fetching its channel roster instead of hardcoding it.
    """
    zevents = [e for e in events if e["name"].startswith("ZEvent")]
    if not zevents:
        raise RuntimeError("No event named 'ZEvent *' found in /events response")
    return max(zevents, key=lambda e: e["schedule"]["start"])


def _build_row(
    participation: ParticipationOverviewDict, goal: DonationGoalDict, snapshot_at: str
) -> DonationGoal:
    """Maps one (participation, goal) pair from the API onto a DonationGoal row."""
    twitch = participation.get("socials", {}).get("twitch")
    return DonationGoal(
        participation_id=participation["id"],
        streamer_name=participation["name"],
        twitch_login=twitch["login"] if twitch else None,
        twitch_id=twitch["id"] if twitch else None,
        goal_id=goal["id"],
        goal_name=goal["name"],
        goal_amount_eur=goal["amount"] / 100,
        goal_category=goal.get("category"),
        snapshot_at=snapshot_at,
    )


async def _fetch_goals_for_participation(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    participation_id: str,
) -> list[DonationGoalDict]:
    async with semaphore:
        goals: list[DonationGoalDict] = await _fetch_json(
            session, f"/participations/{participation_id}/donation_goals"
        )
        return goals


async def fetch_all_donation_goals(
    session: aiohttp.ClientSession,
) -> list[DonationGoal]:
    """Fetches one row per (streamer, donation goal) pair for the current Zevent.

    Only participations with at least one configured goal trigger a detail
    request — most of a ~300-streamer roster has none until close to the
    event (observed: 11/319 a week out from Zevent 2026).
    """
    events: list[EventDict] = await _fetch_json(session, "/events")
    event = _pick_latest_zevent(events)

    overview: list[ParticipationOverviewDict] = await _fetch_json(
        session, f"/events/{event['id']}/donation_goals/overview"
    )
    with_goals = [p for p in overview if p["donation_goals_count"] > 0]
    LOGGER.info(
        "[donation_goals] %d/%d participations have goals configured (event=%s)",
        len(with_goals),
        len(overview),
        event["name"],
    )

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_GOAL_REQUESTS)
    goal_lists = await asyncio.gather(
        *(
            _fetch_goals_for_participation(session, semaphore, p["id"])
            for p in with_goals
        )
    )

    snapshot_at = datetime.now(UTC).isoformat(timespec="microseconds")
    return [
        _build_row(participation, goal, snapshot_at)
        for participation, goals in zip(with_goals, goal_lists)
        for goal in goals
    ]


def _write_checkpoint(rows: list[DonationGoal]) -> None:
    """Persists `rows` to CHECKPOINT_PATH, overwriting the previous checkpoint.

    Written via a temp file + rename so a crash mid-write can't leave a
    truncated/corrupt checkpoint behind (same approach as zevent_api.py's
    _write_checkpoint).
    """
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = CHECKPOINT_PATH.with_suffix(".json.tmp")
    _ = tmp_path.write_text(
        json.dumps([asdict(row) for row in rows], ensure_ascii=False)
    )
    _ = tmp_path.replace(CHECKPOINT_PATH)


async def poll_forever() -> None:
    """Polls the donation-goals API forever, logging and pushing one snapshot
    per interval.

    On exit (including Ctrl+C), waits for any NiFi push still in flight before
    returning — see nifi_client.wait_for_pending_pushes.
    """
    try:
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    rows = await fetch_all_donation_goals(session)
                except aiohttp.ClientError:
                    LOGGER.exception("Failed to fetch donation goals from %s", API_BASE)
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    continue

                _write_checkpoint(rows)

                streamer_count = len({row.participation_id for row in rows})
                LOGGER.info(
                    "[donation_goals] %d goal(s) across %d streamer(s)",
                    len(rows),
                    streamer_count,
                )

                # NiFi's PutDatabaseRecord for this stream must use Statement
                # Type = UPSERT (Update Keys = participation_id, goal_id) —
                # see sql/002_donation_goals.sql — so a re-poll overwrites
                # each goal in place instead of appending a duplicate.
                nifi_client.push_batch_background(
                    "zevent_donation_goals.py",
                    "donation_goals",
                    [asdict(row) for row in rows],
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
