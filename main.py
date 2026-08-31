"""Zevent multi-channel extractor: aggregates live-stream data for every Twitch
channel currently participating in Zevent.

The channel roster is fetched from zevent.fr/api/ (see zevent_api.py) rather
than hardcoded, since it's ~300+ channels and changes from one edition to
the next. It's fetched at startup, then re-fetched every
ROSTER_REFRESH_INTERVAL_SECONDS so a channel that joins mid-event is picked
up without a restart (see ZeventBot._roster_refresh_loop).

Collects, per channel:
  - stream metadata: title, category, start time, viewer_count (polled from
    Helix, batched 100 logins/request to stay under Twitch's per-call limit)
  - chat message count, and message count per chatter (via anonymous Twitch
    IRC, sharded across multiple connections — see ChatConnection)

EventSub (stream.online/offline, channel.update, channel.raid) is
intentionally not used here: at 300+ channels, subscribing to 3-4 events per
channel risks hitting per-websocket subscription limits, and everything it
would tell us (live status, title/category, raids aside) is already covered
by the Helix poll. Raids are not tracked as a result.

Lands two parquet streams under PARQUET_OUTPUT_DIR (default "data/"),
zstd-compressed:
  - data/live_chat/  — one row per chat message, any channel
  - data/metadata/   — one row per channel per METADATA_SNAPSHOT_INTERVAL_SECONDS
Each stream rolls over to a new file every MAX_ROWS_PER_PARQUET_FILE rows
(default 100,000), with a zero-padded part number in the filename. Buffered
rows are also flushed to disk every FLUSH_INTERVAL_SECONDS (default 30s), and
on a clean shutdown (Ctrl+C or SIGTERM) — which also waits for any NiFi push
still in flight (see nifi_client.wait_for_pending_pushes) so a slow request
isn't cancelled mid-send.
"""

import asyncio
import json
import logging
import os
import random
import re
import signal
import time
from collections.abc import Coroutine
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, override

import aiohttp
import pyarrow as pa
import pyarrow.parquet as pq
import twitchio
from dotenv import load_dotenv
from twitchio.ext import commands

import logging_setup
import nifi_client
import zevent_api

_ = load_dotenv()

LOGGER: logging.Logger = logging.getLogger("zevent_extractor")

CLIENT_ID: str = os.environ["TWITCH_CLIENT_ID"]
CLIENT_SECRET: str = os.environ["TWITCH_CLIENT_SECRET"]
METADATA_SNAPSHOT_INTERVAL_SECONDS: int = int(
    os.environ.get("METADATA_SNAPSHOT_INTERVAL_SECONDS", "15")
)
# The roster otherwise only comes from zevent.fr/api/ once, at startup (see
# module docstring) — this re-fetches it periodically so a channel added
# mid-event gets picked up without a restart. Matched to 15s (rather than
# something coarser) because that's the endpoint's own server-side refresh
# cadence (see zevent_api.py) — polling faster would just re-read the same
# data, and it costs one unauthenticated GET, not a Helix call, unless a
# genuinely new channel is found.
ROSTER_REFRESH_INTERVAL_SECONDS: int = int(
    os.environ.get("ROSTER_REFRESH_INTERVAL_SECONDS", "15")
)
# How often unresolved chatter_ids are looked up via Helix Get Users (batched,
# HELIX_BATCH_SIZE per call) to fill in account_created_at/broadcaster_type.
# These don't change message to message like the IRC-tag-derived badges/
# user_type do, so they're cached in memory per chatter_id rather than
# looked up on every message — see ZeventBot._chatter_lookup_loop.
CHATTER_LOOKUP_INTERVAL_SECONDS: int = int(
    os.environ.get("CHATTER_LOOKUP_INTERVAL_SECONDS", "30")
)

# Twitch caps both "get streams" and "get users" at 100 login/id values per call.
HELIX_BATCH_SIZE: int = 100
# How many channels one anonymous IRC connection joins. Sharding across several
# connections bounds the blast radius of a single dropped connection and keeps
# each connection's JOIN burst short.
CHANNELS_PER_IRC_CONNECTION: int = int(
    os.environ.get("CHANNELS_PER_IRC_CONNECTION", "50")
)
# Twitch rate-limits unverified connections to ~20 JOIN/PART per 10s.
IRC_JOIN_PACING_SECONDS: float = float(os.environ.get("IRC_JOIN_PACING_SECONDS", "0.5"))

IRC_WS_URL: str = "wss://irc-ws.chat.twitch.tv:443"
PRIVMSG_RE: re.Pattern[str] = re.compile(
    r"^(?:@(?P<tags>\S+) )?:(?P<nick>[^!]+)!\S+ PRIVMSG #(?P<channel>\S+) "
    r":(?P<text>.*)$"
)

PARQUET_OUTPUT_DIR: Path = Path(os.environ.get("PARQUET_OUTPUT_DIR", "data"))
CHAT_DIR: Path = PARQUET_OUTPUT_DIR / "live_chat"
METADATA_DIR: Path = PARQUET_OUTPUT_DIR / "metadata"
MAX_ROWS_PER_PARQUET_FILE: int = int(
    os.environ.get("MAX_ROWS_PER_PARQUET_FILE", "100000")
)
FLUSH_INTERVAL_SECONDS: int = int(os.environ.get("FLUSH_INTERVAL_SECONDS", "30"))
PARQUET_COMPRESSION: str = os.environ.get("PARQUET_COMPRESSION", "zstd")


def _chunked(items: list[str], size: int) -> list[list[str]]:
    """Splits items into consecutive slices of at most `size` elements each."""
    return [items[i : i + size] for i in range(0, len(items), size)]


def _parse_badges(raw: str | None) -> dict[str, str]:
    """Parses an IRC `badges` tag ("moderator/1,subscriber/12") into a dict.

    Twitch encodes a chatter's roles/status (broadcaster, staff, admin,
    moderator, vip, artist, founder, subscriber, partner, turbo, premium,
    no_audio, no_video, ...) as this one comma-separated tag rather than
    fixed IRC fields — parsed generically here, not as one flag per known
    badge, so a badge Twitch adds or renames later shows up automatically
    instead of needing a schema change.
    """
    if not raw:
        return {}
    return dict(pair.split("/", 1) for pair in raw.split(",") if "/" in pair)


def _parse_emotes(raw: str | None) -> dict[str, int]:
    """Parses an IRC `emotes` tag ("25:0-4,12-16/1902:6-10") into an
    {emote_id: occurrence_count} dict.

    Only covers native Twitch emotes — Twitch's own tag is the only emote
    signal IRC carries. Third-party emotes (7TV/BTTV/FFZ) are plain text in
    message_text with no tag at all; matching those against a fetched
    catalog is a dbt-side join (see emote_catalog.py), not something this
    parser can see.
    """
    if not raw:
        return {}
    counts: dict[str, int] = {}
    for entry in raw.split("/"):
        emote_id, _, positions = entry.partition(":")
        if not positions:
            continue
        occurrences = len([p for p in positions.split(",") if p])
        if occurrences:
            counts[emote_id] = occurrences
    return counts


class BatchParquetWriter:
    """Buffers rows in memory and rolls over to a new parquet file every max_rows rows.

    The filename carries the batch's start time plus a zero-padded part number, so files
    stay identifiable and sortable even though several share the same suffix.
    """

    def __init__(
        self, directory: Path, suffix: str, max_rows: int = MAX_ROWS_PER_PARQUET_FILE
    ) -> None:
        self.directory: Path = directory
        self.suffix: str = suffix
        self.max_rows: int = max_rows
        self.part: int = 1
        self.rows: list[dict[str, Any]] = []
        self.batch_started_at: datetime = datetime.now(UTC)

    def add(self, row: dict[str, Any]) -> None:
        """Buffers one row, flushing immediately if max_rows is reached."""
        self.rows.append(row)
        if len(self.rows) >= self.max_rows:
            self.flush()

    def flush(self) -> None:
        """Writes buffered rows to a new parquet file and pushes them to NiFi.

        No-op if the buffer is empty (e.g. the periodic flush loop firing with
        nothing new to write).
        """
        if not self.rows:
            return
        rows = self.rows
        timestamp = self.batch_started_at.strftime("%Y%m%dT%H%M%S%f")
        path = (
            self.directory
            / f"zevent_{timestamp}Z_{self.suffix}_part{self.part:05d}.parquet"
        )
        pq.write_table(
            pa.Table.from_pylist(rows), path, compression=PARQUET_COMPRESSION
        )
        LOGGER.info(
            "[parquet] wrote %s (%d rows, %s-compressed)",
            path.name,
            len(rows),
            PARQUET_COMPRESSION,
        )
        nifi_client.push_batch_background("main.py", self.suffix, rows)
        self.rows = []
        self.part += 1
        self.batch_started_at = datetime.now(UTC)


@dataclass
class ChannelStats:
    """Running state for one Twitch channel, refreshed on every Helix poll."""

    channel: str
    broadcaster_id: str
    is_live: bool = False
    title: str | None = None
    category: str | None = None
    viewer_count: int | None = None
    stream_started_at: datetime | None = None
    stream_ended_at: str | None = None
    chat_message_count: int = 0
    messages_per_chatter: dict[str, int] = field(default_factory=dict)


@dataclass
class ChatterInfo:
    """Twitch account facts for one chatter_id, resolved once via Helix Get
    Users and reused for every subsequent message from that chatter (see
    ZeventBot.chatter_cache) — unlike badges/user_type, these don't change
    message to message.
    """

    created_at: datetime
    broadcaster_type: str


class ChatConnection:
    """One anonymous IRC connection, joined to a shard of channels.

    Reconnects with backoff on drop so one flaky connection doesn't kill chat
    capture for every channel — only for the ~50 channels in its shard.
    """

    def __init__(self, channels: list[str], bot: "ZeventBot") -> None:
        self.channels: list[str] = channels
        self.bot: ZeventBot = bot

    async def run(self) -> None:
        """Connects, and reconnects with jittered exponential backoff (capped
        at 60s) on drop.

        The jitter (sleep somewhere in [backoff, 2*backoff], capped at 60s,
        instead of exactly backoff) matters at this shard count: a shared
        outage would otherwise have every one of the ~7 shards reconnect in
        lockstep and hit Twitch at the same instant.
        """
        backoff = 5
        while True:
            try:
                await self._connect_once()
                # a clean iteration (should only end via cancellation) resets backoff
                backoff = 5
            except Exception:
                # Broad on purpose: anything from here (a network drop, but also
                # e.g. a parquet write failure inside on_chat_message -> add() ->
                # flush()) should reconnect and keep going rather than silently
                # end chat capture for this shard's ~50 channels for the rest of
                # a 55-hour event.
                sleep_seconds = min(backoff + random.uniform(0, backoff), 60)
                LOGGER.exception(
                    "IRC connection dropped (%d channels), reconnecting in %.1fs",
                    len(self.channels),
                    sleep_seconds,
                )
                await asyncio.sleep(sleep_seconds)
                backoff = min(backoff * 2, 60)

    async def _connect_once(self) -> None:
        """Opens one IRC websocket, joins every channel in this shard, and
        reads chat forever."""
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(IRC_WS_URL) as ws:
                await ws.send_str("CAP REQ :twitch.tv/tags")
                await ws.send_str("PASS SCHMOOPIIE")
                await ws.send_str(f"NICK justinfan{random.randint(10000, 99999)}")
                for channel in self.channels:
                    await ws.send_str(f"JOIN #{channel}")
                    await asyncio.sleep(IRC_JOIN_PACING_SECONDS)
                LOGGER.info(
                    "[irc] joined %d channels on one connection", len(self.channels)
                )

                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    for line in msg.data.strip("\r\n").split("\r\n"):
                        if line.startswith("PING"):
                            await ws.send_str("PONG :tmi.twitch.tv")
                            continue
                        match = PRIVMSG_RE.match(line)
                        if not match:
                            continue
                        tags = dict(
                            pair.split("=", 1)
                            for pair in (match["tags"] or "").split(";")
                            if "=" in pair
                        )
                        chatter = tags.get("display-name") or match["nick"]
                        self.bot.on_chat_message(
                            match["channel"],
                            chatter,
                            tags.get("user-id"),
                            tags.get("tmi-sent-ts"),
                            match["text"],
                            tags.get("badges"),
                            tags.get("user-type"),
                            tags.get("emotes"),
                        )


class ZeventBot(commands.Bot):
    """Watches every channel in `channels`: chat via sharded IRC, metadata via
    batched Helix polls."""

    def __init__(self, channels: list[str]) -> None:
        super().__init__(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, prefix="!")
        self.channels: list[str] = channels
        self.stats: dict[str, ChannelStats] = {}
        self.chat_writer: BatchParquetWriter = BatchParquetWriter(CHAT_DIR, "live_chat")
        self.metadata_writer: BatchParquetWriter = BatchParquetWriter(
            METADATA_DIR, "metadata"
        )
        # Populated by _chatter_lookup_loop; chatter_ids seen in chat but not
        # yet in chatter_cache land in _unresolved_chatter_ids until then.
        self.chatter_cache: dict[str, ChatterInfo] = {}
        self._unresolved_chatter_ids: set[str] = set()
        # asyncio only holds a *weak* reference to a task returned by
        # create_task() — an unreferenced one can be garbage-collected mid-run
        # (see event_ready below). Keeping strong refs here, the same pattern
        # nifi_client._pending_pushes uses, is what keeps chat/metadata
        # collection from silently dying partway through the event.
        self._background_tasks: set[asyncio.Task[None]] = set()

    def _spawn_background(self, coro: "Coroutine[Any, Any, None]") -> None:
        """asyncio.create_task(), but keeps a strong reference until it completes."""
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def event_ready(self) -> None:
        """Resolves broadcaster IDs, takes an initial poll, then starts all
        background loops."""
        LOGGER.info("Authorized with app token for client id: %s", CLIENT_ID)
        CHAT_DIR.mkdir(parents=True, exist_ok=True)
        METADATA_DIR.mkdir(parents=True, exist_ok=True)

        for chunk in _chunked(self.channels, HELIX_BATCH_SIZE):
            for user in await self.fetch_users(logins=chunk):
                if user.name is None:
                    LOGGER.warning(
                        "Twitch user id=%s has no login name, skipping", user.id
                    )
                    continue
                self.stats[user.name.lower()] = ChannelStats(
                    channel=user.name, broadcaster_id=str(user.id)
                )

        missing = [c for c in self.channels if c.lower() not in self.stats]
        if missing:
            LOGGER.warning("Channels not found on Twitch, skipping: %s", missing)
            missing_lower = {m.lower() for m in missing}
            self.channels = [c for c in self.channels if c.lower() not in missing_lower]

        try:
            await self._poll_all_streams()
        except Exception:
            # Don't let one bad Helix call at startup abort the whole bot before
            # chat/IRC and the periodic loops even start — _metadata_snapshot_loop
            # will retry this every METADATA_SNAPSHOT_INTERVAL_SECONDS anyway.
            LOGGER.exception("Initial stream poll failed, continuing startup")

        connections = [
            ChatConnection(shard, self)
            for shard in _chunked(self.channels, CHANNELS_PER_IRC_CONNECTION)
        ]
        for conn in connections:
            self._spawn_background(conn.run())

        self._spawn_background(self._metadata_snapshot_loop())
        self._spawn_background(self._flush_loop())
        self._spawn_background(self._roster_refresh_loop())
        self._spawn_background(self._chatter_lookup_loop())
        LOGGER.info(
            "Watching %d channels across %d IRC connection(s)",
            len(self.channels),
            len(connections),
        )

    async def _flush_loop(self) -> None:
        """Periodically flushes buffered rows so data lands on disk/NiFi
        continuously."""
        while True:
            await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
            try:
                self.chat_writer.flush()
                self.metadata_writer.flush()
            except Exception:
                LOGGER.exception("Periodic flush failed, retrying next interval")

    def on_chat_message(
        self,
        channel: str,
        chatter: str,
        chatter_id: str | None,
        sent_ts_ms: str | None,
        text: str,
        badges: str | None,
        user_type: str | None,
        emotes: str | None,
    ) -> None:
        """Records one chat message against its channel's stats and buffers
        it for writing.

        Silently ignores messages from a channel we didn't resolve at startup
        (shouldn't happen — IRC only receives messages for channels we joined).
        """
        stats = self.stats.get(channel.lower())
        if stats is None:
            LOGGER.debug("Ignoring chat message from unknown channel %r", channel)
            return

        now = datetime.now(UTC)
        message_sent_at = (
            datetime.fromtimestamp(int(sent_ts_ms) / 1000, tz=UTC)
            if sent_ts_ms
            else now
        )

        stats.chat_message_count += 1
        stats.messages_per_chatter[chatter] = (
            stats.messages_per_chatter.get(chatter, 0) + 1
        )

        chatter_info = self.chatter_cache.get(chatter_id) if chatter_id else None
        if chatter_id and chatter_info is None:
            self._unresolved_chatter_ids.add(chatter_id)

        self.chat_writer.add(
            {
                "channel": stats.channel,
                "chatter": chatter,
                "chatter_id": chatter_id,
                "message_text": text,
                "message_sent_at": message_sent_at,
                "captured_at": now,
                "badges": json.dumps(_parse_badges(badges), ensure_ascii=False),
                "user_type": user_type,
                "emotes": json.dumps(_parse_emotes(emotes), ensure_ascii=False),
                "account_created_at": chatter_info.created_at if chatter_info else None,
                "broadcaster_type": chatter_info.broadcaster_type
                if chatter_info
                else None,
            }
        )

    async def _poll_all_streams(self) -> None:
        """Batched Helix poll for every channel's live status, then a metadata
        snapshot row per channel. Doubles as both the parquet/NiFi write and the
        source of the aggregate log line below — no separate polling loop.
        """
        now = datetime.now(UTC)
        live_by_login: dict[str, twitchio.Stream] = {}
        for chunk in _chunked(self.channels, HELIX_BATCH_SIZE):
            logins: list[int | str] = list(chunk)
            async for stream in self.fetch_streams(user_logins=logins):
                if stream.user.name is None:
                    continue
                live_by_login[stream.user.name.lower()] = stream

        # list(...): _refresh_roster() can add entries to self.stats between
        # awaits in this loop (fetch_streams above yields control); iterating
        # a live dict that grows underneath it raises RuntimeError.
        for login, stats in list(self.stats.items()):
            stream = live_by_login.get(login)
            if stream:
                stats.is_live = True
                stats.title = stream.title
                stats.category = stream.game_name
                stats.viewer_count = stream.viewer_count
                stats.stream_started_at = stream.started_at
            else:
                if stats.is_live:
                    stats.stream_ended_at = now.isoformat()
                stats.is_live = False
                stats.viewer_count = None

            self.metadata_writer.add(
                {
                    "channel": stats.channel,
                    "broadcaster_id": stats.broadcaster_id,
                    "is_live": stats.is_live,
                    "title": stats.title,
                    "category": stats.category,
                    "viewer_count": stats.viewer_count,
                    "duration_seconds": (now - stream.started_at).total_seconds()
                    if stream
                    else None,
                    "stream_started_at": stats.stream_started_at,
                    "snapshot_at": now,
                }
            )

        live_count = sum(1 for s in self.stats.values() if s.is_live)
        total_viewers = sum(s.viewer_count or 0 for s in self.stats.values())
        LOGGER.info(
            "[poll] %d/%d channels live, %d total viewers",
            live_count,
            len(self.stats),
            total_viewers,
        )

    async def _metadata_snapshot_loop(self) -> None:
        """Runs _poll_all_streams() every METADATA_SNAPSHOT_INTERVAL_SECONDS,
        forever.

        Unlike ChatConnection.run(), _poll_all_streams() had no error handling:
        one Helix hiccup (timeout, rate limit, transient 5xx) would raise out of
        this background task and silently kill metadata snapshots for every
        channel for the rest of the run, with chat capture unaffected and no
        visible crash. Catching and logging here lets the loop self-heal on the
        next interval instead of dying for good.
        """
        while True:
            await asyncio.sleep(METADATA_SNAPSHOT_INTERVAL_SECONDS)
            try:
                await self._poll_all_streams()
            except Exception:
                LOGGER.exception(
                    "Metadata snapshot poll failed, retrying next interval"
                )

    async def _chatter_lookup_loop(self) -> None:
        """Resolves account_created_at/broadcaster_type for every chatter_id
        in _unresolved_chatter_ids, every CHATTER_LOOKUP_INTERVAL_SECONDS.

        A chatter's first few messages (before this loop's next tick, or
        before the lookup returns) land with these two columns null; every
        message after that is filled in from chatter_cache. Same broad
        except/log/retry-next-tick pattern as _metadata_snapshot_loop: one
        Helix hiccup shouldn't stop this for the rest of the run, and any
        chatter_id not resolved this pass (including one Get Users didn't
        return, e.g. a deleted account) just stays pending for the next.
        """
        while True:
            await asyncio.sleep(CHATTER_LOOKUP_INTERVAL_SECONDS)
            if not self._unresolved_chatter_ids:
                continue
            try:
                for chunk in _chunked(
                    list(self._unresolved_chatter_ids), HELIX_BATCH_SIZE
                ):
                    ids: list[int | str] = list(chunk)
                    for user in await self.fetch_users(ids=ids):
                        chatter_id = str(user.id)
                        self.chatter_cache[chatter_id] = ChatterInfo(
                            created_at=user.created_at,
                            broadcaster_type=user.broadcaster_type,
                        )
                        self._unresolved_chatter_ids.discard(chatter_id)
            except Exception:
                LOGGER.exception("Chatter lookup failed, retrying next interval")

    async def _roster_refresh_loop(self) -> None:
        """Re-fetches the Zevent roster every ROSTER_REFRESH_INTERVAL_SECONDS
        and starts watching any channel that has joined since startup.

        Channels are only ever added, never removed here: a channel missing
        from one API response is far more likely a transient zevent.fr
        hiccup than an actual withdrawal, and a channel that really did stop
        streaming already shows up as offline via _poll_all_streams.
        """
        while True:
            await asyncio.sleep(ROSTER_REFRESH_INTERVAL_SECONDS)
            try:
                await self._refresh_roster()
            except Exception:
                LOGGER.exception("Roster refresh failed, retrying next interval")

    async def _refresh_roster(self) -> None:
        """Fetches the current roster and, for any login not already in
        self.stats, resolves its broadcaster id, adds it to polling, and
        spawns new IRC connection(s) sharded the same way as at startup."""
        roster = await _fetch_roster()
        new_logins = [c for c in roster if c.lower() not in self.stats]
        if not new_logins:
            return

        new_channels: list[str] = []
        for chunk in _chunked(new_logins, HELIX_BATCH_SIZE):
            for user in await self.fetch_users(logins=chunk):
                if user.name is None:
                    LOGGER.warning(
                        "Twitch user id=%s has no login name, skipping", user.id
                    )
                    continue
                login = user.name.lower()
                if login in self.stats:
                    continue
                self.stats[login] = ChannelStats(
                    channel=user.name, broadcaster_id=str(user.id)
                )
                new_channels.append(user.name)

        if not new_channels:
            return

        self.channels.extend(new_channels)
        shards = _chunked(new_channels, CHANNELS_PER_IRC_CONNECTION)
        for shard in shards:
            self._spawn_background(ChatConnection(shard, self).run())

        LOGGER.info(
            "[roster] %d new channel(s) joined (%d new IRC connection(s)), "
            "now watching %d total: %s",
            len(new_channels),
            len(shards),
            len(self.channels),
            new_channels,
        )

    @override
    async def close(self, **options: object) -> None:
        """Flushes buffered rows, waits for any in-flight NiFi push, then
        closes the bot."""
        self.chat_writer.flush()
        self.metadata_writer.flush()
        await nifi_client.wait_for_pending_pushes()
        await super().close(**options)


async def _fetch_roster() -> list[str]:
    """Fetches the current Zevent streamer roster (Twitch logins) from
    zevent.fr/api/."""
    async with aiohttp.ClientSession() as session:
        snapshot = await zevent_api.fetch_snapshot(session)
    return [s.twitch_login for s in snapshot.streamers]


def main() -> None:
    """Entry point: fetches the roster, authenticates with a Twitch app token,
    then runs forever, restarting the whole bot on any unhandled crash.

    Uses an app access token (Client Credentials Grant) rather than Device
    Code Flow: Get Streams/Get Users are public, read-only endpoints that
    don't need a user token, and an app token needs a confidential client
    (client_id + client_secret) but never needs a human to authorize it, on
    this startup or any crash-triggered restart after it — see
    ZeventBot.__init__ and CLIENT_SECRET above. DCF was tried first and
    dropped: its user token needs an hourly-ish refresh, and twitchio's
    refresh occasionally raced and got rejected by Twitch as an invalid
    refresh token, which stranded metadata polling until a human re-ran the
    device flow — unacceptable for an unattended 55-hour event.
    """
    logging_setup.setup_logging()
    _ = signal.signal(signal.SIGTERM, signal.default_int_handler)

    async def runner() -> None:
        channels = await _fetch_roster()
        LOGGER.info("Fetched %d channels from zevent.fr/api/", len(channels))
        async with ZeventBot(channels) as bot:
            await bot.start(with_adapter=False, load_tokens=False, save_tokens=False)

    backoff = 5
    while True:
        try:
            asyncio.run(runner())
            return
        except KeyboardInterrupt:
            LOGGER.warning("Shutting down due to KeyboardInterrupt.")
            return
        except Exception:
            sleep_seconds = min(backoff, 60)
            LOGGER.exception("main() crashed, restarting in %ds", sleep_seconds)
            time.sleep(sleep_seconds)
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    main()
