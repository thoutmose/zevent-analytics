"""Zevent multi-channel extractor: aggregates live-stream data for every Twitch
channel currently participating in Zevent.

The channel roster is fetched once at startup from zevent.fr/api/ (see
zevent_api.py) rather than hardcoded, since it's ~300+ channels and changes
from one edition to the next.

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
import logging
import os
import random
import re
import signal
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
METADATA_SNAPSHOT_INTERVAL_SECONDS: int = int(
    os.environ.get("METADATA_SNAPSHOT_INTERVAL_SECONDS", "15")
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

SCOPES: twitchio.Scopes = twitchio.Scopes()


def _chunked(items: list[str], size: int) -> list[list[str]]:
    """Splits items into consecutive slices of at most `size` elements each."""
    return [items[i : i + size] for i in range(0, len(items), size)]


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
            except (aiohttp.ClientError, ConnectionResetError, asyncio.TimeoutError):
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
                        )


class ZeventBot(commands.Bot):
    """Watches every channel in `channels`: chat via sharded IRC, metadata via
    batched Helix polls."""

    def __init__(self, channels: list[str]) -> None:
        super().__init__(client_id=CLIENT_ID, scopes=SCOPES, prefix="!")
        self.channels: list[str] = channels
        self.stats: dict[str, ChannelStats] = {}
        self.chat_writer: BatchParquetWriter = BatchParquetWriter(CHAT_DIR, "live_chat")
        self.metadata_writer: BatchParquetWriter = BatchParquetWriter(
            METADATA_DIR, "metadata"
        )
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
        LOGGER.info("Authorized as user id: %s", self.bot_id)
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

        await self._poll_all_streams()

        connections = [
            ChatConnection(shard, self)
            for shard in _chunked(self.channels, CHANNELS_PER_IRC_CONNECTION)
        ]
        for conn in connections:
            self._spawn_background(conn.run())

        self._spawn_background(self._metadata_snapshot_loop())
        self._spawn_background(self._flush_loop())
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
            self.chat_writer.flush()
            self.metadata_writer.flush()

    def on_chat_message(
        self,
        channel: str,
        chatter: str,
        chatter_id: str | None,
        sent_ts_ms: str | None,
        text: str,
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

        self.chat_writer.add(
            {
                "channel": stats.channel,
                "chatter": chatter,
                "chatter_id": chatter_id,
                "message_text": text,
                "message_sent_at": message_sent_at,
                "captured_at": now,
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

        for login, stats in self.stats.items():
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
        forever."""
        while True:
            await asyncio.sleep(METADATA_SNAPSHOT_INTERVAL_SECONDS)
            try:
                await self._poll_all_streams()
            except (aiohttp.ClientError, asyncio.TimeoutError):
                LOGGER.exception("Metadata poll failed, retrying next interval")

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
    """Entry point: fetches the roster, authorizes via Device Code Flow, then
    runs forever."""
    logging_setup.setup_logging()
    _ = signal.signal(signal.SIGTERM, signal.default_int_handler)

    async def runner() -> None:
        channels = await _fetch_roster()
        LOGGER.info("Fetched %d channels from zevent.fr/api/", len(channels))
        async with ZeventBot(channels) as bot:
            resp = (await bot.login_dcf()) or {}
            print(f"Authorize this app: {resp.get('verification_uri', '')}")
            await bot.start_dcf(
                device_code=resp.get("device_code"), interval=resp.get("interval", 5)
            )

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        LOGGER.warning("Shutting down due to KeyboardInterrupt.")


if __name__ == "__main__":
    main()
