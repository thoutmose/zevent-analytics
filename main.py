"""Zevent channel extractor: aggregates live-stream data for one Twitch channel.

Collects, while the channel is live:
  - stream metadata: title, category, start/end time, duration
  - viewer_count samples (polled from Helix; EventSub has no viewer-count event)
  - chat message count, and message count per chatter
  - subs and raids received (only deliverable if this account is the
    broadcaster or a moderator of the target channel - see README)

This is a same-day test script: it prints aggregated stats as JSON to stdout
on every poll so you can verify the data is being captured correctly before
wiring it into Airflow/NiFi.
"""

import asyncio
import json
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

import twitchio
from twitchio import eventsub
from twitchio.ext import commands
from dotenv import load_dotenv

load_dotenv()

LOGGER = logging.getLogger("zevent_extractor")

CLIENT_ID = os.environ["TWITCH_CLIENT_ID"]
TARGET_CHANNEL = os.environ["TWITCH_CHANNEL"]
POLL_INTERVAL_SECONDS = int(os.environ.get("TWITCH_POLL_INTERVAL_SECONDS", "60"))

SCOPES = twitchio.Scopes()
SCOPES.user_read_chat = True
SCOPES.user_bot = True
SCOPES.channel_read_subscriptions = True


@dataclass
class ChannelStats:
    channel: str
    broadcaster_id: str
    is_live: bool = False
    title: str | None = None
    category: str | None = None
    stream_started_at: str | None = None
    stream_ended_at: str | None = None
    viewer_count_samples: list[dict] = field(default_factory=list)
    chat_message_count: int = 0
    messages_per_chatter: dict[str, int] = field(default_factory=dict)
    chat_messages: list[dict] = field(default_factory=list)
    subscriptions: int = 0
    raids_received: list[dict] = field(default_factory=list)


class ZeventBot(commands.Bot):
    def __init__(self) -> None:
        super().__init__(client_id=CLIENT_ID, scopes=SCOPES, prefix="!")
        self.stats: ChannelStats | None = None

    async def event_ready(self) -> None:
        LOGGER.info("Authorized as user id: %s", self.bot_id)

        users = await self.fetch_users(logins=[TARGET_CHANNEL])
        if not users:
            raise RuntimeError(f"Channel not found: {TARGET_CHANNEL}")
        broadcaster = users[0]
        self.stats = ChannelStats(channel=TARGET_CHANNEL, broadcaster_id=str(broadcaster.id))

        # These three work with just an app/DCF token for ANY public channel.
        for sub in (
            eventsub.StreamOnlineSubscription(broadcaster_user_id=broadcaster.id),
            eventsub.StreamOfflineSubscription(broadcaster_user_id=broadcaster.id),
            eventsub.ChannelUpdateSubscription(broadcaster_user_id=broadcaster.id),
        ):
            await self.subscribe_websocket(sub)

        # Chat and raid work for ANY channel too (chat: as_bot=True uses our
        # own user:read:chat-scoped token, which Twitch allows to read chat
        # in channels we don't own; raid needs no auth at all). Subs is the
        # exception - channel:read:subscriptions can only be granted by the
        # broadcaster themselves, so it will fail unless we ARE that channel.
        for sub in (
            eventsub.ChatMessageSubscription(broadcaster_user_id=broadcaster.id, user_id=self.bot_id),
            eventsub.ChannelSubscribeSubscription(broadcaster_user_id=broadcaster.id),
            eventsub.ChannelRaidSubscription(to_broadcaster_user_id=broadcaster.id),
        ):
            try:
                await self.subscribe_websocket(sub, as_bot=True)
            except Exception:
                LOGGER.exception(
                    "Could not subscribe to %s on %s (likely missing mod/bot permission on that channel)",
                    sub.type,
                    TARGET_CHANNEL,
                )

        await self._poll_stream_info()
        asyncio.create_task(self._poll_loop())
        LOGGER.info("Watching %s (broadcaster_id=%s)", TARGET_CHANNEL, broadcaster.id)

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
            await self._poll_stream_info()
            self._dump_stats()

    async def _poll_stream_info(self) -> None:
        streams = [s async for s in self.fetch_streams(user_logins=[TARGET_CHANNEL])]
        stats = self.stats
        now = datetime.now(timezone.utc).isoformat()

        if streams:
            stream = streams[0]
            stats.is_live = True
            stats.title = stream.title
            stats.category = stream.game_name
            stats.stream_started_at = stream.started_at.isoformat()
            stats.viewer_count_samples.append({"t": now, "viewers": stream.viewer_count})
            LOGGER.info(
                "[poll] live viewers=%s title=%r category=%r", stream.viewer_count, stream.title, stream.game_name
            )
        else:
            if stats.is_live:
                stats.stream_ended_at = now
            stats.is_live = False
            LOGGER.info("[poll] channel offline")

    def _dump_stats(self) -> None:
        LOGGER.info("STATS %s", json.dumps(asdict(self.stats), default=str, ensure_ascii=False))

    async def event_message(self, payload: twitchio.ChatMessage) -> None:
        stats = self.stats
        stats.chat_message_count += 1
        chatter = payload.chatter.name
        stats.messages_per_chatter[chatter] = stats.messages_per_chatter.get(chatter, 0) + 1
        stats.chat_messages.append(
            {
                "t": datetime.now(timezone.utc).isoformat(),
                "chatter_id": payload.chatter.id,
                "chatter": chatter,
                "text": payload.text,
            }
        )
        LOGGER.info("[chat] %s: %s", chatter, payload.text)

    async def event_subscription(self, payload: twitchio.ChannelSubscribe) -> None:
        self.stats.subscriptions += 1
        LOGGER.info("[sub] %r", payload)

    async def event_raid(self, payload: twitchio.ChannelRaid) -> None:
        self.stats.raids_received.append(
            {
                "from": payload.from_broadcaster.name,
                "viewers": payload.viewer_count,
                "t": datetime.now(timezone.utc).isoformat(),
            }
        )
        LOGGER.info("[raid] %r", payload)

    async def event_stream_online(self, payload: twitchio.StreamOnline) -> None:
        LOGGER.info("[stream.online] %r", payload)
        await self._poll_stream_info()

    async def event_stream_offline(self, payload: twitchio.StreamOffline) -> None:
        LOGGER.info("[stream.offline] %r", payload)
        self.stats.is_live = False
        self.stats.stream_ended_at = datetime.now(timezone.utc).isoformat()
        self._dump_stats()

    async def event_channel_update(self, payload: twitchio.ChannelUpdate) -> None:
        LOGGER.info("[channel.update] %r", payload)
        self.stats.title = payload.title
        self.stats.category = payload.category_name


def main() -> None:
    twitchio.utils.setup_logging(level=logging.INFO)

    async def runner() -> None:
        async with ZeventBot() as bot:
            resp = (await bot.login_dcf()) or {}
            print(f"Authorize this app: {resp.get('verification_uri', '')}")
            await bot.start_dcf(device_code=resp.get("device_code"), interval=resp.get("interval", 5))

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        LOGGER.warning("Shutting down due to KeyboardInterrupt.")


if __name__ == "__main__":
    main()
