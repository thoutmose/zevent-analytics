"""Optional push of extraction batches to the NiFi ingestion webhook (see
ARCHITECTURE.md).

Set NIFI_WEBHOOK_URL to enable. Left unset, main.py and zevent_api.py behave
exactly as before (parquet-only / stdout-only) — this module is an additive
sink, not a replacement.

Each batch carries a batch_id (UUID) and a row_number per row, so the NiFi
flow can use `INSERT ... ON CONFLICT (batch_id, row_number) DO NOTHING`
downstream and stay safe to replay after a crash or a dead-letter retry.
"""

import asyncio
import json
import logging
import os
import uuid
from datetime import UTC, date, datetime
from typing import Any

import aiohttp
from dotenv import load_dotenv

_ = load_dotenv()

LOGGER: logging.Logger = logging.getLogger("nifi_client")

NIFI_WEBHOOK_URL: str | None = os.environ.get("NIFI_WEBHOOK_URL")
NIFI_REQUEST_TIMEOUT_SECONDS: int = int(
    os.environ.get("NIFI_REQUEST_TIMEOUT_SECONDS", "10")
)

# Tracks in-flight push_batch() background tasks (see push_batch_background), so a
# graceful shutdown can await them via wait_for_pending_pushes() instead of cancelling
# one mid-request. A cancelled-but-already-sent request is ambiguous: NiFi may have
# already ingested it, so blindly retrying on "failure" can silently double-insert
# (seen in practice: a client-side timeout after the request had, in fact, landed).
_pending_pushes: set[asyncio.Task[None]] = set()


def _json_default(value: object) -> str:
    """json.dumps(default=...) hook: normalizes dates/datetimes to one fixed format.

    main.py passes raw datetime objects for some fields (e.g. captured_at,
    message_sent_at). A plain `default=str` would format those with str()
    ("2026-08-28 21:04:59.123456+00:00"), which differs from the .isoformat()
    strings other fields already use ("2026-08-28T21:04:59.123456+00:00"). NiFi's
    JsonTreeReader needs one consistent, fixed-width format to infer these as
    TIMESTAMP columns instead of TEXT, so every datetime/date is normalized here.
    """
    if isinstance(value, datetime):
        # timespec="microseconds" forces the fractional part to always appear
        # (isoformat() otherwise omits it when microsecond==0), so every
        # timestamp matches NiFi's Timestamp Format with one fixed-width pattern.
        return value.isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


async def push_batch(source: str, stream: str, rows: list[dict[str, Any]]) -> None:
    """POSTs one batch to NIFI_WEBHOOK_URL (a NiFi ListenHTTP processor).

    No-op if NIFI_WEBHOOK_URL is unset or rows is empty. Prefer
    push_batch_background() over calling this directly with asyncio.create_task,
    so the task is tracked and can be awaited on shutdown.
    """
    if not NIFI_WEBHOOK_URL or not rows:
        return

    batch_id = str(uuid.uuid4())
    payload = {
        "source": source,
        "stream": stream,
        "batch_id": batch_id,
        "batched_at": datetime.now(UTC).isoformat(),
        # batch_id is duplicated onto every row (not just the envelope) because the
        # NiFi flow splits "rows" into one flowfile per row before PutDatabaseRecord,
        # and needs batch_id present on each one for the UNIQUE (batch_id, row_number)
        # constraint downstream.
        "rows": [
            {"row_number": i, "batch_id": batch_id, **row} for i, row in enumerate(rows)
        ],
    }
    body = json.dumps(payload, default=_json_default, ensure_ascii=False)
    LOGGER.debug(
        "[nifi] sending %s/%s batch_id=%s (%d rows, %d bytes)",
        source,
        stream,
        batch_id,
        len(rows),
        len(body),
    )

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                NIFI_WEBHOOK_URL,
                data=body,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=NIFI_REQUEST_TIMEOUT_SECONDS),
            ) as resp:
                resp.raise_for_status()
        LOGGER.info(
            "[nifi] pushed %s/%s batch_id=%s (%d rows)",
            source,
            stream,
            batch_id,
            len(rows),
        )
    except (aiohttp.ClientError, asyncio.TimeoutError):
        # A timeout here is ambiguous, not necessarily a failure: the request may
        # have already reached NiFi even though we didn't see the response in time.
        # Don't auto-retry on this exception — a blind retry can double-insert.
        LOGGER.exception(
            "[nifi] push %s/%s batch_id=%s (%d rows) to %s did not confirm — "
            "it may or may not have landed",
            source,
            stream,
            batch_id,
            len(rows),
            NIFI_WEBHOOK_URL,
        )


def push_batch_background(source: str, stream: str, rows: list[dict[str, Any]]) -> None:
    """Fires push_batch() as a tracked background task (fire-and-forget for the
    caller, but visible to wait_for_pending_pushes() for a graceful shutdown)."""
    if not NIFI_WEBHOOK_URL or not rows:
        return
    task = asyncio.create_task(push_batch(source, stream, rows))
    _pending_pushes.add(task)
    task.add_done_callback(_pending_pushes.discard)


async def wait_for_pending_pushes(timeout: float = 30.0) -> None:
    """Awaits every push_batch_background() task still in flight.

    Call this during shutdown, before the event loop closes, so an in-progress
    push isn't cancelled mid-request (see the module docstring on _pending_pushes
    for why that's worse than just waiting). Bounded by `timeout` so one hung
    request can't block shutdown forever.
    """
    pending = list(_pending_pushes)
    if not pending:
        return
    LOGGER.info("[nifi] waiting for %d pending push(es) before shutdown", len(pending))
    _, still_pending = await asyncio.wait(pending, timeout=timeout)
    if still_pending:
        LOGGER.warning(
            "[nifi] %d push(es) still in flight after %.0fs, letting "
            "shutdown proceed anyway",
            len(still_pending),
            timeout,
        )
