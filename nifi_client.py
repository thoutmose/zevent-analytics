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
import random
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
# How many times to retry a batch that NiFi rejected (503, ListenHTTP's connection
# backpressure) or that failed ambiguously (timeout/connection error) before giving
# up. Both are safe to retry now that batch_id is generated once and reused across
# attempts (see push_batch): a 503 means ListenHTTP never accepted the flowfile, and
# an ambiguous failure that *did* land anyway is caught by ON CONFLICT (batch_id,
# row_number) DO NOTHING downstream (DO UPDATE for the upsert streams, keyed on their
# own business key instead — see ARCHITECTURE.md step 6b) — a retry becomes a no-op
# rather than a duplicate insert either way. Found necessary live on srv-dev,
# 2026-09-02: a stress test showed ListenHTTP starts returning 503 well before
# Postgres's own write ceiling is anywhere near reached, and this module previously
# just dropped the batch on the first 503 with no retry at all.
NIFI_MAX_RETRIES: int = int(os.environ.get("NIFI_MAX_RETRIES", "4"))
NIFI_RETRY_BACKOFF_SECONDS: float = float(
    os.environ.get("NIFI_RETRY_BACKOFF_SECONDS", "1.0")
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
    """POSTs one batch to NIFI_WEBHOOK_URL (a NiFi ListenHTTP processor), retrying
    on backpressure (503) or an ambiguous connection failure up to NIFI_MAX_RETRIES
    times with exponential backoff.

    No-op if NIFI_WEBHOOK_URL is unset or rows is empty. Prefer
    push_batch_background() over calling this directly with asyncio.create_task,
    so the task is tracked and can be awaited on shutdown.
    """
    if not NIFI_WEBHOOK_URL or not rows:
        return

    # Generated once and reused across every retry attempt below — this is what
    # makes retrying safe (see NIFI_MAX_RETRIES' docstring): downstream ON CONFLICT
    # only dedupes a replay if it's still the same batch_id.
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

    async with aiohttp.ClientSession() as session:
        for attempt in range(1, NIFI_MAX_RETRIES + 1):
            last_status: int | None = None
            try:
                async with session.post(
                    NIFI_WEBHOOK_URL,
                    data=body,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=NIFI_REQUEST_TIMEOUT_SECONDS),
                ) as resp:
                    last_status = resp.status
                    resp.raise_for_status()
                LOGGER.info(
                    "[nifi] pushed %s/%s batch_id=%s (%d rows)%s",
                    source,
                    stream,
                    batch_id,
                    len(rows),
                    f" on retry {attempt}/{NIFI_MAX_RETRIES}" if attempt > 1 else "",
                )
                return
            except (aiohttp.ClientError, asyncio.TimeoutError):
                # 503 (ListenHTTP's connection backpressure) is the expected retry
                # case; anything else (a connection reset, a timeout) is ambiguous —
                # it may have already landed — but is just as safe to retry here
                # given the stable batch_id above. A genuine 4xx client error (bad
                # payload) would keep failing identically on every attempt, so this
                # doesn't try to distinguish it from a transient one; it just retries
                # everything and gives up after NIFI_MAX_RETRIES either way.
                if attempt == NIFI_MAX_RETRIES:
                    LOGGER.exception(
                        "[nifi] push %s/%s batch_id=%s (%d rows) to %s did not "
                        "confirm after %d attempt(s) — it may or may not have landed",
                        source,
                        stream,
                        batch_id,
                        len(rows),
                        NIFI_WEBHOOK_URL,
                        attempt,
                    )
                    return
                backoff = NIFI_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
                LOGGER.warning(
                    "[nifi] push %s/%s batch_id=%s attempt %d/%d failed "
                    "(status=%s), retrying in %.1fs",
                    source,
                    stream,
                    batch_id,
                    attempt,
                    NIFI_MAX_RETRIES,
                    last_status,
                    backoff,
                )
                await asyncio.sleep(backoff + random.uniform(0, backoff * 0.1))


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
