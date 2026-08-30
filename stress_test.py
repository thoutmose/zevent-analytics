"""Ramping load test against the NiFi ingestion webhook (see ARCHITECTURE.md).

Fires POST /ingest batches of synthetic live_chat rows at increasing concurrency to
find where the pipeline (ListenHTTP -> RouteOnAttribute -> SplitJson ->
PutDatabaseRecord -> Postgres) starts falling behind or dropping data. All rows are
tagged with channel=f"stress-test-{RUN_ID}" and source="stress-test" so they're
identifiable and safe to delete afterward:

    DELETE FROM bronze_live_chat WHERE channel LIKE 'stress-test-%';

Usage:
    uv run python stress_test.py [--url http://localhost:8888/ingest]
                                  [--stages 1,2,5,10,25,50,100]
                                  [--stage-seconds 15] [--batch-rows 20]

Each stage runs N concurrent workers, each looping "build batch -> POST -> repeat"
back-to-back (no think time) for --stage-seconds. Stops early once two consecutive
stages show >20% request failure, since that's already past the pipeline's limit.
"""

import argparse
import asyncio
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import aiohttp

DEAD_LETTER_DIR = Path(__file__).parent / "nifi" / "dead-letter"


@dataclass
class StageResult:
    concurrency: int
    requests: int = 0
    rows_sent: int = 0
    successes: int = 0
    failures: int = 0
    latencies: list[float] = field(default_factory=list)

    @property
    def error_rate(self) -> float:
        return self.failures / self.requests if self.requests else 0.0

    def percentile(self, p: float) -> float:
        if not self.latencies:
            return 0.0
        s = sorted(self.latencies)
        idx = min(int(len(s) * p), len(s) - 1)
        return s[idx]


def make_batch(run_id: str, n_rows: int) -> dict:
    batch_id = str(uuid.uuid4())
    now = datetime.now(UTC).isoformat(timespec="microseconds")
    return {
        "source": "stress-test",
        "stream": "live_chat",
        "batch_id": batch_id,
        "batched_at": now,
        "rows": [
            {
                "row_number": i,
                "batch_id": batch_id,
                "channel": f"stress-test-{run_id}",
                "chatter": f"loadgen_{i}",
                "chatter_id": str(i),
                "message_text": "x" * 80,
                "message_sent_at": now,
                "captured_at": now,
            }
            for i in range(n_rows)
        ],
    }


async def worker(
    session: aiohttp.ClientSession,
    url: str,
    run_id: str,
    batch_rows: int,
    stop_at: float,
    result: StageResult,
) -> None:
    timeout = aiohttp.ClientTimeout(total=10)
    while time.monotonic() < stop_at:
        payload = make_batch(run_id, batch_rows)
        t0 = time.monotonic()
        result.requests += 1
        result.rows_sent += batch_rows
        try:
            async with session.post(url, json=payload, timeout=timeout) as resp:
                await resp.read()
                result.latencies.append(time.monotonic() - t0)
                if resp.status < 300:
                    result.successes += 1
                else:
                    result.failures += 1
        except (aiohttp.ClientError, asyncio.TimeoutError):
            result.latencies.append(time.monotonic() - t0)
            result.failures += 1


async def run_stage(
    url: str, run_id: str, concurrency: int, seconds: float, batch_rows: int
) -> StageResult:
    result = StageResult(concurrency=concurrency)
    stop_at = time.monotonic() + seconds
    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=connector) as session:
        await asyncio.gather(
            *(
                worker(session, url, run_id, batch_rows, stop_at, result)
                for _ in range(concurrency)
            )
        )
    return result


def dead_letter_count() -> int:
    if not DEAD_LETTER_DIR.exists():
        return 0
    return sum(1 for _ in DEAD_LETTER_DIR.iterdir())


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--url", default="http://localhost:8888/ingest")
    parser.add_argument("--stages", default="1,2,5,10,25,50,100,200")
    parser.add_argument("--stage-seconds", type=float, default=15.0)
    parser.add_argument("--batch-rows", type=int, default=20)
    args = parser.parse_args()

    stages = [int(s) for s in args.stages.split(",")]
    run_id = uuid.uuid4().hex[:8]
    print(f"# Stress test run_id={run_id} target={args.url}")
    print(
        "# Cleanup after: DELETE FROM bronze_live_chat WHERE "  # nosec B608 - printed operator hint only, never executed as SQL
        f"channel = 'stress-test-{run_id}';\n"
    )

    dl_before = dead_letter_count()
    print(
        f"{'conc':>5} {'reqs':>7} {'rows':>8} {'ok':>7} {'fail':>6} {'err%':>6} "
        f"{'p50ms':>7} {'p95ms':>7} {'p99ms':>7} {'req/s':>8} {'rows/s':>9}"
    )

    consecutive_bad = 0
    for conc in stages:
        result = await run_stage(
            args.url, run_id, conc, args.stage_seconds, args.batch_rows
        )
        rps = result.requests / args.stage_seconds
        rows_per_s = result.rows_sent / args.stage_seconds
        print(
            f"{conc:>5} {result.requests:>7} {result.rows_sent:>8} "
            f"{result.successes:>7} {result.failures:>6} "
            f"{result.error_rate * 100:>5.1f}% "
            f"{result.percentile(0.50) * 1000:>7.0f} "
            f"{result.percentile(0.95) * 1000:>7.0f} "
            f"{result.percentile(0.99) * 1000:>7.0f} "
            f"{rps:>8.1f} {rows_per_s:>9.1f}"
        )

        if result.error_rate > 0.20:
            consecutive_bad += 1
            if consecutive_bad >= 2:
                print(
                    "\n# Stopping early: >20% error rate at "
                    f"concurrency={conc} for 2 consecutive stages."
                )
                break
        else:
            consecutive_bad = 0

        await asyncio.sleep(2)  # brief cooldown between stages

    dl_after = dead_letter_count()
    print(
        f"\n# Dead-letter files: {dl_before} before -> {dl_after} after "
        f"(+{dl_after - dl_before})"
    )


if __name__ == "__main__":
    asyncio.run(main())
