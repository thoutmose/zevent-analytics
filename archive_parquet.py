"""Archives old local Parquet files to a remote cold-storage host.

Not part of the always-on extractors (main.py) or the CD pipeline (which
only deploys the NiFi stack, see DEPLOYMENT.md) — this is a standalone,
one-shot maintenance script meant to be run periodically (e.g. cron) on
whatever host is running the extractors, since nothing else in this repo
ever cleans up PARQUET_OUTPUT_DIR.

Every *.parquet file older than ARCHIVE_MIN_AGE_SECONDS gets bundled into a
single tar.zst archive, rsynced to ARCHIVE_REMOTE_HOST:ARCHIVE_REMOTE_PATH,
verified by sha256, and only then has its originals deleted locally — see
archive_common.py for the shared bundle/transfer/verify/delete mechanics
(also used by archive_logs.py). Any failure leaves every original file
untouched — the whole batch is naturally retried on the next run, since
the filesystem itself is the retry state.

ARCHIVE_REMOTE_HOST is meant to be a Host alias the operator defines in
their own ~/.ssh/config (HostName/User/IdentityFile) — this script has no
credentials of its own, it just shells out to ssh/rsync and relies on
ambient SSH config/agent, the same trust model CD's rsync-over-SSH deploy
step already uses.
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq
from dotenv import load_dotenv

import archive_common
import logging_setup

_ = load_dotenv()

LOGGER: logging.Logger = logging.getLogger("archive_parquet")

PARQUET_OUTPUT_DIR: Path = Path(os.environ.get("PARQUET_OUTPUT_DIR", "data"))
ARCHIVE_REMOTE_HOST: str = os.environ.get("ARCHIVE_REMOTE_HOST", "srv-services")
ARCHIVE_REMOTE_PATH: str = os.environ.get(
    "ARCHIVE_REMOTE_PATH", "zevent-parquet-archive"
)
# Safety margin before a file is eligible for archival. Not tied to
# FLUSH_INTERVAL_SECONDS: main.py's BatchParquetWriter.flush() writes each
# file atomically in one pq.write_table() call, so there's no "still being
# appended to" race — the (much narrower) risk is listing a filename mid
# write, which a flat, generous default guards against regardless of the
# extractor's own flush cadence.
ARCHIVE_MIN_AGE_SECONDS: int = int(os.environ.get("ARCHIVE_MIN_AGE_SECONDS", "600"))


def find_candidates(root: Path, min_age_seconds: float) -> tuple[list[Path], int]:
    """Returns (eligible files, skipped count) under `root`.

    A file is skipped (not eligible this run) if it's younger than
    min_age_seconds, or if it doesn't yet parse as valid Parquet metadata
    (logged as a warning) — both cases are expected to self-resolve on a
    later run, not permanent failures.
    """
    now = time.time()
    candidates: list[Path] = []
    skipped = 0
    for path in sorted(root.rglob("*.parquet")):
        age = now - path.stat().st_mtime
        if age < min_age_seconds:
            skipped += 1
            continue
        try:
            _ = pq.read_metadata(path)
        except Exception:
            LOGGER.warning(
                "[archive] skipping %s: not yet readable as Parquet (age=%.0fs)"
                " - likely still being written",
                path,
                age,
            )
            skipped += 1
            continue
        candidates.append(path)
    return candidates, skipped


def run(*, dry_run: bool = False) -> tuple[int, int, int, int]:
    """Scans PARQUET_OUTPUT_DIR and archives every eligible file.

    Returns (archived, skipped, failed, bytes_freed) counts for the run.
    """
    candidates, skipped = find_candidates(PARQUET_OUTPUT_DIR, ARCHIVE_MIN_AGE_SECONDS)
    return archive_common.run_bundled(
        candidates,
        skipped,
        PARQUET_OUTPUT_DIR,
        ARCHIVE_REMOTE_HOST,
        ARCHIVE_REMOTE_PATH,
        "parquet",
        dry_run=dry_run,
    )


def main() -> None:
    """Entry point: runs one archival pass and exits 1 if anything failed."""
    logging_setup.setup_logging()
    parser = argparse.ArgumentParser(
        description=(
            "Archives Parquet files older than ARCHIVE_MIN_AGE_SECONDS to "
            "ARCHIVE_REMOTE_HOST, deleting the local copy only once the "
            "remote copy's sha256 is verified to match."
        )
    )
    _ = parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan and log what would be archived, without transferring or "
        "deleting anything.",
    )
    args = parser.parse_args()

    _, _, failed, _ = run(dry_run=args.dry_run)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
