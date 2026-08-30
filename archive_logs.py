"""Archives closed/rotated log backups to a remote cold-storage host.

Companion to archive_parquet.py, sharing its tar+zstd bundle + verify +
delete-on-confirm mechanics (see archive_common.py). Only rotated backups (e.g.
debug.log.3, debug.log.3.gz) are eligible — the active files
logging.yaml's handlers are currently appending to (info.log, debug.log,
etc.) are never touched. Unlike Parquet, there's no age-guard/race to worry
about here: logging.CompressedRotatingFileHandler never writes to a backup
again once it's created (doRollover() either shifts it straight to the next
numbered name, or it's the terminal .<backupCount> that gets deleted), so a
rotated file is immutable from the moment it exists.

Active basenames are read from logging.yaml itself (each handler's
`filename:`), not hardcoded, so this stays in sync if a handler is ever
added, renamed, or removed.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

import archive_common
import logging_setup

_ = load_dotenv()

LOGGER: logging.Logger = logging.getLogger("archive_logs")

LOG_DIR: Path = logging_setup.LOG_DIR
ARCHIVE_REMOTE_HOST: str = os.environ.get("ARCHIVE_REMOTE_HOST", "srv-services")
ARCHIVE_LOGS_REMOTE_PATH: str = os.environ.get(
    "ARCHIVE_LOGS_REMOTE_PATH", "zevent-logs-archive"
)


def active_log_basenames(config_path: Path) -> set[str]:
    """Returns the active log filenames declared by logging.yaml's handlers.

    These are the files actively being appended to right now — never
    eligible for archival, regardless of age. Handlers with no `filename`
    (e.g. console/error_console, both StreamHandlers) are skipped.
    """
    with config_path.open() as f:
        config = yaml.safe_load(f)
    return {
        Path(handler["filename"]).name
        for handler in config.get("handlers", {}).values()
        if "filename" in handler
    }


def find_candidates(root: Path, active_basenames: set[str]) -> list[Path]:
    """Returns rotated log backups directly under `root`.

    A file qualifies if its name starts with a known active basename plus a
    "." (e.g. "debug.log.3" or "debug.log.3.gz" for active basename
    "debug.log"). The bare active file itself, and anything not matching a
    known active basename (e.g. dev_soak_test.py and its report), are never
    included.
    """
    candidates: list[Path] = []
    for path in sorted(root.iterdir()):
        if not path.is_file():
            continue
        for base in active_basenames:
            if path.name != base and path.name.startswith(base + "."):
                candidates.append(path)
                break
    return candidates


def run(*, dry_run: bool = False) -> tuple[int, int, int, int]:
    """Scans LOG_DIR and archives every rotated backup found.

    Returns (archived, skipped, failed, bytes_freed) counts for the run.
    `skipped` is always 0 here — every rotated backup found is a candidate,
    there's no age filter to skip on (see module docstring).
    """
    active = active_log_basenames(logging_setup.CONFIG_PATH)
    candidates = find_candidates(LOG_DIR, active)
    return archive_common.run_bundled(
        candidates,
        0,
        LOG_DIR,
        ARCHIVE_REMOTE_HOST,
        ARCHIVE_LOGS_REMOTE_PATH,
        "logs",
        dry_run=dry_run,
    )


def main() -> None:
    """Entry point: runs one archival pass and exits 1 if anything failed."""
    logging_setup.setup_logging()
    parser = argparse.ArgumentParser(
        description=(
            "Archives rotated log backups (never the active files) to "
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
