"""Shared tar+zstd bundle + verify + delete-on-confirm mechanics.

Used by both archive_parquet.py and archive_logs.py: bundles every
candidate file from one run into a single tar.zst archive, rsyncs that one
archive to a remote host, verifies its sha256 matches, and only then
deletes the original local files (plus the local bundle itself, which is
just a temporary artifact — never kept locally). Any failure at any step
(building the bundle, rsync, ssh, or a checksum mismatch) leaves every
original file untouched — the whole batch is naturally retried on the
caller's next run, since the filesystem itself is the retry state.

Bundling trades per-file retry granularity (a single bad file used to fail
just that file) for fewer round trips and one dated cold-storage archive
per run — a deliberate choice given Parquet files are already individually
zstd-compressed and rsync/ssh overhead dominated at scale.
"""

import hashlib
import logging
import subprocess
from datetime import UTC, datetime
from pathlib import Path

LOGGER: logging.Logger = logging.getLogger("archive_common")

# Reused on every ssh call: rsync's own, plus the remote mkdir and the
# verify sha256sum, all against the same host in one run.
SSH_MULTIPLEX_ARGS: list[str] = ["-o", "ControlMaster=auto", "-o", "ControlPersist=60s"]


def sha256_local(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _ensure_remote_dir(remote_host: str, remote_path: str) -> bool:
    result = subprocess.run(
        ["ssh", *SSH_MULTIPLEX_ARGS, remote_host, "mkdir", "-p", remote_path],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        LOGGER.error(
            "[archive] failed to create remote directory %s: %s",
            remote_path,
            result.stderr.strip(),
        )
        return False
    return True


def _build_bundle(candidates: list[Path], base_dir: Path, bundle_path: Path) -> bool:
    """Tars+zstd-compresses every candidate (relative to base_dir) into bundle_path.

    Returns False (and logs) on failure. Cleans up the intermediate file
    list either way.
    """
    filelist_path = bundle_path.with_suffix(bundle_path.suffix + ".filelist")
    relative_names = [str(p.relative_to(base_dir)) for p in candidates]
    _ = filelist_path.write_text("\n".join(relative_names) + "\n")
    try:
        result = subprocess.run(
            [
                "tar",
                "--zstd",
                "-cf",
                str(bundle_path),
                "-C",
                str(base_dir),
                "--files-from",
                str(filelist_path),
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            LOGGER.error(
                "[archive] failed to build bundle %s: %s",
                bundle_path,
                result.stderr.strip(),
            )
            return False
        return True
    finally:
        filelist_path.unlink(missing_ok=True)


def run_bundled(
    candidates: list[Path],
    skipped: int,
    base_dir: Path,
    remote_host: str,
    remote_path: str,
    bundle_prefix: str,
    *,
    dry_run: bool = False,
) -> tuple[int, int, int, int]:
    """Bundles every file in `candidates` into one tar.zst and archives it.

    Returns (archived, skipped, failed, bytes_freed). If any step fails,
    the whole batch is counted as failed and every original file is left
    untouched — see module docstring for the all-or-nothing trade-off.
    """
    if not candidates:
        LOGGER.info(
            "[archive] run complete: 0 archived, %d skipped, 0 failed, 0 bytes freed",
            skipped,
        )
        return 0, skipped, 0, 0

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    bundle_name = f"{bundle_prefix}-{timestamp}.tar.zst"

    if dry_run:
        total_bytes = sum(p.stat().st_size for p in candidates)
        for path in candidates:
            LOGGER.info("[archive][dry-run] would bundle %s", path)
        LOGGER.info(
            "[archive][dry-run] would archive %d files (%d bytes) -> %s:%s/%s",
            len(candidates),
            total_bytes,
            remote_host,
            remote_path,
            bundle_name,
        )
        LOGGER.info(
            "[archive] run complete: %d archived, %d skipped, 0 failed, 0 bytes freed",
            len(candidates),
            skipped,
        )
        return len(candidates), skipped, 0, 0

    if not _ensure_remote_dir(remote_host, remote_path):
        return 0, skipped, len(candidates), 0

    bundle_path = base_dir / f".{bundle_name}.tmp"
    try:
        if not _build_bundle(candidates, base_dir, bundle_path):
            return 0, skipped, len(candidates), 0

        local_digest = sha256_local(bundle_path)
        bundle_size = bundle_path.stat().st_size
        remote_target = f"{remote_path.rstrip('/')}/{bundle_name}"

        rsync_result = subprocess.run(
            [
                "rsync",
                "-a",
                "--checksum",
                "-e",
                f"ssh {' '.join(SSH_MULTIPLEX_ARGS)}",
                str(bundle_path),
                f"{remote_host}:{remote_target}",
            ],
            capture_output=True,
            text=True,
        )
        if rsync_result.returncode != 0:
            LOGGER.error(
                "[archive] rsync failed for bundle %s: %s",
                bundle_name,
                rsync_result.stderr.strip(),
            )
            return 0, skipped, len(candidates), 0

        ssh_result = subprocess.run(
            ["ssh", *SSH_MULTIPLEX_ARGS, remote_host, "sha256sum", remote_target],
            capture_output=True,
            text=True,
        )
        if ssh_result.returncode != 0:
            LOGGER.error(
                "[archive] could not verify %s on %s: %s",
                remote_target,
                remote_host,
                ssh_result.stderr.strip(),
            )
            return 0, skipped, len(candidates), 0

        stdout_tokens = ssh_result.stdout.split()
        if not stdout_tokens:
            LOGGER.error(
                "[archive] could not parse sha256sum output for %s: %r",
                remote_target,
                ssh_result.stdout,
            )
            return 0, skipped, len(candidates), 0
        remote_digest = stdout_tokens[0]
        if remote_digest != local_digest:
            LOGGER.error(
                "[archive] checksum mismatch for bundle %s: local=%s remote=%s"
                " - leaving originals in place",
                bundle_name,
                local_digest,
                remote_digest,
            )
            return 0, skipped, len(candidates), 0

        bytes_freed = 0
        failed = 0
        for path in candidates:
            try:
                size = path.stat().st_size
                path.unlink()
                bytes_freed += size
            except OSError:
                LOGGER.exception(
                    "[archive] bundle %s verified but failed to delete original %s",
                    bundle_name,
                    path,
                )
                failed += 1
        archived = len(candidates) - failed

        LOGGER.info(
            "[archive] archived %d files -> %s:%s (sha256 verified, %d bytes"
            " bundled), deleted originals",
            archived,
            remote_host,
            remote_target,
            bundle_size,
        )
        LOGGER.info(
            "[archive] run complete: %d archived, %d skipped, %d failed,"
            " %d bytes freed",
            archived,
            skipped,
            failed,
            bytes_freed,
        )
        return archived, skipped, failed, bytes_freed
    finally:
        bundle_path.unlink(missing_ok=True)
