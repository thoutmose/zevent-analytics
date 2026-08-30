import hashlib
import subprocess
from pathlib import Path

import pytest

import archive_common

REAL_RUN = subprocess.run


class FakeRun:
    """Lets real `tar` calls through (safe, local, deterministic) while
    faking rsync/ssh (the actual network operations). Set
    `sha256sum_stdout_from_bundle=True` to have the sha256sum reply
    computed on the fly from whatever bundle file rsync was just given,
    so the verify step naturally succeeds without hardcoding a digest."""

    def __init__(self):
        self.calls: list[list[str]] = []
        self.rsync_returncode = 0
        self.rsync_stderr = ""
        self.mkdir_returncode = 0
        self.mkdir_stderr = ""
        self.sha256sum_returncode = 0
        self.sha256sum_stdout = ""
        self.sha256sum_stderr = ""
        self.sha256sum_stdout_from_bundle = False

    def __call__(self, argv: list[str], **kwargs) -> subprocess.CompletedProcess:
        if argv[0] == "tar":
            return REAL_RUN(argv, **kwargs)
        self.calls.append(argv)
        if argv[0] == "rsync":
            if self.sha256sum_stdout_from_bundle:
                local_bundle = Path(argv[-2])
                digest = hashlib.sha256(local_bundle.read_bytes()).hexdigest()
                self.sha256sum_stdout = f"{digest}  remote/path\n"
            return subprocess.CompletedProcess(
                argv, self.rsync_returncode, stdout="", stderr=self.rsync_stderr
            )
        if "mkdir" in argv:
            return subprocess.CompletedProcess(
                argv, self.mkdir_returncode, stdout="", stderr=self.mkdir_stderr
            )
        if "sha256sum" in argv:
            return subprocess.CompletedProcess(
                argv,
                self.sha256sum_returncode,
                stdout=self.sha256sum_stdout,
                stderr=self.sha256sum_stderr,
            )
        raise AssertionError(f"unexpected subprocess call: {argv}")


@pytest.fixture
def fake_run(monkeypatch: pytest.MonkeyPatch) -> FakeRun:
    fake = FakeRun()
    monkeypatch.setattr(archive_common.subprocess, "run", fake)
    return fake


def test_run_bundled_empty_candidates_is_a_noop(tmp_path: Path, fake_run: FakeRun):
    archived, skipped, failed, bytes_freed = archive_common.run_bundled(
        [], 3, tmp_path, "srv-services", "archive", "parquet", dry_run=False
    )

    assert (archived, skipped, failed, bytes_freed) == (0, 3, 0, 0)
    assert fake_run.calls == []


def test_run_bundled_dry_run_makes_no_calls_and_keeps_files(
    tmp_path: Path, fake_run: FakeRun
):
    a = tmp_path / "a.parquet"
    a.write_bytes(b"data")

    archived, skipped, failed, bytes_freed = archive_common.run_bundled(
        [a], 0, tmp_path, "srv-services", "archive", "parquet", dry_run=True
    )

    assert (archived, skipped, failed, bytes_freed) == (1, 0, 0, 0)
    assert a.exists()
    assert fake_run.calls == []


def test_run_bundled_success_deletes_originals_and_temp_bundle(
    tmp_path: Path, fake_run: FakeRun
):
    a = tmp_path / "live_chat" / "a.parquet"
    b = tmp_path / "metadata" / "b.parquet"
    a.parent.mkdir(parents=True)
    b.parent.mkdir(parents=True)
    a.write_bytes(b"chat-data")
    b.write_bytes(b"metadata-data")
    fake_run.sha256sum_stdout_from_bundle = True

    archived, skipped, failed, bytes_freed = archive_common.run_bundled(
        [a, b], 0, tmp_path, "srv-services", "archive", "parquet", dry_run=False
    )

    assert (archived, skipped, failed) == (2, 0, 0)
    assert bytes_freed == len(b"chat-data") + len(b"metadata-data")
    assert not a.exists()
    assert not b.exists()
    # the temp bundle and its filelist must not survive the run
    assert list(tmp_path.glob(".*")) == []


def test_run_bundled_names_the_bundle_with_the_given_prefix(
    tmp_path: Path, fake_run: FakeRun
):
    a = tmp_path / "a.parquet"
    a.write_bytes(b"data")
    fake_run.sha256sum_stdout_from_bundle = True

    archive_common.run_bundled(
        [a], 0, tmp_path, "srv-services", "archive", "parquet", dry_run=False
    )

    rsync_call = next(c for c in fake_run.calls if c[0] == "rsync")
    remote_arg = rsync_call[-1]
    assert "srv-services:archive/parquet-" in remote_arg
    assert remote_arg.endswith(".tar.zst")


def test_run_bundled_rsync_failure_keeps_all_originals(
    tmp_path: Path, fake_run: FakeRun
):
    a = tmp_path / "a.parquet"
    a.write_bytes(b"data")
    fake_run.rsync_returncode = 1
    fake_run.rsync_stderr = "connection refused"

    archived, skipped, failed, bytes_freed = archive_common.run_bundled(
        [a], 0, tmp_path, "srv-services", "archive", "parquet", dry_run=False
    )

    assert (archived, failed) == (0, 1)
    assert a.exists()
    assert not any("sha256sum" in c for c in fake_run.calls)
    # temp bundle must be cleaned up even on failure
    assert list(tmp_path.glob(".*")) == []


def test_run_bundled_checksum_mismatch_keeps_all_originals(
    tmp_path: Path, fake_run: FakeRun
):
    a = tmp_path / "a.parquet"
    a.write_bytes(b"data")
    fake_run.sha256sum_stdout = "0" * 64 + "  remote/path\n"

    archived, skipped, failed, bytes_freed = archive_common.run_bundled(
        [a], 0, tmp_path, "srv-services", "archive", "parquet", dry_run=False
    )

    assert (archived, failed) == (0, 1)
    assert a.exists()


def test_run_bundled_remote_mkdir_failure_keeps_all_originals(
    tmp_path: Path, fake_run: FakeRun
):
    a = tmp_path / "a.parquet"
    a.write_bytes(b"data")
    fake_run.mkdir_returncode = 1
    fake_run.mkdir_stderr = "permission denied"

    archived, skipped, failed, bytes_freed = archive_common.run_bundled(
        [a], 0, tmp_path, "srv-services", "archive", "parquet", dry_run=False
    )

    assert (archived, failed) == (0, 1)
    assert a.exists()
    assert not any(c[0] == "rsync" for c in fake_run.calls)


def test_build_bundle_preserves_relative_paths_and_content(tmp_path: Path):
    """Direct test of _build_bundle (no rsync/ssh involved) - confirms the
    real tar+zstd output round-trips the original relative paths/content.

    Python's stdlib tarfile doesn't understand zstd, so this shells out to
    the same `tar` CLI the implementation itself uses, to list/extract."""
    a = tmp_path / "live_chat" / "a.parquet"
    a.parent.mkdir(parents=True)
    a.write_bytes(b"chat-payload")

    bundle_path = tmp_path / "bundle.tar.zst"
    ok = archive_common._build_bundle([a], tmp_path, bundle_path)

    assert ok is True
    assert bundle_path.exists()

    listing = subprocess.run(
        ["tar", "--zstd", "-tf", str(bundle_path)],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "live_chat/a.parquet" in listing.stdout

    extract_dir = tmp_path / "extracted"
    extract_dir.mkdir()
    subprocess.run(
        ["tar", "--zstd", "-xf", str(bundle_path), "-C", str(extract_dir)],
        check=True,
    )
    assert (extract_dir / "live_chat" / "a.parquet").read_bytes() == b"chat-payload"

    # filelist temp file must not survive
    assert not bundle_path.with_suffix(bundle_path.suffix + ".filelist").exists()
