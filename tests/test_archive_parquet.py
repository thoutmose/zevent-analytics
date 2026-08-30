import hashlib
import os
import subprocess
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import archive_common
import archive_parquet

REAL_RUN = subprocess.run


def _write_parquet(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _age_file(path: Path, seconds: float) -> None:
    old = path.stat().st_mtime - seconds
    os.utime(path, (old, old))


class FakeRun:
    """Lets real `tar` calls through; fakes rsync/ssh and, when
    `verify_ok` is True, replies to the sha256sum check with the actual
    digest of whatever bundle rsync was given, so verification succeeds."""

    def __init__(self, verify_ok: bool = True):
        self.calls: list[list[str]] = []
        self.verify_ok = verify_ok
        self.rsync_returncode = 0

    def __call__(self, argv: list[str], **kwargs) -> subprocess.CompletedProcess:
        if argv[0] == "tar":
            return REAL_RUN(argv, **kwargs)
        self.calls.append(argv)
        if argv[0] == "rsync":
            return subprocess.CompletedProcess(
                argv, self.rsync_returncode, stdout="", stderr=""
            )
        if "mkdir" in argv:
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")
        if "sha256sum" in argv:
            if not self.verify_ok:
                return subprocess.CompletedProcess(argv, 0, stdout="0" * 64, stderr="")
            rsync_call = next(c for c in self.calls if c[0] == "rsync")
            digest = hashlib.sha256(Path(rsync_call[-2]).read_bytes()).hexdigest()
            return subprocess.CompletedProcess(argv, 0, stdout=f"{digest}\n", stderr="")
        raise AssertionError(f"unexpected subprocess call: {argv}")


@pytest.fixture
def fake_run(monkeypatch: pytest.MonkeyPatch) -> FakeRun:
    fake = FakeRun()
    monkeypatch.setattr(archive_common.subprocess, "run", fake)
    return fake


def test_find_candidates_skips_files_younger_than_min_age(tmp_path: Path):
    fresh = tmp_path / "fresh.parquet"
    old = tmp_path / "old.parquet"
    _write_parquet(fresh, [{"n": 1}])
    _write_parquet(old, [{"n": 1}])
    _age_file(old, 1000)

    candidates, skipped = archive_parquet.find_candidates(tmp_path, 600)

    assert candidates == [old]
    assert skipped == 1


def test_find_candidates_skips_invalid_parquet(tmp_path: Path, caplog):
    bad = tmp_path / "bad.parquet"
    bad.write_bytes(b"not actually parquet")
    _age_file(bad, 1000)

    candidates, skipped = archive_parquet.find_candidates(tmp_path, 600)

    assert candidates == []
    assert skipped == 1
    assert "not yet readable as Parquet" in caplog.text


def test_find_candidates_recurses_subdirs_preserving_relative_path(tmp_path: Path):
    chat = tmp_path / "live_chat" / "a.parquet"
    metadata = tmp_path / "metadata" / "b.parquet"
    _write_parquet(chat, [{"n": 1}])
    _write_parquet(metadata, [{"n": 1}])
    _age_file(chat, 1000)
    _age_file(metadata, 1000)

    candidates, skipped = archive_parquet.find_candidates(tmp_path, 600)

    assert set(candidates) == {chat, metadata}
    assert skipped == 0


def test_run_bundles_and_deletes_eligible_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_run: FakeRun
):
    monkeypatch.setattr(archive_parquet, "PARQUET_OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(archive_parquet, "ARCHIVE_MIN_AGE_SECONDS", 600)
    local = tmp_path / "live_chat" / "a.parquet"
    _write_parquet(local, [{"n": 1}])
    _age_file(local, 1000)

    archived, skipped, failed, bytes_freed = archive_parquet.run(dry_run=False)

    assert (archived, skipped, failed) == (1, 0, 0)
    assert bytes_freed > 0
    assert not local.exists()


def test_run_reports_failure_when_transfer_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_run: FakeRun
):
    monkeypatch.setattr(archive_parquet, "PARQUET_OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(archive_parquet, "ARCHIVE_MIN_AGE_SECONDS", 600)
    local = tmp_path / "live_chat" / "a.parquet"
    _write_parquet(local, [{"n": 1}])
    _age_file(local, 1000)
    fake_run.rsync_returncode = 1

    archived, skipped, failed, bytes_freed = archive_parquet.run(dry_run=False)

    assert (archived, skipped, failed) == (0, 0, 1)
    assert bytes_freed == 0
    assert local.exists()


def test_run_skips_transfer_entirely_in_dry_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_run: FakeRun
):
    monkeypatch.setattr(archive_parquet, "PARQUET_OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(archive_parquet, "ARCHIVE_MIN_AGE_SECONDS", 600)
    local = tmp_path / "live_chat" / "a.parquet"
    _write_parquet(local, [{"n": 1}])
    _age_file(local, 1000)

    archived, skipped, failed, bytes_freed = archive_parquet.run(dry_run=True)

    assert (archived, skipped, failed) == (1, 0, 0)
    assert bytes_freed == 0
    assert local.exists()
    assert fake_run.calls == []
