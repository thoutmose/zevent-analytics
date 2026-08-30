from pathlib import Path

import pytest

import archive_logs

SAMPLE_LOGGING_YAML = """
handlers:
  info_file_handler:
    filename: logging/info.log
  debug_file_handler:
    filename: logging/debug.log
  console:
    class: logging.StreamHandler
"""


@pytest.fixture
def config_path(tmp_path: Path) -> Path:
    path = tmp_path / "logging.yaml"
    path.write_text(SAMPLE_LOGGING_YAML)
    return path


def test_active_log_basenames_reads_filenames_from_handlers(config_path: Path):
    basenames = archive_logs.active_log_basenames(config_path)

    assert basenames == {"info.log", "debug.log"}


def test_active_log_basenames_skips_handlers_without_filename(config_path: Path):
    basenames = archive_logs.active_log_basenames(config_path)

    # "console" has no filename key - a StreamHandler, not eligible for
    # archival regardless.
    assert "console" not in basenames


def test_find_candidates_excludes_active_bare_files(tmp_path: Path):
    (tmp_path / "info.log").write_text("active")
    (tmp_path / "debug.log").write_text("active")

    candidates = archive_logs.find_candidates(tmp_path, {"info.log", "debug.log"})

    assert candidates == []


def test_find_candidates_includes_rotated_backups(tmp_path: Path):
    (tmp_path / "info.log").write_text("active")
    (tmp_path / "info.log.1.gz").write_bytes(b"rotated-gz")
    (tmp_path / "info.log.2").write_text("rotated-plain")

    candidates = archive_logs.find_candidates(tmp_path, {"info.log"})

    assert set(candidates) == {tmp_path / "info.log.1.gz", tmp_path / "info.log.2"}


def test_find_candidates_excludes_unrelated_files(tmp_path: Path):
    (tmp_path / "dev_soak_test.py").write_text("not a log")
    (tmp_path / "dev_soak_test_report.log").write_text("not a rotated backup")

    candidates = archive_logs.find_candidates(tmp_path, {"info.log", "debug.log"})

    assert candidates == []


def test_find_candidates_ignores_subdirectories(tmp_path: Path):
    (tmp_path / "subdir").mkdir()
    (tmp_path / "subdir" / "info.log.1.gz").write_bytes(b"nested")

    candidates = archive_logs.find_candidates(tmp_path, {"info.log"})

    assert candidates == []
