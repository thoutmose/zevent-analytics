from pathlib import Path

import pytest

import nifi_client

import main


def test_chunked_splits_into_full_chunks():
    assert main._chunked(["a", "b", "c", "d"], 2) == [["a", "b"], ["c", "d"]]


def test_chunked_leaves_remainder_in_last_chunk():
    assert main._chunked(["a", "b", "c"], 2) == [["a", "b"], ["c"]]


def test_chunked_chunk_larger_than_input():
    assert main._chunked(["a", "b"], 100) == [["a", "b"]]


def test_chunked_empty_input():
    assert main._chunked([], 100) == []


def test_privmsg_re_matches_message_with_tags():
    line = (
        "@display-name=SomeUser;user-id=42;tmi-sent-ts=1700000000000 "
        ":someuser!someuser@someuser.tmi.twitch.tv PRIVMSG #zevent :hello chat!"
    )
    match = main.PRIVMSG_RE.match(line)

    assert match is not None
    assert match["nick"] == "someuser"
    assert match["channel"] == "zevent"
    assert match["text"] == "hello chat!"
    assert "display-name=SomeUser" in match["tags"]


def test_privmsg_re_matches_message_without_tags():
    line = ":someuser!someuser@someuser.tmi.twitch.tv PRIVMSG #zevent :no tags here"
    match = main.PRIVMSG_RE.match(line)

    assert match is not None
    assert match["tags"] is None
    assert match["channel"] == "zevent"
    assert match["text"] == "no tags here"


def test_privmsg_re_does_not_match_non_privmsg_line():
    line = ":tmi.twitch.tv 001 justinfan12345 :Welcome, GLHF!"
    assert main.PRIVMSG_RE.match(line) is None


def test_batch_parquet_writer_flush_is_noop_on_empty_buffer(tmp_path: Path):
    writer = main.BatchParquetWriter(tmp_path, "test", max_rows=10)
    writer.flush()

    assert list(tmp_path.iterdir()) == []
    assert writer.part == 1


def test_batch_parquet_writer_rolls_over_at_max_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(nifi_client, "NIFI_WEBHOOK_URL", None)
    writer = main.BatchParquetWriter(tmp_path, "test", max_rows=2)
    writer.add({"n": 1})
    writer.add({"n": 2})

    assert writer.rows == []
    assert writer.part == 2
    assert len(list(tmp_path.glob("*.parquet"))) == 1
