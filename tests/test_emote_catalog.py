import json
from dataclasses import asdict
from pathlib import Path

import pytest

import emote_catalog as ec_mod


def test_chunked_splits_into_full_chunks():
    assert ec_mod._chunked(["a", "b", "c", "d"], 2) == [["a", "b"], ["c", "d"]]


def test_chunked_empty_input():
    assert ec_mod._chunked([], 100) == []


def test_parse_twitch_emotes_maps_fields():
    data = [{"id": "524589", "name": "zrtTap"}]
    entries = ec_mod._parse_twitch_emotes(
        data, "channel", "zerator", "2026-08-31T00:00:00"
    )

    assert entries == [
        ec_mod.EmoteCatalogEntry(
            "twitch", "channel", "zerator", "524589", "zrtTap", "2026-08-31T00:00:00"
        )
    ]


def test_parse_twitch_emotes_global_uses_sentinel_channel():
    data = [{"id": "1", "name": "EWCcrush"}]
    entries = ec_mod._parse_twitch_emotes(
        data, "global", ec_mod.GLOBAL_CHANNEL, "2026-08-31T00:00:00"
    )

    assert entries[0].scope == "global"
    assert entries[0].channel == ec_mod.GLOBAL_CHANNEL


def test_parse_7tv_emotes_maps_fields():
    emotes = [{"id": "01FS5ZCFG0000500DPPCXJWCP8", "name": "o7"}]
    entries = ec_mod._parse_7tv_emotes(
        emotes, "channel", "zerator", "2026-08-31T00:00:00"
    )

    assert entries == [
        ec_mod.EmoteCatalogEntry(
            "7tv",
            "channel",
            "zerator",
            "01FS5ZCFG0000500DPPCXJWCP8",
            "o7",
            "2026-08-31T00:00:00",
        )
    ]


def test_parse_bttv_emotes_uses_code_not_name():
    emotes = [{"id": "54fa8f1401e468494b85b537", "code": ":tf:"}]
    entries = ec_mod._parse_bttv_emotes(
        emotes, "global", ec_mod.GLOBAL_CHANNEL, "2026-08-31T00:00:00"
    )

    assert entries[0].emote_code == ":tf:"


def test_parse_ffz_emotes_stringifies_numeric_id():
    emotes = [{"id": 33355, "name": "FeelsBadMan"}]
    entries = ec_mod._parse_ffz_emotes(
        emotes, "channel", "zerator", "2026-08-31T00:00:00"
    )

    assert entries[0].emote_id == "33355"


def test_flatten_ffz_sets_walks_given_set_ids():
    body = {
        "sets": {
            "3": {"emoticons": [{"id": 9, "name": "ZrehplaR"}]},
            "1539687": {"emoticons": [{"id": 10, "name": "Other"}]},
        }
    }

    flat = ec_mod._flatten_ffz_sets(body, [3])

    assert flat == [{"id": 9, "name": "ZrehplaR"}]


def test_flatten_ffz_sets_missing_set_id_returns_empty():
    body = {"sets": {}}

    assert ec_mod._flatten_ffz_sets(body, [999]) == []


def test_write_checkpoint_roundtrip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    checkpoint_path = tmp_path / "checkpoint.json"
    monkeypatch.setattr(ec_mod, "CHECKPOINT_PATH", checkpoint_path)

    entry = ec_mod.EmoteCatalogEntry(
        "twitch", "global", ec_mod.GLOBAL_CHANNEL, "1", "Kappa", "2026-08-31T00:00:00"
    )
    ec_mod._write_checkpoint([entry])

    on_disk = json.loads(checkpoint_path.read_text())
    assert on_disk == [asdict(entry)]


def test_write_checkpoint_empty_list(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    checkpoint_path = tmp_path / "checkpoint.json"
    monkeypatch.setattr(ec_mod, "CHECKPOINT_PATH", checkpoint_path)

    ec_mod._write_checkpoint([])

    assert json.loads(checkpoint_path.read_text()) == []
