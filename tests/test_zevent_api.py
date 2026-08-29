import json
from dataclasses import asdict
from pathlib import Path

import pytest

import zevent_api
from zevent_api import RawSnapshotDict

RAW_API_SAMPLE: RawSnapshotDict = {
    "websiteMode": "day",
    "donationAmount": {"number": 1234567.89},
    "viewersCount": {"number": 296175},
    "live": [
        {
            "twitch_id": "123456",
            "twitch": "somestreamer",
            "display": "SomeStreamer",
            "profileUrl": "https://twitch.tv/somestreamer",
            "online": True,
            "game": "Just Chatting",
            "viewersAmount": {"number": 4200},
            "donationAmount": {"number": 999.5},
        },
        {
            "twitch_id": "654321",
            "twitch": "offlinestreamer",
            "display": "OfflineStreamer",
            "profileUrl": "https://twitch.tv/offlinestreamer",
            "online": False,
            "game": "",
            "viewersAmount": {"number": 0},
            "donationAmount": {"number": 0},
        },
    ],
}


def test_parse_snapshot_maps_event_level_fields():
    snapshot = zevent_api._parse_snapshot(RAW_API_SAMPLE)

    assert snapshot.website_mode == "day"
    assert snapshot.total_donation_amount_eur == 1234567.89
    assert snapshot.total_viewer_count == 296175
    assert len(snapshot.streamers) == 2


def test_parse_snapshot_maps_per_streamer_fields():
    snapshot = zevent_api._parse_snapshot(RAW_API_SAMPLE)
    online = snapshot.streamers[0]

    assert online.twitch_id == "123456"
    assert online.twitch_login == "somestreamer"
    assert online.display_name == "SomeStreamer"
    assert online.online is True
    assert online.viewer_count == 4200
    assert online.donation_amount_eur == 999.5


def test_parse_snapshot_empty_roster():
    data: RawSnapshotDict = {**RAW_API_SAMPLE, "live": []}
    snapshot = zevent_api._parse_snapshot(data)

    assert snapshot.streamers == []


def test_write_checkpoint_roundtrip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    checkpoint_path = tmp_path / "checkpoint.json"
    monkeypatch.setattr(zevent_api, "CHECKPOINT_PATH", checkpoint_path)

    snapshot = zevent_api._parse_snapshot(RAW_API_SAMPLE)
    zevent_api._write_checkpoint(snapshot)

    assert checkpoint_path.exists()
    on_disk = json.loads(checkpoint_path.read_text())
    assert on_disk == asdict(snapshot)


def test_write_checkpoint_creates_parent_dirs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    checkpoint_path = tmp_path / "nested" / "dir" / "checkpoint.json"
    monkeypatch.setattr(zevent_api, "CHECKPOINT_PATH", checkpoint_path)

    zevent_api._write_checkpoint(zevent_api._parse_snapshot(RAW_API_SAMPLE))

    assert checkpoint_path.exists()


def test_write_checkpoint_no_leftover_tmp_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    checkpoint_path = tmp_path / "checkpoint.json"
    monkeypatch.setattr(zevent_api, "CHECKPOINT_PATH", checkpoint_path)

    zevent_api._write_checkpoint(zevent_api._parse_snapshot(RAW_API_SAMPLE))

    assert not checkpoint_path.with_suffix(".json.tmp").exists()
