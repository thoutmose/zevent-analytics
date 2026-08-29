import json
from dataclasses import asdict
from pathlib import Path

import pytest

import zevent_donation_goals as goals_mod
from zevent_donation_goals import (
    DonationGoalDict,
    EventDict,
    ParticipationOverviewDict,
)

SAMPLE_PARTICIPATION: ParticipationOverviewDict = {
    "id": "019fc7a3-9216-7aa9-b100-29c2caf533ed",
    "name": "SomeStreamer",
    "donation_goals_count": 2,
    "socials": {"twitch": {"id": "44842076", "login": "somestreamer"}},
}

SAMPLE_GOAL: DonationGoalDict = {
    "id": "01a0259f-8dd2-774b-a269-d3e9743cb920",
    "name": "Tier list un peu border",
    "amount": 300000,
    "category": "recurent",
    "accomplished": True,
    "participation_id": "019fc7a3-9216-7aa9-b100-29c2caf533ed",
}


def test_pick_latest_zevent_ignores_non_zevent_events():
    events: list[EventDict] = [
        {
            "id": "e1",
            "name": "Birds Of Prey #6",
            "schedule": {
                "start": "2026-11-08T13:00:00Z",
                "end": "2026-11-09T00:00:00Z",
            },
        },
        {
            "id": "e2",
            "name": "ZEvent 2025",
            "schedule": {
                "start": "2025-09-04T10:00:00Z",
                "end": "2025-09-08T00:00:00Z",
            },
        },
        {
            "id": "e3",
            "name": "ZEvent 2026",
            "schedule": {
                "start": "2026-09-03T18:00:00Z",
                "end": "2026-09-07T00:00:00Z",
            },
        },
        {
            "id": "e4",
            "name": "ZEvent 2019",
            "schedule": {
                "start": "2019-09-20T16:00:00Z",
                "end": "2019-09-23T00:00:00Z",
            },
        },
    ]

    latest = goals_mod._pick_latest_zevent(events)

    assert latest["name"] == "ZEvent 2026"


def test_pick_latest_zevent_raises_without_any_zevent():
    events: list[EventDict] = [
        {
            "id": "e1",
            "name": "Birds Of Prey #6",
            "schedule": {
                "start": "2026-11-08T13:00:00Z",
                "end": "2026-11-09T00:00:00Z",
            },
        }
    ]

    try:
        _ = goals_mod._pick_latest_zevent(events)
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected RuntimeError")


def test_build_row_maps_fields_and_converts_cents_to_euros():
    row = goals_mod._build_row(SAMPLE_PARTICIPATION, SAMPLE_GOAL, "2026-08-29T00:00:00")

    assert row.participation_id == "019fc7a3-9216-7aa9-b100-29c2caf533ed"
    assert row.streamer_name == "SomeStreamer"
    assert row.twitch_login == "somestreamer"
    assert row.twitch_id == "44842076"
    assert row.goal_id == "01a0259f-8dd2-774b-a269-d3e9743cb920"
    assert row.goal_name == "Tier list un peu border"
    assert row.goal_amount_eur == 3000.0
    assert row.goal_category == "recurent"


def test_build_row_drops_completion_field():
    row = goals_mod._build_row(SAMPLE_PARTICIPATION, SAMPLE_GOAL, "2026-08-29T00:00:00")

    assert not hasattr(row, "accomplished")
    assert "accomplished" not in asdict(row)


def test_build_row_handles_missing_socials():
    participation: ParticipationOverviewDict = {
        "id": "p1",
        "name": "NoSocials",
        "donation_goals_count": 1,
        "socials": {},
    }

    row = goals_mod._build_row(participation, SAMPLE_GOAL, "2026-08-29T00:00:00")

    assert row.twitch_login is None
    assert row.twitch_id is None


def test_write_checkpoint_roundtrip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    checkpoint_path = tmp_path / "checkpoint.json"
    monkeypatch.setattr(goals_mod, "CHECKPOINT_PATH", checkpoint_path)

    row = goals_mod._build_row(SAMPLE_PARTICIPATION, SAMPLE_GOAL, "2026-08-29T00:00:00")
    goals_mod._write_checkpoint([row])

    on_disk = json.loads(checkpoint_path.read_text())
    assert on_disk == [asdict(row)]


def test_write_checkpoint_empty_list(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    checkpoint_path = tmp_path / "checkpoint.json"
    monkeypatch.setattr(goals_mod, "CHECKPOINT_PATH", checkpoint_path)

    goals_mod._write_checkpoint([])

    assert json.loads(checkpoint_path.read_text()) == []
