from datetime import UTC, date, datetime

import pytest

import nifi_client


def test_json_default_formats_datetime_with_microseconds():
    dt = datetime(2026, 8, 28, 21, 4, 59, tzinfo=UTC)
    assert nifi_client._json_default(dt) == "2026-08-28T21:04:59.000000+00:00"


def test_json_default_formats_date():
    assert nifi_client._json_default(date(2026, 8, 28)) == "2026-08-28"


def test_json_default_falls_back_to_str():
    assert nifi_client._json_default(42) == "42"


async def test_push_batch_is_noop_without_webhook_url(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(nifi_client, "NIFI_WEBHOOK_URL", None)

    # Would raise (no server to POST to) if push_batch didn't short-circuit
    # before opening a session.
    await nifi_client.push_batch("main.py", "live_chat", [{"channel": "x"}])


async def test_push_batch_is_noop_with_empty_rows(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(nifi_client, "NIFI_WEBHOOK_URL", "http://example.invalid")

    await nifi_client.push_batch("main.py", "live_chat", [])


def test_push_batch_background_does_not_track_task_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(nifi_client, "NIFI_WEBHOOK_URL", None)

    nifi_client.push_batch_background("main.py", "live_chat", [{"channel": "x"}])

    assert len(nifi_client._pending_pushes) == 0
