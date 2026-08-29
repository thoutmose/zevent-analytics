import importlib

import pytest

import http_config


def test_user_agent_default():
    _ = importlib.reload(http_config)
    assert http_config.USER_AGENT.startswith("twitch-analytics/0.1")


def test_user_agent_env_override(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HTTP_USER_AGENT", "custom-agent/1.0")
    _ = importlib.reload(http_config)

    assert http_config.USER_AGENT == "custom-agent/1.0"

    monkeypatch.delenv("HTTP_USER_AGENT", raising=False)
    _ = importlib.reload(http_config)
