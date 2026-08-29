"""Shared HTTP client config for scripts that call a public JSON API directly
(zevent_api.py, zevent_donation_goals.py) — not main.py, which talks to
Twitch through twitchio's own client plus raw IRC, neither of which take a
custom User-Agent header the way a plain aiohttp GET does.

A single, env-overridable constant here means both scripts identify
themselves identically to whatever they call, and a deployment can change
its contact info in `.env` without touching either script.
"""

import os

from dotenv import load_dotenv

_ = load_dotenv()

USER_AGENT: str = os.environ.get(
    "HTTP_USER_AGENT",
    "zevent-analytics/0.1 (https://github.com/thoutmose/zevent-analytics)",
)
