"""Sets required env vars before any test module imports main.py/zevent_api.py.

main.py reads os.environ["TWITCH_CLIENT_ID"] (no default) at module import
time, so it must exist before collection ever imports that module — a
regular fixture runs too late.
"""

import os

_ = os.environ.setdefault("TWITCH_CLIENT_ID", "test-client-id")
_ = os.environ.setdefault("APP_ENV", "development")
