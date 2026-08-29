"""Centralized logging configuration, loaded from logging.yaml.

Call setup_logging() once at the start of each entry point (main.py,
zevent_api.py) instead of ad hoc logging.basicConfig()/twitchio.utils.setup_logging()
calls, so every module's logs (zevent_extractor, nifi_client, zevent_api,
twitchio.*, aiohttp) land in the same place: colored console output plus a set
of rotating files under logging/ (info/warn/errors/critical/debug/logs.log).
"""

import gzip
import logging.config
import logging.handlers
import os
import shutil
from pathlib import Path

import yaml

CONFIG_PATH: Path = Path(__file__).parent / "logging.yaml"
LOG_DIR: Path = Path(__file__).parent / "logging"


class CompressedRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """RotatingFileHandler that gzips each rotated-out file.

    Long-running processes (the ~55h Zevent event) roll over many times;
    plain RotatingFileHandler leaves every backup uncompressed. Referenced
    from logging.yaml as logging_setup.CompressedRotatingFileHandler.
    """

    def rotation_filename(self, default_name: str) -> str:
        return default_name + ".gz"

    def rotate(self, source: str, dest: str) -> None:
        with open(source, "rb") as f_in, gzip.open(dest, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(source)


def setup_logging(env: str | None = None) -> None:
    """Configures logging from logging.yaml for the given profile.

    `env` selects a profile from logging.yaml's `loggers:` section
    ("development": every handler, or "production": console + info.log
    only) and applies its level/handlers to the root logger — every module
    logger in this project propagates to root by default, so this is what
    actually determines where log output goes. Defaults to the APP_ENV
    environment variable, falling back to "development" if unset.

    Raises KeyError if `env` doesn't match a profile defined in logging.yaml.
    """
    env = env or os.environ.get("APP_ENV", "development")
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    with CONFIG_PATH.open() as f:
        config = yaml.safe_load(f)

    profile = config.pop("loggers")[env]
    config["root"] = {
        "level": profile["level"],
        "handlers": profile["handlers"],
        "propagate": True,
    }

    logging.config.dictConfig(config)
