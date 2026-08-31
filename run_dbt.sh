#!/usr/bin/env bash
# Runs the dbt project against this checkout's own .env — invoked only via
# the dbt (manual) GitHub Actions workflow (.github/workflows/dbt.yml,
# workflow_dispatch only — never on push/PR/schedule) or by hand.
#
# Deliberately NOT wired into cd.yml's automatic deploy chain: dbt
# transforms data that's continuously landing regardless of whether a
# deploy just happened, so tying it to every deploy would be both
# unnecessary (nothing about a code deploy requires an immediate rebuild)
# and a surprise trigger nobody asked for. Triggering it is a deliberate,
# separate decision every time.
#
# Runs as thoutmose (via `sudo -u thoutmose` from the deploy user — see
# DEPLOYMENT.md's ".env.postgres on srv-prod" section for the sudoers grant
# this requires), not as deploy: dbt's profiles.yml reads POSTGRES_HOST /
# PGBOUNCER_PORT / POSTGRES_DB / POSTGRES_USER / POSTGRES_PASSWORD from the
# main .env, the same one deploy must never be able to read (see
# docker-compose.yml's env-file split for why) — thoutmose already can.
set -euo pipefail
cd "$(dirname "$0")"

set -a
source .env
set +a

export DBT_TARGET="${DBT_TARGET:-prod}"

uv run --group dbt --project-dir dbt --profiles-dir dbt dbt deps
uv run --group dbt --project-dir dbt --profiles-dir dbt dbt build
