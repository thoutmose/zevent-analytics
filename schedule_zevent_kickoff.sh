#!/usr/bin/env bash
# One-shot Zevent kickoff scheduler. Run this directly on srv-prod, inside
# this checkout, a few minutes before 20:00 Europe/Paris on the day the
# event starts. It installs two crontab entries (both under CRON_TZ=
# Europe/Paris, so they fire at true Europe/Paris wall-clock time regardless
# of the server's own system timezone — requires Debian/Ubuntu's stock cron,
# which honors CRON_TZ; this will silently use the system timezone instead
# on a cron implementation that doesn't):
#
#   1. Restart, ONCE TODAY at 20:00 — `docker compose restart nifi` (same
#      command cd.yml's own deploy step uses) plus a kill+relaunch of
#      emote_catalog.py, which has no systemd unit (see README.md's
#      "Running as a service") and is otherwise only ever started by hand.
#      Pinned to today's day-of-month/month, so it fires once and then
#      never matches again until the same date next year.
#   2. dbt build, every hour on the hour, recurring indefinitely via
#      run_dbt.sh (the same script the "dbt (manual)" GitHub Actions
#      workflow invokes) — until you remove it yourself (`crontab -e`).
#
# Safe to re-run: both jobs live in one marked block, stripped and
# reinserted each time, so re-running never duplicates entries — it just
# recomputes today's date.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"
mkdir -p logging

UV_BIN="/home/thoutmose/.local/bin/uv"

BLOCK_START="# --- zevent kickoff schedule (managed by schedule_zevent_kickoff.sh) ---"
BLOCK_END="# --- end zevent kickoff schedule ---"

# Today's date in Europe/Paris, unpadded (GNU date's %-d/%-m) so cron never
# has to parse a leading-zero field.
TODAY_DOM="$(TZ=Europe/Paris date +%-d)"
TODAY_MON="$(TZ=Europe/Paris date +%-m)"

RESTART_CMD="cd $REPO_DIR && docker compose --env-file .env.nifi restart nifi >> logging/cron_restart.log 2>&1; pkill -f 'uv run emote_catalog.py' || true; sleep 2; setsid $UV_BIN run emote_catalog.py < /dev/null >> logging/cron_emote_catalog.log 2>&1 &"
RESTART_LINE="0 20 $TODAY_DOM $TODAY_MON * $RESTART_CMD"

DBT_CMD="cd $REPO_DIR && ./run_dbt.sh >> logging/cron_dbt.log 2>&1"
DBT_LINE="0 * * * * $DBT_CMD"

echo "== Installing crontab block =="
existing="$(crontab -l 2>/dev/null || true)"
if [[ -n "$existing" ]]; then
    # Drop any previous run's block (inclusive of both markers) before
    # reinserting a fresh one below.
    updated="$(printf '%s\n' "$existing" | awk -v s="$BLOCK_START" -v e="$BLOCK_END" '
        $0==s {skip=1}
        !skip {print}
        $0==e {skip=0}
    ')"
else
    updated=""
fi

{
    if [[ -n "$updated" ]]; then
        printf '%s\n' "$updated"
    fi
    printf '%s\n' "$BLOCK_START"
    printf '%s\n' "CRON_TZ=Europe/Paris"
    printf '%s\n' "$RESTART_LINE"
    printf '%s\n' "$DBT_LINE"
    printf '%s\n' "$BLOCK_END"
} | crontab -

echo "  -> restart (once today 20:00 Europe/Paris): $RESTART_LINE"
echo "  -> dbt build (hourly, Europe/Paris):         $DBT_LINE"
echo
echo "Done. Current crontab:"
crontab -l
