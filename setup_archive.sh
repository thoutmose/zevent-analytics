#!/usr/bin/env bash
# One-time (idempotent) machine setup for archive_parquet.py and
# archive_logs.py: generates a dedicated SSH key for ARCHIVE_REMOTE_HOST if
# one doesn't exist, adds a matching ~/.ssh/config alias, and installs both
# hourly cron jobs (both scripts share the same SSH access - only their
# remote destination directory differs). Safe to re-run — every step checks
# the current state first and skips what's already done, so running it
# again (e.g. on a second machine, or after a key rotation) never
# duplicates anything.
#
# Run once per machine that will run these scripts (dev, prod, or wherever
# the extractors actually run - see README.md "Running as a service"). See
# README.md "One-time SSH setup (per machine)" for what each step below
# does and why.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

REMOTE_HOST="${ARCHIVE_REMOTE_HOST:-srv-services}"
REMOTE_HOSTNAME="${ARCHIVE_REMOTE_HOSTNAME:-100.74.173.8}"
REMOTE_USER="${ARCHIVE_REMOTE_USER:-thoutmose}"
KEY_PATH="${ARCHIVE_SSH_KEY_PATH:-$HOME/.ssh/srv_services_ed25519}"
SSH_CONFIG="$HOME/.ssh/config"
UV_BIN="$(command -v uv)"

# Installs (or updates) one cron line, identified by its trailing marker
# comment so re-runs replace rather than duplicate it. $1: marker text
# (used verbatim as the crontab comment), $2: the command to run hourly.
install_cron_line() {
    local marker="# $1 (managed by setup_archive.sh)"
    local new_line="0 * * * * cd $(pwd) && $2  $marker"
    local existing updated
    existing="$(crontab -l 2>/dev/null || true)"
    if grep -qF "$marker" <<<"$existing"; then
        echo "already installed (updating in case the path/uv changed): $1"
        updated="$(grep -vF "$marker" <<<"$existing" || true)"
    else
        echo "installing: $1"
        updated="$existing"
    fi
    {
        if [[ -n "$updated" ]]; then
            printf '%s\n' "$updated"
        fi
        printf '%s\n' "$new_line"
    } | crontab -
    echo "  -> $new_line"
}

echo "== 1/5: SSH key =="
if [[ -f "$KEY_PATH" ]]; then
    echo "already exists: $KEY_PATH"
else
    ssh-keygen -t ed25519 -N "" -C "archive_parquet.py@$(hostname)" -f "$KEY_PATH"
    echo "generated: $KEY_PATH"
fi

echo
echo "== 2/5: ~/.ssh/config alias =="
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
touch "$SSH_CONFIG"
if grep -qx "Host $REMOTE_HOST" "$SSH_CONFIG" 2>/dev/null; then
    echo "already present: Host $REMOTE_HOST in $SSH_CONFIG"
else
    {
        echo ""
        echo "Host $REMOTE_HOST"
        echo "    HostName $REMOTE_HOSTNAME"
        echo "    User $REMOTE_USER"
        echo "    IdentityFile $KEY_PATH"
        echo "    IdentitiesOnly yes"
    } >> "$SSH_CONFIG"
    echo "added: Host $REMOTE_HOST -> $SSH_CONFIG"
fi
chmod 600 "$SSH_CONFIG"

echo
echo "== 3/5: verify SSH access =="
if ssh -o BatchMode=yes -o ConnectTimeout=5 "$REMOTE_HOST" 'echo OK' >/dev/null 2>&1; then
    echo "OK - key-based auth to $REMOTE_HOST already works"
else
    echo "Key-based auth to $REMOTE_HOST doesn't work yet - the public key"
    echo "isn't authorized there. Run this once (needs an existing way in:"
    echo "an already-authorized key, or a password from whoever manages"
    echo "that server):"
    echo
    echo "    ssh-copy-id -i $KEY_PATH.pub $REMOTE_USER@$REMOTE_HOSTNAME"
    echo
    echo "then re-run this script."
    exit 1
fi

echo
echo "== 4/5: cron jobs =="
install_cron_line "archive_parquet.py" "$UV_BIN run archive_parquet.py >> /dev/null 2>&1"
install_cron_line "archive_logs.py" "$UV_BIN run archive_logs.py >> /dev/null 2>&1"

echo
echo "== 5/5: logging profile (APP_ENV) =="
CURRENT_APP_ENV="$(grep -E '^APP_ENV=' .env 2>/dev/null | tail -1 | cut -d= -f2- || true)"
CURRENT_APP_ENV="${CURRENT_APP_ENV:-development (default, unset in .env)}"
echo "current: APP_ENV=$CURRENT_APP_ENV"
case "$(pwd)" in
    *-prod)
        if [[ "$CURRENT_APP_ENV" != "production" ]]; then
            echo "WARNING: this checkout's path ends in -prod but APP_ENV isn't"
            echo "  'production' - it'll log at DEBUG to five rotating files instead"
            echo "  of the quieter INFO/console+info.log production profile. Add"
            echo "  'APP_ENV=production' to .env if this is meant to be the real"
            echo "  production run (see README.md 'Logging')."
        fi
        ;;
    *)
        echo "(not a *-prod checkout path - development logging is expected here)"
        ;;
esac

echo
echo "Done. Current crontab:"
crontab -l
