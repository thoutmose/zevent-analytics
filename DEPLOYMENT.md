# Deploying to srv-prod

CD ([`.github/workflows/cd.yml`](.github/workflows/cd.yml)) deploys **only
the NiFi stack** (`docker-compose.yml` + `drivers/`) to srv-prod — matching
`README.md`'s "srv-prod runs NiFi only". `main.py`/`zevent_api.py` aren't
deployed by this pipeline; run them wherever/whenever the event needs them.

It never runs off a bare `push`: it waits for the `CI` workflow on `main` to
finish successfully, and then still pauses for a **manual approval** before
touching the server (see step 3). A red build can never reach prod, and
nothing reaches prod unattended.

## One-time setup

### 1. Generate a dedicated deploy key

Don't reuse your personal SSH key. On your own machine:

```bash
ssh-keygen -t ed25519 -C "twitch-analytics-cd" -f ./srv-prod-deploy-key -N ""
```

This produces `srv-prod-deploy-key` (private) and `srv-prod-deploy-key.pub`
(public).

### 2. Provision a restricted deploy user on srv-prod

As someone with access to srv-prod (not something I can do from here — this
touches a server outside this repo):

```bash
# On srv-prod:
sudo useradd -m -s /bin/bash deploy
sudo usermod -aG docker deploy   # so `docker compose` works without sudo
sudo -u deploy mkdir -p /home/deploy/.ssh
echo "<contents of srv-prod-deploy-key.pub>" | sudo -u deploy tee -a /home/deploy/.ssh/authorized_keys
sudo chmod 700 /home/deploy/.ssh && sudo chmod 600 /home/deploy/.ssh/authorized_keys

sudo mkdir -p /opt/twitch-analytics/drivers /opt/twitch-analytics/nifi/dead-letter
sudo chown -R deploy:deploy /opt/twitch-analytics
```

### 3. Create the `production` GitHub Environment (the approval gate)

In the repo on GitHub: **Settings → Environments → New environment**, name it
`production`, then under **Deployment protection rules** add yourself (or
whoever should sign off) as a **required reviewer**. This is what makes every
deploy stop and wait for a human click — the workflow's `environment:
production` block is what invokes it.

### 4. Add repo secrets

**Settings → Secrets and variables → Actions → Secrets** (never paste these
into a chat, issue, or commit — GitHub secrets are the only place they
belong):

| Secret | Value |
|---|---|
| `SRV_PROD_SSH_KEY` | contents of `srv-prod-deploy-key` (the private half) |
| `SRV_PROD_HOST` | srv-prod's hostname or IP |
| `SRV_PROD_USER` | `deploy` |
| `SRV_PROD_SSH_PORT` | only if not `22` |
| `SRV_PROD_KNOWN_HOSTS` | output of `ssh-keyscan -p <port> <host>` run once, from a machine you trust, against srv-prod |

`SRV_PROD_KNOWN_HOSTS` pins the host key so the deploy step verifies it's
really talking to srv-prod — skip it and a spoofed/MITM'd host could
silently receive your deploy key's session instead.

And **Settings → Secrets and variables → Actions → Variables** (non-secret,
fine as plain variables):

| Variable | Value |
|---|---|
| `SRV_PROD_DEPLOY_PATH` | only if not `/opt/twitch-analytics` |
| `SRV_PROD_NIFI_HOSTNAME` | only if not `nifi.thoutmose.me` (cosmetic — shown as the environment URL in the Actions UI) |

### 5. `.env` on srv-prod

`docker-compose.yml` reads `NIFI_ADMIN_USERNAME`, `NIFI_ADMIN_PASSWORD`, and
`NIFI_WEB_PROXY_HOST` from a `.env` file next to it. CD deliberately never
writes this file — create it once, by hand, in `/opt/twitch-analytics/.env`
on the server, and it'll persist across deploys:

```bash
# On srv-prod, as the deploy user:
cat > /opt/twitch-analytics/.env <<'EOF'
NIFI_ADMIN_USERNAME=admin
NIFI_ADMIN_PASSWORD=<a real password, min 12 chars>
NIFI_WEB_PROXY_HOST=nifi.thoutmose.me
EOF
chmod 600 /opt/twitch-analytics/.env
```

## What the pipeline actually does

1. `CI` runs on every push/PR (lint, type check, tests, SQL/OpenAPI lint,
   config validation, secret scan, dependency audit).
2. On a successful `CI` run on `main`, `CD` starts and immediately pauses at
   the `production` environment for a required reviewer to approve it
   (**Actions tab → the waiting run → Review deployments**).
3. Once approved: `rsync` syncs `docker-compose.yml` and `drivers/` to
   `$SRV_PROD_DEPLOY_PATH` over SSH (`nifi/dead-letter/` is left alone — it's
   a live volume mount with production data, not a deploy artifact).
4. `docker compose pull && docker compose up -d --remove-orphans` on
   srv-prod.
5. A health check polls `https://localhost:8443/nifi/` on srv-prod for up to
   ~100s and fails the deploy if NiFi doesn't come back up.

## Manual redeploy

**Actions → CD (srv-prod) → Run workflow** — re-runs the same steps against
whatever is currently on `main`, still gated by the same approval step.
