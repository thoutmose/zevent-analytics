# Deploying to srv-prod

CD ([`.github/workflows/cd.yml`](.github/workflows/cd.yml)) deploys **only
the NiFi stack** (`docker-compose.yml` + `drivers/`) to srv-prod, into the
same checkout (`~/twitch-analytics-prod`) that
`main.py`/`zevent_api.py`/`zevent_donation_goals.py` run from for the real
event (see `README.md`'s "Running as a service"). CD only ever touches the
NiFi-owned paths in that checkout (`docker-compose.yml`, `drivers/`,
`nifi/`) — the Python scripts, `.env`, and `.git` are untouched by it, and
are set up and started the same manual, systemd way described in
`README.md`, independent of what CD does.

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
```

`deploy` needs write access to only the NiFi-owned paths inside the
extractors' checkout (`docker-compose.yml`, `drivers/`, `nifi/`) — not the
whole checkout, so a compromised deploy key still can't reach `.env`, `.git`,
or the Python scripts. Do this with a shared group rather than `chown -R`
the whole directory:

```bash
# On srv-prod, once ~/twitch-analytics-prod exists (see README.md):
sudo groupadd twitch-deploy
sudo usermod -aG twitch-deploy deploy
sudo usermod -aG twitch-deploy <the user the extractors' checkout belongs to>

cd ~/twitch-analytics-prod
sudo chgrp -R twitch-deploy docker-compose.yml drivers nifi
sudo chmod -R g+rwX drivers nifi
sudo chmod g+s drivers nifi   # new files rsync'd in inherit the group
sudo chmod g+rw docker-compose.yml
```

Group write on those paths isn't enough on its own — `deploy` also needs
*traversal* rights on every directory above them (the home directory is
normally `750`, which blocks `deploy` entirely), and it must never get
that by joining the checkout owner's personal group, since that would also
grant read access to `.env`. Use a `setfacl` execute-only grant instead —
this lets `deploy` pass through without being able to list either
directory's contents or read anything it wasn't explicitly given access to:

```bash
sudo apt-get install -y acl   # if setfacl isn't already installed
sudo setfacl -m g:twitch-deploy:x ~                        # the home dir
sudo setfacl -m g:twitch-deploy:x ~/twitch-analytics-prod   # the checkout root
```

One more wrinkle: `deploy` has *write* access to `docker-compose.yml` but
not *ownership* of it (the checkout owner is), and two things that need
ownership, not just write access, would otherwise break the sync:

- rsync's default behavior is to write a temp file next to the target and
  rename it into place — renaming needs write on the *containing*
  directory, which `deploy` deliberately doesn't have. `--inplace` writes
  directly to the target file instead, which only needs the file's own
  write bit.
- rsync's archive mode (`-a`) also tries to replicate the source's owner,
  group, permissions, and timestamps onto the destination — each of those
  requires being the file's owner (or root), which `deploy` isn't. Pass
  `--no-owner --no-group --no-perms --no-times` (or just don't use `-a`)
  so only file *content* transfers.

`docker-compose.yml`'s group ownership only needs setting once — a later
`git checkout`/`merge`/`pull` run directly in this checkout (as opposed to
CD's rsync, which never touches it) rewrites the file and silently drops
the custom group, so re-run the `chgrp`/`chmod` above if that ever happens.
sudo chmod g+s drivers nifi   # new files rsync'd in inherit the group
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
| `SRV_PROD_DEPLOY_PATH` | the extractors' checkout, e.g. `/home/thoutmose/twitch-analytics-prod` (only needed if it's not the literal path `/opt/twitch-analytics`) |
| `SRV_PROD_NIFI_HOSTNAME` | only if not `nifi.thoutmose.me` (cosmetic — shown as the environment URL in the Actions UI) |

### 5. `.env.nifi` on srv-prod

`docker-compose.yml` reads `NIFI_ADMIN_USERNAME`, `NIFI_ADMIN_PASSWORD`, and
`NIFI_WEB_PROXY_HOST` for its `${...}` interpolation from a **separate**
`.env.nifi` file next to it — deliberately not the same `.env` the Python
extractors read their Twitch/Postgres secrets from, even though both now
live in the same checkout. `docker compose up` has to read whichever file
supplies those values, and it runs as `deploy`; `deploy` can be given read
access to `.env.nifi` alone via the shared group, but must never be able to
read the main `.env`. CD deliberately never writes `.env.nifi` — create it
once, by hand, and pass `--env-file .env.nifi` on every `docker compose`
invocation that needs it (CD's "Apply stack" step already does):

```bash
# On srv-prod:
cat > /home/thoutmose/twitch-analytics-prod/.env.nifi <<'EOF'
NIFI_ADMIN_USERNAME=admin
NIFI_ADMIN_PASSWORD=<a real password, min 12 chars>
NIFI_WEB_PROXY_HOST=nifi.thoutmose.me
EOF
sudo chown <checkout owner>:twitch-deploy /home/thoutmose/twitch-analytics-prod/.env.nifi
chmod 640 /home/thoutmose/twitch-analytics-prod/.env.nifi   # owner rw, group r, other none
```

For any manual `docker compose` command on this checkout (not just CD's),
remember `--env-file .env.nifi` — without it, Compose falls back to
looking in `.env` (or nowhere), and `NIFI_ADMIN_PASSWORD` has no default
(`${NIFI_ADMIN_PASSWORD:?...}`), so the command fails loudly rather than
silently misconfiguring.

## What the pipeline actually does

1. `CI` runs on every push/PR (lint, type check, tests, SQL/OpenAPI lint,
   config validation, secret scan, dependency audit).
2. On a successful `CI` run on `main`, `CD` starts and immediately pauses at
   the `production` environment for a required reviewer to approve it
   (**Actions tab → the waiting run → Review deployments**).
3. Once approved: `rsync` syncs `docker-compose.yml` and `drivers/` to
   `$SRV_PROD_DEPLOY_PATH` (the extractors' checkout) over SSH, writing only
   to the NiFi-owned paths the `deploy` user has group access to
   (`nifi/dead-letter/` is left alone either way — it's a live volume mount
   with production data, not a deploy artifact).
4. `docker compose pull && docker compose up -d --remove-orphans` on
   srv-prod.
5. A health check polls `https://localhost:8443/nifi/` on srv-prod for up to
   ~100s and fails the deploy if NiFi doesn't come back up.

## Manual redeploy

**Actions → CD (srv-prod) → Run workflow** — re-runs the same steps against
whatever is currently on `main`, still gated by the same approval step.
