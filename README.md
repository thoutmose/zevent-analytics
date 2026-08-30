# zevent-analytics

![Zevent](img/zevent.jpg)

![Python](https://img.shields.io/badge/Python-3.12%2B-3776AB?logo=python&logoColor=white)
![uv](https://img.shields.io/badge/uv-package%20manager-DE5FE9?logo=uv&logoColor=white)
![TwitchIO](https://img.shields.io/badge/TwitchIO-3.x-9146FF?logo=twitch&logoColor=white)
![aiohttp](https://img.shields.io/badge/aiohttp-async%20HTTP-2C5BB4)
![PyArrow](https://img.shields.io/badge/PyArrow-Parquet%20%2B%20zstd-150458)
![Apache NiFi](https://img.shields.io/badge/Apache%20NiFi-1.24-728E9B?logo=apachenifi&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)
![PgBouncer](https://img.shields.io/badge/PgBouncer-connection%20pooling-4169E1)
![Docker Compose](https://img.shields.io/badge/Docker%20Compose-NiFi%20stack-2496ED?logo=docker&logoColor=white)
![Ruff](https://img.shields.io/badge/lint%20%2F%20format-Ruff-D7FF64?logo=ruff&logoColor=black)
![ty](https://img.shields.io/badge/type%20check-ty-FCA121)
[![CI](https://github.com/thoutmose/zevent-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/thoutmose/zevent-analytics/actions/workflows/ci.yml)

Extracts live-stream data for every Twitch channel participating in
[Zevent](https://zevent.fr/) — chat, viewer counts, stream metadata, and the
event's own donation/streamer feed — and lands it in a PostgreSQL bronze
layer through an Apache NiFi ingestion pipeline, with a local Parquet copy
of everything along the way.

## Table of contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Infrastructure](#infrastructure)
- [Design decisions](#design-decisions)
- [Reliability mechanisms](#reliability-mechanisms)
- [Performance](#performance)
- [Project structure](#project-structure)
- [Getting started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [1. Register a Twitch application](#1-register-a-twitch-application)
  - [2. Configure environment variables](#2-configure-environment-variables)
  - [3. Bring up the NiFi stack](#3-bring-up-the-nifi-stack)
  - [4. Apply the database schema](#4-apply-the-database-schema)
  - [5. Build the NiFi flow](#5-build-the-nifi-flow)
  - [6. Run the extractors](#6-run-the-extractors)
- [Configuration reference](#configuration-reference)
- [Data model](#data-model)
- [Logging](#logging)
- [Development](#development)
- [Known limitations](#known-limitations)
- [Efficiency & stability recommendations](#efficiency--stability-recommendations)
- [Future data analysis tooling](#future-data-analysis-tooling)

## Overview

Three independent extractors feed the same pipeline:

| Script | Watches | Source | Auth |
|---|---|---|---|
| [`main.py`](main.py) | Every channel in the current Zevent roster (300+) | Twitch Helix (batched polling) + anonymous IRC (sharded connections) | Device Code Flow, app-only |
| [`zevent_api.py`](zevent_api.py) | The whole event at once | [`zevent.fr/api/`](https://zevent.fr/api/), public/unauthenticated | none |
| [`zevent_donation_goals.py`](zevent_donation_goals.py) | Every streamer's donation goals | `api.ppr.evenmorestats.fr` (the JSON backend behind [`zevent.gdoc.fr/participations`](https://zevent.gdoc.fr/participations), public/unauthenticated) | none |

All three push their batches to an [Apache NiFi](https://nifi.apache.org/)
flow over HTTP (`NIFI_WEBHOOK_URL`) which routes, splits, and writes them
into PostgreSQL — see [`ARCHITECTURE.md`](ARCHITECTURE.md) for the full
design rationale (batching, backpressure, idempotency, dead-lettering). Left
unset, all three scripts still run standalone — `main.py` lands Parquet
files locally, the other two still write their local checkpoint — NiFi is
an additive sink, not a hard dependency.

`main.py` no longer targets a single hardcoded channel: it fetches the
current Zevent streamer list from `zevent.fr/api/` at startup and watches
all of them. EventSub (`stream.online`/`offline`, `channel.update`,
`channel.raid`) was dropped in favor of pure Helix polling once the channel
count crossed into the hundreds — subscribing to 3–4 events per channel
risks per-websocket subscription limits at that scale. Raids aren't tracked
as a result.

## Architecture

```mermaid
flowchart LR
    subgraph Sources
        TW["Twitch<br/>(Helix + anonymous IRC)"]
        ZV["zevent.fr/api/"]
        DGAPI["api.ppr.evenmorestats.fr<br/>(zevent.gdoc.fr's backend)"]
    end

    subgraph "Python extractors"
        MAIN["main.py<br/>(sharded IRC + batched Helix poll)"]
        ZAPI["zevent_api.py<br/>(event-wide snapshot)"]
        DGOAL["zevent_donation_goals.py<br/>(per-streamer goal list)"]
    end

    subgraph "Local landing zone"
        PARQUET[("Parquet files<br/>data/live_chat/, data/metadata/")]
    end

    subgraph "Apache NiFi (srv-prod)"
        LISTEN["ListenHTTP :8080"]
        EJP["EvaluateJsonPath<br/>(promote stream attr)"]
        MERGE["MergeContent<br/>(correlate by stream)"]
        ROUTE["RouteOnAttribute"]
        SPLIT["SplitJson<br/>(rows → 1 flowfile each)"]
        PUTDB["PutDatabaseRecord<br/>(INSERT, bronze_live_chat/<br/>metadata_snapshots/zevent_snapshots)"]
        PUTDBUP["PutDatabaseRecord<br/>(UPSERT, bronze_donation_goals)"]
        DEADLETTER[("PutFile<br/>dead-letter")]
    end

    subgraph "srv-db"
        PGB["PgBouncer :6432"]
        PG[("PostgreSQL<br/>bronze_* tables")]
    end

    TW --> MAIN
    ZV --> ZAPI
    ZV -.roster at startup.-> MAIN
    DGAPI --> DGOAL
    MAIN --> PARQUET
    MAIN -- NIFI_WEBHOOK_URL --> LISTEN
    ZAPI -- NIFI_WEBHOOK_URL --> LISTEN
    DGOAL -- NIFI_WEBHOOK_URL --> LISTEN
    LISTEN --> EJP --> MERGE --> ROUTE --> SPLIT
    SPLIT -- stream != donation_goals --> PUTDB
    SPLIT -- stream == donation_goals --> PUTDBUP
    PUTDB -- success --> PG
    PUTDB -- failure --> DEADLETTER
    PUTDBUP -- success --> PG
    PUTDBUP -- failure --> DEADLETTER
    PGB --> PG
    PUTDB -.via PgBouncer.-> PGB
    PUTDBUP -.via PgBouncer.-> PGB
```

`srv-prod` runs the prod extractors alongside NiFi; PostgreSQL and PgBouncer
run on a separate `srv-db` and aren't managed by this repo. See
[`ARCHITECTURE.md`](ARCHITECTURE.md) for the deployment topology, the
reverse-proxy setup, and every deviation from the original design.

### The NiFi flow, processor by processor

![Apache NiFi flow](img/apache-nifi-flow.png)

A live capture of the flow above (dev instance) — the numbers on each
processor are 5-minute rolling stats, not fixed labels. Left to right, this
is the ingestion path every batch from all three extractors takes:

1. **`ListenHTTP`** — the ingestion endpoint (`NIFI_WEBHOOK_URL`). Each POST
   is one complete, self-contained batch envelope (`batch_id`, `stream`,
   `rows[]`) from one `nifi_client.push_batch` call, not a stream of
   individual events — there's nothing to correlate across requests.
2. **`EvaluateJsonPath`** — promotes the envelope's `$.stream` field
   (`live_chat` / `metadata` / `zevent_snapshot` / `donation_goals`) onto the
   flowfile as an attribute, so the next step can route without re-parsing
   the body on every hop.
3. **`RouteOnAttribute`** — the only branch point in the flow, on that
   `stream` attribute: `insert` (the three append-only streams), `upsert`
   (`donation_goals`, the one stream that overwrites in place), or
   `unmatched` for anything else.
4. **`SplitJson`** (one per branch) — expands the envelope's `rows[]` array
   into one flowfile per row. Each row already carries its own `batch_id` +
   `row_number` (set Python-side, see `nifi_client.py`) — that's what makes
   a re-sent batch idempotent at the database layer, not anything NiFi does.
5. **`PutDatabaseRecord`** (one per branch) — the sink. The INSERT branch
   picks its target table (`bronze_live_chat` / `bronze_metadata_snapshots` /
   `bronze_zevent_snapshots`) with a NiFi Expression Language ternary on the
   `stream` attribute, so one processor covers three tables instead of three
   near-identical ones. The UPSERT branch always writes
   `bronze_donation_goals` with `Update Keys = participation_id, goal_id`.
6. **`UpdateAttribute`** — every failure path in the flow (`EvaluateJsonPath`
   failing on unparseable JSON, `RouteOnAttribute`'s `unmatched`, either
   `PutDatabaseRecord` failing to write) is unified here before touching
   disk, rewriting `filename` to `${filename}-${UUID()}`. This isn't
   decorative: `SplitJson` gives every row from one batch the *same*
   filename, and the next processor's conflict strategy is `fail` on a
   duplicate name — without a uniquifier, only the first failing row per
   batch ever reached disk and every other one was silently destroyed. See
   [`ARCHITECTURE.md`](ARCHITECTURE.md) for the numbers from when this was
   found.
7. **`PutFile` (dead-letter)** — one file per failed row, name now
   guaranteed unique, written to `nifi/dead-letter/` for manual replay (see
   [Known limitations](#known-limitations)) — not an automated retry.

Every connection here carries NiFi's default backpressure (10,000 flowfiles
/ 1 GB): it just doesn't render as a number on an idle canvas — NiFi only
color-codes a connection once its queue nears the threshold. A flow
definition for all of this ships at
[`flow-templates/zevent-ingest-flow.json`](flow-templates/zevent-ingest-flow.json)
(see [`ARCHITECTURE.md`](ARCHITECTURE.md) for how to import it).

## Infrastructure

Everything above — three extractors, NiFi routing, the PostgreSQL sink, and
cold-storage archival — runs on six guests on a single Proxmox host,
provisioned via cloud-init (`qm`/`pct`) with no Terraform/Ansible layer on
top. Real LAN addresses aren't listed here, same policy as
[`ARCHITECTURE.md`](ARCHITECTURE.md#topology) and for the same reason (this
file is committed to git) — resolve hosts by their Tailscale MagicDNS name.

```mermaid
flowchart LR
    subgraph PVE["Proxmox host"]
        NPM["srv-npm<br/>reverse proxy<br/>LXC · 2 vCPU / 2 GB / 8 GB"]
        DEV["srv-dev<br/>extractors + dev NiFi<br/>VM · 4 vCPU / 8 GB / 40 GB"]
        PROD["srv-prod<br/>extractors + prod NiFi<br/>VM · 6 vCPU / 12 GB / 40 GB"]
        DB["srv-db<br/>PostgreSQL + PgBouncer<br/>VM · 4 vCPU / 8 GB / 100 GB"]
        SVC["srv-services<br/>cold-storage archive + pgAdmin<br/>VM · 6 vCPU / 12 GB / 450 GB"]
        MON["srv-monitoring<br/>standalone, unused by this repo<br/>LXC · 2 vCPU / 2 GB / 20 GB"]
    end
    CD["cd.yml runner<br/>(GitHub Actions)"]

    NPM -- "nifi.thoutmose.me" --> PROD
    NPM -- "nifi-dev.thoutmose.me" --> DEV
    PROD == same LAN ==> DB
    DEV -. Tailscale .-> DB
    CD -. "Tailscale, deploy" .-> PROD
    DEV -. "archive_parquet.py / archive_logs.py" .-> SVC
    PROD -. "archive_parquet.py / archive_logs.py" .-> SVC
    SVC -. "pgAdmin" .-> DB
```

| Host | Role | Type | vCPU | RAM | Disk |
|---|---|---|---|---|---|
| `srv-dev` | Dev machine — extractors + this repo's checkout + dev NiFi | VM | 4 | 8 GB | 40 GB |
| `srv-prod` | Prod machine — extractors + this repo's checkout + prod NiFi  | VM | 6 | 12 GB | 40 GB |
| `srv-db` | PostgreSQL + PgBouncer — not managed by this repo | VM | 4 | 8 GB | 100 GB |
| `srv-services` | Cold-storage target for [`archive_parquet.py`](archive_parquet.py)/[`archive_logs.py`](archive_logs.py), plus [pgAdmin](https://www.pgadmin.org/) (`dpage/pgadmin4`, port 5050) for ad-hoc Postgres administration | VM | 6 | 12 GB | 450 GB |
| `srv-npm` | Reverse proxy in front of `*.thoutmose.me` | LXC | 2 | 2 GB | 8 GB |
| `srv-monitoring` | Monitoring stack — standalone, not integrated with this repo | LXC | 2 | 2 GB | 20 GB |
| **Total** | | | **24** | **44 GB** | **658 GB** |

24 vCPU and 44 GB of RAM, split across 4 VMs and 2 LXC containers on one
physical box, is the entire footprint for ingesting ~300 channels' live
chat plus event-wide/donation-goal polling in real time, routing it through
NiFi, and sinking it to Postgres — see [Performance](#performance) for the
actual throughput (74 msg/s sustained, ~190 msg/s peak) that footprint
sustains.

## Design decisions

### Why Apache NiFi

The pipeline needs an HTTP ingestion endpoint, per-batch routing by data
type, and a database sink with a dead-letter path for failures. NiFi
provides all of that as configurable processors (`ListenHTTP`,
`RouteOnAttribute`, `PutDatabaseRecord`, `PutFile`) instead of code this
project would otherwise have to write and operate itself — a message
broker (Kafka or similar) would still need a consumer and a
database-writing sink built on top of it for the same result. The
trade-off is that NiFi's flow lives in its own UI/REST API rather than in
version-controlled code — mitigated here by documenting every processor's
configuration in [`ARCHITECTURE.md`](ARCHITECTURE.md) since no flow export
can be validated without a running instance to import it into.

### Why PostgreSQL + JSONB

PostgreSQL and PgBouncer were the given target (see
[`ARCHITECTURE.md`](ARCHITECTURE.md)), not something this project
evaluated against alternatives. It's a good fit for what's actually
needed: `UNIQUE (batch_id, row_number)` gives exactly-once semantics on
replay for free, and `jsonb` holds `zevent_api.py`'s per-streamer array
(`bronze_zevent_snapshots.streamers`) without a rigid one-column-per-field
schema. Nothing here is written to be queried at analytical scale — it's a
bronze/raw landing layer, not a warehouse.

### Why Parquet as a dual-write, not just a local cache

`main.py` wrote Parquet before NiFi/PostgreSQL entered the picture at all
— `nifi_client.py`'s own docstring calls it "an additive sink, not a
replacement". Keeping the Parquet write means the extraction survives
NiFi being down, misconfigured, or not deployed yet, and gives a
zstd-compressed local copy to re-derive from independent of the DB. It
also acts as the buffer during a NiFi outage: at the ~190 msg/s peak
estimated in [Reliability mechanisms](#reliability-mechanisms), even a
30-minute outage is only ~340K messages of chat, comfortably absorbed as
local Parquet. The trade-off is the two sinks can disagree if one push
fails and the other doesn't (see [Known limitations](#known-limitations))
— acceptable for a bronze layer that's meant to be replayed, not treated
as a single source of truth.

### Why `zevent_donation_goals.py` calls a JSON API instead of scraping HTML

`zevent.gdoc.fr/participations` is a client-rendered Nuxt SPA — its HTML
response carries no data at all, only a JS bundle that fetches everything
from `api.ppr.evenmorestats.fr` after the page loads. Scraping the rendered
page would mean running a headless browser just to read back JSON the page
itself already fetched over plain HTTP; calling that backend directly
(`/events`, `/events/{id}/donation_goals/overview`,
`/participations/{id}/donation_goals` — reverse-engineered from the SPA's JS
bundle) is lighter, faster, and exactly the same approach `zevent_api.py`
already takes against `zevent.fr/api/` instead of scraping `zevent.fr`.

Each goal also carries an `accomplished` flag (whether it's been met) —
dropped on purpose. This module tracks what the goals *are*, not how close
to met they are; `bronze_zevent_snapshots.total_donation_amount_eur`
(`zevent_api.py`) already covers overall progress.

Unlike the two append-only extractors above, this table is meant to reflect
only the *current* set of goals, not a history of every poll — see
`sql/002_donation_goals.sql` and [Data model](#data-model) for how that's
implemented (UPSERT, not INSERT) and its one known gap (removed goals aren't
deleted).

## Reliability mechanisms

Sized against Zevent 2025 — the same format Zevent 2026 follows: 327
channels, 55h of live, 751,889 peak viewers, 296,175 average viewers. At
Twitch's rough 1–3 chat messages/min per 100 viewers (the ratio drops as a
channel gets bigger — chat scrolls too fast to read, and slow-mode often
throttles the biggest ones further), that puts extraction/load at roughly
**74 msg/s sustained, ~190 msg/s at peak**, and the peak itself is a ramp
over tens of minutes (day/night viewer cycle) rather than a sudden spike —
there's no wall of traffic arriving in seconds to design around.

What's actually implemented and testable in this repo, as of the latest
multi-channel rewrite:

- **Idempotent replay** — every row carries `batch_id` + `row_number`;
  `UNIQUE (batch_id, row_number)` (`sql/001_bronze_schema.sql`) makes a
  re-sent batch a no-op instead of a duplicate.
- **Dead-lettering, not data loss** — `PutDatabaseRecord`'s `failure`
  relationship (and `RouteOnAttribute`'s `unmatched`) routes through an
  `UpdateAttribute` processor that rewrites `filename` to
  `${filename}-${UUID()}` before reaching `PutFile`, then lands in
  `nifi/dead-letter/`. The `UpdateAttribute` step matters because every row
  NiFi splits out of one source batch inherits that batch's original
  `filename` — without it, `PutFile`'s `fail`-on-conflict strategy plus its
  auto-terminated `failure` relationship meant only the *first* failing row
  per batch ever reached disk; every other one silently vanished (confirmed
  on the dev NiFi instance: 27,890 filename collisions logged against only
  164 files that actually survived in `nifi/dead-letter/`). If `srv-prod`'s
  NiFi flow was built the same way, it likely has the same gap and needs the
  same fix.
- **Graceful shutdown waits for in-flight pushes** —
  [`nifi_client.wait_for_pending_pushes`](nifi_client.py) is awaited
  before the event loop closes, so a NiFi push isn't cancelled mid-request
  (a cancelled-but-already-sent request is ambiguous — see that
  function's docstring for the double-insert this prevents).
- **IRC sharded across reconnecting connections, with jitter** — each
  [`ChatConnection`](main.py) covers `CHANNELS_PER_IRC_CONNECTION`
  channels and reconnects with jittered exponential backoff on drop (sleep
  somewhere in `[backoff, 2*backoff]`, capped at 60s, not exactly
  `backoff`), so one flaky connection only affects its own shard, and a
  shared outage across shards doesn't send every one of them back to
  Twitch in lockstep.
- **Zevent API checkpoint** — [`zevent_api.py`](zevent_api.py) has no
  Parquet dual-write, so `_write_checkpoint` persists the last
  successfully fetched snapshot to `ZEVENT_CHECKPOINT_PATH` (default
  `data/zevent_checkpoint.json`) after every poll, via a temp-file-plus-
  rename so a crash mid-write can't corrupt it. A restart, or a
  zevent.fr/api/ outage (it happened for ~17min during Zevent 2024),
  always leaves a recent known-good snapshot on disk.
- **Rate-limit-aware fan-out** — Helix calls (`Get Users`/`Get Streams`)
  are chunked to 100 logins per request (Twitch's own limit); IRC `JOIN`s
  are paced (`IRC_JOIN_PACING_SECONDS`) to stay under Twitch's per-10s
  connection limit.
- **NiFi's built-in per-connection backpressure** — every connection
  between processors has an object/size threshold NiFi enforces natively;
  synthetically load-tested on `srv-dev` (see [Performance](#performance)
  below), though not yet against real Zevent traffic.
- **Archival, not accumulation** —
  [`archive_parquet.py`](archive_parquet.py)/[`archive_logs.py`](archive_logs.py)
  are standalone, cron-run scripts (not part of the always-on extractors):
  each run bundles every eligible file into one tar.zst archive (see
  [`archive_common.py`](archive_common.py)), rsyncs that single bundle to
  `ARCHIVE_REMOTE_HOST`, confirms its sha256 matches the local bundle, and
  only then deletes the original local files — a transfer failure or hash
  mismatch leaves every original untouched and the whole batch is simply
  retried on the next run. `--dry-run` reports what would be archived/
  deleted without touching anything, given the delete step is irreversible.

## Performance

Real event numbers (throughput, dead-letter rate, IRC reconnect count per
shard, manual-restart count) are still not yet measured — Zevent hasn't
happened yet on this timeline, and this note will be filled in afterward
with real numbers pulled from `logging/` (see [Logging](#logging)) and
PostgreSQL itself, not projected ones.

What *has* been measured: a synthetic capacity/stress test of the
ingestion pipeline's ceiling, run against the dev NiFi instance (`srv-dev`,
`zevent-dev` database) on 2026-08-30 via `stress_test.py` — a ramping load
generator posting synthetic `live_chat` batches to `/ingest`. This
exercises the pipeline itself (webhook → NiFi → Postgres), not real Twitch
chat volume.

### Baseline (NiFi's untuned defaults)

Every processor in the flow defaults to NiFi's `Concurrent Tasks = 1`; the
`DBCPConnectionPool` defaults to `Max Total Connections = 8`; every
inter-processor connection defaults to a 10,000-flowfile backpressure
threshold.

- **1 concurrent producer:** ~220 req/s (~4,400 rows/s) accepted, 0%
  errors, sustained.
- **≥2 concurrent producers:** immediate HTTP 503 from `ListenHTTP`,
  within ~5ms — confirmed via NiFi's own servlet response and logs, not a
  client-side artifact. Root cause: with every processor capped at 1
  concurrent task, the queue immediately downstream of `ListenHTTP` fills
  almost instantly, and `ListenHTTP` starts rejecting new connections
  outright instead of queuing them.
- **Practical risk this exposes:** `zevent-main.service`,
  `zevent-api.service`, and `zevent-donation-goals.service` all push to
  the same `NIFI_WEBHOOK_URL` independently. `nifi_client.push_batch`
  deliberately does not retry a definite HTTP-level rejection (only
  ambiguous timeouts — see [Reliability
  mechanisms](#reliability-mechanisms)), so two services posting at the
  same instant could silently drop one batch under this untuned default.

### After retuning

Changes applied live via the NiFi REST API (see the persistence caveat
below):

| Setting | Before | After |
|---|---|---|
| `ListenHTTP` / `PutDatabaseRecord (INSERT)` Concurrent Tasks | 1 | 8 |
| `EvaluateJsonPath` / `RouteOnAttribute` / `SplitJson (insert)` Concurrent Tasks | 1 | 4 |
| `SplitJson (upsert)` / `PutDatabaseRecord (UPSERT)` / dead-letter processors Concurrent Tasks | 1 | 2 |
| `DBCPConnectionPool` Max Total Connections | 8 | 24 |
| Connection backpressure object threshold | 10,000 | 50,000 |
| `nifi.content.repository.archive.max.retention.period` | 7 days | 2 minutes |
| `nifi.content.repository.archive.max.usage.percentage` | 50% | 90% |

Results:

- **Webhook acceptance:** clean (0% errors) up to concurrency=25, ~162,850
  rows/s, p50/p95/p99 latency 7/12/22ms.
- **Ceiling moved, not removed:** error rate crosses 20% at concurrency=50;
  full saturation (100% errors) at concurrency=100. The breaking point
  shifted from "any 2nd concurrent connection" to roughly 25–50 concurrent
  producers.
- **Sustained, steady-state database insert rate: ~2,700–3,000 rows/sec**,
  measured directly against `bronze_live_chat` row growth (not just
  HTTP-layer acceptance) — comfortably above the ~190 msg/s peak estimated
  in [Reliability mechanisms](#reliability-mechanisms).

### Stability findings

- **Content-repository archive throttling is a whole-partition check, not
  a NiFi-specific one.** `nifi.content.repository.archive.max.usage.percentage`
  compares against the entire filesystem NiFi's content repo lives on —
  on a disk already >50% full from unrelated data, sustained high-volume
  writes stall (`Unable to write flowfile content ... waiting for archive
  cleanup`) regardless of how little NiFi itself has archived. Hit this on
  `srv-dev` (a shared 38GB disk, ~69% used from non-NiFi data) well before
  NiFi's own footprint was meaningful (its archive was 1.64MB at the time).
- **Deep queues degrade throughput non-linearly.** Once a connection's
  queue passes NiFi's in-memory swap threshold (10,000 flowfiles), it
  starts swapping to disk; a backlog that oscillates around that threshold
  causes repeated swap-out/swap-in churn that dropped measured drain
  throughput from ~3,000 rows/s to ~60 rows/s. Keeping bursts under ~10K
  flowfiles deep (or raising the swap threshold alongside backpressure)
  avoids the cliff.
- **`DBCPConnectionPool`'s `Password` property is masked (`********`) on
  every `GET`.** Re-submitting a fetched `properties` dict verbatim on a
  `PUT` — even to change an unrelated property like pool size — silently
  overwrites the real password with that placeholder. Caused an
  ~8.5-minute production DB-write outage on `srv-dev` during this testing
  (12:45:36–12:54:05 UTC, 2026-08-30) before being caught and reverted.
  Any future scripted config change via the NiFi API must strip or
  re-supply sensitive properties explicitly, never round-trip them.

### Caveats

- These are synthetic capacity numbers from `srv-dev`, not real Zevent
  traffic — the real-event numbers noted at the top of this section are
  still pending.
- The retuned settings above were applied live via the NiFi REST API and a
  `nifi.properties` edit inside the running container. **Neither is
  persisted** — recreating the container (`docker compose up
  --force-recreate`, or any rebuild — a plain `docker restart` is fine)
  reverts both to NiFi's defaults, silently re-introducing the
  concurrency-of-1 ceiling. If these settings should be permanent, they
  need to move into the flow template
  (`flow-templates/zevent-ingest-flow.json`) and
  `docker-compose.yml`/a mounted `nifi.properties` respectively.

## Project structure

```
.
├── main.py                  # multi-channel Twitch extractor (chat + metadata)
├── zevent_api.py             # zevent.fr/api/ poller (event-wide snapshot)
├── zevent_donation_goals.py   # per-streamer donation goal poller
├── archive_parquet.py        # moves old local Parquet files to cold storage (cron)
├── setup_archive.sh          # one-time SSH+cron setup for archive_parquet.py
├── nifi_client.py             # shared HTTP push helper (batching, retries-safe)
├── logging_setup.py           # loads logging.yaml, picks dev/prod handler profile
├── logging.yaml                # rotating file handlers + colored console
├── sql/001_bronze_schema.sql    # PostgreSQL bronze tables (applied on srv-db)
├── sql/002_donation_goals.sql    # bronze_donation_goals (latest-state, UPSERT)
├── docker-compose.yml              # local NiFi stack (srv-prod only)
├── nifi/dead-letter/                 # PutDatabaseRecord failures land here
├── drivers/                            # PostgreSQL JDBC driver, mounted into NiFi
├── openapi.yaml                          # every external Twitch API call, documented
├── ARCHITECTURE.md                         # target production pipeline, in depth
├── DEPLOYMENT.md                             # CD setup: secrets, srv-prod access
├── .github/workflows/                          # CI (lint/test/security) + CD (srv-prod)
└── data/                                         # local Parquet output (gitignored)
```

## Getting started

### Prerequisites

- Python 3.12+ and [uv](https://docs.astral.sh/uv/)
- Docker + Docker Compose (for the local NiFi stack)
- A reachable PostgreSQL instance behind PgBouncer (see
  [`ARCHITECTURE.md`](ARCHITECTURE.md) — not provisioned by this repo)

### 1. Register a Twitch application

At https://dev.twitch.tv/console/apps — set **Category** to `Application
Integration` and **Client Type** to `Public` (Device Code Flow needs a
public client, no client secret).

### 2. Configure environment variables

```bash
cp .env.example .env
```

Fill in `TWITCH_CLIENT_ID` at minimum. See
[Configuration reference](#configuration-reference) below for everything
else — most have sane defaults.

### 3. Bring up the NiFi stack

```bash
uv sync
docker compose up -d
```

NiFi's UI/API is served over HTTPS at `https://localhost:8443` (self-signed
cert; single-user login only works over HTTPS — see
[`ARCHITECTURE.md`](ARCHITECTURE.md#4-notes-on-this-implementation-deviations-from-the-original-spec)
for why). The data-ingestion port (`ListenHTTP`) is `localhost:8888`.

### 4. Apply the database schema

```bash
psql "postgresql://<user>@<srv-db-host>:5432/zevent" -f sql/001_bronze_schema.sql
psql "postgresql://<user>@<srv-db-host>:5432/zevent" -f sql/002_donation_goals.sql
```

### 5. Build the NiFi flow

This repo doesn't ship an exported NiFi template (a hand-authored one can't
be validated without a running instance to import it into). Build it per
the [Architecture](#architecture) diagram above — `ListenHTTP` →
`EvaluateJsonPath` → `MergeContent` → `RouteOnAttribute` → `SplitJson` →
`PutDatabaseRecord` (+ dead-letter `PutFile` on failure). Full processor
configuration is in [`ARCHITECTURE.md`](ARCHITECTURE.md#2-how-the-pieces-in-this-repo-map-onto-the-diagram).
The `donation_goals` stream needs its own `PutDatabaseRecord` branch (routed
by `RouteOnAttribute` on `stream == "donation_goals"`) configured with
**Statement Type: UPSERT** and **Update Keys: participation_id, goal_id** —
every other stream uses plain `INSERT` — see `sql/002_donation_goals.sql`.

### 6. Run the extractors

```bash
uv run main.py                    # every Zevent channel: chat + metadata
uv run zevent_api.py              # event-wide snapshot (donations, viewer counts)
uv run zevent_donation_goals.py   # every streamer's donation goal list
```

Each is independent — run any subset of them. On first run, `main.py`
prints a Device Code Flow URL: open it, log in, authorize. The token is
cached in `.tio.tokens.json` so later runs don't need to re-authorize
(as long as the process shuts down cleanly — see
[`nifi_client.wait_for_pending_pushes`](nifi_client.py)).

### Running as a service (start/stop)

For a long-running event (Zevent runs ~55h), the three extractors run as
systemd services rather than a foreground `uv run`. Dev and prod are two
entirely separate checkouts on two separate machines — `twitch-analytics` on
`srv-dev`, pointing at srv-dev's own local NiFi (`zevent-dev` database), and
`twitch-analytics-prod` on `srv-prod`, pointing at srv-prod's own local NiFi
(`zevent` database) — each with its own `.env`. Each checkout has its own
`.tio.tokens.json`, so each needs its own one-time Twitch device-code
approval. On srv-prod, that same checkout is also where CD deploys to: the
NiFi stack (`docker-compose.yml` + `drivers/`), the Python extractors and
archive scripts (source, `sql/`, dependency files), and — via a narrowly
scoped sudoers grant for the deploy user — a `uv sync` plus a restart of the
three `zevent-*-prod` systemd units (see DEPLOYMENT.md); on srv-dev, both
NiFi and the extractors are set up and updated by hand in its own checkout,
just without CD.

```bash
# Start (dev):
sudo systemctl start zevent-api zevent-donation-goals zevent-main

# Start (prod):
sudo systemctl start zevent-api-prod zevent-donation-goals-prod zevent-main-prod

# Stop — either environment, same pattern with/without the -prod suffix:
sudo systemctl stop zevent-api zevent-donation-goals zevent-main

# Status / follow logs for one service:
sudo systemctl status zevent-main
sudo journalctl -u zevent-main -f

# Prevent a stopped service from coming back on the next reboot:
sudo systemctl disable zevent-api-prod zevent-donation-goals-prod zevent-main-prod
```

All six are `Restart=on-failure` — a crash restarts automatically, a manual
`stop` does not (until the next reboot, unless also `disable`d).

### Archiving old Parquet files and log backups

[`archive_parquet.py`](archive_parquet.py) and
[`archive_logs.py`](archive_logs.py) are one-shot maintenance scripts, not
services — neither is installed or scheduled by anything in this repo on
its own. Both share the same tar+zstd bundle + verify + delete-on-confirm
mechanics (see [`archive_common.py`](archive_common.py)) and the same
`ARCHIVE_REMOTE_HOST`, just different destination directories: every file
found eligible in one run is bundled into a single dated
`<prefix>-<timestamp>.tar.zst` archive (e.g.
`parquet-20260830T085635Z.tar.zst`), transferred as that one file, verified
by sha256, and only then has its originals deleted locally — real
cold-storage semantics (one dated bundle per run) rather than many small
files landing individually, and fewer round trips at scale. The trade-off:
verification and deletion apply to the whole batch at once, not per file —
if any step fails, every original in that run's batch is left untouched
(not just one), to be retried as part of the next run's (larger) batch.

- `archive_parquet.py`: Parquet files older than `ARCHIVE_MIN_AGE_SECONDS`
  under `PARQUET_OUTPUT_DIR`, to `ARCHIVE_REMOTE_PATH`.
- `archive_logs.py`: closed/rotated log backups under `logging/` (e.g.
  `debug.log.3`, `debug.log.3.gz`) — never the active files
  [`logging.yaml`](logging.yaml)'s handlers are currently writing to — to
  `ARCHIVE_LOGS_REMOTE_PATH`. There's no age guard here the way Parquet
  needs one: a rotated backup is immutable from the moment it exists, since
  `CompressedRotatingFileHandler` never writes to one again.

#### One-time setup (per machine)

Run [`./setup_archive.sh`](setup_archive.sh) once on each machine that will
run either script (dev box, `srv-prod`, or wherever the extractors actually
run — see [Running as a service](#running-as-a-service-startstop)). It's
idempotent — every step checks the current state first, so re-running it (a
second machine, a key rotation, after pulling a newer version of the
script) never duplicates anything:

```bash
./setup_archive.sh
```

It handles, in order: generating a dedicated SSH key for this purpose if one
doesn't already exist; adding a `Host srv-services` alias to `~/.ssh/config`
so `ARCHIVE_REMOTE_HOST=srv-services` (the default in `.env.example`)
resolves without an explicit `-i` flag anywhere; verifying key-based auth
actually works; installing both scripts' hourly cron jobs (see
[Running it](#running-it) below); and a check that warns if a `*-prod`
checkout's `.env` doesn't have `APP_ENV=production` set (see
[Logging](#logging)) — everything except the logging check is silent when
already correctly set up.

The one step it can't do unattended: if the SSH key it generates isn't yet
authorized on `srv-services`, it prints the exact `ssh-copy-id` command to
run once (needs some existing way in — an already-authorized key, or a
one-time password from whoever manages that server) and exits; re-run
`./setup_archive.sh` afterward to pick up where it left off.

Once SSH access works, `ARCHIVE_REMOTE_PATH` and `ARCHIVE_LOGS_REMOTE_PATH`
(see [Configuration reference](#configuration-reference)) are each created
automatically on srv-services the first time their script actually has a
file to send there — nothing to create manually ahead of time.

#### Running it

`./setup_archive.sh` already installs both of these; shown here for
reference or to adjust manually:

```bash
# Keeps each run's candidate set small, which matters for SSH-call overhead
# — see each script's own module docstring. Logs go through the same
# logging_setup.py profile (APP_ENV) as the other scripts, so failures also
# show up in logging/errors.log (until archive_logs.py itself archives that
# backup, at which point it's on srv-services instead).
0 * * * * cd /path/to/twitch-analytics && uv run archive_parquet.py >> /dev/null 2>&1
0 * * * * cd /path/to/twitch-analytics && uv run archive_logs.py >> /dev/null 2>&1
```

## Configuration reference

All variables live in `.env` (see [`.env.example`](.env.example) for the
authoritative, commented list). Highlights:

| Variable | Default | Purpose |
|---|---|---|
| `TWITCH_CLIENT_ID` | *(required)* | Twitch app client ID |
| `CHANNELS_PER_IRC_CONNECTION` | `50` | Channels per anonymous IRC connection (sharded across `ceil(n / this)` connections) |
| `IRC_JOIN_PACING_SECONDS` | `0.5` | Delay between IRC `JOIN`s, to respect Twitch's rate limit |
| `METADATA_SNAPSHOT_INTERVAL_SECONDS` | `15` | Batched Helix poll interval (all channels, all metadata) |
| `ROSTER_REFRESH_INTERVAL_SECONDS` | `15` | How often `main.py` re-fetches the roster to pick up new streamers mid-event |
| `ZEVENT_API_POLL_INTERVAL_SECONDS` | `20` | `zevent.fr/api/` poll interval |
| `FLUSH_INTERVAL_SECONDS` | `30` | How often buffered rows are flushed to Parquet/NiFi |
| `MAX_ROWS_PER_PARQUET_FILE` | `100000` | Row cap before a Parquet file rolls over |
| `PARQUET_COMPRESSION` | `zstd` | Codec passed to `pyarrow.parquet.write_table` |
| `PARQUET_OUTPUT_DIR` | `data` | Local Parquet landing zone |
| `ARCHIVE_REMOTE_HOST` | `srv-services` | SSH destination for `archive_parquet.py`/`archive_logs.py` (recommend an `~/.ssh/config` alias) |
| `ARCHIVE_REMOTE_PATH` | `zevent-parquet-archive` | Remote directory archived Parquet files land in |
| `ARCHIVE_MIN_AGE_SECONDS` | `600` | Minimum file age before `archive_parquet.py` will archive it |
| `ARCHIVE_LOGS_REMOTE_PATH` | `zevent-logs-archive` | Remote directory archived log backups land in |
| `NIFI_WEBHOOK_URL` | *(unset)* | NiFi `ListenHTTP` endpoint; unset = Parquet/stdout only |
| `APP_ENV` | `development` | Logging profile — see [Logging](#logging) |
| `NIFI_ADMIN_USERNAME` / `NIFI_ADMIN_PASSWORD` | — | NiFi UI single-user login (docker-compose) |
| `DONATION_GOALS_POLL_INTERVAL_SECONDS` | `300` | `zevent_donation_goals.py` poll interval |
| `DONATION_GOALS_MAX_CONCURRENCY` | `5` | Max concurrent per-streamer goal-detail requests |

## Data model

Four tables in PostgreSQL, one per `stream` value pushed through NiFi.

`bronze_live_chat`, `bronze_metadata_snapshots`, and
`bronze_zevent_snapshots` (`sql/001_bronze_schema.sql`) are append-only:
every row carries `batch_id` + `row_number`, and `UNIQUE (batch_id,
row_number)` makes a replayed batch a no-op instead of a duplicate.

| Table | Fed by | Notable columns |
|---|---|---|
| `bronze_live_chat` | `main.py` (IRC) | `channel`, `chatter`, `chatter_id`, `message_text`, `message_sent_at`, `captured_at` |
| `bronze_metadata_snapshots` | `main.py` (Helix poll) | `channel`, `is_live`, `title`, `category`, `viewer_count`, `duration_seconds`, `stream_started_at`, `snapshot_at` |
| `bronze_zevent_snapshots` | `zevent_api.py` | `website_mode`, `total_donation_amount_eur`, `total_viewer_count`, `streamers` (`jsonb`: `twitch_id`, `twitch_login`, `display_name`, `profile_url`, `online`, `game`, `viewer_count`, `donation_amount_eur` per streamer) |

`bronze_donation_goals` (`sql/002_donation_goals.sql`) is different: it
holds only the *current* set of goals, not a history of every poll.
`UNIQUE (participation_id, goal_id)` is the NiFi `PutDatabaseRecord` UPSERT
key (see [step 5](#5-build-the-nifi-flow)), so a re-poll overwrites each
goal's row in place.

| Table | Fed by | Notable columns |
|---|---|---|
| `bronze_donation_goals` | `zevent_donation_goals.py` | `participation_id`, `streamer_name`, `twitch_login`, `twitch_id`, `goal_id`, `goal_name`, `goal_amount_eur`, `goal_category`, `snapshot_at` |

## Logging

Configured centrally by [`logging_setup.py`](logging_setup.py) from
[`logging.yaml`](logging.yaml) — every module logger (`zevent_extractor`,
`nifi_client`, `zevent_api`, `zevent_donation_goals`, `twitchio.*`)
propagates to the root logger, which is what actually determines where
output goes.

Two profiles, selected via `APP_ENV`:

- **`development`** (default) — colored console, plus rotating files under
  `logging/`: `info.log`, `warn.log`, `errors.log`, `critical.log`,
  `debug.log`.
- **`production`** — console + `logging/info.log` only.

Each file rotates at 10 MB, keeping 20 backups.

## Development

```bash
uv sync --group dev     # ruff, ty, sqlfluff, pytest, bandit, pip-audit

uv run ruff format .    # formatting
uv run ruff check .     # linting
uv run ty check .       # static type checking
uv run sqlfluff lint sql/  # SQL lint
uv run pytest            # unit tests (tests/)
uv run bandit -c pyproject.toml -r .  # static security lint
uv run pip-audit --strict             # dependency vulnerability scan
```

All of the above are expected to pass clean on every file in this repo — CI
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs the same
checks, plus OpenAPI lint, `docker-compose.yml`/YAML validation, and secret
scanning, on every push and pull request. CD
([`.github/workflows/cd.yml`](.github/workflows/cd.yml)) deploys the NiFi
stack, the Python extractors, and the archive scripts to srv-prod after CI
passes on `main` — refreshing dependencies and restarting the three
`zevent-*-prod` systemd units — gated by manual approval — see
[`DEPLOYMENT.md`](DEPLOYMENT.md) for setup and how it works.

## Known limitations

- **Parquet and PostgreSQL can drift.** The Parquet file is written first,
  then pushed to NiFi — if that push fails or times out, the row exists
  locally but not (yet, or ever) in Postgres. Failures NiFi does receive
  land in `nifi/dead-letter/` for manual replay; nothing reconciles the two
  sinks automatically.
- **Roster additions are live, removals aren't.** `main.py` re-fetches the
  roster every `ROSTER_REFRESH_INTERVAL_SECONDS` (default 15s) and starts
  watching any new streamer without a restart (`_roster_refresh_loop`,
  `_refresh_roster`) — but a streamer that drops out of `zevent.fr/api/` is
  never un-watched; its IRC connection and Helix polling keep running until
  the process restarts. Deliberate: a channel missing from one response is
  far more likely a transient API hiccup than a real withdrawal, and a
  streamer who actually stopped already shows up as offline via the
  metadata snapshot.
- **No raid tracking.** Dropped along with EventSub when the channel count
  made per-channel subscriptions impractical (see [Overview](#overview)).
- **No donation *count*.** `zevent.fr/api/` exposes cumulative donation
  *amount* only, per streamer and event-wide — not a count of individual
  donations. `stats.zevent.fr` has more detail but sits behind an
  interactive bot-challenge that isn't scripted around here.
- **No "viewer typology"** (lurker vs. chatter, new vs. returning) — Twitch
  doesn't expose a full viewer list, only active chatters, and only via a
  moderator-only endpoint.
- **Removed donation goals aren't deleted.** `bronze_donation_goals` is kept
  current via UPSERT, keyed on `(participation_id, goal_id)` — if a goal is
  later pulled from `zevent.gdoc.fr` upstream, its row simply stops being
  updated rather than being removed. Not expected to matter in practice
  (goals are observed to only be added as the event approaches), but a
  stale row would need a manual `DELETE`.
- **`api.ppr.evenmorestats.fr` is undocumented and unofficial.** It's the
  backend `zevent.gdoc.fr` (a third-party companion site, not run by Zevent
  itself) happens to call — reverse-engineered from its JS bundle, not a
  published/versioned API. It could change shape or move without notice;
  `zevent_donation_goals.py` isn't insulated against that beyond normal
  `aiohttp.ClientError` handling.

## Efficiency & stability recommendations

Not implemented — a review of what's genuinely missing, on top of what's
already covered in [Reliability mechanisms](#reliability-mechanisms) and
[Known limitations](#known-limitations) above:

- **Local-disk exhaustion isn't monitored.** If `ARCHIVE_REMOTE_HOST`
  becomes unreachable for an extended stretch, `archive_parquet.py`
  correctly leaves files in place rather than losing them (see
  [Reliability mechanisms](#reliability-mechanisms)) — but nothing alerts on
  `data/` growing unbounded in the meantime. A full local disk silently
  stops new Parquet writes (and, downstream, new NiFi pushes) rather than
  failing loudly. Worth a simple disk-usage check (cron or a NiFi/Postgres-
  side alert) once `archive_parquet.py` is in regular use.
- **Parquet/Postgres drift has no reconciliation.** Already flagged as a
  known limitation; concretely, a periodic job comparing Parquet row counts
  (or `batch_id`s) against `bronze_*` tables would turn "nothing reconciles
  the two sinks automatically" into a detectable, not just theoretical, gap.
- **No metrics distinct from log files.** Throughput, dead-letter rate, and
  reconnect counts (see [Performance](#performance)) are all meant to be
  pulled from `logging/` and PostgreSQL after the fact — there's no live
  counter/gauge surface during the event itself, so a developing problem
  (e.g. a stuck IRC shard) is only visible by tailing logs, not by
  glancing at a dashboard.
- **PgBouncer's own pool sizing is still unverified under real load.**
  NiFi's per-connection backpressure and its `DBCPConnectionPool` sizing
  have now been synthetically load-tested (see
  [Performance](#performance) — ~2,700–3,000 rows/s sustained), but that
  only exercises NiFi's side of the JDBC connection; PgBouncer's own pool
  config on `srv-db` (not managed by this repo) against that same
  throughput hasn't been separately verified.

## Future data analysis tooling

A proposition, not an implementation — this repo currently has no
visualization layer at all; it ends at the `bronze_*` tables in PostgreSQL
on `srv-db` (managed outside this repo, see
[ARCHITECTURE.md](ARCHITECTURE.md)), plus the Parquet copy now also fed
into cold storage by `archive_parquet.py`. The main risk for any future
analysis tool is naively loading "all the data" into memory client-side
(a browser tab, a `pandas.read_parquet` glob, an unpaginated dashboard
query) instead of pushing aggregation down to where the data already lives:

- **Ad-hoc analysis: DuckDB.** Its `postgres_scanner` extension can query
  `bronze_*` directly with no ETL step, and it can also query the archived
  Parquet files (locally, or over `httpfs`/an SSH-mounted path) without
  materializing them fully in memory — columnar, with predicate/projection
  pushdown. This is the natural tool for one-off exploration after the
  event, against either sink.
- **Interactive/scripted analysis: Polars.** Prefer a `LazyFrame` pipeline
  (filter/aggregate, `.collect()` only at the end) over
  `pandas.read_parquet()` on a whole directory glob, which forces the full
  dataset into memory up front regardless of how much of it the analysis
  actually needs.
- **A dashboard, if one is built:** either Streamlit/Evidence reading
  cached, already-aggregated DuckDB/Postgres queries (never a raw
  `SELECT *` bound to page load), or a no-code BI tool (Grafana/Metabase)
  pointed straight at Postgres — consistent with Postgres already being the
  source of truth outside this repo, rather than re-deriving one from
  Parquet.
