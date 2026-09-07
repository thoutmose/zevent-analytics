# Incidents

A log of production incidents that caused data loss or an outage — what
broke, how bad it was, and what changed as a result. Companion to
[`ARCHITECTURE.md`](ARCHITECTURE.md)'s dated tuning/drift entries: those
cover changes made *while* getting the pipeline production-ready; this file
covers things that broke *in production* after that point.

## 2026-09-04 — IRC chat capture silently stalled for ~7h20m (no heartbeat)

**Severity: high.** ~7h20m of live chat data loss for the actual event
window this pipeline exists to capture, with zero alerting — found by a
human noticing the gap, not by anything in this repo.

### Timeline (UTC)

- **01:23:35** — srv-prod hit a ~5min DNS/network outage
  (`socket.gaierror: Temporary failure in name resolution`, `Network is
  unreachable`). Also hit `zevent_api.py` and `zevent_donation_goals.py`,
  running on the same host.
- **~01:29** — `zevent_api.py` and `zevent_donation_goals.py` self-healed
  via their normal `aiohttp` request timeouts and resumed polling. All 7 of
  `main.py`'s IRC shard connections did not — every `ChatConnection` had
  gone silently dead.
- **01:23 – 08:44** — `bronze_live_chat` received zero new rows. No error,
  warning, or reconnect-attempt log line during this window: the process
  was alive, Helix polling in the same process was healthy, and nothing
  indicated the chat path had failed.
- **08:44** — a human restarted `zevent-main-prod.service` by hand after
  noticing the gap. Chat capture resumed immediately.

### Context and root cause

`main.py`'s `ChatConnection._connect_once()` (`main.py:340`, pre-fix) opened
its websocket with `session.ws_connect(IRC_WS_URL)` — `aiohttp`'s
`heartbeat` argument defaults to `None`, i.e. no WS ping/pong keepalive.

When the network blip killed the underlying TCP connections, there was
nothing to notice: `aiohttp` doesn't independently detect a dead socket, and
`async for msg in ws:` simply blocked forever waiting for bytes that would
never arrive. No exception was ever raised, so `ChatConnection.run()`'s
existing jittered-backoff reconnect logic (`main.py:319-334` — see
[README.md, "Reliability mechanisms"](README.md#reliability-mechanisms))
never fired; there was nothing for it to catch. All 7 shards failed the same
way, simultaneously, for the same reason.

The same process's Helix roster/metadata polling used plain HTTP requests
with normal timeouts, and recovered on its own within minutes — proving the
stall was specific to the heartbeat-less IRC path, not the network blip
itself (which was over in ~5 minutes) or the process as a whole.

This is the same class of failure as [[nifi-dbcp-not-read-from-env]] and the
`.env.postgres` drift — a dependency fails in a way that produces no log
line and no exception, so nothing downstream (alerting, a human tailing
logs, the existing retry logic) ever gets a signal to act on.

### Impact

- `bronze_live_chat` got zero rows across all channels from 01:23 to 08:44
  UTC (~7h20m) — during what would be live event traffic, not idle time.
- 27 channels were actively being captured during the gap.
- No alerting fired. The gap was only caught by a human noticing the
  absence of expected activity, well after the fact.

### Fix

Committed 2026-09-04, `02e0431` —
`fix(main): add IRC websocket heartbeat to detect dead chat connections`:

- `session.ws_connect(IRC_WS_URL, heartbeat=30)` in `main.py` — `aiohttp`
  now pings the connection every 30s and closes it if unanswered, which
  feeds back into the existing reconnect-with-backoff loop instead of
  bypassing it.
- A native dbt source `freshness:` block on `bronze_live_chat`
  (`loaded_at_field: ingested_at`, `warn_after: 15 minutes`,
  `error_after: 60 minutes`) in `dbt/models/staging/_staging__sources.yml`,
  as a backstop for a *different* future stall this specific fix wouldn't
  catch.

**Residual gap:** `dbt source freshness` is a separate CLI invocation, not
run by `dbt build` — see [README.md, "Data transformation
(dbt)"](README.md#data-transformation-dbt). `run_dbt.sh` only calls
`dbt build`, so this freshness check currently has no automatic trigger
wiring it to an alert; it has to be run manually (or via the `dbt (manual)`
GitHub Actions workflow) to be useful even now.

### Recovery

22,718 of the lost messages, across 23 of the 27 affected channels, were
recovered from Twitch VOD chat-replay (`TwitchDownloaderCLI`, run on
srv-dev rather than srv-prod, to avoid bulk-scraping from the same IP the
live Helix polling depends on) and bulk-loaded into `bronze_live_chat` under
`batch_id = 'dc7619b5-548a-45b6-b791-9d961add303e'` — that `batch_id` is the
marker for "recovered via VOD replay, not live IRC capture" if this data is
ever audited later. `ingested_at` on these rows reflects the 2026-09-04
~10:00 UTC recovery time, while `message_sent_at`/`captured_at` reflect the
original 01:23–08:44 window.

4 channels (`drakeoz_`, `koala_cosy`, `lagameuseelle`, `manglouste`) had no
matching VOD for their live session during the gap (likely VOD storage
disabled) and remain unrecovered.

### If this happens again

Don't assume it's the same gap — that specific one is closed. Check, in
order:

1. `journalctl -u zevent-main-prod.service` for the actual trigger (network
   blip, Twitch-side issue, process crash, etc.).
2. `systemctl show zevent-main-prod.service -p
   ExecMainStartTimestamp,NRestarts` and `git log -1 -- main.py`, to confirm
   the code actually running on srv-prod matches what's committed — this
   incident's fix only helps if it's deployed.

## 2026-09-05 — PostgreSQL cache hit ratio dropped to ~70% on zevent (bgwriter throughput capped)

**Severity: low.** No outage, no data loss — cache hit ratio on 3
freshly-rebuilt dbt tables dropped to 44–60% during a full-refresh +
ingestion write burst. DB-wide hit ratio stayed healthy (99.3%+) throughout;
surfaced by a human noticing the table-level ratio late afternoon, not by
any alert.

### Timeline (UTC)

- **since 2026-09-02** — `bgwriter_lru_maxpages` had been left at its
  default (100 pages/cycle, ≈4MB/s) even as ingestion load grew
  (COPY-to-staging + merge every 1–2s, targeting 10k TPS).
- **18:00–18:02** — `dbt build` full-refreshed
  `int_donations__leaderboard_rank`, `int_chat__inter_arrival`, and
  `mart_streams__liveness_reconciliation`, writing ~900MB of new pages.
- **18:21–18:39** — the following checkpoint absorbed the resulting
  dirty-page backlog: 101,522 buffers written (19.4% of all of
  `shared_buffers`) in a single cycle — the largest spike in the log.
- **~19:59** — diagnosis confirmed live via logs, `pg_stat_bgwriter`, and
  `pg_statio_user_tables`.
- **20:07:23** — `bgwriter_lru_maxpages = 1000` (10x) deployed in
  `conf.d/zevent-tuning.conf` on srv-db and reloaded via SIGHUP — no
  connection drop, ingestion never interrupted.
- **20:09–20:12** — `dbt build` re-run to validate: the
  `buffers_backend`/`buffers_clean` ratio improved from 21:1 to 8.6:1.

### Context and root cause

`bgwriter_lru_maxpages`, capped at its default of ~4MB/s, couldn't keep up
with the combined write burst of continuous ingestion and the dbt
full-refresh. Backends ended up cleaning their own dirty pages instead of
the bgwriter doing it ahead of time (cumulative `buffers_backend` ran ~20x
`buffers_clean`), which is what showed up as a dropped cache hit ratio on
the tables being rebuilt.

### Impact

- Hit ratio on the 3 rebuilt tables fell to 44%, 53%, and 60% individually
  (vs. 91–100% on the large `bronze_*` tables).
- DB-wide hit ratio stayed at 99.3%+ throughout — the degradation was only
  visible at the per-table, real-time level, not in any cumulative average.
- No outage, no data loss. Real impact was near-zero: the whole 6GB database
  fits in the OS page cache, so no actual disk read was involved — confirmed
  via `pg_stat_io` (reads=131 for the relevant context).

### Fix

`bgwriter_lru_maxpages = 1000` in `conf.d/zevent-tuning.conf` on srv-db,
applied via a SIGHUP reload (no restart, no dropped connections).

**Non-issue, ruled out explicitly:** the low ratio on the 3 int_/mart_
tables persists even after the fix. This is expected PostgreSQL behavior —
`CREATE TABLE AS` uses a 16MB `BAS_BULKWRITE` ring buffer that isn't
tunable — not a symptom of the original problem, confirmed via `pg_stat_io`.
Forcing it to 100% would degrade cache protection for genuinely hot tables
(`bronze_live_chat`) for a cosmetic gain.

### Status

Closed. Two unrelated items noted for later, not investigated further here:
`pg_stat_statements` is not installed, and the `zevent_readonly` role is
missing permissions on `dbt_run_results` and `bronze_live_chat`.

## 2026-09-05 (evening) — Repeated full-table scans of `bronze_live_chat`, and unresolved `work_mem` spill on dbt runs

**Severity: low.** No outage, no data loss, no errors. A follow-up dig, same
day as the bgwriter entry above, to confirm that fix actually resolved the
underlying pressure. It found two further (and now fixed) sources of
unnecessary I/O against `bronze_live_chat`, plus one still-open source of
dbt-specific disk spill.

### Finding 1: `stg_bronze__live_chat` re-scanned bronze_live_chat on every downstream ref

`stg_bronze__live_chat` was a plain passthrough view (`select * from
source`) — dbt's default staging materialization. 14 downstream
`int_*`/`mart_*` models, plus one duplicated CTE, reference it directly. As
a view, every one of those refs re-ran the underlying scan against the full
2GB `bronze_live_chat` table, so a single `dbt build` did ~15 full
sequential scans of the same table instead of one.

- **Detected via:** `pg_stat_user_tables.seq_scan` on `bronze_live_chat`
  running ~90x higher than `blks_read` cumulative would justify for the
  table's actual size.
- **Fix:** `{{ config(materialized='table', indexes=[...]) }}` on
  `stg_bronze__live_chat` (see
  [`dbt/models/staging/stg_bronze__live_chat.sql`](dbt/models/staging/stg_bronze__live_chat.sql)) —
  one scan to build the table per `dbt build`, instead of one per consumer.
  Verified live: `relkind = 'r'` post-build, a single scan observed instead
  of 15.
- **Status:** Closed.

### Finding 2: a DBeaver dashboard query scanned bronze_live_chat every ~15s, continuously, outside dbt

A separate, unrelated source of the same symptom: `refresh_db.sql`, an
auto-refreshing DBeaver dashboard query (`application_name = "DBeaver
26.2.0 - SQLEditor <refresh_db.sql>"`), ran `SELECT MAX(ingested_at) FROM
bronze_live_chat` on a ~15s interval. `ingested_at` had no usable index, so
every refresh triggered a full sequential scan of the table — independent
of, and in addition to, Finding 1's dbt-time scans.

- **Detected via:** `pg_stat_activity` traced live, after ruling out the
  NiFi merge function, triggers, and postgres_exporter's own queries.
- **Considered:**
  - A BRIN index on `ingested_at` — created, but ineffective: Postgres's
    planner only applies the MIN/MAX-from-index optimization with a
    B-tree, not a BRIN (confirmed via `EXPLAIN`). Left in place
    (`bronze_live_chat_ingested_at_brin_idx`) — harmless, but doesn't fix
    this.
  - A B-tree on `ingested_at` — proposed, not applied.
  - Stopping the script — what was actually done.
- **Fix applied:** `refresh_db.sql` stopped by the user. Verified:
  `seq_scan` on `bronze_live_chat` flat, no `refresh_db.sql` session left in
  `pg_stat_activity`.
- **Status:** Closed, but not durably fixed. If `refresh_db.sql` (or the
  same query, typed by hand in a DBeaver tab) runs again, the scan comes
  back. A B-tree on `ingested_at` is the actual fix if this needs to be
  permanent — not applied here.

### Finding 3 (open): `work_mem` (32MB) too small for dbt's larger models, spilling to disk

Analytical models sorting/hashing 3-6M rows
(`int_donations__streamer_deltas`, `int_chat__emote_usage`, etc.) run under
`work_mem` sized for the ingestion OLTP path, not dbt's analytical queries —
pushing their sorts/hashes to disk instead of memory.

- **Detected via:** `pg_stat_database.temp_bytes` ≈ 114GB across ~125k temp
  files, cumulative since postmaster start (~2.5 days) — confirmed stable
  outside of dbt runs, so the spill is tied specifically to dbt, not a
  continuous drip from ingestion.
- **Complication:** there's no Postgres role dedicated to dbt —
  `zevent_user` serves both NiFi ingestion and dbt (same `POSTGRES_USER` in
  `.env`; see [`DEPLOYMENT.md`, ".env.postgres on
  srv-prod"](DEPLOYMENT.md#6-envpostgres-on-srv-prod)) — so `ALTER ROLE
  zevent_user SET work_mem = ...` would also change ingestion's
  `work_mem`. Flagged before it was applied.
- **Options considered, none applied:**
  1. A dedicated `zevent_dbt` role + grants + a PgBouncer/`profiles.yml`
     update — the correct fix, but needs work outside dbt and outside this
     repo.
  2. A dbt `on-run-start` hook doing a session-scoped `SET work_mem` —
     untested whether this holds correctly across dbt's 4 concurrent
     threads.
  3. Do nothing; freeze dbt runs until after the event.
- **Status:** Open, deferred. Impact judged low (0 errors, just extra I/O
  per run) relative to Findings 1 and 2 above. **If dbt runs do get frozen
  as a mitigation:** confirm the hourly cron that runs dbt (outside of this
  repo/environment) is paused too — otherwise it keeps running dbt on
  schedule regardless of any manually-paused runs — and note that freezing
  dbt also freezes the marts (leaderboard, donation timeseries, etc.) that
  may be shown live during the event.
