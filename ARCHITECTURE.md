# Architecture

Deployment topology, the NiFi flow build, and deviations from the original
design. A scrubbed export of the actual flow (no real host/credentials —
see "Importing the flow template" below) ships at
[`flow-templates/zevent-ingest-flow.json`](flow-templates/zevent-ingest-flow.json).

## Topology

| Host | Role |
|---|---|
| srv-dev | Dev machine — runs the Python extractors, this repo's checkout, and its own dev NiFi (`nifi-dev.thoutmose.me`) |
| srv-prod | Runs Apache NiFi only (`docker-compose.yml`), nothing else — `nifi.thoutmose.me` |
| srv-db | PostgreSQL + PgBouncer (`bronze_*` tables, in both a `zevent` and a `zevent-dev` database). Not managed by this repo |
| srv-npm | Reverse proxy in front of `*.thoutmose.me` hostnames |

Actual addresses (Tailscale IPs, LAN IPs, ports) deliberately aren't listed
here — this file is committed to git. Resolve hosts by their Tailscale
MagicDNS name (`tailscale status`) instead. srv-prod and srv-db share a LAN —
NiFi's `DBCPConnectionPool` connects to PgBouncer over that LAN, not
Tailscale, since both boxes sit on it directly; see the local `.env` on
srv-dev (`POSTGRES_HOST`, `PGBOUNCER_PORT`, `POSTGRES_DB`) for the real
values, which are gitignored.

CD (`.github/workflows/cd.yml`) reaches srv-prod over Tailscale (its runner
joins the tailnet via `tailscale/github-action` before any SSH step), since
srv-prod has no public IP.

### Reverse proxy

`nifi.thoutmose.me` → srv-prod `:8443`, `nifi-dev.thoutmose.me` → srv-dev's
own NiFi instance (run `docker compose up` from this repo on srv-dev for a
local/dev NiFi, same `docker-compose.yml`, own `.env`). Both need a proxy
host entry in whatever's running on srv-npm forwarding to the target's
tailnet IP and port, with SSL termination — `NIFI_WEB_PROXY_HOST` on each
NiFi instance must match the hostname the proxy forwards as `Host:`, or NiFi
rejects the request.

## Building the NiFi flow

`docker-compose.yml` only stands up a bare NiFi instance — the actual
ingestion flow is built by hand in the NiFi UI (`https://<host>:8443/nifi/`).
This mirrors the README's diagram, with one deliberate simplification noted
below.

### 1. Controller service: `DBCPConnectionPool`

Create at the root process group level (`+` in the Controller Services list,
NiFi Settings):

| Property | Value |
|---|---|
| Database Connection URL | `jdbc:postgresql://<POSTGRES_HOST>:<PGBOUNCER_PORT>/<POSTGRES_DB>` — values from the local `.env` on srv-dev; `zevent` for the srv-prod flow, `zevent-dev` for the srv-dev flow |
| Database Driver Class Name | `org.postgresql.Driver` |
| Database Driver Location(s) | `/opt/nifi/nifi-current/drivers/postgresql-42.7.4.jar` |
| Database User | `zevent_user` |
| Password | (from the local `.env` on srv-dev — enter directly in the UI, never commit it) |

Enable it after creating.

### 2. `ListenHTTP`

| Property | Value |
|---|---|
| Listening Port | `8080` (host-mapped to `8888` — see `docker-compose.yml`) |
| Base Path | (leave default) |

This is what `NIFI_WEBHOOK_URL` points at (`http://<host>:8888/`).

### 3. `EvaluateJsonPath`

| Property | Value |
|---|---|
| Destination | `flowfile-attribute` |
| (dynamic property) `stream` | `$.stream` |

Promotes the envelope's `stream` field (`live_chat` / `metadata` /
`zevent_snapshot` / `donation_goals` — see `nifi_client.push_batch`) onto the
flowfile as an attribute, for routing.

Wire its `failure` relationship (fires on content that isn't valid JSON at
all) to the dead-letter path (step 7a) too — it's auto-terminated by
default, which silently drops malformed input instead of capturing it. Found
by deliberately POSTing malformed JSON to `ListenHTTP` and checking
`nifi/dead-letter/` came up empty when it shouldn't have.

### 4. `RouteOnAttribute`

| Relationship | Condition |
|---|---|
| `insert` | `${stream:in('live_chat', 'metadata', 'zevent_snapshot')}` |
| `upsert` | `${stream:equals('donation_goals')}` |

Unmatched flowfiles go to `unmatched` — wire that to the dead-letter path
(step 7a) too, same as a downstream failure.

### 5. `SplitJson` (one instance per branch, or share one before branching — either works since it only reads `$.rows`)

| Property | Value |
|---|---|
| JsonPath Expression | `$.rows` |

Splits the envelope's `rows` array into one flowfile per row. Each row
already carries `batch_id` and `row_number` (see `nifi_client.py`), which is
what the `UNIQUE (batch_id, row_number)` constraint on the bronze tables
relies on.

### 6a. `PutDatabaseRecord` — INSERT branch (`insert` relationship)

| Property | Value |
|---|---|
| Record Reader | `JsonTreeReader` (create one, Schema Access Strategy = Infer Schema) |
| Database Connection Pooling Service | the `DBCPConnectionPool` from step 1 |
| Statement Type | `INSERT` |
| Table Name | `${stream:equals('live_chat'):ifElse('bronze_live_chat', ${stream:equals('metadata'):ifElse('bronze_metadata_snapshots', 'bronze_zevent_snapshots')})}` |

One processor handles all three INSERT-only tables via that expression
(matches the README diagram's single `PUTDB` node) — the `stream` attribute
survives `SplitJson` since children inherit the parent flowfile's attributes.

### 6b. `PutDatabaseRecord` — UPSERT branch (`upsert` relationship)

| Property | Value |
|---|---|
| Record Reader | same `JsonTreeReader` |
| Database Connection Pooling Service | the `DBCPConnectionPool` from step 1 |
| Statement Type | `UPSERT` |
| Table Name | `bronze_donation_goals` |
| Update Keys | `participation_id, goal_id` |

Matches `sql/002_donation_goals.sql`'s intent: a re-poll of an existing goal
overwrites it in place instead of appending a duplicate.

### 7a. `UpdateAttribute` (unique dead-letter filename)

| Property | Value |
|---|---|
| (dynamic property) `filename` | `${filename}-${UUID()}` |

Every failure path lands here first: `EvaluateJsonPath`'s `failure`,
`RouteOnAttribute`'s `unmatched`, and both `PutDatabaseRecord`'s `failure`.

This processor exists because of a real bug, not defensive-programming
paranoia: `SplitJson` gives every row it emits the *same* `filename`
(inherited from the one batch flowfile they were split from), and the next
step's Conflict Resolution Strategy is `fail`. Without a uniquifier here,
only the *first* failing row of a batch ever reached disk — every other row
hit a filename collision at `PutFile` and was silently destroyed (`PutFile`'s
`failure` relationship is auto-terminated, so the collision itself produced
no dead-letter file either). Confirmed on the dev instance before this was
added: 27,890 `already exists` collisions logged against only 164 files that
actually survived in `nifi/dead-letter/`.

### 7b. `PutFile` (dead-letter)

| Property | Value |
|---|---|
| Directory | `/opt/nifi/dead-letter` |

Wire step 7a's `success` relationship here — not the four failure paths
directly. This is the volume mounted to `nifi/dead-letter/` on the host (see
`docker-compose.yml`); failures land there for manual replay instead of
being silently dropped. The file on disk is the raw flowfile content only —
no attribute records *why* it failed; that's in NiFi's own log
(`nifi-app.log`, grep by timestamp) or Data Provenance, not on the file
itself.

### Deviation from the original design: no `MergeContent`

The original design (see the README's mermaid diagram) merges same-stream
flowfiles before splitting. This build skips it: each `push_batch` call from
the Python side already POSTs one complete, self-contained batch — merging
independent HTTP requests back together before `SplitJson` re-expands them
adds a real correctness question (how to validly combine multiple JSON
envelopes into one document) for a marginal throughput gain that Zevent's
actual volume (~300 streamers, polled every 15–30s) doesn't need. If a real
need for NiFi-side batching shows up later, add `MergeContent` (Merge
Strategy: Bin-Packing, Correlation Attribute: `stream`) between
`RouteOnAttribute` and `SplitJson`.

### Importing the flow template

[`flow-templates/zevent-ingest-flow.json`](flow-templates/zevent-ingest-flow.json)
is a flow definition exported straight from a running instance (NiFi's
Process Group menu → Download Flow Definition, or
`GET /nifi-api/process-groups/{id}/download`) — every processor, connection,
and relationship from the steps above, already wired. `Database Connection
URL` and `Database User` are scrubbed to placeholders (sensitive properties
like the password are never included in a NiFi export in the first place).

To use it: empty process group canvas → right-click → "Upload Flow
Definition" → select the file. Then open the imported `DBCPConnectionPool`
and fill in the real `Database Connection URL` / `Database User` / `Password`
from the local `.env`, and enable it. This is faster and less error-prone
than following steps 1–7b by hand, but keep this doc in sync with the live
flow anyway — the JSON isn't diffable in a PR the way code is, so a future
change made only in the NiFi UI will silently drift from both.

### Verifying it end-to-end

```bash
curl -X POST http://<nifi-host>:8888/ -H 'Content-Type: application/json' -d '{
  "source": "manual-test",
  "stream": "live_chat",
  "batch_id": "'"$(uuidgen)"'",
  "batched_at": "'"$(date -u +%FT%TZ)"'",
  "rows": [{"row_number": 0, "batch_id": "test", "channel": "test", "chatter": "test",
            "chatter_id": "1", "message_text": "hello", "message_sent_at": "'"$(date -u +%FT%TZ)"'",
            "captured_at": "'"$(date -u +%FT%TZ)"'"}]
}'
```

Then check `bronze_live_chat` on srv-db for the row. Also test the
dead-letter path itself, deliberately — don't just trust that it works
because it looks wired up correctly:

```bash
curl -X POST http://<nifi-host>:8888/ -H 'Content-Type: application/json' -d 'not valid json'
```

A failure should show up as a new, uniquely-named file under
`nifi/dead-letter/` within a few seconds — if the directory doesn't grow,
something upstream of `PutFile` is silently swallowing it (see step 7a).
