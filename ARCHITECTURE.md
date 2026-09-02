# Architecture

Deployment topology, the NiFi flow build, and deviations from the original
design. A scrubbed export of the actual flow (no real host/credentials —
see "Importing the flow template" below) ships at
[`flow-templates/zevent-ingest-flow.json`](flow-templates/zevent-ingest-flow.json).

## Topology

| Host | Role |
|---|---|
| srv-dev | Dev machine — runs the Python extractors, this repo's checkout, and its own dev NiFi (`nifi-dev.thoutmose.me`) |
| srv-prod | Prod extractors (`twitch-analytics-prod` checkout) + Apache NiFi (`docker-compose.yml`) — `nifi.thoutmose.me` |
| srv-db | PostgreSQL + PgBouncer (`bronze_*` tables, in both a `zevent` and a `zevent-dev` database). Not managed by this repo |
| srv-services | Cold-storage target for `archive_parquet.py`/`archive_logs.py` (see README's "Archiving old Parquet files and log backups") |
| srv-npm | Reverse proxy in front of `*.thoutmose.me` hostnames |
| srv-monitoring | Monitoring stack. Standalone — not integrated with this repo |

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
| Database Connection URL | `jdbc:postgresql://<POSTGRES_HOST>:<PGBOUNCER_PORT>/<POSTGRES_DB>?stringtype=unspecified&prepareThreshold=0` — host/port/db from the local `.env` on srv-dev; `zevent` for the srv-prod flow, `zevent-dev` for the srv-dev flow. `prepareThreshold=0` is required, not optional, behind PgBouncer in transaction-pooling mode — see the note below the table |
| Database Driver Class Name | `org.postgresql.Driver` |
| Database Driver Location(s) | `/opt/nifi/nifi-current/drivers/postgresql-42.7.4.jar` |
| Database User | `zevent_user` |
| Password | (from the local `.env` on srv-dev — enter directly in the UI, never commit it) |
| Max Total Connections | `24` (default is 8 — see "Tuning for higher throughput" below) |
| Max Wait Time | `500 millis` (default; left untouched — not part of the tested tuning below) |

**`prepareThreshold=0` disables the PostgreSQL JDBC driver's server-side
prepared statements entirely.** Without it, `PutDatabaseRecord`/`ExecuteSQL`
intermittently fail with `prepared statement "S_n" does not exist` or
`"S_n" already exists` — a statement prepared on one pooled backend
connection doesn't necessarily exist (or can collide with another client's)
on whichever physical connection PgBouncer hands out next in transaction
pooling mode. Found live on srv-dev, 2026-09-01, right after PgBouncer was
reinstalled — it silently broke the UPSERT stream and the merge function in
step 8 below until this was added.

Enable it after creating.

### 2. `ListenHTTP`

| Property | Value |
|---|---|
| Listening Port | `8080` (host-mapped to `8888` — see `docker-compose.yml`) |
| Base Path | `ingest` |
| Concurrent Tasks (Scheduling tab) | `8` (default is 1 — see "Tuning for higher throughput" below) |

This is what `NIFI_WEBHOOK_URL` points at (`http://<host>:8888/ingest`).

### 3. `EvaluateJsonPath`

| Property | Value |
|---|---|
| Destination | `flowfile-attribute` |
| (dynamic property) `stream` | `$.stream` |
| Concurrent Tasks (Scheduling tab) | `4` (default is 1 — see "Tuning for higher throughput" below) |

Promotes the envelope's `stream` field (`live_chat` / `metadata` /
`zevent_snapshot` / `donation_goals` / `emote_catalog` — see
`nifi_client.push_batch`) onto the flowfile as an attribute, for routing.

Wire its `failure` relationship (fires on content that isn't valid JSON at
all) to the dead-letter path (step 7a) too — it's auto-terminated by
default, which silently drops malformed input instead of capturing it. Found
by deliberately POSTing malformed JSON to `ListenHTTP` and checking
`nifi/dead-letter/` came up empty when it shouldn't have.

### 4. `RouteOnAttribute`

| Relationship | Condition |
|---|---|
| `insert` | `${stream:in('live_chat', 'metadata', 'zevent_snapshot')}` |
| `upsert` | `${stream:in('donation_goals', 'emote_catalog')}` |
| Concurrent Tasks (Scheduling tab) | `4` (default is 1 — see "Tuning for higher throughput" below) |

Unmatched flowfiles go to `unmatched` — wire that to the dead-letter path
(step 7a) too, same as a downstream failure.

### 5. `SplitJson` (one instance per branch, or share one before branching — either works since it only reads `$.rows`)

| Property | Value |
|---|---|
| JsonPath Expression | `$.rows` |
| Concurrent Tasks (Scheduling tab) | `4` on the `insert` branch's instance, `2` on the `upsert` branch's instance (both default to 1 — see "Tuning for higher throughput" below) |

Splits the envelope's `rows` array into one flowfile per row. Each row
already carries `batch_id` and `row_number` (see `nifi_client.py`), which is
what the `UNIQUE (batch_id, row_number)` constraint on the bronze tables
relies on.

### 5b. `MergeContent` — INSERT branch only (between `SplitJson (insert)` and `PutDatabaseRecord (INSERT)`)

| Property | Value |
|---|---|
| Merge Strategy | `Bin-Packing Algorithm` |
| Correlation Attribute Name | `stream` |
| Minimum Number of Entries | `200` |
| Maximum Number of Entries | `1000` |
| Max Bin Age | `1 sec` |
| Merge Format | `Binary Concatenation` |
| Delimiter Strategy | `Text` |
| Header File | `[` (literal text — see note below) |
| Demarcator File | `,` |
| Footer File | `]` |
| Attribute Strategy | `Keep Only Common Attributes` |

**The property is always named `Header File`/`Footer File`/`Demarcator
File` regardless of mode, and the NiFi UI always labels it just "Header"
(etc.) — but its *value* is interpreted differently depending on
`Delimiter Strategy`: literal inline text when `Delimiter Strategy=Text`
(our case, above), or an actual file path to read from when `Delimiter
Strategy=Filename`.** There is no separate property for the other mode; it's
the same key, reinterpreted. Two mistakes are easy to make here, both
tripped over live on srv-dev, 2026-09-01, in that order:

1. Typing a value into a property literally *named* `Header`/`Footer`/
   `Demarcator` (as opposed to the real `...File` property the UI happens
   to label the same way) creates a bogus dynamic property NiFi has no
   validator for, and invalidates the processor (`'Header' validated
   against '[' is invalid because 'Header' is not a supported property or
   has no Validator associated with it`) — `SplitJson (insert)`'s output
   queue backed up to its 50,000 backpressure cap because of it.
2. Fixing that by pointing the *real* `Header File` property at an actual
   file path (reasonable, given the name) silently breaks it a different
   way under `Delimiter Strategy=Text`: the literal path string itself
   becomes the header content of every merged flowfile, corrupting the
   JSON `PutDatabaseRecord (INSERT)` then fails to parse
   (`JsonParseException: ... was expecting either '*' or '/' for a
   comment`, since the path string isn't valid JSON either). No file or
   mount is needed at all — just the single characters in the table above.

Re-assembles same-`stream` rows that `SplitJson (insert)` just split apart
back into a single JSON array (via the `[`/`,`/`]` delimiters), so
`PutDatabaseRecord (INSERT)`'s `JsonTreeReader` gets a multi-record flowfile
and can issue one batched JDBC insert per bin instead of one row at a time —
see "Tuning for higher throughput" below for why this is a separate lever
from Concurrent Tasks. `upsert` deliberately skips this: donation_goals/
emote_catalog poll volume doesn't need the batching, and the `upsert`
relationship never routes through it. Wire its `merged` relationship to
`PutDatabaseRecord (INSERT)` and its `failure` relationship to the
dead-letter path (step 7a), same as every other processor here.

### 6a. `PutDatabaseRecord` — INSERT branch (`insert` relationship, via step 5b's `MergeContent`)

| Property | Value |
|---|---|
| Record Reader | `JsonTreeReader` (create one, Schema Access Strategy = Infer Schema) |
| Database Connection Pooling Service | the `DBCPConnectionPool` from step 1 |
| Statement Type | `INSERT` |
| Table Name | `${stream:equals('live_chat'):ifElse('bronze_live_chat_staging', ${stream:equals('metadata'):ifElse('bronze_metadata_snapshots', 'bronze_zevent_snapshots')})}` |
| Concurrent Tasks (Scheduling tab) | `8` (default is 1 — see "Tuning for higher throughput" below) |

One processor handles all three INSERT-only tables via that expression
(matches the README diagram's single `PUTDB` node) — the `stream` attribute
survives `SplitJson` since children inherit the parent flowfile's attributes.
**`live_chat` writes to `bronze_live_chat_staging`, not `bronze_live_chat`
itself** — see `sql/006_live_chat_staging.sql` and step 8 below; the other
two streams still write straight to their real tables, their volume doesn't
need the staging treatment.

**A schema migration that `ALTER TABLE`s one of these three existing tables
(rather than creating a new one, like `sql/003_live_chat_chatter_attributes.sql`
did to `bronze_live_chat`) needs a follow-up step this processor's config
doesn't show:** its default `Table Schema Cache Size` (100) means it already
has the table's old column list cached from earlier pushes. Applying the
migration alone changes nothing visible — new/changed columns are silently
dropped as unmatched fields (`Ignore Unmatched Fields`, no error, nothing in
dead-letter) until this processor (or all of NiFi) is stopped/restarted so it
re-reads the table's real columns. `001`/`002` never hit this, since a brand
new table has no stale cache entry to invalidate. Confirmed on srv-dev,
2026-08-31.

### 6b. `PutDatabaseRecord` — UPSERT branch (`upsert` relationship)

| Property | Value |
|---|---|
| Record Reader | same `JsonTreeReader` |
| Database Connection Pooling Service | the `DBCPConnectionPool` from step 1 |
| Statement Type | `UPSERT` |
| Table Name | `${stream:equals('donation_goals'):ifElse('bronze_donation_goals', 'bronze_emote_catalog')}` |
| Update Keys | `${stream:equals('donation_goals'):ifElse('participation_id, goal_id', 'service, scope, channel, emote_id')}` |
| Concurrent Tasks (Scheduling tab) | `2` (default is 1 — see "Tuning for higher throughput" below) |

One processor handles both UPSERT-only tables via these expressions, same
`stream`-attribute-survives-`SplitJson` reasoning as 6a. Matches
`sql/002_donation_goals.sql`/`sql/005_emote_catalog.sql`'s intent: a re-poll
overwrites an existing goal/emote row in place instead of appending a
duplicate. `bronze_emote_catalog.channel` is `NOT NULL` (a `'__global__'`
sentinel, not `NULL`, for global-scope rows) specifically so this UPSERT's
`ON CONFLICT` actually matches repeat global rows — Postgres treats `NULL` as
distinct from `NULL` in a `UNIQUE` constraint, so a nullable key column here
would silently turn every re-poll of global emotes into new duplicate inserts
instead of in-place updates. See `sql/005_emote_catalog.sql`'s header.

### 7a. `UpdateAttribute` (unique dead-letter filename)

| Property | Value |
|---|---|
| (dynamic property) `filename` | `${filename}-${UUID()}` |
| Concurrent Tasks (Scheduling tab) | `2` (default is 1 — see "Tuning for higher throughput" below) |

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
| Concurrent Tasks (Scheduling tab) | `2` (default is 1 — see "Tuning for higher throughput" below) |

Wire step 7a's `success` relationship here — not the four failure paths
directly. This is the volume mounted to `nifi/dead-letter/` on the host (see
`docker-compose.yml`); failures land there for manual replay instead of
being silently dropped. The file on disk is the raw flowfile content only —
no attribute records *why* it failed; that's in NiFi's own log
(`nifi-app.log`, grep by timestamp) or Data Provenance, not on the file
itself.

### 8. `ExecuteSQL` (merge `bronze_live_chat_staging`) — no incoming connection, timer-driven

| Property | Value |
|---|---|
| Database Connection Pooling Service | the `DBCPConnectionPool` from step 1 |
| SQL select query | `SELECT * FROM bronze_merge_live_chat_staging()` |
| Scheduling Strategy | `Timer driven` |
| Run Schedule | `1 sec` |

Not part of the row's path through the flow — it has no incoming
connection, so it runs purely on its own schedule (standard NiFi pattern for
a periodic timer; `GenerateFlowFile` uses the same trick). Every second, it
calls `bronze_merge_live_chat_staging()` (`sql/006_live_chat_staging.sql`),
which atomically drains whatever step 6a has written to
`bronze_live_chat_staging` since the last run and folds it into
`bronze_live_chat` with one bulk `INSERT ... SELECT ... ON CONFLICT DO
NOTHING`. Auto-terminate its `success`/`failure` relationships — the
output (a row count) isn't consumed by anything downstream, but check NiFi's
bulletins if rows stop landing in `bronze_live_chat`, since a `failure` here
means the merge itself errored, not a row-level problem `PutDatabaseRecord`
would dead-letter.

Ships in `flow-templates/zevent-ingest-flow.json` like every other processor
in this doc — exported from a validated running instance, not hand-written
(see "Importing the flow template").

### Tuning for higher throughput

The flow as originally built ran every processor at NiFi's default `Concurrent
Tasks: 1`, with `DBCPConnectionPool`'s `Max Total Connections: 8` and every
connection's backpressure threshold at NiFi's default 10,000 flowfiles. Since
`SplitJson` (step 5) explodes each batch into one flowfile per row before
`PutDatabaseRecord`, a single-threaded `PutDatabaseRecord` means every row
insert to srv-db is serialized through one JDBC connection at a time — this
is what actually caps end-to-end throughput, not `ListenHTTP` or the Python
side. At this untuned baseline, `ListenHTTP` itself falls over first: any 2nd
concurrent producer gets an immediate HTTP 503 (see README's "Performance"
section for the full baseline numbers).

The values now baked into `flow-templates/zevent-ingest-flow.json` and the
tables above — `ListenHTTP`/`PutDatabaseRecord (INSERT)`: 8 concurrent tasks,
`EvaluateJsonPath`/`RouteOnAttribute`/`SplitJson (insert)`: 4, `SplitJson
(upsert)`/`PutDatabaseRecord (UPSERT)`/both dead-letter processors: 2,
`DBCPConnectionPool`: 24 max connections, every connection's backpressure
threshold: 50,000 — are exactly what README's "Performance" section
stress-tested against srv-dev on 2026-08-30, not new/unvalidated numbers.
That test found a **sustained ~2,700–3,000 rows/sec** ceiling into
`bronze_live_chat` even with this tuning applied (`stress_test.py`, measured
directly against table row growth) — concurrency alone moved the *breaking
point* (503s at 2+ producers → 20% error rate at 50 concurrent producers) far
more than it moved the sustained insert ceiling, since `PutDatabaseRecord`
doing one JDBC statement per row is still the bottleneck underneath the
added parallelism. Step 5b's `MergeContent` addition targets the
one-statement-per-row half of that; step 6a/8's `bronze_live_chat_staging`
(`sql/006_live_chat_staging.sql`) targets the other half — writing into an
`UNLOGGED` table skips WAL entirely for the hot insert path, at the cost of
that table's own contents not surviving a crash (fine here: it only ever
holds a few seconds of not-yet-merged rows, and a lost row is a lost insert,
not corrupted state — `bronze_live_chat` itself stays a normal WAL-logged
table). Applying the concurrency/pool/backpressure values:

1. **Importing the template fresh** (a new srv-dev instance, say) picks these
   values up automatically.
2. **An already-running flow** (srv-prod) does not update from this file —
   per "Importing the flow template" above, the JSON is a reference export,
   not a live sync. Apply each changed property by hand in the NiFi UI
   (right-click the processor → Configure → Scheduling tab for Concurrent
   Tasks; the `DBCPConnectionPool` controller service's Properties tab for
   the pool settings; right-click each connection → Configure → Settings tab
   for backpressure), then stop/start the processor (or the controller
   service, for the pool change) so it actually takes effect.

Also stress-tested but **not yet persisted anywhere in this repo** — applied
live via a `nifi.properties` edit inside the running container, per README's
"Caveats" — and lost on the next container recreate:
`nifi.content.repository.archive.max.retention.period` (`7 days` → `2 mins`)
and `nifi.content.repository.archive.max.usage.percentage` (`50%` → `90%`).
The stock `apache/nifi` image's `start.sh` only maps a fixed, curated set of
env vars onto `nifi.properties` (`NIFI_WEB_*`, `NIFI_CLUSTER_*`,
`NIFI_JVM_HEAP_*`, etc. — see the image's `scripts/start.sh`); these two
aren't in that set, so persisting them needs either a mounted
`nifi.properties` overlay or a custom entrypoint wrapper in
`docker-compose.yml` — deliberately not added here without review, since
these two properties directly gate how aggressively NiFi throttles writes
under disk pressure (see "Stability findings" in README) and getting the
override wrong is exactly the kind of change that caused the ~8.5-minute
production DB-write outage noted there. Apply by hand (NiFi UI or a
`nifi.properties` edit + restart) until that's built and reviewed.

Two things outside this repo's control that this depends on, and that were
**not** verified as part of this change:

- **srv-db's PgBouncer/Postgres must actually have room for 24 concurrent
  connections** from this one `DBCPConnectionPool` (on top of whatever else
  uses that pool) — raising NiFi's max without checking PgBouncer's
  `pool_size`/`max_client_conn` and Postgres's `max_connections` first can
  just trade a NiFi-side bottleneck for a PgBouncer-side one. Not managed by
  this repo (see the Topology table above).
- **NiFi's own `Maximum Timer Driven Thread Count`** (Controller Settings →
  General, default `10` in 1.24.0) is the shared thread pool every
  processor's Concurrent Tasks draws from, across the whole instance. The
  values above sum to 37 threads from this flow alone (8 + 4 + 4 + 4 + 2 + 8
  + 2 + 2 + 2 + 1 for MergeContent, across all ten processors), well past
  that default — srv-dev's instance was raised to `50` before the stress
  test in README's "Performance" section (this is a whole-instance setting,
  not part of the flow's own config, so it isn't in
  `flow-templates/zevent-ingest-flow.json` and needs the same by-hand
  treatment on any other instance, fresh import or not). On an unraised
  instance, NiFi silently queues excess scheduled tasks rather than erroring,
  so throughput looks tuning-resistant instead of visibly failing.

`docker-compose.yml` also now sets `NIFI_JVM_HEAP_INIT`/`NIFI_JVM_HEAP_MAX`
(defaulting to `2g`/`4g`, overridable via `.env` — see `.env.example`)
instead of the image's stock `512m`/`512m`, since holding many more
in-flight flowfiles at once (a direct effect of the concurrency and
backpressure increases above) needs more heap headroom before NiFi starts
GC-thrashing. This one *does* take effect on container recreation (`docker
compose up -d`) without any manual UI step, but still needs the container
actually restarted on srv-prod to pick it up.

Re-run `stress_test.py` against the target instance after applying the NiFi
UI changes to confirm the new ceiling before assuming 6,000 TPS is met.

### Deviation from the original design: `MergeContent` after `SplitJson`, not before

The original design (see the README's mermaid diagram) merges same-stream
flowfiles *before* splitting — i.e. combine independent HTTP requests back
together, then re-expand. This build never did that: each `push_batch` call
from the Python side already POSTs one complete, self-contained batch, and
merging independent envelopes before `SplitJson` re-expands them raises a
real correctness question (how to validly combine multiple JSON envelopes
into one document) that Zevent's actual polling volume (~300 streamers,
every 15–30s) doesn't need an answer to.

What step 5b adds instead is `MergeContent` *after* `SplitJson (insert)`,
re-merging the same rows it just split into row-record-per-array-entry
batches before `PutDatabaseRecord (INSERT)` — this sidesteps the
envelope-merging question entirely (it's re-merging same-shape row records
it produced itself, not independent client requests) while still letting
`PutDatabaseRecord` batch multiple rows per JDBC statement instead of issuing
one statement per row. This is what "Tuning for higher throughput" above
means by "concurrency alone moved the breaking point, not the sustained
ceiling" — Concurrent Tasks parallelizes *how many* single-row inserts run
at once, but doesn't change that each one is still a single-row JDBC
statement; `MergeContent` is the lever that changes that. `upsert` doesn't
get this treatment (see step 5b) since its volume doesn't need it and
merging UPSERT semantics across rows is its own correctness question this
change deliberately didn't reopen.

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
BATCH_ID=$(uuidgen)
curl -X POST http://<nifi-host>:8888/ingest -H 'Content-Type: application/json' -d '{
  "source": "manual-test",
  "stream": "live_chat",
  "batch_id": "'"$BATCH_ID"'",
  "batched_at": "'"$(date -u +%FT%TZ)"'",
  "rows": [{"row_number": 0, "batch_id": "'"$BATCH_ID"'", "channel": "test", "chatter": "test",
            "chatter_id": "1", "message_text": "hello", "message_sent_at": "'"$(date -u +%FT%TZ)"'",
            "captured_at": "'"$(date -u +%FT%TZ)"'"}]
}'
```

`row_number`'s `batch_id` must be a real UUID matching `bronze_live_chat`'s
`UUID NOT NULL` column — a placeholder string like `"test"` fails at
`PutDatabaseRecord` (JSON-parses fine, so it's not caught until the DB
write) and silently lands in dead-letter instead of the row landing where
you'd expect. Confirmed by tripping over exactly this live on srv-dev,
2026-09-01.

Then check `bronze_live_chat` on srv-db for the row. Also test the
dead-letter path itself, deliberately — don't just trust that it works
because it looks wired up correctly:

```bash
curl -X POST http://<nifi-host>:8888/ingest -H 'Content-Type: application/json' -d 'not valid json'
```

A failure should show up as a new, uniquely-named file under
`nifi/dead-letter/` within a few seconds — if the directory doesn't grow,
something upstream of `PutFile` is silently swallowing it (see step 7a).
