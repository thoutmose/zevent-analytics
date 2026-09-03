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
| Database Connection URL | `jdbc:postgresql://<POSTGRES_HOST>:<PGBOUNCER_PORT>/<POSTGRES_DB>?stringtype=unspecified&prepareThreshold=0&reWriteBatchedInserts=true` — host/port/db from the local `.env` on srv-dev; `zevent` for the srv-prod flow, `zevent-dev` for the srv-dev flow. `prepareThreshold=0` is required, not optional, behind PgBouncer in transaction-pooling mode — see the note below the table |
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

**`reWriteBatchedInserts=true` rewrites the JDBC driver's own batched
`INSERT`s into a single multi-values statement instead of `N` separate
round-trips**, independent of `prepareThreshold=0` — one governs server-side
statement caching, the other client-side batch shaping, and the two don't
conflict. This is what actually lets `PutDatabaseRecord`'s `Maximum Batch
Size` (`1000`, both branches) and step 5b's `MergeContent` re-batching pay
off at the JDBC layer rather than just the flowfile layer; without it the
driver still issues one `INSERT` per row even inside a single `executeBatch`
call.

`PutDatabaseRecord`'s own `Maximum Batch Size` (both branches) is `2000`
(raised from `1000` — a lower bound, not itself stress-tested at `2000`;
`MergeContent` still caps a merged flowfile at `1000` records, so this is
headroom for a future bin-size increase, not something the current pipeline
exercises today).

Enable it after creating.

### 1b. Controller service: `AvroSchemaRegistry` (`AvroSchemaRegistry (ingest streams)`)

Create at the root process group level, same as step 1. Holds one named Avro
schema per `stream` value — `live_chat`, `metadata`, `zevent_snapshot`,
`donation_goals`, `emote_catalog` — added as dynamic properties (property
name = schema name, value = the Avro schema JSON). Field lists come straight
from `sql/001_bronze_schema.sql`, `002_donation_goals.sql`,
`003_live_chat_chatter_attributes.sql`, `004_live_chat_emotes.sql`,
`005_emote_catalog.sql`, and `006_live_chat_staging.sql` — a column that's
`NOT NULL` there is a required (non-nullable) Avro field here; everything
else is `["null", "<type>"]` with `"default": null`. `batch_id`/`row_number`
are included in every schema (even `emote_catalog`, whose table doesn't have
those columns) since `nifi_client.py` always sends them on every row
regardless of stream — `PutDatabaseRecord`'s existing `Ignore Unmatched
Fields` already drops them at the SQL layer for tables that don't have a
matching column, same as it does today.

**`badges`, `emotes` (live_chat) and `streamers` (zevent_snapshot) are typed
as plain `string`, not a nested record/map** — despite being JSONB columns.
Both producers pre-serialize them to JSON text before the row ever reaches
`nifi_client.py`: `main.py`'s `_chat_row`/message handler does
`"badges": json.dumps(_parse_badges(badges), ...)` and
`"emotes": json.dumps(_parse_emotes(emotes), ...)`, and `zevent_api.py`
does the same for `"streamers"` (see the comment at `zevent_api.py:173`).
So on the wire these are JSON strings, not nested objects — an Avro schema
that typed them as a `record`/`map` would fail to parse every row that
carries one. `stringtype=unspecified` on the JDBC URL (step 1) is what lets
`PutDatabaseRecord` hand a plain string to a `jsonb` column without an
explicit cast.

Timestamp/UUID-shaped fields (`*_at`, `batch_id`, `participation_id`,
`goal_id`) are typed as `string` too, not an Avro logical type — the JSON
payload already sends them as ISO-8601/UUID text, and the JDBC driver
(again via `stringtype=unspecified`) does the actual cast to
`timestamptz`/`uuid` on write. This matches what `Infer Schema` was already
inferring for these fields; switching to an explicit schema changes where
the type comes from, not what type gets sent.

**Creating this controller service via the NiFi REST API (`POST
/nifi-api/process-groups/{id}/controller-services`) needs the right `bundle`
— don't copy it from a flow-template export.** `AvroSchemaRegistry` lives in
`nifi-registry-nar` on a stock `apache/nifi:1.24.0` image; the bundle recorded
in `flow-templates/zevent-ingest-flow.json` from an earlier export was
`nifi-record-serialization-services-nar`, which 409s with `Found bundle ...
but does not support org.apache.nifi.schemaregistry.services.AvroSchemaRegistry`
on a fresh instance. Confirmed the correct bundle for any component this way
happens instead of by matching the last export: `GET
/nifi-api/flow/controller-service-types`, filter by `type`. Found live on
srv-prod, 2026-09-03, applying this exact schema registry for the first time.

### 1c. `JsonTreeReader`'s Schema Access Strategy

Set on the reader created in step 6a/6b (shared by both `PutDatabaseRecord`
instances):

| Property | Value |
|---|---|
| Schema Access Strategy | `Use 'Schema Name' Property` |
| Schema Registry | the `AvroSchemaRegistry` from step 1b |
| Schema Name | `${stream}` |

Was `Infer Schema` — that worked, but silently mistypes a column that's
always-null in a given batch (e.g. `duration_seconds` before a channel's
first metadata poll resolves) instead of erroring, and re-infers from
scratch every batch instead of validating against one fixed shape. `${stream}`
is already set by step 3 (`EvaluateJsonPath`) before `PutDatabaseRecord`,
and matches step 1b's schema names exactly.

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

**`flow-templates/zevent-ingest-flow.json` had `upsert` set to just
`${stream:equals('donation_goals')}`** (this table's value, matching
`sql/005_emote_catalog.sql`'s intent, was never applied to the actual
processor) — every `emote_catalog` flowfile fell through to `unmatched` and
was dead-lettered instead of reaching `PutDatabaseRecord (UPSERT)`. Fixed in
the template on 2026-09-02; re-check `nifi/dead-letter/` for accumulated
`emote_catalog` flowfiles from before this fix if applying it to an
already-running instance (step 4's config change, same as any other
processor property — stop/apply/restart).

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

**One malformed row now fails the whole bin it landed in, not just itself.**
Before this processor existed, `PutDatabaseRecord (INSERT)` issued one plain
`INSERT` per row, so a bad row only failed itself. With bin-packing in front
of it, up to `Maximum Number of Entries` (1000) rows share a single JDBC
batch statement — if even one of them violates a constraint (a `NOT NULL`
column with no value, say), the whole batch fails and every row in that bin
routes to `failure` together, including the ones that would otherwise have
inserted cleanly. Found live on srv-prod, 2026-09-03, replaying dead-lettered
rows from an unrelated incident: 2,264 corrupted rows (all fields empty
except `batch_id`/`row_number`) collaterally dead-lettered 161 otherwise-good
rows they happened to share a bin with. Not something to design around here —
`PutDatabaseRecord`'s `Statement Type=INSERT` has no partial-batch-success
mode — but worth knowing before assuming a `failure` flowfile from this path
means every row in it was actually bad.

### 6a. `PutDatabaseRecord` — INSERT branch (`insert` relationship, via step 5b's `MergeContent`)

| Property | Value |
|---|---|
| Record Reader | `JsonTreeReader` (create one — see step 1b for its Schema Access Strategy config) |
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
threshold: 500,000 objects / 2 GB (originally 50,000 / 1 GB, see below) —
are exactly what README's "Performance" section stress-tested against
srv-dev on 2026-08-30, not new/unvalidated numbers. That test found a
**sustained ~2,700–3,000 rows/sec** ceiling into `bronze_live_chat` even
with this tuning applied (`stress_test.py`, measured directly against table
row growth) — concurrency alone moved the *breaking point* (503s at 2+
producers → 20% error rate at 50 concurrent producers) far more than it
moved the sustained insert ceiling, since `PutDatabaseRecord` doing one JDBC
statement per row is still the bottleneck underneath the added parallelism.
Step 5b's `MergeContent` addition targets the one-statement-per-row half of
that; step 6a/8's `bronze_live_chat_staging`
(`sql/006_live_chat_staging.sql`) targets the other half — writing into an
`UNLOGGED` table skips WAL entirely for the hot insert path, at the cost of
that table's own contents not surviving a crash (fine here: it only ever
holds a few seconds of not-yet-merged rows, and a lost row is a lost insert,
not corrupted state — `bronze_live_chat` itself stays a normal WAL-logged
table).

**Re-tested on srv-dev, 2026-09-02, after the staging-table change above:**
sustained insert rate into `bronze_live_chat` (measured the same way, direct
table row growth, not `stress_test.py`'s own reported HTTP accept rate —
that number just reflects what `ListenHTTP` queued, not what actually
landed) reached **~15,000–20,000 rows/sec**, ~5-7x the pre-staging-table
ceiling and comfortably past the 6,000 TPS target. But the connections'
50,000-object backpressure threshold turned out to be the *new* limiting
factor, well before that write ceiling: `ListenHTTP` started returning 503s
at only 25 concurrent producers (63.8% error rate, vs. the 50-producer/20%
figure the original tuning found), because `SplitJson (insert)`'s
one-flowfile-per-row output fills a 50,000-object queue almost immediately
under a burst, and backpressure there propagates back through
`RouteOnAttribute`/`EvaluateJsonPath` to `ListenHTTP` itself. Real traffic
wasn't dropped by this (main.py's requests still got a 200, just delayed
behind the queue — see `nifi_client.py`'s retry, added the same day, for the
case where a producer's request *does* get bounced), but a real ZEvent burst
large enough to hit this threshold would visibly lag before landing.
Raised every connection's threshold to 500,000 objects / 2 GB (10x) to give
substantially more burst headroom under the now-confirmed ~15-20k rows/sec
drain rate, applied live via the NiFi API with the whole process group
briefly stopped/restarted around the batch of connection updates (NiFi
rejects a connection's backpressure change while its destination processor
is running) — the pause measured under a second and produced zero
`main.py` failures. Re-running the same escalation afterward reproduced the
identical breaking point at the *new* threshold's equivalent load level,
confirming the change took effect and isn't materially different in kind,
just headroom — this is a bigger buffer before backpressure engages, not a
fix to the underlying one-flowfile-per-row bottleneck itself.

**Sustained-load caveat, found the same day (2026-09-02), that qualifies the
500,000-object threshold above: it trades 503-rejection risk for disk-
exhaustion risk under sustained (not bursty) overload, and srv-dev's disk
margin is not big enough to absorb that trade blindly.** The bursts above
(20s/stage, escalating concurrency) all self-limited to a few minutes total
and a few GB of transient disk use, recovering fully within a couple of
minutes of the burst ending. A follow-up test tried to validate *sustained*
load instead — `stress_test.py --stages 5 --stage-seconds 1800` (one stage,
30 minutes, meant to approximate a sustained ~2,900 rows/sec) — and had to
be aborted at the 5-minute mark: srv-dev's disk went from 18 GB free to
8.0 GB free in those 5 minutes (`/opt/nifi/nifi-current/content_repository`
alone reached 8.2 GB), on a trajectory to fill the disk within another ~4
minutes. Two things compounded:

1. `stress_test.py`'s concurrency isn't a rate control — each worker loops
   "build batch → POST → repeat" with no think time, so its actual rows/sec
   depends on how fast the pipeline responds, not the `--stages` number.
   The **~2,900 rows/sec figure logged for `concurrency=5` in the escalating
   test above was itself congestion-limited** (that stage ran with a
   backlog already built up from the concurrency=1/2 stages before it) —
   run standalone from an empty queue, `concurrency=5` actually drove
   **~11,000+ rows/sec**, not the ~2,900 the earlier reading suggested.
   `stress_test.py` has no `--rate`/`--rows-per-second` flag; getting a
   true fixed-rate sustained test needs one added (paced workers, not
   free-running ones).
2. At that rate, sustained rather than bursted, the 500,000-object / 2 GB
   per-connection thresholds do exactly what they were raised to do — keep
   absorbing instead of rejecting — which means NiFi keeps writing incoming
   flowfile content to `content_repository` faster than the pipeline drains
   it. A burst that stays within a couple of GB of transient content for a
   couple of minutes is fine; the same absorption behavior held open for
   30 minutes at ~11,000 rows/sec was headed well past srv-dev's 38 GB disk
   entirely.

Stopping the load generator was enough to recover cleanly with **zero data
loss**: the queue (1.3M objects at the moment of stopping) drained to 0,
`content_repository` shrank back to 18 MB, and disk returned to the 18 GB
baseline within a few minutes on its own — no crash, no dead-letter growth,
real traffic (`main.py`, sharing the same queues) caught back up to normal
lag once the backlog cleared. So the *failure mode* here is safe (it
degrades to "keeps queueing, eventually the operator has to intervene," not
"silently drops or corrupts data") — but "the operator has to intervene
before disk hits 0" is not a plan for an unattended overnight peak.

**This directly qualifies the "20M messages, hour-long peak" capacity
question:** the realistic peak-rate estimates worked out elsewhere (roughly
1,700–2,800 rows/sec for a 15–50%-of-total-in-one-hour concentration) are
still comfortably under the ~15,000–20,000 rows/sec *database* write
ceiling — that part of the earlier answer holds. What's now known to be
*not* validated is whether srv-dev's disk can sustain something approaching
that rate, or higher, for the full duration of an hour-long peak, given a
sustained rate anywhere near what caused this test to eat 10 GB in 5
minutes would exhaust the disk well before the hour is up. Before trusting
an hour-plus peak unattended:

- Add rate-pacing to `stress_test.py` (or use a separate tool) to run a
  true fixed-rate sustained test — e.g. a controlled ~2,000–3,000 rows/sec
  for the full duration expected on the day — and watch disk trend over
  the *whole* window, not just the first 5 minutes.
- Reconsider whether 500,000 objects / 2 GB per connection is the right
  number given srv-dev's actual disk size, rather than picking it purely
  to clear the concurrency=200 burst test above — a smaller increase (e.g.
  150,000–200,000) still comfortably clears that same burst test while
  capping worst-case transient disk use lower.
- Since this failure mode degrades safely (queueing, not data loss) rather
  than catastrophically, an operational alert on NiFi host disk headroom
  during the event is a cheaper mitigation than getting the threshold
  number exactly right in advance.

Applying the concurrency/pool/backpressure values:

1. **Importing the template fresh** (a new srv-dev instance, say) picks these
   values up automatically.
2. **An already-running flow** (srv-prod) does not update from this file —
   per "Importing the flow template" above, the JSON is a reference export,
   not a live sync. Apply each changed property by hand in the NiFi UI
   (right-click the processor → Configure → Scheduling tab for Concurrent
   Tasks; the `DBCPConnectionPool` controller service's Properties tab for
   the pool settings; right-click each connection → Configure → Settings tab
   for backpressure), then stop/start the processor (or the controller
   service, for the pool change) so it actually takes effect. **Or script it
   against the REST API** — this is what was actually used to bring
   srv-prod's live flow in line on 2026-09-03 (see "srv-prod drift found and
   fixed" below): fetch each component, `PUT` back its revision plus the
   changed field only, stop/start around any change to a running
   processor or enabled controller service. The one hazard doing it this way
   (vs. the UI): a `GET` masks every sensitive property as `********`, so
   never spread a fetched `properties` dict into a `PUT` body — build the
   payload from just the fields you're actually changing, or you'll silently
   overwrite something like `DBCPConnectionPool`'s `Password` with the
   placeholder itself (see "Stability findings and stress-test validation"
   below for both times this actually happened).

Also stress-tested, and now persisted via `nifi/tune-provenance-retention.sh`
(the same entrypoint script that patches provenance retention — see
`docker-compose.yml`'s `entrypoint`), rather than only ever applied live via
a `nifi.properties` edit inside the running container and lost on the next
container recreate:
`nifi.content.repository.archive.max.retention.period` (`7 days` → `2 mins`)
and `nifi.content.repository.archive.max.usage.percentage` (`50%` → `90%`).
The stock `apache/nifi` image's `start.sh` only maps a fixed, curated set of
env vars onto `nifi.properties` (`NIFI_WEB_*`, `NIFI_CLUSTER_*`,
`NIFI_JVM_HEAP_*`, etc. — see the image's `scripts/start.sh`); these two
aren't in that set, which is why the entrypoint script patches
`nifi.properties` directly, the same way it already did for provenance
retention. These two properties directly gate how aggressively NiFi
throttles writes under disk pressure (see "Stability findings and
stress-test validation" below),
and getting the override wrong is exactly the kind of change that caused the
~8.5-minute production DB-write outage noted there — reapply and confirm
with a rolling restart on one instance before trusting this on srv-prod.

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

### srv-prod drift found and fixed, 2026-09-03

srv-dev's tuning above was applied and validated on 2026-08-30/09-02; it was
never applied to srv-prod's live flow until 2026-09-03, well after the
`RouteOnAttribute` fix date below — meaning srv-prod ran the *original,
untuned* flow (every processor at `Concurrent Tasks=1`, no
`AvroSchemaRegistry`, no `MergeContent`/`ExecuteSQL`, default backpressure)
for that whole window. Diagnosing the live flow via the REST API (`GET
/nifi-api/flow/process-groups/root`, `.../controller-services`,
`/nifi-api/controller/config`) before assuming srv-dev's fixes had propagated
surfaced four bugs specific to srv-prod's copy of the flow, two of which were
silently destroying/dropping data in production:

1. **`RouteOnAttribute`'s `upsert` condition was still
   `${stream:equals('donation_goals')}`**, not the
   `${stream:in('donation_goals', 'emote_catalog')}` fix already dated
   2026-09-02 in step 4 above and already correct in
   `flow-templates/zevent-ingest-flow.json` — the template fix was never
   applied to the running instance. Every `emote_catalog` row was falling
   through to `unmatched` and dead-lettering instead of upserting.
2. **The `UpdateAttribute` dead-letter uniquifier (step 7a) didn't exist on
   srv-prod at all** — every failure/unmatched relationship routed straight
   into `PutFile`, which fails on the filename collision described in step
   7a's own header (`SplitJson` gives every row from one batch the same
   filename). This was silently destroying all but the first failing row of
   any batch, exactly the bug step 7a documents fixing — it had just never
   been fixed on this instance.
3. **`EvaluateJsonPath`'s `failure` relationship was auto-terminated**, not
   wired to the dead-letter path per step 3's own instruction — malformed
   JSON POSTed to `ListenHTTP` was silently dropped, not even reaching
   dead-letter.
4. **`PutDatabaseRecord (UPSERT)`'s `Table Name` and `Update Keys` were
   hardcoded to `donation_goals`'s literal values** (`bronze_donation_goals`
   / `participation_id, goal_id`) instead of step 6b's stream-conditional
   expression. This had zero visible effect until bug #1 above was fixed —
   `emote_catalog` rows never reached this processor before then — but once
   fixed, every `emote_catalog` upsert started failing with `Record does not
   have a value for the Required column 'participation_id'`, since it was
   being forced into `bronze_donation_goals` regardless of stream.
   `donation_goals` itself "worked" the whole time only because the
   hardcoded values happened to be exactly what it needed.

Fixed in that order (#1 and #2 as one change, #3 alongside them, #4 as a
follow-up once testing every stream's schema surfaced it) via the REST API,
stop → fix → restart, same pattern as the concurrency/pool/backpressure
changes above. Verified with the same manual `curl .../ingest` checks step
2's "Verifying it end-to-end" section describes, plus a per-stream round
trip through all 5 Avro schemas once `AvroSchemaRegistry` was added — that's
what caught bug #4, which a live-traffic-only check could have missed for a
long time given how rarely `emote_catalog` batches occur.

### Stability findings and stress-test validation

Relocated here from README.md's "Performance" section, which now keeps only
a compact summary — this is the full detail behind those numbers.

**srv-dev, 2026-08-30:**

- **Content-repository archive throttling is a whole-partition check, not a
  NiFi-specific one.** `nifi.content.repository.archive.max.usage.percentage`
  compares against the entire filesystem NiFi's content repo lives on — on a
  disk already >50% full from unrelated data, sustained high-volume writes
  stall (`Unable to write flowfile content ... waiting for archive cleanup`)
  regardless of how little NiFi itself has archived. Hit this on `srv-dev`
  (a shared 38GB disk, ~69% used from non-NiFi data) well before NiFi's own
  footprint was meaningful (its archive was 1.64MB at the time).
- **Deep queues degrade throughput non-linearly.** Once a connection's queue
  passes NiFi's in-memory swap threshold (20,000 flowfiles), it starts
  swapping to disk; a backlog that oscillates around that threshold causes
  repeated swap-out/swap-in churn that dropped measured drain throughput
  from ~3,000 rows/s to ~60 rows/s. Keeping bursts under ~10K flowfiles deep
  (or raising the swap threshold alongside backpressure) avoids the cliff.
- **`DBCPConnectionPool`'s `Password` property is masked (`********`) on
  every `GET`.** Re-submitting a fetched `properties` dict verbatim on a
  `PUT` — even to change an unrelated property like pool size — silently
  overwrites the real password with that placeholder. Caused an
  ~8.5-minute production DB-write outage on `srv-dev` during this testing
  (12:45:36–12:54:05 UTC, 2026-08-30) before being caught and reverted. Any
  future scripted config change via the NiFi API must strip or re-supply
  sensitive properties explicitly, never round-trip them. **This recurred
  on srv-prod on 2026-09-03 anyway** (see below) — documenting a footgun
  once wasn't sufficient to prevent it happening again; the actual fix is
  never spreading a fetched `properties` dict into a `PUT` body, treated as
  a hard rule for any future scripted config change, not just a note to
  remember.
- The srv-dev settings above were applied live via the NiFi REST API and a
  `nifi.properties` edit inside the running container. **Neither persists
  past a container recreate** — `docker compose up --force-recreate` (or
  any rebuild — a plain `docker restart` is fine) reverts both to NiFi's
  defaults, silently re-introducing the concurrency-of-1 ceiling, unless
  they're baked into the flow template and `docker-compose.yml`/a mounted
  `nifi.properties` respectively (which is what steps 1-8 and
  `docker-compose.yml`'s `NIFI_JVM_HEAP_*`/entrypoint script now do).

**srv-prod, 2026-09-03 (production validation):**

The full tuning set above — concurrency, backpressure (raised further to
500,000/2GB), `DBCPConnectionPool` pool size, instance thread count,
`AvroSchemaRegistry`/schema validation, `MergeContent`/`ExecuteSQL` — plus
the four bugs in "srv-prod drift found and fixed" above, were applied to
srv-prod's live flow via the same REST-API approach (scripted stop → update
→ restart). `stress_test.py --url http://localhost:8888/ingest` (default
stages) run directly against srv-prod while it was serving real traffic:

| Concurrency | Requests | Rows accepted | Errors | p50/p95/p99 ms | req/s | rows/s |
|---|---|---|---|---|---|---|
| 1 | 25,211 | 504,220 | 0% | 0/1/2 | 1,680.7 | 33,614.7 |
| 2 | 37,691 | 753,820 | 0% | 1/2/4 | 2,512.7 | 50,254.7 |
| 5 | 63,425 | 1,268,500 | 0% | 1/3/7 | 4,228.3 | 84,566.7 |
| 10 | 73,868 | 1,477,360 | 0% | 2/4/8 | 4,924.5 | 98,490.7 |
| 25 | 75,958 | 1,519,160 | 0% | 4/9/18 | 5,063.9 | 101,277.3 |
| 50 | 68,027 | 1,360,540 | 0% | 9/21/41 | 4,535.1 | 90,702.7 |
| 100 | 65,697 | 1,313,940 | 0% | 22/32/51 | 4,379.8 | 87,596.0 |
| 200 | 65,810 | 1,316,200 | 0% | 44/62/80 | 4,387.3 | 87,746.7 |

0% request failure at every stage up to 200 concurrent producers (the
untuned baseline fell over at 2), and 0 dead-letter files generated (4,445
before and after). Sustained *database* write rate — direct `bronze_live_chat`
row growth, not `stress_test.py`'s own HTTP accept count — was 3,207,161
rows landed over 147 seconds, ≈21,800 rows/sec, meeting/slightly exceeding
the ~15,000–20,000 rows/sec ceiling srv-dev's testing found, comfortably
past the 6,000 TPS Zevent target. Disk went from 100GB free to 94GB free
during the run (srv-prod has a 116GB disk, well past srv-dev's 38GB margin)
and recovered to 99GB free after test data was cleaned up (`DELETE FROM
bronze_live_chat WHERE channel LIKE 'stress-test-%'` — the table has no
index on `channel`, so this needed several passes to avoid timing out on
the ~3.2M synthetic rows; a `ctid`-batched delete works if a single
statement keeps timing out).

The `DBCPConnectionPool` masked-`Password` mistake above recurred here: a
"Max Total Connections" update re-submitted the GET response's `********`
placeholder as the real property value, causing an ~80-second production
DB-write outage (07:40:38–07:41:58 UTC) before being caught and fixed.
~4,057 rows dead-lettered during the window were identified and replayed
through `/ingest` (idempotent via `UNIQUE(batch_id, row_number)`); 2,264 of
them turned out to be unrelated, unrecoverable corruption from a concurrent
processor stop/restart (all fields empty except
`batch_id`/`row_number`/`snapshot_at`), not the password incident itself.

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

**Re-exported from srv-prod on 2026-09-03**, after applying and verifying
every optimization in "Tuning for higher throughput" above plus the four
srv-prod-specific bugs in "srv-prod drift found and fixed, 2026-09-03" — this
supersedes the earlier srv-dev-based export. Re-scrub `Database Connection
URL`/`Database User` back to `<POSTGRES_HOST>:<PGBOUNCER_PORT>/<POSTGRES_DB>`/
`<DB_USER>` placeholders before committing any future re-export (`Password`
is excluded automatically by NiFi's export — that part's never an issue,
only the two non-sensitive connection-identity fields need manual scrubbing).

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
