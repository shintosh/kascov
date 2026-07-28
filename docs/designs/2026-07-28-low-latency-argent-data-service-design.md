# Low-Latency Argent Data Service Design

Date: 2026-07-28

Status: Approved design

## Goal

Turn self-hosted Kascov into a low-latency covenant data service for Argent
applications.

The service must:

- preprocess pending transactions for operator-approved Argent artifacts;
- publish accepted state only after an atomic canonical commit;
- preserve accepted and removed delivery history indefinitely;
- support native browser `EventSource` reconnection through `Last-Event-ID`;
- minimize acceptance-to-delivery latency; and
- characterize and then increase single-node query and stream capacity.

The first implementation keeps one logical canonical writer per Kaspa network.
SQLite remains the initial storage engine. A measured decision gate determines
whether a later clean cutoff to RocksDB is justified.

## Non-Goals

This design does not:

- accept public Argent artifact uploads;
- make pending data authoritative;
- index every non-covenant Kaspa transaction;
- shard canonical writes by covenant or application;
- introduce a second production storage engine;
- add a message broker or distributed write protocol; or
- design Kaschess-specific rules or APIs.

## Approved Decisions

The following decisions are closed:

1. Latency is the primary objective. Throughput and serving capacity follow.
2. The API exposes both pending and accepted information.
3. Pending information is best-effort and reversible.
4. Accepted information is durable, ordered, resumable, and reorg-aware.
5. Accepted delivery history remains available indefinitely.
6. Only operator-approved Argent artifacts run in preprocessing.
7. Each network has one logical canonical writer.
8. Native browser `EventSource` and `Last-Event-ID` are required.
9. Kascov is measured and tuned before any RocksDB decision.

## Evidence

This design uses these source documents:

- `README.md`;
- `docs/Architecture.md`;
- `docs/Storage Schema.md`;
- `docs/Sync Engine.md`;
- `docs/Roadmap.md`;
- Kasdex's indexer throughput and RocksDB synthetic benchmarks; and
- the Shinto Argent, Kasdex, and game research dated 2026-07-28.

The source review used Kascov commit
`04f84e0f7aad24d63765586a44877d621c5a72e7`.

Current evidence shows:

- the accepted follower sleeps for two seconds after each successful pass;
- Kaspa wRPC supports virtual-chain change notifications;
- pending transactions are polled every 250 milliseconds by default;
- accepted SSE messages are built before the corresponding store commit;
- the current SSE channel has no replay and skips lagged messages;
- the current SSE cap is 512 subscribers per network;
- SQLite uses WAL mode and a ten-second busy timeout;
- API handlers open independent SQLite read connections;
- optional KCC20 derivation runs inside canonical write transactions; and
- some cached aggregate builds have taken 7 to 21 seconds.

The existing Kasdex throughput numbers are not an accepted-covenant baseline.
Kasdex indexed every transaction in added blocks and omitted complete reorg
application.

## Domain-Neutral Capability

The required capability is an ordered, low-latency projection service over an
external canonical event source.

The service observes tentative and committed changes. It derives approved
application views. It exposes current state and a resumable change feed while
preserving one canonical write owner.

Argent is the first downstream decoder. The substrate remains independent of
Chess, tokens, or another specific application.

## Current Owners And Adjacent Features

| Responsibility | Current owner | Reusable seam | Required change |
|---|---|---|---|
| Kaspa acceptance | `kaspad` virtual chain | Accepted transaction IDs and notifications | Use notifications as immediate wakeups |
| Node type isolation | `kascov-core::node` | `ChainSource` and stable Kascov models | Add notification capability without leaking Kaspa types |
| Covenant classification | `kascov-core::sync` | Acceptance-ordered classifier and intra-block overlay | Produce a precomputed commit batch |
| Canonical persistence | `kascov-core::store::Store` | Atomic apply, rollback, cursor, WAL | Add durable delivery log and shorten writer holds |
| Pending observation | Kascov mempool poller | In-memory classification and shared broadcast | Add approved Argent preprocessing and stable pending identity |
| HTTP and SSE delivery | `kascov serve` | Axum routes, cache, SSE broadcast | Add durable replay and `Last-Event-ID` |
| Argent semantics | Approved artifact bundle | Artifact ABI and actor metadata | Add bounded generic decoder integration |
| Optional analytics | Token, search, and galaxy derivations | Deterministic projections | Remove non-critical work from canonical latency |

No current Shinto feature owns covenant ingestion. Shinto and Kaschess consume
the service later through separate integration designs.

## Current Data Paths

### Pending

```text
kaspad mempool
  -> 250 ms full-mempool poll
  -> classify new transaction IDs
  -> in-memory pending set
  -> best-effort broadcast and snapshot
```

### Accepted

```text
two-second loop
  -> get_virtual_chain_from_block(cursor)
  -> concurrent block and mergeset fetch
  -> acceptance-ordered classification
  -> callback broadcasts each event
  -> SQLite block transaction commits
  -> cursor advances
```

The broadcast-before-commit order is not suitable for authoritative Argent
consumers. A process failure can publish an event whose commit did not finish.

## Selected Architecture

```text
                               +-------------------------+
                               | approved Argent bundle  |
                               +------------+------------+
                                            |
kaspad ----> node adapter ----> normalize and classify
  |                  |                     / \
  |                  |                    /   \
  | mempool          | virtual-chain     /     \
  v                  v                  v       v
pending lane     accepted reconciler  raw facts  Argent view
  |                  |                  \       /
  |                  +---- fetch --------\-----/
  |                                      |
  v                                      v
in-memory pending                 canonical write owner
  |                                      |
  v                                      v
best-effort events          atomic state + delivery-log commit
                                         |
                                         v
                                  committed delivery hub
                                    /             \
                                   v               v
                              EventSource        query API
```

One process can follow several networks. Each network still has a separate
database, cursor, delivery sequence, and logical writer.

## Node Notification And Reconciliation

The node adapter subscribes to virtual-chain change notifications with accepted
transaction IDs enabled.

Notifications are wakeups, not persistence authority. After each notification,
the reconciler calls `get_virtual_chain_from_block` from the durable cursor.
This call returns the complete removed and added sequence.

The reconciler also runs immediately after connection and reconnection. A
bounded watchdog triggers reconciliation when notifications remain silent
past a measured healthy interval.

The watchdog does not create a second ingestion path. Every trigger invokes
the same cursor-based reconciler.

Repeated notifications coalesce into one pending wakeup. The reconciler loops
until the stored cursor reaches the node tip observed after its last pass.

## Accepted Processing

For each accepting chain block, Kascov performs these stages:

1. Fetch the accepting block and required mergeset blocks concurrently.
2. Resolve every accepted transaction body.
3. Classify transactions in node-provided acceptance order.
4. Match outputs and spends against approved Argent artifacts.
5. Decode bounded Argent state outside the database transaction.
6. Build one immutable `AcceptedBlockBatch`.
7. Apply the batch through the network's canonical writer.
8. Publish the returned committed delivery records.

An unresolved accepted transaction body fails the pass. The durable cursor
does not advance.

The batch contains raw covenant facts, UTXO changes, accepted ordering,
payloads, approved Argent results, and bounded decode failures.

Argent decode failure does not discard the raw accepted fact. The batch stores
the failure status and artifact identity. A later repair can replay from the
canonical event without asking the node for pruned data.

## Canonical Write Contract

The store exposes one domain operation for an accepted block:

```text
apply_accepted_block(batch) -> CommittedBatch
```

The operation atomically writes:

- covenant events and current UTXOs;
- approved Argent application projections;
- decode failure records;
- delivery-log records;
- processed DAA and chain cursor; and
- the next delivery sequence.

`CommittedBatch` contains only records that survived the commit. Callers cannot
publish the pre-commit `AcceptedBlockBatch`.

Rollback has the matching contract:

```text
rollback_removed_blocks(blocks) -> CommittedRemovalBatch
```

Rollback updates current state and appends durable `removed` delivery records
in the same transaction. It never rewinds or reuses a delivery sequence.

## Durable Delivery Log

Each network database owns:

- an immutable random `stream_epoch`;
- a monotonically increasing `stream_seq`; and
- an append-only delivery log.

The public cursor is opaque:

```text
<stream_epoch>:<stream_seq>
```

The sequence identifies delivery order, not Kaspa consensus order by itself.
Each record also carries accepting block, DAA, transaction index, event index,
transaction ID, and covenant ID.

Durable record kinds are:

- `accepted`;
- `removed`; and
- `projection_repaired` when a stored decode failure later succeeds.

Pending records do not enter this log. Optional analytics do not add canonical
delivery records unless their public contract later requires durable replay.

A filtered-stream `checkpoint` is a synthesized delivery frame. It references
an existing scanned high-water sequence and does not allocate a new sequence.

## Native EventSource And Last-Event-ID

The combined stream endpoint remains one-way SSE:

```text
GET /data/{network}/stream
GET /data/{network}/stream?after=<cursor>
```

Native browser `EventSource` cannot set an arbitrary initial request header.
Clients use `?after=` for the first connection. Browser reconnection then sends
the last received SSE ID through the standard `Last-Event-ID` header.

Cursor precedence is:

1. a valid `Last-Event-ID` request header;
2. otherwise, a valid `after` query value;
3. otherwise, the current durable high-water cursor.

The header takes precedence because browser reconnection keeps the original
query string while advancing `Last-Event-ID` automatically.

Every durable frame includes an SSE `id:` line:

```text
id: 6f48d4...:18442
event: accepted
data: {"cursor":"6f48d4...:18442", ...}
```

Durable `removed`, `projection_repaired`, and `checkpoint` frames also include
`id:`. Pending, pending-resolution, ready, and heartbeat frames omit `id:`.
They must never reset or replace the browser's durable reconnect cursor.

The stream emits `retry: 1000` as an initial reconnect hint. Proxy and server
keep-alives use SSE comments, which do not affect `Last-Event-ID`.

Delivery is at least once. A disconnect can repeat the last durable frame.
Clients deduplicate by the opaque cursor.

### Replay And Live Handoff

The server performs this gap-free handoff:

1. Subscribe the connection to the committed delivery hub.
2. Read the current durable high-water sequence.
3. Replay stored records strictly after the client cursor through high-water.
4. Ignore queued hub records at or below that high-water sequence.
5. Deliver later committed records from the hub.

The delivery hub receives only post-commit records. The durable log remains
the recovery source.

The endpoint merges this durable hub with the separate pending hub. Pending
frames can interleave with durable replay, but they never define accepted
ordering and never change the reconnect cursor.

If a connection falls behind the bounded live buffer, the server closes it.
Native `EventSource` reconnects with `Last-Event-ID` and replays the gap. The
server never skips ahead silently.

Replay reads the delivery log in bounded pages. A long historical replay can
span several connection lifetimes. Each reconnect continues from the last SSE
ID. The paginated `/events` endpoint remains the efficient bulk-history path.

### Filtered Streams

Filters may select covenant, artifact, application, or actor identity.

The cursor remains global per network. When a filtered scan advances without
a matching record, the server periodically emits a durable `checkpoint` frame
with the scanned high-water cursor.

The checkpoint prevents every reconnect from rescanning an old unmatched
range. A client that changes its filter must start from a snapshot cursor or
an explicitly selected earlier cursor.

### Invalid Or Foreign Cursors

An epoch mismatch means the database was rebuilt or the cursor belongs to a
different database generation.

The server emits an SSE `reset` event with no durable `id:`. Its data contains
the current epoch, snapshot endpoint, and reason. The application must close
its `EventSource`, fetch a snapshot, and open a new stream from the snapshot's
cursor.

After `reset`, the server keeps that connection in a reset-only heartbeat
state until the client closes it or the normal stream lifetime expires. An
unhandled reset therefore cannot create a rapid stale-cursor reconnect loop.

A sequence ahead of the current high-water mark uses the same reset contract.
Malformed cursors fail with HTTP 400 before the SSE response starts.

The supported client library handles `reset` automatically. Raw `EventSource`
users handle the named event with `addEventListener("reset", ...)`.

### HTTP And Proxy Contract

The SSE response uses `Content-Type: text/event-stream`, `Cache-Control:
no-store`, immediate flushing, and disabled proxy buffering. Response
compression stays disabled for the live stream.

The reverse proxy must preserve the request's `Last-Event-ID` header. It must
not cache, coalesce, or buffer the stream. The reference proxy configuration
and its integration test own this proof.

Same-origin browser use remains the primary contract. Public cross-origin use
keeps the existing read-only CORS policy and requires no custom JavaScript
request headers.

## Pending Processing

The pending lane keeps a separate in-memory state per network.

For each new mempool transaction, it:

1. classifies covenant effects against the last committed state;
2. matches only approved Argent artifacts;
3. performs bounded preprocessing;
4. inserts or updates one transaction entry; and
5. publishes best-effort frames without SSE `id:`.

The pending event identity is stable within the database epoch:

```text
transaction ID + covenant ID + event ordinal
```

When an accepted record has the same transaction identity, it links to the
pending identity. Clients can replace their speculative view directly.

When a transaction leaves the mempool without acceptance, Kascov publishes a
best-effort `pending_resolved` frame with `resolution: dropped`.

Pending overflow, reconnecting, disabled, and stale status remains explicit.
No pending failure delays accepted ingestion.

## Operator-Approved Argent Artifacts

The first release loads an operator-owned artifact manifest at startup.

Each entry contains:

- application identity;
- artifact content hash;
- artifact format version;
- artifact byte location;
- enabled state; and
- bounded decode limits.

The service verifies each content hash before activation. Invalid entries fail
closed and appear in health output. The public API cannot add or modify the
manifest.

Every derived record stores the artifact content hash used for decoding.
Replacing an artifact creates a new identity. It does not reinterpret old
records silently.

Artifact state matching must use deterministic artifact metadata. The matcher
must not execute arbitrary artifact code for every unrelated covenant.

## Query API

The API exposes bounded application-oriented reads:

- current application or actor state;
- state by covenant or outpoint;
- accepted transition history;
- transaction preprocessing results;
- pending transactions by application;
- decode failures and projection cursor; and
- durable events after an opaque cursor.

Every response includes:

- network;
- stream epoch and current cursor;
- processed and tip DAA;
- projection cursor when a projection can lag; and
- freshness or incomplete-history status.

Point and bounded-page reads are the latency-critical API. Large analytics and
full snapshots retain separate cache and freshness contracts.

## Optional Projection Scheduling

Canonical covenant state and approved Argent state stay in the accepted commit.

Token accounting, search, galaxy geometry, reports, and similar aggregates
move behind a bounded projection queue when profiling proves they extend the
critical writer hold.

The canonical writer remains the only database writer. It drains projection
work between accepted batches through the same owner task.

Accepted reconciliation always has priority. Each optional projection stores
its own processed delivery cursor and reports lag through health and API
metadata.

Projection work runs in bounded chunks. The writer yields before each chunk
when accepted reconciliation has queued work.

## Public Access And Overload

Bounded read and EventSource APIs remain public in the first release. Artifact
approval, repair, migration, profiling, and benchmark controls remain private.

Every list, replay, and filter has a bounded page or batch size. The service
caps streams, replay work, request concurrency, and response bytes per process.

When capacity is exhausted, the service rejects new work with `429` or `503`
and an appropriate retry hint. It does not drop canonical delivery records.

A slow EventSource connection is closed. Its browser reconnects and resumes
from `Last-Event-ID`. Pending hints can be lost under overload because pending
state is explicitly non-authoritative.

## Performance Measurement

### Stage 0: Baseline

Benchmark unmodified Kascov against a fixed local node and chain interval.

Record timestamps for:

- node notification or poll observation;
- reconciliation start;
- transaction-body resolution;
- classification completion;
- Argent preprocessing completion;
- commit start and completion;
- delivery-hub publication; and
- client receipt.

Record resource data for:

- CPU and resident memory;
- SQLite database and WAL bytes;
- database busy time;
- fetch concurrency and RPC latency;
- write transaction duration;
- query latency during ingestion;
- stream delivery lag; and
- JSON bytes and serialization time.

### Stage 1: Storage-Independent Corrections

Measure again after:

- virtual-chain notification wakeups;
- post-commit publication;
- durable replay and `Last-Event-ID`;
- bounded approved-artifact preprocessing; and
- critical versus optional projection separation.

### Stage 2: SQLite Tuning

Tune only measured limits. Candidate changes include prepared reads, indexes,
WAL checkpoint policy, transaction shape, adaptive fetch depth, and bounded
catch-up batching.

At the live tip, commit one accepting block without an artificial batching
delay. During catch-up, adaptive batches may trade bounded latency for higher
throughput.

### Initial Performance Gates

The first controlled benchmark targets:

- accepted notification-to-delivery p95 below 250 milliseconds at the tip;
- pending observation-to-delivery p95 below 250 milliseconds;
- at least five times live-chain catch-up throughput;
- point-read p95 below 20 milliseconds during steady ingestion;
- bounded-page p95 below 50 milliseconds during steady ingestion;
- no skipped durable events under reconnect and slow-client tests; and
- no unbounded memory, WAL, queue, or stream growth.

Load tests increase request and stream concurrency until a gate fails. The
report records the maximum passing capacity on the named hardware. It does not
claim a hardware-independent user limit.

## RocksDB Decision Gate

RocksDB becomes a design candidate only when Stage 2 evidence shows that one
of these storage limits prevents the performance gates:

- canonical commit duration;
- read contention during writes;
- database or WAL growth;
- restart or recovery duration; or
- query index cost that RocksDB can serve with bounded explicit keys.

The comparison must replay the same accepted batches and queries. It must
verify identical current state, delivery log, rollback result, and cursor.

If RocksDB wins, a separate design defines one clean production cutoff. The
final code does not keep SQLite and RocksDB as permanent interchangeable
backends.

## Failure And Recovery Semantics

| Failure | Required result |
|---|---|
| Node notification missed | Cursor reconciliation catches the complete change |
| Node disconnect | Reconnect, reconcile from durable cursor, publish no guess |
| Accepted body missing | Fail the pass and retain the cursor |
| Argent decode failure | Commit raw fact plus bounded failure record |
| Database commit failure | Publish no accepted or removed event |
| Process dies after commit | Replay committed record from delivery log |
| Process dies before commit | Reconcile the same block again |
| SSE disconnect | Browser sends `Last-Event-ID`; server replays strictly after it |
| SSE client lags | Close connection; reconnect and replay from durable log |
| Database epoch changes | Emit reset instructions; client fetches a snapshot |
| Reorg | Atomically update current state and append `removed` records |
| Optional projection fails | Preserve canonical state and expose projection lag |

All apply, rollback, replay, and projection operations are idempotent for their
declared batch or cursor identity.

## Existing Database Migration

Kascov history is valuable because regular nodes prune old transaction bodies.
The migration must preserve the current SQLite database.

The upgrade adds the stream epoch, sequence, delivery log, artifact provenance,
and projection cursors in place. After the schema upgrade, one bounded offline
backfill seeds durable `accepted` records from retained canonical events.

The seed order uses stored accepting order fields. Rows that predate complete
ordering metadata receive an explicit incomplete-order marker.

The first stream discovery response reports:

- earliest available cursor;
- current cursor;
- delivery-history start time or DAA; and
- whether the pre-upgrade delivery order is complete.

Historical removed events that Kascov deleted before this upgrade cannot be
reconstructed. The API states that boundary. All later accepted and removed
delivery records remain indefinite.

After migration, only the new post-commit publication path remains. There is
no legacy hint-only accepted stream.

## Operations And Visibility

Health output adds per-network values for:

- last node notification time;
- notification-to-reconciliation delay;
- accepted cursor and stream high-water cursor;
- accepted commit p50, p95, and p99;
- pending and accepted delivery latency;
- delivery-hub subscribers, lag, and disconnects;
- artifact load and decode failures;
- optional projection cursors and lag; and
- database, WAL, queue, and backup state.

Benchmark and profiling controls remain operator-only. Public clients receive
bounded health and freshness fields, not internal traces or node credentials.

Backups must preserve the stream epoch and delivery log. Restore verification
checks the canonical cursor, high-water sequence, and sampled replay before the
instance serves traffic.

## API Capacity And Horizontal Scale

The first optimized release uses one process and one logical writer per network.
User count primarily increases reads, serialization, streams, and bandwidth.
It does not increase canonical chain writes.

The benchmark finds the maximum passing capacity on the target host. If reads
or streams saturate before ingestion, a later design may add delivery replicas.

That later design must retain one canonical writer and one ordered delivery
log. It must not add competing chain followers as write authorities.

## Architecture Discipline

### Current Owner

Kascov's acceptance-driven sync engine owns chain interpretation. Its store
owns current covenant state and rollback. The serve worker owns delivery.

### Extension Point

Extend the existing `ChainSource`, accepted batch, store apply and rollback,
and Axum SSE surfaces. Do not create a parallel indexer.

### Composition Decision

The generic substrate handles normalized covenant facts, approved decoder
results, durable delivery, and queries. Argent supplies artifact-driven decode
metadata. Kaschess remains a downstream consumer.

### Rejected Parallel Paths

- No SQLite plus RocksDB dual authority.
- No separate message-broker authority.
- No polling-only accepted path beside notification reconciliation.
- No pre-commit accepted hint stream.
- No public artifact execution path.

### Visibility Gate

Stage latency, durable cursor progress, stream replay, queue bounds, and
projection lag must be observable before optimization claims close.

### Admin Gate

Artifact approval, profiling, forced reconciliation, migration, repair, and
benchmark controls remain operator-only.

### Flatness Impact

The design keeps one chain reconciler, one writer, one canonical store, and one
delivery cursor per network. It replaces the hint-only accepted stream instead
of adding a second stream contract.

## Complexity Ledger

| Concept | Reused | Added | Unified or removed |
|---|---|---|---|
| Canonical writer | Current per-network follower and store | Post-commit batch result | Pre-commit accepted publication removed |
| Durable state | Current SQLite covenant index | Delivery log, stream epoch, artifact provenance | No second database |
| Pending state | Current in-memory feed | Approved Argent preprocessing | Still non-authoritative |
| Event delivery | Existing SSE and broadcast | Replay hub and EventSource cursor contract | Hint-only accepted semantics removed |
| Projections | Current deterministic derivations | Per-projection cursor and bounded scheduling | Non-critical work leaves critical commit |
| Repair | Current cursor reconciliation and rollback | Decode repair from durable facts | No node re-fetch for stored decode failures |
| User concepts | Covenant, event, pending, accepted | Artifact, actor, cursor, removed | No Kaschess concepts in substrate |
| Operations | Existing health and backups | Stage metrics and replay verification | One owner per network remains |

## Verification Ownership

The implementation plan must provide local commands for:

- scripted accepted, pending, reorg, restart, and replay tests;
- native `EventSource` reconnect tests with `Last-Event-ID`;
- malformed, foreign-epoch, filtered, and ahead-cursor tests;
- crash-before-commit and crash-after-commit tests;
- deterministic database migration tests;
- fixed-fixture ingestion benchmarks;
- concurrent read and SSE load tests; and
- backup and restore replay verification.

Release All, Promote Stable, Darwin publication, and generic deploy or cutover
paths remain unchanged. Feature correctness stays in local Kascov tests and
benchmark commands.

## Documentation Impact

Implementation must update:

- `docs/Architecture.md`;
- `docs/Sync Engine.md`;
- `docs/Storage Schema.md`;
- API and client documentation;
- deployment and backup instructions; and
- the roadmap entry for low-latency Argent service support.

## Design Confidence

The design is factually complete for implementation planning.

It preserves one canonical writer and one persistence authority. It defines
pending versus accepted semantics, commit ordering, reorg delivery, native
`EventSource` recovery, migration limits, observability, and the RocksDB gate.

No unresolved product or architecture decision remains. Exact schema names,
Rust types, benchmark scripts, and task boundaries belong in the implementation
plan.

## Superloop prompt

Execute the approved low-latency Argent data service design in
`docs/designs/2026-07-28-low-latency-argent-data-service-design.md` from the
current committed `shintosh/kascov` main branch.

Goal: make Kascov a low-latency covenant data service with best-effort pending
Argent preprocessing and authoritative post-commit accepted delivery.

Observable success: native browser `EventSource` reconnects with standard
`Last-Event-ID` and replays every durable accepted, removed, and checkpoint
record without gaps. Accepted events publish only after atomic state, Argent
projection, delivery-log, and cursor commit. Fixed-fixture benchmarks report
stage latency, catch-up throughput, read load, stream load, memory, database,
and WAL behavior. The initial gates in the design pass or the evidence names
the exact limiting stage.

Scope: notification-driven accepted reconciliation, bounded pending and
approved-artifact preprocessing, committed batch contracts, indefinite durable
delivery history, resumable SSE, bounded application queries, existing SQLite
migration, stage metrics, load tests, and current documentation.

Hard constraints: keep one logical canonical writer and one production store
per network. Treat notifications only as wakeups and reconcile from the durable
cursor. Never publish accepted data before commit. Pending frames must not emit
SSE `id:` fields. Durable frames must emit opaque epoch-and-sequence IDs that
native `EventSource` returns through `Last-Event-ID`. Preserve existing Kascov
history. Do not add public artifact uploads, a broker authority, dual SQLite
and RocksDB paths, Kaschess-specific substrate concepts, release-workflow tests,
or deployment gates. Benchmark and tune SQLite before any separate RocksDB
design decision.
