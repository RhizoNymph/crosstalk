# Postgres stores: detections that survive a restart (design)

Roadmap item P7.3 ("surface on Postgres"). The goal: a `crosstalk serve
--role all` that is killed and started again over the same database ends
up in the same state as one that was never interrupted. That covers the
agents, conversations, spans, matches, channels, transmissions, verdicts,
edges, the watermark, alerts and the audit, and the API shows every
detection committed before the crash.

**Status: design reviewed; workstream S (the spec changes) landed; W1
(transport) implemented on `feat/pg-w1-transport`: `PgBus` and the spool
([pg_bus.md](pg_bus.md), [publish_spool.md](publish_spool.md)), `DbLink`
in the testkit; the rest not implemented yet.** The user reviewed it (PR #103) and settled
every open question; see [Decisions](#decisions). The spec surface the
workstreams build against is in [Spec changes](#spec-changes-landed-workstream-s),
with the final names and invariant numbers (INV-1200 to INV-1221).

## Scope

- A survey of what is persisted today and what is not.
- Per store, L3 to L8: on Postgres, or derived and rebuilt at start. For
  each Postgres store: its schema, indexes and migrations, laid out the way
  `crosstalk-store` already lays them out, and any Postgres extension it
  needs (none new).
- A durable bus (`PgBus`, an `EventBus` over Postgres). It lets work in
  flight between consumers survive a restart.
- Transaction boundaries. What commits atomically, how the store outboxes
  reach the bus, idempotency and delivery guarantees per consumer, and how
  each consumer resumes after a restart (cursors, checkpoints, the L5 tick
  checkpoint and the L7 watermark).
- The publish spool: envelopes published while the database is down are
  spooled to disk and sent on recovery.
- Retention and persisted watermarks.
- Restart semantics end to end: `/readyz`, `/healthz`, the API, and what is
  recomputed.
- The test strategy: reference-model agreement, restart tests, the
  conformance suite over Postgres, and the node0 test database.
- An ordered implementation plan in parallel workstreams, with file and
  crate ownership, the spec changes that land first, and the expected
  merge conflicts.

## Non-scope

- Multi-node operation: JetStream, several pipeline processes sharing
  correlator shards, coordinated upgrades, and split `proxy` and
  `pipeline` roles talking through `PgBus` (decision Q9: P9). The design is single
  pipeline process per database, and it enforces that (see
  [Single writer](#single-pipeline-process)).
- A Postgres `BlobStore` (`PgBlobStore` in the spec's list). Bodies stay
  in `FsBlobStore` on the persistent volume, which already survives a
  restart.
- An exchange store in the spec. The P3 exchange log
  (`exchange-log.jsonl`) is on the persistent volume and survives a
  restart; replacing it stays a separate gap (decision Q8).
- Wiring L6's search corpus and alerts consumers and a real topic model
  into `Live`. This page gives their stores a place in the Postgres
  bundle. Running the consumers is P6 wiring, a prerequisite of P7.3's
  "graph, evidence and alert" end-to-end test.
- TLS to Postgres, connection pooling beyond `PoolSettings`, backups, and
  point-in-time recovery.
- TimescaleDB or any other new extension. Decision D3 stands: plain
  Postgres 18 with `vector` and `pg_trgm`.

## Survey: what is persisted today

### What `serve` runs

`crosstalk serve` (every role but `analysis`) runs a `Live` process
(`crates/gateway/src/live/`) over `crosstalk-memory`'s reference stores,
L3's `MemoryConversations` and L4's `MemoryProvenanceStore`, on an
in-process `MpscBus`. **No Postgres store is used by `serve`.** A `store`
section only makes the gateway open a pool that `/readyz` probes.
`crosstalk migrate` ensures the extensions but runs **no** layer's
migrations. `crates/gateway/src/store.rs` (`migrate_all`) still says no
layer has migrations, but five layers have them now.

What survives a restart today:

| What | Where | Survives |
| --- | --- | --- |
| Message bodies and media | `FsBlobStore` at `blobs.root` (persistent volume) | yes |
| `ExchangeCaptured` envelopes | `exchange-log.jsonl` beside the blobs (bus consumer `exchange-log`, fsynced before ack) | yes |
| Everything L3 to L8 decided | memory stores | **no** |
| Events in flight on the bus, dead letters | `MpscBus`, `DeadLetters` (memory) | **no** |
| L7 watermark | `InMemoryEdgeStore` | **no** (it restarts at 0) |
| L5 correlator state (held writes, open transmissions, held matches, delivery records) | `FlowConsumer` shards (memory) | **no** |
| L5 extraction step state (calls awaiting a result, history calls, delivered results) | `live::layers::extract::Extraction` (memory) | **no** |
| Node facts, live feed log | `crosstalk-surface` (memory) | no, by design (rebuilt or resynced) |

### Postgres implementations that exist but are not wired

Each layer crate owns `crates/<layer>/migrations/` (schema named after
the layer; `crosstalk_store::migrate`), and its model tests check it
against the `crosstalk-memory` reference.

| Layer | Crate, module | Spec traits implemented | Migrations | Outbox |
| --- | --- | --- | --- | --- |
| L3 | `crosstalk-reconstruct` `agents` (`PgAgents`) | `AgentDirectory` (merge table cached in memory, loaded at start), `IdentityResolver`, `AgentLifecycle`, `ClaimStore`, `ActivityStore`, `AgentReads` | `0001_agents.sql` | `reconstruct.outbox (seq, event)`: delivered to an awaited `EventSink` after commit, then deleted; `flush_outbox` republishes leftovers |
| L3 | `thread::pg` (`PgConversations`) | crate trait `ConversationStore` (the `Threader`'s store); **not** `ExchangePlacements` | `0002_conversations.sql`, `0003_seen_messages.sql` | none (the consumer publishes derived envelopes itself, ids from `ids::derived_event_id`) |
| L4 | `crosstalk-provenance` `store::pg` (`PgProvenanceStore`), `index::pg` (`PgFingerprintIndex`) | crate trait `ProvenanceStore`; `FingerprintIndex`. **Not** `SpanIndex` | `0001_provenance.sql`, `0002_forwarded_spans.sql` | none (engine returns envelopes with derived ids; replay-complete by status: `Pending`, `Scanned`, `Indexed`, `Failed`) |
| L5 | `crosstalk-flow` `store` (`PgChannelRegistry`, `PgTransmissionStore`, `PgShardTicks`) | `ChannelRegistry`, `ChannelTraffic`, `ChannelReads`, `AccessStore`, `ChannelDirectory` (supersessions cached), `TransmissionStore`, `TransmissionVerdicts` | `0001_flow_store.sql` (with `shard_ticks`, `cursors`) | `flow.outbox (seq, event, staged_at)`: `Relay` sends to an **unbounded mpsc** `EventSink` and deletes in the same transaction. A crash after that commit and before the forwarder publishes **loses** the events |
| L6 | `crosstalk-analysis` `search` (`PgSearchIndex`, `PgProjectionSource`), `alerts` (`PgAlertStore`) | `SearchIndex`, `SearchCorpus`, `ProjectionSource`; `AlertRuleStore`, `AlertTriage`, `AlertRuleMaintenance`, `AlertActions`, `AlertReads` | `0001_search.sql`, `0002_alerts.sql` | `analysis.outbox (seq, event)`: awaited `EventSink` (`BusSink` mints ids at the clock), rows deleted after; `flush` at consumer start |
| L7 | `crosstalk-topology` `store` (`PgEdgeStore`) | `EdgeStore`, `WatermarkRead` (watermark persisted in `topology.state`, never lowered) | `0001_topology.sql` (range-partitioned bucket tables) | `topology.outbox (seq, event \| traffic window)`: one drain at a time (advisory lock), in commit order, awaited `Announce` |

Missing Postgres implementations of spec store traits: `TopicCatalog` and
`TopicLifecycle` (`PgTopicCatalog`), `ProjectionStore`
(`PgProjectionStore`), `AuditLog`, `OperatorStore`, `SinkRegistry`
(`PgAuditLog`, `PgOperatorStore`, `PgSinkRegistry`), `EventBus` and
`DeadLetterStore` on Postgres, `FrontierSource` (`PgFrontierSource`),
`SpanIndex` on `PgProvenanceStore`, and `ExchangePlacements` on
`PgConversations`.

Envelope ids of consumer-published events: L3 and L4 derive them from
their input, so a redelivery republishes the same ids. L5's `Publisher`,
the gateway's L6 `Classifier`, L7's `BusAnnouncer` and the outbox sinks
mint fresh ULIDs at the clock, so a redelivery republishes the same
event under a **new** id.

## Design overview

```text
                              one Postgres database, one schema per layer
 proxy ─▶ capture (L1) ─▶ Ingester ─publish─▶ ┌──────────────── transport (PgBus) ────────────────┐
                                              │ events (seq, id UNIQUE, subject, at, envelope)     │
 store write txn ─▶ <layer>.outbox ─relay─▶   │ groups (name, subjects, retry, admitted_through)   │
   (stable id + at minted once,               │ deliveries (group, seq, attempt, state, at)        │
    publish is idempotent on id)              │ dead_letters (group, event id, envelope, ...)      │
                                              └──────────┬─────────────────────────────────────────┘
                     next / ack / nack per consumer group│  (LISTEN/NOTIFY wake-ups, polling fallback)
   ┌──────────────┬───────────────┬──────────────────────┼──────────────┬─────────────┬────────────┐
   ▼              ▼               ▼                      ▼              ▼             ▼            ▼
 L3 reconstruct  L4 provenance   L5 flow (checkpointed)  L6 classify   L7 topology   surface      exchange
 PgAgents        PgProvenance    PgChannelRegistry       PgTopicCatalog PgEdgeStore  relay/feed   log
 PgConversations + extraction ─▶ PgTransmissionStore     PgTransmission (watermark)  NodeCache    (jsonl)
                 step (ledger)   flow.checkpoints        Store                        (rebuilt)
                                 flow.held_writes
                                                    Surface<PgStores> ◀── HTTP API
```

The design rests on seven decisions:

1. **The stores are the source of truth.** Every fact the API shows is in
   a Postgres store when it is visible. Nothing the API reads lives only
   in memory, except caches that are rebuilt from the stores at start
   (node facts, the supersession and merge tables, provenance's token and
   k-gram caches) and the live feed log, which clients resync.
2. **A durable bus (`PgBus`) carries work between consumers.** Every
   envelope `publish` returned `Ok` for is in `transport.events`. Every
   group gets it until the group acks it, across restarts. This is the
   implementation the spec's `FrontierSource` doc already assumes ("the
   transport's delivery and dead-letter tables"). The in-process
   `MpscBus` stays for memory mode and the simulation tests.
3. **Store events reach the bus at least once, under one envelope id.**
   The per-layer outboxes stay. Each outbox row gets its envelope id and
   time once, in its own committed transaction before the first publish.
   `PgBus::publish` is idempotent on the envelope id, so a relay that
   crashes after publishing and before deleting republishes a no-op. The
   effect is that each store event is published exactly once, with no
   cross-crate transaction.
4. **Consumers are at least once with idempotent effects, and every
   derived envelope id is a function of the input.** A redelivery redoes
   the same writes (keyed by derived ids, so they are no-ops) and
   republishes the same envelopes (deduplicated at the bus). L3 and L4
   already work this way. L5, the L6 classifier and L7 are brought in line
   by a spec primitive (`EventId::derive`).
5. **The one stateful consumer, L5, checkpoints.** The correlator shards,
   held writes and uncarried matches are snapshotted to `flow.checkpoints`,
   in the same transaction as `flow.shard_ticks`. The flow group acks a
   delivery only after a checkpoint that covers it, so a restart restores
   the snapshot, and the bus redelivers everything after it. The
   extraction step's inputs to L5 become durable before L4 acks (see
   [L5](#l5-flow-checkpoint-and-restore)).
6. **One pipeline process per database.** `serve` takes a session
   advisory lock. A second pipeline process against the same database
   stays not ready instead of corrupting the correlator's single-writer
   assumption.
7. **Nothing is dropped while the database is down.** `SpoolingBus`
   wraps `PgBus` and appends every envelope it cannot publish to an
   fsynced, append-only spool on the data volume. It drains the spool in
   order, under the same envelope ids, when the database returns. Until
   the spool is empty, later envelopes queue behind it. The frontier
   counts spooled envelopes as pending
   ([The publish spool](#the-publish-spool-database-down)).

## Per-store decisions and schemas

Conventions, as the existing migrations already use them: one schema per
layer, created by `crosstalk_store::migrate` with
`"<layer>"._sqlx_migrations`; files `crates/<layer>/migrations/NNNN_name.sql`
embedded with `sqlx::migrate!("./migrations")`; runtime queries (no
`query!`) qualify every table with the schema; ids are ULID text in
`COLLATE "C"` columns (L4 keeps its 16-byte `bytea`); times are
microseconds since the epoch in `bigint`; spec values are their wire JSON
in `text` (never `jsonb`, which rewrites the text and refuses `\u0000`);
every write is one `SERIALIZABLE` transaction under `retry_serializable`
unless noted. Applied migrations are never edited; every change below is a
new numbered file. The DDL for stores that do not exist yet (bus, topics,
projections, surface, flow restart tables) is a sketch of the columns and
indexes. Its workstream finalizes it against the spec types' wire forms.

### Summary

| Layer | Store | Decision | New migration |
| --- | --- | --- | --- |
| L2 | `PgBus`, `PgDeadLetters` | **new**, Postgres | `transport/0001_bus.sql` |
| L2 | `SpoolingBus` (publish spool) | **new**, local disk (data volume) | none (files under `<data dir>/spool/`) |
| L3 | `PgAgents`, `PgConversations` | Postgres (exists); add `ExchangePlacements`; stable outbox ids | `reconstruct/0005_outbox_ids.sql` |
| L4 | `PgProvenanceStore`, `PgFingerprintIndex` | Postgres (exists); add `SpanIndex` | none |
| L4 | `TokenCache`, `KGramCache` | derived, rebuilt lazily | none |
| L5 | `PgChannelRegistry`, `PgTransmissionStore`, `PgShardTicks` | Postgres (exists); stable outbox ids; access sequence | `flow/0002_restart.sql` |
| L5 | correlator shards, held writes, uncarried matches | **checkpointed** to Postgres | `flow/0002_restart.sql` |
| L5 | extraction step state | **new ledger** in Postgres | `flow/0002_restart.sql` |
| L6 | `PgSearchIndex`, `PgAlertStore`, `PgProjectionSource` | Postgres (exists); stable outbox ids | `analysis/0003_outbox_ids.sql` |
| L6 | `PgTopicCatalog` (`TopicCatalog`, `TopicLifecycle`) | **new**, Postgres | `analysis/0004_topics.sql` |
| L6 | `PgProjectionStore` | **new**, Postgres | `analysis/0005_projections.sql` |
| L7 | `PgEdgeStore` | Postgres (exists); stable outbox ids | `topology/0002_outbox_ids.sql` |
| L7 | `PgFrontierSource` | **new**, reads only (composer) | none |
| L8 | `PgAuditLog`, `PgOperatorStore`, `PgSinkRegistry` | **new**, Postgres | `surface/0001_surface.sql` |
| L8 | `NodeCache` | derived: `NodeFeeder::rebuild` at start | none |
| L8 | live feed log | derived: new epoch at start, clients resync | none |
| L8 | `EvidenceRecords` | composer view over `PgProvenanceStore` and `PgChannelRegistry` | none |

### L2: `PgBus` and `PgDeadLetters` (crosstalk-transport, schema `transport`)

`PgBus` implements `EventBus`, `Subscription` and `DeadLetterStore` with
the group semantics `MpscBus` has: every group gets each envelope
published under one of its subjects after the group's first subscribe;
deliveries carry `attempt`; ack, nack with clamped backoff, ack timeout;
dead letters after `max_attempts`, stored before release; strict decoding.
It is the bus `serve` uses whenever `store` is configured.

```sql
-- crates/transport/migrations/0001_bus.sql (schema "transport")
CREATE TABLE events (
    seq      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id       text COLLATE "C" NOT NULL UNIQUE CHECK (length(id) = 26), -- Envelope::id
    subject  text NOT NULL,                                            -- Subject wire name
    at       bigint NOT NULL,                                          -- Envelope::at
    envelope text NOT NULL                                             -- Envelope wire JSON
);
CREATE INDEX events_by_subject ON events (subject, seq);
CREATE INDEX events_by_at ON events (at);

CREATE TABLE groups (
    name             text PRIMARY KEY,
    subjects         text[] NOT NULL,          -- sorted, distinct
    max_attempts     integer NOT NULL CHECK (max_attempts > 0),
    initial_backoff_ms bigint NOT NULL CHECK (initial_backoff_ms > 0),
    max_backoff_ms   bigint NOT NULL CHECK (max_backoff_ms >= initial_backoff_ms),
    admitted_through bigint NOT NULL           -- last events.seq admitted into deliveries
);

-- One row per admitted, unacked event of a group. An ack deletes it.
CREATE TABLE deliveries (
    group_name   text NOT NULL REFERENCES groups (name),
    seq          bigint NOT NULL REFERENCES events (seq),
    at           bigint NOT NULL,              -- the event's time (frontier)
    attempt      integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    state        text NOT NULL CHECK (state IN ('ready', 'held', 'delayed')),
    available_at bigint,                       -- 'delayed': bus-clock micros it becomes ready
    last_error   text,
    PRIMARY KEY (group_name, seq),
    CHECK ((state = 'delayed') = (available_at IS NOT NULL))
);
CREATE INDEX deliveries_ready ON deliveries (group_name, seq) WHERE state = 'ready';
CREATE INDEX deliveries_delayed ON deliveries (group_name, available_at) WHERE state = 'delayed';
CREATE INDEX deliveries_oldest ON deliveries (group_name, at);

CREATE TABLE dead_letters (
    group_name text NOT NULL,
    event_id   text COLLATE "C" NOT NULL,
    seq        bigint NOT NULL,
    at         bigint NOT NULL,
    envelope   text NOT NULL,
    attempts   integer NOT NULL CHECK (attempts > 0),
    last_error text NOT NULL,
    PRIMARY KEY (group_name, event_id)
);
-- DeadLetterStore::list: newest envelope first, ties by group.
CREATE INDEX dead_letters_listing ON dead_letters (event_id DESC, group_name DESC);
CREATE INDEX dead_letters_oldest ON dead_letters (group_name, at);
```

Control flow:

- **publish(envelope)**: encode, then `INSERT ... ON CONFLICT (id) DO
  NOTHING`, then commit, then `NOTIFY transport_events`. An envelope id
  already in the log is `Ok` and adds nothing (proposed invariant
  `transport.publish.idempotent-on-id`). `publish` never waits for group
  room: the log is the queue, so the `MpscBus` backpressure invariants
  (INV-102, INV-103) are scoped to `MpscBus` (see the spec changes).
- **subscribe(subjects, group, retry)**: upsert `groups`. A new group
  starts at `admitted_through = max(seq)`. An existing group with other
  subjects or another policy is `GroupSubjectMismatch` or
  `GroupRetryMismatch`, as now.
- **next()**: in one transaction:
  1. Admit up to `group_capacity - held` new events (`seq >
     admitted_through`, subject in the group's set, in seq order) as
     `ready`, and advance `admitted_through`.
  2. Turn due `delayed` rows `ready`.
  3. Take the lowest-seq `ready` row `FOR UPDATE SKIP LOCKED`, set it
     `held`, `attempt + 1`.

  The ack deadline is held in process memory (tokio `Instant`). When
  nothing is ready, wait for `LISTEN transport_events` or the poll
  interval (`bus.poll_ms`, default 250 ms).
- **ack(id)**: `DELETE` the row. **nack** and **timeout**: `delayed` with
  backoff, or on the last attempt insert the dead letter and delete the
  delivery in one transaction (stored before release, INV-118).
- **Restart**: at start the bus sets every `held` row `delayed` with
  `last_error = 'process restarted while holding the delivery'`. That
  counts the attempt, as `MpscBus` counts a dropped subscription. Safe
  because only one pipeline process exists
  ([Single writer](#single-pipeline-process)).
- **Bus time**: `available_at` is read from the bus's injected `Clock`,
  never from `now()` in SQL, so `PgBus` runs under paused time like
  `MpscBus`.
- **Reads for the frontier and health** (inherent, not spec):
  `group_stats()` gives each group's pending count, oldest pending `at`
  and dead letter count, plus the oldest dead letter `at`.
- **Retention**: `prune(now, keep)` deletes events every group has
  admitted and acked (`seq < min over groups of min(pending seq,
  admitted_through + 1)`) and older than `keep` (`bus.retention_ms`,
  default 7 days). The gateway's retention tick calls it. Partitioning
  `events` by `seq` range (drop a partition instead of deleting rows) is
  deferred until volume shows a need, as D3 did for buckets.

Postgres features: `FOR UPDATE SKIP LOCKED` and `LISTEN`/`NOTIFY`
(`sqlx::postgres::PgListener`, included in sqlx's `postgres` feature).
No extension.

### L3: reconstruct

- `PgAgents` and `PgConversations` already exist. The wiring builds them
  over one pool. `PgAgents`' merge table is loaded at construction, so
  the synchronous `AgentDirectory::canonical` works after a restart.
- **Add `ExchangePlacements` for `PgConversations`**: `thread_records`
  already holds each exchange's conversation and outcome, so this is a
  read and needs no new table.
- **Stable outbox ids** (`0005_outbox_ids.sql`; `0004` is the conversation reads):

  ```sql
  ALTER TABLE outbox ADD COLUMN envelope_id text COLLATE "C" CHECK (length(envelope_id) = 26),
                     ADD COLUMN at bigint;
  ALTER TABLE outbox ADD CONSTRAINT outbox_stamped CHECK ((envelope_id IS NULL) = (at IS NULL));
  ```

  This is the same change in every layer's outbox (see
  [The outbox relay](#the-outbox-relay)).
- The cursor key passed to `PgAgents` comes from the deployment secret
  (see [Cursor keys](#cursor-keys-and-id-generators)).

### L4: provenance

**Status (W3, `feat/pg-w3-provenance`): done in `crates/provenance`.**
`SpanIndex` (and `ProvenanceReads`) on `PgProvenanceStore` landed earlier
with the conversation reads (`store/reads.rs`); W3 added
`ProvenanceStore::started_at` (memory and Postgres) and
`Provenance::started_at` for the L4 stage, model-agreement tests of every
match rule on Postgres (`integration::rules`), the restart replay and
token-observation retention tests (`integration::restart`), and the
consumer's derived-id DST (INV-1202's provenance path). No migration. W8
switches `crates/gateway/src/live/layers/l4.rs` from its `started` map to
`engine.started_at(delta.exchange)` (falling back to the envelope's time
only when `None`).

- `PgProvenanceStore` and `PgFingerprintIndex` already exist. The engine
  is replay-complete: a redelivered delta returns the stored envelopes,
  and a `Scanned` exchange redoes its index writes. The one known
  imprecision stays: a crash between `observe` and `mark_indexed`
  observes again, one extra count per text until retention.
- **Add `SpanIndex` for `PgProvenanceStore`.** `spans(ids)` reads the
  `spans` table by primary key. `record` is a no-op for spans the engine
  committed, because the engine writes spans through `commit_scan`. The
  evidence page reads spans through it.
- The L4 stage's in-memory `started: BTreeMap<ExchangeId, Timestamp>`
  becomes a read of `provenance.exchanges.started_at` (a new
  `ProvenanceStore::started_at`), so a restart loses nothing.
- Token observations (the spread rule's distinct-token hashes,
  `spread.tokens_per_text`) are `FingerprintIndex::observe` calls, so they
  are already in `provenance.observations` and `provenance.observed`, aged
  out by the index's retention. `TokenCache` and `KGramCache` are
  read-through caches, empty after a restart.
- No migration.

### L5: flow

The stores (`PgChannelRegistry`, `PgTransmissionStore`, `PgShardTicks`)
exist. What is new is the consumer's durable state:

```sql
-- crates/flow/migrations/0002_restart.sql (schema "flow")

-- Stable envelope ids (see the outbox relay).
ALTER TABLE outbox ADD COLUMN envelope_id text COLLATE "C" CHECK (length(envelope_id) = 26),
                   ADD COLUMN at bigint,
                   ADD CONSTRAINT outbox_stamped CHECK ((envelope_id IS NULL) = (at IS NULL));

-- The order accesses were recorded in, so a restore re-feeds exactly the
-- accesses its checkpoint has not seen. NULL for rows recorded before the
-- migration (they predate every checkpoint).
CREATE SEQUENCE access_recording;
ALTER TABLE accesses ADD COLUMN recorded_seq bigint UNIQUE DEFAULT nextval('access_recording');

-- Writes held until their outcome is final (Extracted::Write { outcome: None }),
-- written when held, deleted when released (WriteResult or the settle tick).
CREATE TABLE held_writes (
    access_id   text COLLATE "C" PRIMARY KEY CHECK (length(access_id) = 26),
    agent       text COLLATE "C" NOT NULL,
    settles_at  bigint NOT NULL,
    extracted   text NOT NULL                -- the Extracted::Write, wire JSON
);
CREATE INDEX held_writes_settling ON held_writes (settles_at);

-- One snapshot per correlator shard: its media, open transmissions,
-- held and uncarried matches and delivery records, and the access and
-- tick it covers. Written in the transaction that writes shard_ticks.
CREATE TABLE checkpoints (
    shard            integer PRIMARY KEY CHECK (shard >= 0),
    format           integer NOT NULL CHECK (format > 0),   -- snapshot encoding version
    ticked_through   bigint NOT NULL,
    accesses_through bigint NOT NULL,                       -- accesses.recorded_seq covered
    taken_at         bigint NOT NULL,
    state            bytea NOT NULL                         -- versioned encoding of the shard
);

-- The extraction step's ledger: calls awaiting their result, history
-- calls, results already delivered, and each conversation's context.
CREATE TABLE extract_pending (
    agent        text COLLATE "C" NOT NULL,
    call_id      text NOT NULL,
    conversation text COLLATE "C" NOT NULL,
    call         text NOT NULL,              -- ToolCall wire JSON
    writes       text NOT NULL,              -- [(Locator, AccessId)] wire JSON
    PRIMARY KEY (agent, call_id, conversation)
);
CREATE TABLE extract_history (
    conversation text COLLATE "C" NOT NULL,
    call_id      text NOT NULL,
    call         text NOT NULL,
    PRIMARY KEY (conversation, call_id)
);
CREATE TABLE extract_delivered (
    agent      text COLLATE "C" NOT NULL,
    key        bytea NOT NULL CHECK (length(key) = 32),
    at         bigint NOT NULL,
    PRIMARY KEY (agent, key)
);
CREATE INDEX extract_delivered_age ON extract_delivered (at);
CREATE TABLE extract_contexts (
    conversation text COLLATE "C" PRIMARY KEY,
    context      text NOT NULL,              -- ConversationContext, wire JSON
    updated_at   bigint NOT NULL
);
CREATE TABLE extract_done (                  -- deltas whose extraction committed
    exchange text COLLATE "C" PRIMARY KEY,
    at       bigint NOT NULL
);
```

The extraction ledger is a crate-level port in `crosstalk-flow`
(`extract::ExtractionLedger`, with a memory and a Postgres
implementation), not a spec trait. The extraction step lives in the
gateway today, but it is L5's logic. **Proposed:** move it into
`crosstalk-flow` (`flow::extract::step`), generic over the ledger and a
span source port, which the gateway implements over `PgProvenanceStore`
as it does now over the memory store. Then the step, its ledger and its
tables are all in one crate. See [L5: checkpoint and restore](#l5-flow-checkpoint-and-restore)
for the control flow.

### L6: analysis

- `PgSearchIndex`, `PgAlertStore` and `PgProjectionSource` exist.
  `0003_outbox_ids.sql` makes their outbox ids stable.
- **`PgTopicCatalog`** (`0004_topics.sql`) implements `TopicCatalog` and
  `TopicLifecycle`, and is the one publisher of `TopicVersionDropped`:

  ```sql
  CREATE TABLE topic_versions (
      version     bigint PRIMARY KEY CHECK (version >= 0),
      info        text NOT NULL,          -- TopicVersionInfo wire JSON (state, fitted_at, model, ...)
      state       text NOT NULL CHECK (state IN ('fitting', 'ready', 'active', 'superseded', 'dropped')),
      pinned      boolean NOT NULL DEFAULT false
  );
  CREATE TABLE topics (
      version  bigint NOT NULL REFERENCES topic_versions (version),
      topic    text COLLATE "C" NOT NULL,
      topic_row text NOT NULL,             -- Topic wire JSON (label, terms, centroid)
      frozen_size bigint,                  -- set when the version drops
      PRIMARY KEY (version, topic)
  );
  CREATE TABLE lineage (
      from_version bigint NOT NULL, to_version bigint NOT NULL,
      lineage text NOT NULL,               -- TopicLineage wire JSON
      PRIMARY KEY (from_version, to_version)
  );
  CREATE TABLE assignments (
      version      bigint NOT NULL REFERENCES topic_versions (version),
      transmission text COLLATE "C" NOT NULL,
      topic        text COLLATE "C",       -- NULL: outlier
      from_agent   text COLLATE "C" NOT NULL,
      to_agent     text COLLATE "C" NOT NULL,
      PRIMARY KEY (version, transmission)
  );
  CREATE INDEX assignments_by_topic ON assignments (version, topic);
  CREATE SEQUENCE topic_version_numbers;   -- a failed fit's number is never reused
  ```

  Sizes are counted at read time through the `AgentDirectory`, as in
  memory (sender and reader in different clusters). Frozen sizes are
  written when a version drops, and its assignments are deleted in the
  same transaction (INV-564).
- **`PgProjectionStore`** (`0005_projections.sql`): `projection_jobs (id,
  info text, state, requested_at, lease_until, fitted_at)` with the
  partial indexes `(requested_at, id) WHERE state = 'queued'` and
  `(lease_until) WHERE state = 'fitting'`, and `projection_frames (job,
  frame bytea)`. `claim` uses `FOR UPDATE SKIP LOCKED`. Leases and
  expiry compare the `now` argument, never `now()`.
- No extension beyond `vector` (search embeddings) and `pg_trgm`.

### L7: topology

- `PgEdgeStore` exists. The watermark is persisted in `topology.state`
  and never lowered, restarts included (`topology.watermark.monotone`,
  INV-592). `0002_outbox_ids.sql` adds stable ids to its outbox rows (the
  event rows; traffic rows coalesce into one `Changed::Traffic` per drain,
  stamped when the drain stamps it).
- **`PgFrontierSource`** implements `FrontierSource`. It reads three
  layers' state (the bus's deliveries and dead letters, L5's
  `shard_ticks`, the proxy's in-flight registry), so it lives in the
  composer (`crosstalk-gateway`), not in `crosstalk-topology`:

  ```text
  ticked_through = PgShardTicks::earliest()        (flow.shard_ticks, the checkpoint's tick)
  oldest_pending = min( PgBus::group_stats() oldest pending `at` and oldest dead letter `at`
                          over groups reconstruct, provenance, flow, classify, topology
                          (TransmissionClassified{cause: Refit} excluded),
                        min started_at of exchanges in flight at the proxy (memory registry),
                        SpoolingBus::oldest_at() of envelopes still in the spool )
  ```

  A dead letter holds the watermark back on purpose (INV
  `topology.frontier.covers-pending`). The registry of exchanges in flight
  at the proxy is in memory. It is lost on a restart together with the
  connections it tracked.

### L8: surface

- **`PgAuditLog`**, **`PgOperatorStore`**, **`PgSinkRegistry`**
  (`crates/surface/migrations/0001_surface.sql`, schema `surface`;
  `crosstalk-surface` gains `sqlx` and `crosstalk-store`, which the
  architecture test allows, since `store` is `Open`):

  ```sql
  CREATE TABLE audit (
      seq    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      id     text COLLATE "C" NOT NULL UNIQUE,
      at     bigint NOT NULL,
      author text NOT NULL,                 -- AuditAuthor wire JSON
      kind   text NOT NULL CHECK (kind IN ('operator', 'config', 'export')),
      entry  text NOT NULL                  -- AuditEntry wire JSON
  );
  CREATE INDEX audit_by_at ON audit (at DESC, id DESC);
  CREATE TABLE audit_subjects (             -- AuditEntry::subjects, for filtered reads
      audit_seq bigint NOT NULL REFERENCES audit (seq),
      subject   text NOT NULL,
      PRIMARY KEY (subject, audit_seq)
  );
  -- Write-ahead record of an action call in progress (decision Q3).
  CREATE TABLE action_intents (
      id     text COLLATE "C" PRIMARY KEY,
      at     bigint NOT NULL,
      record text NOT NULL                  -- caller snapshot and action, wire JSON
  );
  CREATE TABLE operator_directory (
      singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
      directory text NOT NULL               -- OperatorDirectory wire JSON
  );
  CREATE TABLE sinks (
      id            text COLLATE "C" PRIMARY KEY,
      info          text NOT NULL,          -- SinkInfo wire JSON
      last_delivery text                    -- last DeliveryRecord wire JSON
  );
  ```

  The audit table is append-only: the store issues no `UPDATE` or `DELETE`
  on it, and the deployment's role gets no such grant (the spec's
  `AuditLog` contract). `OperatorStore::load` diffs, stores the directory
  and appends the config entries in one transaction (INV-543, INV-555).
- `NodeCache`: rebuilt at start (`NodeFeeder::rebuild` over `AgentReads`
  and `ChannelReads`), then kept current from the bus as now.
- The live feed log: memory. A restart starts a new epoch. A client that
  resumes with a cursor of the old epoch gets `Resync`, which the feed
  already defines. Nothing to persist.
- `EvidenceRecords`: a composer struct (`crosstalk-api`) reading spans
  from `PgProvenanceStore` (`SpanIndex`) and accesses and resources from
  `PgChannelRegistry`. It replaces `MemoryEvidence`, which holds spans in
  memory only.

## Transactional boundaries and delivery

### What commits atomically

| Unit | One transaction |
| --- | --- |
| A store write and the events it decides | The change plus its `<layer>.outbox` rows (exists in L3, L5, L6, L7). "A store publishes what it decides" (P0.6) holds because an event exists exactly when its change committed |
| Stamping outbox rows | `UPDATE <layer>.outbox SET envelope_id, at WHERE envelope_id IS NULL`, committed before any publish |
| Publishing an envelope | One `transport.events` insert (idempotent on id) |
| A dead letter | Insert into `dead_letters` + delete the delivery |
| An L5 checkpoint | Every shard's `checkpoints` row + its `shard_ticks` row |
| Holding or releasing a write | Insert or delete in `flow.held_writes` (a release then calls `record_access`, which is idempotent on the derived access id) |
| An extraction ledger update for one delta | The ledger rows it changes + `extract_done (exchange)` |
| An operator config load | The directory + its config audit entries |
| A topic version drop | Frozen sizes + deleted assignments + the `TopicVersionDropped` outbox row |
| L7 apply | Contribution row + bucket upserts + traffic outbox row (exists) |

What is **not** atomic, and why that is safe:

- A consumer's store writes, its publishes, and its ack are separate
  steps. Order: write, publish, then ack (`transport.consumer.ack-after-outputs`,
  INV-598; `topology.consumer.ack-after-publish`, INV-336). A crash at
  any point redelivers the input. The writes are idempotent (derived
  ids), and the publishes deduplicate (derived envelope ids).
- An operator action's effect (an L5, L3 or L6 store) and its audit entry
  (L8) are in different crates' transactions. The spec says "a database
  `SurfaceStores` makes them one transaction". That would need a
  transaction to cross trait calls, which the spec traits do not allow.
  Decided: a write-ahead intent (Q3, see the audit change below).

### The outbox relay

All four outboxes get one discipline. Each crate keeps its own relay code,
so no workstream touches another layer's crate:

```text
relay(batch):
  1. txn A: SELECT seq ... ORDER BY seq FOR UPDATE SKIP LOCKED LIMIT n   (topology: under its drain lock)
            for rows with envelope_id IS NULL: mint (clock reading, ULID generator) and UPDATE
            COMMIT                                                         -- ids are now fixed
  2. for each row in seq order: bus.publish(Envelope { id: envelope_id, at, event }).await
            -- PgBus: ON CONFLICT (id) DO NOTHING; a republish is a no-op
  3. txn B: DELETE the published rows
on start: relay until empty (before the stage subscribes)
after every committed write: relay (as now); a failure leaves rows for the next relay
```

- Every sink becomes awaited: an `EventBus` publish that has returned.
  This retires flow's unbounded mpsc `EventSink` and the gateway's
  `forward_outbox` task for the Postgres stores. That closes today's
  window where a crash between the relay's delete and the forwarder's
  publish loses events.
- Ids are minted by the relay at the injected clock's reading, never by
  the store. Stores keep taking time as an argument.
- Order: topology keeps commit order (its advisory-locked drain). The
  others keep "events of concurrent writes may be relayed out of commit
  order; readers re-query", as now.

### Delivery guarantee per consumer

| Group (stage) | Input | Effects | Guarantee | Resume |
| --- | --- | --- | --- | --- |
| `exchange-log` | `ExchangeCaptured` | jsonl append (skips known ids) | at least once, effect exactly once | bus cursor |
| `live-l3-reconstruct` | `ExchangeCaptured` | `PgAgents`, `PgConversations` (thread record keyed by exchange) | at least once; a redelivery returns the stored outcome and republishes the same derived envelopes | bus cursor |
| `live-l4-provenance` + extraction | `ExchangeCaptured`, `ConversationDelta` | provenance store and index (status machine); extraction ledger, `held_writes`, `record_access` through L5 | at least once; replay-complete; the delta is acked only after L5 confirmed its extracted inputs are durable | bus cursor |
| `live-l5-flow` | `ExchangeCaptured`, `ContentMatched`, `ChannelDiscovered`, `ChannelPromoted`, `AgentMerged`, `AgentUnmerged`, ticks | registry, transmissions; `ChannelCrossAccessed`, `TransmissionConfirmed`/`Suspected` | at least once; acks deferred to the next checkpoint; decisions idempotent (derived transmission and channel ids) | checkpoint + redelivery of unacked + re-feed of accesses after the checkpoint |
| `live-l6-classify` | `TransmissionConfirmed` | catalog assignment, `Classified` save; `TransmissionClassified` | at least once, idempotent (assignment keyed by (version, transmission)); envelope id derived | bus cursor |
| `live-l7-topology` | `TransmissionClassified`, `AccessRecorded`, `VerdictSet`, version events | `PgEdgeStore::apply` (idempotent per (version, transmission) and per access id); `EdgeUpdated` | at least once; `EdgeUpdated` before ack (INV-336); envelope id derived | bus cursor |
| `live-evidence` | `SpanOriginated`, `SpanRelayed` | none on Postgres (evidence reads the stores) | the slot is removed in Postgres mode | - |
| `live-surface-relay` | every subject but `ExchangeCaptured` | node cache, feed append | at least once; the node cache is idempotent; the feed may repeat an entry across a restart (new epoch) | bus cursor + rebuild |
| `alerts`, `search` (P6 wiring) | as their consumers define | `PgAlertStore`, `PgSearchIndex` | at least once, already idempotent (search_alerts: "redelivery after a crash") | bus cursor + outbox flush at start |

Nothing is exactly once end to end. The observable state is: each input's
effect lands once, because effects are keyed by derived ids, and each
event lands in the log once per envelope id.

### L5: flow checkpoint and restore

```text
                         ┌────────── L4 stage (group provenance) ──────────┐
ConversationDelta ─▶ engine.process ─▶ extraction step (flow::extract::step)
                         ledger txn (pending/history/delivered/contexts + extract_done)
                         ─▶ Extracted ─▶ FlowConsumer (local channel) ──reply: durable──┐
                     publish provenance envelopes ◀─────────────────────────────────────┘
                     ack the delta
FlowConsumer:
  Extracted::Read / final Write ─▶ add_resource, record_access (accesses.recorded_seq) ─▶ reply ─▶ shards
  Extracted::Write{outcome: None} ─▶ held_writes INSERT ─▶ reply; release: record_access, then DELETE
  bus delivery (group flow) ─▶ shards; delivery kept unacked
  tick(now) ─▶ shards.tick; decisions applied through the stores (idempotent)
  checkpoint (every flow.checkpoint_ms, default 10 s, or when unacked ≥ group_capacity / 2):
      txn: checkpoints (all shards, accesses_through = max recorded_seq fed, ticked_through)
           + shard_ticks (same ticked_through)
      then ack every delivery the snapshot covers
restore (start):
  load checkpoints (or empty shards when none; a format mismatch is a typed start error, Q2)
  load held_writes into HeldWrites
  re-feed accesses WHERE recorded_seq > accesses_through ORDER BY recorded_seq
  subscribe group flow: the bus redelivers every unacked delivery (attempt + 1)
  tick at the clock's now
```

- The snapshot covers exactly the inputs fed before it: acked deliveries,
  plus accesses up to `accesses_through`. Everything after is redelivered
  or re-fed. The correlator accepts evidence in any order, duplicated and
  late (its DST suite), so re-feeding across the checkpoint gives the
  decisions the uninterrupted run made. Decisions are keyed by derived
  ids, so redoing one is a no-op.
- The checkpoint is written before the acks. A crash between them
  redelivers inputs the snapshot already holds, and the correlator
  absorbs the duplicates.
- `ticked_through` in `shard_ticks` never runs ahead of a snapshot, so the
  frontier (`ticked_through - settle_after`) never claims ticks a restored
  shard has not run.
- Deferred acks keep the flow group's deliveries pending for up to one
  checkpoint interval. The frontier counts them, which holds the
  watermark back by at most that interval (seconds, against a
  `settle_after` of minutes). The flow group's ack timeout must exceed the
  checkpoint interval; the bus config checks this at start.
- Snapshot size grows with what the shards hold (up to
  `content_retention` of delivery records). The snapshot is versioned
  (`format`). Decided (Q2): a snapshot whose `format` the binary does not
  read is a start error (`FlowRestoreError::IncompatibleSnapshot`), and
  the process does not consume. The operator runs `crosstalk migrate
  --reset-correlator`, which deletes `flow.checkpoints` and resets
  `shard_ticks` to the latest stored tick. The correlator then restarts
  empty, and pairings pending at the upgrade are lost, knowingly. A binary
  that can read an older format converts it on load.

### Single pipeline process

`serve` (roles that run `Live`) takes `pg_try_advisory_lock(<crosstalk
pipeline key>)` on a dedicated connection and holds it for its lifetime.
If the lock is held elsewhere, `/readyz` reports `pipeline_lock: held
elsewhere`, and the process neither consumes nor relays. Because of this
lock, the bus can reset `held` deliveries at start, and the correlator can
assume it is the only writer of `checkpoints`. The `api` role (API only)
takes no lock and reads the stores.

### Cursor keys and id generators

- Page cursors that the Postgres stores issue are table rows (`flow.cursors`,
  `topology.cursors`) and survive a restart. Cursors keyed by a MAC (the
  surface's `CursorKey`, which is drawn at start today, and `PgAgents`'
  cursor key) would all turn `InvalidCursor` after a restart. **Proposed:**
  derive each key from the deployment secret with the spec's keyed hasher
  and a per-store domain label (`crosstalk.cursor.v1.surface`, ...). A
  secret rotation then invalidates outstanding cursors, which is accepted
  (decision Q4).
- `Live` seeds every id generator from `seed`. With persistent stores, a
  restart with the same seed replays the same random stream. Ids that
  must be reproducible are already derived from their input (envelopes,
  spans, matches, accesses, resources, discovered channels,
  transmissions). Every other minted id (agents, merges, declared
  channels, rules, alerts, audit entries) must not collide with persisted
  ones. In `serve`, generators are seeded from OS entropy (proposed
  invariant `surface.ids.unique-across-restart`). Tests keep fixed seeds
  and a clock that only moves forward.

### The publish spool (database down)

Decided (Q5): an envelope published while the database is unreachable is
**spooled to local disk and sent on recovery, never dropped**. The spool
is in `crosstalk-transport`, as `SpoolingBus<B>`, an `EventBus` decorator
over `PgBus` (`src/spool/`). It sits next to `FsBlobStore`, the crate's
other disk store. The gateway wraps `PgBus` in it whenever `store` is
configured, so every publisher goes through it: the `Ingester`'s
`ExchangeCaptured`, consumer-derived events, and outbox relays.

**What gets spooled.** In practice only capture produces new input while
the database is down:
- Every consumer's store is Postgres, so no consumer gets far enough to
  publish.
- Outbox rows are already in the database, so a relay simply waits.
- Operator actions fail with the store's unavailability error and are not
  spooled; the caller sees it.

A consumer that committed its writes just before the outage and then
fails to publish spools those events. They carry derived ids, so a later
redelivery republishes them as no-ops. Only `BusError::Disconnected` (the
database unreachable: `ConnectionLost`, `PoolTimedOut`) is spooled.
`PublishRejected`, `Encode` and the other errors pass through unchanged.

**Where and how it is stored.** The spool lives in `<data dir>/spool/`,
the persistent volume beside `blobs/` and `exchanges/`. It is overridable
by the gateway's `spool.dir`, which must stay under the data directory.
It holds:

- `LOCK`: held with `File::try_lock` (std, no new dependency) for the
  process's lifetime. A second process on the same data directory fails
  at start with `SpoolError::Locked`.
- `segment-<first record number, 20 digits>.log`: append-only segments.
  Each one starts with the header `b"CTSPOOL1"`. Each record is:

  ```text
  u32 LE  payload length (≤ 16 MiB)
  [u8;16] first 16 bytes of BLAKE3(record number LE ‖ payload)
  u64 LE  record number (dense, from 1, never reused)
  payload Envelope wire JSON (the bus codec's bytes, unchanged)
  ```

  A segment rolls at `spool.segment_bytes` (default 64 MiB).
- `cursor`: the number of the last record the bus holds, plus its
  segment, as one small JSON file. It is replaced atomically: write
  `cursor.tmp`, `fdatasync`, `rename`, `fsync` the directory.

**Durability: the fsync points.**
1. On append, the record is written, then `fdatasync`ed, before `publish`
   returns `Ok`. `Ok` means the envelope is in `transport.events` or
   durably in the spool (proposed `transport.spool.ok-means-durable`).
2. Creating a segment `fsync`s the new file and then the directory before
   its first record counts as appended.
3. Advancing the cursor is the write-rename-fsync sequence above, done
   after the bus transaction that took the batch committed.
4. A segment is deleted only once the cursor is past its last record,
   followed by a directory `fsync`.

**Crash during spooling.** On open, each segment is read from the cursor
on:
- A short record or a checksum mismatch at the **tail** of the **last**
  segment is a torn append. That record's `publish` never returned `Ok`,
  so its exchange was never acknowledged as captured (as for INV-128). It
  is truncated and logged at warn with its byte count.
- A bad record anywhere else is `SpoolError::Corrupt { segment, offset }`.
  Draining stops, and `/readyz` and `/healthz` report it with the
  segment. The remaining records stay on disk for an operator to keep or
  discard with `crosstalk spool --discard-corrupt` (a new subcommand).
  Nothing past the corruption is silently skipped.

**Ordering and stable ids.** The envelope's id and `at` are minted before
the spool sees it (under the `Ingester`'s id lock for captures; derived
for consumer events). The spool stores the envelope bytes unchanged, so a
replayed record has exactly the id it was minted with.

`SpoolingBus` has three states, changed only under its own publish mutex:
- `Direct`: publish goes straight to `PgBus`.
- `Spooling`: the database is down; publish appends.
- `Draining`: the database is back and the spool is non-empty; publish
  still appends to the tail, behind the backlog.

So once anything is spooled, every later envelope goes behind it until
the spool is empty (proposed `transport.spool.no-overtaking`). Envelopes
therefore reach the bus log in publish order, which keeps the `Ingester`'s
"ids reach the bus in increasing order" guarantee.

Replay is idempotent with the bus's rule. The drainer publishes a batch
in one `transport.events` transaction (`ON CONFLICT (id) DO NOTHING`),
then advances the cursor. A crash between the two re-sends the batch,
which inserts nothing (`transport.publish.idempotent-on-id`). Outbox rows
never pass through the spool (they wait in the database), so the outbox
and spool id rules cannot collide. An event that is both spooled and
later republished from a redelivery has one id and lands once.

**Draining relative to new traffic.**
- A connection probe (every `spool.probe_ms`, default 1 s, and on every
  failed append) moves `Spooling` to `Draining` once the database answers.
- The drainer sends batches of `spool.drain_batch` (default 256) records
  in order.
- New publishes keep appending to the tail. When the drainer reaches the
  tail it takes the publish mutex, sees the spool empty, and switches to
  `Direct`; the next publish goes to the bus.
- If the database drops again mid-drain, the state returns to `Spooling`
  with no record lost or reordered.
- At start, a non-empty spool means the process starts in `Spooling`, or
  `Draining` if the database answers. The spool is opened and recovered
  before the capture stage accepts its first exchange.
- Draining needs only `transport.events`, so it runs concurrently with the
  rest of recovery. Pipeline groups see spooled events as ordinary new
  log entries.

**The watermark.** A spooled envelope is input not yet processed. The
frontier must count it: `PgFrontierSource`'s `oldest_pending` is also no
later than the `at` of the oldest record still in the spool
(`SpoolingBus::oldest_at()`, kept in memory and rebuilt on open; proposed
`topology.frontier.covers-spool`). Without this, after recovery the
watermark could finalize a bucket that a still-spooled capture belongs to.

**Bounds.**
- `spool.max_bytes` defaults to 1 GiB. It must leave room on the volume
  for the blobs; the gateway checks the volume's free space against it at
  start and warns.
- An append that would exceed it is refused with `BusError::SpoolFull {
  bytes }` (spec change). Capture counts the exchange as `spool_full`; its
  bodies are already in the blob store but no event names them.
- A disk I/O error on append is `BusError::Disconnected`, counted as
  `spool_io`.
- A full spool never blocks: the proxy keeps forwarding and the client
  never waits on the spool or the database (proposed
  `ingress.proxy.forwarding-independent-of-capture-store`).

The drop is honest and counted, and it is the only loss the design
allows. Raising `max_bytes` is the operator's lever.

**Metrics** (`/metrics`):
- `crosstalk_spool_state{state="direct|spooling|draining|corrupt"}` (one
  series is 1);
- `crosstalk_spool_bytes`, `crosstalk_spool_records`,
  `crosstalk_spool_oldest_age_seconds` (by the clock: now minus the oldest
  record's `at`);
- `crosstalk_spool_appended_total`, `crosstalk_spool_drained_total`;
- `crosstalk_spool_rejected_total{reason="full|io"}`;
- `crosstalk_spool_truncated_bytes_total`.

Capture adds `crosstalk_capture_uncaptured_total{reason="spool_full"}`.

**Config** (gateway, `spool` section, every key defaulted):
`{"max_bytes": 1073741824, "segment_bytes": 67108864, "drain_batch": 256,
"probe_ms": 1000}`, plus an optional `dir`. Each value goes through
checked constructors; `segment_bytes` must not exceed `max_bytes`.

**Memory mode** (no `store`): there is no spool. `MpscBus` never
disconnects.

## Retention and persisted watermarks

| What | Bound | Where enforced | Persisted state |
| --- | --- | --- | --- |
| Content-confirmed pairing (`flow.content_retention_ms`, 30 days) | correlator: a held match pairs a write up to the retention; delivery records dropped `content_retention + keep` after their last read | in the shards (checkpointed) | `flow.checkpoints` |
| Span index (`provenance.index.retention_secs`, 30 days) | spans expire; postings evicted; observations (fingerprints and token hashes) aged out; request lists pruned | `Provenance::expire(now)` on the L4 stage's tick | `provenance.spans` states, `observations.at` |
| Seen messages (`ThreadConfig` retention, 30 days) | the seen set used for deltas | L3 threader | `reconstruct.seen_messages.seen_at` |
| Topic versions | the catalog's `RetentionPolicy` (pins) | `PgTopicCatalog::enforce_retention` on activation, unpin | `analysis.topic_versions.state`, frozen sizes |
| Edge buckets | per dropped version; partitions | `PgEdgeStore` (drop version) | `topology.versions` |
| Projection frames | frame retention (surface config) | `PgProjectionStore::expire(now)` | `analysis.projection_jobs.fitted_at` |
| Extraction ledger | `extract_delivered`, `extract_contexts`, `extract_done` older than `content_retention` | the L4 stage's tick | `at` / `updated_at` columns |
| Bus log | acked by every group and older than `bus.retention_ms` (7 days proposed) | `PgBus::prune(now, keep)` on the gateway's retention tick | `transport.events` |
| Publish spool | records deleted (by segment) once drained; bounded by `spool.max_bytes` | `SpoolingBus` drain | `<data dir>/spool/cursor` |
| Dead letters | never dropped automatically; an operator replays them | - | `transport.dead_letters` |
| Page cursors | `flow.cursors.issued_at`, `topology.cursors` older than a cursor TTL (1 day proposed) | each store's prune on tick | issue times |
| Outbox | deleted after publish | relays | - |
| Live feed log | feed retention (tokio `Instant`) | surface writer | none (memory, by design) |

Every retention job takes `now` from the injected clock and computes the
horizon in Rust. No store reads `now()` in SQL for a decision.

Watermarks and progress markers that are persisted:

- L7 watermark: `topology.state.watermark_micros` (exists; never lowered).
- L5 tick checkpoint: `flow.shard_ticks.ticked_through`, written with the
  snapshot.
- L5 access coverage: `flow.checkpoints.accesses_through`.
- L4 index sequence: `provenance.index_seq` and per-span `index_seq`
  (exists; the scan watermark).
- Bus progress per group: `transport.groups.admitted_through` + the
  `deliveries` rows (pending = not acked).
- Topic catalog: active version in `analysis.topic_versions`; alert rule
  version in `analysis.alert_rule_state` (exists).

## Restart semantics end to end

Start sequence of `serve --role all` with a `store` section:

1. Parse config. Open the spool, which takes its `LOCK` and recovers a
   torn tail. Bind the proxy, which forwards at once: forwarding never
   waits on the database. Capture publishes through `SpoolingBus`, so it
   spools until the database answers, then drains
   ([The publish spool](#the-publish-spool-database-down)). Connect to
   Postgres, retrying every 5 s as now. Steps 2 to 6 wait for the
   connection.
2. Check migrations: every layer's applied versions equal its embedded
   head (a real check replacing today's vacuous one). Behind is a start
   error that names the layer. `serve` never migrates; `crosstalk migrate`
   does (now running every layer, `transport` first).
3. Take the pipeline advisory lock.
4. Build the Postgres stores. `PgAgents` loads its merge table and the
   registry its supersessions.
5. Recovery:
   1. `PgBus::start`: held deliveries become delayed (counted attempt).
   2. Relay every outbox until empty: L3, L5, L6, L7.
   3. Restore L5: checkpoint, held writes, re-feed accesses.
   4. Rebuild the node cache.
   5. Open a new live feed epoch.
6. Subscribe every group (existing groups resume; none starts at the log
   head except a brand-new group), spawn the stages, the ticker, the
   retention tick and the API.
7. `/readyz` reports `"pipeline": "running"` and `"recovery": "done"`. The
   capture side was ready from step 1.

`/readyz` (body extends today's):

```json
{"ready": true, "role": "all", "status": "ok",
 "database": "reachable", "migrations": "at_head",
 "pipeline_lock": "held",
 "capture": "durable", "pipeline": "running",
 "recovery": "done",
 "tasks": [{"name": "exchange_log", "running": true}, {"name": "capture", "running": true},
           {"name": "live", "running": true}, {"name": "proxy", "running": true},
           {"name": "api", "running": true}]}
```

- For roles that capture (`all`, `proxy`), `ready` means the proxy
  forwards and capture is durable: published to the bus, or appended to a
  spool with room. So `ready` stays true (200) while the database is
  down, with `"status": "degraded"`, `"database": "unreachable: ..."`,
  `"capture": "spooling"` and `"pipeline": "waiting_for_database"`.
  Readiness gates harness traffic, and the harness must keep working
  through a database outage.
- `ready` is false (503) when:
  - the spool is full (`"capture": "dropping: spool full"`) or corrupt
    (`"capture": "spool corrupt: <segment>"`);
  - migrations are behind (`"migrations": "behind: flow 1 < 2"`);
  - the pipeline lock is held elsewhere;
  - a task stopped;
  - the process drains.
- For the `api` role, `ready` also needs the database: the API reads only
  the stores.
- Recovery and draining keep `ready` true with `"status": "degraded"`.
  They show as `"recovery": "relaying_outboxes" | "restoring_flow" |
  "rebuilding_nodes" | "done"` and `"capture": "draining (n records)" |
  "durable"`.
- A backlog does not make the process unready. The backlog is in
  `/healthz`.

`/healthz` adds a `bus` section and reports persisted state:

```json
{"status": "ok", "capture": {...}, "pipeline": {...}, "log": {...},
 "live": {"stages": {"l3-reconstruct": 0, ...}, "watermark_micros": 1790845200000000},
 "bus": {"groups": {"live-l5-flow": {"pending": 12, "oldest_pending_micros": 1790845212000000,
                                      "dead_letters": 0}, ...}},
 "recovery": {"outbox_relayed": 3, "flow_checkpoint_micros": 1790845210000000,
              "accesses_refed": 4, "deliveries_redelivered": 17},
 "spool": {"state": "draining", "records": 120, "bytes": 1843200,
           "oldest_at_micros": 1790845100000000, "max_bytes": 1073741824,
           "appended": 120, "drained": 0, "rejected_full": 0, "rejected_io": 0,
           "truncated_bytes": 0}}
```

- `spool` is present whenever `store` is configured. `state` is one of
  `direct`, `spooling`, `draining` or `corrupt`. `status` is `degraded`
  while the state is not `direct`.

- `live.watermark_micros` is the persisted watermark from the first
  request. It is never lower than before the crash.
- `live.stages`, `capture`, `pipeline` and `log` are process counters.
  They restart at 0, as today.

The API after a restart:

- Everything committed before the crash is visible as soon as the API is
  ready: agents (merges and claims), conversations, channels (discoveries,
  supersessions, policy history), resources and accesses, transmissions in
  every state with their verdict logs, edges and series up to the persisted
  watermark, topic versions and assignments, search documents, alerts and
  rules, projections, the audit log, operators and sinks, and dead letters.
- Detections still in progress at the crash resume. Inputs the bus had
  not seen acked are redelivered, L5 picks up from its checkpoint, and
  transmissions that were suspected or awaiting content confirm, suspect
  or expire exactly as they would have, at the same derived ids. Their
  events reach the API and the live feed as they settle.
- Cursors issued before the crash keep working: table cursors persist and
  MAC keys derive from the secret. A live feed stream ends at shutdown.
  A reconnect with an old-epoch cursor gets `Resync`.
- An exchange in flight at the proxy when the process died is lost. Its
  client saw the connection cut. An exchange whose spool append had not
  been fsynced (a torn tail) was never acknowledged as captured. Every
  envelope whose publish returned `Ok`, to the bus or to the spool,
  reaches the bus log after the restart.
- Exchanges captured during a database outage appear once the spool
  drains and the pipeline processes them, at their capture times. The
  watermark waited for them (`topology.frontier.covers-spool`).

What is recomputed rather than restored: the node cache (from the
stores), provenance's token and k-gram caches (lazily), the surface's
search-model book (a later page under a changed model refuses as it
would), the frontier (from the bus and `shard_ticks`), and the correlator
work since the last checkpoint (by redelivery and re-feed).

## Test strategy

All database tests use `crosstalk_store::TestDb::new_or_skip` on a
multi-threaded tokio runtime with small pools (`TestDb::default_pool_settings`,
4 connections). Without `TEST_DATABASE_URL` they print their skip line and
pass. On this machine, `TestDb` finds `TEST_DATABASE_URL` in
`/home/nymph/Code/ai/crosstalk/.env.test` (the node0 Postgres), walking up
from the worktree. Tests and agents never print, copy or commit its
value. A "Network is unreachable" failure is reported, never worked
around.

1. **Reference-model agreement** for every new store, as the existing
   `pg_*` tests do. The `crosstalk-memory` harness for the trait
   (`model::analysis` for topics and projections, `model::surface` for
   audit, operators and sinks) runs proptest sequences against a fresh
   migrated database and the memory reference, and requires equal
   observable results. The database is truncated between cases, as
   `reconstruct::tests::pg::truncate` does. Also: `SpanIndex` on
   `PgProvenanceStore` against `MemoryProvenanceStore`, and
   `ExchangePlacements` on `PgConversations` against
   `MemoryConversations`.
2. **Bus conformance.** The transport's existing bus tests and DST suite
   become generic over `EventBus + DeadLetterStore` and run over
   `MpscBus` and `PgBus`. New Postgres-only tests:
   - publish survives a restart (drop the bus and pool without shutdown,
     open a new `PgBus` on the same database, and the group receives the
     envelope);
   - publish is idempotent on id;
   - held rows return after a restart with the attempt counted;
   - dead letters persist and replay;
   - `prune` never drops an unacked event;
   - `group_stats` agrees with the deliveries.
3. **Outbox relay crash points**, per layer. A fault-injecting bus fails
   or "crashes" (the relay future is dropped) after the stamp, after the
   publish, and before the delete. A new relay then publishes each staged
   event exactly once in the log, with the stamped id.
4. **L5 restore DST** (`crosstalk-flow`). Over the existing correlator
   DST input generator: run uninterrupted, and run with a crash at a
   seeded point (between store write and publish, between checkpoint and
   ack, mid-extraction, mid-release of a held write), restore from the
   database and continue. Every decision (opened, suspected, confirmed,
   discarded, at the same ids and times) must equal the uninterrupted
   run's.
5. **End-to-end restart tests** (`crates/e2e`, Postgres-gated):
   - Run a scenario through `Live` over Postgres under `Ticking::OnSettle`:
     the M2 wiki dead drop, the AI Village and SALT samples the eval
     smoke uses.
   - Kill it at K seeded points. A kill aborts every task and drops the
     pool without a graceful shutdown, so in-memory state is lost as in a
     real crash.
   - Start a new `Live` over the same database and settle.
   - Compare its observable state, read only through the spec's read
     traits and the HTTP API, with an uninterrupted run over Postgres and
     with one over the memory stores. Compared: transmissions with states,
     routes and matches; channels; edges and totals; watermark; verdicts;
     alerts; audit.
   - The equivalence is exact except for minted ids. Agents are compared
     up to renaming by their first evidence item, and envelope ids and
     process counters are excluded.
6. **Conformance over Postgres.** A third real harness for
   `crosstalk-conformance`: `crosstalk_world` seeds a fresh migrated
   database through `WorldStores` implemented by the Postgres bundle, and
   the surface runs over `PgStores`, in process and over HTTP. Its
   expected-failures list must be empty, as the memory harnesses' are.
   This is the "Postgres harness" step of conformance.md's next steps.
7. **Gateway tests.**
   - `crosstalk migrate` runs every layer and is idempotent.
   - `/readyz` reports `behind` for an unmigrated database, `held
     elsewhere` for a second pipeline process, and each recovery phase.
   - `/healthz` reports the persisted watermark after a restart.
   - `serve` with `store` uses `PgBus` (an architecture-level test that
     `MpscBus` is never built in Postgres mode).
8. **P7.3's end-to-end test.** Wiki demo traffic through the proxy,
   restart, then the API shows the graph, the evidence and the alert. The
   alert part depends on the P6 alerts consumer being wired.
9. **The spool.** The database outage is simulated with `DbLink`, a
   loopback TCP relay in `crosstalk-testkit` between the pool and the
   node0 server. It can be cut (every connection reset, new ones refused)
   and restored. node0 is never stopped.
   - Unit tests (`crosstalk-transport`, `tempfile`):
     - the record format round-trips;
     - a torn tail is truncated: every byte prefix of a last record,
       written then reopened;
     - a mid-segment checksum failure is `Corrupt` and stops draining;
     - the cursor is replaced atomically (a crash at each step of
       write, fsync, rename leaves either the old or the new cursor);
     - segments roll and are deleted;
     - `SpoolFull` at the bound, with nothing written past it;
     - a second open of the same directory is `Locked`.
   - DST (paused time, a fault layer over the file operations and the
     inner bus):
     - every `Ok` publish reaches the inner bus exactly once by id, in
       publish order, across seeded crashes during append, during drain
       (after the batch commit, before the cursor) and during the state
       switch;
     - nothing published while non-empty overtakes the backlog;
     - `oldest_at` never runs ahead of the oldest unsent record.
   - Integration over `PgBus` and `DbLink`:
     - DB down then up: cut the link, publish N, restore it, and
       `transport.events` holds exactly the N ids in order;
     - kill while spooling: cut, publish, drop the process state without
       shutdown, reopen with the link still cut, publish more, restore,
       and every `Ok` id is in the log once.
   - End to end (`crates/e2e`, Postgres-gated):
     - Run the restart scenarios through the proxy with the link cut for
       a seeded window mid-stream.
     - Then, in a separate run, cut the link, kill the gateway while it
       spools, restart it with the link still cut, and restore the link.
     - Compare both with an uninterrupted run using the same state
       comparison as test 5.
     - Assert that `/readyz` stays 200 with `capture: spooling` and then
       `draining`, that `/healthz`'s `spool` section and the
       `crosstalk_spool_*` series move as specified, and that the
       watermark never passes the oldest spooled capture's time.
   - Spool full: with a small `max_bytes`, captures past the bound are
     counted `spool_full`, the proxy still answers every client
     unchanged, `/readyz` turns 503 (`dropping: spool full`), and after
     the drain it is 200 again.

## Spec changes (landed, workstream S)

Accepted by the user's review and applied on `feat/pg-spec` (workstream
S). They are the shared boundary W1 to W9 build against. The final
surface, with where each piece lives:

| Item | Spec surface | Invariants |
| --- | --- | --- |
| Derived envelope ids | `EventId::derive(parent: EventId, label: &'static str, ordinal: u32) -> EventId` (`spec/types/ids.rs`): parent's millisecond, then 80 bits of BLAKE3 over `"crosstalk.envelope.derived.v1"`, the label (u32 BE length + bytes), the parent (u128 BE) and the ordinal (u32 BE); pinned by a test vector | INV-1200 `canonical.ids.derived-event-id` (unit, property: done); INV-1202 `transport.consumer.derived-envelope-ids` (dst per consumer: reconstruct, provenance, flow, topology, gateway classifier) |
| `PgBus` and durability | `l2_transport.rs` module doc: `PgBus` (single node, durable) among the `EventBus` implementations; durability, idempotent publish and restart redelivery stated there | INV-1203 `transport.durability.pg-publish-persisted`; INV-1204 `transport.publish.idempotent-on-id`; INV-1205 `transport.restart.held-redelivered`; INV-102 and INV-103 rationale scoped to `MpscBus` (their statements already named it) |
| Publish spool | `BusError::SpoolFull { bytes: u64 }` (not serialized; `QueryError::from(BusError)` maps it to `Store`); `l2_transport.rs` names `SpoolingBus<B>` as the decorator `serve` puts in front of `PgBus` | INV-1206 `transport.spool.ok-means-durable`; INV-1207 `transport.spool.drained-once-under-its-id`; INV-1208 `transport.spool.no-overtaking`; INV-1209 `transport.spool.torn-tail-only`; INV-1210 `transport.spool.bounded` |
| Outbox ids | none (crate-level relays) | INV-1211 `reconstruct.outbox.stable-envelope-id`; INV-1212 `flow.outbox.stable-envelope-id`; INV-1213 `analysis.outbox.stable-envelope-id`; INV-1214 `topology.outbox.stable-envelope-id` |
| L5 restore | none (crate-level ports) | INV-1215 `flow.consumer.restore-equivalent`; INV-1216 `flow.checkpoint.ticks-with-state` |
| Frontier | `FrontierSource` doc (`l7_topology.rs`): spooled envelopes count as pending; `PgFrontierSource` lives in `crosstalk-gateway`. INV-581's integration evidence moved to `crosstalk_gateway::integration::pg_frontier_covers_pending_deliveries` | INV-1217 `topology.frontier.covers-spool` |
| Audit atomicity (Q3) | `AuditOutcome::Interrupted` (wire `{"type": "interrupted"}`, `OutcomeKind::Interrupted`; `result()` reads it back as `ActionError::Store { reason: INTERRUPTED_REASON }`); `AuditIntent` (checked: `AuditIntent::new(id, at, caller, action)` refuses a caller without the action's permission; `entry(outcome)`, `interrupted()`; wire `{"id", "at", "caller", "action"}`, golden `surface_actions/audit/audit_intent.json`); trait `AuditIntents: AuditLog` with `intend(&AuditIntent)`, `complete(AuditEntry)` (append + remove the intent, one transaction) and `recover_interrupted() -> Vec<AuditId>`. The audit module doc and `OperatorActions::act`'s doc describe the write-ahead flow | INV-1218 `surface.audit.no-silent-effect`; INV-459 gains the `Interrupted` and intent permission tests; INV-364 and INV-458 rationales describe the intent |
| Cursor keys (Q4) | `KeyedHasher::derive_key(label: &'static str) -> DerivedKey` (`ids/secret.rs`): BLAKE3 `derive_key` mode over the current version's key and the label; `DerivedKey { version(), as_bytes() }`, no serde or `Clone`, redacted `Debug`. Labels: `crosstalk.cursor.v1.<store>` | INV-1201 `canonical.ids.derived-key-per-purpose` (unit: done); INV-1219 `surface.cursor.survives-restart` |
| Unique ids across restarts | none | INV-1220 `surface.ids.unique-across-restart` |
| Forwarding | `l0_ingress.rs` module doc | INV-1221 `ingress.proxy.forwarding-independent-of-capture-store` |

Every new invariant's evidence is a planned path with `agent = "false"`,
except INV-1200 and INV-1201, whose spec tests exist. Each workstream
flips the evidence it implements. Downstream type-shape fixes:
`ui/src/pages/audit/entry.rs` shows `Interrupted` as a rejected outcome
with `INTERRUPTED_REASON`, and `crates/world/tests/history.rs` counts it
with `Succeeded` (no world history has one).

Deviations from the proposal below:
- L3 keeps `crosstalk_reconstruct::ids::derived_event_id`. It derives from
  the exchange id with a byte salt under its own domain, so switching it
  to `EventId::derive` would change every L3 envelope id, not refactor
  it. `transport.consumer.derived-envelope-ids` therefore reads "a
  function of that delivery's envelope (its id, or the id of the input it
  carries)".
- Item 7 said no type change was needed for cursor keys, but
  `KeyedHasher` had no way to derive a key, so `derive_key` and
  `DerivedKey` were added.
- Item 6 named no trait for the intent; `AuditIntent` and `AuditIntents`
  were added so W7 (`PgAuditLog`, the memory reference) and W8 (the
  surface's `act`) share one contract. `AuditIntents` is a separate trait,
  so the existing `AuditLog` implementations still build.
- Item 8 chose `surface.ids.unique-across-restart` (not `canonical`): the
  composer that seeds the generators upholds it.

The proposal as reviewed:

1. **Derived envelope ids** (`spec/types/ids`): `EventId::derive(parent:
   EventId, label: &'static str, ordinal: u32) -> EventId`. It keeps the
   parent's ULID time, and the random part is 80 bits of BLAKE3 over
   `(label, parent, ordinal)`. This generalizes
   `crosstalk_reconstruct::ids::derived_event_id`. New invariant
   `transport.consumer.derived-envelope-ids` (INV-X): "Every envelope a
   pipeline consumer publishes because of a delivery has an id that is a
   function of that delivery's envelope id and the event's place among
   its outputs." Evidence: dst per consumer.
2. **`PgBus` in L2's docs and invariants.**
   - `l2_transport.rs`' implementations list gains `PgBus` (single node,
     durable) beside `MpscBus` and `JetStreamBus`.
   - New `transport.durability.pg-publish-persisted` (INV-X, integration):
     the INV-128 statement for `PgBus`.
   - New `transport.publish.idempotent-on-id` (INV-X): "publishing an
     envelope whose id the bus already holds returns Ok and delivers
     nothing new" (`PgBus`; `MpscBus` keeps the consumer-side `Dedup`).
   - New `transport.restart.held-redelivered` (INV-X): "a delivery held
     by a process that stopped is redelivered with its attempt counted."
   - INV-102 and INV-103 (backpressure) are scoped to `MpscBus` in their
     statements (a durable log has no bounded queue to wait on).
3. **Outbox ids**: one invariant per layer that relays
   (`reconstruct.outbox.stable-envelope-id`,
   `flow.outbox.stable-envelope-id`, `analysis.outbox.stable-envelope-id`,
   `topology.outbox.stable-envelope-id`, INV-X): "each staged event is
   published under one envelope id, fixed before its first publish, and
   never before the transaction that staged it committed."
4. **L5 restore**: `flow.consumer.restore-equivalent` (INV-X, dst): "a flow
   consumer restored from its last checkpoint, with the unacked deliveries
   redelivered and the accesses after the checkpoint re-fed, decides the
   same transmissions at the same ids as one that never stopped."
   `flow.checkpoint.ticks-with-state` (INV-X): "`shard_ticks` never names
   a tick later than the stored snapshot of that shard."
5. **`FrontierSource` doc**: `PgFrontierSource` lives in the composer
   (it reads L2, L5 and the proxy). The planned evidence path
   `crosstalk_topology::integration::pg_frontier_covers_pending_deliveries`
   becomes `crosstalk_gateway::...` (INV `topology.frontier.covers-pending`).
6. **Audit atomicity** (`l8_surface/audit.rs`, decided Q3): replace "a
   database `SurfaceStores` makes them one transaction" with a write-ahead
   intent:
   - Before the effect, the surface records an intent.
   - Afterwards, one transaction appends the entry and removes the intent.
   - At start, each leftover intent is appended with a new
     `AuditOutcome::Interrupted`: the effect may or may not have applied.

   New invariant `surface.audit.no-silent-effect` (INV-X): "every action
   call whose effect may have applied has an audit entry, `Interrupted`
   at worst."
7. **Cursor keys**: `surface.cursor.survives-restart` (INV-X): "a cursor
   the surface issued resolves after a restart with the same deployment
   secret." The derivation uses the spec's keyed hasher with a domain
   label, so no type change is needed.
8. **Unique ids across restarts**: `surface.ids.unique-across-restart`
   (INV-X; or under `canonical`, which owns ids): "an id minted after a
   restart never equals a persisted id of the same kind."
9. **The publish spool** (decided Q5):
   - `BusError::SpoolFull { bytes: u64 }` (`l2_transport.rs`), and the
     `l2_transport.rs` doc names `SpoolingBus` as the decorator `serve`
     puts in front of `PgBus`.
   - INV-128's sibling for the spool (`transport.durability.pg-publish-persisted`)
     reads "in `transport.events` or durably spooled".
   - New invariants (INV-X):
     - `transport.spool.ok-means-durable` (dst, integration): "a
       `SpoolingBus::publish` that returned `Ok` has its envelope in the
       inner bus's log or fsynced in the spool; a crash or restart never
       loses it."
     - `transport.spool.drained-once-under-its-id` (dst, integration):
       "every spooled envelope reaches the inner bus under the id it was
       spooled with; a drain repeated after a crash adds nothing to the
       log."
     - `transport.spool.no-overtaking` (dst): "while the spool holds a
       record, no envelope published after it reaches the inner bus
       before it."
     - `transport.spool.torn-tail-only` (unit): "recovery discards only
       an incomplete last record of the last segment, whose publish never
       returned `Ok`; any other bad record stops draining as `Corrupt`."
     - `transport.spool.bounded` (unit, integration): "the spool never
       holds more than `max_bytes`; an append past it is refused with
       `SpoolFull` without waiting."
     - `topology.frontier.covers-spool` (dst): "`FrontierSource::frontier`
       returns an `oldest_pending` no later than the `at` of every
       envelope still in the spool."
     - `ingress.proxy.forwarding-independent-of-capture-store`
       (integration): "the proxy forwards and relays every request
       unchanged whatever the state of the database, the bus or the
       spool."
10. **No change** for the checkpoint itself, the extraction ledger, held
   writes or `ExchangePlacements`. They are crate-level ports or existing
   traits.

The coordinator numbered them in the P7.3 block (INV-1200 to INV-1249);
S used INV-1200 to INV-1221.

## Implementation plan

```text
S (spec, first) ──┬─▶ W1 transport ─┐
                  ├─▶ W2 L3 ────────┤
                  ├─▶ W3 L4 ────────┤
                  ├─▶ W4 L5 ────────┼─▶ W8 composition (api + gateway) ─▶ W9 restart e2e + conformance on Postgres
                  ├─▶ W5 L6 ────────┤
                  ├─▶ W6 L7 ────────┤
                  └─▶ W7 L8 ────────┘
```

| WS | Branch | Owns (files, crates) | Delivers |
| --- | --- | --- | --- |
| S (landed) | `feat/pg-spec` | `spec/types/ids*`, `spec/types/interfaces/l2_transport.rs`, `l7_topology.rs` (doc), `l8_surface/audit.rs`, `spec/invariants/INV-X-*` | the spec changes above, with type and unit evidence for `EventId::derive` |
| W1 | `feat/pg-bus` | `crates/transport/{src/pg/**, src/spool/**, migrations/**, Cargo.toml}`, transport tests and dst made bus-generic; `crates/testkit/src/db_link.rs` | `PgBus`, `PgDeadLetters`, `group_stats`, `prune`, restart reset; **`SpoolingBus`** (segments, cursor, lock, states, drain, bounds, `oldest_at`, stats); `DbLink`; bus conformance over both buses; spool unit, DST and integration tests |
| W2 | `feat/l3-restart` | `crates/reconstruct/**` | `ExchangePlacements` for `PgConversations`; outbox stamp + awaited bus sink; derived ids checked on redelivery; `0004_outbox_ids.sql`. **Implemented** (branch `feat/pg-w2-reconstruct`): the migration is `0005_outbox_ids.sql` (`0004` went to the conversation reads); `EventSink` gained `stamp`, and `agents::outbox::relay` stamps, publishes and deletes; `PgAgents::open_with_secret` and `with_cursor_secret` derive cursor keys (`crosstalk.cursor.v1.agents`, `crosstalk.cursor.v1.conversations`); tests `tests::pg_outbox`, `tests::pg_placement`, `tests::dst::redelivery_republishes_the_same_envelope_ids`, `tests::cursor_keys`. `AgentSeen` moved from the consumer to the agent store's outbox (staged by `create` from traffic and `attach_evidence`, memory reference changed to match), so a redelivery never loses it |
| W3 | `feat/l4-pg-span-index` | `crates/provenance/**` | `SpanIndex` for `PgProvenanceStore`; `started_at` read; retention tests including token observations |
| W4 | `feat/flow-checkpoint` | `crates/flow/**` (with `extract::step` moved in from the gateway, see below) | `0002_restart.sql`; outbox stamp + awaited sink; held writes; access sequence; checkpoint/restore; extraction ledger (memory + Pg); `Publisher` derived ids; restore DST |
| W5 | `feat/l6-pg-topics-projections` | `crates/analysis/**` | `PgTopicCatalog`, `PgProjectionStore`, outbox stamp; model tests vs memory. **Implemented** on `feat/pg-w5-analysis` (see [search_alerts](search_alerts.md), second half): also `crosstalk_analysis::classify::Classifier`, the idempotent classification step with derived envelope ids, for W8 to wire in place of the gateway's |
| W6 | `feat/l7-restart` (as `feat/pg-w6-topology`) | `crates/topology/**` | outbox stamp; `BusAnnouncer` derived ids from the input; consumer restart tests. **Done:** `0002_outbox_ids.sql` (`envelope_id`, `at`); the relay stamps in a committed transaction (`OutboxIds`: clock + ULID generator) and then publishes stamped rows (`OutboxRelay::run(announcer, ids, poll)`); `Announce` takes a finished `Envelope`, and `consumer::handle` takes the delivered `&Envelope` and publishes `EdgeUpdated` as `EventId::derive(delivery, "edge-updated", 0)` at the delivery's `at`; relay crash-point, consumer restart and watermark-restart tests. The gateway's L7 stage got the two-line caller change |
| W7 | `feat/surface-pg-stores` | `crates/surface/**` (`migrations/`, `src/pg/**`) | `PgAuditLog` (+ intents), `PgOperatorStore`, `PgSinkRegistry`; cursor key from secret; model tests vs `model::surface` |
| W8 | `feat/gateway-postgres-live` | `crates/api/src/{in_process/**, pg/**}`, `crates/gateway/**` | `PgStores: SurfaceStores` and `EvidenceRecords` over Postgres; `InProcess` generic over the bundle; `Live` generic over a `LiveStoreSet` (memory, Postgres); `PgFrontierSource` (including the spool's `oldest_at`); advisory lock; recovery sequence; the `spool` config section, building `SpoolingBus` over `PgBus`, the `spool_full` capture outcome, the `crosstalk spool --discard-corrupt` subcommand, the spool's `/readyz`, `/healthz` and `/metrics` reporting; `migrate` runs every layer; readiness/health; retention tick; classifier derived ids; removal of the extraction step from `live/layers/extract.rs` |
| W9 | `test/postgres-restart` | `crates/e2e/**`, `crates/conformance/**`, `crates/api/tests/conformance_pg.rs`, `crates/client/tests/conformance_pg.rs` | restart e2e, the spool's end-to-end tests (outage window, kill while spooling, spool full), conformance on Postgres, P7.3's end-to-end test |

Ordering and parallelism:

- S lands first: every workstream uses `EventId::derive` or cites the new
  invariants. W1 to W7 then run in parallel in separate worktrees, each
  inside its own crate.
- W4's move of the extraction step from `crates/gateway/src/live/layers/extract.rs`
  into `crates/flow` is the one cross-crate move. W4 adds the flow
  version. W8 deletes the gateway's and switches the L4 stage to it. Until
  W8, both exist.
- W8 can start against the traits with W1 to W7's types stubbed, but it
  merges last. W9's harness scaffolding (world seeding into `PgStores`)
  can start once W5 and W7 merge.
- Each workstream runs crate-scoped checks only; W8 and W9 run the
  Postgres-gated suites against node0.
- **The spool belongs to W1 (transport), not W8.** It is an `EventBus`
  decorator with disk I/O, as `FsBlobStore` is, and its correctness (the
  `Ok` contract, ordering, ids, idempotent drain) is bus semantics that the
  bus's DST checks. W8 only configures it, wires it, adds it to the
  frontier and reports it. `SpoolingBus` is generic over the inner bus, so
  W1 can test it over `MpscBus` with a fault layer before `PgBus` is done.

Expected merge conflicts (acceptable; the merger resolves them):

- Root `Cargo.toml` and `Cargo.lock`: W1 and W7 add `sqlx` and
  `crosstalk-store` to `transport` and `surface`; W8 adds Postgres crates
  to the gateway's and api's manifests.
- `docs/OVERVIEW.md` (every workstream updates its feature entry) and
  `docs/roadmap.md`.
- `spec/invariants/`: INV-X files from several branches, renumbered at
  merge.
- `crates/memory/**`, if a harness `make` signature needs a tweak for a
  Postgres subject (W5, W7). Kept to additive changes.
- `crates/gateway/src/live/layers/{l4.rs, extract.rs}` between W4 (adds
  the flow step) and W8 (switches to it), by construction sequential.
- `crates/gateway/src/store.rs` (W8) against any concurrent gateway work.
- `crates/testkit/**`: W1 adds `DbLink`, and W9 may add scenario helpers.
  Both are additive.

## Decisions

The user decided every question after reviewing PR #103:

| # | Question | Decision |
| --- | --- | --- |
| Q1 | Durable bus now? | **`PgBus` in this item** (not waiting for JetStream, not replaying from stores) |
| Q2 | Correlator durability | **Per-shard snapshots with deferred acks.** A snapshot the binary cannot read is a start error (`IncompatibleSnapshot`) that needs `crosstalk migrate --reset-correlator`; pending pairings are then knowingly lost |
| Q3 | Audit and effect atomicity | **Write-ahead intent plus `AuditOutcome::Interrupted`** |
| Q4 | Cursor keys | **Derived from the deployment secret**; a rotation invalidates outstanding cursors |
| Q5 | Capture while the database is down | **Spool to local disk and send on recovery; never drop** while the spool has room ([The publish spool](#the-publish-spool-database-down)) |
| Q6 | Bus log retention | **7 days** after every group acked (`bus.retention_ms`) |
| Q7 | Memory mode | **Kept**: `serve` without `store` runs on memory stores and `MpscBus` (dev, `try-claude-code.sh`) |
| Q8 | Exchange store | **Keep the jsonl exchange log**; an exchange store in the spec stays a separate gap |
| Q9 | Split `proxy`/`pipeline` roles over `PgBus` | **Deferred to P9**; only `--role all` captures and detects end to end |

## Files

What the implementation touches (planned; nothing exists yet unless
marked):

| File | Role | Key exports |
| --- | --- | --- |
| `crates/transport/migrations/0001_bus.sql` | bus log, groups, deliveries, dead letters | - |
| `crates/transport/src/pg/{mod,publish,subscribe,dead_letters,prune,stats}.rs` | `PgBus` | `PgBus`, `PgSubscription`, `PgDeadLetters`, `GroupStats`, `PgBusConfig` |
| `crates/transport/src/spool/{mod,segment,record,cursor,drain,state}.rs` | the publish spool | `SpoolingBus`, `SpoolConfig`, `SpoolState`, `SpoolStats`, `SpoolError` (`Locked`, `Corrupt`, `Io`) |
| `crates/testkit/src/db_link.rs` | a cuttable TCP relay to the test database | `DbLink` (`start`, `url`, `cut`, `restore`) |
| `crates/reconstruct/migrations/0005_outbox_ids.sql`; `src/agents/outbox.rs`; `src/publish.rs`; `src/thread/pg.rs`; `src/ids.rs` (W2, implemented) | stable outbox ids; `ExchangePlacements`; cursor keys from the secret | `PgConversations: ExchangePlacements`, `EventSink::stamp`, `Stamp`, `PgAgents::open_with_secret`, `cursor_key` |
| `crates/provenance/src/store/{mod,memory,pg,reads}.rs`, `src/engine.rs` (W3, done) | `SpanIndex` (in `reads.rs`), `started_at` | `PgProvenanceStore: SpanIndex`, `ProvenanceStore::started_at`, `Provenance::started_at` |
| `crates/flow/migrations/0002_restart.sql`; `src/store/outbox.rs`; `src/consumer/{checkpoint,restore,held}.rs`; `src/extract/{step,ledger}.rs` | checkpoint, held writes, ledger, extraction step | `Checkpoint`, `restore`, `ExtractionLedger`, `PgExtractionLedger`, `MemoryExtractionLedger`, `ExtractionStep` |
| `crates/analysis/migrations/0003_outbox_ids.sql`, `0004_topics.sql`, `0005_projections.sql`; `src/topics/**`, `src/projections/**` | L6 stores | `PgTopicCatalog`, `PgProjectionStore` |
| `crates/topology/migrations/0002_outbox_ids.sql`; `src/outbox.rs` | stable ids | - |
| `crates/surface/migrations/0001_surface.sql`; `src/pg/{audit,operators,sinks}.rs`; `src/cursor.rs` | L8 stores, key derivation | `PgAuditLog`, `PgOperatorStore`, `PgSinkRegistry`, `CursorKey::derive` |
| `crates/api/src/pg/{mod,evidence}.rs` | the Postgres store bundle | `PgStores`, `PgEvidence` |
| `crates/gateway/src/live/{mod,store_set,recovery,frontier}.rs`, `src/store.rs`, `src/ops/mod.rs`, `src/config/sections.rs`, `src/cli.rs` | composition, recovery, frontier, readiness, the `spool` section and `spool` subcommand | `LiveStoreSet`, `PgFrontierSource`, `Recovery`, `PipelineLock`, `SpoolSection` |
| `crates/e2e/src/restart.rs`, `crates/e2e/tests/restart/**` | restart tests | - |
| `spec/types/ids.rs`, `spec/types/ids/secret.rs` (exist) | derived envelope ids, derived keys | `EventId::derive`, `KeyedHasher::derive_key`, `DerivedKey` |
| `spec/types/interfaces/l2_transport.rs` (exists) | bus docs, spool error | `BusError::SpoolFull` |
| `spec/types/interfaces/l8_surface/audit.rs` (exists) | write-ahead intents | `AuditOutcome::Interrupted`, `INTERRUPTED_REASON`, `AuditIntent`, `AuditIntents` |
| `spec/types/tests/{derived_ids,audit_intents}.rs` (exist) | spec tests for the above | - |
| `spec/invariants/INV-1200..1221-*.toml` (exist) | the P7.3 invariants | - |
| `docs/features/postgres_stores.md` (this page, exists) | the design | - |

## Invariants and constraints

- The stores are the source of truth: nothing the API reads lives only in
  memory, except caches rebuilt from the stores and the live feed log
  (resynced).
- An event exists in a layer's outbox exactly when the change that
  decided it committed. It is published under one envelope id, fixed
  before its first publish, and never before that commit.
- `PgBus::publish` returning `Ok` means the envelope is in
  `transport.events`. Every group subscribed at that time receives it
  until the group acks it, across restarts.
- Every consumer writes, then publishes, then acks. Every write is
  idempotent on a derived id, and every derived envelope id is a function
  of the input.
- The flow group acks a delivery only after a checkpoint that covers it.
  `shard_ticks` never runs ahead of the stored snapshot.
- The L7 watermark is persisted and never lowered. A dead letter in a
  pipeline group holds it back.
- One pipeline process per database (advisory lock). `serve` never runs
  migrations, and it refuses to consume against a database behind its
  migrations.
- No store reads a clock. Retention horizons, leases, bus delays and
  checkpoint times come from the injected clock.
- Each layer's tables live in its own schema and are created only by its
  own numbered migrations. Applied migrations are never edited.
- `SpoolingBus::publish` returns `Ok` only once the envelope is in the
  bus log or fsynced in the spool. Spooled envelopes reach the bus under
  their own ids, in publish order, once each, and nothing overtakes them.
- The spool is bounded. A full spool refuses and counts, and never
  blocks the proxy. Forwarding never depends on the database, the bus or
  the spool.
- The frontier counts every spooled envelope as pending.
- No new Postgres extension; `vector` and `pg_trgm` only.
- No test prints, copies or commits `TEST_DATABASE_URL`.
