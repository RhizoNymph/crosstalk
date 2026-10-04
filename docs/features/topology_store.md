# Topology store (L7 on Postgres)

`crosstalk-topology` (`crates/topology`), roadmap item P6.1: the spec's
`EdgeStore` on Postgres, and the L7 bus consumer that feeds it. Plain
Postgres with range-partitioned bucket tables (decision D3, no
TimescaleDB), sqlx with runtime-checked queries (decision D2).

## Scope

- `PgEdgeStore`: every `EdgeStore` method. That covers edge buckets per
  topic version and access buckets per resource at the store's
  `BucketWidth`, and aligned graph windows read into a checked
  `TopologyGraph`. It also covers totals, agent traffic, series, the
  channel-centred `BipartiteGraph`, the drill-down (`transmissions`), the
  verdict copy (`judge`), version readiness, activation and retention,
  and the persisted watermark.
- What the store reads from other layers at query time (`TopologyEnv`):
  - the topic catalog's history and topics, through the spec's
    `TopicCatalog`;
  - merges and supersessions (`AgentDirectory`, `ChannelDirectory`);
  - node facts (`NodeFacts`), including each access bucket's resource
    resolved to its channel now (`NodeFacts::channel_of`).
- The transactional outbox and its relay. They publish the events the store
  decides after commit: `TopicVersionActivated`, `WatermarkAdvanced`,
  `Changed::Watermark`, and coalesced traffic changes (the
  `Changed::Traffic` hook).
- The topology consumer (group `topology`), generic over the spec
  `EdgeStore`, `FrontierSource` and an `Announce` publisher.

## Non-scope

- `PgFrontierSource` (the transport's delivery tables and the proxy's
  in-flight registry). The consumer takes any `FrontierSource`.
- `WatermarkRead` for readers outside L7. That is a cache of
  `WatermarkAdvanced`, which the wiring step builds from the bus.
- Wiring the consumer and relay into the gateway (`pipeline::Pipeline`).
  `consumer::run` and `OutboxRelay::run` are shaped like the exchange
  log's stage, so that step can add them.
- `Changed::Traffic` itself. The follow-mode spec batch adds it; see
  [Traffic](#traffic-and-the-changedtraffic-hook).
- Time-based retention of old partitions (P9). Version retention deletes
  rows.

## Data and control flow

```text
bus ── group "topology" ──▶ consumer::run
  TransmissionClassified ─▶ apply ─ Ok(key) ─▶ Announce EdgeUpdated(key) ─▶ ack
                              │        Refit: then activate(version)
                              ├ SelfEdge | VersionNotRetained ─▶ ack
                              └ LateContribution (logged at error) | Store ─▶ nack
  TopicVersionReady   ─▶ version_ready, activate ─▶ ack
  TopicVersionDropped ─▶ drop_version ─▶ ack (failure: error, nack)
  VerdictSet          ─▶ judge ─▶ ack
  AccessRecorded      ─▶ apply_access(access.id, agent, resource, op.kind(), at) ─▶ ack
  every bucket width  ─▶ FrontierSource::frontier ─▶ advance_watermark

PgEdgeStore write (READ COMMITTED):
  state row FOR SHARE (apply) | FOR UPDATE (activate, drop_version, advance_watermark)
  rows + outbox rows ─▶ COMMIT ─▶ Wake::poke (capacity-1 channel)
OutboxRelay::run ─▶ drain: advisory lock, rows by seq, Announce, delete
  traffic rows ─▶ one hull window per batch ─▶ traffic_notification (HOOK)

PgEdgeStore read (REPEATABLE READ READ ONLY, one snapshot):
  watermark + dropped versions ─▶ TopicCatalog history ─▶ resolve version
  ─▶ bucket sums (and false detections under Exclude) ─▶ fold in Rust
```

### Apply

`apply` makes three round trips in the common case:

1. One query locks the state row `FOR SHARE` and reads the watermark, the
   version's status and any stored contribution for (version,
   transmission).
2. One statement (data-modifying CTEs) stores the contribution, upserts its
   bucket with `transmissions + 1, matched_bytes + bytes`, and writes the
   bucket's traffic row. All three happen only when the contribution was
   new (`ON CONFLICT DO NOTHING`).
3. The commit.

The order of checks follows the reference: a dropped version
(`VersionNotRetained`), then a self-edge (`SelfEdge`), then an
already-stored contribution (its stored key, no lateness check), then
`LateContribution` for an activated version whose bucket the watermark
finalizes. A `Refit` contribution that is applied, already applied or a
self-edge is recorded in `refit_processed`, which is what `activate`
counts against the `TopicVersionReady` count.

A concurrent apply of the same (version, transmission) waits on the
unique key and then does nothing; the loser returns the stored key.
Concurrent applies into one bucket serialize on the bucket row and add,
so none is lost.

### Control changes

`activate`, `drop_version` and `advance_watermark` lock the state row
`FOR UPDATE`. An apply that would become late, or would land in a version
being dropped, waits for the change and then sees it.

- **`activate`** switches `state.active_version` and marks the version
  activated. It writes `TopicVersionActivated { version, previous }` to the
  outbox in the same transaction.
- **`drop_version`** marks the version dropped and deletes its buckets,
  contributions and refit records in one transaction. Readers on an
  earlier snapshot still see all of them; later readers see it dropped.
- **`advance_watermark`** persists the new value and writes
  `WatermarkAdvanced` and `Changed::Watermark` to the outbox together.

### Reads

Each read opens one snapshot and reads, in order:

1. the watermark and the dropped versions, in one query;
2. the catalog history, against which the filter's `TopicVersionSelector`
   resolves with retained meaning "not dropped here";
3. the filter's topics against the resolved version's topics, but only
   when the filter lists topics;
4. the bucket sums of that version in the window, per stored key (and per
   point, for a series);
5. under `FalseDetections::Exclude` only, the false detections: stored
   contributions whose verdict copy is `FalseDetection`.

`store::fold` then does the rest in Rust:

1. Resolve agents and channels through the directories and drop the
   resulting self-edges.
2. Admit through `TopologyFilter::admits` with verdicts left aside.
3. Subtract the false detections the rest of the filter admits.
4. Build the edges and shares, the nodes from `NodeFacts` (with the spec's
   defaults for unseen nodes), the access edges and the series groups.

`channel_topology` draws an access only when its resource resolves to a
channel whose facts list it as `Listing::Channel`. A channel's topics
for `admits_access` come from its channel-routed buckets, with false
detections subtracted under `Exclude`. Self-edges count there.

The drill-down reads the contributions table, so its window need not be
aligned. Its cursors are rows of `topology.cursors`, keyed by a random
token and bound to the request (edge, window and filter, as JSON) and to
the version the first page resolved.

### Partitions

`edge_buckets` and `access_buckets` are `PARTITION BY RANGE
(bucket_start)`. `Partitions::ensure` creates `<table>_p<start>` for the
`PartitionSpan` (default one day) holding a bucket, outside the write
transaction, before the first row of that range. The DDL is idempotent: a
lost creation race counts as created. Each process remembers what it
made, so the hot path skips the DDL after the first write of a range.

### Traffic and the `Changed::Traffic` hook

Every committed change to an edge or access bucket writes a traffic row
holding the bucket's window. There is no traffic row for a redelivery, a
self-edge or a late contribution. After commit the store pokes the relay
through a capacity-1 channel, so a burst of commits wakes it once. The
relay also polls.

Each drain reads up to 512 rows in seq order. It publishes the events in
order, then coalesces every traffic row of the batch into one window that
covers them all. That window goes to `outbox::traffic_notification`.

**HOOK:** `traffic_notification` returns `None` until the follow-mode
spec batch adds `Changed::Traffic(TimeWindow)`. It then returns
`Some(BusEvent::Changed(Changed::Traffic(window)))`, with no other
change. Because it runs after commit and covers every traffic row it
deletes, the notification covers every committed change.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/topology/migrations/0001_topology.sql` | The `topology` schema: state, versions, contributions, refit records, partitioned edge and access buckets, accesses, verdicts, cursors, outbox | — |
| `crates/topology/src/lib.rs` | Crate doc, modules | — |
| `crates/topology/src/codec.rs` | Spec values as columns and back (ULID text, bigint micros, route JSON, op and verdict codes) | `CodecError`, encoders and decoders |
| `crates/topology/src/env.rs` | What reads consult from other layers | `TopologyEnv`, `Env { catalog, directory, nodes }`, `EnvAliases`, `default_agent`, `default_channel` |
| `crates/topology/src/store/mod.rs` | The store, its config, migrations | `PgEdgeStore` (`new`, `pool`, `config`, `env`), `EdgeStoreConfig`, `MIGRATIONS`, `migrate`, `bucket_of`, `aligned` |
| `crates/topology/src/store/write.rs` | Applies, verdicts, readiness, activation, retention, watermark, accesses | — |
| `crates/topology/src/store/read.rs` | Snapshots, version resolution, row reads, graph, totals, agent traffic, channel graph, series | — |
| `crates/topology/src/store/fold.rs` | Resolution, filter, `Exclude` subtraction, edges, nodes, access edges, series groups | — |
| `crates/topology/src/store/drill.rs` | `transmissions` and its cursors | — |
| `crates/topology/src/store/edge_store.rs` | `impl EdgeStore for PgEdgeStore` | — |
| `crates/topology/src/store/partition.rs` | On-demand range partitions | `PartitionSpan`, `Partitions`, `Bucketed` |
| `crates/topology/src/store/error.rs` | Failures and their mapping to `EdgeError` / `EdgeQueryError` | `DbError`, `Failed` |
| `crates/topology/src/outbox.rs` | Outbox, relay, publisher | `Announce`, `AnnounceError`, `BusAnnouncer`, `OutboxRelay` (`run`), `drain`, `Wake`, `traffic_notification` (hook), `RelayError` |
| `crates/topology/src/consumer.rs` | The topology consumer | `GROUP`, `SUBJECTS`, `group`, `ConsumerSettings`, `run`, `handle`, `recompute`, `Outcome` |
| `crates/topology/src/tests/` | Fixed-input tests, shared support (`PgSubject` for the model harness) | — |
| `crates/topology/src/integration/` | The model test against `InMemoryEdgeStore`, and one-behaviour Postgres scenarios | — |
| `crates/topology/src/props/` | Proptest properties against Postgres (graph, series, versions) | — |
| `crates/topology/src/dst/` | Paused-time consumer tests over `MpscBus` and the reference store | — |

## Invariants and constraints

- The store holds the same observable behaviour as crosstalk-memory's
  `InMemoryEdgeStore` under `check_edge_store`
  (`integration::pg_matches_reference`). That run also checks every graph
  against the fold oracle and every series total against the graph.
- Applies are idempotent per (transmission, version), and no concurrent
  apply into a bucket is lost (`topology.apply.*`).
- No bucket of an activated version that the exposed watermark finalizes
  changes. The state-row lock orders every apply against every watermark
  advance (`topology.watermark.rejects-late`, `final-buckets`).
- The watermark is persisted and never lowered, restarts included.
- A dropped version is marked dropped and emptied in one transaction.
  Reads see all of a version or `NotRetained`
  (`topology.retention.*`).
- Events the store decides are in the outbox exactly when their change
  committed. They are published after commit, in commit order, at least
  once.
- The consumer acks a contribution only after its apply committed and its
  `EdgeUpdated` published. It acks self-edges and dropped versions. It
  recomputes the watermark at least once per bucket width
  (`topology.consumer.*`, `topology.edge-updated.after-apply`,
  `topology.watermark.recompute-cadence`).
- Spliced SQL is limited to the fixed partition table names and two
  integers. Everything else is bound.
- Ids sort as ULIDs only in Rust: no SQL ordering depends on text
  collation.

## Tests

- `tests`: unit evidence on fixed inputs. Most run against the database;
  the window and grid refusals run on a pool that never connects.
- `integration`: the model test (32 cases of up to 40 operations) and
  Postgres scenarios: redelivery, concurrency, immediate reads,
  monotonicity, the drill-down against the graph, drops, the outbox order,
  and the watermark across a restart.
- `props` (and `tests::verdict_props`): 37 properties, 10 cases each,
  every case on a fresh store.
- `dst`: the consumer under paused time. These cover a self-edge acked
  once, a crash between apply and publish, `EdgeUpdated` never before its
  apply, and the recompute cadence.

Every database test passes with a skip line when `TEST_DATABASE_URL` is
unset.
