# Flow store

The L5 stores on Postgres (`crates/flow/src/store/`, crate `crosstalk-flow`,
roadmap P5) and the flow layer's migrations (`crates/flow/migrations/`, schema
`flow`). They implement the store traits of `crosstalk_spec::interfaces::l5_flow`
as they stand on this base: the channel registry with its traffic writes and
reads, and the transmission store with its verdict logs. They are
model-tested against the `crosstalk-memory` reference stores.

## Scope

- `PgChannelRegistry<D>`: `ChannelRegistry` (lookup, declare, set_policy,
  policy_history, promote, promotion_coverage, resource_use),
  `ChannelTraffic` (add_resource, record_access, discover,
  record_transmission, set_detection), `ChannelReads` (channel, channels,
  transmissions) and `ChannelDirectory` (canonical). `D` is the
  `AgentDirectory` agents are resolved through at every read.
- `PgTransmissionStore<D>`: `TransmissionStore` (save, transmission) and
  `TransmissionVerdicts` (set, log, quality).
- The access store: accesses (with a `write_outcome` column for the eval
  spec's `WriteOutcome`) recorded through `ChannelTraffic::record_access`
  and read by `resource_use`; indexed by id for the coming
  `AccessStore::accesses`.
- Declarations, promotion, supersession and policy history.
- The transactional outbox and its relay (`EventSink`, `Relay`).
- Server-side cursors for the registry's three lists (`prune_cursors`).
- The supersession directory cache, `ShardKey` (the correlator's shard key:
  the canonical channel, or the resource while it is on no channel) and the
  shard tick checkpoints (`PgShardTicks`).
- `ChannelIdSource` and `UlidChannelIds` for declared channel ids.

## Non-scope

- `ResourceExtractor`, `Correlator` and the flow bus consumer (sibling
  branches `feat/flow-extract`, `feat/flow-correlator`). The consumer calls
  these stores; the stores never call it.
- `AccessStore::accesses` and `TransmissionStore::list`: the tables are
  shaped for them (accesses by id, transmissions by
  `(state, channel_id, opened_at)`), but the traits are not on this base.
- Retention of old rows, and cursor pruning on a schedule (the function
  exists; nothing calls it yet).
- Gateway wiring.

## Decisions

**Publishing: a transactional outbox, relayed after commit.** A store
publishes what it decides (`ChannelDiscovered` from discover, which decides
Created vs Existing inside its transaction; `ChannelPromoted`; `VerdictSet`;
every `Changed`). The deciding transaction stages its events in
`flow.outbox`; after the commit the store relays every staged event to its
`EventSink` (a `tokio::sync::mpsc::UnboundedSender<BusEvent>` the wiring
drains into the bus) and deletes the rows. Consequences:

- a refused or rolled-back write publishes nothing; a retried transaction
  stages its events once (failed attempts roll back with them);
- nothing is relayed before its change is visible, so a consumer that
  re-queries on an event sees the change;
- delivery is at least once: if the relay fails or the receiver is gone, the
  events stay staged and any later relay on the database (any node) sends
  them; a relay whose own commit fails may resend;
- concurrent relays take disjoint rows (`FOR UPDATE SKIP LOCKED`); events of
  two concurrent writes can be relayed out of commit order.

**Serializable writes.** Every write runs in one `SERIALIZABLE` transaction
under `crosstalk_store::retry_serializable` (default policy, overridable with
`with_retry`). A body reads what it decides on, checks, writes and stages
events; a refusal aborts with the spec error and changes nothing.
Serialization failures and deadlocks retry the whole body; the body is pure
apart from the transaction (ids are drawn as described below).

**Reads.** A read is one snapshot: a single statement where possible
(`lookup`, `channel`, `policy_history`, `log`), else a `REPEATABLE READ READ
ONLY` transaction (`channels`, `transmissions`, `resource_use`,
`promotion_coverage`). Single statements matter: every round trip to the
test server costs tens of milliseconds.

**Values as wire JSON in TEXT.** Spec values (origins, policies, decisions,
resources, accesses, transmissions, verdict records, events, cursor keys)
are stored as the JSON text of their wire form, in `TEXT` (not `JSONB`,
which refuses `\u0000` and rewrites text). Scalar columns beside them (kind,
created_at, superseded_by, state, route, channel_id, opened_at, confirmed,
verdict) are derived from the value in the same statement, by one function
per table, and carry the indexes and checks. Ids are ULID text in `"C"`
collation (text order is id order); times are microseconds in `BIGINT`.

**Domain logic stays in the spec.** Promotion runs `promotion::plan` (and the
preview `promotion::coverage`) over every stored channel, read in registry
(id) order in the promoting transaction; policy decisions go through
`PolicyHistory::record` over the stored history; verdicts through
`VerdictLog::record`; traffic through `CrossTraffic::tally`; filters through
`ChannelFilter::keeps`. Detection moves are `registry::detection`, the same
transition function as the reference.

**Directory.** `ChannelDirectory::canonical` is synchronous, so the registry
keeps the supersession table in memory. Supersession only grows (a
superseded channel stays superseded by the same declared channel), so the
cache can only be behind, never wrong. It is loaded by `open`, extended by
each promotion this registry commits before `promote` returns, and caught up
with other nodes' promotions by `refresh_directory` (the flow consumer calls
it on `ChannelPromoted`). Reads that resolve channels inside SQL
(`COALESCE(superseded_by, id)`) never depend on the cache.

**Declared ids.** `declare` takes its id from a `ChannelIdSource`:
`pending(at)` inside the transaction, `consume(id)` after the commit.
`UlidChannelIds` mints a fresh ULID per call (a refused or retried attempt
skips one); the model test uses the reference's sequence so declared ids
compare equal.

**Cursors.** A token (`fl-<list>-<n>`) names a row in `flow.cursors` holding
the list, the binding (the canonical channel, window or filter as JSON) and
the last sort key served. A token from another list, another binding or
another database is `InvalidCursor`. Tokens survive restarts and work on
every node.

## Data and control flow

```text
flow consumer ── add_resource / record_access / discover / record_transmission / set_detection
surface      ── declare (config) / set_policy / promote / promotion_coverage / reads
                  │
                  ▼
   retry_serializable(pool, policy, body)            BEGIN ISOLATION LEVEL SERIALIZABLE
     body: rows::* reads ─▶ spec logic (plan, PolicyHistory::record, next_origin, ...)
           ─▶ refusal: TxError::Abort(spec error)    (rollback, nothing staged)
           ─▶ writes (channels, resources, policy_decisions, channel_traffic, ...)
           ─▶ outbox::stage(events)                  INSERT INTO flow.outbox
   COMMIT  (40001 / 40P01: rerun the whole body)
     │
     ▼
   Relay::relay   SELECT .. FOR UPDATE SKIP LOCKED ─▶ EventSink ─▶ DELETE sent rows
   promote only:  directory.extend(superseded → promoted)
```

**Discovery race.** Two discoveries from one resource both read it on no
channel and both write its row (and insert their channel). Under
`SERIALIZABLE` the later committer fails with a serialization failure; its
retry reads the resource on the winner's channel and returns
`Existing(winner)` without staging anything. So a resource is on at most one
channel, and one `ChannelDiscovered` is published.

**Traffic at the read.** `channel` and `channels` read each channel row with
two arrays in the same statement: its own resources in joining order, and
the recorded transmissions whose route channel is its canonical channel or a
channel that one superseded. The traffic is `CrossTraffic::tally` of them
with agents resolved through `D`; `ChannelWithTraffic::new` drops it for a
superseded channel. `channels` walks `(created_at, id)` descending in chunks
of 128 and keeps rows `ChannelFilter::keeps` until one more than the page.
`transmissions` walks `(opened_at, transmission_id)` descending over the
canonical channel's members, filtering confirmation in SQL and crossing in
Rust. `resource_use` pages resources by id descending that have an access in
the window and sums accesses per canonical agent and kind.

## Tables (`crates/flow/migrations/0001_flow_store.sql`)

| Table | Holds | Keys and indexes |
| --- | --- | --- |
| `channels` | id, kind, origin, created_at, superseded_by, policy | PK id; `(created_at DESC, id DESC)`; superseded_by; declared ids |
| `resources` | id, locator_key, resource, channel_id, listed_seq | PK id; unique locator_key; `(channel_id, listed_seq)` |
| `accesses` | id, resource_id, agent, at, kind, write_outcome, access | PK id; `(resource_id, at)` |
| `policy_decisions` | channel_id, seq, at, decision | PK (channel_id, seq); `(channel_id, at, seq)` |
| `channel_traffic` | transmission_id, channel_id (stored route), opened_at, confirmed, transmission | PK transmission_id; `(channel_id, opened_at DESC, transmission_id DESC)` |
| `transmissions` | id, state, route, channel_id, opened_at, transmission | PK id; `(state, channel_id, opened_at DESC, id DESC)`; `(opened_at, id)` |
| `verdicts` | transmission_id, revision, verdict, record | PK (transmission_id, revision) |
| `outbox` | seq, event, staged_at | PK seq |
| `cursors` | token, list, binding, after_key, issued_at | PK token; issued_at |
| `shard_ticks` | shard, ticked_through | PK shard |

`channel_traffic` is the registry's record of channel transmissions
(`record_transmission`), kept apart from `transmissions` (the transmission
store's `save`): the two traits are separate in the spec and in the
reference, and `record_transmission` reports `Unchanged` against what it
recorded, not what was saved.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/flow/migrations/0001_flow_store.sql` | Schema `flow` | — |
| `crates/flow/src/store/mod.rs` | Module docs, re-exports, migrations | `MIGRATIONS`, `migrate`, every type below |
| `crates/flow/src/store/codec.rs` | Ids, times and JSON to columns and back | `CodecError` |
| `crates/flow/src/store/error.rs` | `Fault` inside bodies, mapping into spec errors | `FlowStoreError` |
| `crates/flow/src/store/outbox.rs` | Staging and relaying events | `EventSink`, `Relay` |
| `crates/flow/src/store/cursor.rs` | The cursor book and paging | `prune_cursors` |
| `crates/flow/src/store/directory.rs` | Supersession cache, shard keys | `ShardKey`, `ShardIndex` |
| `crates/flow/src/store/ids.rs` | Declared channel ids | `ChannelIdSource`, `UlidChannelIds`, `IdSourceError` |
| `crates/flow/src/store/shards.rs` | Shard tick checkpoints | `PgShardTicks` |
| `crates/flow/src/store/registry/mod.rs` | The registry and its trait impls | `PgChannelRegistry` |
| `crates/flow/src/store/registry/rows.rs` | Row reads and writes, lookup, policy history | — |
| `crates/flow/src/store/registry/declarations.rs` | declare, set_policy, promote, coverage bodies | — |
| `crates/flow/src/store/registry/traffic.rs` | `ChannelTraffic` bodies | — |
| `crates/flow/src/store/registry/detection.rs` | Detection transition function | — |
| `crates/flow/src/store/registry/reads.rs` | `ChannelReads`, resource use, policy history reads | — |
| `crates/flow/src/store/transmissions.rs` | The transmission store and verdicts | `PgTransmissionStore` |
| `crates/flow/src/store/tests/` | Unit, property, model and integration tests | — |

## Invariants and constraints

- At most one channel per resource, also under concurrent discovery
  (INV-852); discover creates only from a resource on no channel whose
  lookup is `NoChannel` (INV-851); lookup creates nothing (INV-850) and never
  names a superseded channel (INV-656).
- Declared patterns never overlap, also under concurrent declarations
  (INV-259).
- A channel's stored policy is its history's current one, written in the
  same transaction (INV-449).
- A promotion applies `promotion::plan` in one transaction (INV-657),
  publishes one `ChannelPromoted` after commit (INV-658), and a refusal
  changes nothing (INV-490); its preview reads like it and changes nothing
  (INV-686); a superseded channel takes no policy (INV-659).
- A confirmation routed through a superseded channel advances the canonical
  channel's detection; the superseded one stays frozen (INV-740). Detection
  moves follow INV-1030 and INV-1031.
- `save` keeps the verdict log (INV-811); `set` never writes the
  transmission (INV-529), stages `VerdictSet` once per append (INV-527).
- Supersession resolves in one step (INV-651), in SQL and in the cache.
- No `unwrap`/`expect` outside tests; every stored value that fails to
  decode is a typed `CodecError` surfaced as the operation's `Store` error.

## Tests

- Unit: codec round trips and ordering, error mapping, ULID ids, the shard
  key of a superseded channel.
- Property (no database): `tests::detection` — INV-1030 and INV-1031 over
  generated origins and transmission states.
- Model (Postgres): `tests::model` drives the reference harnesses'
  strategies (`registry_ops`, `verdict_ops`) on a migrated database emptied
  per case, through the harnesses' own `run_case`
  (`registry_agrees_with_reference_model`,
  `transmission_store_agrees_with_reference_model`). Budgets are smaller
  than the in-memory harnesses' because every step reads everything back
  over the network.
- Integration (Postgres): `tests::registry` (one per registry invariant),
  `tests::concurrency` (discover races, declaration races),
  `tests::verdicts` (outbox, verdicts, at-least-once relay).
