# Surface service (`crosstalk-surface`, in-process `crosstalk-api`)

The L8 surface as running code (roadmap P2.6): `QueryApi`,
`OperatorActions` and `LiveFeed` from
`spec/types/interfaces/l8_surface*`, export with its manifest and audit,
and the spec's `NodeFacts` cache, all generic over the spec's L3–L8 store
traits. `crosstalk-api` builds it in process over the `crosstalk-memory`
reference stores, the backend a UI links in tests and development.

The contract is the spec's ([query_surface](query_surface.md),
[read_models](read_models.md), [export](export.md),
[wire_contract](wire_contract.md)); this page is the implementation.

## Scope

- `Surface<S: SurfaceStores>`: every `QueryApi` method, `OperatorActions::act`
  and `Surface::request` (`ActionRequest::into_action`, then `act`), and
  `LiveFeed::subscribe`.
- Permissions per method, checked before any read or effect.
- `Watermarked` reads (the watermark read before the data), paging and the
  cursors the surface issues itself (`transmissions_by_id`).
- Typed errors: every store error through the spec's one `From` impl into
  `QueryError` or `ActionError`.
- The audit record of every action call (`OperatorRecord`, inverting exactly
  through `AuditOutcome`) and every export (refused, started, ended,
  abandoned).
- The live feed: the feed log (epoch, sequence, retention), the writer task,
  streams with resume, resync, heartbeats, lag and session ends, and a bus
  consumer that appends before it acks.
- Export: refusals, plan, limits, header, sealed rows, trailer, the BLAKE3
  row hasher, and `SpecExportSource`, an `ExportSource` over the spec's read
  traits.
- `NodeCache` (the spec's `NodeFacts`) and `NodeFeeder`, which keeps it
  current from L3's and L5's events and rebuilds it from the stores.
- `crosstalk_api::InProcess`: the surface over the memory stores, with the
  relay from the stores' outbox to the node facts and the live feed.

## Non-scope

- The HTTP server, routes, SSE framing on the wire and authentication
  (P7.1). `InProcess::caller` takes a `RequestIdentity` the caller already
  verified.
- Porting the UI fixture's world generator; the UI seeds through the spec's
  write traits on `InProcess::stores`.
- Postgres stores and the integration evidence they carry (P7.3); alert
  sink delivery (`AlertSink` implementations); config loading beyond the
  operator directory; benchmarks.
- The pipeline consumers (L3–L7). In process, nothing turns a published
  `PolicyChanged` into a recorded decision except `SetPolicy` itself, and
  nothing counts accesses or classifications into L7 except whoever seeds.

## Shape

```text
crosstalk-surface (layer crate: depends on crosstalk-spec, blake3, thiserror, tokio, tracing)
  SurfaceStores ── one associated type per spec store trait group, Clone handles for the writers
  Surface<S> { stores, clock: Arc<dyn Clock>, config, ids: IdMinter, cursors: CursorKey,
               search_models: SearchModels, feed: FeedHandle }
     ├─ QueryApi        (query/*)      permission → watermark → store reads → Watermarked / Page
     ├─ OperatorActions (actions/*)    permission → one store write, stamped → audit entry → result
     ├─ LiveFeed        (live/*)       permission → FeedHandle::open → FeedStream
     └─ export          (export/*)     permission → format → watermark → ExportSource::plan → limits
                                       → header → audit Started → SurfaceExport (sealed rows)
  NodeCache ◀── NodeFeeder::apply / rebuild / consume ── AgentReads, ChannelReads, ChannelRegistry
crosstalk-api (composer)
  InProcess::start(options) ─ memory stores ─ one Outbox ─ relay task ─▶ NodeFeeder, FeedHandle
                             └─ Surface<MemoryStores>, operators loaded, node facts rebuilt
```

`SurfaceStores` names, per group: `Agents` (`AgentDirectory`,
`IdentityResolver`, `AgentReads`), `Channels` (`ChannelDirectory`,
`ChannelRegistry`, `ChannelReads`), `Transmissions` (`TransmissionStore`,
`TransmissionVerdicts`), `Topics` (`TopicCatalog`), `Search`, `Embedder`,
`Projections`, `Alerts` (`AlertReads`, `AlertActions`, `AlertRuleStore`),
`Edges` (`EdgeStore`), `Audit`, `Operators`, `Sinks`, `DeadLetters`, `Bus`
(`EventBus`), `Blobs`, `Evidence` and `Export` (`ExportSource`). A store whose
trait writes through `&mut self` is `Clone`, a handle on the same store: the
surface clones it per write and holds no lock of its own across a store call.

`EvidenceRecords` (spans, accesses and resources by id) is a port the spec
does not define yet: no spec read trait returns them. It lives in
`stores.rs` until it moves into the spec.

## Data and control flow

### Queries

Every method starts with `require(caller, permission)`: `Forbidden {
missing }` before anything is read. Then, by area:

- **L7 views** (`topology`, `channel_topology`, `edge_transmissions`,
  `series`, `watermark`) pass through to `EdgeStore`, which reads its
  watermark before its buckets; errors map by `QueryError::from`.
  `overview` takes `EdgeStore::totals` first, then the queues:
  `QueueCounts::tally` over a full traversal of the open alerts and of the
  unreviewed channels in force.
- **Channel rows** (`channel`, `channels`): L7's watermark, then the
  registry's channels, then per row (`query/channel_rows.rs`):
  - superseded: `SupersededInto::of` the superseding channel's record;
  - in force: `ChannelCounts::tally` of a full `resource_use` traversal over
    the window (all time when none: the epoch to the last bucket boundary
    before year 10000, which the stores can encode), transmissions from
    `ChannelCounts::routed` of one `EdgeStore::graph` per page under the
    default filter, and `last` = the later of the latest access and the
    latest confirmation. No spec read returns a channel's latest access, so
    it is found exactly by bisection over `resource_use` windows `[t, end)`
    (at most 64 one-item reads); the latest confirmation is the detection's
    `last_transmission`'s `Confirmed::at`;
  - the seed resource is found among the channel's all-time resources, or in
    the evidence records; `ChannelRow::new` checks the result.
- **Channel names** read each asked channel and the channel it resolves to,
  with discovered channels' seed locators, and run `channels::resolve_names`.
  **The promotion preview** stamps the declaration as `PromoteChannel` would
  (caller, now) and returns `PromotionPreview::from_registry` of
  `ChannelRegistry::promotion_coverage`.
- **Agents**: `AgentReads::list`, then one `EdgeStore::agent_traffic` over
  the page's agents, whose watermark the page carries; `agent` the same for
  `AgentReads::cluster`.
- **Transmission rows by id**: the surface pages the selection itself. The
  first page resolves the selector as a linked view does (retention deciding
  what is retained); the cursor is `hex(version ‖ last id ‖ request digest)
  "_" hex(MAC)` under a keyed BLAKE3 MAC drawn at start, so a forged,
  altered or other request's cursor is `InvalidCursor`. A later page checks
  the pinned version is still retained. Each row is `TransmissionSummary::of`
  with the current verdict (judgeable states only) and the topic from the
  stored classification: no spec read returns a transmission's assignment
  under another version, so one stored under another version reads as
  `Unassigned`.
- **Search** embeds the text for `Semantic` and `Hybrid` only. The surface
  remembers which model each issued search cursor's traversal was embedded
  with (the newest 4096) and answers a later page under another model with
  `Conflict(EmbeddingModelChanged)` before asking the index, whose cursor
  would otherwise refuse the re-embedded query as an unknown cursor.
- **Evidence** reads the transmission, then for each content match the
  origin span (evidence records) and both bodies (blob store, decoded with
  the spec's strict decoder), cutting `Excerpted::of`; then each access its
  co-access records name and its resource; then `TransmissionEvidence::
  assemble` over what was read. A missing record is a store fault (`Store`);
  a dropped body is `BodyDropped`.
- **Projections**: `fit_projection` resolves the version, refuses topics
  outside it (`TopicsNotInVersion`), and enqueues
  `ProjectionInfo::queued` with the embedder's model; the other three pass
  through.
- **Present**: the clock (never earlier than L7's watermark), L7's bucket
  width, the configured formats, `AlertReads::rule_version`, and the
  configured remap threshold and frame retention (the wiring hands the alert
  and projection stores the same values).
- **Topics, history, verdicts, quality, audit, operators, sinks, dead
  letters** pass through; `policy_history` and `verdicts` turn the store's
  unknown-id error into `None`.

### Actions

```text
act(caller, action): at = clock.now()
  caller lacks action.required_permission() ─▶ Err(Forbidden { missing })
  else apply: one store call, author = caller.operator(), time = at
  record = OperatorRecord::new(caller, action, AuditOutcome::of(&result)); append at `at`
  appended ─▶ result      append failed ─▶ Err(Store)
```

- `SetPolicy`: unknown channel `NotFound`, superseded (through
  `ChannelDirectory`) `Conflict(ChannelSuperseded)`; then
  `ChannelRegistry::set_policy` with the caller's decision (so the caller's
  next read shows it; the registry announces it with `Changed::Channel`),
  then one `PolicyChanged` envelope on the bus. `Recorded::Duplicate` is
  `Unchanged`.
- `MergeAgents` → `IdentityResolver::merge(request, at)` → `Merged(id)`;
  `Unmerge` → `unmerge(merge, caller, at)`; `RenameAgent` → `rename`.
- `PromoteChannel` → `ChannelRegistry::promote(channel, Promotion::new(..,
  caller, at, ..))` → `ChannelPromoted { channel, SupersededChannels::new }`.
- `Acknowledge`, `Resolve` → `AlertActions`; `CreateRule`, `UpdateRule`,
  `SetRuleEnabled` → `AlertRuleStore`; `SetVerdict` →
  `TransmissionVerdicts::set` (`Appended` is `Applied`); pins →
  `TopicCatalog::pin` and `unpin`.
- `ReplayDeadLetter` → `DeadLetterStore::replay`. The spec has no
  `ActionError::from(BusError)`; `actions/errors.rs` maps it as the query
  mapping does (unknown letter `NotFound`, the rest `Store`).
- `Surface::request` stamps an `ActionRequest` with the caller; a merge
  naming one agent twice is `InvalidInput(SelfMerge)` and is not audited.

### Live feed

```text
bus group `live` ─▶ FeedWriter::consume: append each Changed, then ack (nack if the writer stopped)
FeedHandle::append ─▶ writer task: prune by retention (tokio Instant), append (epoch, seq+1),
                      try_send to each visible stream; a full buffer ends that stream with Lagged;
                      then publish the new head (watch)
subscribe(caller, resume) ─ View ─▶ writer: FeedWindow::resume → Live | Replay{after} | Resync(reason)
FeedStream::next: an end reason wins (Lagged, SessionEnded, ShuttingDown) → replay items → buffered
                  live items → after `heartbeat` idle: Heartbeat at max(head read first, last passed)
FeedHandle::end_sessions / end_all_sessions / config_loaded(changes): SessionEnded
```

The writer sends an entry to every stream before it publishes the entry's
seq as the head, so a stream that reads the head and then finds its buffer
empty was sent every entry up to that head: the heartbeat's cursor is the
newest entry delivered or passed over. `config_loaded` ends every stream
when the access mode changed, otherwise the streams of each operator a
`SetOperator` or `RemoveOperator` names.

### Export

```text
export(caller, request): at = now
  permission ── missing ──▶ Refused(Forbidden)        format ── unwritten ──▶ Refused(UnsupportedFormat)
  W = EdgeStore::watermark ─▶ ExportSource::plan(request, W) ── Err ─▶ Refused(QueryError::from)
  ExportLimits::check(rows) ── over ─▶ Refused(Conflict(ExportTooLarge))
  ExportHeader::new(id, request, caller, max(at, W), W, basis, model, gateway, rows)
  append Started ── fails ─▶ Err(Store), no rows
  SurfaceExport { SealedRows(source, Blake3RowHasher), EndGuard }
    trailer ─▶ append Ended        dropped first ─▶ EndGuard appends Abandoned { rows handed out }
```

A refusal whose audit append fails is returned as `Store`. `Abandoned` is
appended from a task spawned on the current runtime (`Drop` cannot await).

`SpecExportSource<E, P, T, M>` plans from the spec's read traits, reading
every row up front (the count must be known first):

| Dataset | Rows from |
| --- | --- |
| projection | `ProjectionStore::projection`, `projection_rows`; labels from `TopicCatalog::topics` |
| accesses | `EdgeStore::channel_topology` per bucket of the settled window |
| edges | `EdgeStore::graph` per bucket under one-topic filters; outliers as what no topic accounts for |
| topics | `EdgeStore::totals` of the settled window under a one-topic filter (aligned windows only) |
| transmissions, verdicts | refused as `Store`: no spec trait lists a window's transmissions |

### Node facts

```text
events naming agents: Changed::Agent, AgentSeen, AgentRenamed, AgentMerged, AgentUnmerged,
                      ConversationDelta (claims change with every exchange, unannounced)
events naming channels: Changed::Channel, ChannelDiscovered, DeclaredChannelUnused,
                        ChannelPromoted (and every superseded channel), PolicyChanged
NodeFeeder::apply ─ re-read each id ─▶ agent: canonical id; AgentReads::cluster → label and stored
                                        parent (the record), state kind, cluster claims; a merged
                                        id's entry removed
                                       channel: ChannelReads::channel; in force → origin, detection
                                        and policy kinds, summary; superseded → removed, and its
                                        superseding channel re-read
NodeFeeder::rebuild ─ AgentReads::list and ChannelReads::channels (in force), swapped in whole
```

The summary is the pattern's text for a channel declared before traffic,
otherwise the seed locator's text, plus ` (+n)` for the further accessed
resources the canonical channel holds (`resource_use` over all time); the
id summary when the seed's locator cannot be read. Text forms are in
`nodes/summary.rs`. The edge store reads the cache synchronously through the
spec's `NodeFacts`; the wiring hands it the same `NodeCache` the feeder
writes.

### In process (`crosstalk-api`)

`InProcess::start(InProcessOptions)` builds every memory store on one
`Outbox` (agents, channels resolving through the agents, verdicts, catalog,
search, projections, alerts, the edge store over the catalog, a
`Directory` of both and the `NodeCache`, audit log, operators, sinks), an
`MpscBus` (its dead letters, and the bus `SetPolicy` publishes on), a
`MemoryBlobStore`, `MemoryEvidence` (spans, accesses and resources the
seeder adds) and a `SpecExportSource`; spawns the feed writer; loads
`options.access` and ends sessions for the changes; rebuilds the node
facts; spawns the relay (outbox → `NodeFeeder::apply`, `Changed` →
`FeedHandle::append`); and builds `Surface<MemoryStores>`.
`InProcess::caller` answers `OperatorStore::caller`; `shutdown` stops the
relay and ends every stream with `ShuttingDown`.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/surface/src/lib.rs` | Crate doc, modules, re-exports | `Surface`, `SurfaceConfig`, `SurfaceStores`, `EvidenceRecords`, `NodeCache`, `NodeFeeder` |
| `crates/surface/src/stores.rs` | The store bundle and the evidence port | `SurfaceStores`, `EvidenceRecords`, `RecordReadError` |
| `crates/surface/src/config.rs` | What `present` reports, export bounds, feed limits | `SurfaceConfig` |
| `crates/surface/src/service.rs` | The service, permission check, windows, pages | `Surface::new`, `stores`, `config`, `feed` |
| `crates/surface/src/ids.rs` | One ULID generator over the clock | `IdMinter` |
| `crates/surface/src/cursor.rs` | Surface-issued cursors; search models per cursor | `CursorKey`, `RequestDigest`, `SearchModels` |
| `crates/surface/src/audit.rs` | Appending entries | (crate) `audit_append` |
| `crates/surface/src/query/mod.rs` | `impl QueryApi`, one line per method | — |
| `crates/surface/src/query/{channels,channel_rows,agents,alerts,topology,topics,content,evidence,projections,admin}.rs` | Per-area handlers | (crate) `*_query` |
| `crates/surface/src/actions/mod.rs` | `impl OperatorActions`, `Surface::request` | — |
| `crates/surface/src/actions/apply.rs` | Each action's store call | — |
| `crates/surface/src/actions/errors.rs` | `BusError` for actions | — |
| `crates/surface/src/live/mod.rs` | `impl LiveFeed`, the feed handle | `FeedHandle`, `FeedClosed`, `FeedStream`, `FeedWriter` |
| `crates/surface/src/live/{log,writer,stream}.rs` | Feed log, writer task and bus consumer, stream | — |
| `crates/surface/src/export/mod.rs` | `QueryApi::export` | `ExportRowsOf` |
| `crates/surface/src/export/{hasher,stream,source}.rs` | Row hasher, audited stream, spec-trait source | `Blake3RowHasher`, `SurfaceExport`, `SpecExportSource`, `PlannedRows` |
| `crates/surface/src/nodes/{mod,feeder,summary}.rs` | Node facts cache, feeder, summaries | `NodeCache`, `NodeFeeder`, `NodeFeedError` |
| `crates/surface/src/tests/` | Unit and property tests over the memory stores (`world.rs` wires them) | — |
| `crates/surface/src/dst/` | Simulation tests under `crosstalk-sim` | — |
| `crates/surface/src/props.rs` | The excerpt property | — |
| `crates/api/src/in_process/mod.rs` | The in-process surface | `InProcess`, `InProcessOptions`, `InProcessError` |
| `crates/api/src/in_process/stores.rs` | The memory stores as `SurfaceStores` | `MemoryStores`, `MemoryEvidence`, `Directory` |

## Invariants and constraints

- Every query and action checks its one permission before reading or
  changing anything; a forbidden action is still audited, as `Forbidden`.
- An action's author and time are the caller's operator and the time the
  surface accepted it; the client stamps nothing.
- An action call that returns `Ok` or a refusal leaves exactly one operator
  entry whose `AuditOutcome::result` is the returned value; `Store` leaves at
  most one. In process the effect and the entry are two calls: an audit log
  that refuses after the effect leaves it unaudited (reported as `Store`); a
  database `SurfaceStores` makes them one transaction.
- `Watermarked` responses carry a watermark read before their data.
- Surface cursors are unforgeable (keyed MAC) and bound to their request.
- The live feed never waits for a stream; within a stream cursors share the
  epoch and never go down; heartbeats carry the newest passed entry.
- An export audits a refusal, or `Started` before its header and then
  `Ended` or `Abandoned`.
- `crosstalk-surface` depends on the spec only (plus blake3, thiserror,
  tokio, tracing); memory, sim, testkit and transport are dev-dependencies.
  Its `tokio` test-util (paused time) comes through `crosstalk-sim`, a
  dev-dependency.
- Time comes from the injected `Clock`; elapsed time (retention, heartbeats)
  from `tokio::time::Instant`.
- No lock is held across an `.await`: the id minter and the search-model
  book are short `std::sync::Mutex` sections, the node cache a `RwLock`.

## Testing

`crates/surface/src/tests/world.rs` wires every memory store as a gateway
would (one outbox, the directories, the node cache in the edge store) with
seven configured operators, one per permission profile, and seeds through
the spec's write traits. Unit tests (`tests::{permissions, actions,
outcomes, alerts, reads, channels, content, export, live, nodes}`), property
tests (`tests::props::*`, `props`) and simulations (`dst::{live, actions,
reads}`, `crosstalk_sim::sim_test!`) are the evidence of the surface
invariants; `crates/api/src/tests.rs` runs the in-process surface end to
end.
