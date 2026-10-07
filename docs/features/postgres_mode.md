# Postgres mode: `serve` on the Postgres stores (P7.3, W8)

With a `store` section, `crosstalk serve` runs the whole pipeline and the
L8 surface on the Postgres stores, `PgBus` behind the publish spool, and
recovers on start: a process killed and started again over the same
database ends in the state of one that never stopped. This page is the
composition (workstream W8 of [postgres_stores.md](postgres_stores.md),
which holds the reviewed design and its decisions). Without a `store`
section nothing changes: memory stores and `MpscBus` (decision Q7).

## Scope

- `crosstalk-api`: the Postgres store bundle `PgStores<E, B>`
  (`SurfaceStores` over one pool, every layer's Postgres store), its
  evidence records `PgEvidence`, the id policy `PgIds`, and `InProcess`
  generic over a `HostedStores` bundle (memory or Postgres).
- `crosstalk-gateway`:
  - `Live` generic over a `LiveStoreSet` (`MemorySet`, `PgSet`), the stage
    machinery shared, the Postgres stages (`live::pg`), the recovery
    sequence (`Live::start_pg`), `PgFrontierSource`;
  - the gate and the `spool` section (`spool.rs`), the `spool_full`
    capture outcome;
  - `crosstalk migrate` running every layer's migrations,
    `--reset-correlator`, the migration head check and the pipeline lock
    (`store/`);
  - `crosstalk spool --discard-corrupt`;
  - `/readyz`, `/healthz` and `/metrics` in Postgres mode;
  - the start of each role in Postgres mode (`gateway/postgres.rs`).

## Non-scope

- The stores themselves, `PgBus` and the spool: W1 to W7 (their crates).
- Split `proxy`/`pipeline`/`api` processes talking through `PgBus`
  (decision Q9: P9). Each role still runs alone; only `all` captures,
  detects and serves end to end. The `api` role reads the stores and
  rebuilds its node facts every few seconds; its live feed carries only
  config loads.
- The restart and outage end-to-end tests through the proxy, conformance
  over Postgres: W9.
- Wiring the L6 search and alerts consumers, a real embedder and topic
  model (P6).
- A volume free-space check against `spool.max_bytes` (no portable
  statvfs without a new dependency; see Gaps).

## Data and control flow

### Start (`gateway::start`, role `all`)

```text
config (store section) ─▶ DATABASE_URL, deployment secret (ingress.secrets)
lazy pool (no connection yet) ─▶ PgBus::new ─▶ SpoolingBus::open(<data dir>/spool, Gated<PgBus>)
   capture pipeline over the spool (Bodies::SkipStored: FsBlobStore never deletes)
   proxy listener: forwards and captures at once            ops listener   api listener (503 until set)
pipeline task:
   wait for the database (status waiting_for_database)
   ─▶ migrations::check_heads: every layer's applied head == the binary's (behind: not ready, re-checked)
   ─▶ PipelineLock::try_take (held elsewhere: not ready, retried; its own connection, pinged)
   ─▶ Live::start_pg:
        recovering_bus     PgBus::recover_held
        relaying_outboxes  PgStores::open (L6 outboxes flushed), PgAgents::flush_outbox,
                           flow Relay::relay, topology drain
        restoring_flow     FlowConsumer::with_durability(PgFlowDurability)::restore
                           (IncompatibleSnapshot: stopped, needs migrate --reset-correlator)
        rebuilding_nodes   InProcess::host (operators loaded, node facts rebuilt, new feed
                           epoch), Surface::recover_interrupted, persisted watermark read
        subscribing        exchange log + every slot's group (pumped), stages spawned,
                           L7 outbox relay, retention tick, ticker; the gate opens
   ─▶ running; the API router is set; until shutdown, or the lock is lost (pipeline stops)
```

**The gate.** A consumer group that does not exist starts at the log's
head. On a fresh database the pipeline groups are created only when
recovery subscribes them, while capture runs from the first second. So
the bus behind the spool refuses publishes (`Disconnected`) until every
group subscribed: captures and recovery's own publishes wait in the
spool, fsynced, and drain in order under their ids once the gate opens.
`/readyz` shows `capture: spooling` until then.

### Stages over Postgres (`live::pg`)

| Slot | Stage | Over |
| --- | --- | --- |
| L3 | `PgReconstruct`: `ReconstructConsumer`, the exchange kept in `PgExchanges` first | `PgAgents`, `PgConversations` |
| L4 | `PgProvenanceStage`: the engine, then `ExtractionStep` handing to L5 through `DurableInputs`; publish after `deliver` returned `Ok`; tick: `expire` and ledger expiry at `content_retention` | `PgProvenanceStore`, `PgFingerprintIndex`, `PgExtractionLedger` |
| L5 | its own loop (`l5.rs`): commands, checkpoint interval, extracted batches, deliveries kept unacked; checkpoint then ack; under `Ticking::OnSettle` also on every tick | `PgChannelRegistry`, `PgTransmissionStore`, `PgFlowDurability` |
| L6 | `PgClassify`: `crosstalk_analysis::classify::Classifier` (derived id), publish then ack | `PgTopicCatalog`, `PgTransmissionStore` |
| L7 | `PgTopology`: `topology::consumer::handle`; tick: `PgFrontierSource` then `advance_watermark` | `PgEdgeStore` |
| surface relay | node facts and live feed from the bus | the hosted surface |

No evidence slot: `PgEvidence` reads spans from L4's `SpanIndex` and
accesses and resources from L5's tables (a read-only stopgap read of
`flow.accesses`/`flow.resources`: the Postgres registry does not
implement the spec's `AccessStore` yet).

**Pumped subscriptions.** `PgSubscription::next` dropped while its take
commits leaves the delivery held until the reaper's provisional deadline.
Stage loops `select!` deliveries against commands, so each stage reads
through a pump task (`pump.rs`) that fetches only on a pull and keeps a
fetched delivery for the next pull. The one remaining cancellation is an
ack that arrives while the pump fetches (the flow consumer's checkpoint
acks).

### The bus clock

`PgBus` times its delays (a nacked or timed-out delivery's
`available_at`, a recovered hold's backoff) with the wall clock
(`live::pg::bus_clock`), whatever clock the live process runs on, as
`MpscBus` times its retries on tokio time. A delayed delivery becomes
ready only once the bus clock passes its `available_at`; under a manual
clock that only `Live::settle(until)` moves, a delay taken during a settle
never came due while that settle waited for the groups to empty, and the
settle waited forever. That is what hung the restart e2e on node0: a
retried or timed-out delivery, for example one whose take a pump cancelled
for a checkpoint ack (held until the reaper's provisional deadline, then
delayed), sat delayed past a frozen manual clock. `serve` runs on the wall
clock, so a real restart never hit it; the fix keeps every harness on a
manual clock out of it (regression: `integration::pg_settle_finishes_while_a_delivery_is_retried`).

### Bodies across a restart

`Excerpted::BodyDropped` means only that the blob store has no body under
the hash (`BlobStore::get` returned `None`). No content-retention policy
exists yet: neither blob store deletes anything (the spec's `BlobStore`
has no delete), so no drop decision has to survive a restart. `serve`
reopens `FsBlobStore` at `blobs.root` on its volume and shows the same
excerpts after a restart. The restart e2e first gave each process a fresh
in-memory blob store, so the restarted one found no bodies and showed
`BodyDropped` where the uninterrupted run showed excerpts; it now reopens
one filesystem store across the restart, as `serve` does. When content
retention lands and deletes bodies, `Bodies::SkipStored` must give way to
`PutEvery` (or learn of deletions) and the evidence of a dropped body must
read `BodyDropped` before and after a restart alike.

### Diagnosing a stalled wait

`live::pg::diagnose` reads what a Postgres-mode process waits on: the
frontier against the watermark and the frontier's parts (shard ticks, the
spool's oldest record, unadmitted entries per group, group stats), the
pipeline lock's holders (`pg_locks`), the spool's state and counters, the
four outbox tables, the pool (size, idle, in use) and the recovery steps
reached. `Live::<PgSet>::diagnose` and `PgParts::diagnose` (a clone kept
aside before `start_pg`) read it; `diagnose::bounded(limit, what, work,
diagnosis)` runs a wait for at most `limit` and returns `Stalled` (what,
how long, the diagnosis) past it. Every Postgres-mode test bounds its
waits on settles, recovery, deliveries, the lock and shutdowns with it
(five minutes each), so a stall fails with its cause instead of hanging.

### Settling and shutdown

`Live::settle` is shared. A set with deferred acks (`PgSet`) drains the
stages (the flow consumer checkpoints and acks on a drain) before it
reads the groups; quiet means no pending delivery in any slot's group
(`PgBus::group_stats`), nothing in the spool, and no row in the four
store outboxes. Shutdown drains the stages the same way, stops the bus,
joins the stages; the gateway then closes the spool's drainer, the bus
and the pool. `Live::kill` stops as a killed process would (every task
aborted, nothing acked or checkpointed, the bus's tasks stopped without
touching its tables), for restart tests.

### Frontier (`live/frontier.rs`)

```text
ticked_through = PgShardTicks::ticked_through(shards)          (epoch until every shard ticked)
oldest_pending = min(pending and dead-letter `at` of the five pipeline groups,
                     oldest `at` of a log entry a pipeline group takes but has not admitted,
                     spool oldest_at)
```

A group admits log entries into deliveries only when its consumer asks
for the next one, so `group_stats` misses an envelope in `transport.events`
past the group's `admitted_through` (the Postgres run of
`pg_frontier_covers_the_spool` found it: a drained envelope was pending in
no statistic). The frontier reads those entries itself (`UNADMITTED`, a
read-only query of the `transport` schema; the bus has no read for it). The
spool is read first, then the unadmitted entries, then the group stats, so
an envelope moving from the spool to the log, or from the log into a
delivery, between two reads is counted by one of them. In-flight proxy exchanges are
not counted, as in memory mode (no registry exists; their times are later
than `now - settle_after`).

### Readiness, health, metrics

| `/readyz` field | Values |
| --- | --- |
| `status` | `ok`, `degraded` (database down, recovery, spool not `direct`), `draining` |
| `database` | `reachable`, `unreachable: <why>` |
| `migrations` | `unknown`, `at_head`, `behind: flow 1 < 2, ...` |
| `pipeline_lock` | `not_taken`, `held`, `held elsewhere`, `lost` (absent for `api`) |
| `capture` | `durable`, `spooling`, `draining (<n> records)`, `dropping: spool full`, `spool corrupt: <segment>` (roles that capture) |
| `pipeline` | `waiting_for_database`, `waiting_for_migrations`, `waiting_for_lock`, `recovering`, `running`, `stopped: <why>` |
| `recovery` | `pending`, a step, `done`, `failed: <why>` |

Ready (200) unless: the spool is full or corrupt, migrations are behind,
the lock is held elsewhere or lost, the pipeline stopped, a task stopped,
or the process drains. The `api` role also needs the database reachable
and migrations at head. A database outage, recovery or a drain leave it
ready, degraded. "Full" means the last capture was refused full and the
spool has not drained back to `direct` since.

`/healthz` adds `bus.groups.<name>.{pending, oldest_pending_micros,
dead_letters}` (read from the database; left out while it does not
answer), `recovery.{outbox_relayed, flow_checkpoint_micros,
accesses_refed, deliveries_redelivered}` and `spool.{state, records,
bytes, oldest_at_micros, oldest_age_seconds, max_bytes, appended,
drained, rejected_full, rejected_io, truncated_bytes,
capture_spool_full}`; `live.watermark_micros` is the persisted watermark
from the first report. `/metrics` adds `crosstalk_spool_*` (`state{state}`, `bytes`,
`records`, `oldest_age_seconds`, `appended_total`, `drained_total`,
`rejected_total{reason="spool_full"|"spool_io"}`, `truncated_bytes_total`:
the names and label values crosstalk-infra's dashboard and alerts use) and the
`spool_full` reason of `crosstalk_capture_uncaptured_total` and outcome of
`crosstalk_pipeline_exchanges_total`.

### Commands

- `crosstalk migrate --config <path> [--reset-correlator]`: extensions,
  then every layer's migrations (`transport`, `canonical`, `reconstruct`,
  `provenance`, `flow`, `analysis`, `topology`, `surface`); idempotent.
  `--reset-correlator` then replaces L5's checkpoint with empty shards
  (`PgFlowDurability::reset_correlator`, decision Q2).
- `crosstalk spool --config <path> --discard-corrupt`: with the gateway
  stopped, drops the corrupt record that stopped the drain and everything
  after it in its segment.

### Ids and cursor keys

`serve` draws every Postgres-mode generator from OS entropy
(`gateway::postgres::SERVE_IDS = PgIds::Entropy`): sink stamps, agent,
merge, conversation, declared channel, alert, operator-audit and surface
ids, envelope ids of captures. Tests pass `PgIds::Seeded(seed)`, which
seeds each generator with `seed ^ purpose` as the memory set does (so a
seeded Postgres run mints the memory run's agent and conversation ids).
Cursor keys derive from the deployment secret, one label per store
(`crosstalk.cursor.v1.{surface,agents,conversations,audit,exchanges,
topics,search,projections,alerts}`).

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/api/src/pg/mod.rs` | the bundle, its types, open | `PgStores`, `PgOpen`, `PgSettings`, `PgIds`, `PgDirectory`, `PgStoresError`, store type aliases |
| `crates/api/src/pg/evidence.rs` | evidence records over Postgres | `PgEvidence` |
| `crates/api/src/in_process/mod.rs` | `InProcess<S: HostedStores>`; `host` | `HostedStores`, `CursorSecret`, `InProcess::host` |
| `crates/gateway/src/live/store_set.rs` | the set trait, memory set | `LiveStoreSet`, `Quiet`, `MemorySet`, `MemoryQuiet` |
| `crates/gateway/src/live/pg/mod.rs` | the Postgres set and recovery | `PgSet`, `PgLayers`, `PgQuiet`, `PgParts`, `Live::start_pg` |
| `crates/gateway/src/live/pg/{l3,l4,l5,l6,l7}.rs` | the stages | `PgReconstruct`, `PgProvenanceStage`, `PgFlowConsumer`, `PgClassify`, `PgTopology` |
| `crates/gateway/src/live/pg/pump.rs` | cancel-safe subscriptions | `Pumped`, `pump` |
| `crates/gateway/src/live/pg/diagnose.rs` | what a stalled wait waits on | `diagnose`, `DiagnoseFrom`, `PgDiagnosis`, `bounded`, `Stalled` |
| `crates/gateway/src/live/frontier.rs` | the frontier | `PgFrontierSource`, `SpoolBacklog`, `combine` |
| `crates/gateway/src/live/recovery.rs` | status for ops | `PipelineStatus`, `StatusReporter`, `RecoveryStep`, `RecoveryReport` |
| `crates/gateway/src/spool.rs` | gate, spooled bus, discard | `Gate`, `Gated`, `LiveBus`, `discard_corrupt` |
| `crates/gateway/src/store/{mod,migrations,lock}.rs` | migrate, head check, lock, lazy pool | `migrate`, `MigrateOptions`, `LAYERS`, `check_heads`, `PipelineLock`, `lazy_pool` |
| `crates/gateway/src/gateway/{postgres,late}.rs` | Postgres-mode start, API before the surface | `PgRunning`, `spawn_pipeline`, `spawn_api`, `SERVE_IDS`, `LateRouter` |
| `crates/gateway/src/ops/{mod,metrics}.rs` | readiness, health, metrics | `PgOps`, `BusReport`, `SpoolReport` |
| `crates/gateway/src/config/sections.rs` | `store.bus`, `spool` | `SpoolSection`, `StoreSection::MIN_CONNECTIONS` |

## Tests

- No database (run): `postgres_down::proxy_forwards_through_outage_and_full_spool`
  (INV-1221: every corpus case forwarded unchanged with the database down,
  captures spooled, `/readyz` 200 degraded `capture: spooling`; then a
  spool too small for anything: every capture `spool_full`, the proxy
  still answers, `/readyz` 503 `dropping: spool full`, the metrics),
  `postgres_down::a_second_process_on_the_data_directory_is_refused`;
  `dst::frontier_covers_spooled_envelopes` (INV-1217, seeded outages and
  drains over the real spool; a frontier ignoring the spool fails it);
  `dst::classifier_redelivery_republishes_the_same_envelope_ids`
  (INV-1202, the L6 step `serve` runs); `live::tests::serve_seeds_id_generators_from_entropy`
  (INV-1220); config, CLI, frontier, recovery-status and head-check unit
  tests.
- Postgres (`TestDb::new_or_skip`, written, not run by W8: node0 is
  unreachable from the workstream's sandbox): `integration::pg_migrate_runs_every_layer_and_is_idempotent`,
  `integration::pg_readyz_reports_behind_then_every_recovery_step`,
  `integration::pg_second_pipeline_waits_while_the_lock_is_held_elsewhere`,
  `integration::pg_frontier_covers_pending_deliveries` (INV-581),
  `integration::pg_frontier_covers_the_spool`,
  `integration::pg_watermark_survives_a_restart`, and
  `crosstalk-e2e`'s `postgres::postgres_mode_answers_as_memory_mode_and_after_a_restart`
  (the wiki relay through memory-mode and Postgres-mode `Live`, the same
  `QueryApi` answers, and the same after a restart).
- Memory mode unchanged: every `ct-eval replay` of the bench runs is
  byte-identical to the base build (export, evidence, score, report).

## Invariants and constraints

| Invariant | Evidence (agent: ran) |
| --- | --- |
| INV-1217 `topology.frontier.covers-spool` | `crosstalk_gateway::dst::frontier_covers_spooled_envelopes` (ran) |
| INV-1220 `surface.ids.unique-across-restart` | unit `crosstalk_gateway::live::tests::serve_seeds_id_generators_from_entropy` (ran); integration is W9's |
| INV-1221 `ingress.proxy.forwarding-independent-of-capture-store` | `crosstalk_gateway::postgres_down::proxy_forwards_through_outage_and_full_spool` (ran) |
| INV-581 `topology.frontier.covers-pending` | `crosstalk_gateway::integration::pg_frontier_covers_pending_deliveries` (Postgres, not run) |
| INV-1202 `transport.consumer.derived-envelope-ids` | `crosstalk_gateway::dst::classifier_redelivery_republishes_the_same_envelope_ids` (ran; the flag covers the whole list, left for the merger) |

- `serve` never migrates; it consumes only against a database at head.
- One pipeline process per database; a process without the lock neither
  consumes nor relays; a lost lock stops the pipeline.
- Forwarding never waits on the database, the bus or the spool; capture
  publishes through the spool, which refuses only when full.
- No pipeline group misses an envelope published before it first
  subscribed (the gate).
- The flow group acks only after a checkpoint; the bus's ack timeout must
  exceed `flow.checkpoint_ms` and `flow.checkpoint_unacked` must fit the
  group capacity (checked at start).
- Memory mode is byte-identical to before (replays).
- Pool: at least `StoreSection::MIN_CONNECTIONS` (3); the pipeline lock
  holds a connection of its own.

## Gaps

- `PgTransmissionStore::list` was a stub returning `Store`; W8 implemented
  it (see [flow_store](flow_store.md)). No other "not implemented on
  Postgres" stub is left in the stores `serve` uses.
- `PgChannelRegistry` implements neither the spec's `AccessStore` nor a
  resource read by id; `PgEvidence` reads `flow.accesses`/`flow.resources`
  itself, and `PgQuiet` counts the four outboxes by SQL. Both belong in
  `crosstalk-flow` (and the layer crates) as reads.
- The free-space warning against `spool.max_bytes` is not implemented.
- The `api` role, run beside a pipeline process, records leftover action
  intents as interrupted at its own start (single-node assumption, Q9).
- A start that fails after `PgStores::open` leaves the hosted surface's
  relay and feed tasks to end on their own.
