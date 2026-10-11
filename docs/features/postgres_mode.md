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

### Classification and late content

L5 and L6 write the same `flow.transmissions` row; both go through
`TransmissionStore::modify` (a `FOR UPDATE` read and the write in one
transaction, INV-1230), so a classification never drops content the flow
consumer added after the classifier read the row. See
[flow_store.md](flow_store.md#concurrent-writers).

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

### A Postgres outage restarts the gateway (stopgap)

An outage that drops the pipeline lock's own connection (a database
restart, a reset connection, a cut link) fails the lock's next ping
(within `store::lock::PING`, 5 s). The pipeline stops (`pipeline:
stopped: pipeline lock lost: <why> (database reachable|unreachable)`,
`pipeline_lock: lost`, 503), logged as a structured error (`cause =
pipeline_lock_lost`, `session`, `database`). `Running::fatal` resolves
with `FatalCause::PipelineLockLost`; `serve` shuts down gracefully within
`Running::shutdown_bound` and exits non-zero. The supervisor (compose:
`restart: unless-stopped`) starts it again: the proxy listens at once,
captures spool while the database is down (200 degraded,
`waiting_for_database`), and once it answers the pipeline recovers and
the spool drains. Current behaviour: **a Postgres outage restarts the
gateway, a few seconds of proxy downtime**; nothing captured is lost (the
spool is on disk). INV-1221 is scoped to a running process accordingly.

**After a network partition, a restarted gateway waits up to about 25 s
for the old session's lock.** A partition (rather than a Postgres
restart) can leave the stopped process's backend alive on the server,
holding the pipeline advisory lock until the server notices the dead
peer. With the OS's TCP keepalive defaults that is hours; with the
server's `tcp_keepalives_idle=10`, `tcp_keepalives_interval=5`,
`tcp_keepalives_count=3` (crosstalk-infra's deploy/postgres setting) it
is about 25 s. Meanwhile the restarted gateway reports `pipeline_lock:
held elsewhere`, keeps forwarding and spooling, and takes the lock once
the server reaps the session; nothing terminates another backend. The
gateway sets no client-side keepalives: its own side already sees the
cut within the lock's 5 s ping. `spool_outage_twice` allows the lock
`LOCK_RELEASE` (60 s) to come free and otherwise fails naming every
session on the database, which one holds the lock, and the server's
connection count and `tcp_keepalives_*` (`common::gateway::sessions`, read
over the test's own pool, not the gateway's), so it passes once the
server setting is applied.

Follow-up: resume in process (back to waiting for the database and the
lock, recovery again, no restart), with a fresh `PgBus` per pipeline
generation, replaceable `Late` handles and the lock's loss told apart
from another holder.

`serve` and every `crosstalk` command run on `crosstalk_gateway::runtime`
(one builder, shared with the tests that must run as `serve` does).

`Pumped::next` keeps its pull's reply in the `Pumped`, so a stage loop
that drops a `next` future (a command won its `select!`) after the pump
answered no longer loses the delivery until the ack timeout; settles that
kept draining could lose it on every redelivery and hang.
`transport.stage-pump.no-dropped-delivery` (INV-X; the layer prefix is the transport consumer contract the gateway's pump implements) pins it:
`live::pg::pump::tests::a_next_dropped_after_its_answer_keeps_the_delivery`
drives the pump (generic over `Subscription`) with a scripted
subscription on a paused clock, drops a `next` after its answer, and
requires the next call to return the same delivery at once (it fails on
the old `Pumped`). Memory mode never goes through `Pumped`: `MemorySet`
uses `MpscSubscription`, whose `next` keeps its pending receiver across a
cancellation (`crates/transport/src/bus/subscription.rs`, `next`).

### Cancellation audit (bus reads, outbox polls, stream reads)

Every `select!`, `timeout` or race in Postgres mode with a bus `next`, an
outbox poll or a stream read as a branch. A branch is cancel-safe when
dropping it mid-flight loses nothing it already took.

| Where | Branches | Cancel-safe | Holds a transaction or connection across an await here | Why |
| --- | --- | --- | --- | --- |
| generic stage loop, `crates/gateway/src/live/stage.rs:484` (L3, L4, L6, L7, surface relay) | commands, `Pumped::next` | yes | no (a oneshot reply only) | `Pumped::next` keeps its pull's reply in the `Pumped` (`pump.rs`, `next`); a dropped call's answer goes to the next call |
| durable flow loop, `crates/gateway/src/live/pg/l5.rs:116` | commands, checkpoint interval, extracted inputs (mpsc), `Pumped::next` | yes | no in the branches; the checkpoint's transaction runs in an arm body, never raced | `Interval::tick` and mpsc `recv` are cancel-safe; `Pumped::next` as above |
| the pump's fetch, `crates/gateway/src/live/pg/pump.rs:117` | requests (mpsc), `PgSubscription::next` | yes | no since 686e5c9: the take's transaction lives in its own task. Before, yes: a fetch dropped mid-take dropped its transaction | an ack or nack arriving mid-fetch (the flow loop acks at its checkpoints while a pull waits) drops the fetch; `PgSubscription::next` runs each take in a task of its own and resumes it on the next call (`crates/transport/src/pg/subscription.rs`, `take_task`) |
| a take's task, `crates/transport/src/pg/subscription.rs` (`try_take`) | none (runs to its commit) | not raced | yes, the take's transaction across its statements (row locks on `transport.deliveries`, `groups` `FOR UPDATE`) | DB awaits only. Aborted when its subscription drops (`Drop for PgSubscription`), so no take outlives its subscription, bus and pool; its rollback is flushed as the connection returns to the pool, or the server ends it after `idle_in_transaction_session_timeout` |
| `PgSubscription::next`'s wait, `crates/transport/src/pg/subscription.rs:437` | closed, wake (watch), sleep | yes | no | waits only; the take finished before it |
| bus reaper, `crates/transport/src/pg/reaper.rs:219` | messages (mpsc), next deadline | yes | `fail` holds its transaction across DB awaits only, after the select | `lock_timeout = 2s`: a row locked by a stuck session delays that reaping only |
| topology outbox relay, `crates/topology/src/outbox.rs:223`, and `publish_batch` (`:340`) | wake (mpsc), sleep | yes | yes in `publish_batch`: the `topology.outbox` transaction (with its advisory transaction lock) is held across each `announce`, a bus publish through the spool | not raced; when the relay task is aborted (shutdown, kill) the transaction is dropped and rolled back on the connection's return, or ended by the server's timeout. Accepted: the publish is bounded by `publish_timeout`, or spools at once |
| exchange log consumer, `crates/gateway/src/log/consumer.rs:68` | none (`while let`) over `Pumped::next` | yes | no | not raced |
| surface relay into the hosted surface, `crates/api/src/in_process/mod.rs:522` | published events (mpsc), settle requests (mpsc) | yes | no in the branches (`apply_all` reads the stores in its arm body) | mpsc `recv` is cancel-safe |
| outbox forwarder (memory set), `crates/gateway/src/live/relay.rs:33` | flushes, outbox (mpsc) | yes | no | mpsc `recv` |
| spool drainer, `crates/transport/src/spool/drain.rs:65` | notify, sleep | yes | no (its batch publish is one transaction in `PgBus::publish_batch`, after the select) | waits only |
| bus listener, `crates/transport/src/pg/listen.rs:24` | none | yes | a `LISTEN` connection, no transaction | a lost connection reconnects; notifications are hints |
| pipeline lock holder, `crates/gateway/src/store/lock.rs` (`hold`) | interval | yes | its own connection (session advisory lock), no transaction | dropped, the connection closes and the server releases the lock |
| `Live::shutdown` drain and joins, `crates/gateway/src/live/mod.rs:547`, `:562`, `:620`; `Live::kill` | the stages' drain, a group's emptiness, each task by a deadline | not by design | tasks aborted there may hold a transaction | an aborted task's transaction is rolled back when its connection returns to the pool (the runtime is alive), or when the process exits (the socket closes); a held delivery left behind is returned by the next start's `recover_held` |
| `settle`/`quiet`, `crates/gateway/src/live/settle.rs` | none over the bus | yes | no | commands and group stats only |
| recovery, `Live::start_pg` | none | yes | per-statement only | sequential |

Every Postgres-mode pool (`store::lazy_pool_named`; the gateway names its
sessions `crosstalk-<role>-<pid>`, the pipeline lock
`crosstalk-pipeline-lock-<pid>`) sets
`idle_in_transaction_session_timeout = '30s'` on each new connection:
whatever leaves a transaction idle (a task parked or abandoned mid-way,
a process that keeps a socket open but stops driving it), the server ends
the session within 30 s and its locks are released. Regression:
`crosstalk_gateway::integration::pg_an_idle_transaction_is_ended_and_its_locks_released`.

The memory set never goes through `Pumped`: `MemorySet::Sub =
MpscSubscription` (`crates/gateway/src/live/store_set.rs:70`), whose
`next` keeps its pending receiver across a cancelled call
(`crates/transport/src/bus/subscription.rs:142-146`).

Regression tests: `live::pg::pump::tests::a_next_dropped_after_its_answer_keeps_the_delivery`
(no database) and `crosstalk_transport::integration::pg_next_dropped_mid_take_hands_out_the_hold_on_the_next_call`
(Postgres: a `next` dropped mid-take; the take commits on its own and the
next call hands the hold out at attempt 1, at once).

### A delivery lost to a race: redelivered, or dead-lettered

What happens to a held row nobody acks (`crates/transport/src/pg/reaper.rs`,
`fail`):

- While the process's bus runs, its reaper takes the hold back at the
  real deadline (handout + `ack_timeout`, 30 s by default) or, for a take
  whose caller never saw it, the provisional one (take start + 2 ×
  `ack_timeout` + `publish_timeout`, 65 s). That counts as an attempt
  (the take already counted it). Below `retry.max_attempts` (`bus.retry`,
  5 by default) the row is delayed by the backoff (`initial_backoff`
  100 ms doubled per attempt, capped at `max_backoff` 30 s) and
  redelivered: nothing is lost, the event arrives up to about
  `ack_timeout` + backoff late (30 to 65 s, plus up to 30 s).
- The same delivery lost `max_attempts` times in a row is dead-lettered
  (`transport.dead_letters`, "ack timeout after 30000 ms"): kept, listed,
  replayable (`ReplayDeadLetter`), but no longer delivered on its own. For
  the surface relay that means a node fact or live feed event not applied
  until a replay or the next rebuild (`InProcess::host` rebuilds node facts
  at start).
- When the bus's tasks are stopped (`Live::shutdown`/`kill`, the
  lock-loss stopgap), nothing reaps: a held row stays held until the next
  process's `recover_held`, which delays it with the attempt counted, or
  dead-letters it on its last attempt.

So a single lost race delays an event by at most about 95 s and loses
nothing; only five losses of one delivery dead-letter it. With every
consumer cancel-safe (the table above), no race is known to lose one.

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
| `crates/gateway/src/runtime.rs` | the runtime `serve` runs on, shared with the tests | `builder`, `build` |
| `crates/e2e/src/answers.rs` | the answer set and the renaming of minted ids | `answers`, `Answers::renamed` |
| `crates/e2e/tests/common/{mod,gateway}.rs` | the Postgres-mode harness and the gateway outage harness | `start_pg`, `uninterrupted`, `Gateway`, `Run`, `outage` |
| `crates/e2e/tests/{spool_outage,spool_outage_twice,spool_outage_serve_runtime}/main.rs` | the outage stopgap end to end | tests |

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
- Outage stopgap (Postgres, a cuttable `DbLink`, wiki relay through the
  proxy): `spool_outage::database_cut_mid_stream_spools_then_drains_to_the_uninterrupted_answers`
  (cut, forwarded unchanged and spooled, `Running::fatal` = lock lost, a
  graceful stop, a fresh start on the same data directory with the
  database still down, the drain, every capture in the log once, the
  uninterrupted run's answers once renamed), `spool_outage_twice::database_cut_twice_drains_to_the_uninterrupted_answers`,
  and `spool_outage_serve_runtime::database_cut_on_the_serve_runtime_restarts_and_drains_to_the_uninterrupted_answers`
  (every gateway task on `runtime::builder()`, 2 MiB worker stacks).
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
