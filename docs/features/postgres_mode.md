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
- The restart and outage end-to-end tests and conformance over Postgres
  are W9's; see [Restart, outage and conformance
  tests](#restart-outage-and-conformance-tests-w9).
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

Every cancellable point in Postgres mode, in the gateway's live process
(`crates/gateway/src/live/pg`), the bus (`crates/transport/src/pg`), the
spool, and every layer's outbox relay and stamping. For each: whether a
`select!`/timeout can drop it mid-flight safely, whether it holds a
transaction or a connection across an await there, and what a dropped
future, a runtime shutdown or `Live::kill` leaves behind.

| Where | Cancel-safe | Holds a transaction or connection across an await here | A dropped future, runtime shutdown or kill leaves |
| --- | --- | --- | --- |
| generic stage loop `crates/gateway/src/live/stage.rs:484` (L3, L4, L6, L7, surface relay): commands vs `Pumped::next` | yes: `Pumped::next` keeps its pull's reply in the `Pumped` | no (a oneshot reply) | nothing; a dropped `next`'s answer is returned by the next call (`live::pg::pump::tests::a_next_dropped_after_its_answer_keeps_the_delivery`) |
| durable flow loop `crates/gateway/src/live/pg/l5.rs:116`: commands, checkpoint interval, extracted inputs, `Pumped::next` | yes (interval, mpsc and `Pumped::next` are) | no in its branches; the checkpoint's transaction runs in an arm body | killed: unacked deliveries stay held until the next start's `recover_held`; inputs after the last checkpoint are re-fed by `restore` |
| the pump `crates/gateway/src/live/pg/pump.rs:117`: requests vs `PgSubscription::next` | yes | no: since 686e5c9 a take's transaction lives in its own task | dropped with its subscription: the take is aborted (below) |
| a take's task, `crates/transport/src/pg/subscription.rs` `try_take` (spawned by `take_task`, wrapped in `progress::Tracked`) | runs to its commit; abandoned past `take_limit` | yes: the take's transaction (group row `FOR UPDATE`, delivery rows) across its statements | aborted (subscription dropped, or stalled past `take_limit` = max(2 × publish timeout, 10 s) outside `acquire`): the transaction is dropped and rolled back as the connection returns to the pool; a hold it already committed is reaped at the provisional deadline. Its own `lock_timeout` (= the limit) ends a server-side lock wait first. Runtime shutdown: the socket closes and the server rolls back. Any other stall: the server ends it after `idle_in_transaction_session_timeout` (30 s) |
| `PgSubscription::next`'s wait `crates/transport/src/pg/subscription.rs` (`closed`, `wake`, sleep) | yes | no | nothing |
| bus reaper `crates/transport/src/pg/reaper.rs:219`, `fail` | yes (work after the select) | `fail`'s transaction across DB awaits only, `lock_timeout = 2s` | a lock wait delays that reaping only (retried at the poll); shutdown or kill stops reaping, and the next start's `recover_held` returns what was held. Its liveness is reported (`PgBus::reaper`) |
| bus listener `crates/transport/src/pg/listen.rs:24` | yes | a `LISTEN` connection, no transaction | nothing (notifications are hints) |
| `PgBus::publish` / `publish_batch` `crates/transport/src/pg/publish.rs` | not raced (each awaited to the end) | one transaction (with its append lock) across DB awaits only | dropped mid-commit: rolled back, or committed; the caller's retry is idempotent on id |
| spool append `crates/transport/src/spool/mod.rs` (`blocking`) | yes | no (the file work runs on the blocking pool and finishes even if the caller goes) | a torn tail at most, truncated at the next open (`transport.spool.torn-tail-only`) |
| spool drainer `crates/transport/src/spool/drain.rs:65` | yes (notify, sleep) | its batch publish is one bus transaction, after the select | killed between a batch's commit and the cursor: the batch is re-sent, deduplicated by id |
| topology outbox relay `crates/topology/src/outbox.rs:223` (wake vs sleep), `publish_batch` | yes | **no since this change**: the batch is read in one transaction and deleted in another, under the drain lock; the publishes run between them. Before, the read transaction (with its advisory lock) was held across every publish | a relay aborted or hung mid-publish holds nothing; unpublished rows stay stamped and are republished under their ids (`integration::outbox::a_relay_hung_mid_publish_holds_no_transaction`) |
| reconstruct, flow and analysis outbox relays (`crates/reconstruct/src/agents/outbox.rs` `relay`, `crates/flow/src/store/outbox.rs` `Relay::relay`, `crates/analysis/src/pg/outbox.rs` `relay`) | not raced | stamping is one committed transaction; publishes and the delete run outside it | rows stamped and not deleted are republished under their ids by the next relay (INV-1211 to INV-1213) |
| store writes through `crosstalk_store::retry_serializable` (every layer) | not raced | one attempt's transaction across DB awaits only; the backoff sleeps outside it | an aborted task's transaction is rolled back on the connection's return, or by the server's timeout |
| exchange log consumer `crates/gateway/src/log/consumer.rs:68` | yes (not raced) | no | nothing |
| surface relay into the hosted surface `crates/api/src/in_process/mod.rs:522` | yes (mpsc) | no in its branches | node facts are rebuilt at the next start |
| outbox forwarder (memory set) `crates/gateway/src/live/relay.rs:33` | yes (mpsc) | no | memory mode only |
| pipeline lock holder `crates/gateway/src/store/lock.rs` (`hold`) | yes | its own session (advisory lock), no transaction | dropped: the connection closes and the server releases the lock; a partition: released when the server reaps the session (tcp keepalives, about 25 s) |
| `Live::shutdown` `crates/gateway/src/live/mod.rs:547`, `:562`, `:620`; `Live::kill` | not by design | aborted tasks may hold a transaction | rolled back on the connection's return (the runtime is alive) or when the socket closes; the backstop ends any left idle |
| settle and quiet `crates/gateway/src/live/settle.rs`, recovery `Live::start_pg` | yes | per statement only | nothing |

**Backstop.** Every pool from `store::lazy_pool_named` (the gateway names
its sessions `crosstalk-<role>-<pid>`, the pipeline lock
`crosstalk-pipeline-lock-<pid>`, the e2e harness
`crosstalk-e2e-live-<n>`) sets `idle_in_transaction_session_timeout =
'30s'` in `after_connect`: whatever leaves a transaction idle, the server
ends that session within 30 s and its locks are released
(`crosstalk_gateway::integration::pg_an_idle_transaction_is_ended_and_its_locks_released`).

**What a stall's diagnosis shows.** `live::pg::diagnose` lists the
pending deliveries (state, attempt, due time, last error), every session
that is idle in a transaction or holds locks on the bus tables (with its
`application_name`, client port, backend and transaction start), this
process's takes in flight (group, statement, running time, time since its
task was last polled), how many takes were abandoned past their limit,
and the reaper's liveness.

**The abandoned transactions (node0, 686e5c9, `spool_outage` run 3).**
Six sessions idle in transaction since the same 7 ms, each the bus's take
transaction (`crates/transport/src/pg/subscription.rs`, `try_take`): one
per pipeline group of the reader process (the six stage groups; the
gateway has a seventh, the exchange log), started together by the wake-up
of a settle tick's publish. The server waited for the client
(`ClientRead`) after statements that the take follows at once with
another (`make due` then `take`; `take` then `commit`; `next due` then
`commit`): the client had the connections checked out (9 of 11 in use)
and stopped driving them. No rollback was queued because the
transactions were never dropped: sqlx queues a rollback only when a
`Transaction` is dropped, and these futures were parked, alive. Meanwhile
the relay group's delivery (seq 51) stayed held past every deadline: the
reaper did not take it back, either because one of these transactions'
row locks blocked its `fail` (which then waited without bound) or
because the reaper itself was not being polled; the diagnosis now
reports the reaper's liveness to tell which. Why the takes stopped
being polled is not yet known; the takes' poll times and phases in the
next stall's diagnosis tell a scheduling stall (`last polled` long ago)
from a wait on I/O (`last polled` recent). Either way the take is now
abandoned at its limit and rolled back, its lock waits end at
`lock_timeout`, and the server ends whatever is left after 30 s.

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

## Restart, outage and conformance tests (W9)

Tests (`crates/e2e`, `crates/api/tests`, `crates/client/tests`, a
skip hook in `crates/conformance`); the production fixes they led to are
described above. Every wait is bounded at five minutes
and fails with `live::pg::diagnose` (or, for a gateway, its readiness,
health and diagnosis).

### Comparing runs: `crosstalk_e2e::answers`

`answers(surface, caller, transmissions, scenario, watermark)` reads the
whole answer set (agents by session, the topology graph, the A to B
channel edge's transmissions, every stored transmission with its summary
and evidence page, the channels, the watermark), the comparison
`tests/postgres.rs` makes. `Answers::renamed` makes two runs comparable
when their minted ids differ (a restarted process draws new ids; `serve`
draws from OS entropy): every `<Kind>Id(<ulid>)` in the debug text becomes
`<Kind>Id(@<millisecond>#<n>)`, keeping the millisecond the traffic fixes
and numbering ids per kind and millisecond by first appearance. Agents
are thereby named by their first evidence (A is the agent first seen at
A's first exchange); distinct ids stay distinct, a duplicate stays a
duplicate, and every other byte is compared as read. Checked without a
database by `restart::renamed_answers_agree_across_id_seeds` (two memory
runs with different seeds) and the module's unit tests.

### Kill and restart (`crates/e2e/tests/restart`)

```text
Volume (spool dir, FsBlobStore root, ManualClock) outlives the processes
start_pg(Seeded(SEED + generation)) ─ feed / settle ─▶ kill_pg | stop_pg
   kill_pg: Live::kill, the spool's drainer stopped; the next open retries while LOCK is held
start_pg on the same Volume ─ feed the rest ─ settle(ends), settle(ends + 1 h)
   ─▶ answers.renamed() == the uninterrupted memory run's, at both passes
```

| Test | Kill point |
| --- | --- |
| `killed_while_feeding_finishes_as_uninterrupted` | after each of the first three exchanges (three kills, four processes) |
| `killed_mid_settle_finishes_as_uninterrupted` | 30 ms into the first settle |
| `killed_right_after_a_checkpoint_finishes_as_uninterrupted` | as soon as a `flow.checkpoints` row is rewritten (its `xmin` changes), before the acks it covers |
| `killed_with_outbox_rows_pending_finishes_as_uninterrupted` | as soon as a store outbox holds a row (the run on node0 killed with 4 staged; recovery relayed 4) |
| `graceful_restart_mid_stream_finishes_as_uninterrupted` | graceful stop after A's exchanges |

The aimed kills land a moment after their trigger, so the exact point
varies; each test asserts what holds wherever it lands, and logs what the
database held after the kill and the restart's recovery report.
`cursors_resolve_after_restart` (INV-1219) pages agents, conversations
and the audit log one item at a time and resolves each first page's
cursor after a restart (a later clock, a new seed) to the same second
page. `minted_ids_never_collide_after_restart` (INV-1220) runs three
processes on `SERVE_IDS` (`PgIds::Entropy`), killed or stopped in turn,
each merging B into A, unmerging and renaming at the very same instant:
every merge id is new and no audit id repeats.

A start that finds no free connection (the server's taken by other
sessions, a killed process's not gone yet) is retried every second until
the bound, as `serve`'s pipeline task retries it, logging the server's
connections by `application_name` (`common::start_pg_on`,
`start_survives_a_pool_exhausted_at_open`); `kill_pg` closes the killed
process's pool so its sessions go, as a crashed process's do.

**Fault model: a lossy link.** `DbLink::start_lossy` (crosstalk-testkit)
relays every chunk after a seeded delay, mostly jitter and, with a set
probability, a 200 to 400 ms retransmission stall (`Lossy::tailscale_bad_day`:
15%, the node0 loop's link). `killed_after_a_checkpoint_over_a_lossy_link_finishes_as_uninterrupted`
runs both processes over it, and
`crosstalk_transport::integration::pg_a_hold_no_reaper_tracks_is_swept_and_redelivered`
does too: slowness may delay a decision or a redelivery, never lose one.

### The spool through the gateway (`crates/e2e/tests/spool*`)

The harness is `crates/e2e/tests/common/gateway.rs`. `spool` holds the
spool-full test; `spool_outage`, `spool_outage_twice` and `spool_kill` hold
one database test each, run on 16 MiB stacks (`common::big_stack`, the
killed gateway's runtime too) so a stack overflow aborts one binary only
and names its thread (`gateway-test`, `gateway-test-worker`,
`killed-gateway-worker`).
`spool_outage_serve_runtime` runs the cut-once outage (lock lost, graceful
stop, restart, drain) with every gateway task on a runtime from
`crosstalk_gateway::runtime::builder`, the builder `serve` uses (tokio's
default worker count and 2 MiB worker stacks), only its threads named
`serve-runtime-worker`: an overflow on the outage path there is a
production bug, not the harness's.

The wiki relay is sent as HTTP through `gateway::start_on` (role `all`,
`store` and `spool` sections, a manual clock set to each exchange's end,
the flow tick a day apart) to a `FakeUpstream`; every request and
response is checked byte for byte. `DATABASE_URL` points at a `DbLink`
(the test server's URL with the link's host and port; never printed).
After the last gateway stops, a `Live::<PgSet>` over the same database
and the gateway's blob store settles both passes and reads the answers,
compared renamed with an uninterrupted gateway run's.

- `full_spool_refuses_and_proxy_still_forwards` (INV-1210, no database):
  a spool bounded at what two captures take (measured by a first gateway)
  holds two, refuses two at once (`spool_full`, `rejected_full`), never
  exceeds the bound; the proxy answers all four unchanged; `/readyz` 503
  `dropping: spool full`; the metrics.
- `database_cut_mid_stream_spools_then_drains_to_the_uninterrupted_answers`:
  cut after the first exchange, two exchanges forwarded and spooled
  (`capture: spooling`, `crosstalk_spool_*`); the lock's loss
  (`Running::fatal`), a graceful stop, a fresh start on the same data
  directory with the link still cut (records recovered, 200 degraded,
  `waiting_for_database`); restored, the spool drains (`drained: 2`),
  `/readyz` `ok`; the last exchange; every capture in the log once; equal
  answers. `database_cut_twice_drains_to_the_uninterrupted_answers`: the
  same, twice.
- `gateway_killed_while_spooling_drains_once_after_restart`: the first
  gateway runs on a runtime of its own; cut, one capture spooled, the
  runtime shut down (every task dropped, as a crash); restarted with the
  link still cut, the record is recovered from disk and the next capture
  spools behind it (`waiting_for_database`); restored, drained; every
  capture is in `transport.events` exactly once; equal answers.

### Conformance over Postgres

The L8 conformance suite runs over the Postgres stores in process
(`crates/api/tests/conformance_pg.rs`, feature `world`) and over HTTP
through `crosstalk-client` (`crates/client/tests/conformance_pg.rs`), both
on `crates/api/tests/support/pg_world.rs`:

- seeding the world through the Postgres stores is tens of thousands of
  serializable transactions (far beyond the per-step `WAIT` of 300 s on a
  remote server), so each test process seeds each distinct world once
  into a template database and every test gets its own clone of it. A
  world is distinct per access config (the world's directory plus the
  harness's operators: none in process, one per permission set over
  HTTP); knobs are surface options applied per test, so one template
  serves every knob;
- the template (`pg_world/template.rs`), seeded by whichever test asks
  first, on the helper runtime, while every other test of the process
  waits on the same outcome (a `watch` per access config; a failure is
  every waiting test's failure). Its whole seed is bounded by
  `TEMPLATE_WAIT` (2 h), separate from `WAIT`:
  `TestDb::create_template` (stale templates dropped first), every layer
  migrated, extensions created; `PgStores::open` with
  `PgIds::Seeded(SEED)` on a bus that forwards every event to the
  surface's relay and records it; `InProcess::host`
  (`CursorSecret::Derived`); the world seeded through `WorldStores` on the
  bundle, bodies through a `RecordingBlobs` over the bundle's
  `MemoryBlobStore`; the L7 outbox drained; the relay settled. Then the
  surface, the L7 relay and the dead-letter bus (and its `LISTEN`
  connection) are shut down, the pool closed and `TestDb::freeze` refuses
  new connections and terminates any left, since `CREATE DATABASE …
  TEMPLATE` needs none on the template. The seed checks that the bundle's
  blob store holds exactly the bodies it recorded, and warns when any
  outbox (`reconstruct`, `flow`, `analysis`, `topology`) still holds rows,
  which a clone's stores would publish again at open;
- each test: `TemplateDb::instantiate` (a fresh `crosstalk_test_*`
  database, dropped with the world); a fresh `MemoryBlobStore` given every
  recorded body; `PgStores::open` with `PgIds::Seeded(SEED ^ CLONE_IDS)`
  (the template's generators already minted `SEED`'s ids into the rows the
  clone copies; a clone draws elsewhere, as a restarted deployment does)
  and the L7 relay's ids likewise; `InProcess::host` with the test's knobs
  (node facts rebuilt from the clone; reloading the same access config
  changes nothing); the recorded events replayed into the relay channel in
  their order, so the node facts and the live feed hold what they held
  after the original seed (the feed's epoch is the clock's reading at
  host, before the anchor, as it was); the L7 outbox drained; settled; the
  clock moved to the anchor; the fitter started with the watermark read
  from the clone; the scenario bound from the clone's reads and the
  template's `Scenario`. Dead letters, bindings and cursor keys (derived
  from the secret) come through the clone unchanged;
- databases are created and dropped on a helper multi-thread runtime
  (`TestDb` needs one); the template is never dropped by its process (it
  lives in a static) and is dropped by the next `create_template` on the
  server once its process is gone (same host) or it is a day old (any
  host), or by the manual `crosstalk_test_%` cleanup (deploy.md);
- projection jobs are fitted by a test-side copy of the in-process fitter
  over `PgProjectionSource` with `FakeLayoutFitter`;
- the HTTP harness authenticates against the directory stored in
  Postgres;
- without `TEST_DATABASE_URL` every test skips through
  `Harness::unavailable` (new in `crosstalk-conformance`); a configured
  but unreachable server fails every test with the template's error;
- expected failures: none (`SURFACE_FAILURES`);
- gaps worked around in the harness: `PgChannelRegistry` has no
  `AccessStore` (binding reads accesses through `PgEvidence`), the
  in-process fitter is memory-only and `pub(crate)`, `PgEdgeStore` has no
  `WatermarkRead` (a fixed watermark dates the samples).

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
| `crates/e2e/tests/restart/main.rs` | kill and restart, cursors, ids (W9) | tests |
| `crates/e2e/tests/{spool,spool_outage,spool_outage_twice,spool_kill,spool_outage_serve_runtime}/main.rs` | the spool through the gateway (W9) | tests |
| `crates/gateway/src/main.rs`, `crates/gateway/src/gateway/mod.rs` | `serve` exits non-zero once the pipeline's lock is lost (stopgap) | `Running::fatal`, `Fatal`, `FatalCause`, `Running::shutdown_bound` |
| `crates/api/tests/support/pg_world.rs`, `crates/{api,client}/tests/conformance_pg.rs` | conformance over Postgres (W9): a clone of the template per test | `Databases`, `provision`, `PgWorld`, `Forwarding`, `SeedClock`, `WAIT`, `TEMPLATE_WAIT`, `CLONE_IDS` |
| `crates/api/tests/support/pg_world/template.rs` | the template seeded once per process and access config | `template`, `Template`, `PgSeeding`, `RecordingBlobs` |
| `crates/store/src/test_db/template.rs` | template databases: create, freeze, clone, stale cleanup | `TemplateDb`, `TestDb::{create_template, freeze}`, `drop_stale_templates` |
| `crates/conformance/src/harness/mod.rs` | `Harness::unavailable`: skip a harness whose backend is not configured (W9) | `Harness` |

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

- W9 suite (Postgres unless noted; every wait bounded at five minutes
  with the diagnosis): `crosstalk-e2e` `restart` (kills while feeding,
  mid-settle, after a checkpoint, with outbox rows pending; a graceful
  restart; `cursors_resolve_after_restart`, `minted_ids_never_collide_after_restart`;
  `renamed_answers_agree_across_id_seeds` without a database), `spool`
  (`full_spool_refuses_and_proxy_still_forwards`, no database),
  `spool_outage`, `spool_outage_twice`, `spool_outage_serve_runtime`,
  `spool_kill`; `crosstalk-api --features world --test conformance_pg`,
  `crosstalk-client --test conformance_pg`; `crosstalk-transport`
  `integration::pg_next_dropped_mid_take_hands_out_the_hold_on_the_next_call`;
  `crosstalk-topology` `integration::outbox::a_relay_hung_mid_publish_holds_no_transaction`;
  `crosstalk-gateway` `integration::pg_an_idle_transaction_is_ended_and_its_locks_released`
  and `live::pg::pump::tests::a_next_dropped_after_its_answer_keeps_the_delivery`
  (no database).

## Invariants and constraints

| Invariant | Evidence (agent: ran) |
| --- | --- |
| INV-1217 `topology.frontier.covers-spool` | `crosstalk_gateway::dst::frontier_covers_spooled_envelopes` (ran) |
| INV-1220 `surface.ids.unique-across-restart` | unit `crosstalk_gateway::live::tests::serve_seeds_id_generators_from_entropy` (ran); integration `crosstalk_e2e::restart::minted_ids_never_collide_after_restart` (passed on node0 at d8f5412; flag pending a green node0 run on this branch) |
| INV-1219 `surface.cursor.survives-restart` | integration `crosstalk_e2e::restart::cursors_resolve_after_restart` (passed on node0; flag pending a green node0 run on this branch) |
| INV-1210 `transport.spool.bounded` | integration `crosstalk_e2e::spool::full_spool_refuses_and_proxy_still_forwards` (no database; ran; flag pending with the others) |
| INV-X `transport.stage-pump.no-dropped-delivery` | unit `crosstalk_gateway::live::pg::pump::tests::a_next_dropped_after_its_answer_keeps_the_delivery` (ran) |
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
- Postgres conformance: no session is ever connected to a frozen
  template; each test's world is its own clone, dropped with it; a clone
  starts as the seeded world was after its settle (rows, bodies, applied
  events, clock at the anchor); clone ids are drawn from
  `SEED ^ CLONE_IDS`, never `SEED`; the test assertions are unchanged.

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
- The Postgres conformance template is seeded once per test process: a
  new process (each suite binary, each run) seeds again; templates are not
  reused across runs. The template outlives its process until the next
  `create_template` on the server drops it.
- The Postgres conformance suites have not run green on node0 with the
  template flow yet.
