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
  - the gate and the `spool` section (`spool.rs`), the gate's watch that
    opens it in a process without the lock once every pipeline group
    exists (`gateway/postgres/gate_watch/`, the group list in
    `live/pg/groups.rs`), the `spool_full` capture outcome;
  - `crosstalk migrate` running every layer's migrations,
    `--reset-correlator`, the migration head check, the pipeline lock and
    its fence (`store/`: the epoch and the shared fence every pipeline
    connection joins, so a standby gateway can take over safely);
  - the statement and lock bounds every pipeline connection runs with
    (`store.pipeline_timeouts`, `store/timeouts/`), so a stalled statement
    is cancelled and retried instead of hanging a stage or the reaper;
  - `crosstalk spool --discard-corrupt`;
  - `/readyz`, `/healthz` and `/metrics` in Postgres mode;
  - the start of each role in Postgres mode (`gateway/postgres/`: the
    capture side and the shutdown in `mod.rs`, the pipeline task's
    incarnation loop in `pipeline.rs`, the `api` role in `api.rs`), and
    the in-process resume after a lost pipeline lock;
  - the planned handover on a graceful shutdown (`gateway/postgres/handover/`,
    `live/pg/quiesce.rs`): in-flight work finished or released without
    spending an attempt, the pool closed, then the lock unlocked
    ([planned_handover.md](planned_handover.md)).

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
lazy template pool (no connection yet) ─▶ capture's own pool ─▶ PgBus::publisher
   ─▶ SpoolingBus::open(<data dir>/spool, Gated<PgBus>)
   capture pipeline over the spool (Bodies::SkipStored: FsBlobStore never deletes)
   proxy listener: forwards and captures at once            ops listener   api listener (503 until set)
gate watch (beside the pipeline task, until the gate is open; gateway::postgres::gate_watch):
   at start, then every GATE_POLL (1 s) while this process does not hold the lock:
   every pipeline group's row in transport.groups? ─▶ the gate opens (groups_found)
pipeline task:
   wait for the database (status waiting_for_database)
   ─▶ migrations::check_heads: every layer's applied head == the binary's (behind: not ready, re-checked)
   ─▶ PipelineLock::try_take within TAKE_TIMEOUT (held elsewhere or timed out: not ready, retried;
        its own connection, pinged every PING with a PING_TIMEOUT deadline; watched from here on)
   ─▶ incarnation n: a pool of its own (crosstalk-pipeline-<pid>-<n>) and PgBus::new over it
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
   ─▶ running; the API router is set; until shutdown (a planned handover: no delivery taken,
      in-flight work finished or released, the pool closed, the lock unlocked), or the lock is
      lost: the incarnation is abandoned (nothing drained) and the task waits for the database
      and the lock again
```

**The gate.** A consumer group that does not exist starts at the log's
head: `PgBus::subscribe` inserts its row with `admitted_through` set to
the log's head in the same statement, and a take admits only entries
past it, so anything published before the row exists never reaches the
group (lost to it, and to the detection after it). On a fresh database
the pipeline groups are created only when recovery subscribes them,
while capture runs from the first second. So the bus behind the spool
refuses publishes (`Disconnected`) until every pipeline group exists:
captures and recovery's own publishes wait in the spool, fsynced, and
drain in order under their ids once the gate opens. `/readyz` shows
`capture: spooling` until then. The gate opens, for good, either way it
learns that every group exists (INV-1241
`ingress.capture.gate-opens-once-groups-exist`):

- **Subscribed** (`GateOpened::GroupsSubscribed`): this process's
  incarnation subscribed every group (the last step of `Live::start_pg`).
  On a fresh database this is the only way: nobody else creates them.
- **Found** (`GateOpened::GroupsFound`): the gate's watch
  (`gateway::postgres::gate_watch`, spawned beside the pipeline task by
  `spawn_pipeline`, ended with it) looks at start and then every
  `GATE_POLL` (1 s), while the gate is closed and this process does not
  hold the lock (a holder's own subscribe opens it), for the rows of every
  group in `live::pg::groups::PipelineGroups`: one query on the template
  pool, in a transaction whose `statement_timeout` and `lock_timeout` are
  the pipeline's (`store.pipeline_timeouts`, `SET LOCAL`) and bounded
  client-side by the statement bound. All present: the gate opens and the
  watch ends. Any missing, or the look failing or timing out: the gate
  stays closed (a failure is warned at its first in a row and every 30th)
  and it looks again a poll later.

**Why "every row exists" is the right condition.** A group's row and its
admission base are written in one statement, groups are never deleted,
and an append takes a sequence number above the head every existing row
was based at, so once every row exists each later append is admitted by
every group at its next take, whichever process subscribes it and
whenever. No group's base or admission needs to be checked beyond the
row: `admitted_through` is set by the insert, and a group that has not
taken yet admits from it. The list the watch looks for is the one
`Live::start_pg` subscribes (`PipelineGroups`: the exchange log's group
when the incarnation runs one, then `PG_SLOTS`, each slot filled or the
start refused with `LiveError::SlotEmpty` / `SlotOutsideGroups`), so a
process never opens on a subset of what its own pipeline would subscribe.
The migrations' head is not a condition: the gate guards delivery, not
schema compatibility, and an open gate stays open across later
migrations as before. Gateways sharing a database run the same roles
(the exchange log's group is in the list only for `all` and `pipeline`).

**Before**, the gate opened only at this process's own subscribe, so a
gateway restarted while another held the lock spooled every capture
until it took the lock itself, and the holder's pipeline saw none of
them. The node0 two-gateway fault run: `crosstalk` killed, the standby
took the lock, `crosstalk` restarted as a non-holder; 97 exchanges sat in
its spool (`inner bus unreachable; spooling`, `from=direct`) for five
minutes and reached the bus only when it took the lock as epoch 4, past
the watermark: late L7 dead letters. A gateway whose gate had opened
before it lost the lock kept publishing (the s2/s3 runs).

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
`PgProvenanceStore`'s `SpanIndex` answers with the spans the scans
committed and the spans recorded through `SpanIndex::record` with no
scan behind them (`provenance.recorded_spans`): the conformance world's
seed records each content match's origin span that way, so the evidence
page finds it ([provenance.md](provenance.md)).

**Pumped subscriptions.** `PgSubscription::next` dropped while its take
commits leaves the delivery held until the reaper's provisional deadline.
Stage loops `select!` deliveries against commands, so each stage reads
through a pump task (`pump.rs`) that fetches only on a pull and keeps a
fetched delivery for the next pull. The one remaining cancellation is an
ack that arrives while the pump fetches (the flow consumer's checkpoint
acks).

### The bus clock

`PgBus` times its delays (a nacked or timed-out delivery's
`available_at`) with the wall clock
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

### Losing the pipeline lock: abandon and resume in process

A lost lock never stops the process (INV-1237
`ingress.capture.never-stopped-by-lock-loss`). The proxy keeps listening,
capture keeps appending to the spool, and the pipeline task walks back
through waiting for the database and the lock, recovery and running,
in the same process. The supervisor's restart policy now only covers a
crash (see [deploy](deploy.md)).

**Detection.** An outage that drops the pipeline lock's own connection (a
database restart, a reset connection, a cut link, `pg_terminate_backend`)
fails the lock's next ping (within `store::lock::PING`, 5 s). A silent
partition (nothing answered, nothing reset) times the next ping out
instead: each ping must answer within `store::lock::PING_TIMEOUT` (4 s),
so either way the holder notices within `store::lock::DETECTION` =
`PING + PING_TIMEOUT` (9 s) and `PipelineLock::lost` resolves with a typed
`LockLoss` (`PingFailed { failure }`, `PingTimedOut { after }` or
`HolderStopped`). The holder reports first, then drops the connection
unclosed. The loss is watched from the moment the lock is taken, through
`Live::start_pg` and the API router's setup, not only once the pipeline
runs.

**The incarnation lifecycle** (`crates/gateway/src/gateway/postgres/pipeline.rs`).
The pipeline task is one loop over an explicit state:

```text
Waiting ─ database answers, migrations at head, PipelineLock::try_take_reporting ─▶ Starting{lock}
   (the take: the lock, the epoch bumped to e, then fencing until no connection of an earlier
    incarnation is left; a stop ends the wait; the session failing meanwhile: a loss, counted)
Starting{lock}: incarnation n = a pool of its own (application_name crosstalk-pipeline-<pid>-<n>-e<e>,
                the template pool's options, each connection joining the fence under e)
                + PgBus::new over it ─▶ Live::start_pg ─▶ Late set
   the lock lost (or a stop) during the start: the start future is dropped, its pool fenced,
   its bus abandoned ─▶ Waiting
Running(Incarnation): until the stop (handed back for the graceful drain) or the loss
Abandoning{incarnation, loss}:
   status at once: pipeline_lock lost, pipeline abandoning, lock_losses + 1, last_loss
   Late cleared: the API answers 503 "starting", /healthz has no live section
   the pool fenced: Pool::close called, not awaited (every acquire fails from now on; a
     connection an aborted task returns is closed, never pinged); the close itself runs
     bounded (CLOSE_BOUND, 1 s) in a task of its own
   Live::abandon(LOST_SHUTDOWN = 1 s): every stage, the ticker, the exchange log, L7's
     outbox relay and the retention prune aborted and joined; PgBus::abandon: the bus
     closed, its listener and reaper aborted and joined, every take in flight aborted and
     joined; the surface's relay and feed stopped. No Drain command: no checkpoint, ack,
     nack or outbox relay after the loss
   the lock dropped ─▶ pipeline waiting_for_database ─▶ Waiting
```

What the next incarnation's recovery finds is what a killed process
leaves: holds stay `held` (the reaper was aborted, so a dropped
subscription nacks nothing) and `recover_held` readies them at once (no
wait for the stale-hold sweep, about 65 s), the store outboxes are
relayed, the flow consumer restores its last checkpoint and re-feeds what
came after. The spool's gate stays open: the groups exist.

**Bus and pool ownership.** Capture appends through a bus and a pool of
its own, which no incarnation shares: `PgBus::publisher` (no `LISTEN`
task, no reaper; a subscribe on it is refused) over a pool of
`CAPTURE_CONNECTIONS` (2) named `crosstalk-capture-<pid>`. Each
incarnation builds `PgBus::new` over its own pool; its subscriptions, its
listener and its takes (spawned in tasks of their own that "commit
whether or not the caller waits") belong to it, and `PgBus::abandon`
aborts and joins all of them. `PgBus::shutdown` and `abandon` are
terminal, which is why a bus is never reused across incarnations. The
process's template pool (`crosstalk-<role>-<pid>`) only probes the
database and checks the migrations' heads; each incarnation's pool takes
its options (the acquire timeout, `max_connections`) and runs
`store::session_setup` then joins the pipeline fence on every new
connection (`incarnation_pool`; capture's `derived_pool` only runs the
session setup).

**Late handles.** `gateway::late::LateValue<T>` (a `watch` channel)
holds the live process's reporter, stage flags and exchange-log tasks for
the ops listener, and `LateRouter` the API router; each incarnation sets
them, and a loss clears them. While none is set the `live` and
`exchange_log` task probes read running (nothing to stop) and the API
answers `503` with "the surface is starting".

**The bound must stay below the server's reap (INV-1236).** The server
releases a partitioned session's advisory lock when it reaps the session,
`store::lock::SERVER_REAP`: about 25 s with deploy's
`tcp_keepalives_idle=10` plus `tcp_keepalives_interval=5` ×
`tcp_keepalives_count=3`, and `tcp_user_timeout=25000`. From then on
another process may take the lock; a holder that had not noticed would
run a second pipeline on the same database (on the node0 bench run
20261012T180130Z, before the deadline, the gateway read `pipeline_lock:
held` and `pipeline: running` for about 33 s after the server had reaped
its session, 12 s past the link's return). Compile-time assertions keep
`DETECTION < SERVER_REAP` (in `store::lock`) and
`gateway::postgres::LOST_REPORTED` (`DETECTION` plus the 2 s probe, 11 s)
plus `LOST_SHUTDOWN` (1 s) below it (in `gateway::postgres`). Change
`PING`, `PING_TIMEOUT` or the server's keepalives only together. Taking
the lock is bounded too (`store::lock::TAKE_TIMEOUT`, 10 s,
`TakeError::TimedOut`): a connect or `pg_try_advisory_lock` across a
partition is retried instead of hanging the start. Evidence:
`integration::lock::pg_a_silently_partitioned_lock_session_is_lost_within_the_bound`
(the lock's connection through a `DbLink` partitioned silently: failed
before the fix), `pg_a_running_pipeline_reports_its_partitioned_lock_lost_within_the_bound`
and `pg_taking_the_lock_across_a_silent_partition_times_out`.

**No owner write after the abandon (INV-1238).** Once `Live::abandon`
returns, nothing of the incarnation can commit to what only the lock's
holder writes (the bus's deliveries and groups, the flow checkpoint and
held writes, the four outboxes): its pool was fenced before anything was
aborted, and every task and take was joined. The loss is logged as a
structured error at once (`cause = pipeline_lock_lost`, `reason =
ping_failed|ping_timed_out|holder_stopped`, `session`, `incarnation`),
the abandon as a warning (`incarnation`, `joined`, `tasks_joined`,
`bus_joined`, `took_ms`). Evidence: `crosstalk-e2e`'s `lock_loss` target
(below) and `crosstalk_transport::integration::abandon::pg_an_abandoned_bus_joins_its_takes_and_leaves_its_holds_for_recovery`.

### The fence: no write from an older epoch

Taking the lock bumps the pipeline epoch (`transport.pipeline_epoch`) and
returns only once no connection of an earlier pipeline incarnation, in any
process, is left; every incarnation connection joins a shared advisory
fence and refuses itself unless the epoch is its own. So a standby gateway
that takes the lock while the old owner has not yet noticed its loss (its
lock session terminated with the database reachable) never runs beside it
(INV-1239). While the take waits, `/readyz` reads `held` / `fencing`, 200
degraded; `/healthz` adds `recovery.epoch`, `/metrics`
`crosstalk_pipeline_epoch`. The wait never gives up: a stopped old owner
with open connections blocks the takeover until they die. Data and
control flow, the ordering argument, failure modes and files:
[pipeline_fence.md](pipeline_fence.md).

**Bounded shutdown.** A graceful shutdown never waits on a dead link: the
pipeline task's stop is bounded by the flush deadline (past it the task is
aborted), each pool's close by `CLOSE_BOUND` (1 s; past it the pool is
dropped and its connections close as they come back), the store probe's
close too. sqlx 0.9's `Pool::close` waits for every checked-out
connection, and one returned after a cancelled query pings without a
deadline; `PgListener`'s drop sends `UNLISTEN *` without one. Across a
silent partition both wait for TCP (53 s on the node0 bench, with the
proxy refusing throughout). `integration::pool_close::pg_a_partitioned_pool_close_waits_for_an_aborted_query`
shows the cause, and that a pool closed before the abort does not wait;
`lock_loss::partition::a_shutdown_across_a_partition_returns_within_its_bound`
the gateway's bound. `/readyz`'s `capture` reads `stopped` (503) once the
process drains or the proxy's or the capture stage's task ended, rather
than reporting the spool's state for a capture that no longer runs.

**After a network partition, the resume may wait up to about 25 s for
the old session's lock.** A partition (rather than a Postgres restart)
can leave the abandoned incarnation's lock session alive on the server,
holding the advisory lock until the server notices the dead peer. With
the OS's TCP keepalive defaults that is hours; with the server's
`tcp_keepalives_idle=10`, `tcp_keepalives_interval=5`,
`tcp_keepalives_count=3` (crosstalk-infra's deploy/postgres setting) it
is about 25 s. Meanwhile the gateway reports `pipeline_lock: held
elsewhere` (200 degraded: it held the lock before), keeps forwarding and
spooling, and takes the lock once the server reaps the session; nothing
terminates another backend. The gateway sets no client-side keepalives
(sqlx 0.9 has no option for them): its own side sees a cut or a silent
partition within the lock's `DETECTION` (9 s). `Gateway::settled` in the
e2e harness allows the lock `LOCK_RELEASE` (60 s) to come free and
otherwise fails naming every session on the database, which one holds
the lock, and the server's connection count and `tcp_keepalives_*`
(`common::gateway::sessions`, read over the test's own pool, not the
gateway's).

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

### Bounded statements: the pipeline's statement and lock timeouts

Across a silent partition every pipeline wait is already bounded: the
lock's ping times out within `DETECTION` (9 s) and the incarnation is
abandoned, its tasks aborted. The case this closes is a server that still
answers the lock's ping but stalls a pipeline statement: a row or table
lock held by another session, a stuck vacuum, disk pressure. Before, a
stage, the reaper or a checkpoint then waited without end while the
gateway reported `running` (INV-1240 `transport.pipeline.statements-bounded`).

**What is bounded.** Every connection of a pipeline incarnation's pool
(`gateway::postgres::incarnation_pool_bounded`) sets, as it connects and
for its whole session, `statement_timeout` and `lock_timeout` from the
store section's `pipeline_timeouts` (`store::timeouts::PipelineTimeouts`):

```text
after_connect: session_setup (idle_in_transaction_session_timeout = 30s)
   ─▶ SELECT set_config('statement_timeout', '<statement_ms>ms', false),
             set_config('lock_timeout', '<lock_ms>ms', false),
             set_config('crosstalk.statement_timeout', '<statement_ms>ms', false),
             set_config('crosstalk.retention_timeout', '<retention_ms>ms', false)
   ─▶ fence::join (shared fence, epoch check)
```

`{"statement_ms": 20000, "lock_ms": 15000, "retention_ms": 900000}` by
default, each key optional; each at least 1 ms and at most 2147483647 ms
(the server's range), the lock's at most the statement's (a longer one
never fires), the retention's at least the statement's; a refused value
is a config parse error.

**Retention deletes get the longer bound.** The retention deletes are
single unbatched `DELETE`s whose size grows with how long retention did
not run; under the 20 s statement bound one could be cancelled every
round after a long stop, which would silently stop retention. Each runs
in a transaction that first raises `statement_timeout` with `SET LOCAL`
to `retention_ms` (`crosstalk_store::raise_for_retention`, reading the
session's `crosstalk.retention_timeout`) and, where the transaction goes
on to a pipeline write, puts it back (`restore_after_retention`, from
`crosstalk.statement_timeout`); `lock_timeout` is left as it is. On a
session without the settings both are no-ops. Covered: the bus's hourly
log `prune` (`transport/src/pg/prune.rs`), L4's `PgProvenanceStore::prune`
(per tick), the fingerprint index's observation age-out
(`provenance/src/index/pg.rs` `age_out`, raised then restored inside
`insert`, `observe` and `evict`), the extraction ledger's `expire` (per
tick), `PgProjectionStore::expire` (lapsed projection frames),
`PgConversations::forget_seen` and `flow::store::prune_cursors` (no
pipeline caller today). 15 min covers on the order of 10^8 deleted rows.
The real follow-up is batching those deletes. The bus's own tighter bounds keep
theirs: a take's `SET LOCAL lock_timeout` (`take_limit`, 10 s) and the
reaper's `fail` (2 s). Not bounded: capture's publisher pool (`derived_pool`:
a stalled append fails at the publish's own client-side `publish_timeout`,
5 s, as `Disconnected`, and spools), the template pool (probes and the
head check, each bounded client-side), the `api` role's pool, the lock's
session (its ping deadline bounds it) and `crosstalk migrate`. In role
`all` the API's reads go through the running incarnation's stores, so
they are bounded too: a read past 20 s fails as a store error.

**Why 20 s and 15 s.** Every pipeline statement runs in milliseconds, and
the longest legitimate ones (below) stay well inside 20 s, while a stage
or the reaper is never parked longer than that. The lock bound is above
the bus's 10 s and 2 s ones. Both are below the default 30 s ack timeout,
and the statement bound must be (`PipelineTimeouts::check_ack_timeout`,
refused at start as `StartError::PipelineTimeouts`): a stage whose
statement stalls to the bound releases its delivery before the reaper
takes the hold back, and only the reaper's take-back leads to an attempt
spent (see "A stall spends no attempt").

**What a cancel becomes.** The server cancels the statement (SQLSTATE
`57014` `query_canceled`, or `55P03` `lock_not_available`) and aborts its
transaction. `crosstalk_store::classify` types it
`DbFailure::TimedOut(TimeoutKind::{Statement, Lock})`, displayed
`statement timeout (SQLSTATE 57014)` / `lock timeout (SQLSTATE 55P03)`;
`is_transient()` (with a lost connection, a pool timeout and a conflict),
not `is_retryable()` (`retry_serializable` does not re-run the
transaction in place, which would multiply the stall: the caller's retry
does). `classify` counts it by kind (`crosstalk_store::statement_timeouts`,
`/healthz` `statement_timeouts`, `/metrics`
`crosstalk_pg_statement_timeouts_total{kind="statement"|"lock"}`) and logs
it once at warn (`kind`, `sqlstate`, the server's `message`); the caller's
own warn names the stage or statement (`stage's store stalled; released
without spending an attempt` with `stage`, `event`, `attempt`, `kind`,
`sqlstate`, `reason`; `could not take a hold back` with `group`, `seq`,
`timed_out`; `sweeping stale holds failed`; `checkpoint not taken`;
`flow step failed`). The bus reports a timeout as `BusError::Disconnected`,
as its own publish timeout does.

| Wait (call site) | A cancel classifies as | The caller then | Verdict |
| --- | --- | --- | --- |
| reaper `fail` (`transport/src/pg/reaper.rs`, ack timeout or dropped holder) | `TimedOut(Lock)` (its `SET LOCAL` 2 s) or `TimedOut(Statement)` | warn, the hold re-queued at `now + poll` | retried |
| reaper `sweep_stale` | `TimedOut` | warn, next sweep one ack timeout later; the loop goes on reaping deadlines | retried |
| a take (`subscription.rs` `try_take`) | `TimedOut` | warn `next failed; retrying`, at the poll interval | retried |
| stage ack (`PgSubscription::ack`) | `Disconnected` | hold kept, `delivery not settled` warn; the reaper redelivers at the ack deadline (stages are idempotent) | redelivered |
| stage nack (`fail`) | `Disconnected` | warn; the reaper takes the hold back at its deadline | redelivered |
| L3 writes (`PgExchanges::put`, reconstruct's stores) | a store error | `StageError::Retry`, a stall: release | redelivered, no attempt spent |
| **L4 provenance writes** (`PgProvenanceStore`: `record_exchange`, the scan's records) | was `Server { 57014 }` ─▶ `ProvenanceStoreError::Rejected` ─▶ `EngineError` not transient ─▶ `StageError::Reject` ─▶ **acked: the exchange's or delta's provenance lost**. Now `Unavailable` (transient) | `StageError::Retry`, a stall: release | **fixed**: redelivered, no attempt spent |
| L4 fingerprint index, extraction step and ledger | `IndexError::Store` (classified now, so counted), `Retry` | release | redelivered, no attempt spent |
| L4 waiting on L5 (`DurableInputs::deliver`) when L5's store stalled | `NotDurable::Stalled(kind)` | `StageError::Stalled`: release | redelivered, no attempt spent |
| L4 tick (`expire`, ledger expiry), L7 tick (frontier, watermark) | store errors | warn, next tick | retried |
| L5 flow steps (registry, transmissions, agents) | `Store { reason }`: not permanent | step kept in the backlog, retried with the next input, interval or drain | retried |
| L5 checkpoint | a durability error | warn `checkpoint not taken`; acks wait for the next checkpoint; a stalled one stops L5 taking deliveries until a checkpoint is taken again | retried, no attempt spent |
| L6 classify (`TransmissionStore::modify`, catalog) | a store error | `StageError::Retry`, a stall: release | redelivered, no attempt spent |
| L7 edges (`apply`, `judge`, `apply_access`, versions) | an edge error | `Outcome::Nack`, a stall: release | redelivered, no attempt spent |
| outbox relays while running (L7 relay task; L3, L5, L6 relays after a write) | a relay error | warn, next wake-up or poll; stamped rows republished under their ids (INV-1211 to INV-1213) | retried |
| recovery (`recover_held`, `PgStores::open`, the relays, `restore`, `InProcess::host`, subscribing) | `LiveError` (never `is_permanent` for a database error) | `StartFailed::Retry`: the incarnation is abandoned and started again after `RETRY` | retried |
| surface relay's node facts (`NodeFeeder::apply_all`) | a read error | warn `node facts not refreshed`; not retried until the next start rebuilds them | counted, continues (as for any database error before) |
| `/healthz` `group_stats` (capture's bus: no server-side bound) | - | bounded client-side: `HEALTH_BUS_BOUND` (2 s), then `bus: {"unavailable": {"timed_out": {"bound_ms": 2000}}}` (warn); a failed read (database out of reach) leaves `bus` out | answers |
| LISTEN `recv` (`listen.rs`, an incarnation connection) | no statement runs while it waits, so neither bound applies | not bounded and need not be: notifications are hints (each waiting `next` re-reads at the poll, 250 ms), the task blocks nothing, a partition is caught by the lock's `DETECTION` and the abandon aborts and joins it | covered |

**Long statements on the pipeline pool**, against the 20 s default on a
large deployment: `recover_held` updates at most `group_capacity` (1024)
held rows per group; the outbox relays go batch by batch; the flow
restore reads one checkpoint row per shard plus the inputs since it (a
few seconds' worth), and a checkpoint's snapshot of tens of MB writes in
well under a second; `InProcess::host` reads the agents and channels once.
None comes near 20 s. The retention deletes are the exception: in
steady state each deletes an hour's or a tick's worth of rows, but after
the gateway was stopped for days at a high capture rate the first one
deletes everything that aged out meanwhile. They run under `retention_ms`
(above): Postgres deletes on the order of 10^5 to 10^6 indexed rows a
second, so 15 min covers far more than 20 s's millions.

**A stall spends no attempt.** A statement or lock timeout is an
infrastructure stall, not a property of the message, so it never
consumes a delivery attempt and never dead-letters (before this branch a
stall just waited and recovered when it ended; it must not now leave a
gap that needs a replay). The generic stage loop runs each `handle` in
`crosstalk_store::observe_timeouts` (a task-local the classification
marks), so whatever error type a layer reports the failure through, a
`StageError::Retry` asked for while one of the handle's statements was
cancelled at its bound becomes `StageError::Stalled { kind, reason }`
(`live::stage::stalled`); L4 also maps `NotDurable::Stalled`, which the
flow consumer answers when its own store stalled
(`FlowConsumer::handle_batch` under the same observation). A stall is
settled with `Subscription::release` (spec, l2: by default a nack; a bus
without such stalls may treat it so) rather than a nack. `PgBus`'s
release (`Failure::Stalled` in the reaper's `fail`) delays the row by
`STALL_RETRY` (1 s, clamped to the group's backoff bounds) and gives its
attempt back (`attempt - 1`; the next take counts it again), and never
dead-letters. A hold is now keyed by `held_at` as well as its attempt, so
a second hold on the same attempt is never taken back by the first
hold's deadlines. Spinning is bounded: each stalled round waits out a
statement or lock bound, then `STALL_RETRY`. A `Reject` is the message's
and is never turned into a stall; a failure of the message (`Retry` with
no timeout seen, a nack) still spends attempts and dead-letters on the
last. Two paths could still spend attempts during a stall and are
closed: a statement stalled past the ack timeout (the reaper's take-back
precedes the release, and the next take counts), hence the statement
bound below the ack timeout; and L5, whose deliveries wait for a
checkpoint, which stops taking new deliveries while its checkpoint is
stalled. The edge left: a handle that waits out most of the lock bound,
then stalls a later statement to the statement bound, can outlast the
ack timeout once.

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
| pipeline lock holder `crates/gateway/src/store/lock.rs` (`hold`) | yes | its own session (advisory lock), no transaction | each ping bounded by `PING_TIMEOUT`; a failed or timed-out ping reports `LockLoss` within `DETECTION` (9 s), then drops the connection. Dropped: the connection closes and the server releases the lock; a partition: released when the server reaps the session (`SERVER_REAP`, tcp keepalives, about 25 s), after the holder has abandoned its incarnation (INV-1236) |
| `Live::abandon` `crates/gateway/src/live/mod.rs`, `PgBus::abandon` `crates/transport/src/pg/mod.rs` (a lost lock) | not by design: everything is aborted | aborted tasks and takes may hold a transaction | the pool is fenced first, so each connection an aborted task returns is closed (never pinged) and its transaction rolled back by the server; every task and take is joined, so none commits after the abandon returns (INV-1238). Holds stay held for the next incarnation's `recover_held` |
| `Live::shutdown` `crates/gateway/src/live/mod.rs:547`, `:562`, `:620`; `Live::kill` | not by design | aborted tasks may hold a transaction | rolled back on the connection's return (the runtime is alive) or when the socket closes; the backstop ends any left idle |
| settle and quiet `crates/gateway/src/live/settle.rs`, recovery `Live::start_pg` | yes | per statement only | nothing |

**Backstop.** Every pool from `store::lazy_pool_named`, and every pool
`gateway::postgres::derived_pool` builds from one (the gateway names its
template pool's sessions `crosstalk-<role>-<pid>`, capture's
`crosstalk-capture-<pid>`, pipeline incarnation n's
`crosstalk-pipeline-<pid>-<n>`, the pipeline lock
`crosstalk-pipeline-lock-<pid>`, the e2e harness
`crosstalk-e2e-live-<n>`), sets `idle_in_transaction_session_timeout =
'30s'` in `after_connect` (an incarnation's pool also sets its statement
and lock bounds, "Bounded statements"): whatever leaves a transaction
idle, the server ends that session within 30 s and its locks are released
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
- When the bus's tasks are stopped (`Live::shutdown`/`kill`, a lost
  lock's `Live::abandon`), nothing reaps: a held row stays held until the next
  process's `recover_held`, which makes it `ready` at once, with no
  backoff and the attempt counted (the dead holder's take counted it;
  recovery adds none), or dead-letters it on its last attempt. Takes go
  by log `seq`, so a single consumer of the group takes, after a restart:
  every `ready` row in `seq` order, the recovered ones among them (each
  ahead of every entry published after it, a drained spool record
  included), then entries admitted from the log as it pulls, in `seq`
  order; a row a live process delayed before the stop (a nack, an ack
  timeout) joins in `seq` order once its `available_at` passes
  (`transport.restart.recovered-ready-first`).
  Until this fix the recovered row was delayed by the group's backoff,
  so the drained record was taken first: L2 promises no order
  (`transport.ordering.unconstrained`), and L3 threads by arrival (a
  later turn threaded first starts the conversation; the earlier one then
  starts another), so an outage that interrupted a delivery changed the
  answers. That exposure is closed at L2 for the restart case. Other
  redeliveries in a live process (a nack, an ack timeout, a dropped
  subscription, the stale-hold sweep) keep their backoff and can still
  reorder. L3's conversation stores hold the turn-key-order result
  whatever the arrival order, within the reorder window
  (`reconstruct.thread.order-independent`,
  [reconstruct](reconstruct.md#delivery-order)): an earlier turn arriving
  second rewinds the later one and both land in one conversation. What
  arrival order still decides: the later turn's delta was published when
  it arrived (L4 and L5 keep it), and a new agent's id and first-seen
  time come from the first of its exchanges L3 handled.

So a single lost race delays an event by at most about 95 s and loses
nothing; only five losses of one delivery dead-letter it. With every
consumer cancel-safe (the table above), no race is known to lose one.

### Settling and shutdown

`Live::settle` is shared. A set with deferred acks (`PgSet`) drains the
stages (the flow consumer checkpoints and acks on a drain) before it
reads the groups; quiet (`PgQuiet::busy`) means no row in the four store
outboxes, nothing in the spool, no log entry a slot's group takes but has
not admitted yet (past its `admitted_through`), and no pending delivery
in any slot's group (`PgBus::group_stats`), read in that order: an
envelope goes outbox row ─▶ spool record ─▶ log entry ─▶ delivery, each
step committed before the earlier copy goes, so one that moves between
two reads is seen by the later one. `Live::shutdown` drains the stages
the same way, stops the bus, joins the stages (what harnesses call). The
gateway's shutdown no longer drains on Postgres: a running incarnation is
handed over ([planned_handover.md](planned_handover.md), INV-1242). No
delivery is taken any more, each stage finishes the one it handles, L5
answers L4's last inputs and then checkpoints and acks what it holds (the
pump still serves acks after its subscription ended), what is still held
goes back ready on the same attempt (`PgBus::release_held`), the
incarnation's pool closes, and only then is the lock unlocked on its own
session (`PipelineLock::release`, which takes the handover's
`PoolsClosed` proof). The whole handover ends within
`shutdown.flush_timeout_ms`; the gateway then closes the spool's drainer,
capture's bus and pool and the template pool, each close bounded by
`CLOSE_BOUND`. Before, the shutdown drained the groups (a moving target
while another gateway captures), dropped the lock before closing the pool,
left held deliveries for the next holder's recovery with their attempt
counted, and L5's last acks failed (`Disconnected`).

The unadmitted entries were missing until the node0 loop on b170b79
(`spool_outage_twice` 3 of 10, `restart`'s `killed_while_feeding` 1 of
10). A group admits log entries into deliveries only when its consumer
pulls, so between L6 committing `TransmissionClassified` (and acking its
own delivery) and L7 and L5 taking it, every slot's group read as empty
and two quiet rounds could fall in that window: the settle returned, the
answers were read without the L7 edge (and once with the transmission
still `Confirmed`), and the edge was stored when the process stopped,
after the read (the database matched a passing run's row for row). After
a restart, all four captures of `killed_while_feeding` sat in the log
unadmitted when the last process's first settle began, which read every
answer empty. `integration::pg_quiet_counts_entries_no_group_admitted_yet`
pins it. The gateway harness's `Gateway::settled` (`crates/e2e/tests/common/gateway.rs`)
had the same gap and counts `UNADMITTED_ENTRIES` too: returning early
there let `spool_outage_twice`'s second cut land while L3 was handling the
third exchange; the next process's `recover_held` delayed that delivery
by its backoff and the drained fourth exchange, taken first, started the
conversation the third should have (an extra conversation, agent B's id
minted from the fourth exchange). `recover_held` now makes that delivery
ready at once, so the third exchange is taken first
(`integration::pg_recovered_hold_is_ready_and_taken_first`); and L3 threads
the third into the conversation it opens, before the fourth, whatever
order they arrive in (`reconstruct.thread.order-independent`;
`crates/e2e/tests/reorder`), though B's id would still come from the
fourth. `Live::abandon` stops as a killed process would (every task
aborted and joined, nothing acked or checkpointed, the bus's tasks and
takes stopped and joined without touching its tables): what a lost lock
does to its incarnation; `Live::kill`, for restart tests, is it with a
generous deadline.

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
| `pipeline_lock` | `not_taken`, `held` (fencing and handing over too), `held elsewhere`, `lost`, `released` (a planned handover let it go) (absent for `api`) |
| `capture` | `durable`, `spooling`, `draining (<n> records)`, `dropping: spool full`, `spool corrupt: <segment>`, `stopped` (the process drains, or the proxy's or capture's task ended) (roles that capture) |
| `pipeline` | `waiting_for_database`, `waiting_for_migrations`, `waiting_for_lock`, `fencing` (the lock taken, an earlier incarnation's connections closing), `recovering`, `running`, `abandoning` (the lock was lost), `handing_over`, `handed_over` (a planned shutdown), `stopped: <why>` |
| `recovery` | `pending`, a step, `done`, `failed: <why>` |
| `handover` | absent until a planned shutdown hands the pipeline over; then `quiescing`, `releasing_holds`, `closing_pool`, `releasing_lock`, `done` |

Ready (200) unless: the spool is full or corrupt, capture stopped,
migrations are behind, the lock is held elsewhere by a process this one
never took it from (a standby at its start), the pipeline stopped or is
handed over, a task stopped, or the process drains. The `api` role also needs the database
reachable and migrations at head. A database outage, recovery or a drain
leave it ready, degraded, and so does the whole way back after a lost
lock: `held` / `running` ─▶ `lost` / `abandoning` ─▶ `lost` /
`waiting_for_database` ─▶ `held elsewhere` / `waiting_for_lock` (while
another session holds it) ─▶ `held` / `fencing` ─▶ `held` / `recovering`
─▶ `held` / `running`, every step 200 degraded, as at a start with the
database down.

The two `held elsewhere` answers differ on purpose. A resuming owner
keeps capturing into the spool for the pipeline it will resume, so taking
it out of rotation would only refuse traffic it is serving. A standby
that never held the lock keeps the existing standby contract: its 503
steers traffic to the process that owns the pipeline.

"Full" means the last capture was refused full and the spool has not
drained back to `direct` since.

`/healthz` adds `bus.groups.<name>.{pending, oldest_pending_micros,
dead_letters}` (read from the database within `HEALTH_BUS_BOUND`, 2 s;
otherwise `bus.unavailable.timed_out.bound_ms`; left out while the
database refuses the read), `statement_timeouts.{statement, lock}`
(statements the server cancelled at a pipeline bound, see "Bounded
statements"), `recovery.{outbox_relayed, flow_checkpoint_micros,
accesses_refed, deliveries_redelivered, incarnation, epoch, lock_losses,
last_loss.{reason, at_micros}, handover}` (`handover`: the planned
handover's step, absent before one; the last recovery's counts; the
incarnations started, the pipeline epoch the process last took the lock
under (0 before its first take), the locks lost and the last loss over
the process's life) and `spool.{state, records,
bytes, oldest_at_micros, oldest_age_seconds, max_bytes, appended,
drained, rejected_full, rejected_io, truncated_bytes,
capture_spool_full}`; `live.watermark_micros` is the persisted watermark
from the first report. `/metrics` adds `crosstalk_spool_*` (`state{state}`, `bytes`,
`records`, `oldest_age_seconds`, `appended_total`, `drained_total`,
`rejected_total{reason="spool_full"|"spool_io"}`, `truncated_bytes_total`:
the names and label values crosstalk-infra's dashboard and alerts use) and the
`spool_full` reason of `crosstalk_capture_uncaptured_total` and outcome of
`crosstalk_pipeline_exchanges_total`, and the lock's series:
`crosstalk_pipeline_lock_losses_total`,
`crosstalk_pipeline_lock_last_loss_timestamp_seconds`,
`crosstalk_pipeline_lock_last_loss_reason{reason}`,
`crosstalk_pipeline_incarnation` and `crosstalk_pipeline_epoch`, and
`crosstalk_pg_statement_timeouts_total{kind="statement"|"lock"}`.

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

A start that fails short of `LiveError::is_permanent` (no free
connection: the server's taken by other sessions, a killed process's not
gone yet, or the bus's recovery finding the pool empty, which it reports
as `BusError::Disconnected`) is retried every second until the bound, by
the same classification `serve`'s pipeline task retries on, logging the server's
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
`spool_outage_serve_runtime` runs the cut-once outage (lock lost, the
incarnation abandoned, the resume in process, the drain) with every gateway task on a runtime from
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
  (`pipeline_lock: lost`, the incarnation abandoned), the same process
  spooling on with the link still cut (200 degraded,
  `waiting_for_database`); restored, it takes the lock again, recovers and
  the spool drains (`drained == appended`), `/readyz` `ok`; the last
  exchange; every capture in the log once; equal answers.
  `database_cut_twice_drains_to_the_uninterrupted_answers`: the same,
  twice, in one process.
- `gateway_killed_while_spooling_drains_once_after_restart`: the first
  gateway runs on a runtime of its own; cut, one capture spooled, the
  runtime shut down (every task dropped, as a crash); restarted with the
  link still cut, the record is recovered from disk and the next capture
  spools behind it (`waiting_for_database`); restored, drained; every
  capture is in `transport.events` exactly once; equal answers.

### Losing the lock while the gateway runs (`crates/e2e/tests/lock_loss`)

One target, its tests taking turns (`harness::serial`), each on its own
database; the uninterrupted run's answers are computed once. A connect
prober dials the proxy every 25 ms and counts refusals; a sampler reads
`/readyz` every 100 ms.

- `partition::a_silent_partition_never_stops_capture_and_resumes_in_process`,
  `a_postgres_stop_never_stops_capture_and_resumes_in_process` (`cut()`:
  every session reset, connects refused) and
  `a_partition_past_the_server_reap_never_stops_capture_and_resumes_in_process`
  (INV-1237): the fault held past `LOST_REPORTED` plus 5 s plus 5 s (and
  past `SERVER_REAP` plus 10 s), exchanges forwarded during it, then
  healed. Zero refused or failed connects, every exchange answered
  unchanged, `/readyz` 200 throughout and never `stopped`, `lost` seen;
  the pipeline runs again, the drain after the heal within 45 s (the
  abandoned holds ready at once, not after the stale-hold sweep),
  `drained == appended`, every capture in the log once and the
  uninterrupted run's answers.
- `partition::a_shutdown_across_a_partition_returns_within_its_bound`: a
  graceful shutdown with the database silently partitioned returns within
  `Running::shutdown_bound`.
- `rival::a_terminated_lock_session_and_a_rival_holder_leave_no_owner_write_after_the_abandon`
  (INV-1238): a commit-time audit (a deferred constraint trigger per table
  records the table, `application_name` and `clock_timestamp()` as the
  write commits) on the bus's deliveries and groups, the flow checkpoint
  and held writes and the four outboxes; `pg_terminate_backend` on the
  lock's session with the database reachable, a rival session taking the
  lock for 10 s while exchanges arrive. No audited commit by the
  abandoned incarnation's `application_name` after its abandon, its
  sessions gone; the loop waits `held elsewhere` (200 degraded), then
  takes the lock, recovers and drains; the writes in the window before
  the loss was reported are printed.
- `repeated::repeated_lock_losses_leave_no_pool_listener_or_task_behind`
  (INV-1238): five losses and resumes in one process; after each, the
  previous incarnations' server sessions are gone, the current one's
  within its pool size, and the runtime's alive tasks back at the
  baseline.
- `standby::a_standby_takes_over_a_terminated_lock_session_with_no_older_epoch_write`
  (INV-1239): two gateways on one database (the standby's own data
  directory, its blob store a link to the owner's), the same audit plus
  one on `transport.pipeline_epoch`. The owner's lock session is
  terminated with the database reachable while an exchange arrives; the
  standby takes the lock (when the owner wins the retake race instead, its
  session is terminated again, up to 6 rounds). No audited write of an
  older epoch (by `application_name`'s `-e<epoch>`) commits after the
  standby's incarnation's first write; no older-epoch session is left; the
  old owner stands by (`held elsewhere`, 200 degraded) with zero refused
  connects, captures the rest, and the standby drains it all to the
  uninterrupted run's answers. The bump time, the fence wait and the older
  writes inside it are printed.
- `standby::a_standby_takes_over_promptly_from_a_killed_owner` (INV-1239):
  the owner on a runtime of its own, killed; the standby is past its fence
  within its retry and take (10 s plus slack) under the next epoch, runs,
  captures the rest and ends as the uninterrupted run.
- `gate::a_gateway_started_beside_a_lock_holder_publishes_its_captures_directly`
  (INV-1241, the node0 regression): the owner runs (every group exists); a
  second gateway (its own data directory, the owner's blob store linked)
  starts as a non-holder, its gate opens within `GATE_POLL` plus slack of
  its start, it captures the rest with nothing through its spool
  (`appended == 0`, `direct`), never runs an incarnation, and the owner's
  exchange log holds every capture; the run ends as the uninterrupted one.
- `gate::a_gateway_on_a_fresh_database_spools_until_the_groups_exist`
  (INV-1241): a rival session holds the lock on a fresh database; the
  gateway's gate stays closed through three polls, its captures wait in
  the spool, no group row and no log entry exist; once the rival lets go
  the gateway takes the lock, its subscribe opens the gate, the spool
  drains, and the run ends as the uninterrupted one.

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
| `crates/gateway/src/live/recovery.rs` | status for ops | `PipelineStatus`, `StatusReporter`, `RecoveryStep`, `RecoveryReport`, `HandoverStep` |
| `crates/gateway/src/spool.rs` | gate, spooled bus, discard | `Gate` (`open`, `open_by`), `GateOpened`, `Gated`, `LiveBus`, `discard_corrupt` |
| `crates/gateway/src/gateway/postgres/gate_watch/mod.rs` | the gate's watch: opens a non-holder's gate once every pipeline group exists | `GATE_POLL`, `watch`, `WatchEnd`, `GateWatch` (`spawn`, `for_pipeline`), `GroupCheck`, `PgGroupCheck`, `GroupsFound`, `MissingGroups`, `missing_from`, `GroupCheckError` |
| `crates/gateway/src/gateway/postgres/gate_watch/tests.rs` | the watch on a scripted check and paused clock | tests |
| `crates/gateway/src/live/pg/groups.rs` | the pipeline's consumer groups: one list for the subscribe and the gate's watch | `PipelineGroups`, `ExchangeLogGroup`, `PG_SLOTS` |
| `crates/gateway/src/integration/gate.rs` | Postgres: the group check, its bound, a non-holder publishing directly, a fresh database spooling | tests |
| `crates/e2e/tests/lock_loss/gate.rs` | two gateways: one beside the lock holder publishing directly; a fresh database spooling until the groups exist | tests |
| `crates/gateway/src/store/{mod,migrations,lock}.rs` | migrate, head check, lock (its take bumps the epoch and fences), lazy pool and the session setup every pooled connection runs | `migrate`, `MigrateOptions`, `LAYERS`, `check_heads`, `PipelineLock` (`try_take`, `try_take_reporting`, `epoch`, `lost`, `release`), `TakeError`, `ReleaseError`, `PIPELINE_LOCK`, `PIPELINE_FENCE`, `lazy_pool`, `session_setup` |
| `crates/gateway/src/store/fence/mod.rs` | the epoch, a pipeline connection joining the fence, the bump | `Epoch`, `EpochError`, `FenceError` (`Fenced`, `Fencing`), `join`, `read_epoch`, `FENCE_POLL`, `FENCE_REPORT` |
| `crates/gateway/src/store/fence/wait.rs` | a new holder's wait for the fence, over any `FenceSession` (the lock's `PgConnection`) | `wait_for_fence`, `FenceSession`, `Fenced`, `FenceWaitError`, `Blocker` |
| `crates/transport/migrations/0003_pipeline_epoch.sql` | the one-row epoch table | - |
| `crates/gateway/src/store/timeouts/mod.rs` | the pipeline's statement and lock bounds: checked, defaulted, read from `store.pipeline_timeouts`, set on each pipeline connection | `PipelineTimeouts` (`new`, `statement`, `lock`, `session_settings`, `apply`, `DEFAULT_STATEMENT`, `DEFAULT_LOCK`), `InvalidPipelineTimeouts` |
| `crates/store/src/retention.rs` | the longer bound retention deletes raise their transaction to | `raise_for_retention`, `restore_after_retention`, `STATEMENT_BOUND`, `RETENTION_BOUND` |
| `crates/store/src/timeouts/mod.rs` | a statement cancelled at a bound: its kind, the process's counts | `TimeoutKind`, `TimeoutCounts`, `statement_timeouts`; `DbFailure::TimedOut`, `DbFailure::{is_timeout, is_transient}` (`error.rs`) |
| `crates/gateway/src/integration/statement_timeouts.rs` | Postgres: the bounds on the pipeline pools only, a statement and a lock wait cancelled, a blocked stage write retried, the reaper past a blocked sweep, `/healthz` within its bound | tests |
| `crates/gateway/src/gateway/postgres/mod.rs` | Postgres-mode start: the template and capture pools, the incarnation pools that set the statement and lock bounds and join the fence, capture's publish-only bus and spool, the bounded shutdown (a running incarnation handed over) | `PgRunning`, `PgStopped`, `RETRY`, `CaptureSide`, `capture_side`, `derived_pool`, `incarnation_pool`, `incarnation_pool_bounded`, `close_bounded`, `StopHandle::stop_by`, `PipelineStart`, `Late`, `SERVE_IDS`, `LOST_SHUTDOWN`, `LOST_REPORTED`, `CLOSE_BOUND`, `CAPTURE_CONNECTIONS` |
| `crates/gateway/src/gateway/postgres/handover/{mod,tests}.rs` | the planned handover on a graceful shutdown: steps as types, bounds, the proof the lock's release takes ([planned_handover.md](planned_handover.md)) | `hand_over`, `Handover`, `PoolsClosed`, `Outgoing`, `OutgoingIncarnation`, `HandoverReport`, `Bounded`, `TAIL` |
| `crates/gateway/src/live/pg/quiesce.rs` | a Postgres live process stopping on purpose: no delivery taken, every stage finishing its own | `Live::<PgSet>::quiesce`, `Quiesced`, `QuiesceReport` |
| `crates/transport/src/integration/handover.rs` | Postgres: released holds ready at once on the same attempt | tests |
| `crates/e2e/tests/lock_loss/handover.rs` | two gateways: the owner shut down gracefully with traffic flowing, the standby taking over | tests |
| `crates/gateway/src/gateway/postgres/pipeline.rs` | the pipeline task: waiting (and fencing), starting, running and abandoning incarnations, each with its own pool and bus under the lock's epoch; the gate's watch spawned beside it | `spawn_pipeline`, `application_name`, `runs_exchange_log` |
| `crates/gateway/src/gateway/postgres/api.rs` | the `api` role's surface | `spawn_api`, `ApiStart` |
| `crates/gateway/src/gateway/late.rs` | what an incarnation sets and a loss clears: the API router (503 before), the ops handles | `LateRouter`, `LateValue` |
| `crates/transport/src/pg/{mod,progress,subscription}.rs` | the bus an incarnation owns: abandon with every take joined; a planned handover's release of every hold; the publish-only bus capture appends through | `PgBus::abandon`, `Abandoned`, `PgBus::release_held`, `HANDOVER_REASON`, `PgBus::publisher` |
| `crates/gateway/src/ops/{mod,metrics}.rs` | readiness, health (the bus read bounded), metrics | `PgOps`, `BusSection`, `BusUnavailable`, `BusReport`, `StatementTimeoutsReport`, `HEALTH_BUS_BOUND`, `SpoolReport` |
| `crates/gateway/src/config/sections.rs` | `store.bus`, `store.pipeline_timeouts`, `spool` | `SpoolSection`, `StoreSection::MIN_CONNECTIONS`, `StoreSection::pipeline_timeouts` |
| `crates/gateway/src/runtime.rs` | the runtime `serve` runs on, shared with the tests | `builder`, `build` |
| `crates/e2e/src/answers.rs` | the answer set and the renaming of minted ids | `answers`, `Answers::renamed` |
| `crates/e2e/tests/common/{mod,gateway}.rs` | the Postgres-mode harness and the gateway outage harness | `start_pg`, `uninterrupted`, `Gateway`, `Run`, `outage` |
| `crates/e2e/tests/restart/main.rs` | kill and restart, cursors, ids (W9) | tests |
| `crates/e2e/tests/{spool,spool_outage,spool_outage_twice,spool_kill,spool_outage_serve_runtime}/main.rs` | the spool through the gateway (W9) | tests |
| `crates/e2e/tests/reorder/main.rs` | the relay's third exchange delivered after its fourth, memory and Postgres mode: the in-order threads and the uninterrupted answers, agents by role | tests |
| `crates/e2e/tests/lock_loss/{main,harness,partition,rival,repeated,standby}.rs` | the lock lost while the gateway runs: no refused connect, no owner write after the abandon, no leak, a standby taking over with no older-epoch write | tests, `Prober`, `Sampler`, `epoch_of` |
| `crates/gateway/src/integration/pool_close.rs` | why a shutdown across a partition hung: sqlx's `Pool::close` | test |
| `crates/gateway/src/main.rs`, `crates/gateway/src/gateway/mod.rs` | `serve` runs until SIGINT or SIGTERM; nothing else stops it | `Running::shutdown`, `Running::shutdown_bound` |
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
  `integration::pg_quiet_counts_entries_no_group_admitted_yet` (a settle
  is busy while a slot's group has a log entry it has not admitted),
  `integration::pg_watermark_survives_a_restart`, and
  `crosstalk-e2e`'s `postgres::postgres_mode_answers_as_memory_mode_and_after_a_restart`
  (the wiki relay through memory-mode and Postgres-mode `Live`, the same
  `QueryApi` answers, and the same after a restart).
- Outage, resumed in process (Postgres, a cuttable `DbLink`, wiki relay
  through the proxy): `spool_outage::database_cut_mid_stream_spools_then_drains_to_the_uninterrupted_answers`
  (cut, forwarded unchanged and spooled, the lock lost and the incarnation
  abandoned, the same process spooling on with the database down, the
  link restored, the resume and the drain, every capture in the log once,
  the uninterrupted run's answers once renamed), `spool_outage_twice::database_cut_twice_drains_to_the_uninterrupted_answers`,
  and `spool_outage_serve_runtime::database_cut_on_the_serve_runtime_resumes_and_drains_to_the_uninterrupted_answers`
  (every gateway task on `runtime::builder()`, 2 MiB worker stacks).
- Fence (Postgres): `integration::fence::pg_a_fenced_incarnation_cannot_connect_after_a_newer_epoch`,
  `pg_a_take_waits_for_an_older_connection_and_completes_once_it_closes`
  (past `TAKE_TIMEOUT`), `pg_the_pipeline_reports_fencing_while_an_older_connection_lives`,
  `pg_the_publisher_pool_is_unaffected_by_the_fence`; `crosstalk-e2e`
  `lock_loss::standby` (above). No database: `store::fence::tests::*`
  (the wait on a scripted session and paused clock: never gives up, its
  blockers read at most every `FENCE_REPORT`, a failed or silent try a
  loss; the epoch), `live::recovery::tests::fencing_is_ready_degraded_with_recovery_pending`,
  `the_report_carries_the_epoch`, `ops::tests::the_recovery_report_carries_the_epoch_in_health_and_metrics`,
  `store::migrations::tests::transport_head_creates_the_pipeline_epoch`.
- The capture gate (INV-1241). Postgres (written; node0 runs pending):
  `integration::gate::{pg_the_group_check_finds_only_the_groups_that_exist,
  pg_a_blocked_group_check_returns_at_its_lock_bound,
  pg_a_non_holder_publishes_directly_once_the_groups_exist,
  pg_a_non_holder_on_a_fresh_database_spools_until_the_groups_exist}`;
  `crosstalk-e2e` `lock_loss::gate` (above). No database (run):
  `gateway::postgres::gate_watch::tests::*` (the watch on a scripted check
  and paused clock: opens at the first look when every group exists,
  stays closed and looks once per poll while one is missing or a look
  fails, never looks while this process holds the lock, ends without a
  look once the subscribe opened the gate, stops with its handle),
  `live::pg::groups::tests::*` (the list).
- Bounded statements (Postgres, written; node0 runs pending):
  `integration::statement_timeouts::{pg_pipeline_pools_carry_the_bounds_and_the_publisher_pool_none,
  pg_a_pipeline_statement_past_its_bound_is_cancelled_as_a_statement_timeout,
  pg_a_pipeline_lock_wait_past_its_bound_is_cancelled_as_a_lock_timeout,
  pg_a_stage_write_blocked_past_the_lock_timeout_is_retried_and_completes,
  pg_the_reaper_survives_a_stalled_sweep,
  pg_healthz_answers_within_its_bound_while_group_stats_is_blocked,
  pg_the_bounds_hold_on_every_connection_of_the_pool,
  pg_retention_deletes_run_under_the_longer_bound,
  pg_the_bus_prune_runs_under_the_retention_bound}`. No database (run):
  `store::timeouts::tests::*` (defaults, checks, the store section),
  `ops::health_bound_tests::*` (`/healthz` within its bound against a
  server that never answers; the bus section's JSON; the counts in health
  and metrics), `crosstalk_store::timeouts::tests::*` (typing, transience,
  counting), `crosstalk_transport::pg::row::timeout_tests::*`,
  `crosstalk_provenance::pg::timeout_tests::*`.
- Lock loss (Postgres): `crosstalk-e2e` `lock_loss` (above);
  `integration::pool_close::pg_a_partitioned_pool_close_waits_for_an_aborted_query`;
  `crosstalk-transport` `integration::abandon::pg_an_abandoned_bus_joins_its_takes_and_leaves_its_holds_for_recovery`.
  No database: `live::recovery::tests::a_resuming_process_stays_ready_degraded_after_a_loss`,
  `crosstalk_transport::pg::progress::tests::aborted_takes_are_gone_once_none_by_returns`,
  `crosstalk_transport::pg::tests::a_publish_only_bus_refuses_subscriptions`.
- Planned handover (INV-1242). No database (run):
  `gateway::postgres::handover::tests::*` (five: the order, each step
  reported while it runs, every stalled step cut at its bound and the
  later ones still run, the whole within the deadline plus `TAIL`),
  `live::pg::pump::tests::acks_after_the_subscription_ended_still_land`,
  `live::recovery::tests::a_handover_is_not_ready_and_names_its_step`,
  `ops::handover_tests::readyz_and_healthz_name_the_handover_step`.
  Postgres (run on node0, 10× clean): `crosstalk-transport`
  `integration::handover::*` (two), `crosstalk-e2e`
  `lock_loss::handover::a_planned_shutdown_hands_the_pipeline_to_the_standby_cleanly`.
- Memory mode unchanged: every `ct-eval replay` (since retired) of the bench runs was
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
| INV-1231 `transport.stage-pump.no-dropped-delivery` | unit `crosstalk_gateway::live::pg::pump::tests::a_next_dropped_after_its_answer_keeps_the_delivery` (ran) |
| INV-1236 `transport.pipeline-lock.loss-detected-before-reap` | unit `crosstalk_gateway::store::lock::tests::detection_is_one_ping_and_its_deadline` (ran); integration `crosstalk_gateway::integration::lock::pg_a_silently_partitioned_lock_session_is_lost_within_the_bound` (failed on node0 before the fix) and `pg_a_running_pipeline_reports_its_partitioned_lock_lost_within_the_bound` (now: `lost`, the incarnation abandoned, 200 degraded) |
| INV-1237 `ingress.capture.never-stopped-by-lock-loss` | integration `crosstalk_e2e::lock_loss::partition::{a_silent_partition,a_postgres_stop,a_partition_past_the_server_reap}_never_stops_capture_and_resumes_in_process` (written; node0 runs pending) |
| INV-1238 `transport.pipeline-lock.no-owner-write-after-abandon` | unit `crosstalk_transport::pg::progress::tests::aborted_takes_are_gone_once_none_by_returns` (ran); integration `crosstalk_e2e::lock_loss::rival::a_terminated_lock_session_and_a_rival_holder_leave_no_owner_write_after_the_abandon`, `crosstalk_e2e::lock_loss::repeated::repeated_lock_losses_leave_no_pool_listener_or_task_behind`, `crosstalk_transport::integration::abandon::pg_an_abandoned_bus_joins_its_takes_and_leaves_its_holds_for_recovery` (written; node0 runs pending) |
| INV-1239 `transport.pipeline-lock.no-write-from-an-older-epoch` | unit `crosstalk_gateway::store::fence::tests::the_wait_never_gives_up_and_completes_once_the_fence_is_free`, `a_try_without_an_answer_is_a_lock_loss_within_the_ping_deadline`, `a_failed_try_while_waiting_is_a_lock_loss` (ran); integration `crosstalk_gateway::integration::fence::*` (four) and `crosstalk_e2e::lock_loss::standby::*` (two) (written; node0 runs pending) |
| INV-1241 `ingress.capture.gate-opens-once-groups-exist` | unit `crosstalk_gateway::gateway::postgres::gate_watch::tests::*` (seven), `crosstalk_gateway::live::pg::groups::tests::*` (four) (ran); integration `crosstalk_gateway::integration::gate::*` (four) and `crosstalk_e2e::lock_loss::gate::*` (two) (written; node0 runs pending) |
| INV-1242 `transport.pipeline-lock.planned-handover-clean` | unit `crosstalk_gateway::gateway::postgres::handover::tests::*` (five), `crosstalk_gateway::live::pg::pump::tests::acks_after_the_subscription_ended_still_land`, `crosstalk_gateway::live::recovery::tests::a_handover_is_not_ready_and_names_its_step`, `crosstalk_gateway::ops::handover_tests::readyz_and_healthz_name_the_handover_step` (ran); integration `crosstalk_transport::integration::handover::*` (two) and `crosstalk_e2e::lock_loss::handover::a_planned_shutdown_hands_the_pipeline_to_the_standby_cleanly` (written; node0 runs pending) |
| INV-1240 `transport.pipeline.statements-bounded` | unit `crosstalk_store::timeouts::tests::*`, `crosstalk_transport::pg::row::timeout_tests::*`, `crosstalk_provenance::pg::timeout_tests::*`, `crosstalk_gateway::store::timeouts::tests::*`, `crosstalk_gateway::ops::health_bound_tests::healthz_answers_within_its_bound_while_the_bus_read_stalls` (ran); integration `crosstalk_gateway::integration::statement_timeouts::*` (nine; written, node0 runs pending) |
| INV-1233 `transport.settle.quiet-covers-unadmitted` | integration `crosstalk_gateway::integration::pg_quiet_counts_entries_no_group_admitted_yet` (ran on node0; failed before the fix) |
| INV-1221 `ingress.proxy.forwarding-independent-of-capture-store` | `crosstalk_gateway::postgres_down::proxy_forwards_through_outage_and_full_spool` (ran) |
| INV-581 `topology.frontier.covers-pending` | `crosstalk_gateway::integration::pg_frontier_covers_pending_deliveries` (Postgres, not run) |
| INV-1202 `transport.consumer.derived-envelope-ids` | `crosstalk_gateway::dst::classifier_redelivery_republishes_the_same_envelope_ids` (ran; the flag covers the whole list, left for the merger) |

- `serve` never migrates; it consumes only against a database at head.
- One pipeline process per database; a process without the lock neither
  consumes nor relays; a lost lock abandons the pipeline's incarnation
  (its pool fenced, every task and take aborted and joined, nothing
  drained) and the process takes the lock again in process.
- Every taking of the lock bumps the pipeline epoch (committed) before it
  tries the exclusive fence, and returns only once the fence was granted:
  no connection of an earlier incarnation is left in any process. Every
  incarnation connection takes the shared fence before it reads the epoch
  and refuses itself unless the epoch is its own. Only incarnation pools
  join the fence. The fence wait never gives up; it is not counted against
  `TAKE_TIMEOUT`; each try is bounded by `PING_TIMEOUT` and a failed one is
  a lock loss.
- Every pipeline incarnation connection runs with `statement_timeout`
  and `lock_timeout` (`store.pipeline_timeouts`, lock at most statement,
  both > 0), set before it joins the fence; retention deletes raise their
  transaction's `statement_timeout` to `retention_ms` (at least the
  statement bound), never the lock bound; no other pool or session
  does. A statement cancelled at either bound is a transient
  `DbFailure::TimedOut`: every pipeline caller retries it (nack and
  redelivery, the next round), none acks it as refused (INV-1240).
  `/healthz` bounds its bus read itself (`HEALTH_BUS_BOUND`).
- A lost lock never stops the proxy or capture; `serve` stops only on
  SIGINT or SIGTERM, and every step of its shutdown that touches the
  database is bounded.
- A graceful shutdown hands a running incarnation over: quiesce, release
  the holds (ready at once, the attempt given back), close the pool, then
  unlock; the lock's release needs the `PoolsClosed` proof only the last
  step makes, and the whole ends within `shutdown.flush_timeout_ms`
  (INV-1242).
- An incarnation's pool and bus are its own and never reused; capture's
  bus is publish-only, over a pool of its own.
- Forwarding never waits on the database, the bus or the spool; capture
  publishes through the spool, which refuses only when full.
- No pipeline group misses an envelope published before it first
  subscribed (the gate). The capture gate opens only once every pipeline
  group's row exists, and then within `GATE_POLL` (1 s) plus one bounded
  look in a process that does not hold the lock (INV-1241
  `ingress.capture.gate-opens-once-groups-exist`); the groups it looks
  for are exactly those `Live::start_pg` subscribes
  (`live::pg::groups::PipelineGroups`).
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

## Known limitations

- **A stopped old owner blocks a takeover.** The fence trades liveness
  for safety: a new holder waits for every connection of an earlier
  incarnation to close, however long. An old owner stopped with its
  connections open (`SIGSTOP`, a hung host whose TCP stays up) keeps the
  new holder at `fencing` until those connections die (keepalives, the
  owner resuming and abandoning, or an operator terminating the sessions
  the fencing warn names). Capture keeps spooling meanwhile.
- **The old owner still writes until its next ping.** After its lock
  session is ended (`PingFailed`), the old incarnation keeps writing for
  up to `PING` (5 s), but no newer incarnation runs until it stops: the
  new holder's take waits at `fencing` (the `standby` test prints those
  writes, all before the new incarnation's first).
- **The SIGTERM restart gap.** A `docker restart` (or any SIGTERM) still
  stops the proxy listener for the shutdown and the new process's start:
  about 40 requests refused per restart at the bench's rate. Only a lost
  lock is resumed in process. The pipeline itself is handed over cleanly
  (a standby takes it within about `RETRY`), but the standby only gets
  traffic once the load balancer moves it there; the stopping owner's
  `/readyz` is 503 from the start of the shutdown.
- **A planned handover's standby waits for its retry.** The standby
  notices the free lock at its next try, up to `RETRY` (2 s) after the
  unlock; nothing wakes it sooner.
- **A partition's `UNLISTEN`.** Dropping an abandoned incarnation's
  `PgListener` spawns sqlx's own `UNLISTEN *` on its connection, which no
  deadline bounds; across a silent partition that detached task waits for
  TCP. Nothing waits for it and it writes nothing; it ends when the link
  heals or TCP gives up.
- **The exchange log's aborted writer.** An exchange-log append in flight
  on the blocking pool when its task is aborted may finish after the next
  incarnation opened the log; the next open truncates a torn tail and the
  consumer counts a duplicate, as after a crash.

## Gaps

- The retention deletes (the bus's hourly `prune`, the per-tick
  provenance and ledger expiries, the index age-out) are single
  `DELETE`s, run under the longer `retention_ms` bound (15 min) so they
  are not cancelled every round. They should delete in batches; that is
  the real fix, after which they can run under the pipeline's bound.

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
