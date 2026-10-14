# PgBus: the durable event bus on Postgres

`crosstalk-transport`'s `pg` module: `PgBus`, an `EventBus` whose log,
consumer groups, deliveries and dead letters live in the `transport`
schema, so work in flight between consumers survives a restart. Roadmap
P7.3, workstream W1 of [Postgres stores](postgres_stores.md) (decision
Q1). The gateway uses it, behind the [publish spool](publish_spool.md),
whenever a `store` is configured (wired by W8: [postgres_mode](postgres_mode.md)).

## Scope

- `PgBus`: `EventBus` and `Subscription` (`PgSubscription`) with
  `MpscBus`'s group semantics: every group gets each envelope published
  under one of its subjects after its first subscribe; subscriptions of a
  group share deliveries, one holder at a time; `attempt` counts
  deliveries; ack, nack with clamped backoff, ack timeout, redelivery after
  a dropped holder; dead letters after `max_attempts`, stored before
  release; strict decoding (an undecodable delivery is reported once and
  terminated).
- `PgDeadLetters`: `DeadLetterStore` (put, replay to one group at
  attempt 1, list newest envelope first with cursors).
- Durability and restart: `publish` returning `Ok` means the envelope is
  in `transport.events`; groups and deliveries persist; `recover_held`
  returns deliveries a stopped process held, ready at once with no
  backoff, attempt counted.
- Idempotent publish on `Envelope::id`.
- `group_stats` (pending count, oldest pending `at`, dead letters, oldest
  dead letter `at` per group) for the frontier and `/healthz`.
- `prune(now, keep)` for log retention (decision Q6).
- `DrainTarget` for the spool: `probe` and a one-transaction
  `publish_batch`.
- Migrations (`crates/transport/migrations/0001_bus.sql`,
  `0002_held_at.sql`, `0003_pipeline_epoch.sql`), `MIGRATIONS` and
  `migrate`. The third adds `transport.pipeline_epoch`, the one-row epoch
  the gateway's pipeline lock bumps and its pipeline connections check
  ([pipeline_fence.md](pipeline_fence.md)); the bus itself never reads it.
- Structured config (`PgBusConfig`).

## Non-scope

- Building it in `serve`, the `bus` config section, calling `prune` from
  the retention tick, `PgFrontierSource`, the pipeline advisory lock and
  `/readyz`/`/healthz` reporting: W8 (`crosstalk-gateway`).
- Several pipeline processes on one database: `recover_held` assumes one
  (the gateway's pipeline lock). Split proxy/pipeline roles are P9 (Q9).
- Partitioning `transport.events` by `seq` range (deferred until volume
  shows a need).
- `publish_encoded` (foreign bytes): only `MpscBus` has it; every
  `PgBus` log entry was encoded from an `Envelope`.
- Backpressure: the log is the queue, so `publish` never waits for group
  room (`transport.backpressure.*` are `MpscBus`'s).

## Schema

`crates/transport/migrations/0001_bus.sql`, schema `transport` (run by
`crosstalk_transport::pg::migrate`, i.e. `crosstalk_store::migrate` with
`Layer::Transport`). Ids are ULID text in `COLLATE "C"` columns, group
names `COLLATE "C"`, times microseconds in `bigint`, envelopes their wire
JSON in `text`.

| Table | Columns | Notes |
| --- | --- | --- |
| `events` | `seq` identity PK, `id` UNIQUE, `subject`, `at`, `routed`, `envelope` | The log. `routed = false` only for an envelope stored because a dead letter was `put` for it directly: no group admits it |
| `groups` | `name` PK, `subjects` (sorted text[]), `max_attempts`, `initial_backoff_micros`, `max_backoff_micros`, `admitted_through` | One row per group; a new group starts at the log head |
| `deliveries` | (`group_name`, `seq`) PK, `at`, `attempt`, `state` (`ready`/`held`/`delayed`), `available_at`, `last_error` | Admitted, unacked entries; `available_at` set exactly when `delayed` |
| `dead_letters` | (`group_name`, `event_id`) PK, `seq`, `at`, `envelope`, `attempts`, `last_error` | Never pruned automatically |

Deviations from the design's sketch: `groups.*_backoff_micros` (not
`_ms`: `RetryPolicy` durations are compared exactly as stored), the
`events.routed` flag, `COLLATE "C"` group names (so dead letters list in
the byte order `MpscBus` uses), and indexes `deliveries_by_seq`,
`dead_letters_by_seq` for prune.

## Data and control flow

```text
publish(envelope) / publish_batch(envelopes)          one transaction, bounded by publish_timeout
  pg_advisory_xact_lock(PUBLISH_LOCK)                 commits happen in seq order
  INSERT INTO events ... ON CONFLICT (id) DO NOTHING  per envelope; an id already there adds nothing
  pg_notify('transport_events') if anything was new
  COMMIT; wake local waiters
  (timeout → Disconnected: unknown whether it landed; a retry under the same id is harmless)

subscribe(subjects, group, retry)
  INSERT INTO groups (..., admitted_through = max(seq)) ON CONFLICT DO NOTHING
  read the row back: other subject set → GroupSubjectMismatch; other policy → GroupRetryMismatch

next()                                                 loop of one transaction per try
  SELECT ... FROM groups WHERE name = g FOR UPDATE     one admitter per group
  room = group_capacity - count(deliveries of g)
  one statement: head = max(seq); candidates = events with seq > admitted_through,
                 routed, subject in the group's set, first room+1 by seq;
                 admit the first `room` as 'ready'
  admitted_through = last admitted (if candidates exceeded room) else head
  delayed rows with available_at <= clock.now() → ready
  take the lowest-seq ready row FOR UPDATE SKIP LOCKED → held, attempt + 1
    (the reaper learns the hold before the commit, with a provisional deadline
     of 2 x ack_timeout + publish_timeout from the take's start)
  COMMIT
  row  → deadline = handout + ack_timeout, told to the reaper and kept locally
         (the ack timeout never counts the pool wait or the take's round trips)
         decode strictly → Delivery { id: process counter, attempt, envelope }
         undecodable → delete the row, warn, Some(Err(Decode)) once
  none → wait for: a wake-up (local publish/ack/nack/replay, NOTIFY via the
         listener task), the earliest delayed row's due time, or `poll`
  database error → logged, retried at `poll` (consumers only see deliveries,
                   decode errors, or None after shutdown)

ack(id)       held here and before its deadline? DELETE ... WHERE state='held' AND attempt=a
              (0 rows → UnknownDelivery)
nack(id, d)   fail(Nack): see below
reaper task   deadline reached → fail(Timeout); subscription dropped → fail(Dropped) at once

fail(group, seq, attempt, failure)                     one transaction, conditional
  row still held at that attempt? (no → no-op / UnknownDelivery)
  attempt >= max_attempts → INSERT dead letter (replacing one for the same envelope and
                            group) + DELETE delivery  (stored before release)
  else → delayed, available_at = clock.now() + delay
         nack: retry_after clamped to initial..=max backoff
         timeout, dropped: initial * 2^(attempt-1), capped

recover_held()                                         at start, before consumers subscribe
  held rows on their group's last attempt → dead letters ("process restarted while
  holding the delivery"); every other held row → ready at once, no backoff,
  available_at NULL (attempt as the dead holder was given it: the take
  counted it, as MpscBus counts a dropped holder; recovery adds none)

PgDeadLetters
  put     → the envelope into events (routed = false if new) + upsert the letter
  replay  → letter? group subscribed and takes its subject? → delivery 'ready' at
            attempt 0 + delete the letter + notify, one transaction
  list    → (event_id, group_name) descending, keyset after the cursor's position

prune(now, keep)
  horizon = min over groups of (lowest pending seq, else admitted_through + 1)
  DELETE events with seq < horizon, at < now - keep, named by no delivery and no
  dead letter (no groups: age alone decides)
```

Ordering. Admission advances `admitted_through` past entries other
subjects own and up to the log head the admitting statement saw. That is
sound because every log append holds a transaction-scoped advisory lock
(`PUBLISH_LOCK`), so appends commit in `seq` order: a statement that sees
`seq n` committed has seen every committed `seq` below it. Without it, a
publish that took `seq 10` but committed after `seq 11` would be skipped.

Time. `available_at` and due times come from the injected `Clock`; ack
deadlines are tokio `Instant`s held by the reaper task, as on `MpscBus`.
Nothing reads `now()` in SQL. Tests run in real time (a paused clock
cannot drive network I/O), so the DST suite stays `MpscBus`'s and the
group semantics are checked by the conformance suite over both buses.

Tasks. `PgBus::new` touches no database (it can be built while the
database is down, which the spool needs). It spawns the listener task
(`LISTEN transport_events`, reconnecting every `poll`, waking everyone on
each notification and each reconnect) and the reaper task. Both stop on
`shutdown` or when the last `PgBus` handle drops; subscriptions then see
`None`. Neither touches the database on stop: a dropped bus is a stopped
process. `PgBus::publisher` runs neither: a bus that only appends (and
reads its statistics) refuses every subscribe (`PublishRejected`).

Ownership and abandon. A bus owns its subscriptions' takes: each take's
task registers its abort handle in the bus's take registry
(`progress::Takes::attach`). `PgBus::abandon(deadline)` is the terminal
stop a process that lost its pipeline lock uses: the bus closes (waiting
`next` calls return `None`), the listener and the reaper are aborted and
joined, and every take in flight is aborted and joined (the registry is
empty once each take's future has been dropped, and a take's transaction
lives in that future, so none commits afterwards). The reaper fails
nothing on the way out and a subscription dropped afterwards nacks
nothing: every hold stays `held` for the next holder's `recover_held`.
Close the bus's pool first (the gateway's fence): a connection an aborted
take gives back to a closed pool is closed, not pinged across a dead link.
`shutdown` and `abandon` are terminal, so the gateway never reuses a bus:
each pipeline incarnation builds `PgBus::new` over a pool of its own,
while capture appends through a `PgBus::publisher` over another pool that
no incarnation shares (`docs/features/postgres_mode.md`, "Losing the
pipeline lock"). Report: `Abandoned { tasks_joined, takes_aborted,
takes_joined }`.

Restart order. Takes go by `seq` among `ready` rows. `recover_held` makes
every recovered hold `ready` with no backoff, so after a restart a single
consumer of a group takes its `ready` rows (recovered or never taken) in
`seq` order, then what it admits from the log as it pulls, in `seq`
order; rows a live process delayed before the stop join once due. A
recovered delivery is therefore taken before every entry published after
it, a spool record drained after the outage included
(`transport.restart.recovered-ready-first`). This is the only redelivery
without a backoff: a nack, an ack timeout and a dropped holder are
failures of a live consumer and keep `transport.retry.backoff-floor`.
The stale-hold sweep keeps it too: it cannot tell a hold a dying process
committed after the next `recover_held` from one a live taker lost track
of, and by the time a hold is stale the consumer has taken later entries
for minutes, so no order is left to restore.

Stale holds. Each hold records `held_at` (bus clock; `0002_held_at.sql`).
A process that subscribed a group sweeps, every ack timeout, the holds
older than `reaper::stale_after` (3 × ack timeout + publish timeout + a
take's limit) and fails each as a timeout, its attempt counted
(`transport.hold.always-reaped`): a hold its taker lost track of, or one a
dying process committed after the next process's `recover_held`, is
redelivered rather than held for good. `PgBus::reaper().swept` counts
them. A take that finishes just as its limit runs out is handed out, not
thrown away.

Cancel safety. Each take's transaction runs in a task of its own
(`take_task`), kept in the subscription: a `next` dropped while it waits
leaves the take running to its commit, and the next call hands out what it
took, as `MpscBus` returns the same delivery. Only a subscription dropped
with a take in flight leaves a committed hold unseen; the reaper already
knows it with its provisional deadline (one attempt and that delay lost,
nothing else). The reaper's own transaction sets `lock_timeout = 2s`, so a
row locked by a stuck session delays its reaping, never all reaping.
Regression: `integration::pg_next_dropped_mid_take_hands_out_the_hold_on_the_next_call`.

Bounded statements. In the gateway every connection of a pipeline
incarnation's pool, the pool an incarnation's `PgBus::new` runs over,
carries `statement_timeout` and `lock_timeout` (`store.pipeline_timeouts`,
60 s and 15 s by default; [postgres_mode.md](postgres_mode.md), "Bounded
statements"); a take's and the reaper's `SET LOCAL lock_timeout` keep
their tighter bounds, and `prune` raises its transaction to the longer
retention bound (`retention_ms`, 15 min: `crosstalk_store::raise_for_retention`),
since its single delete grows with a long stop. A statement the server cancels at a bound (SQLSTATE
`57014` or `55P03`) classifies as `DbFailure::TimedOut` and the bus
reports it `BusError::Disconnected` (`row::bus_error`), as its own publish
timeout does. Each bus caller retries: the reaper's `sweep_stale` at its
next round (its loop goes on reaping deadlines meanwhile), its `fail` at
the poll interval, a take at the poll interval; a failed ack or nack
leaves the hold for the reaper, which redelivers it. `group_stats` on
capture's publish-only bus has no server-side bound; `/healthz` bounds it
client-side (2 s). The `LISTEN` connection runs no statement while it
waits, so neither bound applies; it needs none (notifications are hints,
a waiting `next` re-reads every poll, and a lost lock's abandon aborts the
task).

Transactions are `READ COMMITTED` with explicit locks (the queue-table
pattern), not the `SERIALIZABLE` retries the stores use.

## Configuration

`PgBusConfig` decodes like `BusConfig` (every field optional, unknown
fields refused, durations in `_micros`):

| Field | Default | Meaning |
| --- | --- | --- |
| `group_capacity` | 1024 | envelopes a group tracks at once; `next` admits only while it has room |
| `ack_timeout_micros` | 30 s | how long a delivery may be held |
| `poll_micros` | 250 ms | how often a waiting `next` re-reads without a notification |
| `publish_timeout_micros` | 5 s | the bound on `publish` (and `probe`), connecting included; past it, `Disconnected` |
| `retention_micros` | 7 days | what the retention tick passes to `prune` |

The design named `bus.poll_ms` and `bus.retention_ms`; the crate keeps its
`_micros` convention. The flow group's ack timeout must exceed the L5
checkpoint interval; W8 checks that at start.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/transport/migrations/0001_bus.sql` | the schema | - |
| `crates/transport/migrations/0002_held_at.sql` | when each hold was taken (the stale-hold sweep) | - |
| `crates/transport/migrations/0003_pipeline_epoch.sql` | the pipeline epoch (the gateway's fence; not the bus's) | - |
| `src/pg/mod.rs` | the bus handle, tasks, recovery, abandon, the publish-only bus, trait impls | `PgBus`, `PgBus::{publisher, abandon}`, `Abandoned`, `Recovered`, `MIGRATIONS`, `migrate` |
| `src/pg/progress.rs` | each take in flight: its phase, its abort handle; abort and join them all | `Takes::{attach, abort_all, none_by}` (crate), `TakeReport`, `ReaperReport` |
| `src/pg/config.rs` | config and its checked decode | `PgBusConfig`, `InvalidPgBusConfig` |
| `src/pg/publish.rs` | log appends, probe | `PUBLISH_LOCK`, `CHANNEL` (crate) |
| `src/pg/subscription.rs` | subscribe, `next`, ack, nack | `PgSubscription` |
| `src/pg/reaper.rs` | failure handling, the deadline task | `fail`, `ReaperMsg` (crate) |
| `src/pg/listen.rs` | the `LISTEN` task | (crate) |
| `src/pg/dead_letters.rs` | dead-letter store and cursors | `PgDeadLetters` |
| `src/pg/stats.rs` | per-group backlog | `GroupStats` |
| `src/pg/prune.rs` | retention | (crate) |
| `src/pg/row.rs` | wire forms, error mapping | (crate) |
| `src/conformance/` | the bus conformance suite over `MpscBus` and `PgBus` | (tests) |
| `src/integration/mod.rs` | Postgres-only tests and the spool over `PgBus` | (tests) |
| `src/integration/abandon.rs` | an abandoned bus: takes joined, holds left for recovery | (tests) |

## Tests

- `conformance::{mpsc,pg}::*`: twelve cases written once over a `Kit`
  (groups and sharing, no backfill, a group kept without consumers,
  subject filter with unchanged envelopes, unknown deliveries, attempts
  then dead letter, ack timeout with the late ack refused, a dropped
  holder, replay, subscribe mismatches, dead-letter listing and cursors).
- `integration::*` (Postgres): `pg_publish_survives_restart`,
  `pg_publish_is_idempotent_on_id`,
  `pg_held_delivery_redelivered_after_restart` (and dead-lettering on the
  last attempt), `pg_recovered_hold_is_ready_and_taken_first` (a
  recovered hold is ready at once and taken before entries published
  after it), `pg_dead_letters_persist_and_replay`,
  `pg_prune_never_drops_an_unacked_event`,
  `pg_group_stats_agree_with_deliveries`,
  `pg_group_capacity_bounds_admission`,
  `pg_publish_while_unreachable_is_disconnected`,
  `abandon::pg_an_abandoned_bus_joins_its_takes_and_leaves_its_holds_for_recovery`
  (a hold and a take in flight at the abandon: every take joined, the
  hold still held and nothing changed after it; the next bus readies it
  at once).
- No database: `pg::tests::a_publish_only_bus_refuses_subscriptions`,
  `pg::progress::tests::aborted_takes_are_gone_once_none_by_returns`.

## Invariants and constraints

| Invariant | Evidence |
| --- | --- |
| INV-1203 `transport.durability.pg-publish-persisted` | `integration::pg_publish_survives_restart` |
| INV-1204 `transport.publish.idempotent-on-id` | `integration::pg_publish_is_idempotent_on_id` |
| INV-1205 `transport.restart.held-redelivered` | `integration::pg_held_delivery_redelivered_after_restart` |
| INV-1234 `transport.restart.recovered-ready-first` | `integration::pg_recovered_hold_is_ready_and_taken_first` |
| INV-X `transport.retention.prune-keeps-unfinished` | `integration::pg_prune_never_drops_an_unacked_event` |
| INV-1238 `transport.pipeline-lock.no-owner-write-after-abandon` (with the gateway) | `pg::progress::tests::aborted_takes_are_gone_once_none_by_returns`, `integration::abandon::pg_an_abandoned_bus_joins_its_takes_and_leaves_its_holds_for_recovery` |
| INV-1240 `transport.pipeline.statements-bounded` (with the gateway) | `pg::row::timeout_tests::a_statement_or_lock_timeout_is_disconnected`, `crosstalk_gateway::integration::statement_timeouts::pg_the_reaper_survives_a_stalled_sweep` |

All four are Postgres integration tests, written but not run by the
workstream (its sandbox could not reach the test server), so their
evidence stays `agent = "false"` until they run.

- `publish` returns `Ok` only after the commit; a log entry exists at most
  once per envelope id.
- Log appends commit in `seq` order (`PUBLISH_LOCK`).
- Once `abandon` returns with `takes_joined`, no take of the bus is in
  flight and none can commit; a publish-only bus never subscribes.
- A delivery row is in exactly one state; `available_at` is set exactly
  when `delayed` (a table check).
- Every failure of a hold is conditional on (`held`, attempt), so late,
  repeated or cancelled failures are no-ops.
- A dead letter is inserted in the transaction that deletes its delivery.
- `prune` never deletes an entry a delivery or a dead letter names, or one
  a group has not passed.
- No SQL reads `now()`; delays use the injected clock.
- Errors and logs never carry payloads: logs carry group, seq, attempt,
  event id; database failures are reported by their classification.
- A statement cancelled at a bound is `Disconnected` to a bus caller,
  never a refusal, and is classified once per failure (the reaper's
  warn reads one classification), so it is counted once.
- Dead-letter cursors are checked with a per-bus-value keyed hash: a token
  another bus value issued, another filter's, or edited text is
  `InvalidCursor` (an integrity check, not a MAC; cursors die with the
  process, which the surface's own cursors wrap).
