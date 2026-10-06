# PgBus: the durable event bus on Postgres

`crosstalk-transport`'s `pg` module: `PgBus`, an `EventBus` whose log,
consumer groups, deliveries and dead letters live in the `transport`
schema, so work in flight between consumers survives a restart. Roadmap
P7.3, workstream W1 of [Postgres stores](postgres_stores.md) (decision
Q1). The gateway uses it, behind the [publish spool](publish_spool.md),
whenever a `store` is configured (wiring is W8).

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
  returns deliveries a stopped process held, attempt counted.
- Idempotent publish on `Envelope::id`.
- `group_stats` (pending count, oldest pending `at`, dead letters, oldest
  dead letter `at` per group) for the frontier and `/healthz`.
- `prune(now, keep)` for log retention (decision Q6).
- `DrainTarget` for the spool: `probe` and a one-transaction
  `publish_batch`.
- Migrations (`crates/transport/migrations/0001_bus.sql`), `MIGRATIONS`
  and `migrate`.
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
  holding the delivery"); every other held row → delayed by its group's backoff
  (attempt counted, as MpscBus counts a dropped holder)

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
process.

Cancel safety. A `next` dropped while its transaction commits may leave
the row held; the reaper already knows the hold with its provisional
deadline, so it is taken back then (one attempt and that delay lost,
nothing else). `MpscBus`'s
stronger guarantee (the next call returns the same delivery) does not
hold here.

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
| `src/pg/mod.rs` | the bus handle, tasks, recovery, trait impls | `PgBus`, `Recovered`, `MIGRATIONS`, `migrate` |
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

## Tests

- `conformance::{mpsc,pg}::*`: twelve cases written once over a `Kit`
  (groups and sharing, no backfill, a group kept without consumers,
  subject filter with unchanged envelopes, unknown deliveries, attempts
  then dead letter, ack timeout with the late ack refused, a dropped
  holder, replay, subscribe mismatches, dead-letter listing and cursors).
- `integration::*` (Postgres): `pg_publish_survives_restart`,
  `pg_publish_is_idempotent_on_id`,
  `pg_held_delivery_redelivered_after_restart` (and dead-lettering on the
  last attempt), `pg_dead_letters_persist_and_replay`,
  `pg_prune_never_drops_an_unacked_event`,
  `pg_group_stats_agree_with_deliveries`,
  `pg_group_capacity_bounds_admission`,
  `pg_publish_while_unreachable_is_disconnected`.

## Invariants and constraints

| Invariant | Evidence |
| --- | --- |
| INV-1203 `transport.durability.pg-publish-persisted` | `integration::pg_publish_survives_restart` |
| INV-1204 `transport.publish.idempotent-on-id` | `integration::pg_publish_is_idempotent_on_id` |
| INV-1205 `transport.restart.held-redelivered` | `integration::pg_held_delivery_redelivered_after_restart` |
| INV-X `transport.retention.prune-keeps-unfinished` | `integration::pg_prune_never_drops_an_unacked_event` |

All four are Postgres integration tests, written but not run by the
workstream (its sandbox could not reach the test server), so their
evidence stays `agent = "false"` until they run.

- `publish` returns `Ok` only after the commit; a log entry exists at most
  once per envelope id.
- Log appends commit in `seq` order (`PUBLISH_LOCK`).
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
- Dead-letter cursors are checked with a per-bus-value keyed hash: a token
  another bus value issued, another filter's, or edited text is
  `InvalidCursor` (an integrity check, not a MAC; cursors die with the
  process, which the surface's own cursors wrap).
