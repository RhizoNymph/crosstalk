# Planned handover: a clean pipeline handover on a planned shutdown

Two gateways share one database: the owner holds the pipeline lock, a
standby waits for it ([postgres_mode.md](postgres_mode.md), "Losing the
pipeline lock"; [pipeline_fence.md](pipeline_fence.md)). When the owner
is stopped on purpose (SIGTERM, `docker stop`, a rolling restart) it now
hands the pipeline over: it stops taking deliveries, finishes or releases
what is in flight without spending an attempt, checkpoints, closes its
pipeline pool and only then unlocks the lock on its own session. The
standby's next try takes the lock, its fence finds nothing to wait for,
and it runs about one lock retry (`RETRY`, 2 s) after the release
(INV-1242 `transport.pipeline-lock.planned-handover-clean`).

## Scope

- The order of a Postgres-mode shutdown's pipeline part, as types
  (`gateway::postgres::handover`): `Handover<Quiescing>` ─▶ `<Quiesced>`
  ─▶ `<HoldsReleased>` ─▶ `<PoolClosed>` ─▶ `HandoverReport`, each step
  bounded, and the proof (`PoolsClosed`) the lock's explicit release
  takes.
- Quiescing a live process on Postgres (`Live::<PgSet>::quiesce`): no
  delivery taken, each stage finishing its delivery, L5's last checkpoint
  and acks, every task joined.
- Releasing what is still held (`PgBus::release_held`): ready at once, the
  attempt given back.
- Unlocking the lock explicitly (`PipelineLock::release`).
- The pump serving acks after its subscription ended, and L5 answering
  L4's last inputs before it ends.
- `/readyz` and `/healthz` during the handover.

## Non-scope

- The proxy and capture's shutdown: unchanged. The proxy stops accepting
  and drains in-flight streams (`shutdown.drain_timeout_ms`), the capture
  stage drains its channel into the spool or the bus, and with the
  capture gate open (INV-1241) captures publish directly. The SIGTERM
  restart gap at the proxy (connections refused while the process stops
  and starts) is unchanged.
- Draining the bus's backlog before stopping: the bus is durable and the
  next holder takes it. Memory mode still drains (`Live::shutdown`), as do
  harnesses that call `Live::shutdown` on Postgres.
- A lost lock (abandon, nothing drained: INV-1238), a killed owner (the
  server ends its sessions, the fence finds nothing to wait for), and a
  partitioned owner: unchanged.
- Waking the standby at once (a `NOTIFY` on release): the handover gap is
  the standby's retry, which the task does not shorten.
- The `api` role (it holds no lock) and memory mode.

## Before (measured from the code at b482993)

- SIGTERM: `/readyz` 503 `draining`; the proxy drained (up to 45 s by
  default), the API stopped, capture drained, then the pipeline task
  handed back `(Live, PipelineLock)` and `Live::shutdown` **drained the
  stages until every pipeline group was empty**, sending `Drain` (L5
  checkpoint and ack) between reads. With another gateway capturing into
  the same bus that is a moving target, cut only at the flush deadline
  (10 s by default), while the owner kept the lock.
- Then `PgBus::shutdown` (every `next` returns `None`; the listener and
  **the reaper aborted**), the stages joined (those past the deadline
  aborted), then **`drop(lock)`**: the lock's holder task aborted and its
  connection dropped unclosed (no unlock; the server released the lock on
  the socket's EOF), and only then the incarnation's pool closed
  (`CLOSE_BOUND`, 1 s). The status kept reading `held` / `running`.
- The standby retries the lock every `RETRY` (2 s, after the database
  probe and the migrations' head check), so it took the lock within about
  2 s of the drop, and its fence then waited for the old pool's
  connections, closed a moment later (up to `CLOSE_BOUND`).
- In-flight work: deliveries still held when the stages stopped (a stage
  aborted at the deadline, a take committed as its subscription dropped)
  stayed `held`: the reaper, already aborted, never handled the dropped
  subscriptions' holds. They were not left to their ack deadline: the
  standby's `recover_held` readied them at once, but **with their attempt
  counted**, so each planned restart spent an attempt on them and
  dead-lettered those on a group's last one. **L5's last acks failed**:
  after the bus stopped, its loop's final checkpoint was taken, but the
  acks went through a pump that had already ended with its subscription
  (`Disconnected`), so every delivery that checkpoint covered was
  redelivered and handled again by the next holder. **An L4 delta whose
  inputs L5 never answered was nacked** (`NotDurable::Stopped` is a retry:
  an attempt spent).

## Data and control flow

```text
SIGTERM ─▶ Running::shutdown: phase draining (/readyz 503)
  ─▶ proxy stops accepting, in-flight streams drain (shutdown.drain_timeout_ms)
  ─▶ API listener stops
  ─▶ PgRunning::shutdown(deadline = now + shutdown.flush_timeout_ms):
       capture stage drains its channel (into the spool or, gate open, the bus)
       pipeline task told to stop ─▶ (Live<PgSet>, PipelineLock) when an incarnation runs
       hand_over(OutgoingIncarnation{live, bus, pool, lock}, status, quiesce_by = deadline - TAIL):
         Handover<Quiescing>      status pipeline handing_over, handover quiescing
           Live::quiesce(quiesce_by), backstop QUIESCE_BACKSTOP:
             ticker aborted ─▶ PgBus::shutdown (every next: None; listener, reaper aborted)
             ─▶ each stage finishes the delivery it handles, acks it, pulls: None, ends
                L4's last delta: L5 answers its inputs until L4 ends (its sender dropped)
                L5: its last checkpoint, then acks through the pump (still serving acks)
             ─▶ exchange log: its last append and ack, then None, ends
             (each joined by quiesce_by; one still running is aborted: what it held is released)
             ─▶ L7's outbox relay and the retention prune aborted, joined
             ─▶ PgBus::abandon: every take in flight aborted and joined (no hold commits after)
             ─▶ the surface's relay and live feed stopped
         Handover<Quiesced>       handover releasing_holds
           PgBus::release_held within RELEASE_HOLDS_BOUND (on the incarnation's pool, under the
           lock): UPDATE deliveries SET state ready, available_at NULL, attempt - 1,
           last_error "released at a planned handover" WHERE state held
         Handover<HoldsReleased>  handover closing_pool
           the incarnation's pool closed within CLOSE_BOUND: its connections leave the fence
         Handover<PoolClosed>     handover releasing_lock
           PipelineLock::release(PoolsClosed) within UNLOCK_BOUND: the holder task, between two
           pings, runs pg_advisory_unlock(PIPELINE_LOCK) (within PING_TIMEOUT), closes the session
           (refused, failed or late: the session dropped, which releases the lock)
         HandoverReport           pipeline_lock released, pipeline handed_over, handover done
       ─▶ API surface task stopped ─▶ spool closed (records stay on disk) ─▶ capture's bus,
          capture's and the template pool closed
  ─▶ store probe closed ─▶ ops listener stopped (it answers until here)

standby (meanwhile): held elsewhere / waiting_for_lock ─ every RETRY: probe, head check, take
  ─▶ the lock free: bump the epoch, fence (no connection of the old epoch left: one poll)
  ─▶ recovery (recover_held finds nothing held) ─▶ running
```

`Handover<Quiescing>::begin` reports the first step; each later method
consumes the handover and returns the next, reporting its step. Only
`Handover<PoolClosed>::release_lock` constructs `PoolsClosed`, whose
field is private to the module, and `PipelineLock::release` takes one:
releasing the lock before the quiesce (and so L5's last checkpoint), the
release of the holds and the pool's close does not compile.

**Bounds.** The quiesce ends at `quiesce_by` (its joins abort what runs
past it), and `QUIESCE_BACKSTOP` (0.5 s) later at the latest; the release
within `RELEASE_HOLDS_BOUND` (2 s), the close within `CLOSE_BOUND` (1 s),
the unlock within `UNLOCK_BOUND` (2 s). `TAIL` is their sum (5.5 s);
`quiesce_by` is the flush deadline minus `TAIL` (at least now), so the
handover ends by the flush deadline and `Running::shutdown_bound` (drain +
flush + 10 s) is unchanged. A step past its bound is logged and the next
step runs: a quiesce cut at the backstop leaves its tasks detached (they
keep their connections, and so the fence, until they end); holds not
released are returned by the next holder's `recover_held` (attempt
counted, the old behaviour); a pool not closed keeps its open connections'
shared fence, which the standby's take waits for (INV-1239); a lock not
unlocked goes with its session.

## Status, logs and metrics

| Where | During the handover | After |
| --- | --- | --- |
| `/readyz` (503, `status: draining`) | `pipeline_lock: held`, `pipeline: handing_over`, `handover: quiescing` / `releasing_holds` / `closing_pool` / `releasing_lock`, `recovery: done` | `pipeline_lock: released`, `pipeline: handed_over`, `handover: done` |
| `/healthz` (`status: draining`) | `recovery.handover`: the step | `recovery.handover: done` |

`PipelineStatus::blocks_readiness` is true while handing over, so a
process whose phase did not read `draining` would still be unready.
Logs: `pipeline handover step` (`step`) at each step; `live process
quiesced: every stage finished its delivery` (`stages_finished`,
`takes_aborted`) or its warn naming what the deadline cut; `held
deliveries released for a handover` (`released`); a warn per step past its
bound; `pipeline handed over; the lock is free` (`clean`, `took_ms`,
`stages_finished`, `stages_aborted`, `released`, `pool_closed`,
`lock_unlocked`). `ShutdownReport::handover` carries the
`HandoverReport`; `stages_drained` and `log_drained` mean, in Postgres
mode, that no stage and no exchange-log task was cut short. No metric:
the process exits.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/gateway/src/gateway/postgres/handover/mod.rs` | the handover: steps as types, bounds, the proof the lock's release takes, the real outgoing incarnation | `hand_over`, `Handover`, `Quiescing`, `Quiesced`, `HoldsReleased`, `PoolClosed`, `PoolsClosed`, `Outgoing`, `OutgoingIncarnation`, `HandoverReport`, `Bounded`, `QUIESCE_BACKSTOP`, `RELEASE_HOLDS_BOUND`, `UNLOCK_BOUND`, `TAIL` |
| `crates/gateway/src/gateway/postgres/handover/tests.rs` | the order and the bounds over a scripted incarnation on a paused clock | tests |
| `crates/gateway/src/live/pg/quiesce.rs` | a Postgres live process stopping on purpose | `Live::<PgSet>::quiesce`, `Quiesced`, `QuiesceReport` |
| `crates/gateway/src/gateway/postgres/mod.rs` | the shutdown that runs the handover; the lock retry the standby waits | `PgRunning::shutdown`, `PgStopped`, `RETRY` |
| `crates/gateway/src/gateway/mod.rs` | the gateway's shutdown report | `ShutdownReport::handover` |
| `crates/gateway/src/store/lock.rs` | the explicit unlock, on the holder's task | `PipelineLock::release`, `ReleaseError` |
| `crates/transport/src/pg/mod.rs` | the release of every hold, attempt given back | `PgBus::release_held`, `HANDOVER_REASON` |
| `crates/gateway/src/live/pg/pump.rs` | acks, nacks and releases served after the subscription ended | `pump`, `Pumped` |
| `crates/gateway/src/live/pg/l5.rs` | L5 answers L4's last inputs, then checkpoints and acks once | - |
| `crates/gateway/src/live/recovery.rs` | the status | `HandoverStep`, `PipelinePhase::HandingOver`, `LockState::Released`, `PipelineStatus::handover`, `RecoveryReport::handover` |
| `crates/gateway/src/ops/mod.rs`, `crates/gateway/src/ops/handover_tests.rs` | `/readyz`'s `handover`; tests | `Readiness::handover` |
| `crates/transport/src/integration/handover.rs` | Postgres: released holds ready at once on the same attempt | tests |
| `crates/e2e/tests/lock_loss/handover.rs` | Postgres, two gateways: the owner shut down with traffic flowing, the standby taking over | tests |

## Tests

- No database (run): `gateway::postgres::handover::tests::{the_steps_run_in_order_each_reported_while_it_runs,
  a_stalled_quiesce_still_hands_over_within_the_bound,
  each_stalled_step_is_cut_at_its_bound,
  every_step_stalled_ends_by_the_deadline_and_the_tail,
  the_tail_fits_inside_the_default_flush_timeout}`,
  `live::pg::pump::tests::acks_after_the_subscription_ended_still_land`,
  `live::recovery::tests::a_handover_is_not_ready_and_names_its_step`,
  `ops::handover_tests::readyz_and_healthz_name_the_handover_step`.
- Postgres (written; node0 runs pending): `crosstalk-transport`
  `integration::handover::{pg_released_holds_are_ready_at_once_on_the_same_attempt,
  pg_a_release_with_nothing_held_releases_nothing}`; `crosstalk-e2e`
  `lock_loss::handover::a_planned_shutdown_hands_the_pipeline_to_the_standby_cleanly`
  (two gateways, one exchange in the owner's pipeline as it stops and one
  captured by the standby during the handover; the handover clean; the
  standby holding the lock within `RETRY` + 2 s of the release and running
  within `RETRY` + 6 s; no session of an older epoch then; nothing for its
  `recover_held`; every delivery the owner left or released taken again on
  the same attempt; 0 dead letters; no `late contribution`; each capture
  once in the bus log and once across the two exchange logs; the
  uninterrupted run's answers).

## Invariants and constraints

- INV-1242 `transport.pipeline-lock.planned-handover-clean`: a planned
  stop runs quiesce, release the holds, close the pool, unlock, in that
  order, each bounded; the lock's release needs the `PoolsClosed` proof
  only the last step makes; released deliveries are ready at once on the
  same attempt and never dead-lettered for it.
- `release_held` runs only after every take is joined and before the
  pool closes, under the lock: every `held` row is this holder's (the
  same assumption `recover_held` makes).
- The handover ends by the flush deadline: `TAIL` < `flush_timeout_ms`
  (the default leaves the quiesce 4.5 s).
- Whatever a cut step leaves, the fence covers (INV-1239); the handover
  never terminates another session.
- The quiesce waits for no backlog: the bus is durable.
