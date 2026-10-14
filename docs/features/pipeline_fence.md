# The pipeline fence: no write from an older epoch

One pipeline process per database is the pipeline lock's job
(`store::lock`, [postgres_mode.md](postgres_mode.md), "Losing the pipeline
lock"). The lock alone does not exclude two writers: when the server ends
the lock's session while the database stays reachable
(`pg_terminate_backend`, a backend crash: `LockLoss::PingFailed`), the old
holder learns of it only at its next ping, up to `PING` (5 s) later, and
its incarnation's pool still has live connections meanwhile; another
gateway (a standby) may take the lock at once. The fence closes that
window (INV-1239 `transport.pipeline-lock.no-write-from-an-older-epoch`),
so running a standby gateway against the same database is safe.

## Scope

- The pipeline epoch (`transport.pipeline_epoch`, transport migration 3)
  and its bump by each taking of the lock.
- The second advisory key `PIPELINE_FENCE`, joined shared by every
  connection of a pipeline incarnation and taken exclusively, briefly, by
  each new holder after its bump.
- The new holder's bounded, observable, cancel-safe wait
  (`fence::wait`), its status (`fencing`), logs and metric, and a lock
  session failing during it (a lock loss).
- The incarnation pools that join the fence (`incarnation_pool`) and the
  shared session setup they compose with.

## Non-scope

- Detecting a lost lock and abandoning the incarnation: unchanged
  (INV-1236, INV-1238; [postgres_mode.md](postgres_mode.md)).
- Fencing writes that are not the holder's own: capture's appends to the
  log, and envelopes an abandoned incarnation put in the spool before its
  abort, which reach the log through capture's bus (consumers take them
  as redeliveries).
- Liveness when an old owner is stopped with its connections open: the
  wait never gives up (see Failure modes).
- Fencing the `api` role or `crosstalk migrate`: neither runs the
  pipeline.

## Data and control flow

Two things in the database:

- the **epoch**, `transport.pipeline_epoch` (one row, `singleton boolean
  PRIMARY KEY CHECK (singleton)`, `epoch bigint CHECK (epoch >= 0)`,
  seeded 0 by `crates/transport/migrations/0003_pipeline_epoch.sql`): how
  many times the lock was taken;
- a second advisory key, `store::lock::PIPELINE_FENCE` (`xtalkfnc` in
  ASCII, distinct from `PIPELINE_LOCK`), held **shared** by every
  connection of a pipeline incarnation for its whole life and taken
  **exclusively**, briefly, by each new holder of the lock.

```text
PipelineLock::try_take_reporting(url, fencing), on the lock's own session:
  within TAKE_TIMEOUT (10 s): connect ─▶ pg_try_advisory_lock(PIPELINE_LOCK) (false: None)
                             ─▶ UPDATE transport.pipeline_epoch SET epoch = epoch + 1 RETURNING epoch
                                (autocommit: committed before the next step) = e
  fencing(e): status pipeline_lock held, pipeline fencing, recovery.epoch = e
  fence::wait::wait_for_fence (not counted against TAKE_TIMEOUT, never gives up):
     every FENCE_POLL (250 ms): pg_try_advisory_lock(PIPELINE_FENCE), bounded by PING_TIMEOUT
        granted ─▶ pg_advisory_unlock(PIPELINE_FENCE) ─▶ the lock is handed out with Epoch e
        refused ─▶ at most every FENCE_REPORT (5 s): the holders' application_names from
                   pg_locks ⋈ pg_stat_activity, logged (warn: epoch, waited_ms,
                   blocking_sessions, blockers)
        a statement failing or not answering ─▶ TakeError::Lost(LockLoss): a loss like any other
  then the holder task pings the session as before (PipelineLock::lost)

incarnation pool (gateway::postgres::incarnation_pool(template, max, application, e)),
after_connect of every new connection:
  store::session_setup (idle_in_transaction_session_timeout = 30s)
  ─▶ fence::join: pg_try_advisory_lock_shared(PIPELINE_FENCE), then SELECT epoch
     shared refused ─▶ FenceError::Fencing { mine, current }
     epoch != e     ─▶ FenceError::Fenced { mine, current }
     (refused: logged warn, returned as an I/O error; sqlx closes the connection, which
      releases a shared lock it took, and retries until the acquire timeout: PoolTimedOut)
```

## Why the order makes it safe

The new holder commits the bump before it first tries the exclusive fence, and a connection takes its shared
fence before it reads the epoch. Take any connection of an older
incarnation, in any process. Either it took its shared fence before the
new holder's exclusive try was granted: the exclusive cannot be granted
while it is held, so the new holder waits until that connection closes.
Or it took it after the exclusive was released: then it reads the epoch
after the bump committed, sees a newer one than its incarnation's and
refuses itself. A shared try while the exclusive is held is refused
outright. So when the take returns, no connection of an older epoch exists
and none can be made: the new incarnation starts with no older writer
left, and the older incarnation's pool can never hand out a connection
again (its connects fail until its process notices the loss and abandons
it).

## Who joins the fence

Only the pipeline incarnations' pools (`incarnation_pool`). The lock's session, the process's template pool,
capture's publisher pool (`derived_pool`, `crosstalk-capture-<pid>`) and
the `api` role's pool never take the shared fence: capture is never
fenced (INV-1237), and none of them writes what only the lock's holder
writes. `PgPoolOptions::after_connect` replaces any earlier hook, so both
`derived_pool` and `incarnation_pool` set theirs explicitly over the
shared `store::session_setup`. What an abandoned incarnation's stage put
in the spool before its abort still reaches the log through capture's
bus: log appends, which every consumer already tolerates as
redeliveries; the fence covers the holder's own tables.

## Status, logs and metrics

While the take waits, `/readyz` reads `pipeline_lock: held`,
`pipeline: fencing`, `recovery: pending`, 200 `degraded` (as recovering
is), and `/healthz`'s `recovery.epoch` the new epoch. A standby taking
over walks `held elsewhere / waiting_for_lock` (503: it never held the
lock) ─▶ `held / fencing` ─▶ `held / recovering` ─▶ `held / running`. The
old owner, once it notices the loss, walks `lost / abandoning` ─▶ `held
elsewhere / waiting_for_lock` (200 degraded: it held the lock before),
capturing throughout. Logs: the take's start (`pipeline lock taken;
fencing the earlier incarnations`, `epoch`), the wait's blockers (warn,
bounded), its end (`waited_ms`, `polls`), `pipeline lock taken; earlier
incarnations fenced` (`epoch`, `fence_waited_ms`, `fence_polls`) and the
incarnation's start (`incarnation`, `epoch`, `application_name`). Each
refused pipeline connect is a warn naming its `application_name`, epoch
and why. `/metrics`: `crosstalk_pipeline_epoch`.

## Failure modes

- The old owner's lock session terminated, the database reachable (the
  case this closes): the new holder waits until the old owner's next ping
  fails (≤ `PING`, 5 s) and its abandon closes the pool (≤
  `LOST_SHUTDOWN` plus `CLOSE_BOUND`), then starts.
- The old owner killed or its database sessions ended: the server drops
  its sessions with their locks; the fence finds nothing to wait for.
- The old owner partitioned: its partitioned sessions keep their shared
  fence until the server reaps them (about 25 s with deploy's
  `tcp_keepalives_*`, `SERVER_REAP`), as they keep the lock itself.
- The new holder's own lock session fails while it waits: `TakeError::Lost`,
  reported as a loss (`pipeline_lock: lost`, `lock_losses` + 1,
  `last_loss`), and the loop takes the lock again later; the bump stays,
  which only fences more.
- A stop during the wait drops the take: the session goes with it, and
  the lock.
- **Safety over liveness.** The wait never gives up and never proceeds
  without the fence. An old owner stopped with its connections open
  (`SIGSTOP`, a hung host whose TCP stays up) blocks the takeover until
  those connections die: the server's keepalives on a dead host, the
  process resuming and noticing its loss, or an operator's
  `pg_terminate_backend` on the sessions the warn names.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/transport/migrations/0003_pipeline_epoch.sql` | the one-row epoch table, seeded 0 | - |
| `crates/gateway/src/store/lock.rs` | the lock: its take bumps the epoch and fences before handing the lock out | `PIPELINE_LOCK`, `PIPELINE_FENCE`, `PipelineLock::{try_take, try_take_reporting, epoch, lost}`, `TakeError::{Lost, Epoch, FenceNotHeld}`, `TAKE_TIMEOUT` |
| `crates/gateway/src/store/fence/mod.rs` | the epoch, a connection joining the fence, the bump; the safety argument in its docs | `Epoch`, `EpochError`, `FenceError::{Fenced, Fencing}`, `join`, `read_epoch`, `FENCE_POLL`, `FENCE_REPORT` |
| `crates/gateway/src/store/fence/wait.rs` | the new holder's wait over a `FenceSession` (the lock's `PgConnection`; a scripted one in the tests) | `wait_for_fence`, `FenceSession`, `Fenced`, `FenceWaitError`, `Blocker` |
| `crates/gateway/src/store/fence/tests.rs` | the wait on a paused clock, the epoch | tests |
| `crates/gateway/src/store/mod.rs` | the session setup every pooled connection runs | `session_setup`, `lazy_pool_named` |
| `crates/gateway/src/gateway/postgres/mod.rs` | the pools: the incarnation's joins the fence, capture's never does | `incarnation_pool`, `derived_pool` |
| `crates/gateway/src/gateway/postgres/pipeline.rs` | the pipeline task: the take reported as `fencing`, a loss while fencing counted, the incarnation built under the lock's epoch | `application_name(number, epoch)` |
| `crates/gateway/src/live/recovery.rs` | the status | `PipelinePhase::Fencing`, `RecoveryReport::epoch` |
| `crates/gateway/src/ops/metrics.rs` | the gauge | `crosstalk_pipeline_epoch` |
| `crates/gateway/src/integration/fence.rs` | Postgres: refusal after a newer epoch, the wait and its end, the pipeline's `fencing`, the publisher pool untouched | tests |
| `crates/e2e/tests/lock_loss/standby.rs` | two gateways: a terminated lock session and a killed owner, the standby taking over with no older-epoch write | tests |

## Tests

- No database (run): `store::fence::tests::{the_wait_never_gives_up_and_completes_once_the_fence_is_free,
  a_free_fence_completes_at_once, a_failed_try_while_waiting_is_a_lock_loss,
  a_try_without_an_answer_is_a_lock_loss_within_the_ping_deadline,
  a_release_that_finds_no_fence_is_refused,
  the_wait_notices_a_lost_session_within_the_detection_bound,
  the_fence_key_is_distinct_and_positive,
  epochs_count_up_from_zero_and_never_go_negative}`,
  `live::recovery::tests::{fencing_is_ready_degraded_with_recovery_pending, the_report_carries_the_epoch}`,
  `ops::tests::the_recovery_report_carries_the_epoch_in_health_and_metrics`,
  `store::migrations::tests::transport_head_creates_the_pipeline_epoch`.
- Postgres (`crosstalk-gateway`): `integration::fence::{pg_a_fenced_incarnation_cannot_connect_after_a_newer_epoch,
  pg_a_take_waits_for_an_older_connection_and_completes_once_it_closes,
  pg_the_pipeline_reports_fencing_while_an_older_connection_lives,
  pg_the_publisher_pool_is_unaffected_by_the_fence}`.
- Postgres (`crosstalk-e2e`, target `lock_loss`): `standby::{a_standby_takes_over_a_terminated_lock_session_with_no_older_epoch_write,
  a_standby_takes_over_promptly_from_a_killed_owner}`
  ([postgres_mode.md](postgres_mode.md), "Losing the lock while the
  gateway runs").

## Invariants and constraints

- INV-1239 `transport.pipeline-lock.no-write-from-an-older-epoch`: once an
  incarnation has started under the current epoch, no session of an
  older epoch commits a pipeline write.
- The bump commits before the first exclusive try; a connection takes its
  shared fence before it reads the epoch. Changing either order breaks the
  safety argument.
- Only incarnation pools join the fence; the lock's session never holds it
  shared (it would wait for itself), and capture never joins (INV-1237).
- The wait never gives up and never proceeds without the fence; it is not
  counted against `TAKE_TIMEOUT`; each try is bounded by `PING_TIMEOUT`,
  and `FENCE_POLL + PING_TIMEOUT <= DETECTION`, so a lost session is
  noticed while fencing at least as fast as while holding.
- `PIPELINE_FENCE != PIPELINE_LOCK` (a compile-time assertion); both keys
  are positive, so `pg_locks` names them as `classid = key >> 32`,
  `objid = key & 0xFFFF_FFFF`, `objsubid = 1`.
- The epoch never decreases and is never negative (the table's check;
  `EpochError` on read).
- An incarnation's `application_name` is
  `crosstalk-pipeline-<pid>-<n>-e<epoch>`: unique across processes by its
  epoch.
