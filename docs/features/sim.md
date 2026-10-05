# Simulation (`crosstalk-sim`)

The deterministic simulation kit every `dst` invariant is tested with
(roadmap P1.3). A scenario is an async function of a `SimCtx`; the driver
runs it from one seed on a current-thread tokio runtime with paused time,
and everything that varies between runs (task interleavings, injected
faults, clock readings, delivery orders) is a function of that seed. A
failure reports the seed and the step it reached, and
`CROSSTALK_SIM_SEED=<n>` reproduces it.

## Scope

- The driver: `Sim::run`, `Sim::run_with`, `Sim::sweep`, the `sim_test`
  function and the `sim_test!` macro, seed selection from the
  environment, and failure reports.
- Seeded randomness: `Seed`, `SimRng` (SplitMix64), and the checked values
  plans are written in (`Probability`, `DurationRange`).
- Simulated time: `SimClock`, the spec's `Clock` on paused time, with
  steps and per-node skew.
- Fault injection, typed by `FaultPlan`:
  - `FaultyBus<B: EventBus>`: delay, reorder, duplicate, drop then
    redeliver, crash on publish, crash before ack, configured per subject;
  - `FaultyStore<S>`: latency, failure before the call, failure after
    commit, crash after commit, around any store call;
  - `UpstreamFaultInjector`: unreachable, error status, truncation and
    stall, one draw per upstream exchange.
- Simulated nodes: crash reports and a supervisor that restarts a crashed
  node.
- The run's trace, its hash (the determinism check), and the kit's
  self-tests.
- In the spec: the `Clock` trait and `SystemClock`
  (`spec/types/support.rs`), and the two `canonical.clock.*` invariants.

## Non-scope

- Any layer's `dst` test. The kit is infrastructure; each layer writes its
  own tests at the paths its invariants name, with `crosstalk-sim` as a
  dev-dependency. No invariant's evidence is flipped here.
- The real bus (`crosstalk-transport`, P2.1) and the reference stores
  (`crosstalk-memory`, P2.3). The self-tests use a toy bus written in the
  test module.
- Serving HTTP. The fake upstream is `crosstalk-testkit`'s (P1.4); it asks
  `UpstreamFaultInjector` which fault to act out.
- Simulated networks, partitions and multi-process runs (see D4).
- Controlling tokio's own randomness (`select!` branch order); see the
  constraints below.

## Decisions

### D4: tokio paused time plus our own fault layer, not turmoil

turmoil (tokio-rs, 0.7.2, released 2026-04-24, still pre-1.0) simulates
hosts and a network between them: per-host clocks, TCP and UDP through a
simulated fabric, partitions, latency and message loss on links. Its value
is the network. crosstalk's components meet in process: the single-node
gateway connects layers through the in-process bus and hands them stores
as trait objects, so the faults that matter (redelivery, reordering within
a consumer group, lost acks, late events, a store call whose commit the
caller never learns of, a crash between two steps) live at the `EventBus`,
`Subscription` and store trait boundaries, not on a socket. A layer under
test never opens a connection, so turmoil would add a dependency and a
second scheduler for a network nothing crosses, while every fault would
still have to be written as a trait wrapper.

What we use instead:

- **Time and scheduling:** a current-thread tokio runtime built with
  `start_paused(true)` (tokio's `test-util`). Time moves only when every
  task is idle, then jumps to the next timer, so an hour of simulated time
  costs nothing and the order tasks run in depends only on the program.
- **Faults:** wrappers around the spec traits, driven by a seeded RNG.

If a multi-node bus (JetStream) ever needs DST across a real network,
turmoil can be added for that crate alone; nothing here would change.

### The `Clock` lives in the spec

Production code takes the clock (the proxy stamps stage times, the
correlator ticks, leases lapse, retention runs), and layer crates may
depend on `crosstalk-sim` only as a dev-dependency, so the trait cannot
live here. `crosstalk_spec::support::Clock` is one method,
`fn now(&self) -> Timestamp`, with `Send + Sync` supertraits so a clock is
shared across tasks; `SystemClock` reads the operating system's clock
(before the epoch reads as the epoch, past `u64::MAX` microseconds as the
largest `Timestamp`). A closure (`impl Fn() -> Timestamp`) was the
alternative; a named trait documents the monotonicity rule in one place
and can be named in invariants.

Wall time is not monotonic. Elapsed time comes from `tokio::time::Instant`,
which paused time virtualizes, so a component never needs a second clock
abstraction: it takes one `Clock` reading and adds elapsed time. Two
invariants (INV-796, INV-797) state this:

- `canonical.clock.injected` (`lint`): implementation code reads wall time
  only through a `Clock` it was handed and elapsed time only through
  `tokio::time::Instant`; only `SystemClock` calls `SystemTime::now`.
- `canonical.clock.elapsed-from-monotonic` (`dst`): no component derives
  an elapsed time, a deadline or the order of its own instants from two
  `Clock::now` readings.

### Randomness: SplitMix64, written here

No RNG crate. SplitMix64 is a dozen lines, has 64 bits of state and a full
period, is identical on every platform, and matches its published test
vectors (`splitmix_matches_reference_values`). It is not for cryptography.

## Data and control flow

```text
sim_test!/sim_test ── SeedSelection (env or config.default_seeds)
   └─ Sim::sweep ── for each seed ─▶ Sim::run_with(config, seed, scenario)
        ├─ build current-thread runtime, start_paused(true)
        ├─ trace channel (unbounded mpsc): TraceSink ◀── Tracer clones
        ├─ block_on, inside catch_unwind:
        │    Tracer::start (stamps simulated time)
        │    SimCtx { seed, root SimRng(seed), tracer, SimClock("sim") }
        │    tokio::time::timeout(config.time_limit, scenario(ctx))
        ├─ drop the runtime (stops leftover tasks), drain the trace
        └─ Ok(SimReport { seed, trace, elapsed })
           or Err(Box<SimFailure { seed, step, last_step, at, cause, tail }>)
```

**Randomness.** `SimCtx` holds the root `SimRng` behind a mutex. Every
`ctx.rng()`, `ctx.faulty_bus`, `ctx.faulty_store` and
`ctx.upstream_faults` forks a stream from it, so the streams a scenario
uses are a function of the seed and the order it builds them in. A
`FaultyBus` forks a further stream for each subscription.

**Bus faults.** `FaultyBus::publish` draws crash-on-publish and duplicate
for the envelope's subject. On crash the node crashes before the inner
publish; otherwise it publishes, and on duplicate publishes the same
envelope again after the first returned `Ok` (a publisher retry after a
lost acknowledgement). `FaultySubscription::next`:

1. hands out a delivery held back by an earlier delay once its deadline
   passes (kept in the wrapper, so a cancelled `next` loses nothing);
2. pulls a delivery into the window from the inner subscription;
3. while the newest delivery's subject has a reorder fault that fires and
   the window is below its size, pulls another under
   `timeout(reorder.wait)`;
4. takes a uniformly chosen delivery out of the window (a
   `BusReorder` fault when it is not the oldest);
5. drop: forgets it (`Redelivery::AckTimeout`) or nacks it with a drawn
   `retry_after` (`Redelivery::Nack`, reason `DROPPED_DELIVERY_REASON`),
   and loops;
6. delay: holds it until a drawn deadline;
7. records it as outstanding (its subject and envelope id) and returns it.

`ack` draws crash-before-ack for the outstanding delivery's subject: on
crash the node crashes and the ack never reaches the inner bus, which
redelivers after its ack timeout. `nack` forwards.

**Store faults.** `FaultyStore::call(op, injected, call)` draws everything
at once: latency (sleep, then run), then at most one of `fail_before` (the
call's future is dropped unpolled, `Err(injected(FailBefore))`),
`crash_after` (the call ran and succeeded, then the node crashes) and
`fail_after` (the call ran and succeeded, `Err(injected(FailAfter))`). A
call that itself fails is returned as is.

**Crashes.** A crash records the fault, sends a `FaultEvent` on the node's
crash channel, and never completes. `Node::supervise(restarts, factory)`
spawns `factory(Incarnation(0))` as the node's root task and selects
(biased) between its completion and the crash channel; on a crash it
aborts the task (dropping its subscriptions, so the bus redelivers what it
held), records `TraceEvent::Restart`, and spawns the next incarnation, up
to `restarts` times.

**Clock.** `SimClock::now` is `epoch + elapsed + offset`: the run's epoch
(`SimConfig::epoch`, 2026-01-01T00:00:00Z by default), simulated time since
the clock was made, and the steps and skew applied to it, clamped to the
`Timestamp` range. `step` records a `ClockStep` fault. Timers and
`tokio::time::Instant` never step.

**Trace and determinism.** Every fault, step, note, restart and task panic
goes into the trace stamped with simulated time. `Trace::hash` is FNV-1a
over the records' debug forms; two runs of one seed give equal traces
(`same_seed_gives_the_same_trace`).

## API surface

Everything is re-exported at the crate root (`crosstalk_sim::*`).

### Driver (`driver.rs`)

| Item | Signature / shape |
| --- | --- |
| `Sim` | `Sim::run(seed: Seed, scenario: S) -> Result<SimReport, Box<SimFailure>>` where `S: FnOnce(SimCtx) -> Fut, Fut: Future<Output = Result<(), CheckFailed>>` |
| | `Sim::run_with(config: &SimConfig, seed, scenario)`, same bounds |
| | `Sim::sweep(config: &SimConfig, seeds: SeedSelection, scenario: S) -> Result<Vec<RunSummary>, Box<SimFailure>>` where `S: FnMut(SimCtx) -> Fut`; stops at the first failure |
| `sim_test` | `fn sim_test(name: &str, config: &SimConfig, scenario: S)` (`FnMut`); sweeps the env's seeds or `config.default_seeds`, panics with the failure report |
| `sim_test!` | `sim_test! { /// docs\n fn name(ctx) { body } }` or `fn name(ctx) with <config expr> => { body }`; declares a `#[test]`; the body is an async block body returning `Result<(), CheckFailed>` |
| `SimConfig` | `{ time_limit: Duration (1 h), default_seeds: NonZeroU32 (16), epoch: Timestamp (2026-01-01Z), report_tail: usize (20) }`, `Default` |
| `SimCtx` (`Clone`) | `seed() -> Seed`; `rng() -> SimRng` (fresh fork); `tracer() -> &Tracer`; `clock() -> SimClock` (node `sim`); `step(label)`; `note(text)`; `check(holds: bool, message: impl FnOnce() -> String) -> Result<(), CheckFailed>`; `node(name: &str) -> Node`; `faulty_bus(inner: Arc<B>, faults: BusFaults, node: &NodeHandle) -> FaultyBus<B>`; `faulty_store(inner: S, faults: StoreFaults, node: &NodeHandle) -> FaultyStore<S>`; `upstream_faults(faults: UpstreamFaults, node: &NodeHandle) -> UpstreamFaultInjector`; `spawn(label: &str, future) -> SimTask<T>` (`Send + 'static`; a panic fails the run) |
| `SimTask<T>` | `abort(&self)`; `async join(self) -> Result<T, TaskFailed>` |
| `TaskFailed` | `Panicked { task, message }`, `Cancelled { task }` |
| `CheckFailed` | `{ message: String }`, `CheckFailed::new(impl Into<String>)` |
| `SimReport` | `{ seed, trace: Trace, elapsed: Duration }`, `trace_hash() -> TraceHash` |
| `RunSummary` | `{ seed, trace_hash, steps: usize, elapsed }` |
| `SimFailure` | `{ seed, step: usize, last_step: Option<String>, at: Duration, cause: FailureCause, tail: Vec<(usize, TraceRecord)> }`; `Display` names the seed, step, last step, cause, `rerun with CROSSTALK_SIM_SEED=<n>` and the tail |
| `FailureCause` | `Check(CheckFailed)`, `Panicked { message }`, `TaskPanicked { task, message }`, `TimedOut { limit }`, `Runtime { reason }` |
| `SeedSelection` | `One(Seed)`, `Sweep(NonZeroU32)` (seeds `0..k`); `from_env() -> Result<Option<Self>, SeedEnvError>`; `parse(seed: Option<&str>, seeds: Option<&str>)`; `seeds(self) -> impl Iterator<Item = Seed>` |
| `SeedEnvError` | `InvalidSeed { value }`, `InvalidSweep { value }` (also 0), `Both`, `NotUnicode { var }` |
| `SEED_VAR`, `SEEDS_VAR` | `"CROSSTALK_SIM_SEED"`, `"CROSSTALK_SIM_SEEDS"` |

### Randomness (`rng.rs`)

| Item | Signature / shape |
| --- | --- |
| `Seed` | `new(u64)`, `get()`, `Display` and `FromStr` as decimal |
| `SimRng` | `new(Seed)`; `next_u64()`; `below(NonZeroU64) -> u64` (unbiased); `index(len) -> Option<usize>`; `unit() -> f64` in `[0, 1)`; `chance(Probability) -> bool` (`NEVER`/`ALWAYS` draw nothing); `duration_in(DurationRange) -> Duration`; `pick(&[T]) -> Option<&T>`; `shuffle(&mut [T])`; `fork() -> SimRng` |
| `Probability` | `new(f64) -> Result<_, InvalidProbability>` (`[0, 1]`, not NaN); `percent(u8)`; `NEVER`, `ALWAYS`; `get()` |
| `DurationRange` | `new(min, max) -> Result<_, InvalidDurationRange>` (`min <= max`, inclusive); `exactly(d)`; `min()`, `max()` |

### Clock (`clock.rs`, and the spec)

| Item | Signature / shape |
| --- | --- |
| `crosstalk_spec::support::Clock` | `trait Clock: Send + Sync { fn now(&self) -> Timestamp; }` |
| `crosstalk_spec::support::SystemClock` | unit struct, `Clock` over `SystemTime` |
| `SimClock` (`Clone`, shares steps) | `impl Clock`; `step(ClockStep)`; `skewed(&self, node: &str, skew: ClockStep) -> SimClock` (independent from then on); `node() -> &NodeName` |
| `ClockStep` | `Forward(Duration)`, `Back(Duration)` |

### Plan (`plan.rs`)

| Item | Shape |
| --- | --- |
| `FaultPlan` | `{ bus: BusFaults, store: StoreFaults, upstream: UpstreamFaults }`; `none()`, `chaos()` (bus and store chaos, no crashes, no upstream faults) |
| `BusFaults` | `none()`, `uniform(SubjectFaults)`, `with_subject(self, Subject, SubjectFaults) -> Self`, `for_subject(Subject) -> &SubjectFaults` |
| `SubjectFaults` | `{ delay: Option<Timed>, reorder: Option<Reorder>, duplicate: Probability, drop: Option<DropFault>, crash_on_publish: Probability, crash_before_ack: Probability }`; `none()`, `chaos()` (delay 20% 1–50 ms, reorder 30% window 4 wait 5 ms, duplicate 10%, drop 10% nack 10–100 ms) |
| `Timed` | `{ chance: Probability, within: DurationRange }`, `new` |
| `Reorder` | `new(chance, window: usize, wait: Duration) -> Result<_, InvalidReorder>` (window at least 2, wait non-zero); `chance()`, `window()`, `wait()` |
| `DropFault` | `{ chance, redelivery: Redelivery }`; `Redelivery::AckTimeout` or `Redelivery::Nack { after: DurationRange }` |
| `StoreFaults` | `{ latency: Option<Timed>, fail_before, fail_after, crash_after: Probability }`; `none()`, `chaos()` (latency 30% 1–20 ms, 5% each failure, no crash) |
| `UpstreamFaults` | `{ unreachable: Probability, error_status: Option<StatusFault>, truncate: Option<TruncateFault>, stall: Option<Timed> }`; `none()` |
| `StatusFault` | `{ chance, statuses: NonEmpty<ErrorStatus> }`; `ErrorStatus::new(u16)` accepts 300–599 |
| `TruncateFault` | `{ chance, max_chunks: u32 }` |

### Fault wrappers

| Item | Signature / shape |
| --- | --- |
| `FaultyBus<B>` (`Clone`) | `new(inner: Arc<B>, faults, rng, node: NodeHandle)`; `inner() -> &Arc<B>`; `impl EventBus` where `B: EventBus + Send + Sync + 'static`, `Subscription = FaultySubscription<B::Subscription>` |
| `FaultySubscription<S>` | `impl Subscription` where `S: Subscription + Send`; `inner() -> &S` |
| `DROPPED_DELIVERY_REASON` | the nack reason of a dropped delivery |
| `FaultyStore<S>` (`Clone` if `S: Clone`) | `new(inner, faults, rng, node)`; `inner() -> &S`; `async call<T, E, Fut>(&self, op: &'static str, injected: impl FnOnce(InjectedFault) -> E, call: Fut) -> Result<T, E>`; `impl BlobStore` (failures are `BlobError::Unavailable`) and `impl DeadLetterStore` (failures are `BusError::Disconnected`) for `S` implementing them and `Sync` |
| `InjectedFault` | `{ kind: StoreFaultKind (FailBefore, FailAfter), op }`, `Display` |
| `UpstreamFaultInjector` | `new(faults, rng, node)`; `next_exchange(&self) -> Option<UpstreamFault>` |
| `UpstreamFault` | `Unreachable`, `Status(ErrorStatus)`, `Truncate { after_chunks: u32 }`, `Stall(Duration)`; `kind() -> FaultKind` |

### Nodes (`node.rs`)

| Item | Signature / shape |
| --- | --- |
| `Node` | `name()`, `handle() -> NodeHandle`, `async next_crash(&mut self) -> FaultEvent`, `async supervise(&mut self, restarts: u32, factory: F) -> Result<Fut::Output, SuperviseError>` where `F: FnMut(Incarnation) -> Fut, Fut: Future + Send + 'static, Fut::Output: Send + 'static` |
| `NodeHandle` (`Clone`) | `name()`, `tracer()`; given to fault wrappers |
| `Incarnation` | `Incarnation(pub u32)`, 0 for the first run |
| `SuperviseError` | `RestartBudgetExhausted { node, restarts }`, `Panicked { node, message }`, `Cancelled { node }` |

### Trace (`trace.rs`)

| Item | Shape |
| --- | --- |
| `Tracer` (`Clone`) | `elapsed()`, `record(TraceEvent)`, `step(label)`, `note(text)` |
| `TraceEvent` | `Step(String)`, `Note(String)`, `Fault(FaultEvent)`, `Restart { node, incarnation }`, `TaskPanicked { task, message }` |
| `TraceRecord` | `{ at: Duration, event }` |
| `FaultEvent` | `{ kind: FaultKind, node: NodeName, site: FaultSite }` |
| `FaultSite` | `Bus { subject, event: EventId }`, `Store { op }`, `Upstream`, `Clock` |
| `FaultKind` | `BusDelay`, `BusReorder`, `BusDuplicate`, `BusDrop`, `BusCrashOnPublish`, `BusCrashBeforeAck`, `StoreLatency`, `StoreFailBefore`, `StoreFailAfter`, `StoreCrashAfter`, `UpstreamUnreachable`, `UpstreamStatus`, `UpstreamTruncate`, `UpstreamStall`, `ClockStep`; `ALL`, `is_crash()` |
| `Trace` | `records()`, `len()`, `is_empty()`, `hash() -> TraceHash`, `faults()`, `count(FaultKind)`, `notes()`, `last_step()`, `task_panic()`, `tail(n)` |
| `TraceHash` | `TraceHash(pub u64)`, `Display` as 16 hex digits |
| `NodeName` | `new(&str)`, `as_str()`, `Display` |

## Writing a `dst` test

```rust
// crates/transport/src/dst.rs (cfg(test)), with crosstalk-sim as a dev-dependency
use std::sync::Arc;
use crosstalk_sim::{BusFaults, CheckFailed, SubjectFaults, sim_test};

sim_test! {
    fn every_published_envelope_reaches_every_group(ctx) {
        let bus = Arc::new(MpscBus::new(/* .. */));
        let publisher = ctx.node("publisher");
        let consumer = ctx.node("consumer");
        let chaos = BusFaults::uniform(SubjectFaults::chaos());
        let out = ctx.faulty_bus(Arc::clone(&bus), chaos.clone(), &publisher.handle());
        let inn = ctx.faulty_bus(Arc::clone(&bus), chaos, &consumer.handle());
        // subscribe through `inn`, publish through `out`, consume, then:
        ctx.check(all_seen, || format!("missing {missing:?}"))
    }
}
```

Patterns the invariants need:

- **Redelivery, duplicates, late events:** `SubjectFaults` `drop`,
  `duplicate`, `delay` on the consumer's or publisher's `FaultyBus`.
- **Any order within a group** (`transport.ordering.unconstrained`):
  `reorder`, plus several seeds; assert over the trace or notes.
- **Crash between two steps** (apply then publish, handle then ack, commit
  then announce): give the node's code a `FaultyBus` with
  `crash_on_publish` or `crash_before_ack`, or a `FaultyStore` with
  `crash_after`, run it under `node.supervise(restarts, |incarnation| ..)`,
  and check what the restarted incarnation does. `fail_after` gives the
  "committed but the caller saw an error" case without a restart.
- **Clock steps and skew** (stage times, ULIDs under repeating clocks):
  `ctx.clock()`, `SimClock::step(ClockStep::Back(..))`,
  `clock.skewed("node-b", ClockStep::Forward(..))`.
- **Concurrency** (racing merges, triage, enqueues): spawn the racers with
  `ctx.spawn`, give each a `ctx.rng()` stream to jitter with
  `tokio::time::sleep(rng.duration_in(..))`.
- **Another store trait:** a local wrapper type that delegates each method
  through `FaultyStore::call`, or an impl added to `store.rs` here.

## Files

| File | Role | Key exports |
| --- | --- | --- |
| `crates/sim/Cargo.toml` | Manifest: `crosstalk-spec`, `thiserror`, `tracing`, `tokio` (`macros`, `rt`, `sync`, `test-util`, `time`) | — |
| `crates/sim/src/lib.rs` | Crate doc, module list, re-exports | everything below |
| `crates/sim/src/rng.rs` | Seed, SplitMix64, checked plan values; `SimRng` is the spec's `RandomSource`, so a `UlidGenerator` draws ids from the run's seed | `Seed`, `SimRng`, `Probability`, `InvalidProbability`, `DurationRange`, `InvalidDurationRange` |
| `crates/sim/src/clock.rs` | Virtual wall clock | `SimClock`, `ClockStep` |
| `crates/sim/src/plan.rs` | The typed fault plan | `FaultPlan`, `BusFaults`, `SubjectFaults`, `Timed`, `Reorder`, `InvalidReorder`, `DropFault`, `Redelivery`, `StoreFaults`, `UpstreamFaults`, `StatusFault`, `TruncateFault`, `ErrorStatus`, `InvalidErrorStatus` |
| `crates/sim/src/bus.rs` | Bus fault wrapper | `FaultyBus`, `FaultySubscription`, `DROPPED_DELIVERY_REASON` |
| `crates/sim/src/store.rs` | Store fault wrapper, L2 store impls | `FaultyStore`, `InjectedFault`, `StoreFaultKind` |
| `crates/sim/src/upstream.rs` | Upstream fault draws | `UpstreamFaultInjector`, `UpstreamFault` |
| `crates/sim/src/node.rs` | Crash channel and supervisor | `Node`, `NodeHandle`, `Incarnation`, `SuperviseError` |
| `crates/sim/src/trace.rs` | Trace records, tracer, hash | `Tracer`, `Trace`, `TraceRecord`, `TraceEvent`, `FaultEvent`, `FaultKind`, `FaultSite`, `NodeName`, `TraceHash` |
| `crates/sim/src/driver.rs` | Runtime, context, failures, seeds, `sim_test` | `Sim`, `SimCtx`, `SimConfig`, `SimReport`, `RunSummary`, `SimFailure`, `FailureCause`, `CheckFailed`, `SimTask`, `TaskFailed`, `SeedSelection`, `SeedEnvError`, `SEED_VAR`, `SEEDS_VAR`, `sim_test`, `sim_test!` |
| `crates/sim/src/tests/toy_bus.rs` | A toy in-memory `EventBus` (groups, ack, nack, ack timeout) and envelope fixtures | test-only |
| `crates/sim/src/tests/{rng,clock,driver,bus,store,upstream,every_fault}.rs` | Self-tests: RNG vectors and bounds; clock steps, skew, saturation; determinism by trace hash, seed exploration, failure reports, seed selection, the macro; every bus, store and upstream fault; every `FaultKind` firing | test-only |
| `crates/sim/src/tests/ids.rs` | The spec's ULID generators under simulation: concurrent generators on skewed node clocks that step back, never minting an id twice (`canonical.ids.ulid-unique`, [spec_primitives](spec_primitives.md)) | test-only |
| `spec/types/support.rs` | `Clock`, `SystemClock` | |
| `spec/types/tests/support.rs` | `system_clock_reads_the_wall_clock`, `clock_is_shareable_across_tasks` | |
| `spec/invariants/INV-797-canonical.clock.injected.toml`, `INV-796-canonical.clock.elapsed-from-monotonic.toml` | The clock invariants | |

## Invariants and constraints

- **One seed, one run.** Every random choice the kit makes comes from a
  stream forked from the root `SimRng(seed)`. Same seed, same trace hash.
- **Every fault stays within what the real system may do:** at-least-once
  delivery, any order within a consumer group, redelivery after a lost
  delivery or ack, a store call that commits or does not.
- **Faults turned off draw nothing.** `Probability::NEVER` and `None`
  consume no randomness, so disabling one fault does not reshuffle the
  others.
- **Crashes never return.** A crashed call pends until its task is aborted;
  nothing after it runs.
- **A node owns its tasks.** `supervise` aborts only the root task; a node
  that spawns detached tasks must own them (await them or hold a
  `JoinSet`), or they survive its crash.
- **Cancel safety.** `FaultySubscription::next` is cancel-safe when the
  inner `Subscription::next` is (the reorder window pulls under a timeout
  and relies on it). The in-process bus should make `next` cancel-safe;
  the spec does not yet say so.
- **Deadlocks fail, not hang.** A scenario still running at
  `SimConfig::time_limit` of simulated time fails as `TimedOut`.
- **What the kit cannot make deterministic**, and code under DST must
  avoid: `tokio::select!` without `biased;` (tokio seeds its branch order
  randomly per runtime), iterating a `HashMap`/`HashSet` with the default
  hasher where the order is observable, `std::time::{SystemTime, Instant}`
  (`canonical.clock.injected`), `spawn_blocking`, threads, and real I/O.
- **Dev-dependency only.** `crosstalk-sim` turns on tokio's `test-util`;
  the architecture test keeps it out of every layer's normal
  dependencies.
- No `unwrap`/`expect` outside tests; lock poisoning is handled by taking
  the inner value (no lock is held across an await, and a panic that
  poisons one fails the run anyway).
