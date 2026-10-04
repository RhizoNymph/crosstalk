//! The driver: [`Sim::run`] runs one async scenario from one seed on a
//! current-thread tokio runtime with paused time; [`Sim::sweep`] and
//! [`sim_test()`] run it over many seeds and report the first failure with
//! its seed and step.
//!
//! Under paused time the runtime advances the clock to the next timer
//! whenever every task is idle, so a scenario sleeping for an hour finishes
//! at once, and the order tasks run in depends only on the program and the
//! seed. A scenario that deadlocks reaches [`SimConfig::time_limit`] and
//! fails as [`FailureCause::TimedOut`] instead of hanging.

use std::env::{self, VarError};
use std::fmt;
use std::future::Future;
use std::num::NonZeroU32;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crosstalk_spec::support::Timestamp;
use tokio::task::JoinHandle;

use crate::bus::FaultyBus;
use crate::clock::SimClock;
use crate::node::{Node, NodeHandle, panic_message};
use crate::plan::{BusFaults, StoreFaults, UpstreamFaults};
use crate::rng::{Seed, SimRng};
use crate::store::FaultyStore;
use crate::trace::{NodeName, Trace, TraceEvent, TraceHash, TraceRecord, TraceSink, Tracer};
use crate::upstream::UpstreamFaultInjector;

/// The environment variable that reruns exactly one seed.
pub const SEED_VAR: &str = "CROSSTALK_SIM_SEED";
/// The environment variable that sweeps seeds `0..k`.
pub const SEEDS_VAR: &str = "CROSSTALK_SIM_SEEDS";

/// How a run is set up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimConfig {
    /// Simulated time after which a scenario that has not finished fails as
    /// timed out (a deadlock, or a scenario that never ends).
    pub time_limit: Duration,
    /// How many seeds [`sim_test()`] sweeps when neither environment variable
    /// is set.
    pub default_seeds: NonZeroU32,
    /// The wall-clock time the run starts at, for [`SimCtx::clock`].
    pub epoch: Timestamp,
    /// How many of the last trace records a failure report shows.
    pub report_tail: usize,
}

/// 2026-01-01T00:00:00Z.
const DEFAULT_EPOCH: Timestamp = Timestamp::from_micros(1_767_225_600_000_000);

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            time_limit: Duration::from_secs(3600),
            default_seeds: NonZeroU32::MIN.saturating_add(15),
            epoch: DEFAULT_EPOCH,
            report_tail: 20,
        }
    }
}

/// A check the scenario made that did not hold. The driver adds the seed
/// and the step.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("check failed: {message}")]
pub struct CheckFailed {
    pub message: String,
}

impl CheckFailed {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Which seeds to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedSelection {
    /// Exactly this seed (`CROSSTALK_SIM_SEED=<n>`).
    One(Seed),
    /// Seeds `0..k` (`CROSSTALK_SIM_SEEDS=<k>`).
    Sweep(NonZeroU32),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SeedEnvError {
    #[error("{SEED_VAR} must be a u64, got {value:?}")]
    InvalidSeed { value: String },
    #[error("{SEEDS_VAR} must be a positive u32, got {value:?}")]
    InvalidSweep { value: String },
    #[error("set at most one of {SEED_VAR} and {SEEDS_VAR}")]
    Both,
    #[error("{var} is not valid unicode")]
    NotUnicode { var: &'static str },
}

impl SeedSelection {
    /// From [`SEED_VAR`] and [`SEEDS_VAR`]; `None` when neither is set.
    pub fn from_env() -> Result<Option<Self>, SeedEnvError> {
        let read = |var: &'static str| match env::var(var) {
            Ok(value) => Ok(Some(value)),
            Err(VarError::NotPresent) => Ok(None),
            Err(VarError::NotUnicode(_)) => Err(SeedEnvError::NotUnicode { var }),
        };
        Self::parse(read(SEED_VAR)?.as_deref(), read(SEEDS_VAR)?.as_deref())
    }

    /// The selection the two variables' values describe.
    pub fn parse(seed: Option<&str>, seeds: Option<&str>) -> Result<Option<Self>, SeedEnvError> {
        match (seed, seeds) {
            (Some(_), Some(_)) => Err(SeedEnvError::Both),
            (Some(value), None) => value
                .parse()
                .map(|seed| Some(Self::One(seed)))
                .map_err(|_| SeedEnvError::InvalidSeed {
                    value: value.to_owned(),
                }),
            (None, Some(value)) => value
                .trim()
                .parse::<NonZeroU32>()
                .map(|k| Some(Self::Sweep(k)))
                .map_err(|_| SeedEnvError::InvalidSweep {
                    value: value.to_owned(),
                }),
            (None, None) => Ok(None),
        }
    }

    pub fn seeds(self) -> impl Iterator<Item = Seed> {
        let (start, end) = match self {
            Self::One(seed) => (seed.get(), seed.get().saturating_add(1)),
            Self::Sweep(k) => (0, u64::from(k.get())),
        };
        (start..end).map(Seed::new)
    }
}

/// Why a run failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureCause {
    /// The scenario returned a failed check.
    Check(CheckFailed),
    /// The scenario panicked (a failed `assert!`, say).
    Panicked { message: String },
    /// A task spawned with [`SimCtx::spawn`] panicked.
    TaskPanicked { task: String, message: String },
    /// The scenario had not finished after this much simulated time.
    TimedOut { limit: Duration },
    /// The runtime could not be built.
    Runtime { reason: String },
}

impl fmt::Display for FailureCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Check(check) => write!(f, "{check}"),
            Self::Panicked { message } => write!(f, "panicked: {message}"),
            Self::TaskPanicked { task, message } => write!(f, "task {task} panicked: {message}"),
            Self::TimedOut { limit } => {
                write!(
                    f,
                    "not finished after {limit:?} of simulated time (deadlock?)"
                )
            }
            Self::Runtime { reason } => write!(f, "could not build the runtime: {reason}"),
        }
    }
}

/// A failed run: the seed that reproduces it, how far it got, and why.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub struct SimFailure {
    pub seed: Seed,
    /// How many trace records the run made before it failed.
    pub step: usize,
    /// The label of the last step the scenario marked.
    pub last_step: Option<String>,
    /// Simulated time of the last record.
    pub at: Duration,
    pub cause: FailureCause,
    /// The last records, numbered by step.
    pub tail: Vec<(usize, TraceRecord)>,
}

impl fmt::Display for SimFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "simulation failed with seed {} at step {}",
            self.seed, self.step
        )?;
        if let Some(label) = &self.last_step {
            write!(f, " (last step: {label})")?;
        }
        writeln!(f, " after {:?} of simulated time: {}", self.at, self.cause)?;
        writeln!(f, "rerun with {SEED_VAR}={}", self.seed)?;
        for (step, record) in &self.tail {
            writeln!(f, "  #{step} +{:?} {:?}", record.at, record.event)?;
        }
        Ok(())
    }
}

/// A finished run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimReport {
    pub seed: Seed,
    pub trace: Trace,
    /// Simulated time the scenario took.
    pub elapsed: Duration,
}

impl SimReport {
    pub fn trace_hash(&self) -> TraceHash {
        self.trace.hash()
    }
}

/// One run of a sweep, without its trace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunSummary {
    pub seed: Seed,
    pub trace_hash: TraceHash,
    pub steps: usize,
    pub elapsed: Duration,
}

/// What a scenario gets: the seed, random streams, the clock, the tracer,
/// and constructors for nodes and fault wrappers. Cheap to clone; clones
/// share the root random stream.
#[derive(Debug, Clone)]
pub struct SimCtx {
    seed: Seed,
    root: Arc<Mutex<SimRng>>,
    tracer: Tracer,
    clock: SimClock,
}

impl SimCtx {
    fn new(seed: Seed, config: &SimConfig, tracer: Tracer) -> Self {
        Self {
            seed,
            root: Arc::new(Mutex::new(SimRng::new(seed))),
            clock: SimClock::new(config.epoch, NodeName::new("sim"), tracer.clone()),
            tracer,
        }
    }

    pub fn seed(&self) -> Seed {
        self.seed
    }

    /// A fresh random stream forked from the root.
    pub fn rng(&self) -> SimRng {
        self.root
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .fork()
    }

    pub fn tracer(&self) -> &Tracer {
        &self.tracer
    }

    /// The run's wall clock (node `sim`), starting at
    /// [`SimConfig::epoch`]. Use [`SimClock::skewed`] for other nodes'.
    pub fn clock(&self) -> SimClock {
        self.clock.clone()
    }

    /// Marks a step; a failure report names the last one.
    pub fn step(&self, label: impl Into<String>) {
        self.tracer.step(label);
    }

    /// Records a note in the trace (and so in the determinism hash).
    pub fn note(&self, text: impl Into<String>) {
        self.tracer.note(text);
    }

    /// `Err` with `message()` unless `holds`.
    pub fn check(&self, holds: bool, message: impl FnOnce() -> String) -> Result<(), CheckFailed> {
        if holds {
            Ok(())
        } else {
            Err(CheckFailed::new(message()))
        }
    }

    pub fn node(&self, name: &str) -> Node {
        Node::new(name, self.tracer.clone())
    }

    /// A bus for `node`'s code that injects `faults` into `inner`.
    pub fn faulty_bus<B>(
        &self,
        inner: Arc<B>,
        faults: BusFaults,
        node: &NodeHandle,
    ) -> FaultyBus<B> {
        FaultyBus::new(inner, faults, self.rng(), node.clone())
    }

    /// A store for `node`'s code that injects `faults` into `inner`.
    pub fn faulty_store<S>(
        &self,
        inner: S,
        faults: StoreFaults,
        node: &NodeHandle,
    ) -> FaultyStore<S> {
        FaultyStore::new(inner, faults, self.rng(), node.clone())
    }

    /// Upstream faults for `node`'s fake upstream.
    pub fn upstream_faults(
        &self,
        faults: UpstreamFaults,
        node: &NodeHandle,
    ) -> UpstreamFaultInjector {
        UpstreamFaultInjector::new(faults, self.rng(), node.clone())
    }

    /// Spawns a task whose panic fails the run (recorded as
    /// [`TraceEvent::TaskPanicked`]), even if nothing joins it.
    pub fn spawn<T, F>(&self, label: &str, future: F) -> SimTask<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let inner = tokio::spawn(future);
        let abort = inner.abort_handle();
        let tracer = self.tracer.clone();
        let task = label.to_owned();
        let watcher = tokio::spawn(async move {
            match inner.await {
                Ok(output) => Ok(output),
                Err(error) if error.is_panic() => {
                    let message = panic_message(error.into_panic().as_ref());
                    tracer.record(TraceEvent::TaskPanicked {
                        task: task.clone(),
                        message: message.clone(),
                    });
                    Err(TaskFailed::Panicked { task, message })
                }
                Err(_) => Err(TaskFailed::Cancelled { task }),
            }
        });
        SimTask { watcher, abort }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TaskFailed {
    #[error("task {task} panicked: {message}")]
    Panicked { task: String, message: String },
    #[error("task {task} was cancelled")]
    Cancelled { task: String },
}

/// A task spawned with [`SimCtx::spawn`].
#[derive(Debug)]
pub struct SimTask<T> {
    watcher: JoinHandle<Result<T, TaskFailed>>,
    abort: tokio::task::AbortHandle,
}

impl<T> SimTask<T> {
    /// Cancels the task at its next await, as a crash would.
    pub fn abort(&self) {
        self.abort.abort();
    }

    pub async fn join(self) -> Result<T, TaskFailed> {
        match self.watcher.await {
            Ok(result) => result,
            // The watcher only awaits the task and never panics; it is
            // cancelled only with the runtime.
            Err(_) => Err(TaskFailed::Cancelled {
                task: "watcher".to_owned(),
            }),
        }
    }
}

/// How the scenario future ended, inside the runtime.
enum Ended {
    Finished(Result<(), CheckFailed>, Duration),
    TimedOut,
}

/// The simulation driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Sim;

impl Sim {
    /// Runs `scenario` once from `seed` with the default [`SimConfig`].
    pub fn run<S, Fut>(seed: Seed, scenario: S) -> Result<SimReport, Box<SimFailure>>
    where
        S: FnOnce(SimCtx) -> Fut,
        Fut: Future<Output = Result<(), CheckFailed>>,
    {
        Self::run_with(&SimConfig::default(), seed, scenario)
    }

    /// Runs `scenario` once from `seed`.
    pub fn run_with<S, Fut>(
        config: &SimConfig,
        seed: Seed,
        scenario: S,
    ) -> Result<SimReport, Box<SimFailure>>
    where
        S: FnOnce(SimCtx) -> Fut,
        Fut: Future<Output = Result<(), CheckFailed>>,
    {
        let (tx, mut sink) = TraceSink::new();
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                return Err(failure(
                    config,
                    seed,
                    &Trace::default(),
                    FailureCause::Runtime {
                        reason: error.to_string(),
                    },
                ));
            }
        };
        let limit = config.time_limit;
        let ended = catch_unwind(AssertUnwindSafe(|| {
            runtime.block_on(async move {
                let tracer = Tracer::start(tx);
                let ctx = SimCtx::new(seed, config, tracer.clone());
                match tokio::time::timeout(limit, scenario(ctx)).await {
                    Ok(result) => Ended::Finished(result, tracer.elapsed()),
                    Err(_elapsed) => Ended::TimedOut,
                }
            })
        }));
        // Stop every task the scenario left running before reading the
        // trace, so nothing records after the drain.
        drop(runtime);
        let trace = sink.drain();
        let elapsed = match ended {
            Err(payload) => {
                let message = panic_message(payload.as_ref());
                return Err(failure(
                    config,
                    seed,
                    &trace,
                    FailureCause::Panicked { message },
                ));
            }
            Ok(Ended::TimedOut) => {
                return Err(failure(
                    config,
                    seed,
                    &trace,
                    FailureCause::TimedOut { limit },
                ));
            }
            Ok(Ended::Finished(Err(check), _)) => {
                return Err(failure(config, seed, &trace, FailureCause::Check(check)));
            }
            Ok(Ended::Finished(Ok(()), elapsed)) => elapsed,
        };
        if let Some((task, message)) = trace.task_panic() {
            let cause = FailureCause::TaskPanicked {
                task: task.to_owned(),
                message: message.to_owned(),
            };
            return Err(failure(config, seed, &trace, cause));
        }
        Ok(SimReport {
            seed,
            trace,
            elapsed,
        })
    }

    /// Runs `scenario` once per selected seed, stopping at the first
    /// failure.
    pub fn sweep<S, Fut>(
        config: &SimConfig,
        seeds: SeedSelection,
        mut scenario: S,
    ) -> Result<Vec<RunSummary>, Box<SimFailure>>
    where
        S: FnMut(SimCtx) -> Fut,
        Fut: Future<Output = Result<(), CheckFailed>>,
    {
        seeds
            .seeds()
            .map(|seed| {
                let report = Self::run_with(config, seed, &mut scenario)?;
                Ok(RunSummary {
                    seed,
                    trace_hash: report.trace_hash(),
                    steps: report.trace.len(),
                    elapsed: report.elapsed,
                })
            })
            .collect()
    }
}

fn failure(config: &SimConfig, seed: Seed, trace: &Trace, cause: FailureCause) -> Box<SimFailure> {
    Box::new(SimFailure {
        seed,
        step: trace.len(),
        last_step: trace.last_step().map(str::to_owned),
        at: trace
            .records()
            .last()
            .map_or(Duration::ZERO, |record| record.at),
        cause,
        tail: trace
            .tail(config.report_tail)
            .map(|(step, record)| (step, record.clone()))
            .collect(),
    })
}

/// The body of a simulation test: sweeps the seeds the environment selects
/// ([`SEED_VAR`] or [`SEEDS_VAR`]), or `config.default_seeds` seeds, and
/// panics with the failing seed, its step and the rerun command.
pub fn sim_test<S, Fut>(name: &str, config: &SimConfig, scenario: S)
where
    S: FnMut(SimCtx) -> Fut,
    Fut: Future<Output = Result<(), CheckFailed>>,
{
    let seeds = match SeedSelection::from_env() {
        Ok(Some(seeds)) => seeds,
        Ok(None) => SeedSelection::Sweep(config.default_seeds),
        Err(error) => panic!("{name}: {error}"),
    };
    if let Err(failure) = Sim::sweep(config, seeds, scenario) {
        panic!("{name}: {failure}");
    }
}

/// Declares a `#[test]` that runs a scenario under [`sim_test()`].
///
/// ```
/// crosstalk_sim::sim_test! {
///     /// Paused time: an hour passes at once.
///     fn an_hour_passes(ctx) {
///         tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
///         ctx.check(true, || "unreachable".to_owned())
///     }
/// }
/// ```
///
/// With a config: `fn name(ctx) with config_expression => { .. }`. The body
/// is an async block's body evaluating to `Result<(), CheckFailed>`.
#[macro_export]
macro_rules! sim_test {
    ($(#[$meta:meta])* fn $name:ident($ctx:ident) with $config:expr => $body:block) => {
        $(#[$meta])*
        #[test]
        fn $name() {
            $crate::sim_test(
                stringify!($name),
                &$config,
                |$ctx: $crate::SimCtx| async move $body,
            );
        }
    };
    ($(#[$meta:meta])* fn $name:ident($ctx:ident) $body:block) => {
        $crate::sim_test! {
            $(#[$meta])* fn $name($ctx) with $crate::SimConfig::default() => $body
        }
    };
}
