//! The event trace of a run: every injected fault, every step and note the
//! scenario records, restarts and task panics, each stamped with the
//! simulated time since the run began.
//!
//! Records travel over an unbounded channel from every [`Tracer`] clone to
//! the driver, which numbers them in arrival order once the run ends. Two
//! runs with the same seed produce the same records in the same order, so
//! the same [`TraceHash`]; that is the determinism check.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::events::Subject;
use crosstalk_spec::ids::EventId;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// The name of a simulated node: the unit a crash takes down and a
/// supervisor restarts.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeName(Arc<str>);

impl NodeName {
    pub fn new(name: &str) -> Self {
        Self(Arc::from(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Every fault the kit injects. [`FaultKind::ALL`] lists them, so a test
/// can check that a plan exercised each one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FaultKind {
    /// A delivery held back before the consumer sees it.
    BusDelay,
    /// A delivery handed to the consumer ahead of one pulled before it.
    BusReorder,
    /// An envelope published a second time (a publisher retry after a
    /// lost acknowledgement).
    BusDuplicate,
    /// A delivery lost before the consumer sees it; the bus redelivers it
    /// after a nack or an ack timeout.
    BusDrop,
    /// The publishing node crashed before the envelope reached the bus.
    BusCrashOnPublish,
    /// The consuming node crashed after handling a delivery and before its
    /// ack reached the bus.
    BusCrashBeforeAck,
    /// A store call held back before it runs.
    StoreLatency,
    /// A store call failed without running.
    StoreFailBefore,
    /// A store call ran (and committed) but reported failure.
    StoreFailAfter,
    /// The calling node crashed after a store call committed, before it
    /// saw the result.
    StoreCrashAfter,
    /// An upstream that accepts no connection.
    UpstreamUnreachable,
    /// An upstream answering with an error status.
    UpstreamStatus,
    /// An upstream stream cut off before it finished.
    UpstreamTruncate,
    /// An upstream that stops sending for a while.
    UpstreamStall,
    /// A wall clock stepped forwards or backwards.
    ClockStep,
}

impl FaultKind {
    pub const ALL: [FaultKind; 15] = [
        Self::BusDelay,
        Self::BusReorder,
        Self::BusDuplicate,
        Self::BusDrop,
        Self::BusCrashOnPublish,
        Self::BusCrashBeforeAck,
        Self::StoreLatency,
        Self::StoreFailBefore,
        Self::StoreFailAfter,
        Self::StoreCrashAfter,
        Self::UpstreamUnreachable,
        Self::UpstreamStatus,
        Self::UpstreamTruncate,
        Self::UpstreamStall,
        Self::ClockStep,
    ];

    /// Whether the fault takes its node down.
    pub const fn is_crash(self) -> bool {
        matches!(
            self,
            Self::BusCrashOnPublish | Self::BusCrashBeforeAck | Self::StoreCrashAfter
        )
    }
}

/// Where a fault struck.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FaultSite {
    /// An envelope on the bus.
    Bus { subject: Subject, event: EventId },
    /// A store operation, by the name the wrapper gave it.
    Store { op: &'static str },
    /// One upstream exchange.
    Upstream,
    /// A node's wall clock.
    Clock,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FaultEvent {
    pub kind: FaultKind,
    pub node: NodeName,
    pub site: FaultSite,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TraceEvent {
    /// A step the scenario marked; failures name the last one.
    Step(String),
    /// Anything else the scenario records (what a consumer saw, say), which
    /// the determinism hash covers.
    Note(String),
    Fault(FaultEvent),
    /// A supervisor restarted a crashed node; `incarnation` counts from 0
    /// for the first run.
    Restart {
        node: NodeName,
        incarnation: u32,
    },
    /// A task spawned through `SimCtx::spawn` panicked; the run fails.
    TaskPanicked {
        task: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TraceRecord {
    /// Simulated time since the run began.
    pub at: Duration,
    pub event: TraceEvent,
}

/// Records events into the run's trace. Cheap to clone; every clone feeds
/// the same trace.
#[derive(Debug, Clone)]
pub struct Tracer {
    tx: mpsc::UnboundedSender<TraceRecord>,
    start: Instant,
}

impl Tracer {
    /// Starts the clock the records are stamped with. Called inside the
    /// paused runtime, so `start` is simulated time.
    pub(crate) fn start(tx: mpsc::UnboundedSender<TraceRecord>) -> Self {
        Self {
            tx,
            start: Instant::now(),
        }
    }

    /// Simulated time since the run began.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    pub fn record(&self, event: TraceEvent) {
        let record = TraceRecord {
            at: self.elapsed(),
            event,
        };
        // The receiver lives until the driver has drained the trace, after
        // the runtime stopped; a send can only fail from a task that
        // outlived its run, whose records belong to no trace.
        if self.tx.send(record).is_err() {
            tracing::debug!("sim trace record after the run ended");
        }
    }

    pub fn step(&self, label: impl Into<String>) {
        self.record(TraceEvent::Step(label.into()));
    }

    pub fn note(&self, text: impl Into<String>) {
        self.record(TraceEvent::Note(text.into()));
    }

    pub(crate) fn fault(&self, kind: FaultKind, node: &NodeName, site: FaultSite) {
        tracing::debug!(node = %node, kind = ?kind, site = ?site, "sim fault injected");
        self.record(TraceEvent::Fault(FaultEvent {
            kind,
            node: node.clone(),
            site,
        }));
    }
}

/// The driver's end of the trace channel.
pub(crate) struct TraceSink {
    rx: mpsc::UnboundedReceiver<TraceRecord>,
}

impl TraceSink {
    pub(crate) fn new() -> (mpsc::UnboundedSender<TraceRecord>, Self) {
        let (tx, rx) = mpsc::unbounded_channel();
        (tx, Self { rx })
    }

    /// Everything recorded so far, in arrival order.
    pub(crate) fn drain(&mut self) -> Trace {
        let mut records = Vec::new();
        while let Ok(record) = self.rx.try_recv() {
            records.push(record);
        }
        Trace { records }
    }
}

/// A finished run's records, in the order they were recorded. A record's
/// step number is its index.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Trace {
    records: Vec<TraceRecord>,
}

impl Trace {
    pub fn records(&self) -> &[TraceRecord] {
        &self.records
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// FNV-1a over each record's debug form: stable for a given build, so
    /// two runs of one seed hash equal and a divergence shows.
    pub fn hash(&self) -> TraceHash {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;
        let mut hash = OFFSET;
        for record in &self.records {
            for byte in format!("{record:?}\n").bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(PRIME);
            }
        }
        TraceHash(hash)
    }

    pub fn faults(&self) -> impl Iterator<Item = &FaultEvent> {
        self.records
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::Fault(fault) => Some(fault),
                _ => None,
            })
    }

    pub fn count(&self, kind: FaultKind) -> usize {
        self.faults().filter(|fault| fault.kind == kind).count()
    }

    /// The notes, in order: what the scenario chose to record.
    pub fn notes(&self) -> impl Iterator<Item = &str> {
        self.records
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::Note(note) => Some(note.as_str()),
                _ => None,
            })
    }

    pub fn last_step(&self) -> Option<&str> {
        self.records
            .iter()
            .rev()
            .find_map(|record| match &record.event {
                TraceEvent::Step(label) => Some(label.as_str()),
                _ => None,
            })
    }

    /// The first task panic, if any.
    pub fn task_panic(&self) -> Option<(&str, &str)> {
        self.records.iter().find_map(|record| match &record.event {
            TraceEvent::TaskPanicked { task, message } => Some((task.as_str(), message.as_str())),
            _ => None,
        })
    }

    /// The last `n` records with their step numbers.
    pub fn tail(&self, n: usize) -> impl Iterator<Item = (usize, &TraceRecord)> {
        let skip = self.records.len().saturating_sub(n);
        self.records.iter().enumerate().skip(skip)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TraceHash(pub u64);

impl fmt::Display for TraceHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}
