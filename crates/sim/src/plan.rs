//! [`FaultPlan`]: a typed description of the faults a run injects, for the
//! bus (per subject), the stores and the upstreams.
//!
//! Every value is valid by construction: probabilities are in `[0, 1]`,
//! duration ranges are ordered, a reorder window holds at least two
//! deliveries, and an upstream error status is not a success. A fault left
//! at its default (`None` or [`Probability::NEVER`]) never fires and draws
//! no randomness.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::Duration;

use crosstalk_spec::events::Subject;
use crosstalk_spec::support::NonEmpty;

use crate::rng::{DurationRange, Probability};

/// A fault that fires with `chance` and lasts a duration drawn from
/// `within`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Timed {
    pub chance: Probability,
    pub within: DurationRange,
}

impl Timed {
    pub const fn new(chance: Probability, within: DurationRange) -> Self {
        Self { chance, within }
    }
}

/// Delivery reordering within one subscription.
///
/// When the subscription pulls a delivery, with `chance` it holds it and
/// pulls another, waiting up to `wait` of simulated time for one to
/// arrive, until it holds `window` deliveries or none arrives in time;
/// then it hands the consumer a uniformly chosen held delivery. Within one
/// consumer group the bus may deliver pending envelopes in any order
/// (`transport.ordering.unconstrained`), so every order this produces is
/// one the real bus may produce. A held delivery counts against its ack
/// timeout, as it would in a slow consumer, so keep `wait` well under it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Reorder {
    chance: Probability,
    window: NonZeroUsize,
    wait: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidReorder {
    #[error("a reorder window must hold at least 2 deliveries, got {got}")]
    WindowTooSmall { got: usize },
    #[error("a reorder wait must be longer than zero")]
    ZeroWait,
}

impl Reorder {
    pub fn new(chance: Probability, window: usize, wait: Duration) -> Result<Self, InvalidReorder> {
        let window = match NonZeroUsize::new(window) {
            Some(window) if window.get() >= 2 => window,
            _ => return Err(InvalidReorder::WindowTooSmall { got: window }),
        };
        if wait.is_zero() {
            return Err(InvalidReorder::ZeroWait);
        }
        Ok(Self {
            chance,
            window,
            wait,
        })
    }

    pub const fn chance(self) -> Probability {
        self.chance
    }

    /// At least 2.
    pub const fn window(self) -> NonZeroUsize {
        self.window
    }

    pub const fn wait(self) -> Duration {
        self.wait
    }
}

/// How a dropped delivery comes back.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Redelivery {
    /// The wrapper forgets the delivery; the bus redelivers it when its ack
    /// timeout expires. Needs a bus with an ack timeout.
    AckTimeout,
    /// The wrapper nacks the delivery with a `retry_after` drawn from
    /// `after`, as a consumer that never saw it would not, but as a lost
    /// delivery followed by a quick timeout looks. Counts as an attempt.
    Nack { after: DurationRange },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DropFault {
    pub chance: Probability,
    pub redelivery: Redelivery,
}

/// The faults for one subject's envelopes.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SubjectFaults {
    /// Hold a delivery back before the consumer sees it: a late event.
    pub delay: Option<Timed>,
    pub reorder: Option<Reorder>,
    /// Publish the envelope a second time, after the first publish
    /// returned (a publisher retry after a lost acknowledgement). The
    /// duplicate has the same `Envelope::id`.
    pub duplicate: Probability,
    /// Lose a delivery before the consumer sees it.
    pub drop: Option<DropFault>,
    /// Crash the publishing node before the envelope reaches the bus.
    pub crash_on_publish: Probability,
    /// Crash the consuming node when it acks, after it handled the
    /// delivery and before the ack reaches the bus.
    pub crash_before_ack: Probability,
}

impl SubjectFaults {
    /// No faults.
    pub fn none() -> Self {
        Self::default()
    }

    /// Delay, reorder, duplicate and drop (redelivered by nack) at modest
    /// rates; no crashes, which need a supervisor ([`crate::Node::supervise`]).
    pub fn chaos() -> Self {
        let ms = Duration::from_millis;
        Self {
            delay: Some(Timed::new(
                Probability::literal(0.2),
                DurationRange::literal(ms(1), ms(50)),
            )),
            reorder: Some(Reorder {
                chance: Probability::literal(0.3),
                window: NonZeroUsize::MIN.saturating_add(3),
                wait: ms(5),
            }),
            duplicate: Probability::literal(0.1),
            drop: Some(DropFault {
                chance: Probability::literal(0.1),
                redelivery: Redelivery::Nack {
                    after: DurationRange::literal(ms(10), ms(100)),
                },
            }),
            crash_on_publish: Probability::NEVER,
            crash_before_ack: Probability::NEVER,
        }
    }
}

/// Bus faults, configured per subject with a default for the rest.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BusFaults {
    default: SubjectFaults,
    subjects: HashMap<Subject, SubjectFaults>,
}

impl BusFaults {
    pub fn none() -> Self {
        Self::default()
    }

    /// The same faults for every subject.
    pub fn uniform(faults: SubjectFaults) -> Self {
        Self {
            default: faults,
            subjects: HashMap::new(),
        }
    }

    /// `faults` for `subject`'s envelopes instead of the default.
    pub fn with_subject(mut self, subject: Subject, faults: SubjectFaults) -> Self {
        self.subjects.insert(subject, faults);
        self
    }

    pub fn for_subject(&self, subject: Subject) -> &SubjectFaults {
        self.subjects.get(&subject).unwrap_or(&self.default)
    }
}

/// Faults for every call through one [`FaultyStore`](crate::FaultyStore).
/// At most one of the failures fires per call, checked in the order
/// `fail_before`, `crash_after`, `fail_after`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StoreFaults {
    /// Hold the call back before it runs.
    pub latency: Option<Timed>,
    /// Fail without running the call.
    pub fail_before: Probability,
    /// Run the call; if it succeeded (committed), report failure anyway.
    pub fail_after: Probability,
    /// Run the call; if it succeeded, crash the calling node before it sees
    /// the result.
    pub crash_after: Probability,
}

impl StoreFaults {
    pub fn none() -> Self {
        Self::default()
    }

    /// Latency and both failures at modest rates; no crashes.
    pub fn chaos() -> Self {
        let ms = Duration::from_millis;
        Self {
            latency: Some(Timed::new(
                Probability::literal(0.3),
                DurationRange::literal(ms(1), ms(20)),
            )),
            fail_before: Probability::literal(0.05),
            fail_after: Probability::literal(0.05),
            crash_after: Probability::NEVER,
        }
    }
}

/// A non-success HTTP status an upstream answers with: 300 to 599.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErrorStatus(u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("an upstream error status is 300 to 599, got {got}")]
pub struct InvalidErrorStatus {
    pub got: u16,
}

impl ErrorStatus {
    pub fn new(status: u16) -> Result<Self, InvalidErrorStatus> {
        if (300..=599).contains(&status) {
            Ok(Self(status))
        } else {
            Err(InvalidErrorStatus { got: status })
        }
    }

    pub const fn get(self) -> u16 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatusFault {
    pub chance: Probability,
    /// One is chosen uniformly.
    pub statuses: NonEmpty<ErrorStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TruncateFault {
    pub chance: Probability,
    /// The stream is cut after a uniformly chosen `0..=max_chunks` chunks.
    pub max_chunks: u32,
}

/// Faults for upstream exchanges. At most one fires per exchange, checked
/// in the order `unreachable`, `error_status`, `truncate`, `stall`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct UpstreamFaults {
    pub unreachable: Probability,
    pub error_status: Option<StatusFault>,
    pub truncate: Option<TruncateFault>,
    pub stall: Option<Timed>,
}

impl UpstreamFaults {
    pub fn none() -> Self {
        Self::default()
    }
}

/// Everything a run injects.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FaultPlan {
    pub bus: BusFaults,
    pub store: StoreFaults,
    pub upstream: UpstreamFaults,
}

impl FaultPlan {
    pub fn none() -> Self {
        Self::default()
    }

    /// [`SubjectFaults::chaos`] on every subject and [`StoreFaults::chaos`];
    /// no crashes and no upstream faults.
    pub fn chaos() -> Self {
        Self {
            bus: BusFaults::uniform(SubjectFaults::chaos()),
            store: StoreFaults::chaos(),
            upstream: UpstreamFaults::none(),
        }
    }
}
