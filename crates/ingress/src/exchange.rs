//! One in-flight exchange: its id, its stage, its times and what the relay
//! records about its response.
//!
//! **Stages.** [`InFlight`] moves only along the proxy's legal transitions
//! (`ingress.stage.legal-transitions`): `Forwarded → Responding → Completed`,
//! and `Forwarded`/`Responding → Failed`. A call that would make any other
//! move is ignored and returns `false`, so the first failure cause wins
//! (`ingress.failure.cause-classified`) and a terminal stage never changes.
//! `Completed` is reachable only through [`InFlight::finished`], which the
//! relay calls only on the framer's `Finished`
//! (`ingress.stage.completed-requires-finished`).
//!
//! **Times.** [`StageClock`] takes one wall-clock reading when the exchange
//! is routed and derives every later time from `tokio::time::Instant`, which
//! never goes backwards, so `started_at <= first_chunk_at <= ended_at` holds
//! even when the wall clock steps (`ingress.timestamps.stage-order`).

use bytes::Bytes;
use crosstalk_spec::ids::ExchangeId;
use crosstalk_spec::interfaces::l0_ingress::RawResponse;
use crosstalk_spec::observed::exchange::{ExchangeFailure, ExchangeStage, Transport};
use crosstalk_spec::support::{Clock, Timestamp};
use tokio::sync::mpsc;
use tokio::time::Instant;

/// Wall time from one reading plus monotonic elapsed time.
#[derive(Debug, Clone, Copy)]
pub struct StageClock {
    wall: Timestamp,
    start: Instant,
}

impl StageClock {
    /// Read `clock` once, now.
    pub fn start(clock: &dyn Clock) -> Self {
        Self {
            wall: clock.now(),
            start: Instant::now(),
        }
    }

    pub fn started_at(&self) -> Timestamp {
        self.wall
    }

    /// The wall time of `instant`: the reading plus the time since it.
    pub fn at(&self, instant: Instant) -> Timestamp {
        let elapsed = instant.saturating_duration_since(self.start).as_micros();
        let micros = u64::try_from(elapsed).unwrap_or(u64::MAX);
        Timestamp::from_micros(self.wall.as_micros().saturating_add(micros))
    }
}

/// A stage change, for observers (diagnostics and tests).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageEvent {
    pub exchange: ExchangeId,
    pub stage: ExchangeStage,
}

/// Where stage changes are reported, if anywhere. Bounded, and sent with
/// `try_send`: a slow observer loses events, never slows an exchange.
pub type StageObserver = mpsc::Sender<StageEvent>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Forwarded,
    Responding,
    Completed,
    Failed(ExchangeFailure),
}

/// An exchange between being forwarded and its hand-off.
#[derive(Debug)]
pub struct InFlight {
    id: ExchangeId,
    clock: StageClock,
    stage: Stage,
    first_content: Option<Instant>,
    observer: Option<StageObserver>,
}

impl InFlight {
    /// A new exchange, `Forwarded` as of its clock's start.
    pub fn forwarded(id: ExchangeId, clock: StageClock, observer: Option<StageObserver>) -> Self {
        let exchange = Self {
            id,
            clock,
            stage: Stage::Forwarded,
            first_content: None,
            observer,
        };
        exchange.report(ExchangeStage::Forwarded {
            at: clock.started_at(),
        });
        exchange
    }

    pub fn id(&self) -> ExchangeId {
        self.id
    }

    pub fn clock(&self) -> StageClock {
        self.clock
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self.stage, Stage::Completed | Stage::Failed(_))
    }

    /// `Forwarded → Responding` at `now`.
    pub fn first_content(&mut self, now: Instant) -> bool {
        if self.stage != Stage::Forwarded {
            return false;
        }
        self.stage = Stage::Responding;
        self.first_content = Some(now);
        self.report(ExchangeStage::Responding {
            first_chunk_at: self.clock.at(now),
        });
        true
    }

    /// `Responding → Completed`.
    pub fn finished(&mut self) -> bool {
        if self.stage != Stage::Responding {
            return false;
        }
        self.stage = Stage::Completed;
        self.report(ExchangeStage::Completed);
        true
    }

    /// `Forwarded`/`Responding → Failed`. The proxy never assigns
    /// `UnparseableResponse`, the normalizer's verdict; asked to, it records
    /// nothing (`ingress.failure.never-unparseable`).
    pub fn fail(&mut self, failure: ExchangeFailure) -> bool {
        if self.is_terminal() || failure == ExchangeFailure::UnparseableResponse {
            return false;
        }
        self.stage = Stage::Failed(failure);
        self.report(ExchangeStage::Failed(failure));
        true
    }

    fn report(&self, stage: ExchangeStage) {
        if let Some(observer) = &self.observer {
            // A full or closed observer loses the event; it never waits.
            let _ = observer.try_send(StageEvent {
                exchange: self.id,
                stage,
            });
        }
    }

    /// The exchange's record at stream end. A stage still open becomes
    /// `Failed(open)`: an exchange is never handed off unfinished.
    pub(crate) fn record(
        mut self,
        open: ExchangeFailure,
        status: Option<u16>,
        transport: Transport,
        body: CapturedBody,
        ended: Instant,
    ) -> ResponseRecord {
        self.fail(open);
        let outcome = match self.stage {
            Stage::Completed => Outcome::Completed,
            Stage::Failed(failure) => Outcome::Failed(failure),
            // `fail` above made every open stage terminal.
            Stage::Forwarded | Stage::Responding => Outcome::Failed(open),
        };
        ResponseRecord {
            status,
            transport,
            outcome,
            body,
            first_chunk_at: self.first_content.map(|at| self.clock.at(at)),
            ended_at: self.clock.at(ended),
        }
    }
}

/// How an exchange ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    Completed,
    Failed(ExchangeFailure),
}

/// The response bytes capture kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CapturedBody {
    /// Every byte received, in order, as the chunks they arrived in.
    Kept(Vec<Bytes>),
    /// More than the response capture bound arrived; the bytes were let go.
    Overflowed,
}

/// What the relay hands the exchange's capture task when the response
/// stream ends.
#[derive(Debug)]
pub(crate) struct ResponseRecord {
    /// `None` when no response head arrived.
    pub status: Option<u16>,
    pub transport: Transport,
    pub outcome: Outcome,
    pub body: CapturedBody,
    pub first_chunk_at: Option<Timestamp>,
    pub ended_at: Timestamp,
}

impl ResponseRecord {
    /// The spec's `RawResponse`, or `None` when the body overflowed.
    pub fn raw_response(self) -> Option<RawResponse> {
        let CapturedBody::Kept(chunks) = self.body else {
            return None;
        };
        let body = chunks.concat();
        Some(match (self.outcome, self.status) {
            (Outcome::Completed, Some(status)) => RawResponse::Complete { status, body },
            (Outcome::Failed(failure), _) => RawResponse::Failed {
                failure,
                partial_body: body,
            },
            // Completion needs a framer, which needs a response head.
            (Outcome::Completed, None) => RawResponse::Failed {
                failure: ExchangeFailure::UpstreamUnreachable,
                partial_body: body,
            },
        })
    }
}
