//! A planned handover: the lock's holder stops on purpose (SIGTERM, a
//! `docker stop`, a rolling restart) and leaves the pipeline to the next
//! holder (a standby gateway, or its own next start) cleanly.
//!
//! ```text
//! Handover<Quiescing>      status handing_over / quiescing
//!   quiesce(deadline)      no delivery taken any more; each stage finishes the one it handles
//!                          and acks it; L5 checkpoints and acks what it holds; every task ends
//!                          (cut short at the deadline); the bus's takes joined
//! Handover<Quiesced>       status releasing_holds
//!   release_holds()        what is still held goes back ready, its attempt given back
//!                          (PgBus::release_held), on the incarnation's pool, under the lock
//! Handover<HoldsReleased>  status closing_pool
//!   close_pool()           the incarnation's pool closes: its connections leave the fence
//! Handover<PoolClosed>     status releasing_lock
//!   release_lock()         pg_advisory_unlock on the lock's session, which then closes
//! HandoverReport           status handed_over, pipeline_lock released
//! ```
//!
//! The order is the types': each step consumes the handover and returns
//! the next, and only [`Handover<PoolClosed>`] can release the lock, with
//! a [`PoolsClosed`] proof nothing else can make
//! ([`PipelineLock::release`] takes it). So the lock is never released
//! while the incarnation still runs, before its last checkpoint, or while
//! its pool's connections may still write.
//!
//! **Bounded.** Each step has its own bound and a stalled step never stops
//! the next ones: the quiesce ends at its deadline (stages still running
//! then are aborted; the release gives back what they held), a backstop
//! [`QUIESCE_BACKSTOP`] later at the latest; the release within
//! [`RELEASE_HOLDS_BOUND`], the pool's close within [`CLOSE_BOUND`], the
//! unlock within [`UNLOCK_BOUND`]. The whole handover ends by the quiesce
//! deadline plus [`TAIL`]; the caller gives the quiesce its flush deadline
//! minus [`TAIL`], so the handover ends within `shutdown.flush_timeout_ms`.
//! Whatever a cut-short step leaves, the fence covers: a connection still
//! open keeps its shared fence, so the next holder's take waits for it
//! (INV-1239), and a lock not unlocked is released with its session.
//!
//! The standby's take then finds the lock free at its next try (the
//! pipeline task's `RETRY`) and, with every connection of the old epoch
//! closed before the unlock, nothing to fence: the gap is about one retry.

#[cfg(test)]
mod tests;

use std::future::Future;
use std::time::Duration;

use crosstalk_spec::interfaces::l2_transport::BusError;
use crosstalk_store::sqlx::PgPool;
use crosstalk_transport::PgBus;
use tokio::time::Instant;

use super::CLOSE_BOUND;
use crate::live::Live;
use crate::live::pg::PgSet;
use crate::live::pg::quiesce::QuiesceReport;
use crate::live::recovery::{HandoverStep, LockState, PipelinePhase, StatusReporter};
use crate::store::lock::{PipelineLock, ReleaseError};

/// How long releasing what is still held may take.
pub const RELEASE_HOLDS_BOUND: Duration = Duration::from_secs(2);

/// How long the lock's unlock and close may take; past it the session is
/// dropped, which releases the lock as the server ends it.
pub const UNLOCK_BOUND: Duration = Duration::from_secs(2);

/// How long past its deadline the quiesce may take before the handover
/// moves on without it. The quiesce aborts what runs past its deadline
/// itself; this only bounds a quiesce that does not.
pub const QUIESCE_BACKSTOP: Duration = Duration::from_millis(500);

/// What the handover may take after the quiesce's deadline: every later
/// step's bound and the quiesce's backstop.
pub const TAIL: Duration = QUIESCE_BACKSTOP
    .saturating_add(RELEASE_HOLDS_BOUND)
    .saturating_add(CLOSE_BOUND)
    .saturating_add(UNLOCK_BOUND);

/// Proof that the outgoing incarnation's pool was closed (or its close's
/// bound passed: what is left the fence covers). Only
/// [`Handover<PoolClosed>`] makes one, so only the handover's last step
/// can call [`PipelineLock::release`].
#[derive(Debug)]
pub struct PoolsClosed {
    _private: (),
}

/// What a handover acts on: the running incarnation ([`OutgoingIncarnation`]),
/// or a scripted one in the tests. Each method is one step; the handover
/// calls them once each, in order, each under its bound.
pub trait Outgoing: Send {
    /// Stop taking deliveries; finish or give up what is in flight; end
    /// every task by `deadline`.
    fn quiesce(&mut self, deadline: Instant) -> impl Future<Output = QuiesceReport> + Send;
    /// Give back every delivery still held, its attempt with it.
    fn release_holds(&mut self) -> impl Future<Output = Result<u64, BusError>> + Send;
    /// Close the incarnation's pool.
    fn close_pool(&mut self) -> impl Future<Output = ()> + Send;
    /// Release the lock, its pool closed.
    fn release_lock(
        self,
        closed: PoolsClosed,
    ) -> impl Future<Output = Result<(), ReleaseError>> + Send;
}

/// The running incarnation, handed over: its live process, bus, pool and
/// lock.
pub struct OutgoingIncarnation {
    live: Option<Live<PgSet>>,
    bus: PgBus,
    pool: PgPool,
    lock: PipelineLock,
}

impl OutgoingIncarnation {
    pub fn new(live: Live<PgSet>, lock: PipelineLock) -> Self {
        let bus = live.layers().bus.clone();
        let pool = live.layers().pool.clone();
        Self {
            live: Some(live),
            bus,
            pool,
            lock,
        }
    }
}

impl Outgoing for OutgoingIncarnation {
    async fn quiesce(&mut self, deadline: Instant) -> QuiesceReport {
        match self.live.take() {
            Some(live) => Box::pin(live.quiesce(deadline)).await.report,
            None => QuiesceReport {
                stages_finished: 0,
                stages_aborted: 0,
                log_finished: true,
                takes_joined: true,
                surface_stopped: true,
            },
        }
    }

    async fn release_holds(&mut self) -> Result<u64, BusError> {
        self.bus.release_held().await
    }

    async fn close_pool(&mut self) {
        self.pool.close().await;
    }

    async fn release_lock(self, closed: PoolsClosed) -> Result<(), ReleaseError> {
        self.lock.release(closed).await
    }
}

/// How a bounded step ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bounded<T> {
    Done(T),
    /// Past its bound: the handover moved on without it.
    TimedOut {
        after: Duration,
    },
}

impl<T> Bounded<T> {
    fn after(bound: Duration) -> Self {
        Self::TimedOut { after: bound }
    }
}

/// How a handover went, step by step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoverReport {
    pub quiesce: Bounded<QuiesceReport>,
    pub holds: Bounded<Result<u64, BusError>>,
    pub pool_closed: Bounded<()>,
    pub lock: Bounded<Result<(), ReleaseError>>,
    /// From the handover's start to the lock's release.
    pub took: Duration,
}

impl HandoverReport {
    /// Whether every step finished in time and did what it should: the
    /// quiesce clean, the holds released, the pool closed, the lock
    /// unlocked.
    pub fn clean(&self) -> bool {
        matches!(&self.quiesce, Bounded::Done(report) if report.clean())
            && matches!(self.holds, Bounded::Done(Ok(_)))
            && self.pool_closed == Bounded::Done(())
            && self.lock == Bounded::Done(Ok(()))
    }

    /// How many deliveries went back to their groups, when the release
    /// ran.
    pub fn released(&self) -> Option<u64> {
        match self.holds {
            Bounded::Done(Ok(released)) => Some(released),
            _ => None,
        }
    }
}

/// The handover at a step; see the module docs. `Step` holds what the
/// steps before it found.
pub struct Handover<O, Step> {
    outgoing: O,
    status: StatusReporter,
    began: Instant,
    step: Step,
}

/// The first step: nothing done yet.
pub struct Quiescing;

/// Quiesced: the holds are released next.
pub struct Quiesced {
    quiesce: Bounded<QuiesceReport>,
}

/// The holds released: the pool closes next.
pub struct HoldsReleased {
    quiesce: Bounded<QuiesceReport>,
    holds: Bounded<Result<u64, BusError>>,
}

/// The pool closed: the lock may be released.
pub struct PoolClosed {
    quiesce: Bounded<QuiesceReport>,
    holds: Bounded<Result<u64, BusError>>,
    pool_closed: Bounded<()>,
}

/// `/readyz`'s `pipeline` and `handover`, `/healthz`'s
/// `recovery.handover`.
fn report_step(status: &StatusReporter, step: HandoverStep) {
    tracing::info!(step = step.label(), "pipeline handover step");
    status.update(|status| {
        status.phase = PipelinePhase::HandingOver(step);
        status.report.handover = Some(step);
    });
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl<O: Outgoing> Handover<O, Quiescing> {
    /// Begin: the status reads `handing_over` / `quiescing` from here on.
    pub fn begin(outgoing: O, status: StatusReporter) -> Self {
        report_step(&status, HandoverStep::Quiescing);
        Self {
            outgoing,
            status,
            began: Instant::now(),
            step: Quiescing,
        }
    }

    /// Quiesce by `deadline`, [`QUIESCE_BACKSTOP`] later at the latest.
    pub async fn quiesce(self, deadline: Instant) -> Handover<O, Quiesced> {
        let Self {
            mut outgoing,
            status,
            began,
            step: Quiescing,
        } = self;
        let quiesce =
            match tokio::time::timeout_at(deadline + QUIESCE_BACKSTOP, outgoing.quiesce(deadline))
                .await
            {
                Ok(report) => Bounded::Done(report),
                Err(_) => {
                    tracing::error!(
                        backstop_ms = millis(QUIESCE_BACKSTOP),
                        "the quiesce ran past its deadline and backstop; handing over without it \
                     (what still runs keeps its fence until it ends)"
                    );
                    Bounded::after(deadline.saturating_duration_since(began) + QUIESCE_BACKSTOP)
                }
            };
        report_step(&status, HandoverStep::ReleasingHolds);
        Handover {
            outgoing,
            status,
            began,
            step: Quiesced { quiesce },
        }
    }
}

impl<O: Outgoing> Handover<O, Quiesced> {
    /// Release what is still held, within [`RELEASE_HOLDS_BOUND`].
    pub async fn release_holds(self) -> Handover<O, HoldsReleased> {
        let Self {
            mut outgoing,
            status,
            began,
            step: Quiesced { quiesce },
        } = self;
        let holds = match tokio::time::timeout(RELEASE_HOLDS_BOUND, outgoing.release_holds()).await
        {
            Ok(released) => {
                if let Err(error) = &released {
                    tracing::warn!(
                        error = ?error,
                        "releasing the held deliveries failed; the next holder's recovery \
                         returns them (their attempts counted)"
                    );
                }
                Bounded::Done(released)
            }
            Err(_) => {
                tracing::warn!(
                    bound_ms = millis(RELEASE_HOLDS_BOUND),
                    "releasing the held deliveries did not answer in time; the next holder's \
                     recovery returns them (their attempts counted)"
                );
                Bounded::after(RELEASE_HOLDS_BOUND)
            }
        };
        report_step(&status, HandoverStep::ClosingPool);
        Handover {
            outgoing,
            status,
            began,
            step: HoldsReleased { quiesce, holds },
        }
    }
}

impl<O: Outgoing> Handover<O, HoldsReleased> {
    /// Close the incarnation's pool within [`CLOSE_BOUND`].
    pub async fn close_pool(self) -> Handover<O, PoolClosed> {
        let Self {
            mut outgoing,
            status,
            began,
            step: HoldsReleased { quiesce, holds },
        } = self;
        let pool_closed = match tokio::time::timeout(CLOSE_BOUND, outgoing.close_pool()).await {
            Ok(()) => Bounded::Done(()),
            Err(_) => {
                tracing::warn!(
                    bound_ms = millis(CLOSE_BOUND),
                    "the pipeline pool did not close in time; the next holder's fence waits \
                     for what is left"
                );
                Bounded::after(CLOSE_BOUND)
            }
        };
        report_step(&status, HandoverStep::ReleasingLock);
        Handover {
            outgoing,
            status,
            began,
            step: PoolClosed {
                quiesce,
                holds,
                pool_closed,
            },
        }
    }
}

impl<O: Outgoing> Handover<O, PoolClosed> {
    /// Release the lock within [`UNLOCK_BOUND`] (past it, or refused, the
    /// session is dropped and the lock goes with it); report `handed_over`
    /// and `released`.
    pub async fn release_lock(self) -> HandoverReport {
        let Self {
            outgoing,
            status,
            began,
            step:
                PoolClosed {
                    quiesce,
                    holds,
                    pool_closed,
                },
        } = self;
        let closed = PoolsClosed { _private: () };
        let lock = match tokio::time::timeout(UNLOCK_BOUND, outgoing.release_lock(closed)).await {
            Ok(released) => {
                if let Err(error) = &released {
                    tracing::warn!(
                        error = %error,
                        "the pipeline lock was not unlocked; its session was dropped, which \
                         releases it"
                    );
                }
                Bounded::Done(released)
            }
            Err(_) => {
                tracing::warn!(
                    bound_ms = millis(UNLOCK_BOUND),
                    "unlocking the pipeline lock did not finish in time; its session was \
                     dropped, which releases it"
                );
                Bounded::after(UNLOCK_BOUND)
            }
        };
        status.update(|status| {
            status.lock = LockState::Released;
        });
        report_step(&status, HandoverStep::Done);
        let report = HandoverReport {
            quiesce,
            holds,
            pool_closed,
            lock,
            took: began.elapsed(),
        };
        let quiesced = match &report.quiesce {
            Bounded::Done(quiesced) => Some(*quiesced),
            Bounded::TimedOut { .. } => None,
        };
        tracing::info!(
            clean = report.clean(),
            took_ms = millis(report.took),
            stages_finished = quiesced.map(|q| q.stages_finished),
            stages_aborted = quiesced.map(|q| q.stages_aborted),
            released = report.released(),
            pool_closed = report.pool_closed == Bounded::Done(()),
            lock_unlocked = report.lock == Bounded::Done(Ok(())),
            "pipeline handed over; the lock is free"
        );
        report
    }
}

/// Hand `outgoing` over: every step in order (see the module docs), the
/// quiesce by `quiesce_by`. Ends by `quiesce_by` plus [`TAIL`].
pub async fn hand_over<O: Outgoing>(
    outgoing: O,
    status: StatusReporter,
    quiesce_by: Instant,
) -> HandoverReport {
    Handover::begin(outgoing, status)
        .quiesce(quiesce_by)
        .await
        .release_holds()
        .await
        .close_pool()
        .await
        .release_lock()
        .await
}
