//! [`Live::quiesce`]: a Postgres-mode live process stops on purpose, the
//! first step of a planned handover (`gateway::postgres::handover`).
//!
//! ```text
//! ticker aborted ─▶ the bus stops handing out deliveries (PgBus::shutdown: every `next`
//!   returns None; the listener and the reaper aborted, so nothing is reaped on the way out)
//!   ─▶ every stage finishes the delivery it handles and acks it, then ends; L5 answers L4's
//!      last inputs, then checkpoints and acks what it holds (the pump still serves its acks)
//!   ─▶ the exchange log ends after its last append and ack
//!   (each joined by the deadline; one still running then is aborted, and what it held is
//!    released by the next step)
//!   ─▶ L7's outbox relay and the retention prune aborted and joined (outbox rows stay for
//!      the next holder's recovery)
//!   ─▶ the bus abandoned: every take in flight aborted and joined, so no hold commits after
//!   ─▶ the surface's relay and live feed stopped
//! ```
//!
//! Unlike [`Live::shutdown`], nothing waits for the groups to empty: the
//! bus is durable and the next holder (a standby, or this gateway's next
//! start) takes the backlog. Waiting for it would chase another gateway's
//! captures until the deadline.
//!
//! What a stage still holds afterwards (a stage aborted at the deadline,
//! L5's deliveries when its last checkpoint failed, a take that committed
//! as its subscription dropped) stays `held` in the database for
//! [`crosstalk_transport::PgBus::release_held`], which the handover runs
//! next on the same pool, still under the lock.

use crosstalk_store::sqlx::PgPool;
use crosstalk_transport::PgBus;
use tokio::time::Instant;

use super::PgSet;
use crate::live::{Live, join_by};

/// How [`Live::quiesce`] went: what ended on its own and what the deadline
/// cut short, and the incarnation's bus and pool for the steps after it.
#[derive(Debug)]
pub struct Quiesced {
    /// The incarnation's bus: its holds are released next.
    pub bus: PgBus,
    /// The incarnation's pool: closed after the release.
    pub pool: PgPool,
    pub report: QuiesceReport,
}

/// What ended on its own during [`Live::quiesce`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuiesceReport {
    /// Stages that finished their delivery and ended by the deadline.
    pub stages_finished: usize,
    /// Stages the deadline cut short (aborted).
    pub stages_aborted: usize,
    /// The exchange log ended on its own (true when it does not run).
    pub log_finished: bool,
    /// Every take in flight was joined by the deadline: none can commit a
    /// hold after the release.
    pub takes_joined: bool,
    /// The surface's relay and live feed stopped by the deadline.
    pub surface_stopped: bool,
}

impl QuiesceReport {
    /// Whether everything ended on its own by the deadline.
    pub fn clean(&self) -> bool {
        self.stages_aborted == 0 && self.log_finished && self.takes_joined && self.surface_stopped
    }
}

impl Live<PgSet> {
    /// Stop taking deliveries and let every stage finish or give up what
    /// it holds, by `deadline`: see the module docs. Consumes the live
    /// process; its bus and pool are handed back for the handover's next
    /// steps (releasing what is still held, closing the pool).
    pub async fn quiesce(self, deadline: Instant) -> Quiesced {
        if let Some(ticker) = self.ticker {
            ticker.abort();
            // An aborted task's join reports the cancellation.
            let _ = tokio::time::timeout_at(deadline, ticker).await;
        }
        if let Some(capture) = self.capture {
            join_by("capture stage", capture, deadline).await;
        }
        let bus = self.quiet.bus.clone();
        let pool = self.quiet.pool.clone();
        // No `next` hands out anything from here on.
        bus.shutdown();
        let mut stages_finished = 0;
        let mut stages_aborted = 0;
        for running in self.stages {
            match join_by(running.slot.name(), running.task, deadline).await {
                true => stages_finished += 1,
                false => stages_aborted += 1,
            }
        }
        let log_finished = match self.exchange_log {
            Some(task) => join_by("exchange log consumer", task, deadline).await,
            None => true,
        };
        for task in &self.publishers {
            task.abort();
        }
        for task in self.publishers {
            let _ = tokio::time::timeout_at(deadline, task).await;
        }
        let abandoned = Box::pin(bus.abandon(deadline)).await;
        let surface_stopped = tokio::time::timeout_at(deadline, Box::pin(self.backend.shutdown()))
            .await
            .is_ok();
        let report = QuiesceReport {
            stages_finished,
            stages_aborted,
            log_finished,
            takes_joined: abandoned.takes_joined,
            surface_stopped,
        };
        match report.clean() {
            true => tracing::info!(
                stages_finished,
                takes_aborted = abandoned.takes_aborted,
                "live process quiesced: every stage finished its delivery"
            ),
            false => tracing::warn!(
                stages_finished,
                stages_aborted,
                log_finished,
                takes_joined = abandoned.takes_joined,
                surface_stopped,
                "live process quiesced; the deadline cut some of it short (what it held is released)"
            ),
        }
        Quiesced { bus, pool, report }
    }
}
