//! Driving a live process to a fixed point: [`Live::settle`].
//!
//! ```text
//! settle(until): clock ─▶ until (a manual clock; a read clock stays)
//!   pass: quiet ─▶ tick every stage at now, in slot order ─▶ quiet
//!         repeat until a pass handled nothing new (Activity unchanged)
//! quiet:  every slot's group empty, the outbox flushed, every stage's side
//!         inputs drained, twice in a row
//! ```
//!
//! What a pass decides depends only on what was ingested and the clock's
//! time: every id the stages mint is derived from its input or drawn from
//! a seeded generator in input order, the stages tick in a fixed order,
//! and nothing time-driven runs between settles under `Ticking::OnSettle`.

use crosstalk_spec::interfaces::l2_transport::BusError;
use crosstalk_spec::support::Timestamp;
use crosstalk_transport::MpscBus;
use tokio::sync::{mpsc, oneshot};

use super::relay::Flush;
use super::stage::{Command, Slot};
use super::{Live, LiveClock, POLL, Running};

/// More passes than this and the process is not converging: a stage keeps
/// producing work on every tick.
const MAX_PASSES: u32 = 64;

/// Where a settle got to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settled {
    /// The clock's reading the stages ticked at.
    pub at: Timestamp,
    /// How many passes it took; the last changed nothing.
    pub passes: u32,
}

/// Why a settle stopped short.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SettleError {
    #[error("the {} stage has stopped", .0.name())]
    StageStopped(Slot),
    #[error("the outbox forwarder has stopped")]
    OutboxStopped,
    #[error("reading the {} group failed: {error:?}", slot.name())]
    Depth { slot: Slot, error: BusError },
    #[error("still busy after {passes} passes")]
    NotQuiet { passes: u32 },
}

impl Live {
    /// Move the clock to `until` (a manual clock, and only forwards), then
    /// run passes until one changes nothing: every consumer group idle and
    /// the bus empty, then every stage's tick at the clock's time (the
    /// correlator's windows close, provenance evicts), then idle again.
    /// Deterministic for a given input under `Ticking::OnSettle`.
    pub async fn settle(&self, until: Timestamp) -> Result<Settled, SettleError> {
        let at = self.clock.advance_to(until);
        let commands = commands_of(&self.stages);
        let slots: Vec<Slot> = commands.iter().map(|(slot, _)| *slot).collect();
        for pass in 1..=MAX_PASSES {
            let before = self.activity.read();
            self.quiet(&slots, &commands).await?;
            tick_all(&commands, at).await?;
            self.quiet(&slots, &commands).await?;
            if self.activity.read() == before {
                tracing::debug!(at = at.as_micros(), passes = pass, "live process settled");
                return Ok(Settled { at, passes: pass });
            }
        }
        Err(SettleError::NotQuiet { passes: MAX_PASSES })
    }

    /// Wait until nothing is queued anywhere, twice in a row.
    async fn quiet(
        &self,
        slots: &[Slot],
        commands: &[(Slot, mpsc::UnboundedSender<Command>)],
    ) -> Result<(), SettleError> {
        let bus = &self.backend.stores.bus;
        let mut quiet = 0;
        while quiet < 2 {
            idle(bus, slots).await?;
            let (done, flushed) = oneshot::channel();
            self.flushes
                .send(Flush(done))
                .map_err(|_| SettleError::OutboxStopped)?;
            let forwarded = flushed.await.map_err(|_| SettleError::OutboxStopped)?;
            let mut drained = 0;
            for (slot, sender) in commands {
                let (done, answer) = oneshot::channel();
                sender
                    .send(Command::Drain { done })
                    .map_err(|_| SettleError::StageStopped(*slot))?;
                drained += answer.await.map_err(|_| SettleError::StageStopped(*slot))?;
            }
            quiet = match forwarded + drained {
                0 if busy(bus, slots).await? => 0,
                0 => quiet + 1,
                _ => 0,
            };
            tokio::time::sleep(POLL).await;
        }
        Ok(())
    }
}

/// Every running stage's command sender, in slot order.
pub(super) fn commands_of(stages: &[Running]) -> Vec<(Slot, mpsc::UnboundedSender<Command>)> {
    stages
        .iter()
        .map(|running| (running.slot, running.commands.clone()))
        .collect()
}

/// Tick every stage at `now`, one after the other in slot order.
async fn tick_all(
    commands: &[(Slot, mpsc::UnboundedSender<Command>)],
    now: Timestamp,
) -> Result<(), SettleError> {
    for (slot, sender) in commands {
        let (done, ticked) = oneshot::channel();
        sender
            .send(Command::Tick { now, done })
            .map_err(|_| SettleError::StageStopped(*slot))?;
        ticked.await.map_err(|_| SettleError::StageStopped(*slot))?;
    }
    Ok(())
}

/// Tick every stage at the clock's reading every `every` of elapsed time,
/// until a stage stops.
pub(super) async fn tick_periodically(
    commands: Vec<(Slot, mpsc::UnboundedSender<Command>)>,
    clock: LiveClock,
    every: std::time::Duration,
) {
    let mut ticker = tokio::time::interval(every);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if let Err(error) = tick_all(&commands, clock.now()).await {
            tracing::debug!(error = %error, "ticker stopped");
            return;
        }
    }
}

/// Whether any of `slots`' groups holds a delivery.
async fn busy(bus: &MpscBus, slots: &[Slot]) -> Result<bool, SettleError> {
    for slot in slots {
        match bus.depth(&slot.group()).await {
            Ok(Some(depth))
                if depth.ready + depth.delayed + depth.held + depth.exhausted + depth.waiting
                    > 0 =>
            {
                return Ok(true);
            }
            Ok(_) => {}
            Err(error) => return Err(SettleError::Depth { slot: *slot, error }),
        }
    }
    Ok(false)
}

/// Wait until every one of `slots`' groups is empty.
pub(super) async fn idle(bus: &MpscBus, slots: &[Slot]) -> Result<(), SettleError> {
    while busy(bus, slots).await? {
        tokio::time::sleep(POLL).await;
    }
    Ok(())
}
