//! L5 in Postgres mode: the durable flow consumer
//! (`FlowConsumer::with_durability` over `PgFlowDurability`), restored
//! from its checkpoint before its group subscribes, run by its own loop:
//!
//! 1. commands: a tick answers the queued extracted batches, then
//!    `FlowConsumer::tick(now)` (and, when the process ticks only on
//!    settles, checkpoints so L7's frontier sees the tick); a drain
//!    answers the queued batches, then checkpoints and acks what it holds;
//! 2. the checkpoint interval (`flow.checkpoint_ms`): checkpoint, then ack;
//! 3. extracted batches from the L4 stage (`DurableInputs`), each answered
//!    once its steps ran (its accesses, held writes and tool calls stored);
//! 4. deliveries of its group, kept unacked until a checkpoint covers them
//!    (or `flow.checkpoint_unacked` wait, which checkpoints at once).
//!
//! A delivery is acked only after `checkpoint` returned `Ok`
//! (`flow.checkpoint.ticks-with-state`, `flow.consumer.restore-equivalent`).
//! A checkpoint refused while steps wait (`Busy`) leaves the deliveries
//! for the next one; the bus's ack timeout (above the checkpoint interval,
//! checked at start) redelivers them if the process dies first.

use crosstalk_api::pg::{PgAgentStore, PgChannels, PgTransmissions};
use crosstalk_flow::consumer::{ExtractedBatch, FlowConsumer, InputSource, SUBJECTS};
use crosstalk_flow::store::PgFlowDurability;
use crosstalk_spec::interfaces::l2_transport::{DeliveryId, Subscription};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use super::pump::Pumped;
use crate::live::stage::{Command, Control, Slot, SlotTaken, Stages};
use crate::spool::LiveBus;

/// The durable flow consumer over the Postgres stores.
pub type PgFlowConsumer = FlowConsumer<
    PgChannels<LiveBus>,
    PgTransmissions<LiveBus>,
    PgAgentStore<LiveBus>,
    LiveBus,
    PgFlowDurability,
>;

/// Put the (restored) consumer in its slot.
pub fn fill(
    stages: &mut Stages<Pumped>,
    consumer: PgFlowConsumer,
    inputs: mpsc::Receiver<ExtractedBatch>,
    checkpoint_on_tick: bool,
) -> Result<(), SlotTaken> {
    stages.fill_task(
        Slot::L5Flow,
        SUBJECTS.to_vec(),
        move |subscription, control| {
            run(consumer, subscription, control, inputs, checkpoint_on_tick)
        },
    )
}

/// Answer every extracted batch already queued; how many.
async fn answer_queued(
    consumer: &mut PgFlowConsumer,
    inputs: &mut mpsc::Receiver<ExtractedBatch>,
) -> u64 {
    let mut handled = 0;
    while let Ok(batch) = inputs.try_recv() {
        consumer.answer(batch).await;
        handled += 1;
    }
    handled
}

/// Checkpoint, then ack every delivery the checkpoint covers. A refused
/// checkpoint leaves them for the next one.
async fn checkpoint_and_ack(
    consumer: &mut PgFlowConsumer,
    subscription: &mut Pumped,
    unacked: &mut Vec<DeliveryId>,
    slot: Slot,
) {
    match consumer.checkpoint().await {
        Ok(_) => {
            for id in unacked.drain(..) {
                if let Err(error) = subscription.ack(id).await {
                    tracing::warn!(stage = slot.name(), error = ?error, "ack failed; the bus will redeliver what the checkpoint holds");
                }
            }
        }
        Err(error) => {
            tracing::warn!(stage = slot.name(), unacked = unacked.len(), error = %error, "checkpoint not taken; acks wait for the next one");
        }
    }
}

async fn run(
    mut consumer: PgFlowConsumer,
    mut subscription: Pumped,
    control: Control,
    mut inputs: mpsc::Receiver<ExtractedBatch>,
    checkpoint_on_tick: bool,
) {
    let Control {
        retry: _,
        mut commands,
        activity,
        slot,
    } = control;
    let settings = *consumer.settings();
    let mut checkpoints = tokio::time::interval(settings.checkpoint_every);
    checkpoints.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut unacked: Vec<DeliveryId> = Vec::new();
    let mut inputs_open = true;
    tracing::info!(
        stage = slot.name(),
        checkpoint_ms = u64::try_from(settings.checkpoint_every.as_millis()).unwrap_or(u64::MAX),
        "durable flow consumer running"
    );
    loop {
        tokio::select! {
            biased;
            Some(command) = commands.recv() => match command {
                Command::Tick { now, done } => {
                    let handled = answer_queued(&mut consumer, &mut inputs).await;
                    activity.add(handled);
                    consumer.tick(now).await;
                    if checkpoint_on_tick {
                        checkpoint_and_ack(&mut consumer, &mut subscription, &mut unacked, slot).await;
                    }
                    let _ = done.send(());
                }
                Command::Drain { done } => {
                    let handled = answer_queued(&mut consumer, &mut inputs).await;
                    activity.add(handled);
                    if !unacked.is_empty() {
                        checkpoint_and_ack(&mut consumer, &mut subscription, &mut unacked, slot).await;
                    }
                    let _ = done.send(handled);
                }
            },
            _ = checkpoints.tick() => {
                checkpoint_and_ack(&mut consumer, &mut subscription, &mut unacked, slot).await;
            }
            batch = InputSource::recv(&mut inputs), if inputs_open => match batch {
                Some(batch) => {
                    consumer.answer(batch).await;
                    activity.bump();
                }
                None => inputs_open = false,
            },
            next = subscription.next() => {
                let Some(next) = next else { break };
                let delivery = match next {
                    Ok(delivery) => delivery,
                    Err(error) => {
                        tracing::warn!(stage = slot.name(), error = ?error, "delivery failed");
                        continue;
                    }
                };
                consumer.handle_event(&delivery.envelope.event).await;
                activity.bump();
                unacked.push(delivery.id);
                if unacked.len() >= settings.max_unacked.get() {
                    checkpoint_and_ack(&mut consumer, &mut subscription, &mut unacked, slot).await;
                }
            }
        }
    }
    checkpoint_and_ack(&mut consumer, &mut subscription, &mut unacked, slot).await;
    tracing::debug!(
        stage = slot.name(),
        backlog = consumer.backlog(),
        "durable flow consumer stopped: the bus shut down"
    );
}
