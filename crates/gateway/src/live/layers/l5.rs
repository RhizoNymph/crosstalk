//! L5: `crosstalk-flow`'s consumer over the shared registry, transmission
//! and agent stores, fed by its bus group and by the extraction step.
//!
//! It runs as its own task (`Stages::fill_task`) because it has three
//! inputs, taken in this order of priority:
//!
//! 1. commands: a tick first handles every extracted input already queued,
//!    then `FlowConsumer::tick(now)`; a drain handles the queued inputs;
//! 2. extracted inputs from the extraction step;
//! 3. deliveries of its group (`ExchangeCaptured`, `ContentMatched`, ...).
//!
//! The consumer acks a delivery once its steps are queued; a step that
//! fails transiently waits at the head of its queue and is retried on the
//! next input or tick.

use crosstalk_flow::consumer::{Extracted, FlowConsumer, FlowDeps, SUBJECTS};
use crosstalk_memory::flow::{MemoryChannels, MemoryVerdicts};
use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_spec::interfaces::l2_transport::Subscription;
use crosstalk_transport::{MpscBus, MpscSubscription};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::live::stage::{
    Command, Control, Slot, SlotTaken, StageContext, Stages, settle_delivery,
};

type Consumer = FlowConsumer<MemoryChannels<MemoryAgents>, MemoryVerdicts, MemoryAgents, MpscBus>;

/// Put the flow consumer in its slot.
pub fn fill(
    stages: &mut Stages,
    ctx: &StageContext,
    extracted: UnboundedReceiver<Extracted>,
) -> Result<(), SlotTaken> {
    let consumer = FlowConsumer::new(
        ctx.flow,
        FlowDeps {
            registry: ctx.stores.channels.clone(),
            transmissions: ctx.stores.transmissions.clone(),
            agents: ctx.stores.agents.clone(),
            bus: ctx.stores.bus.clone(),
            clock: ctx.clock.clone(),
        },
    );
    stages.fill_task(
        Slot::L5Flow,
        SUBJECTS.to_vec(),
        move |subscription, control| run(consumer, subscription, control, extracted),
    )
}

/// Handle every extracted input already queued; how many.
async fn drain(consumer: &mut Consumer, extracted: &mut UnboundedReceiver<Extracted>) -> u64 {
    let mut handled = 0;
    while let Ok(input) = extracted.try_recv() {
        consumer.handle_extracted(input).await;
        handled += 1;
    }
    handled
}

async fn run(
    mut consumer: Consumer,
    mut subscription: MpscSubscription,
    control: Control,
    mut extracted: UnboundedReceiver<Extracted>,
) {
    let Control {
        retry,
        mut commands,
        activity,
        slot,
    } = control;
    tracing::debug!(stage = slot.name(), "flow consumer running");
    loop {
        tokio::select! {
            biased;
            Some(command) = commands.recv() => match command {
                Command::Tick { now, done } => {
                    let handled = drain(&mut consumer, &mut extracted).await;
                    activity.add(handled);
                    consumer.tick(now).await;
                    let _ = done.send(());
                }
                Command::Drain { done } => {
                    let handled = drain(&mut consumer, &mut extracted).await;
                    activity.add(handled);
                    let _ = done.send(handled);
                }
            },
            Some(input) = extracted.recv() => {
                consumer.handle_extracted(input).await;
                activity.bump();
            }
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
                settle_delivery(slot, &mut subscription, retry, &delivery, Ok(())).await;
            }
        }
    }
    tracing::debug!(
        stage = slot.name(),
        backlog = consumer.backlog(),
        "flow consumer stopped: the bus shut down"
    );
}
