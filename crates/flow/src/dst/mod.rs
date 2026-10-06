//! Deterministic restart simulations of the flow consumer: a seeded
//! scenario run once without interruption and once with the process
//! crashing (in-memory state lost) and restoring at seeded points, with
//! outages leaving work half done when the crash comes.
//!
//! - `flow.consumer.restore-equivalent` (INV-1215): both runs decide the
//!   same transmissions, at the same ids and times, and record the same
//!   channel traffic.
//! - `flow.checkpoint.ticks-with-state` (INV-1216): checked after every
//!   checkpoint the driver takes (`world::Driver::checkpoint`).
//! - `transport.consumer.derived-envelope-ids` (INV-1202, flow's part): a
//!   redelivered input republishes only envelope ids the bus already holds.

mod world;

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::EventId;

use self::world::{Faults, Item, run, scenario};

/// Seeds every simulation sweeps; `CROSSTALK_FLOW_DST_SEEDS` raises it.
fn seeds() -> u64 {
    std::env::var("CROSSTALK_FLOW_DST_SEEDS")
        .ok()
        .and_then(|text| text.parse().ok())
        .unwrap_or(48)
}

/// The transmission events a log holds, by envelope id.
fn transmission_events(log: &BTreeMap<EventId, Envelope>) -> BTreeMap<EventId, BusEvent> {
    log.iter()
        .filter(|(_, envelope)| {
            matches!(
                envelope.event,
                BusEvent::Detect(
                    DetectEvent::TransmissionConfirmed { .. }
                        | DetectEvent::TransmissionSuspected { .. }
                        | DetectEvent::ChannelCrossAccessed { .. }
                )
            )
        })
        .map(|(id, envelope)| (*id, envelope.event.clone()))
        .collect()
}

/// A consumer killed at seeded points (with outages leaving steps queued,
/// held writes released and not yet dropped, checkpoints refused) and
/// restored from its last checkpoint, the unacked deliveries redelivered
/// and the inputs recorded after the checkpoint re-fed, decides what an
/// uninterrupted consumer decides.
#[tokio::test(flavor = "current_thread")]
async fn restore_from_checkpoint_decides_as_uninterrupted() {
    let mut compared = 0usize;
    for seed in 0..seeds() {
        let plan = scenario(seed, true);
        let (uninterrupted_world, uninterrupted) = run(&plan, &Faults::default()).await;
        let uninterrupted_log = uninterrupted_world.log.envelopes();
        let faults = Faults::seeded(seed, plan.items.len());
        let (restored_world, restored) = run(&plan, &faults).await;
        if restored.transmissions != uninterrupted.transmissions {
            let ids: BTreeSet<_> = restored
                .transmissions
                .keys()
                .chain(uninterrupted.transmissions.keys())
                .copied()
                .collect();
            for id in ids {
                let (want, got) = (
                    uninterrupted.transmissions.get(&id),
                    restored.transmissions.get(&id),
                );
                if want != got {
                    eprintln!(
                        "transmission {}:\n  uninterrupted {want:?}\n  restored      {got:?}",
                        id.ulid_text()
                    );
                }
            }
            panic!("seed {seed}, faults {faults:?}: transmissions differ");
        }
        assert_eq!(
            restored.recorded, uninterrupted.recorded,
            "seed {seed}, faults {faults:?}: recorded channel traffic differs"
        );
        // The same transmission events, under the same envelope ids, and
        // every envelope id names one event however often it was
        // republished.
        let log = restored_world.log.envelopes();
        {
            let (want, got) = (transmission_events(&uninterrupted_log), transmission_events(&log));
            for (id, event) in &got {
                if want.get(id) != Some(event) {
                    eprintln!("only restored: {event:?}");
                }
            }
            for (id, event) in &want {
                if got.get(id) != Some(event) {
                    eprintln!("only uninterrupted: {event:?}");
                }
            }
        }
        assert_eq!(
            transmission_events(&log),
            transmission_events(&uninterrupted_log),
            "seed {seed}, faults {faults:?}: transmission events differ"
        );
        let published: BTreeSet<EventId> = restored_world.log.publishes().into_iter().collect();
        assert_eq!(
            published,
            log.keys().copied().collect(),
            "seed {seed}: a publish missing from the log"
        );
        compared += uninterrupted.transmissions.len();
    }
    // The scenarios decide something worth comparing.
    assert!(compared > 0, "no transmission decided in any scenario");
}

/// Without a crash, a durable consumer decides what a volatile one does:
/// checkpoints change nothing.
#[tokio::test(flavor = "current_thread")]
async fn checkpoints_change_no_decision() {
    for seed in 0..seeds().min(16) {
        let (_, plain) = run(&scenario(seed, false), &Faults::default()).await;
        let (_, checkpointed) = run(&scenario(seed, true), &Faults::default()).await;
        assert_eq!(plain, checkpointed, "seed {seed}");
    }
}

/// Every input handled twice in a row (a crash after its outputs were
/// published and before it was acked) publishes nothing the bus log did
/// not already hold: each envelope id is a function of the event, and the
/// repeat decides the same events.
#[tokio::test(flavor = "current_thread")]
async fn redelivery_republishes_the_same_envelope_ids() {
    for seed in 0..seeds().min(24) {
        let plan = scenario(seed, false);
        let world = world::World::new(&plan).await;
        let mut consumer = world.start().await;
        for item in &plan.items {
            let before: BTreeSet<EventId> = world.log.envelopes().keys().copied().collect();
            let published = world.log.publishes().len();
            let again = match item {
                Item::Batch(inputs) => {
                    assert_eq!(consumer.handle_batch(inputs.clone()).await, Ok(()));
                    let first: BTreeSet<EventId> =
                        world.log.envelopes().keys().copied().collect();
                    assert_eq!(consumer.handle_batch(inputs.clone()).await, Ok(()));
                    first
                }
                Item::Deliver(event) => {
                    consumer.handle_event(event).await;
                    let first: BTreeSet<EventId> =
                        world.log.envelopes().keys().copied().collect();
                    consumer.handle_event(event).await;
                    first
                }
                Item::Tick(now) => {
                    consumer.tick(*now).await;
                    continue;
                }
                Item::Checkpoint => continue,
            };
            let after: BTreeSet<EventId> = world.log.envelopes().keys().copied().collect();
            assert_eq!(
                after, again,
                "seed {seed}: the repeat of {item:?} published new envelope ids"
            );
            assert!(
                before.is_subset(&after) && world.log.publishes().len() >= published,
                "seed {seed}: the log lost an envelope"
            );
        }
    }
}

