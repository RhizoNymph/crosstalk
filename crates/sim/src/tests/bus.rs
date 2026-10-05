use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::interfaces::l2_transport::{
    BusError, Delivery, DeliveryId, EventBus, Subscription,
};

use super::toy_bus::{ToyBus, ToySubscription, changed, group, n, retry, watermark};
use crate::bus::{FaultyBus, FaultySubscription};
use crate::driver::{CheckFailed, Sim, SimCtx, SimReport};
use crate::node::SuperviseError;
use crate::plan::{BusFaults, DropFault, Redelivery, Reorder, SubjectFaults, Timed};
use crate::rng::{DurationRange, Probability, Seed};
use crate::trace::{FaultKind, FaultSite, TraceEvent};

const ACK_TIMEOUT: Duration = Duration::from_secs(1);

fn failed(error: BusError) -> CheckFailed {
    CheckFailed::new(format!("bus error: {error:?}"))
}

fn half() -> Probability {
    Probability::new(0.5).expect("valid")
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

async fn subscribe(
    bus: &FaultyBus<ToyBus>,
    subjects: &[Subject],
) -> Result<FaultySubscription<ToySubscription>, CheckFailed> {
    bus.subscribe(subjects, group("g"), retry())
        .await
        .map_err(failed)
}

async fn next(sub: &mut impl Subscription) -> Result<Delivery, CheckFailed> {
    match sub.next().await {
        Some(Ok(delivery)) => Ok(delivery),
        Some(Err(error)) => Err(failed(error)),
        None => Err(CheckFailed::new("bus closed")),
    }
}

/// A faulty bus on node `name` over `inner`.
fn faulty(ctx: &SimCtx, inner: &Arc<ToyBus>, faults: BusFaults, name: &str) -> FaultyBus<ToyBus> {
    let node = ctx.node(name);
    ctx.faulty_bus(Arc::clone(inner), faults, &node.handle())
}

fn run(seed: u64, scenario: impl AsyncFnOnce(SimCtx) -> Result<(), CheckFailed>) -> SimReport {
    Sim::run(Seed::new(seed), move |ctx| scenario(ctx)).expect("passes")
}

#[test]
fn no_faults_pass_deliveries_through_in_order() {
    let report = run(0, async |ctx| {
        let inner = Arc::new(ToyBus::new(ACK_TIMEOUT));
        let bus = faulty(&ctx, &inner, BusFaults::none(), "n");
        let mut sub = subscribe(&bus, &[Subject::Changed]).await?;
        for i in 1..=5 {
            bus.publish(changed(i)).await.map_err(failed)?;
        }
        for i in 1..=5u64 {
            let delivery = next(&mut sub).await?;
            ctx.check(n(&delivery.envelope) == u128::from(i), || {
                "in order".to_owned()
            })?;
            ctx.check(delivery.attempt == NonZeroU32::MIN, || {
                "first attempt".to_owned()
            })?;
            sub.ack(delivery.id).await.map_err(failed)?;
        }
        ctx.check(inner.unacked() == 0, || "all acked".to_owned())
    });
    assert_eq!(report.trace.faults().count(), 0);
}

#[test]
fn duplicate_publishes_the_envelope_again_after_the_first() {
    let faults = BusFaults::uniform(SubjectFaults {
        duplicate: Probability::ALWAYS,
        ..SubjectFaults::none()
    });
    let report = run(0, async |ctx| {
        let inner = Arc::new(ToyBus::new(ACK_TIMEOUT));
        let bus = faulty(&ctx, &inner, faults, "n");
        let mut sub = subscribe(&bus, &[Subject::Changed]).await?;
        bus.publish(changed(1)).await.map_err(failed)?;
        let first = next(&mut sub).await?;
        let second = next(&mut sub).await?;
        ctx.check(first.envelope == second.envelope, || {
            "same envelope".to_owned()
        })?;
        ctx.check(first.id != second.id, || "two deliveries".to_owned())
    });
    assert_eq!(report.trace.count(FaultKind::BusDuplicate), 1);
}

#[test]
fn faults_follow_the_envelope_subject() {
    let faults = BusFaults::none().with_subject(
        Subject::WatermarkAdvanced,
        SubjectFaults {
            duplicate: Probability::ALWAYS,
            ..SubjectFaults::none()
        },
    );
    let report = run(0, async |ctx| {
        let inner = Arc::new(ToyBus::new(ACK_TIMEOUT));
        let bus = faulty(&ctx, &inner, faults, "n");
        let mut sub = subscribe(&bus, &[Subject::Changed, Subject::WatermarkAdvanced]).await?;
        bus.publish(changed(1)).await.map_err(failed)?;
        bus.publish(watermark(2)).await.map_err(failed)?;
        let mut ids = Vec::new();
        for _ in 0..3 {
            ids.push(n(&next(&mut sub).await?.envelope));
        }
        ctx.check(ids == vec![1, 2, 2], || format!("{ids:?}"))
    });
    assert!(report.trace.faults().all(|fault| matches!(
        fault.site,
        FaultSite::Bus {
            subject: Subject::WatermarkAdvanced,
            ..
        }
    )));
}

/// Consumes envelope 1 under `drop`; returns the attempt it arrived with.
fn dropped_then_redelivered(seed: u64, drop: DropFault) -> (SimReport, u32) {
    let faults = BusFaults::uniform(SubjectFaults {
        drop: Some(drop),
        ..SubjectFaults::none()
    });
    let mut attempt = 0;
    let report = run(seed, async |ctx| {
        let inner = Arc::new(ToyBus::new(ACK_TIMEOUT));
        let bus = faulty(&ctx, &inner, faults, "n");
        let mut sub = subscribe(&bus, &[Subject::Changed]).await?;
        bus.publish(changed(1)).await.map_err(failed)?;
        let delivery = next(&mut sub).await?;
        attempt = delivery.attempt.get();
        sub.ack(delivery.id).await.map_err(failed)?;
        ctx.check(inner.unacked() == 0, || "acked once delivered".to_owned())
    });
    (report, attempt)
}

#[test]
fn dropped_delivery_is_nacked_and_redelivered_with_the_next_attempt() {
    let drop = DropFault {
        chance: half(),
        redelivery: Redelivery::Nack {
            after: DurationRange::exactly(ms(30)),
        },
    };
    let mut dropped_somewhere = false;
    for seed in 0..16 {
        let (report, attempt) = dropped_then_redelivered(seed, drop);
        let drops = report.trace.count(FaultKind::BusDrop);
        assert_eq!(attempt as usize, drops + 1, "seed {seed}");
        assert_eq!(report.elapsed, ms(30) * u32::try_from(drops).expect("few"));
        dropped_somewhere |= drops > 0;
    }
    assert!(dropped_somewhere);
}

#[test]
fn dropped_delivery_is_redelivered_after_the_ack_timeout() {
    let drop = DropFault {
        chance: half(),
        redelivery: Redelivery::AckTimeout,
    };
    let mut dropped_somewhere = false;
    for seed in 0..16 {
        let (report, attempt) = dropped_then_redelivered(seed, drop);
        let drops = report.trace.count(FaultKind::BusDrop);
        assert_eq!(attempt as usize, drops + 1, "seed {seed}");
        assert_eq!(
            report.elapsed,
            ACK_TIMEOUT * u32::try_from(drops).expect("few")
        );
        dropped_somewhere |= drops > 0;
    }
    assert!(dropped_somewhere);
}

#[test]
fn delay_hands_the_delivery_out_late() {
    let faults = BusFaults::uniform(SubjectFaults {
        delay: Some(Timed::new(
            Probability::ALWAYS,
            DurationRange::exactly(ms(250)),
        )),
        ..SubjectFaults::none()
    });
    let report = run(0, async |ctx| {
        let inner = Arc::new(ToyBus::new(ACK_TIMEOUT));
        let bus = faulty(&ctx, &inner, faults, "n");
        let mut sub = subscribe(&bus, &[Subject::Changed]).await?;
        bus.publish(changed(1)).await.map_err(failed)?;
        let delivery = next(&mut sub).await?;
        ctx.check(ctx.tracer().elapsed() == ms(250), || {
            format!("arrived at {:?}", ctx.tracer().elapsed())
        })?;
        sub.ack(delivery.id).await.map_err(failed)
    });
    assert_eq!(report.trace.count(FaultKind::BusDelay), 1);
}

#[test]
fn delayed_delivery_survives_a_cancelled_next() {
    let faults = BusFaults::uniform(SubjectFaults {
        delay: Some(Timed::new(
            Probability::ALWAYS,
            DurationRange::exactly(ms(250)),
        )),
        ..SubjectFaults::none()
    });
    run(0, async |ctx| {
        let inner = Arc::new(ToyBus::new(ACK_TIMEOUT));
        let bus = faulty(&ctx, &inner, faults, "n");
        let mut sub = subscribe(&bus, &[Subject::Changed]).await?;
        bus.publish(changed(1)).await.map_err(failed)?;
        let early = tokio::time::timeout(ms(100), sub.next()).await;
        ctx.check(early.is_err(), || "still delayed at 100ms".to_owned())?;
        let delivery = next(&mut sub).await?;
        ctx.check(n(&delivery.envelope) == 1, || {
            "the same delivery".to_owned()
        })?;
        ctx.check(ctx.tracer().elapsed() == ms(250), || "on time".to_owned())
    });
}

#[test]
fn reorder_hands_out_later_deliveries_first() {
    let faults = BusFaults::uniform(SubjectFaults {
        reorder: Some(Reorder::new(Probability::ALWAYS, 3, ms(1)).expect("valid")),
        ..SubjectFaults::none()
    });
    let mut reordered_somewhere = false;
    for seed in 0..16 {
        let mut order = Vec::new();
        let report = run(seed, async |ctx| {
            let inner = Arc::new(ToyBus::new(ACK_TIMEOUT));
            let bus = faulty(&ctx, &inner, faults.clone(), "n");
            let mut sub = subscribe(&bus, &[Subject::Changed]).await?;
            for i in 1..=3 {
                bus.publish(changed(i)).await.map_err(failed)?;
            }
            for _ in 0..3 {
                let delivery = next(&mut sub).await?;
                order.push(n(&delivery.envelope));
                sub.ack(delivery.id).await.map_err(failed)?;
            }
            Ok(())
        });
        let mut sorted = order.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![1, 2, 3], "seed {seed}: each exactly once");
        let reorders = report.trace.count(FaultKind::BusReorder);
        assert_eq!(
            reorders > 0,
            order != vec![1, 2, 3],
            "seed {seed}: {order:?}"
        );
        reordered_somewhere |= reorders > 0;
    }
    assert!(reordered_somewhere);
}

#[test]
fn reorder_window_must_hold_two_and_wait() {
    use crate::plan::InvalidReorder;
    assert_eq!(
        Reorder::new(Probability::ALWAYS, 1, ms(1)),
        Err(InvalidReorder::WindowTooSmall { got: 1 })
    );
    assert_eq!(
        Reorder::new(Probability::ALWAYS, 2, Duration::ZERO),
        Err(InvalidReorder::ZeroWait)
    );
}

#[test]
fn crash_before_ack_restarts_the_consumer_and_the_bus_redelivers() {
    let faults = BusFaults::uniform(SubjectFaults {
        crash_before_ack: half(),
        ..SubjectFaults::none()
    });
    let mut crashed_somewhere = false;
    for seed in 0..16 {
        let report = run(seed, async |ctx| {
            let inner = Arc::new(ToyBus::new(ACK_TIMEOUT));
            let mut node = ctx.node("consumer");
            let bus = ctx.faulty_bus(Arc::clone(&inner), faults.clone(), &node.handle());
            // Registers the group before the publish; never pulls.
            let _registered = subscribe(&bus, &[Subject::Changed]).await?;
            bus.inner().publish(changed(1)).await.map_err(failed)?;
            let tracer = ctx.tracer().clone();
            let handled = node
                .supervise(20, |_incarnation| {
                    let bus = bus.clone();
                    let tracer = tracer.clone();
                    // Each incarnation subscribes again, as a restarted
                    // process would.
                    async move {
                        let mut sub = subscribe(&bus, &[Subject::Changed]).await?;
                        let delivery = next(&mut sub).await?;
                        tracer.note(format!(
                            "handled {} attempt {}",
                            n(&delivery.envelope),
                            delivery.attempt
                        ));
                        sub.ack(delivery.id).await.map_err(failed)?;
                        Ok::<u32, CheckFailed>(delivery.attempt.get())
                    }
                })
                .await
                .map_err(|error| CheckFailed::new(error.to_string()))??;
            ctx.check(inner.unacked() == 0, || "acked in the end".to_owned())?;
            ctx.note(format!("final attempt {handled}"));
            Ok(())
        });
        let crashes = report.trace.count(FaultKind::BusCrashBeforeAck);
        let restarts = report
            .trace
            .records()
            .iter()
            .filter(|r| matches!(r.event, TraceEvent::Restart { .. }))
            .count();
        assert_eq!(crashes, restarts, "seed {seed}");
        assert_eq!(
            report
                .trace
                .notes()
                .filter(|n| n.starts_with("handled"))
                .count(),
            crashes + 1
        );
        assert!(
            report
                .trace
                .notes()
                .any(|note| note == format!("final attempt {}", crashes + 1)),
            "seed {seed}"
        );
        crashed_somewhere |= crashes > 0;
    }
    assert!(crashed_somewhere);
}

#[test]
fn crash_on_publish_never_reaches_the_bus() {
    let faults = BusFaults::uniform(SubjectFaults {
        crash_on_publish: Probability::ALWAYS,
        ..SubjectFaults::none()
    });
    let report = run(0, async |ctx| {
        let inner = Arc::new(ToyBus::new(ACK_TIMEOUT));
        let mut node = ctx.node("publisher");
        let bus = ctx.faulty_bus(Arc::clone(&inner), faults, &node.handle());
        let _sub = subscribe(&bus, &[Subject::Changed]).await?;
        let outcome = node
            .supervise(0, |_| {
                let bus = bus.clone();
                async move { bus.publish(changed(1)).await }
            })
            .await;
        ctx.check(
            matches!(
                outcome,
                Err(SuperviseError::RestartBudgetExhausted { restarts: 0, .. })
            ),
            || format!("{outcome:?}"),
        )?;
        ctx.check(inner.unacked() == 0, || {
            "nothing reached the bus".to_owned()
        })
    });
    assert_eq!(report.trace.count(FaultKind::BusCrashOnPublish), 1);
}

#[test]
fn errors_from_the_inner_bus_pass_through() {
    run(0, async |ctx| {
        let inner = Arc::new(ToyBus::new(ACK_TIMEOUT));
        let bus = faulty(
            &ctx,
            &inner,
            BusFaults::uniform(SubjectFaults::chaos()),
            "n",
        );
        let mut sub = subscribe(&bus, &[Subject::Changed]).await?;
        let unknown = DeliveryId(999);
        let acked = sub.ack(unknown).await;
        ctx.check(acked == Err(BusError::UnknownDelivery(unknown)), || {
            format!("{acked:?}")
        })?;
        let nacked = sub.nack(unknown, ms(1), "x".to_owned()).await;
        ctx.check(nacked == Err(BusError::UnknownDelivery(unknown)), || {
            format!("{nacked:?}")
        })
    });
}

/// Every envelope published under chaos reaches the consumer at least once.
#[test]
fn chaos_keeps_at_least_once_delivery() {
    for seed in 0..8 {
        run(seed, async |ctx| {
            let inner = Arc::new(ToyBus::new(ACK_TIMEOUT));
            let chaos = BusFaults::uniform(SubjectFaults::chaos());
            let publisher = faulty(&ctx, &inner, chaos.clone(), "publisher");
            let consumer = faulty(&ctx, &inner, chaos, "consumer");
            let mut sub = subscribe(&consumer, &[Subject::Changed]).await?;
            let envelopes: Vec<Envelope> = (1..=20).map(changed).collect();
            for envelope in &envelopes {
                publisher.publish(envelope.clone()).await.map_err(failed)?;
            }
            let mut seen = std::collections::BTreeSet::new();
            while seen.len() < envelopes.len() {
                let delivery = next(&mut sub).await?;
                seen.insert(n(&delivery.envelope));
                sub.ack(delivery.id).await.map_err(failed)?;
            }
            ctx.check(seen.len() == 20, || "every envelope".to_owned())
        });
    }
}
