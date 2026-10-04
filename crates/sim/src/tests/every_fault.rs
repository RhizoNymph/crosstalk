//! One scenario that exercises every fault the kit can inject, checked
//! against [`FaultKind::ALL`] over several seeds.

use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::events::Subject;
use crosstalk_spec::interfaces::l2_transport::{BusError, EventBus, Subscription};
use crosstalk_spec::support::NonEmpty;

use super::toy_bus::{ToyBus, changed, group, retry, watermark};
use crate::clock::ClockStep;
use crate::driver::{CheckFailed, Sim, SimCtx};
use crate::plan::{
    BusFaults, DropFault, ErrorStatus, Redelivery, Reorder, StatusFault, StoreFaults,
    SubjectFaults, Timed, TruncateFault, UpstreamFaults,
};
use crate::rng::{DurationRange, Probability, Seed};
use crate::store::InjectedFault;
use crate::trace::FaultKind;

fn failed(error: BusError) -> CheckFailed {
    CheckFailed::new(format!("bus error: {error:?}"))
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn only(faults: SubjectFaults) -> BusFaults {
    BusFaults::uniform(faults)
}

/// Consumes from group `name` until `quiet` passes with no delivery.
async fn drain(
    bus: &impl EventBus,
    name: &str,
    subject: Subject,
    quiet: Duration,
) -> Result<usize, CheckFailed> {
    let mut sub = bus
        .subscribe(&[subject], group(name), retry())
        .await
        .map_err(failed)?;
    let mut handled = 0;
    while let Ok(next) = tokio::time::timeout(quiet, sub.next()).await {
        match next {
            Some(Ok(delivery)) => {
                handled += 1;
                sub.ack(delivery.id).await.map_err(failed)?;
            }
            Some(Err(error)) => return Err(failed(error)),
            None => break,
        }
    }
    Ok(handled)
}

async fn every_fault(ctx: SimCtx) -> Result<(), CheckFailed> {
    let inner = Arc::new(ToyBus::new(Duration::from_secs(1)));

    ctx.step("clock");
    ctx.clock().step(ClockStep::Back(Duration::from_secs(1)));

    ctx.step("bus: duplicate, delay, reorder, drop");
    let consumer = ctx.node("consumer");
    let noisy = only(SubjectFaults {
        delay: Some(Timed::new(
            Probability::ALWAYS,
            DurationRange::exactly(ms(5)),
        )),
        reorder: Some(Reorder::new(Probability::ALWAYS, 4, ms(1)).expect("valid")),
        ..SubjectFaults::none()
    });
    let noisy_bus = ctx.faulty_bus(Arc::clone(&inner), noisy, &consumer.handle());
    let mut noisy_sub = noisy_bus
        .subscribe(&[Subject::Changed], group("noisy"), retry())
        .await
        .map_err(failed)?;
    let lossy = only(SubjectFaults {
        drop: Some(DropFault {
            chance: Probability::ALWAYS,
            redelivery: Redelivery::Nack {
                after: DurationRange::exactly(ms(10)),
            },
        }),
        ..SubjectFaults::none()
    });
    let lossy_bus = ctx.faulty_bus(Arc::clone(&inner), lossy, &consumer.handle());
    let mut lossy_sub = lossy_bus
        .subscribe(&[Subject::Changed], group("lossy"), retry())
        .await
        .map_err(failed)?;
    let publisher = ctx.node("publisher");
    let duplicating = only(SubjectFaults {
        duplicate: Probability::ALWAYS,
        ..SubjectFaults::none()
    });
    let publisher_bus = ctx.faulty_bus(Arc::clone(&inner), duplicating, &publisher.handle());
    for i in 1..=8 {
        publisher_bus.publish(changed(i)).await.map_err(failed)?;
    }
    for _ in 0..16 {
        match noisy_sub.next().await {
            Some(Ok(delivery)) => noisy_sub.ack(delivery.id).await.map_err(failed)?,
            other => return Err(CheckFailed::new(format!("{other:?}"))),
        }
    }
    // Every delivery is dropped, so this never returns one.
    let lost = tokio::time::timeout(ms(50), lossy_sub.next()).await;
    ctx.check(lost.is_err(), || format!("{lost:?}"))?;

    ctx.step("bus: crash on publish");
    let mut crashing_publisher = ctx.node("crashing-publisher");
    let crash_publish = only(SubjectFaults {
        crash_on_publish: Probability::ALWAYS,
        ..SubjectFaults::none()
    });
    let crash_bus = ctx.faulty_bus(
        Arc::clone(&inner),
        crash_publish,
        &crashing_publisher.handle(),
    );
    let outcome = crashing_publisher
        .supervise(0, |_| {
            let bus = crash_bus.clone();
            async move { bus.publish(changed(100)).await }
        })
        .await;
    ctx.check(outcome.is_err(), || format!("{outcome:?}"))?;

    ctx.step("bus: crash before ack");
    let mut crashing_consumer = ctx.node("crashing-consumer");
    let crash_ack = BusFaults::none().with_subject(
        Subject::WatermarkAdvanced,
        SubjectFaults {
            crash_before_ack: Probability::ALWAYS,
            ..SubjectFaults::none()
        },
    );
    let ack_bus = ctx.faulty_bus(Arc::clone(&inner), crash_ack, &crashing_consumer.handle());
    let _registered = ack_bus
        .subscribe(&[Subject::WatermarkAdvanced], group("acker"), retry())
        .await
        .map_err(failed)?;
    inner.publish(watermark(200)).await.map_err(failed)?;
    let outcome = crashing_consumer
        .supervise(0, |_| {
            let bus = ack_bus.clone();
            async move { drain(&bus, "acker", Subject::WatermarkAdvanced, ms(100)).await }
        })
        .await;
    ctx.check(outcome.is_err(), || format!("{outcome:?}"))?;

    ctx.step("store");
    let storer = ctx.node("storer");
    let slow_failing = StoreFaults {
        latency: Some(Timed::new(
            Probability::ALWAYS,
            DurationRange::exactly(ms(3)),
        )),
        fail_before: Probability::ALWAYS,
        ..StoreFaults::none()
    };
    let store = ctx.faulty_store((), slow_failing, &storer.handle());
    let failed_before = store
        .call("put", |f: InjectedFault| f, async { Ok(()) })
        .await;
    ctx.check(failed_before.is_err(), || "failed before".to_owned())?;
    let failing_after = StoreFaults {
        fail_after: Probability::ALWAYS,
        ..StoreFaults::none()
    };
    let store = ctx.faulty_store((), failing_after, &storer.handle());
    let failed_after = store
        .call("put", |f: InjectedFault| f, async { Ok(()) })
        .await;
    ctx.check(failed_after.is_err(), || "failed after".to_owned())?;
    let mut crashing_storer = ctx.node("crashing-storer");
    let crashing = StoreFaults {
        crash_after: Probability::ALWAYS,
        ..StoreFaults::none()
    };
    let store = ctx.faulty_store((), crashing, &crashing_storer.handle());
    let outcome = crashing_storer
        .supervise(0, |_| {
            let store = store.clone();
            async move {
                store
                    .call("put", |f: InjectedFault| f, async { Ok(()) })
                    .await
            }
        })
        .await;
    ctx.check(outcome.is_err(), || format!("{outcome:?}"))?;

    ctx.step("upstream");
    let upstream = ctx.node("upstream");
    let status = ErrorStatus::new(529).expect("an error status");
    let plans = [
        UpstreamFaults {
            unreachable: Probability::ALWAYS,
            ..UpstreamFaults::none()
        },
        UpstreamFaults {
            error_status: Some(StatusFault {
                chance: Probability::ALWAYS,
                statuses: NonEmpty::new(status),
            }),
            ..UpstreamFaults::none()
        },
        UpstreamFaults {
            truncate: Some(TruncateFault {
                chance: Probability::ALWAYS,
                max_chunks: 4,
            }),
            ..UpstreamFaults::none()
        },
        UpstreamFaults {
            stall: Some(Timed::new(
                Probability::ALWAYS,
                DurationRange::exactly(ms(500)),
            )),
            ..UpstreamFaults::none()
        },
    ];
    for plan in plans {
        let fault = ctx
            .upstream_faults(plan, &upstream.handle())
            .next_exchange();
        ctx.check(fault.is_some(), || "an upstream fault".to_owned())?;
    }
    Ok(())
}

#[test]
fn every_fault_kind_fires() {
    for seed in 0..4 {
        let report = Sim::run(Seed::new(seed), every_fault).expect("passes");
        for kind in FaultKind::ALL {
            assert!(
                report.trace.count(kind) > 0,
                "seed {seed}: {kind:?} never fired"
            );
        }
    }
}

#[test]
fn crash_kinds_are_the_three_crashes() {
    let crashes: Vec<_> = FaultKind::ALL
        .into_iter()
        .filter(|k| k.is_crash())
        .collect();
    assert_eq!(
        crashes,
        vec![
            FaultKind::BusCrashOnPublish,
            FaultKind::BusCrashBeforeAck,
            FaultKind::StoreCrashAfter
        ]
    );
}
