use std::time::Duration;

use crosstalk_spec::support::{Clock, Timestamp};

use crate::clock::ClockStep;
use crate::driver::{Sim, SimConfig};
use crate::rng::Seed;
use crate::trace::{FaultKind, FaultSite, NodeName};

const SECOND: u64 = 1_000_000;

fn read(clock: &dyn Clock) -> Timestamp {
    clock.now()
}

#[test]
fn sim_clock_starts_at_the_epoch_and_follows_paused_time() {
    let epoch = SimConfig::default().epoch;
    let report = Sim::run(Seed::new(0), |ctx| async move {
        let clock = ctx.clock();
        ctx.check(read(&clock) == epoch, || "starts at the epoch".to_owned())?;
        tokio::time::sleep(Duration::from_secs(5)).await;
        let later = Timestamp::from_micros(epoch.as_micros() + 5 * SECOND);
        ctx.check(read(&clock) == later, || format!("{:?}", clock.now()))
    })
    .expect("passes");
    assert_eq!(report.elapsed, Duration::from_secs(5));
}

#[test]
fn backward_step_repeats_earlier_readings() {
    let report = Sim::run(Seed::new(0), |ctx| async move {
        let clock = ctx.clock();
        tokio::time::sleep(Duration::from_secs(10)).await;
        let before = clock.now();
        clock.step(ClockStep::Back(Duration::from_secs(3)));
        let after = clock.now();
        ctx.check(after.as_micros() + 3 * SECOND == before.as_micros(), || {
            format!("{before:?} then {after:?}")
        })?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        ctx.check(clock.now() == before, || "repeats the reading".to_owned())
    })
    .expect("passes");
    let steps: Vec<_> = report.trace.faults().collect();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].kind, FaultKind::ClockStep);
    assert_eq!(steps[0].site, FaultSite::Clock);
    assert_eq!(steps[0].node, NodeName::new("sim"));
}

#[test]
fn skewed_clock_steps_independently() {
    Sim::run(Seed::new(0), |ctx| async move {
        let base = ctx.clock();
        let other = base.skewed("node-b", ClockStep::Forward(Duration::from_secs(1)));
        ctx.check(
            other.now().as_micros() == base.now().as_micros() + SECOND,
            || "skewed by a second".to_owned(),
        )?;
        other.step(ClockStep::Back(Duration::from_secs(4)));
        ctx.check(
            other.now().as_micros() + 3 * SECOND == base.now().as_micros(),
            || "only the skewed clock stepped".to_owned(),
        )?;
        ctx.check(other.node() == &NodeName::new("node-b"), || {
            "named".to_owned()
        })
    })
    .expect("passes");
}

#[test]
fn clones_share_steps() {
    Sim::run(Seed::new(0), |ctx| async move {
        let a = ctx.clock();
        let b = a.clone();
        a.step(ClockStep::Forward(Duration::from_secs(60)));
        ctx.check(a.now() == b.now(), || "clones share the offset".to_owned())
    })
    .expect("passes");
}

#[test]
fn clock_saturates_at_the_unix_epoch() {
    Sim::run(Seed::new(0), |ctx| async move {
        let clock = ctx.clock();
        clock.step(ClockStep::Back(Duration::from_secs(u64::MAX / 2)));
        ctx.check(clock.now() == Timestamp::from_micros(0), || {
            format!("{:?}", clock.now())
        })
    })
    .expect("passes");
}
