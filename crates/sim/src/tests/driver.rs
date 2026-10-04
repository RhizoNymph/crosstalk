use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::events::Subject;
use crosstalk_spec::interfaces::l2_transport::{BusError, EventBus, Subscription};

use super::toy_bus::{ToyBus, changed, group, n, retry};
use crate::driver::{
    CheckFailed, FailureCause, SeedEnvError, SeedSelection, Sim, SimConfig, SimCtx,
};
use crate::plan::{FaultPlan, StoreFaults};
use crate::rng::{DurationRange, Seed};
use crate::store::InjectedFault;
use crate::trace::{FaultKind, TraceEvent};

fn bus_failed(error: BusError) -> CheckFailed {
    CheckFailed::new(format!("bus error: {error:?}"))
}

/// Three publishers on their own nodes and one consumer, all under
/// `FaultPlan::chaos()`, with a store call per handled delivery. Records
/// what the consumer sees, in order.
async fn chaos_pipeline(ctx: SimCtx) -> Result<(), CheckFailed> {
    let plan = FaultPlan::chaos();
    let bus = Arc::new(ToyBus::new(Duration::from_millis(500)));
    let consumer = ctx.node("consumer");
    let consumer_bus = ctx.faulty_bus(Arc::clone(&bus), plan.bus.clone(), &consumer.handle());
    let store = ctx.faulty_store((), StoreFaults::chaos(), &consumer.handle());
    let mut subscription = consumer_bus
        .subscribe(&[Subject::Changed], group("g"), retry())
        .await
        .map_err(bus_failed)?;

    let mut publishers = Vec::new();
    for p in 0..3u64 {
        let node = ctx.node(&format!("publisher-{p}"));
        let publisher_bus = ctx.faulty_bus(Arc::clone(&bus), plan.bus.clone(), &node.handle());
        let mut rng = ctx.rng();
        let gap = DurationRange::new(Duration::ZERO, Duration::from_millis(20)).expect("ordered");
        publishers.push(ctx.spawn("publisher", async move {
            let _node = node;
            for i in 0..5 {
                tokio::time::sleep(rng.duration_in(gap)).await;
                publisher_bus.publish(changed(p * 100 + i)).await?;
            }
            Ok::<(), BusError>(())
        }));
    }

    ctx.step("consume");
    let mut seen = HashSet::new();
    while seen.len() < 15 {
        let delivery = match subscription.next().await {
            Some(Ok(delivery)) => delivery,
            Some(Err(error)) => return Err(bus_failed(error)),
            None => return Err(CheckFailed::new("bus closed")),
        };
        let id = n(&delivery.envelope);
        let stored = store
            .call("record", |fault: InjectedFault| fault, async { Ok(()) })
            .await;
        ctx.note(format!(
            "got {id} attempt {} store {stored:?}",
            delivery.attempt
        ));
        seen.insert(id);
        subscription.ack(delivery.id).await.map_err(bus_failed)?;
    }
    for publisher in publishers {
        let published = publisher
            .join()
            .await
            .map_err(|error| CheckFailed::new(error.to_string()))?;
        published.map_err(bus_failed)?;
    }
    Ok(())
}

#[test]
fn same_seed_gives_the_same_trace() {
    for seed in 0..8 {
        let first = Sim::run(Seed::new(seed), chaos_pipeline).expect("passes");
        let second = Sim::run(Seed::new(seed), chaos_pipeline).expect("passes");
        assert_eq!(first.trace_hash(), second.trace_hash(), "seed {seed}");
        assert_eq!(first.trace, second.trace, "seed {seed}");
        assert_eq!(first.elapsed, second.elapsed, "seed {seed}");
        assert!(
            first.trace.faults().next().is_some(),
            "seed {seed} injected nothing"
        );
    }
}

#[test]
fn different_seeds_explore_different_orderings() {
    let orders: HashSet<Vec<String>> = (0..16)
        .map(|seed| {
            let report = Sim::run(Seed::new(seed), chaos_pipeline).expect("passes");
            report.trace.notes().map(str::to_owned).collect()
        })
        .collect();
    assert!(
        orders.len() >= 12,
        "only {} distinct orderings",
        orders.len()
    );
}

#[test]
fn failed_check_reports_seed_and_step() {
    let failure = Sim::run(Seed::new(7), |ctx| async move {
        ctx.step("publish");
        ctx.note("something");
        ctx.step("verify");
        ctx.check(false, || "the invariant broke".to_owned())
    })
    .expect_err("fails");
    assert_eq!(failure.seed, Seed::new(7));
    assert_eq!(failure.step, 3);
    assert_eq!(failure.last_step.as_deref(), Some("verify"));
    assert_eq!(
        failure.cause,
        FailureCause::Check(CheckFailed::new("the invariant broke"))
    );
    assert_eq!(failure.tail.len(), 3);
    let text = failure.to_string();
    assert!(text.contains("seed 7 at step 3"), "{text}");
    assert!(text.contains("rerun with CROSSTALK_SIM_SEED=7"), "{text}");
}

#[test]
fn panic_reports_seed_step_and_simulated_time() {
    let failure = Sim::run(Seed::new(3), |ctx| async move {
        ctx.step("before the panic");
        tokio::time::sleep(Duration::from_secs(2)).await;
        ctx.note("late");
        panic!("boom");
    })
    .expect_err("fails");
    assert_eq!(failure.seed, Seed::new(3));
    assert_eq!(failure.step, 2);
    assert_eq!(failure.last_step.as_deref(), Some("before the panic"));
    assert_eq!(failure.at, Duration::from_secs(2));
    assert_eq!(
        failure.cause,
        FailureCause::Panicked {
            message: "boom".to_owned()
        }
    );
}

#[test]
fn spawned_task_panic_fails_the_run() {
    let failure = Sim::run(Seed::new(1), |ctx| async move {
        let _worker = ctx.spawn("worker", async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            panic!("worker boom");
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(())
    })
    .expect_err("fails");
    assert_eq!(
        failure.cause,
        FailureCause::TaskPanicked {
            task: "worker".to_owned(),
            message: "worker boom".to_owned()
        }
    );
}

#[test]
fn joined_task_reports_its_panic() {
    Sim::run(Seed::new(1), |ctx| async move {
        let worker = ctx.spawn("worker", async { panic!("joined boom") });
        let joined: Result<(), _> = worker.join().await;
        ctx.check(joined.is_err(), || "the join reports the panic".to_owned())
    })
    .expect_err("the panic still fails the run");
}

#[test]
fn aborted_task_is_cancelled_not_failed() {
    Sim::run(Seed::new(1), |ctx| async move {
        let worker = ctx.spawn("worker", std::future::pending::<()>());
        worker.abort();
        let joined = worker.join().await;
        ctx.check(joined.is_err(), || "cancelled".to_owned())
    })
    .expect("an abort is not a failure");
}

#[test]
fn deadlock_times_out_with_its_seed() {
    let config = SimConfig {
        time_limit: Duration::from_secs(10),
        ..SimConfig::default()
    };
    let failure = Sim::run_with(&config, Seed::new(5), |ctx| async move {
        ctx.step("waiting forever");
        std::future::pending::<()>().await;
        Ok(())
    })
    .expect_err("times out");
    assert_eq!(failure.seed, Seed::new(5));
    assert_eq!(
        failure.cause,
        FailureCause::TimedOut {
            limit: Duration::from_secs(10)
        }
    );
    assert_eq!(failure.last_step.as_deref(), Some("waiting forever"));
}

#[test]
fn rerunning_the_reported_seed_reproduces_the_failure() {
    let scenario = |ctx: SimCtx| async move {
        let mut rng = ctx.rng();
        let roll = rng.next_u64() % 4;
        ctx.note(format!("rolled {roll}"));
        ctx.check(roll != 0, || "rolled zero".to_owned())
    };
    let seeds = SeedSelection::Sweep(NonZeroU32::new(64).expect("non-zero"));
    let failure =
        Sim::sweep(&SimConfig::default(), seeds, scenario).expect_err("some seed rolls 0");
    let rerun = Sim::run(failure.seed, scenario).expect_err("the same seed fails again");
    assert_eq!(rerun, failure);
}

#[test]
fn sweep_runs_every_selected_seed_in_order() {
    let mut seen = Vec::new();
    let summaries = Sim::sweep(
        &SimConfig::default(),
        SeedSelection::Sweep(NonZeroU32::new(4).expect("non-zero")),
        |ctx| {
            seen.push(ctx.seed());
            async { Ok(()) }
        },
    )
    .expect("passes");
    let expected: Vec<Seed> = (0..4).map(Seed::new).collect();
    assert_eq!(seen, expected);
    assert_eq!(
        summaries.iter().map(|s| s.seed).collect::<Vec<_>>(),
        expected
    );
}

#[test]
fn sweep_of_one_seed_runs_only_it() {
    let seeds: Vec<Seed> = SeedSelection::One(Seed::new(41)).seeds().collect();
    assert_eq!(seeds, vec![Seed::new(41)]);
}

#[test]
fn seed_selection_parses_the_environment_values() {
    assert_eq!(SeedSelection::parse(None, None), Ok(None));
    assert_eq!(
        SeedSelection::parse(Some("17"), None),
        Ok(Some(SeedSelection::One(Seed::new(17))))
    );
    assert_eq!(
        SeedSelection::parse(None, Some(" 100 ")),
        Ok(Some(SeedSelection::Sweep(
            NonZeroU32::new(100).expect("non-zero")
        )))
    );
    assert_eq!(
        SeedSelection::parse(Some("1"), Some("2")),
        Err(SeedEnvError::Both)
    );
    assert_eq!(
        SeedSelection::parse(Some("-1"), None),
        Err(SeedEnvError::InvalidSeed {
            value: "-1".to_owned()
        })
    );
    assert_eq!(
        SeedSelection::parse(None, Some("0")),
        Err(SeedEnvError::InvalidSweep {
            value: "0".to_owned()
        })
    );
}

#[test]
fn restarts_and_steps_are_traced() {
    let report = Sim::run(Seed::new(0), |ctx| async move {
        ctx.step("one");
        Ok(())
    })
    .expect("passes");
    assert_eq!(
        report.trace.records()[0].event,
        TraceEvent::Step("one".to_owned())
    );
    assert_eq!(report.trace.count(FaultKind::BusDrop), 0);
}

crate::sim_test! {
    /// The macro declares a test that sweeps the default seeds.
    fn macro_declares_a_seed_sweep(ctx) {
        tokio::time::sleep(Duration::from_secs(3600)).await;
        ctx.check(ctx.tracer().elapsed() == Duration::from_secs(3600), || {
            "an hour of paused time".to_owned()
        })
    }
}

crate::sim_test! {
    fn macro_takes_a_config(ctx) with SimConfig {
        default_seeds: NonZeroU32::MIN,
        ..SimConfig::default()
    } => {
        ctx.check(ctx.seed() == Seed::new(0) || std::env::var(crate::SEED_VAR).is_ok()
            || std::env::var(crate::SEEDS_VAR).is_ok(), || {
            "one default seed".to_owned()
        })
    }
}
