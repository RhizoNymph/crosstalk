use std::time::Duration;

use crosstalk_spec::support::NonEmpty;

use crate::driver::{CheckFailed, Sim, SimCtx, SimReport};
use crate::plan::{
    ErrorStatus, InvalidErrorStatus, StatusFault, Timed, TruncateFault, UpstreamFaults,
};
use crate::rng::{DurationRange, Probability, Seed};
use crate::trace::{FaultKind, FaultSite};
use crate::upstream::{UpstreamFault, UpstreamFaultInjector};

fn run(scenario: impl AsyncFnOnce(SimCtx) -> Result<(), CheckFailed>) -> SimReport {
    Sim::run(Seed::new(0), move |ctx| scenario(ctx)).expect("passes")
}

fn injector(ctx: &SimCtx, faults: UpstreamFaults) -> UpstreamFaultInjector {
    let node = ctx.node("upstream");
    ctx.upstream_faults(faults, &node.handle())
}

fn status(code: u16) -> ErrorStatus {
    ErrorStatus::new(code).expect("an error status")
}

#[test]
fn error_status_excludes_success_and_out_of_range() {
    assert_eq!(ErrorStatus::new(200), Err(InvalidErrorStatus { got: 200 }));
    assert_eq!(ErrorStatus::new(299), Err(InvalidErrorStatus { got: 299 }));
    assert_eq!(ErrorStatus::new(600), Err(InvalidErrorStatus { got: 600 }));
    assert_eq!(ErrorStatus::new(99), Err(InvalidErrorStatus { got: 99 }));
    assert_eq!(status(300).get(), 300);
    assert_eq!(status(599).get(), 599);
}

#[test]
fn no_faults_draw_nothing() {
    let report = run(async |ctx| {
        let upstream = injector(&ctx, UpstreamFaults::none());
        let all_clean = (0..100).all(|_| upstream.next_exchange().is_none());
        ctx.check(all_clean, || "a fault fired".to_owned())
    });
    assert_eq!(report.trace.faults().count(), 0);
}

#[test]
fn unreachable_wins_over_later_faults() {
    let faults = UpstreamFaults {
        unreachable: Probability::ALWAYS,
        error_status: Some(StatusFault {
            chance: Probability::ALWAYS,
            statuses: NonEmpty::new(status(500)),
        }),
        ..UpstreamFaults::none()
    };
    let report = run(async |ctx| {
        let fault = injector(&ctx, faults).next_exchange();
        ctx.check(fault == Some(UpstreamFault::Unreachable), || {
            format!("{fault:?}")
        })
    });
    let faults: Vec<_> = report.trace.faults().collect();
    assert_eq!(faults.len(), 1);
    assert_eq!(faults[0].kind, FaultKind::UpstreamUnreachable);
    assert_eq!(faults[0].site, FaultSite::Upstream);
}

#[test]
fn status_is_one_of_the_listed() {
    let mut statuses = NonEmpty::new(status(429));
    statuses.push(status(529));
    let faults = UpstreamFaults {
        error_status: Some(StatusFault {
            chance: Probability::ALWAYS,
            statuses,
        }),
        ..UpstreamFaults::none()
    };
    run(async |ctx| {
        let upstream = injector(&ctx, faults);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..50 {
            match upstream.next_exchange() {
                Some(UpstreamFault::Status(status)) => {
                    seen.insert(status.get());
                }
                other => return Err(CheckFailed::new(format!("{other:?}"))),
            }
        }
        ctx.check(seen == [429, 529].into(), || format!("{seen:?}"))
    });
}

#[test]
fn truncate_cuts_within_the_bound() {
    let faults = UpstreamFaults {
        truncate: Some(TruncateFault {
            chance: Probability::ALWAYS,
            max_chunks: 3,
        }),
        ..UpstreamFaults::none()
    };
    run(async |ctx| {
        let upstream = injector(&ctx, faults);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..100 {
            match upstream.next_exchange() {
                Some(UpstreamFault::Truncate { after_chunks }) => {
                    seen.insert(after_chunks);
                }
                other => return Err(CheckFailed::new(format!("{other:?}"))),
            }
        }
        ctx.check(seen == [0, 1, 2, 3].into(), || format!("{seen:?}"))
    });
}

#[test]
fn stall_lasts_its_drawn_duration() {
    let faults = UpstreamFaults {
        stall: Some(Timed::new(
            Probability::ALWAYS,
            DurationRange::exactly(Duration::from_secs(2)),
        )),
        ..UpstreamFaults::none()
    };
    run(async |ctx| {
        let fault = injector(&ctx, faults).next_exchange();
        ctx.check(
            fault == Some(UpstreamFault::Stall(Duration::from_secs(2))),
            || format!("{fault:?}"),
        )
    });
}
