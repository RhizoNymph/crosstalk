//! Each named scenario, provisioned on its own and checked fact by fact
//! through L8 ([`check::observable`]), and all of them composed.

use crate::harness::Harness;
use crate::scenario::{Scenario, ScenarioError, named};
use crate::support::{World, check};

async fn observe<H: Harness>(harness: &H, scenario: Result<Scenario, ScenarioError>) {
    let scenario = scenario.unwrap_or_else(|e| panic!("{e}"));
    check::observable(&World::open(harness, scenario).await).await;
}

/// Every named scenario validates, alone and composed.
pub async fn every_named_scenario_is_valid<H: Harness>(_harness: &H) {
    let all = named::all().unwrap_or_else(|e| panic!("{e}"));
    let everything = named::everything().unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(everything.parts().len(), all.len());
}

/// Every fact of every named scenario, in one world.
pub async fn everything<H: Harness>(harness: &H) {
    observe(harness, named::everything()).await;
}

macro_rules! observed {
    ($($name:ident),* $(,)?) => {
        $(
            #[doc = concat!("Every fact of [`named::", stringify!($name), "`].")]
            pub async fn $name<H: Harness>(harness: &H) {
                observe(harness, named::$name::scenario()).await;
            }
        )*
    };
}

observed! {
    hijacked_wiki,
    late_confirmation,
    impersonation,
    merges,
    hidden_channel,
    suspected,
    declared,
    lone_resource,
    promotion,
    policies,
    topics,
    verdicts,
    dropped_bodies,
    pipeline,
    routes,
    registered,
}
