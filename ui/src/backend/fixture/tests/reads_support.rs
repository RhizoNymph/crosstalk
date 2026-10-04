//! Helpers shared by the read tests.

use std::num::{NonZeroU16, NonZeroU32};

use super::super::world::ChannelKey;
use super::{first, researcher, scope_with, shared, week};
use crate::backend::Backend;
use crate::contract::graph::{TransmissionSelector, TransmissionSummary};
use crate::contract::research::ProjectionParams;
use crate::contract::scope::{Scope, TopologyFilter};
use crate::contract::search::{SearchMode, SearchRequest, SearchText};

pub const BIG: u32 = 100_000;

pub fn n(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).expect("non-zero")
}

pub fn params(seed: u64, limit: u32) -> ProjectionParams {
    ProjectionParams::new(NonZeroU16::new(15).expect("15"), 0.1, seed, n(limit)).expect("params")
}

pub fn search(text: &str, mode: SearchMode) -> SearchRequest {
    SearchRequest {
        text: SearchText::new(text).expect("text"),
        mode,
    }
}

pub fn channel(key: ChannelKey) -> crosstalk_spec::ids::ChannelId {
    shared().world.scenario.channel(key).expect("channel")
}

pub fn agent(key: &str) -> crosstalk_spec::ids::AgentId {
    shared().world.scenario.agent(key).expect("agent")
}

pub fn with(filter: TopologyFilter) -> Scope {
    scope_with(week().window, filter)
}

pub async fn all_transmissions(scope: &Scope) -> Vec<TransmissionSummary> {
    shared()
        .transmissions(
            &researcher(),
            scope,
            &TransmissionSelector::All,
            &first(BIG),
        )
        .await
        .expect("transmissions")
        .items
}

pub fn sum_shares(shares: impl Iterator<Item = f64>) -> f64 {
    shares.sum()
}
