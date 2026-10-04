//! Helpers shared by the read tests.

use std::num::{NonZeroU16, NonZeroU32};

use crosstalk_spec::interfaces::l6_analysis::SearchHit;
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::lists::{SearchMode, SearchRequest};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionSummary;
use crosstalk_spec::paging::{Page, PageRequest, SearchList};
use crosstalk_spec::support::NonBlank;

use super::super::FixtureBackend;
use super::super::queries::scope::Filter;
use super::super::queries::{Ctx, transmissions};
use super::super::world::ChannelKey;
use super::{scope_with, shared, week};
use crate::backend::{Backend, Result};
use crate::contract::research::ProjectionParams;
use crate::url::scope::{Scope, ViewFilter};

pub const BIG: u32 = 100_000;

pub fn n(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).expect("non-zero")
}

pub fn params(seed: u64, limit: u32) -> ProjectionParams {
    ProjectionParams::new(NonZeroU16::new(15).expect("15"), 0.1, seed, n(limit)).expect("params")
}

pub fn search(text: &str, mode: SearchMode) -> SearchRequest {
    SearchRequest {
        mode,
        text: NonBlank::new(text).expect("text"),
    }
}

/// A page of `search` over a scope's window and filter, as the explore
/// page asks for it.
pub async fn search_in(
    b: &FixtureBackend,
    c: &Caller,
    request: &SearchRequest,
    scope: &Scope,
    page: &PageRequest<SearchList>,
) -> Result<Page<SearchHit, SearchList>> {
    b.search(
        c,
        request,
        Some(scope.window),
        &scope.topology_filter(),
        page,
    )
    .await
    .map(|results| results.page)
}

pub fn channel(key: ChannelKey) -> crosstalk_spec::ids::ChannelId {
    shared().world.scenario.channel(key).expect("channel")
}

pub fn agent(key: &str) -> crosstalk_spec::ids::AgentId {
    shared().world.scenario.agent(key).expect("agent")
}

pub fn with(filter: ViewFilter) -> Scope {
    scope_with(week().window, filter)
}

/// The rows of every transmission `b`'s world holds in the scope (opened in
/// its window, matching its filter as the fixture's scope filter reads it),
/// newest id first, read from the world directly.
pub async fn rows_in(b: &FixtureBackend, scope: &Scope) -> Vec<TransmissionSummary> {
    let state = b.state.read().await;
    let ctx = Ctx::new(&b.world, &state);
    let filter = Filter::new(&ctx, scope).expect("scope");
    let mut rows: Vec<TransmissionSummary> = b
        .world
        .transmissions
        .iter()
        .filter(|record| filter.keeps(record))
        .map(|record| transmissions::summary(&ctx, record, filter.version))
        .collect();
    rows.sort_by_key(|row| std::cmp::Reverse(row.id));
    rows
}

pub async fn all_transmissions(scope: &Scope) -> Vec<TransmissionSummary> {
    rows_in(shared(), scope).await
}

/// The grid the time brush asks for: `points` steps over `window` (fewer
/// when they do not divide it), on the fixture's bucket width.
pub fn grid(
    window: crosstalk_spec::support::TimeWindow,
    points: u32,
) -> crosstalk_spec::aggregates::series::SeriesGrid {
    crate::data::timeline::timeline_grid(window, super::super::clock::BUCKET, n(points))
        .expect("grid")
}

pub fn sum_shares(shares: impl Iterator<Item = f64>) -> f64 {
    shares.sum()
}
