//! Helpers shared by the read tests.

use std::collections::HashSet;
use std::num::NonZeroU32;

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::filter::FalseDetections;
use crosstalk_spec::aggregates::projection::{ProjectionLimit, ProjectionParams};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId};
use crosstalk_spec::interfaces::l6_analysis::SearchHit;
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::lists::{SearchMode, SearchRequest};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionSummary;
use crosstalk_spec::paging::{Page, PageRequest, SearchList};
use crosstalk_spec::support::NonBlank;

use super::super::FixtureBackend;
use super::super::queries::linked::resolve_version;
use super::super::queries::{Ctx, transmissions};
use super::super::world::{ChannelKey, TxRecord};
use super::{scope_with, shared, week};
use crate::backend::{Backend, Result};
use crate::url::scope::{Scope, ViewFilter};

pub const BIG: u32 = 100_000;

pub fn n(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).expect("non-zero")
}

pub fn params(seed: u64, limit: u32) -> ProjectionParams {
    ProjectionParams::new(ProjectionLimit::new(limit).expect("limit"), 15, 100, seed)
        .expect("params")
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

/// A reference filter over the world's records, independent of the linked
/// views: empty lists do not restrict, non-empty lists combine with AND,
/// agents match the sender OR the reader after alias resolution, channels
/// match after supersession, topics are read under the scope's version and
/// outliers never match a topic filter. Transmissions are tested by when
/// they were opened, and unconfirmed ones are kept.
pub struct ScopeFilter<'a> {
    ctx: &'a Ctx<'a>,
    window: crosstalk_spec::support::TimeWindow,
    pub version: TopicModelVersion,
    agents: HashSet<AgentId>,
    channels: HashSet<ChannelId>,
    routes: HashSet<RouteKind>,
    topics: HashSet<TopicId>,
    exclude_false: bool,
}

impl<'a> ScopeFilter<'a> {
    /// Resolves the scope's version as every linked view does.
    pub fn new(ctx: &'a Ctx<'a>, scope: &Scope) -> Result<Self> {
        let version = resolve_version(ctx.world, ctx.state, &scope.topology_filter())?;
        let f = &scope.filter;
        Ok(Self {
            ctx,
            window: scope.window,
            version,
            agents: f.agents.iter().map(|a| ctx.agent(*a)).collect(),
            channels: f.channels.iter().map(|c| ctx.channel(*c)).collect(),
            routes: f.route_kinds.iter().copied().collect(),
            topics: f.topics.iter().copied().collect(),
            exclude_false: f.false_detections == FalseDetections::Exclude,
        })
    }

    /// Whether a transmission is in the scope. Unconfirmed transmissions
    /// match an agent filter by their reader only, and never match a topic
    /// filter.
    pub fn keeps(&self, record: &TxRecord) -> bool {
        let t = &record.transmission;
        if !self.window.contains(t.opened_at) {
            return false;
        }
        if !self.agents.is_empty() {
            let to = self.ctx.agent(t.to);
            let from = record.from.map(|f| self.ctx.agent(f));
            if !self.agents.contains(&to) && !from.is_some_and(|f| self.agents.contains(&f)) {
                return false;
            }
        }
        if !self.channels.is_empty() {
            match &t.route {
                Route::Channel(c) if self.channels.contains(&self.ctx.channel(*c)) => {}
                _ => return false,
            }
        }
        if !self.routes.is_empty() && !self.routes.contains(&RouteKind::from(&t.route)) {
            return false;
        }
        if !self.topics.is_empty()
            && !record
                .topic(self.version)
                .is_some_and(|topic| self.topics.contains(&topic))
        {
            return false;
        }
        if self.exclude_false && self.ctx.verdict(t.id) == Some(Verdict::FalseDetection) {
            return false;
        }
        true
    }
}

/// The rows of every transmission `b`'s world holds in the scope (opened in
/// its window, matching its filter as [`ScopeFilter`] reads it), newest id
/// first, read from the world directly.
pub async fn rows_in(b: &FixtureBackend, scope: &Scope) -> Vec<TransmissionSummary> {
    let state = b.state.read().await;
    let ctx = Ctx::new(&b.world, &state);
    let filter = ScopeFilter::new(&ctx, scope).expect("scope");
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
