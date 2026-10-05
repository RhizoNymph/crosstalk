//! Tests of the fixture backend: the generated world and its scenarios,
//! then every read and action through `QueryApi` and `OperatorActions`.

mod actions_support;
mod agents;
mod audit;
mod channels;
mod export;
mod governance;
mod graph;
mod lists;
mod live;
mod outcomes;
mod projections;
mod promotion;
mod reads_support;
mod replay;
mod rules;
mod scenarios;
mod series;
mod topics;
mod transmissions;
mod triage;
mod world;

use std::num::NonZeroU32;
use std::sync::OnceLock;

use crosstalk_spec::aggregates::edge::{TopologyGraph, Weighting};
use crosstalk_spec::aggregates::node::GraphNode;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use crosstalk_spec::support::TimeWindow;

use super::FixtureBackend;
use super::clock::{DAY, NOW, START, ago};
use super::world::{OPERATOR_ONCALL, OPERATOR_RESEARCHER};
use crate::backend::Result;
use crate::url::scope::{Scope, ViewFilter};
use crosstalk_spec::interfaces::l8_surface::QueryApi;
use crosstalk_spec::paging::{Page, PageRequest};

pub const SEED: u64 = 7;

/// One generated world shared by tests that only read.
pub fn shared() -> &'static FixtureBackend {
    static BACKEND: OnceLock<FixtureBackend> = OnceLock::new();
    BACKEND.get_or_init(|| FixtureBackend::try_new(SEED).expect("fixture generates"))
}

/// A world of its own, for tests that act.
pub fn fresh() -> FixtureBackend {
    FixtureBackend::try_new(SEED).expect("fixture generates")
}

/// Every permission: the researcher's in the fixture's config.
pub const ALL: [Permission; 6] = Permission::ALL;

pub fn researcher() -> Caller {
    crate::testing::caller_of(OPERATOR_RESEARCHER, &ALL)
}

pub fn caller(permissions: &[Permission]) -> Caller {
    crate::testing::caller_of(OPERATOR_ONCALL, permissions)
}

pub fn window(start: crosstalk_spec::support::Timestamp) -> TimeWindow {
    TimeWindow::new(start, NOW).expect("window")
}

pub fn scope_with(window: TimeWindow, filter: ViewFilter) -> Scope {
    Scope {
        window,
        topic_version: TopicModelVersion(2),
        filter,
    }
}

/// The UI's default view: the last 24 hours under the latest version.
pub fn day() -> Scope {
    scope_with(window(ago(DAY)), ViewFilter::default())
}

/// The whole generated week.
pub fn week() -> Scope {
    scope_with(window(START), ViewFilter::default())
}

pub fn first<L>(limit: u32) -> PageRequest<L> {
    crate::pages::common::paging::first(NonZeroU32::new(limit).expect("limit"))
}

/// Follows cursors to the end, checking each page's size.
pub async fn collect<T, L>(
    limit: u32,
    mut fetch: impl AsyncFnMut(PageRequest<L>) -> Result<Page<T, L>>,
) -> Vec<T> {
    let mut out = Vec::new();
    let mut request = first(limit);
    loop {
        let page = fetch(PageRequest {
            size: request.size,
            after: request.after.clone(),
        })
        .await
        .expect("page");
        assert!(page.items().len() <= limit as usize);
        if page.next().is_some() {
            assert_eq!(
                page.items().len(),
                limit as usize,
                "only the last page is short"
            );
        }
        let (items, next) = page.into_parts();
        out.extend(items);
        match next {
            Some(cursor) => request.after = Some(cursor),
            None => return out,
        }
    }
}

/// `topology` over a scope's window and filter, as pages call it.
pub async fn graph_of(
    b: &FixtureBackend,
    c: &Caller,
    scope: &Scope,
    weighting: Weighting,
) -> Result<Watermarked<TopologyGraph>> {
    b.topology(c, scope.window, weighting, &scope.topology_filter())
        .await
}

/// The ids of a graph's agent nodes, in node order.
pub fn node_ids(graph: &TopologyGraph) -> Vec<AgentId> {
    graph
        .nodes()
        .iter()
        .filter_map(|node| match node {
            GraphNode::Agent(agent) => Some(agent.id),
            GraphNode::Channel(_) => None,
        })
        .collect()
}
