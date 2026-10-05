//! Reading the scenario back through the L8 surface, as the UI does: only
//! `QueryApi` calls, generic over the surface, so the same readers work on
//! the in-process composition, on `Live`, or on a client over HTTP.
//!
//! Each reader returns what it found and leaves judging it to the caller
//! (the smoke's tests, or a demo checking that its traffic landed).

use std::time::Duration;

use crosstalk_spec::aggregates::edge::{
    EdgeSelector, EdgeTransmission, TopologyFilter, WeightedEdge, Weighting,
};
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::channels::ChannelRow;
use crosstalk_spec::interfaces::l8_surface::errors::QueryError;
use crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence;
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::lists::{AgentFilter, ChannelFilter};
use crosstalk_spec::interfaces::l8_surface::summary::{TransmissionSelection, TransmissionSummary};
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryApi};
use crosstalk_spec::observed::agent::IdentityEvidence;
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::interfaces::l5_flow::transmissions::{
    TransmissionQuery, TransmissionStore, TransmissionStoreError,
};

use crate::scenario::Scenario;

/// The page size every reader asks for: more than the scenario can make.
const PAGE: u16 = 100;

/// Why a reader could not read.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ReadError {
    #[error("the surface refused the query: {0:?}")]
    Query(QueryError),
    #[error("the scenario's window is empty or out of range")]
    Window,
    #[error("the page size is out of range")]
    PageSize,
    #[error("the transmission selection is invalid")]
    Selection,
    #[error("the edge from {from:?} to {to:?} is a self-edge")]
    SelfEdge { from: AgentId, to: AgentId },
    #[error("the transmission store refused the list: {0:?}")]
    Transmissions(TransmissionStoreError),
}

impl From<QueryError> for ReadError {
    fn from(error: QueryError) -> Self {
        Self::Query(error)
    }
}

fn first_page<L>() -> Result<PageRequest<L>, ReadError> {
    Ok(PageRequest {
        size: PageSize::new(PAGE).map_err(|_| ReadError::PageSize)?,
        after: None,
    })
}

/// The bucket-aligned window around the whole scenario: from its start
/// rounded down to `bucket` to its end rounded up, past the last exchange.
pub fn window(scenario: &Scenario, bucket: Duration) -> Result<TimeWindow, ReadError> {
    let width = u64::try_from(bucket.as_micros())
        .ok()
        .filter(|width| *width > 0)
        .ok_or(ReadError::Window)?;
    let start = scenario.start.as_micros() / width * width;
    let end = (scenario.ends_at().as_micros() / width + 1)
        .checked_mul(width)
        .ok_or(ReadError::Window)?;
    TimeWindow::new(Timestamp::from_micros(start), Timestamp::from_micros(end))
        .map_err(|_| ReadError::Window)
}

/// The scenario's two agents as the surface knows them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Agents {
    /// The writer (session `a`).
    pub a: AgentId,
    /// The reader (session `b`).
    pub b: AgentId,
}

/// What the surface resolved the scenario's sessions to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRead {
    /// Every canonical agent listed in the window.
    pub listed: Vec<AgentId>,
    /// A and B, when each session's id is evidence of exactly one listed
    /// agent.
    pub agents: Option<Agents>,
}

/// List the agents in `window` and find the one holding each scenario
/// session's id as `HarnessSession` evidence.
pub async fn agents<Q: QueryApi + Sync>(
    surface: &Q,
    caller: &Caller,
    scenario: &Scenario,
    window: TimeWindow,
) -> Result<AgentRead, ReadError> {
    let page = surface
        .agents(caller, &AgentFilter::default(), window, &first_page()?)
        .await?;
    let listed: Vec<AgentId> = page
        .value
        .items()
        .iter()
        .map(|row| row.profile.id())
        .collect();
    let mut holders: [Vec<AgentId>; 2] = [Vec::new(), Vec::new()];
    for id in &listed {
        let Some(detail) = surface.agent(caller, *id, window).await? else {
            continue;
        };
        let cluster = &detail.value.cluster;
        let records = std::iter::once(cluster.agent()).chain(cluster.aliases());
        for record in records {
            for evidence in record.evidence.iter() {
                let IdentityEvidence::HarnessSession { session, .. } = evidence else {
                    continue;
                };
                for (index, agent) in scenario.agents.iter().enumerate() {
                    if *session == agent.headers.session_id && !holders[index].contains(id) {
                        holders[index].push(*id);
                    }
                }
            }
        }
    }
    let agents = match (holders[0].as_slice(), holders[1].as_slice()) {
        ([a], [b]) => Some(Agents { a: *a, b: *b }),
        _ => None,
    };
    Ok(AgentRead { listed, agents })
}

/// Every edge of the topology graph in `window`, counted by transmissions.
pub async fn edges<Q: QueryApi + Sync>(
    surface: &Q,
    caller: &Caller,
    window: TimeWindow,
) -> Result<Vec<WeightedEdge>, ReadError> {
    let graph = surface
        .topology(
            caller,
            window,
            Weighting::Transmissions,
            &TopologyFilter::default(),
        )
        .await?;
    Ok(graph.value.edges().to_vec())
}

/// The edge from A to B routed through a channel, if the graph has one.
pub fn channel_edge(edges: &[WeightedEdge], agents: Agents) -> Option<&WeightedEdge> {
    edges.iter().find(|edge| {
        edge.from == agents.a && edge.to == agents.b && matches!(edge.route, Route::Channel(_))
    })
}

/// The transmissions behind `edge` in `window`.
pub async fn edge_transmissions<Q: QueryApi + Sync>(
    surface: &Q,
    caller: &Caller,
    edge: &WeightedEdge,
    window: TimeWindow,
) -> Result<Vec<EdgeTransmission>, ReadError> {
    let selector = EdgeSelector::new(edge.from, edge.to, edge.route.clone()).map_err(|_| {
        ReadError::SelfEdge {
            from: edge.from,
            to: edge.to,
        }
    })?;
    let page = surface
        .edge_transmissions(
            caller,
            &selector,
            window,
            &TopologyFilter::default(),
            &first_page()?,
        )
        .await?;
    Ok(page.value.page.items().to_vec())
}

/// The surface's rows for `ids`.
pub async fn summaries<Q: QueryApi + Sync>(
    surface: &Q,
    caller: &Caller,
    ids: Vec<TransmissionId>,
) -> Result<Vec<TransmissionSummary>, ReadError> {
    let selection = TransmissionSelection::new(ids).map_err(|_| ReadError::Selection)?;
    let page = surface
        .transmissions_by_id(
            caller,
            &selection,
            TopicVersionSelector::Current,
            &first_page()?,
        )
        .await?;
    Ok(page.page.items().to_vec())
}

/// The evidence page of `id`, with the default excerpt context.
pub async fn evidence<Q: QueryApi + Sync>(
    surface: &Q,
    caller: &Caller,
    id: TransmissionId,
) -> Result<Option<TransmissionEvidence>, ReadError> {
    Ok(surface
        .transmission_evidence(caller, id, ExcerptWindow::DEFAULT)
        .await?)
}

/// Every channel the registry lists, activity counted over all time.
pub async fn channels<Q: QueryApi + Sync>(
    surface: &Q,
    caller: &Caller,
) -> Result<Vec<ChannelRow>, ReadError> {
    let page = surface
        .channels(caller, &ChannelFilter::default(), &first_page()?)
        .await?;
    Ok(page.value.items().to_vec())
}

/// Every stored transmission, newest id first, as L5 last saved it
/// (`TransmissionStore::list` over all time, every state and route).
pub async fn all_transmissions<T: TransmissionStore + Sync>(
    store: &T,
) -> Result<Vec<Transmission>, ReadError> {
    let window = TimeWindow::new(Timestamp::from_micros(0), crosstalk_spec::wire::time::MAX)
        .map_err(|_| ReadError::Window)?;
    let query = TransmissionQuery {
        window,
        states: None,
        channel: None,
    };
    let mut request = first_page()?;
    let mut all = Vec::new();
    loop {
        let page = store
            .list(&query, &request)
            .await
            .map_err(ReadError::Transmissions)?;
        let (items, next) = page.into_parts();
        all.extend(items);
        match next {
            Some(next) => request.after = Some(next),
            None => return Ok(all),
        }
    }
}
