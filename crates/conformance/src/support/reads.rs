//! Reads every test area needs, each through the L8 traits only, panicking
//! with the error when a read that must succeed fails.

use crosstalk_spec::aggregates::agents::{AgentDetail, AgentRow};
use crosstalk_spec::aggregates::alert::Alert;
use crosstalk_spec::aggregates::edge::{TopologyFilter, TopologyGraph, Weighting};
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::{AgentId, ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::audit::{AuditEntry, AuditFilter};
use crosstalk_spec::interfaces::l8_surface::channels::ChannelRow;
use crosstalk_spec::interfaces::l8_surface::lists::AgentFilter;
use crosstalk_spec::interfaces::l8_surface::lists::{ChannelFilter, OriginFilter};
use crosstalk_spec::interfaces::l8_surface::summary::{TransmissionSelection, TransmissionSummary};
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, Caller, QueryApi};
use crosstalk_spec::support::TimeWindow;

use super::paging::collect;

/// Every channel row `filter` lists.
pub async fn channel_rows<B: QueryApi>(
    b: &B,
    c: &Caller,
    filter: &ChannelFilter,
) -> Vec<ChannelRow> {
    collect(7, async |p| {
        b.channels(c, filter, &p).await.map(|w| w.value)
    })
    .await
}

/// Every channel row in force and superseded (hidden ones are never
/// listed).
pub async fn every_listed_channel<B: QueryApi>(b: &B, c: &Caller) -> Vec<ChannelRow> {
    let filter = ChannelFilter {
        origin: OriginFilter::WithSuperseded(Vec::new()),
        ..ChannelFilter::default()
    };
    channel_rows(b, c, &filter).await
}

/// The row `channel` answers with, over `window`.
pub async fn channel_row<B: QueryApi>(
    b: &B,
    c: &Caller,
    channel: ChannelId,
    window: Option<TimeWindow>,
) -> ChannelRow {
    b.channel(c, channel, window)
        .await
        .unwrap_or_else(|e| panic!("channel {channel:?}: {e:?}"))
        .unwrap_or_else(|| panic!("channel {channel:?} is known"))
        .value
}

/// Every agent row over `window`.
pub async fn agent_rows<B: QueryApi>(
    b: &B,
    c: &Caller,
    filter: &AgentFilter,
    window: TimeWindow,
) -> Vec<AgentRow> {
    collect(9, async |p| {
        b.agents(c, filter, window, &p).await.map(|w| w.value)
    })
    .await
}

/// The detail `agent` answers with.
pub async fn agent_detail<B: QueryApi>(
    b: &B,
    c: &Caller,
    agent: AgentId,
    window: TimeWindow,
) -> AgentDetail {
    b.agent(c, agent, window)
        .await
        .unwrap_or_else(|e| panic!("agent {agent:?}: {e:?}"))
        .unwrap_or_else(|| panic!("agent {agent:?} is known"))
        .value
}

/// The canonical agent `agent` resolves to now.
pub async fn canonical<B: QueryApi>(
    b: &B,
    c: &Caller,
    agent: AgentId,
    window: TimeWindow,
) -> AgentId {
    agent_detail(b, c, agent, window).await.cluster.agent().id
}

/// Every alert `filter` lists.
pub async fn alerts<B: QueryApi>(b: &B, c: &Caller, filter: &AlertFilter) -> Vec<Alert> {
    collect(50, async |p| b.alerts(c, filter, &p).await).await
}

/// Every audit entry `filter` matches, newest first.
pub async fn audit<B: QueryApi>(b: &B, c: &Caller, filter: &AuditFilter) -> Vec<AuditEntry> {
    collect(100, async |p| b.audit(c, filter, &p).await).await
}

/// The rows of `ids` under `version`, newest id first; unknown ids are
/// left out.
pub async fn rows<B: QueryApi>(
    b: &B,
    c: &Caller,
    ids: &[TransmissionId],
    version: TopicVersionSelector,
) -> Vec<TransmissionSummary> {
    let mut out = Vec::new();
    for chunk in ids.chunks(TransmissionSelection::MAX) {
        let selection = TransmissionSelection::new(chunk.to_vec())
            .unwrap_or_else(|e| panic!("selection: {e:?}"));
        out.extend(
            collect(100, async |p| {
                b.transmissions_by_id(c, &selection, version, &p)
                    .await
                    .map(|page| page.page)
            })
            .await,
        );
    }
    out.sort_by_key(|row| std::cmp::Reverse(row.id));
    out
}

/// The row of one transmission under the current version.
pub async fn row<B: QueryApi>(
    b: &B,
    c: &Caller,
    id: TransmissionId,
) -> Option<TransmissionSummary> {
    rows(b, c, &[id], TopicVersionSelector::Current)
        .await
        .into_iter()
        .next()
}

/// `topology` over `window` under `filter`.
pub async fn graph<B: QueryApi>(
    b: &B,
    c: &Caller,
    window: TimeWindow,
    weighting: Weighting,
    filter: &TopologyFilter,
) -> TopologyGraph {
    b.topology(c, window, weighting, filter)
        .await
        .unwrap_or_else(|e| panic!("topology over {window:?}: {e:?}"))
        .value
}

/// The total a graph counts: transmissions or matched bytes over its
/// edges.
pub fn total(graph: &TopologyGraph, weighting: Weighting) -> u64 {
    graph
        .edges()
        .iter()
        .map(|e| match weighting {
            Weighting::Transmissions => e.stats.transmissions.get(),
            Weighting::MatchedBytes => e.stats.matched_bytes.get(),
        })
        .sum()
}

/// The topic-model version the current selector resolves to, as every
/// linked view resolves it (a default-filter `topology` over `window`).
pub async fn current_version<B: QueryApi>(
    b: &B,
    c: &Caller,
    window: TimeWindow,
) -> TopicModelVersion {
    graph(
        b,
        c,
        window,
        Weighting::Transmissions,
        &TopologyFilter::default(),
    )
    .await
    .topic_version()
}

/// The default filter pinned to `version`.
pub fn pinned(version: TopicModelVersion) -> TopologyFilter {
    TopologyFilter {
        topic_version: TopicVersionSelector::Pinned(version),
        ..TopologyFilter::default()
    }
}

/// The transmissions behind one edge of a `topology` read, as
/// `edge_transmissions` pages them for the same window and filter.
pub async fn edge_rows<B: QueryApi>(
    b: &B,
    c: &Caller,
    edge: &crosstalk_spec::aggregates::edge::WeightedEdge,
    window: TimeWindow,
    filter: &TopologyFilter,
) -> Vec<crosstalk_spec::aggregates::edge::EdgeTransmission> {
    let selector =
        crosstalk_spec::aggregates::edge::EdgeSelector::new(edge.from, edge.to, edge.route.clone())
            .unwrap_or_else(|_| panic!("an edge between two agents: {edge:?}"));
    collect(200, async |p| {
        b.edge_transmissions(c, &selector, window, filter, &p)
            .await
            .map(|w| w.value.page)
    })
    .await
}

/// Every transmission a `topology` read over `window` under `filter`
/// counts, read edge by edge with `edge_transmissions`, as rows under the
/// version the graph resolved; newest id first.
pub async fn counted<B: QueryApi>(
    b: &B,
    c: &Caller,
    window: TimeWindow,
    filter: &TopologyFilter,
) -> Vec<TransmissionSummary> {
    let topology = graph(b, c, window, Weighting::Transmissions, filter).await;
    let mut ids = Vec::new();
    for edge in topology.edges() {
        ids.extend(
            edge_rows(b, c, edge, window, filter)
                .await
                .into_iter()
                .map(|r| r.transmission),
        );
    }
    if ids.is_empty() {
        return Vec::new();
    }
    rows(
        b,
        c,
        &ids,
        TopicVersionSelector::Pinned(topology.topic_version()),
    )
    .await
}
