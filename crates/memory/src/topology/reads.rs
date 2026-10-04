//! [`EdgeStore`] on the reference edge store. Each read takes the store's
//! lock once, reads the watermark first and computes its value from the
//! fold under that one snapshot.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crosstalk_spec::aggregates::access::{AccessEdge, BipartiteGraph, BipartiteParts};
use crosstalk_spec::aggregates::agents::AgentTraffic;
use crosstalk_spec::aggregates::edge::{
    EdgeKey, EdgeSelector, EdgeTotals, EdgeTransmission, EdgeTransmissionPage, TopologyFilter,
    TopologyGraph, TopologyGraphParts, Weighting,
};
use crosstalk_spec::aggregates::filter::VersionUnavailable;
use crosstalk_spec::aggregates::node::GraphNode;
use crosstalk_spec::aggregates::series::{
    BucketWidth, Series, SeriesEdge, SeriesGrid, SeriesGrouping, SeriesGroups, TopologySeries,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::watermark::{PipelineFrontier, Watermark, Watermarked};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::derived::flow::verdict::{Observed, Verdict, VerdictRevision};
use crosstalk_spec::ids::{AgentId, TopicId, TransmissionId};
use crosstalk_spec::interfaces::l7_topology::{
    AccessContribution, EdgeContribution, EdgeError, EdgeQueryError, EdgeStore,
};
use crosstalk_spec::paging::{EdgeTransmissionList, PageRequest};
use crosstalk_spec::support::TimeWindow;

use super::env::{EnvAliases, TopologyEnv};
use super::fold::{
    Counted, EdgeBinding, EdgeResume, access_edges, counted, edges, kind_index, nodes,
    resolve_version, route_key,
};
use super::store::{EdgeState, InMemoryEdgeStore};
use crate::analysis::support::lock;
use crate::surface::paging::page_after;

fn store_error(reason: impl Into<String>) -> EdgeQueryError {
    EdgeQueryError::Store {
        reason: reason.into(),
    }
}

/// Whether both ends of `window` are bucket boundaries.
fn aligned(width: BucketWidth, window: TimeWindow) -> bool {
    width.is_boundary(window.start()) && width.is_boundary(window.end())
}

impl<V: TopologyEnv> InMemoryEdgeStore<V> {
    /// The graph under `version`, from one snapshot.
    fn graph_in(
        &self,
        state: &EdgeState,
        version: TopicModelVersion,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<TopologyGraph, EdgeQueryError> {
        let counted = counted(state, &self.env, version, window, filter);
        let edges = edges(&counted, weighting)?;
        let endpoints: Vec<AgentId> = edges.iter().flat_map(|edge| [edge.from, edge.to]).collect();
        let nodes = nodes(&self.env, endpoints, [], &edges);
        TopologyGraph::new(TopologyGraphParts {
            window,
            weighting,
            topic_version: version,
            nodes,
            edges,
        })
        .map_err(|error| store_error(format!("built an invalid graph: {error:?}")))
    }

    /// The aligned graph read every graph-shaped query shares: the
    /// watermark, then the version, then the graph.
    fn read_graph(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologyGraph>, EdgeQueryError> {
        if !aligned(self.config.bucket_width, window) {
            return Err(EdgeQueryError::UnalignedWindow);
        }
        let state = lock(&self.state);
        let watermark = state.watermark;
        let version = resolve_version(&state, &self.env, filter)?;
        let value = self.graph_in(&state, version, window, weighting, filter)?;
        Ok(Watermarked { watermark, value })
    }
}

/// The key a by-edge series or graph edge sorts under.
fn edge_order(edge: &SeriesEdge) -> (AgentId, AgentId, (u8, String)) {
    (edge.from, edge.to, route_key(&edge.route))
}

/// Groups the fold's contributions into series over `grid`.
fn series_groups(
    counted: &[Counted<'_>],
    grid: SeriesGrid,
    weighting: Weighting,
    grouping: SeriesGrouping,
) -> SeriesGroups {
    let points = grid.points().get() as usize;
    let start = grid.window().start().as_micros();
    let step = grid.step().as_micros().get();
    let point_of = |one: &Counted<'_>| {
        usize::try_from((one.contribution.at.as_micros() - start) / step).unwrap_or(usize::MAX)
    };
    let stat_of = |one: &Counted<'_>| match weighting {
        Weighting::Transmissions => 1,
        Weighting::MatchedBytes => one.contribution.matched_bytes.get(),
    };
    let add = |values: &mut Vec<u64>, one: &Counted<'_>| {
        if let Some(value) = values.get_mut(point_of(one)) {
            *value = value.saturating_add(stat_of(one));
        }
    };
    match grouping {
        SeriesGrouping::Total => {
            let mut values = vec![0; points];
            for one in counted {
                add(&mut values, one);
            }
            SeriesGroups::Total(values)
        }
        SeriesGrouping::Topic => {
            let mut by_topic: BTreeMap<Option<TopicId>, Vec<u64>> = BTreeMap::new();
            for one in counted {
                let values = by_topic
                    .entry(one.contribution.classification.topic)
                    .or_insert_with(|| vec![0; points]);
                add(values, one);
            }
            SeriesGroups::ByTopic(
                by_topic
                    .into_iter()
                    .map(|(key, values)| Series { key, values })
                    .collect(),
            )
        }
        SeriesGrouping::RouteKind => {
            let mut by_kind = BTreeMap::new();
            for one in counted {
                let kind = crosstalk_spec::aggregates::edge::RouteKind::of(&one.route);
                let (_, values) = by_kind
                    .entry(kind_index(kind))
                    .or_insert_with(|| (kind, vec![0; points]));
                add(values, one);
            }
            SeriesGroups::ByRouteKind(
                by_kind
                    .into_values()
                    .map(|(key, values)| Series { key, values })
                    .collect(),
            )
        }
        SeriesGrouping::Edge => {
            let mut by_edge: HashMap<(AgentId, AgentId, Route), Vec<u64>> = HashMap::new();
            for one in counted {
                let values = by_edge
                    .entry((one.from, one.to, one.route.clone()))
                    .or_insert_with(|| vec![0; points]);
                add(values, one);
            }
            let mut series: Vec<Series<SeriesEdge>> = by_edge
                .into_iter()
                .map(|((from, to, route), values)| Series {
                    key: SeriesEdge { from, to, route },
                    values,
                })
                .collect();
            series.sort_by_key(|one| edge_order(&one.key));
            SeriesGroups::ByEdge(series)
        }
    }
}

impl<V: TopologyEnv> EdgeStore for InMemoryEdgeStore<V> {
    async fn apply(&mut self, contribution: &EdgeContribution) -> Result<EdgeKey, EdgeError> {
        self.apply_impl(contribution)
    }

    async fn judge(
        &mut self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
    ) -> Result<Observed, EdgeError> {
        Ok(self.judge_impl(transmission, verdict, revision))
    }

    async fn activate(&mut self, version: TopicModelVersion) -> Result<(), EdgeError> {
        self.activate_if_complete(version).map(|_| ())
    }

    async fn drop_version(&mut self, version: TopicModelVersion) -> Result<(), EdgeError> {
        self.drop_impl(version)
    }

    async fn watermark(&self) -> Result<Watermark, EdgeQueryError> {
        Ok(lock(&self.state).watermark)
    }

    async fn advance_watermark(
        &mut self,
        frontier: PipelineFrontier,
    ) -> Result<Option<Watermark>, EdgeError> {
        Ok(self.advance_impl(frontier))
    }

    async fn apply_access(&mut self, access: &AccessContribution) -> Result<AccessEdge, EdgeError> {
        self.apply_access_impl(access)
    }

    async fn graph(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologyGraph>, EdgeQueryError> {
        self.read_graph(window, weighting, filter)
    }

    async fn totals(
        &self,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<EdgeTotals>, EdgeQueryError> {
        let graph = self.read_graph(window, Weighting::Transmissions, filter)?;
        Ok(Watermarked {
            watermark: graph.watermark,
            value: EdgeTotals::of(&graph.value),
        })
    }

    async fn channel_topology(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<BipartiteGraph>, EdgeQueryError> {
        if !aligned(self.config.bucket_width, window) {
            return Err(EdgeQueryError::UnalignedWindow);
        }
        let state = lock(&self.state);
        let watermark = state.watermark;
        let version = resolve_version(&state, &self.env, filter)?;
        let counted = counted(&state, &self.env, version, window, filter);
        let transmissions = edges(&counted, weighting)?;
        let accesses = access_edges(&state, &self.env, version, window, filter)?;
        let agents: Vec<AgentId> = accesses
            .iter()
            .map(|access| access.agent)
            .chain(transmissions.iter().flat_map(|edge| [edge.from, edge.to]))
            .collect();
        let channels: BTreeSet<_> = accesses
            .iter()
            .map(|access| access.channel)
            .chain(transmissions.iter().filter_map(|edge| match edge.route {
                Route::Channel(channel) => Some(channel),
                Route::Delegation(_) | Route::Direct(_) | Route::Unobserved => None,
            }))
            .collect();
        let nodes: Vec<GraphNode> = nodes(&self.env, agents, channels, &transmissions);
        let value = BipartiteGraph::new(BipartiteParts {
            window,
            weighting,
            topic_version: version,
            nodes,
            accesses,
            transmissions,
        })
        .map_err(|error| store_error(format!("built an invalid channel graph: {error:?}")))?;
        Ok(Watermarked { watermark, value })
    }

    async fn transmissions(
        &self,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> Result<Watermarked<EdgeTransmissionPage>, EdgeQueryError> {
        let mut state = lock(&self.state);
        let watermark = state.watermark;
        let binding = EdgeBinding {
            edge: edge.clone(),
            window,
            filter: filter.clone(),
        };
        let (version, resume) = match &page.after {
            None => (resolve_version(&state, &self.env, filter)?, None),
            Some(cursor) => {
                let resume = state
                    .cursors
                    .resolve(cursor, &binding)
                    .ok_or(EdgeQueryError::InvalidCursor)?;
                if state.dropped.contains(&resume.version) {
                    return Err(EdgeQueryError::Version(VersionUnavailable::NotRetained(
                        resume.version,
                    )));
                }
                (resume.version, Some(resume))
            }
        };
        let from = self.env.canonical_agent(edge.from());
        let to = self.env.canonical_agent(edge.to());
        let route = edge.route().resolved(EnvAliases(&self.env));
        let mut rows: Vec<EdgeTransmission> = counted(&state, &self.env, version, window, filter)
            .into_iter()
            .filter(|one| one.from == from && one.to == to && one.route == route)
            .map(|one| EdgeTransmission {
                transmission: one.contribution.transmission,
                confirmed_at: one.contribution.at,
                matched_bytes: one.contribution.matched_bytes,
                topic: one.contribution.classification.topic,
            })
            .filter(|row| {
                resume.is_none_or(|resume| {
                    (row.confirmed_at, row.transmission)
                        < (resume.confirmed_at, resume.transmission)
                })
            })
            .collect();
        rows.sort_by(|a, b| {
            (b.confirmed_at, b.transmission).cmp(&(a.confirmed_at, a.transmission))
        });
        let page = page_after(&mut state.cursors, rows, page.size, binding, |row| {
            EdgeResume {
                version,
                confirmed_at: row.confirmed_at,
                transmission: row.transmission,
            }
        })
        .map_err(|error| store_error(error.to_string()))?;
        Ok(Watermarked {
            watermark,
            value: EdgeTransmissionPage {
                topic_version: version,
                page,
            },
        })
    }

    async fn agent_traffic(
        &self,
        window: TimeWindow,
        agents: &[AgentId],
    ) -> Result<Watermarked<BTreeMap<AgentId, AgentTraffic>>, EdgeQueryError> {
        let graph =
            self.read_graph(window, Weighting::Transmissions, &TopologyFilter::default())?;
        let counts: BTreeMap<AgentId, AgentTraffic> = graph
            .value
            .nodes()
            .iter()
            .filter_map(|node| match node {
                GraphNode::Agent(agent) => Some((
                    agent.id,
                    AgentTraffic {
                        transmissions_in: agent.transmissions_in,
                        transmissions_out: agent.transmissions_out,
                    },
                )),
                GraphNode::Channel(_) => None,
            })
            .collect();
        let value = agents
            .iter()
            .map(|listed| {
                let canonical = self.env.canonical_agent(*listed);
                (*listed, counts.get(&canonical).copied().unwrap_or_default())
            })
            .collect();
        Ok(Watermarked {
            watermark: graph.watermark,
            value,
        })
    }

    fn bucket_width(&self) -> BucketWidth {
        self.config.bucket_width
    }

    async fn series(
        &self,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologySeries>, EdgeQueryError> {
        let store = self.config.bucket_width;
        if grid.step().bucket() != store {
            return Err(EdgeQueryError::BucketWidthMismatch {
                store,
                grid: grid.step().bucket(),
            });
        }
        let state = lock(&self.state);
        let watermark = state.watermark;
        let version = resolve_version(&state, &self.env, filter)?;
        let counted = counted(&state, &self.env, version, grid.window(), filter);
        let groups = series_groups(&counted, grid, weighting, grouping);
        let value = TopologySeries::new(grid, weighting, version, groups)
            .map_err(|error| store_error(format!("built an invalid series: {error:?}")))?;
        Ok(Watermarked { watermark, value })
    }
}
