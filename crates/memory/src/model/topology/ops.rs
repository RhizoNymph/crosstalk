//! The operations `check_edge_store` generates, and how each is played on
//! both stores.

use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;

use crosstalk_spec::aggregates::edge::{EdgeSelector, TopologyFilter, TopologyGraph};
use crosstalk_spec::aggregates::node::GraphNode;
use crosstalk_spec::aggregates::series::{
    SeriesGrid, SeriesGrouping, SeriesGroups, SeriesStep, TopologySeries,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::watermark::{PipelineFrontier, Watermark, Watermarked};
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::transmission::{Classification, DelegationDirection, Route};
use crosstalk_spec::derived::flow::verdict::{CurrentVerdict, Observed, Verdict};
use crosstalk_spec::events::insight::ClassificationCause;
use crosstalk_spec::ids::{AgentId, TopicId};
use crosstalk_spec::interfaces::l7_topology::{
    AccessContribution, EdgeContribution, EdgeQueryError, EdgeStore,
};
use crosstalk_spec::paging::{EdgeTransmissionList, PageRequest, PageSize};
use crosstalk_spec::support::TimeWindow;

use super::subject::{EdgeSubject, ReferenceEdges};
use super::{Ledger, check_graph, revision, weighting};
use crate::analysis::catalog::Activated;
use crate::model::analysis::harness_model;
use crate::model::build::{
    access, agent, bucket_width, channel, non_zero, raw, topic, transmission, ts, unit, window,
};
use crate::model::{Divergence, holds, same};
use crate::topology::fold::{kind_index, route_key};
use crate::topology::store::Activation;

/// A window of whole 10 µs buckets, or one cut 3 µs into its first bucket.
#[derive(Debug, Clone, Copy)]
pub struct WindowSeed {
    start: u64,
    buckets: u64,
    cut: bool,
}

fn window_seed() -> impl Strategy<Value = WindowSeed> {
    (0u64..25, 1u64..25, prop::bool::weighted(0.1)).prop_map(|(start, buckets, cut)| WindowSeed {
        start,
        buckets,
        cut,
    })
}

impl WindowSeed {
    fn window(self) -> Option<TimeWindow> {
        let start = self.start * 10 + if self.cut { 3 } else { 0 };
        window(start, self.start * 10 + self.buckets * 10)
    }
}

#[derive(Debug, Clone)]
pub enum EdgeOp {
    Apply {
        n: u64,
        from: u64,
        to: u64,
        route: u8,
        at: u64,
        bytes: u64,
        topic: Option<u8>,
        active: bool,
    },
    Refit {
        topics: u8,
        picks: Vec<Option<u8>>,
        shortfall: bool,
    },
    Activate,
    Drop {
        version: u32,
    },
    Judge {
        n: u64,
        verdict: u8,
        revision: u32,
    },
    Advance {
        ticked: u64,
        pending: Option<u64>,
    },
    Access {
        n: u64,
        agent: u64,
        channel: u64,
        write: bool,
        at: u64,
    },
    Merge {
        from: u64,
        into: u64,
    },
    Unmerge {
        agent: u64,
    },
    Supersede {
        channel: u64,
        by: u64,
    },
    Parent {
        agent: u64,
        parent: Option<u64>,
    },
    Graph {
        window: WindowSeed,
        bytes: bool,
        filter: crate::model::analysis::FilterSeed,
    },
    Totals {
        window: WindowSeed,
        filter: crate::model::analysis::FilterSeed,
    },
    Channels {
        window: WindowSeed,
        bytes: bool,
        filter: crate::model::analysis::FilterSeed,
    },
    Drill {
        from: u64,
        to: u64,
        route: u8,
        start: u64,
        length: u64,
        filter: crate::model::analysis::FilterSeed,
        size: u16,
    },
    Traffic {
        window: WindowSeed,
        agents: Vec<u64>,
    },
    Series {
        start: u64,
        steps: u64,
        per_step: u64,
        bytes: bool,
        grouping: u8,
        filter: crate::model::analysis::FilterSeed,
        foreign: bool,
    },
}

pub fn edge_op() -> impl Strategy<Value = EdgeOp> {
    let filter = crate::model::analysis::filter_seed;
    prop_oneof![
        8 => (0u64..12, 0u64..5, 0u64..5, 0u8..6, 0u64..250, 1u64..10, prop::option::of(0u8..3), prop::bool::weighted(0.2))
            .prop_map(|(n, from, to, route, at, bytes, topic, active)| EdgeOp::Apply { n, from, to, route, at, bytes, topic, active }),
        2 => (1u8..4, prop::collection::vec(prop::option::of(0u8..3), 12), prop::bool::weighted(0.2))
            .prop_map(|(topics, picks, shortfall)| EdgeOp::Refit { topics, picks, shortfall }),
        1 => Just(EdgeOp::Activate),
        1 => (0u32..5).prop_map(|version| EdgeOp::Drop { version }),
        2 => (0u64..12, 0u8..3, 1u32..4).prop_map(|(n, verdict, revision)| EdgeOp::Judge { n, verdict, revision }),
        2 => (0u64..300, prop::option::of(0u64..300)).prop_map(|(ticked, pending)| EdgeOp::Advance { ticked, pending }),
        3 => (0u64..10, 0u64..5, 0u64..4, any::<bool>(), 0u64..250)
            .prop_map(|(n, agent, channel, write, at)| EdgeOp::Access { n, agent, channel, write, at }),
        1 => (0u64..5, 0u64..5).prop_map(|(from, into)| EdgeOp::Merge { from, into }),
        1 => (0u64..5).prop_map(|agent| EdgeOp::Unmerge { agent }),
        1 => (0u64..4, 0u64..4).prop_map(|(channel, by)| EdgeOp::Supersede { channel, by }),
        1 => (0u64..5, prop::option::of(0u64..5)).prop_map(|(agent, parent)| EdgeOp::Parent { agent, parent }),
        4 => (window_seed(), any::<bool>(), filter()).prop_map(|(window, bytes, filter)| EdgeOp::Graph { window, bytes, filter }),
        1 => (window_seed(), filter()).prop_map(|(window, filter)| EdgeOp::Totals { window, filter }),
        2 => (window_seed(), any::<bool>(), filter()).prop_map(|(window, bytes, filter)| EdgeOp::Channels { window, bytes, filter }),
        2 => (0u64..5, 0u64..5, 0u8..6, 0u64..200, 1u64..200, filter(), 1u16..4)
            .prop_map(|(from, to, route, start, length, filter, size)| EdgeOp::Drill { from, to, route, start, length, filter, size }),
        1 => (window_seed(), prop::collection::vec(0u64..6, 0..4)).prop_map(|(window, agents)| EdgeOp::Traffic { window, agents }),
        3 => (0u64..10, 1u64..6, 1u64..4, any::<bool>(), 0u8..4, filter(), prop::bool::weighted(0.1))
            .prop_map(|(start, steps, per_step, bytes, grouping, filter, foreign)| EdgeOp::Series { start, steps, per_step, bytes, grouping, filter, foreign }),
    ]
}

/// What the harness tracks of the pipeline it plays.
#[derive(Debug)]
pub struct World {
    /// The facts of each transmission applied, re-classified on a re-fit.
    transmissions: BTreeMap<u64, (u64, u64, u8, u64, u64)>,
    /// The newest version the catalog made ready, which confirmations are
    /// classified under, with its topics; and the active one.
    newest: (TopicModelVersion, Vec<TopicId>),
    active: (TopicModelVersion, Vec<TopicId>),
    /// Ready versions not yet activated in the store.
    pending: BTreeSet<TopicModelVersion>,
    now: u64,
}

impl Default for World {
    fn default() -> Self {
        Self {
            transmissions: BTreeMap::new(),
            newest: (TopicModelVersion(0), Vec::new()),
            active: (TopicModelVersion(0), Vec::new()),
            pending: BTreeSet::new(),
            now: 10_000,
        }
    }
}

fn route(n: u8) -> Route {
    match n % 6 {
        4 => Route::Delegation(DelegationDirection::ChildToParent),
        5 => Route::Unobserved,
        k => Route::Channel(channel(u64::from(k))),
    }
}

fn contribution(
    n: u64,
    facts: (u64, u64, u8, u64, u64),
    version: TopicModelVersion,
    topic: Option<TopicId>,
) -> EdgeContribution {
    let (from, to, route_number, at, bytes) = facts;
    EdgeContribution {
        transmission: transmission(n),
        from: agent(from),
        to: agent(to),
        route: route(route_number),
        at: ts(at),
        matched_bytes: non_zero(bytes),
        classification: Classification {
            version,
            topic,
            watched: false,
        },
    }
}

fn pick(topics: &[TopicId], choice: Option<u8>) -> Option<TopicId> {
    let choice = usize::from(choice?);
    topics.get(choice % topics.len().max(1)).copied()
}

/// An edge as compared: the fold's key and counts, and the share to 1e-9.
type ViewEdge = (AgentId, AgentId, (u8, String), u64, u64, i64);

/// A graph in a form that does not depend on order: edges and nodes
/// sorted, shares to 1e-9.
#[derive(Debug, PartialEq)]
struct GraphView {
    watermark: Watermark,
    version: TopicModelVersion,
    edges: Vec<ViewEdge>,
    nodes: Vec<String>,
}

#[allow(clippy::cast_possible_truncation)]
fn share_key(share: f64) -> i64 {
    (share * 1e9).round() as i64
}

fn graph_view(read: &Watermarked<TopologyGraph>) -> GraphView {
    let mut edges: Vec<_> = read
        .value
        .edges
        .iter()
        .map(|edge| {
            (
                edge.from,
                edge.to,
                route_key(&edge.route),
                edge.stats.transmissions.get(),
                edge.stats.matched_bytes.get(),
                share_key(edge.share.get()),
            )
        })
        .collect();
    edges.sort();
    GraphView {
        watermark: read.watermark,
        version: read.value.topic_version,
        edges,
        nodes: node_keys(&read.value.nodes),
    }
}

fn node_keys(nodes: &[GraphNode]) -> Vec<String> {
    let mut keys: Vec<String> = nodes.iter().map(|node| format!("{node:?}")).collect();
    keys.sort();
    keys
}

fn series_view(read: &Watermarked<TopologySeries>) -> (Watermark, TopicModelVersion, Vec<String>) {
    let mut groups: Vec<String> = match read.value.groups() {
        SeriesGroups::Total(values) => vec![format!("{values:?}")],
        SeriesGroups::ByTopic(series) => series.iter().map(|one| format!("{one:?}")).collect(),
        SeriesGroups::ByRouteKind(series) => series
            .iter()
            .map(|one| format!("{:?}", (kind_index(one.key), &one.values)))
            .collect(),
        SeriesGroups::ByEdge(series) => series
            .iter()
            .map(|one| {
                format!(
                    "{:?}",
                    (
                        one.key.from,
                        one.key.to,
                        route_key(&one.key.route),
                        &one.values
                    )
                )
            })
            .collect(),
    };
    groups.sort();
    (read.watermark, read.value.topic_version(), groups)
}

type Drilled = (
    Vec<(Watermark, TopicModelVersion, Vec<String>, bool)>,
    Option<EdgeQueryError>,
);

async fn drill<S: EdgeStore>(
    store: &S,
    edge: &EdgeSelector,
    window: TimeWindow,
    filter: &TopologyFilter,
    size: PageSize,
) -> Drilled {
    let mut request: PageRequest<EdgeTransmissionList> = PageRequest { size, after: None };
    let mut pages = Vec::new();
    loop {
        match store.transmissions(edge, window, filter, &request).await {
            Err(error) => return (pages, Some(error)),
            Ok(read) => {
                let version = read.value.topic_version;
                let (items, next) = read.value.page.into_parts();
                pages.push((
                    read.watermark,
                    version,
                    items.iter().map(|row| format!("{row:?}")).collect(),
                    next.is_some(),
                ));
                match next {
                    Some(cursor) => request.after = Some(cursor),
                    None => return (pages, None),
                }
            }
        }
    }
}

/// Activate every pending version whose buckets are complete, in order,
/// following each with the catalog's activation and retention's drops.
async fn activate_pending<S: EdgeSubject>(
    step: usize,
    subject: &mut S,
    reference: &mut ReferenceEdges,
    ledger: &mut Ledger,
    world: &mut World,
) -> Result<(), Divergence> {
    for version in world.pending.clone() {
        let theirs = subject.activate_if_complete(version).await;
        let ours = reference.activate_if_complete(version).await;
        same(step, "activate", &theirs, &ours)?;
        if !matches!(ours, Ok(Activation::Switched { .. })) {
            continue;
        }
        world.pending.remove(&version);
        let at = ts(world.now);
        let theirs = subject.catalog_activated(version, at).await;
        let ours = reference.catalog_activated(version, at).await;
        same(step, "catalog activation", &theirs, &ours)?;
        if version == world.newest.0 {
            world.active = world.newest.clone();
        }
        if let Ok(Activated::Switched { dropped, .. }) = ours {
            for gone in dropped {
                let theirs = subject.drop_version(gone).await;
                let ours = reference.drop_version(gone).await;
                same(step, "drop on TopicVersionDropped", &theirs, &ours)?;
                if ours.is_ok() {
                    ledger.applied.retain(|(stored, _), _| *stored != gone);
                }
            }
        }
    }
    Ok(())
}

/// Play `op` on both stores, comparing everything it returns, and keep
/// the ledger and the world.
pub async fn play<S: EdgeSubject>(
    step: usize,
    op: &EdgeOp,
    subject: &mut S,
    reference: &mut ReferenceEdges,
    ledger: &mut Ledger,
    world: &mut World,
) -> Result<(), Divergence> {
    let label = format!("{op:?}");
    world.now += 10;
    match op {
        EdgeOp::Apply {
            n,
            from,
            to,
            route: r,
            at,
            bytes,
            topic: k,
            active,
        } => {
            let facts = (*from, *to, *r, *at, *bytes);
            let (version, topics) = if *active {
                world.active.clone()
            } else {
                world.newest.clone()
            };
            let one = contribution(*n, facts, version, pick(&topics, *k));
            let theirs = subject.apply(&one).await;
            let ours = reference.apply(&one).await;
            same(step, &label, &theirs, &ours)?;
            if ours.is_ok() {
                world.transmissions.entry(*n).or_insert(facts);
                ledger
                    .applied
                    .entry((version, one.transmission))
                    .or_insert(one);
            }
        }
        EdgeOp::Refit {
            topics: count,
            picks,
            shortfall,
        } => {
            let next = TopicModelVersion(
                u32::try_from(
                    crate::analysis::catalog::TopicVersions::history(&reference.catalog)
                        .versions()
                        .len(),
                )
                .unwrap_or(0),
            );
            let model = harness_model();
            let made: Vec<_> = (0..*count)
                .filter_map(|k| {
                    let id = TopicId::from_ulid(raw(u64::from(next.0) * 16 + u64::from(k)));
                    Some(topic(
                        id,
                        next,
                        unit(&model, 1.0, f32::from(k), 0.0)?,
                        ts(world.now),
                    ))
                })
                .collect();
            let ids: Vec<TopicId> = made.iter().map(|one| one.id).collect();
            let theirs = subject.catalog_ready(made.clone(), ts(world.now)).await;
            let ours = reference.catalog_ready(made, ts(world.now)).await;
            same(step, "catalog ready", &theirs, &ours)?;
            let Ok(version) = ours else { return Ok(()) };
            let mut processed = 0u64;
            for (index, (n, facts)) in world.transmissions.clone().into_iter().enumerate() {
                let one = contribution(
                    n,
                    facts,
                    version,
                    pick(&ids, picks.get(index).copied().flatten()),
                );
                let theirs = subject
                    .apply_classified(&one, ClassificationCause::Refit)
                    .await;
                let ours = reference
                    .apply_classified(&one, ClassificationCause::Refit)
                    .await;
                same(step, "refit classification", &theirs, &ours)?;
                if ours.is_ok() {
                    ledger
                        .applied
                        .entry((version, one.transmission))
                        .or_insert(one);
                }
                processed += 1;
            }
            let expected = processed + u64::from(*shortfall);
            subject.version_ready(version, expected).await;
            reference.version_ready(version, expected).await;
            world.newest = (version, ids);
            world.pending.insert(version);
            activate_pending(step, subject, reference, ledger, world).await?;
        }
        EdgeOp::Activate => activate_pending(step, subject, reference, ledger, world).await?,
        EdgeOp::Drop { version } => {
            let version = TopicModelVersion(*version);
            let theirs = subject.drop_version(version).await;
            let ours = reference.drop_version(version).await;
            same(step, &label, &theirs, &ours)?;
            if ours.is_ok() {
                ledger.applied.retain(|(stored, _), _| *stored != version);
            }
        }
        EdgeOp::Judge {
            n,
            verdict,
            revision: r,
        } => {
            let verdict = match verdict {
                0 => None,
                1 => Some(Verdict::Genuine),
                _ => Some(Verdict::FalseDetection),
            };
            let theirs = subject.judge(transmission(*n), verdict, revision(*r)).await;
            let ours = reference
                .judge(transmission(*n), verdict, revision(*r))
                .await;
            same(step, &label, &theirs, &ours)?;
            if matches!(ours, Ok(Observed::Newer)) {
                ledger.judge(
                    transmission(*n),
                    CurrentVerdict {
                        verdict,
                        revision: revision(*r),
                    },
                );
            }
        }
        EdgeOp::Advance { ticked, pending } => {
            let frontier = PipelineFrontier {
                ticked_through: ts(*ticked),
                oldest_pending: pending.map(ts),
            };
            let theirs = subject.advance_watermark(frontier).await;
            let ours = reference.advance_watermark(frontier).await;
            same(step, &label, &theirs, &ours)?;
        }
        EdgeOp::Access {
            n,
            agent: a,
            channel: c,
            write,
            at,
        } => {
            let one = AccessContribution {
                access: access(*n),
                agent: agent(*a),
                channel: channel(*c),
                op: if *write {
                    AccessKind::Write
                } else {
                    AccessKind::Read
                },
                at: ts(*at),
            };
            let theirs = subject.apply_access(&one).await;
            let ours = reference.apply_access(&one).await;
            same(step, &label, &theirs, &ours)?;
        }
        EdgeOp::Merge { from, into } => {
            let theirs = subject.merge(agent(*from), agent(*into));
            same(
                step,
                &label,
                &theirs,
                &reference.merge(agent(*from), agent(*into)),
            )?;
        }
        EdgeOp::Unmerge { agent: a } => {
            subject.unmerge(agent(*a));
            reference.unmerge(agent(*a));
        }
        EdgeOp::Supersede { channel: c, by } => {
            let theirs = subject.supersede(channel(*c), channel(*by));
            same(
                step,
                &label,
                &theirs,
                &reference.supersede(channel(*c), channel(*by)),
            )?;
        }
        EdgeOp::Parent { agent: a, parent } => {
            subject.set_parent(agent(*a), parent.map(agent));
            reference.set_parent(agent(*a), parent.map(agent));
        }
        EdgeOp::Graph {
            window: seed,
            bytes,
            filter,
        } => {
            let Some(window) = seed.window() else {
                return Ok(());
            };
            let filter = filter.build();
            let theirs = subject.graph(window, weighting(*bytes), &filter).await;
            let ours = reference.graph(window, weighting(*bytes), &filter).await;
            same(
                step,
                &label,
                &theirs.as_ref().map(graph_view),
                &ours.as_ref().map(graph_view),
            )?;
            if let Ok(read) = &theirs {
                check_graph(step, &read.value, ledger, &reference.directory, &filter)?;
            }
        }
        EdgeOp::Totals {
            window: seed,
            filter,
        } => {
            let Some(window) = seed.window() else {
                return Ok(());
            };
            let filter = filter.build();
            let theirs = subject.totals(window, &filter).await;
            same(
                step,
                &label,
                &theirs,
                &reference.totals(window, &filter).await,
            )?;
        }
        EdgeOp::Channels {
            window: seed,
            bytes,
            filter,
        } => {
            let Some(window) = seed.window() else {
                return Ok(());
            };
            let filter = filter.build();
            let view = |read: &Watermarked<crosstalk_spec::aggregates::access::BipartiteGraph>| {
                let mut accesses: Vec<String> = read
                    .value
                    .accesses()
                    .iter()
                    .map(|one| format!("{one:?}"))
                    .collect();
                accesses.sort();
                let mut edges: Vec<String> = read
                    .value
                    .transmissions()
                    .iter()
                    .map(|edge| {
                        format!(
                            "{:?}",
                            (
                                edge.from,
                                edge.to,
                                route_key(&edge.route),
                                edge.stats,
                                share_key(edge.share.get())
                            )
                        )
                    })
                    .collect();
                edges.sort();
                (
                    read.watermark,
                    read.value.topic_version(),
                    accesses,
                    edges,
                    node_keys(read.value.nodes()),
                )
            };
            let theirs = subject
                .channel_topology(window, weighting(*bytes), &filter)
                .await;
            let ours = reference
                .channel_topology(window, weighting(*bytes), &filter)
                .await;
            same(
                step,
                &label,
                &theirs.as_ref().map(view),
                &ours.as_ref().map(view),
            )?;
        }
        EdgeOp::Drill {
            from,
            to,
            route: r,
            start,
            length,
            filter,
            size,
        } => {
            let (Ok(edge), Some(window), Ok(size)) = (
                EdgeSelector::new(agent(*from), agent(*to), route(*r)),
                window(*start, start + length),
                PageSize::new(*size),
            ) else {
                return Ok(());
            };
            let filter = filter.build();
            let theirs = drill(subject, &edge, window, &filter, size).await;
            let ours = drill(reference, &edge, window, &filter, size).await;
            same(step, &label, &theirs, &ours)?;
        }
        EdgeOp::Traffic {
            window: seed,
            agents,
        } => {
            let Some(window) = seed.window() else {
                return Ok(());
            };
            let listed: Vec<AgentId> = agents.iter().copied().map(agent).collect();
            let theirs = subject.agent_traffic(window, &listed).await;
            same(
                step,
                &label,
                &theirs,
                &reference.agent_traffic(window, &listed).await,
            )?;
        }
        EdgeOp::Series {
            start,
            steps,
            per_step,
            bytes,
            grouping,
            filter,
            foreign,
        } => {
            let width = if *foreign { 20 } else { 10 };
            let step_micros = 10 * per_step * if *foreign { 2 } else { 1 };
            let (Some(window), Ok(series_step)) = (
                window(start * 20, start * 20 + steps * step_micros),
                SeriesStep::new(bucket_width(width), non_zero(step_micros)),
            ) else {
                return Ok(());
            };
            let Ok(grid) = SeriesGrid::new(window, series_step) else {
                return Ok(());
            };
            let grouping = match grouping {
                0 => SeriesGrouping::Total,
                1 => SeriesGrouping::Topic,
                2 => SeriesGrouping::RouteKind,
                _ => SeriesGrouping::Edge,
            };
            let filter = filter.build();
            let theirs = subject
                .series(grid, weighting(*bytes), grouping, &filter)
                .await;
            let ours = reference
                .series(grid, weighting(*bytes), grouping, &filter)
                .await;
            same(
                step,
                &label,
                &theirs.as_ref().map(series_view),
                &ours.as_ref().map(series_view),
            )?;
            if let Ok(series) = &theirs {
                let graph = subject
                    .graph(grid.window(), weighting(*bytes), &filter)
                    .await;
                if let Ok(graph) = graph
                    && graph.value.topic_version == series.value.topic_version()
                {
                    holds(step, graph.value.total() == series.value.total(), || {
                        format!(
                            "series total {} but graph total {}",
                            series.value.total(),
                            graph.value.total()
                        )
                    })?;
                }
            }
        }
    }
    let theirs = subject.watermark().await;
    same(step, "watermark", &theirs, &reference.watermark().await)
}
