//! What every read computes from the rows it fetched: resolution through
//! the environment, the filter, the `Exclude` subtraction, and the graph,
//! node, access and series shapes. Pure: no database here.
//!
//! The rows are bucket sums as stored (agents as attributed, routes as
//! routed). Each row is resolved (canonical sender and reader, route with
//! its channel canonical), dropped when it resolves to a self-edge, and kept
//! when the filter admits it with its verdicts left aside
//! (`FalseDetections::Include`). Under `FalseDetections::Exclude` the
//! stored contributions of the transmissions the verdict copy holds as
//! `FalseDetection` are resolved the same way and subtracted: they are
//! exactly the transmissions `TopologyFilter::admits` refuses under
//! `Exclude` and admits under `Include`, so the result is the fold of
//! `topology.graph.matches-fold-model`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::NonZeroU64;

use crosstalk_spec::aggregates::access::WeightedAccess;
use crosstalk_spec::aggregates::edge::{
    EdgeStats, RouteKind, TopologyFilter, WeightedEdge, Weighting,
};
use crosstalk_spec::aggregates::filter::{AccessSubject, FalseDetections, FilterSubject};
use crosstalk_spec::aggregates::node::{AgentNode, ChannelNode, GraphNode};
use crosstalk_spec::aggregates::series::{Series, SeriesEdge, SeriesGrouping, SeriesGroups};
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::channel::confirmation::{Confirmation, Listing};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, ChannelId, ResourceId, TopicId};
use crosstalk_spec::support::Share;

use super::error::DbError;
use crate::env::{EnvAliases, TopologyEnv};

/// Summed edge buckets (or one false detection, with `transmissions` 1),
/// as stored. `point` is the series point the row falls in (0 for a graph
/// read).
#[derive(Debug, Clone)]
pub struct EdgeRow {
    pub from: AgentId,
    pub to: AgentId,
    pub route: Route,
    pub topic: Option<TopicId>,
    pub point: usize,
    pub transmissions: u64,
    pub bytes: u64,
}

/// Summed access buckets, as recorded.
#[derive(Debug, Clone, Copy)]
pub struct AccessRow {
    pub agent: AgentId,
    pub resource: ResourceId,
    pub op: AccessKind,
    pub accesses: u64,
}

/// The rows of a read: bucket sums, and under `Exclude` the false
/// detections to subtract.
#[derive(Debug, Clone, Default)]
pub struct Rows {
    pub buckets: Vec<EdgeRow>,
    pub false_detections: Vec<EdgeRow>,
}

/// One resolved key of the fold, with its signed sums netted.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key {
    pub from: AgentId,
    pub to: AgentId,
    pub route: Route,
    pub topic: Option<TopicId>,
    pub point: usize,
}

/// The fold, per resolved key: what the filter admits, false detections
/// subtracted under `Exclude`, keys that net to nothing left out.
pub fn fold<V: TopologyEnv>(
    env: &V,
    filter: &TopologyFilter,
    rows: &Rows,
) -> Result<Vec<(Key, EdgeStats)>, DbError> {
    let aliases = EnvAliases(env);
    let include = TopologyFilter {
        false_detections: FalseDetections::Include,
        ..filter.clone()
    };
    let resolve = |row: &EdgeRow| -> Option<Key> {
        let from = env.canonical_agent(row.from);
        let to = env.canonical_agent(row.to);
        if from == to {
            return None;
        }
        let route = row.route.resolved(aliases);
        let subject = FilterSubject {
            from,
            to,
            route: &route,
            topic: row.topic,
            false_detection: false,
        };
        include.admits(&subject, aliases).then_some(Key {
            from,
            to,
            route,
            topic: row.topic,
            point: row.point,
        })
    };
    let mut sums: HashMap<Key, (u64, u64)> = HashMap::new();
    for row in &rows.buckets {
        if let Some(key) = resolve(row) {
            let entry = sums.entry(key).or_default();
            entry.0 = entry.0.saturating_add(row.transmissions);
            entry.1 = entry.1.saturating_add(row.bytes);
        }
    }
    if filter.false_detections == FalseDetections::Exclude {
        for row in &rows.false_detections {
            if let Some(key) = resolve(row) {
                let entry = sums.get_mut(&key).ok_or_else(|| {
                    DbError::inconsistent("a false detection outside every bucket")
                })?;
                entry.0 = entry
                    .0
                    .checked_sub(row.transmissions)
                    .ok_or_else(|| DbError::inconsistent("false detections exceed their bucket"))?;
                entry.1 = entry.1.checked_sub(row.bytes).ok_or_else(|| {
                    DbError::inconsistent("false detection bytes exceed their bucket")
                })?;
            }
        }
    }
    let mut kept = Vec::with_capacity(sums.len());
    for (key, (transmissions, bytes)) in sums {
        let Some(transmissions) = NonZeroU64::new(transmissions) else {
            continue;
        };
        let matched_bytes = NonZeroU64::new(bytes)
            .ok_or_else(|| DbError::inconsistent("an edge counted transmissions without bytes"))?;
        kept.push((
            key,
            EdgeStats {
                transmissions,
                matched_bytes,
            },
        ));
    }
    Ok(kept)
}

/// A total order on routes, for stable output: kind, then the route's
/// debug text (its channel, direction or carrier).
pub fn route_key(route: &Route) -> (u8, String) {
    (kind_index(RouteKind::of(route)), format!("{route:?}"))
}

pub fn kind_index(kind: RouteKind) -> u8 {
    match kind {
        RouteKind::Channel => 0,
        RouteKind::Delegation => 1,
        RouteKind::Direct => 2,
        RouteKind::Unobserved => 3,
    }
}

fn op_index(op: AccessKind) -> u8 {
    match op {
        AccessKind::Write => 0,
        AccessKind::Read => 1,
    }
}

/// The share `value / total`; `None` only for a ratio out of range, which
/// a sum of positive values never makes.
#[allow(clippy::cast_precision_loss)]
fn share(value: u64, total: u64) -> Result<Share, DbError> {
    Share::new(value as f64 / total as f64)
        .ok_or_else(|| DbError::inconsistent("a share out of range"))
}

fn add(sum: Option<NonZeroU64>, value: NonZeroU64) -> NonZeroU64 {
    sum.map_or(value, |sum| sum.saturating_add(value.get()))
}

/// The graph's edges: the fold summed per (from, to, route), each with its
/// share of the total under `weighting`, ordered by from, to and route.
pub fn edges(
    folded: &[(Key, EdgeStats)],
    weighting: Weighting,
) -> Result<Vec<WeightedEdge>, DbError> {
    let mut sums: HashMap<(AgentId, AgentId, Route), EdgeStats> = HashMap::new();
    for (key, stats) in folded {
        sums.entry((key.from, key.to, key.route.clone()))
            .and_modify(|sum| {
                sum.transmissions = add(Some(sum.transmissions), stats.transmissions);
                sum.matched_bytes = add(Some(sum.matched_bytes), stats.matched_bytes);
            })
            .or_insert(*stats);
    }
    let mut stats: Vec<_> = sums.into_iter().collect();
    stats.sort_by_key(|((from, to, route), _)| (*from, *to, route_key(route)));
    let total = stats.iter().fold(0u64, |sum, (_, stats)| {
        sum.saturating_add(weighting.stat(*stats).get())
    });
    stats
        .into_iter()
        .map(|((from, to, route), stats)| {
            Ok(WeightedEdge {
                from,
                to,
                route,
                share: share(weighting.stat(stats).get(), total)?,
                stats,
            })
        })
        .collect()
}

/// Agent nodes for `endpoints` and every canonical ancestor of one, by id,
/// with counts from `edges`; then channel nodes for `channels`, by id, a
/// channel a transmission edge is routed through confirmed whatever its
/// facts say.
pub fn nodes<V: TopologyEnv>(
    env: &V,
    endpoints: impl IntoIterator<Item = AgentId>,
    channels: impl IntoIterator<Item = ChannelId>,
    edges: &[WeightedEdge],
) -> Vec<GraphNode> {
    let mut agents: BTreeMap<AgentId, AgentNode> = BTreeMap::new();
    let mut frontier: Vec<AgentId> = endpoints.into_iter().collect();
    while let Some(agent) = frontier.pop() {
        if agents.contains_key(&agent) {
            continue;
        }
        let facts = env.agent(agent);
        let parent = facts
            .parent
            .map(|parent| env.canonical_agent(parent))
            .filter(|parent| *parent != agent);
        if let Some(parent) = parent {
            frontier.push(parent);
        }
        agents.insert(
            agent,
            AgentNode {
                id: agent,
                label: facts.label,
                state_kind: facts.state,
                parent,
                claims: facts.claims,
                transmissions_in: 0,
                transmissions_out: 0,
            },
        );
    }
    for edge in edges {
        if let Some(node) = agents.get_mut(&edge.to) {
            node.transmissions_in = node
                .transmissions_in
                .saturating_add(edge.stats.transmissions.get());
        }
        if let Some(node) = agents.get_mut(&edge.from) {
            node.transmissions_out = node
                .transmissions_out
                .saturating_add(edge.stats.transmissions.get());
        }
    }
    let channels: BTreeSet<ChannelId> = channels.into_iter().collect();
    let routed: BTreeSet<ChannelId> = edges
        .iter()
        .filter_map(|edge| match edge.route {
            Route::Channel(channel) => Some(channel),
            Route::Delegation(_) | Route::Direct(_) | Route::Unobserved => None,
        })
        .collect();
    agents
        .into_values()
        .map(GraphNode::Agent)
        .chain(channels.into_iter().map(|channel| {
            let facts = env.channel(channel);
            let confirmation = if routed.contains(&channel) {
                Confirmation::Confirmed
            } else {
                facts
                    .listing
                    .confirmation()
                    .unwrap_or(Confirmation::Confirmed)
            };
            GraphNode::Channel(ChannelNode {
                id: channel,
                label: facts.label,
                origin_kind: facts.origin,
                detection_kind: facts.detection,
                confirmation,
                policy_kind: facts.policy,
                locator_summary: facts.locator_summary,
            })
        }))
        .collect()
}

/// For each canonical channel, the topics of the channel-routed
/// transmissions on it that the filter's `false_detections` keeps.
/// Self-edges count: the content still flowed through the channel.
pub fn channel_topics<V: TopologyEnv>(
    env: &V,
    false_detections: FalseDetections,
    rows: &Rows,
) -> Result<BTreeMap<ChannelId, BTreeSet<TopicId>>, DbError> {
    let mut counts: HashMap<(ChannelId, TopicId), u64> = HashMap::new();
    for row in &rows.buckets {
        if let (Route::Channel(channel), Some(topic)) = (&row.route, row.topic) {
            let entry = counts.entry((*channel, topic)).or_default();
            *entry = entry.saturating_add(row.transmissions);
        }
    }
    if false_detections == FalseDetections::Exclude {
        for row in &rows.false_detections {
            if let (Route::Channel(channel), Some(topic)) = (&row.route, row.topic) {
                let entry = counts.get_mut(&(*channel, topic)).ok_or_else(|| {
                    DbError::inconsistent("a false detection outside every bucket")
                })?;
                *entry = entry
                    .checked_sub(row.transmissions)
                    .ok_or_else(|| DbError::inconsistent("false detections exceed their bucket"))?;
            }
        }
    }
    let mut topics: BTreeMap<ChannelId, BTreeSet<TopicId>> = BTreeMap::new();
    for ((channel, topic), count) in counts {
        if count > 0 {
            topics
                .entry(env.canonical_channel(channel))
                .or_default()
                .insert(topic);
        }
    }
    Ok(topics)
}

/// The access edges of the channel-centred view: access sums with agents
/// resolved and each resource resolved to the canonical channel holding it
/// now, left out when that is no channel or one not listed as a channel,
/// kept by `admits_access` with the channel's confirmation, summed per
/// (agent, channel, op), with shares over all of them.
pub fn access_edges<V: TopologyEnv>(
    env: &V,
    filter: &TopologyFilter,
    accesses: &[AccessRow],
    topics: &BTreeMap<ChannelId, BTreeSet<TopicId>>,
) -> Result<Vec<WeightedAccess>, DbError> {
    let aliases = EnvAliases(env);
    let mut sums: BTreeMap<(AgentId, ChannelId, u8), (AccessKind, u64)> = BTreeMap::new();
    for access in accesses {
        let agent = env.canonical_agent(access.agent);
        let Some(channel) = env.channel_of(access.resource) else {
            continue;
        };
        let Some(Listing::Channel(confirmation)) =
            env.known_channel(channel).map(|facts| facts.listing)
        else {
            continue;
        };
        let channel_topics: Vec<TopicId> = topics
            .get(&channel)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();
        let subject = AccessSubject {
            agent,
            channel,
            confirmation,
            channel_topics: &channel_topics,
        };
        if filter.admits_access(&subject, aliases) {
            let entry = sums
                .entry((agent, channel, op_index(access.op)))
                .or_insert((access.op, 0));
            entry.1 = entry.1.saturating_add(access.accesses);
        }
    }
    let total = sums
        .values()
        .fold(0u64, |sum, (_, n)| sum.saturating_add(*n));
    sums.into_iter()
        .map(|((agent, channel, _), (op, accesses))| {
            Ok(WeightedAccess {
                agent,
                channel,
                op,
                accesses: NonZeroU64::new(accesses)
                    .ok_or_else(|| DbError::inconsistent("an access edge counted nothing"))?,
                share: share(accesses, total)?,
            })
        })
        .collect()
}

/// The series of the fold over `points` points, grouped by `grouping`.
pub fn series_groups(
    folded: &[(Key, EdgeStats)],
    points: usize,
    weighting: Weighting,
    grouping: SeriesGrouping,
) -> SeriesGroups {
    let add = |values: &mut Vec<u64>, key: &Key, stats: &EdgeStats| {
        if let Some(value) = values.get_mut(key.point) {
            *value = value.saturating_add(weighting.stat(*stats).get());
        }
    };
    match grouping {
        SeriesGrouping::Total => {
            let mut values = vec![0; points];
            for (key, stats) in folded {
                add(&mut values, key, stats);
            }
            SeriesGroups::Total(values)
        }
        SeriesGrouping::Topic => {
            let mut by_topic: BTreeMap<Option<TopicId>, Vec<u64>> = BTreeMap::new();
            for (key, stats) in folded {
                add(
                    by_topic.entry(key.topic).or_insert_with(|| vec![0; points]),
                    key,
                    stats,
                );
            }
            SeriesGroups::ByTopic(
                by_topic
                    .into_iter()
                    .map(|(key, values)| Series { key, values })
                    .collect(),
            )
        }
        SeriesGrouping::RouteKind => {
            let mut by_kind: BTreeMap<u8, (RouteKind, Vec<u64>)> = BTreeMap::new();
            for (key, stats) in folded {
                let kind = RouteKind::of(&key.route);
                let (_, values) = by_kind
                    .entry(kind_index(kind))
                    .or_insert_with(|| (kind, vec![0; points]));
                add(values, key, stats);
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
            for (key, stats) in folded {
                add(
                    by_edge
                        .entry((key.from, key.to, key.route.clone()))
                        .or_insert_with(|| vec![0; points]),
                    key,
                    stats,
                );
            }
            let mut series: Vec<Series<SeriesEdge>> = by_edge
                .into_iter()
                .map(|((from, to, route), values)| Series {
                    key: SeriesEdge { from, to, route },
                    values,
                })
                .collect();
            series.sort_by_key(|one| (one.key.from, one.key.to, route_key(&one.key.route)));
            SeriesGroups::ByEdge(series)
        }
    }
}
