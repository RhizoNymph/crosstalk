//! The reference fold: what every graph, series, total and drill-down
//! counts, computed straight from the stored contributions.
//!
//! `counted` is the fold of `topology.graph.matches-fold-model`: the
//! contributions classified under one version (one per transmission) whose
//! time the window holds, sender and reader resolved through the merge
//! table and the route through supersession, resolved self-edges dropped,
//! and those the filter admits kept (under `Exclude`, not those the store's
//! verdict copy holds as `FalseDetection`). Every read builds on it.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crosstalk_spec::aggregates::access::WeightedAccess;
use crosstalk_spec::aggregates::edge::{
    EdgeSelector, EdgeStats, RouteKind, TopologyFilter, WeightedEdge, Weighting,
};
use crosstalk_spec::aggregates::filter::{
    AccessSubject, FalseDetections, FilterSubject, VersionUnavailable,
};
use crosstalk_spec::aggregates::node::{AgentNode, ChannelNode, GraphNode};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::derived::flow::verdict::CurrentVerdict;
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId, TransmissionId};
use crosstalk_spec::interfaces::l7_topology::{EdgeContribution, EdgeQueryError};
use crosstalk_spec::support::{Share, TimeWindow, Timestamp};

use super::env::{EnvAliases, TopologyEnv};
use super::store::EdgeState;

/// The request an edge drill-down cursor is bound to.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeBinding {
    pub edge: EdgeSelector,
    pub window: TimeWindow,
    pub filter: TopologyFilter,
}

/// What an edge drill-down cursor resumes with: the version the first page
/// resolved and the last row served.
#[derive(Debug, Clone, Copy)]
pub struct EdgeResume {
    pub version: TopicModelVersion,
    pub confirmed_at: Timestamp,
    pub transmission: TransmissionId,
}

/// One contribution as a read counts it.
#[derive(Debug, Clone)]
pub struct Counted<'a> {
    pub contribution: &'a EdgeContribution,
    /// Canonical sender and reader, never equal.
    pub from: AgentId,
    pub to: AgentId,
    /// The route with its channel resolved.
    pub route: Route,
}

/// The fold: every contribution of `version` whose time `window` holds that
/// survives resolution and `filter`.
pub(super) fn counted<'a, V: TopologyEnv>(
    state: &'a EdgeState,
    env: &V,
    version: TopicModelVersion,
    window: TimeWindow,
    filter: &TopologyFilter,
) -> Vec<Counted<'a>> {
    let aliases = EnvAliases(env);
    state
        .contributions
        .range((version, TransmissionId::from_ulid(0))..)
        .take_while(|((stored, _), _)| *stored == version)
        .map(|(_, contribution)| contribution)
        .filter(|contribution| window.contains(contribution.at))
        .filter_map(|contribution| {
            let from = env.canonical_agent(contribution.from);
            let to = env.canonical_agent(contribution.to);
            if from == to {
                return None;
            }
            let route = contribution.route.resolved(aliases);
            let subject = FilterSubject {
                from,
                to,
                route: &route,
                topic: contribution.classification.topic,
                false_detection: CurrentVerdict::is_false_detection(
                    state.verdicts.get(&contribution.transmission),
                ),
            };
            filter.admits(&subject, aliases).then_some(Counted {
                contribution,
                from,
                to,
                route,
            })
        })
        .collect()
}

/// The version a read computes under: the selector resolved against the
/// catalog's history with every version not dropped here retained, and the
/// filter's topics checked against it.
pub(super) fn resolve_version<V: TopologyEnv>(
    state: &EdgeState,
    env: &V,
    filter: &TopologyFilter,
) -> Result<TopicModelVersion, EdgeQueryError> {
    let history = env.history();
    let version = filter
        .topic_version
        .resolve(&history, |version| !state.dropped.contains(&version))
        .map_err(EdgeQueryError::Version)?;
    if state.dropped.contains(&version) {
        return Err(EdgeQueryError::Version(VersionUnavailable::NotRetained(
            version,
        )));
    }
    let outside = filter.topics_outside(version, |topic| env.version_of(topic));
    if !outside.is_empty() {
        return Err(EdgeQueryError::TopicsNotInVersion {
            version,
            topics: outside,
        });
    }
    Ok(version)
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

/// The share `value / total`. `None` only when the ratio is out of range,
/// which a sum of positive values never makes.
#[allow(clippy::cast_precision_loss)]
fn share(value: u64, total: u64) -> Option<Share> {
    Share::new(value as f64 / total as f64)
}

fn store_error(reason: &str) -> EdgeQueryError {
    EdgeQueryError::Store {
        reason: reason.to_owned(),
    }
}

/// The graph's edges: the fold summed per (from, to, route), each with its
/// share of the total under `weighting`. Ordered by from, to and route.
pub fn edges(
    counted: &[Counted<'_>],
    weighting: Weighting,
) -> Result<Vec<WeightedEdge>, EdgeQueryError> {
    let mut sums: HashMap<(AgentId, AgentId, Route), (u64, u64)> = HashMap::new();
    for one in counted {
        let entry = sums
            .entry((one.from, one.to, one.route.clone()))
            .or_default();
        entry.0 = entry.0.saturating_add(1);
        entry.1 = entry.1.saturating_add(one.contribution.matched_bytes.get());
    }
    let mut stats: Vec<(AgentId, AgentId, Route, EdgeStats)> = Vec::new();
    for ((from, to, route), (transmissions, bytes)) in sums {
        let stat = EdgeStats {
            transmissions: std::num::NonZeroU64::new(transmissions)
                .ok_or_else(|| store_error("an edge counted nothing"))?,
            matched_bytes: std::num::NonZeroU64::new(bytes)
                .ok_or_else(|| store_error("an edge carried no bytes"))?,
        };
        stats.push((from, to, route, stat));
    }
    stats.sort_by_key(|edge| (edge.0, edge.1, route_key(&edge.2)));
    let total = stats.iter().fold(0u64, |sum, edge| {
        sum.saturating_add(weighting.stat(edge.3).get())
    });
    stats
        .into_iter()
        .map(|(from, to, route, stats)| {
            Ok(WeightedEdge {
                from,
                to,
                route,
                share: share(weighting.stat(stats).get(), total)
                    .ok_or_else(|| store_error("a share out of range"))?,
                stats,
            })
        })
        .collect()
}

/// Agent nodes for `endpoints` and every canonical ancestor of one, by id,
/// with counts from `edges`; then channel nodes for `channels`, by id.
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
        let description = env.agent(agent);
        let parent = description
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
                label: description.label,
                state_kind: description.state,
                parent,
                claims: description.claims,
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
    agents
        .into_values()
        .map(GraphNode::Agent)
        .chain(channels.into_iter().map(|channel| {
            let description = env.channel(channel);
            GraphNode::Channel(ChannelNode {
                id: channel,
                label: description.label,
                origin_kind: description.origin,
                detection_kind: description.detection,
                policy_kind: description.policy,
                locator_summary: description.locator_summary,
            })
        }))
        .collect()
}

/// The access edges of the channel-centred view: access buckets in
/// `window`, agents and channels resolved, kept by `admits_access`, summed
/// per (agent, channel, op), with shares over all of them.
pub(super) fn access_edges<V: TopologyEnv>(
    state: &EdgeState,
    env: &V,
    version: TopicModelVersion,
    window: TimeWindow,
    filter: &TopologyFilter,
) -> Result<Vec<WeightedAccess>, EdgeQueryError> {
    let aliases = EnvAliases(env);
    let topics = channel_topics(state, env, version, window, filter.false_detections);
    let mut sums: BTreeMap<(AgentId, ChannelId, u8), (AccessKind, u64)> = BTreeMap::new();
    for access in state.accesses.values() {
        if !window.contains(access.at) {
            continue;
        }
        let agent = env.canonical_agent(access.agent);
        let channel = env.canonical_channel(access.channel);
        let channel_topics: Vec<TopicId> = topics
            .get(&channel)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();
        let subject = AccessSubject {
            agent,
            channel,
            channel_topics: &channel_topics,
        };
        if filter.admits_access(&subject, aliases) {
            let entry = sums
                .entry((agent, channel, op_index(access.op)))
                .or_insert((access.op, 0));
            entry.1 = entry.1.saturating_add(1);
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
                accesses: std::num::NonZeroU64::new(accesses)
                    .ok_or_else(|| store_error("an access edge counted nothing"))?,
                share: share(accesses, total).ok_or_else(|| store_error("a share out of range"))?,
            })
        })
        .collect()
}

/// For each canonical channel, the topics under `version` of the
/// channel-routed contributions on it in `window` that `false_detections`
/// keeps. Self-edges count: the content still flowed through the channel.
fn channel_topics<V: TopologyEnv>(
    state: &EdgeState,
    env: &V,
    version: TopicModelVersion,
    window: TimeWindow,
    false_detections: FalseDetections,
) -> BTreeMap<ChannelId, BTreeSet<TopicId>> {
    let mut topics: BTreeMap<ChannelId, BTreeSet<TopicId>> = BTreeMap::new();
    let contributions = state
        .contributions
        .range((version, TransmissionId::from_ulid(0))..)
        .take_while(|((stored, _), _)| *stored == version)
        .map(|(_, contribution)| contribution);
    for contribution in contributions {
        if !window.contains(contribution.at) {
            continue;
        }
        let excluded = false_detections == FalseDetections::Exclude
            && CurrentVerdict::is_false_detection(state.verdicts.get(&contribution.transmission));
        if excluded {
            continue;
        }
        if let (Route::Channel(channel), Some(topic)) =
            (&contribution.route, contribution.classification.topic)
        {
            topics
                .entry(env.canonical_channel(*channel))
                .or_default()
                .insert(topic);
        }
    }
    topics
}
