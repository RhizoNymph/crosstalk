//! The topology graph, the channel-centred graph and the overview's counts,
//! as `EdgeStore::graph`, `channel_topology` and `totals` define them.
//!
//! Every view resolves its filter's version once ([`Linked::new`]) and
//! counts confirmed transmissions by `Confirmed::at` over canonical agents
//! and channels; self-edges after resolution are dropped. Windows must be
//! on bucket boundaries. Every result carries the fixture's watermark.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use crosstalk_spec::aggregates::access::{BipartiteGraph, BipartiteParts, WeightedAccess};
use crosstalk_spec::aggregates::alert;
use crosstalk_spec::aggregates::edge::{
    EdgeStats, EdgeTotals, TopologyFilter, TopologyGraph, WeightedEdge, Weighting,
};
use crosstalk_spec::aggregates::watermark::{Watermark, Watermarked};
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::interfaces::l7_topology::EdgeQueryError;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::overview::{OverviewCounts, QueueCounts};
use crosstalk_spec::support::{Share, TimeWindow};

use crate::backend::Result;
use crate::backend::fixture::clock::{BUCKET, WATERMARK};
use crate::contract::alerts::AlertState;

use super::linked::{Counted, Linked};
use super::{Ctx, nodes, route_key};

/// The watermark every aggregate reports: ten minutes before the end of
/// the data, a bucket boundary.
pub fn watermark() -> Watermark {
    Watermark(WATERMARK)
}

pub fn watermarked<T>(value: T) -> Watermarked<T> {
    Watermarked {
        watermark: watermark(),
        value,
    }
}

pub fn store_error(what: &str, detail: impl std::fmt::Debug) -> QueryError {
    QueryError::Store {
        reason: format!("fixture graph: {what}: {detail:?}"),
    }
}

/// Refuses a window not on bucket boundaries.
pub fn aligned(window: TimeWindow) -> Result<()> {
    if BUCKET.is_boundary(window.start()) && BUCKET.is_boundary(window.end()) {
        Ok(())
    } else {
        Err(EdgeQueryError::UnalignedWindow.into())
    }
}

/// `value` over `total` as a share.
#[allow(clippy::cast_precision_loss)]
fn share(value: u64, total: u64) -> Result<Share> {
    let ratio = if total == 0 {
        0.0
    } else {
        value as f64 / total as f64
    };
    Share::new(ratio).ok_or_else(|| store_error("share out of range", (value, total)))
}

/// Sums transmissions into edges per (from, to, route), in key order, with
/// shares of the total under `weighting`.
pub fn edges(counted: &[Counted], weighting: Weighting) -> Result<Vec<WeightedEdge>> {
    type Key = (AgentId, AgentId, (u8, u128, String));
    let mut sums: BTreeMap<Key, (Route, u64, u64)> = BTreeMap::new();
    for c in counted {
        let entry = sums
            .entry((c.from, c.to, route_key(&c.route)))
            .or_insert_with(|| (c.route.clone(), 0, 0));
        entry.1 = entry.1.saturating_add(1);
        entry.2 = entry.2.saturating_add(c.matched_bytes.get());
    }
    let stat = |n: u64, bytes: u64| match weighting {
        Weighting::Transmissions => n,
        Weighting::MatchedBytes => bytes,
    };
    let total = sums.values().fold(0u64, |sum, (_, n, bytes)| {
        sum.saturating_add(stat(*n, *bytes))
    });
    sums.into_iter()
        .map(|((from, to, _), (route, n, bytes))| {
            Ok(WeightedEdge {
                from,
                to,
                route,
                stats: EdgeStats {
                    transmissions: NonZeroU64::new(n)
                        .ok_or_else(|| store_error("empty edge", (from, to)))?,
                    matched_bytes: NonZeroU64::new(bytes)
                        .ok_or_else(|| store_error("edge without bytes", (from, to)))?,
                },
                share: share(stat(n, bytes), total)?,
            })
        })
        .collect()
}

/// The graph over canonical agents for an aligned window, its nodes
/// checked with `TopologyGraph::check_nodes`.
pub fn graph(
    ctx: &Ctx,
    window: TimeWindow,
    weighting: Weighting,
    filter: &TopologyFilter,
) -> Result<TopologyGraph> {
    aligned(window)?;
    let linked = Linked::new(ctx, window, filter)?;
    let edges = edges(&linked.counted(), weighting)?;
    let nodes = nodes::agent_nodes(ctx, edges.iter().flat_map(|e| [e.from, e.to]), &edges)?;
    let graph = TopologyGraph {
        window,
        weighting,
        topic_version: linked.version,
        nodes,
        edges,
    };
    graph
        .check_nodes()
        .map_err(|e| store_error("graph nodes", e))?;
    Ok(graph)
}

pub fn topology(
    ctx: &Ctx,
    window: TimeWindow,
    weighting: Weighting,
    filter: &TopologyFilter,
) -> Result<Watermarked<TopologyGraph>> {
    graph(ctx, window, weighting, filter).map(watermarked)
}

/// What waits for an operator, as `QueueCounts::tally` defines it: alerts
/// in state `Open`, and channels in force whose policy is unreviewed. The
/// fixture's alerts are the contract's (one more suppress reason), so they
/// are counted by the same rule here; channels go through `tally` itself,
/// after dropping superseded ones (the fixture keeps supersession beside
/// the spec channel, not in its origin).
fn queues(ctx: &Ctx) -> QueueCounts {
    let open_alerts = ctx
        .state
        .alerts
        .iter()
        .filter(|a| matches!(a.state, AlertState::Open))
        .count();
    let in_force = ctx
        .state
        .channels
        .values()
        .filter(|r| r.superseded.is_none())
        .map(|r| &r.channel);
    let channels = QueueCounts::tally(std::iter::empty::<&alert::Alert>(), in_force);
    QueueCounts {
        open_alerts: u64::try_from(open_alerts).unwrap_or(u64::MAX),
        unreviewed_channels: channels.unreviewed_channels,
    }
}

/// The overview: `EdgeTotals::of` the graph for the window and filter, and
/// the queues, which no window or filter narrows.
pub fn overview(
    ctx: &Ctx,
    window: TimeWindow,
    filter: &TopologyFilter,
) -> Result<Watermarked<OverviewCounts>> {
    let graph = graph(ctx, window, Weighting::Transmissions, filter)?;
    Ok(watermarked(OverviewCounts {
        activity: EdgeTotals::of(&graph),
        queues: queues(ctx),
    }))
}

fn op_order(op: AccessKind) -> u8 {
    match op {
        AccessKind::Write => 0,
        AccessKind::Read => 1,
    }
}

/// Access buckets in the window, resolved, admitted by
/// `TopologyFilter::admits_access` and summed per (agent, channel, op),
/// with shares of all of them.
fn accesses(linked: &Linked) -> Result<Vec<WeightedAccess>> {
    let ctx = linked.ctx;
    let topics = linked.channel_topics();
    let mut sums: BTreeMap<(AgentId, ChannelId, u8), (AccessKind, u64)> = BTreeMap::new();
    for access in &ctx.world.accesses {
        if !linked.in_window(access.at) {
            continue;
        }
        let Some(raw) = ctx.world.resource_channel.get(&access.resource) else {
            continue;
        };
        let (agent, channel) = (ctx.agent(access.agent), ctx.channel(*raw));
        if !linked.admits_access(agent, channel, &topics) {
            continue;
        }
        let op = access.op.kind();
        let entry = sums
            .entry((agent, channel, op_order(op)))
            .or_insert((op, 0));
        entry.1 = entry.1.saturating_add(1);
    }
    let total = sums
        .values()
        .fold(0u64, |sum, (_, n)| sum.saturating_add(*n));
    sums.into_iter()
        .map(|((agent, channel, _), (op, n))| {
            Ok(WeightedAccess {
                agent,
                channel,
                op,
                accesses: NonZeroU64::new(n)
                    .ok_or_else(|| store_error("empty access edge", (agent, channel)))?,
                share: share(n, total)?,
            })
        })
        .collect()
}

/// The channel-centred graph: access edges, the topology's transmission
/// edges, and nodes for every agent and channel they name.
pub fn channel_topology(
    ctx: &Ctx,
    window: TimeWindow,
    weighting: Weighting,
    filter: &TopologyFilter,
) -> Result<Watermarked<BipartiteGraph>> {
    aligned(window)?;
    let linked = Linked::new(ctx, window, filter)?;
    let transmissions = edges(&linked.counted(), weighting)?;
    let accesses = accesses(&linked)?;
    let agents = accesses
        .iter()
        .map(|a| a.agent)
        .chain(transmissions.iter().flat_map(|e| [e.from, e.to]));
    let channels = accesses
        .iter()
        .map(|a| a.channel)
        .chain(transmissions.iter().filter_map(|e| match e.route {
            Route::Channel(channel) => Some(channel),
            Route::Delegation(_) | Route::Direct(_) | Route::Unobserved => None,
        }));
    let mut nodes = nodes::agent_nodes(ctx, agents, &transmissions)?;
    nodes.extend(nodes::channel_nodes(ctx, channels)?);
    let graph = BipartiteGraph::new(BipartiteParts {
        window,
        weighting,
        topic_version: linked.version,
        nodes,
        accesses,
        transmissions,
    })
    .map_err(|e| store_error("channel-centred graph", e))?;
    Ok(watermarked(graph))
}
