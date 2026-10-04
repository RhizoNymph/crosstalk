//! Topology (agents), the bipartite view (channels as nodes) and the
//! timeline. Graphs count confirmed transmissions over canonical agents;
//! self-edges after alias resolution are dropped.

use std::collections::{BTreeMap, HashMap};
use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use crosstalk_spec::aggregates::edge::{EdgeStats, TopologyGraph, WeightedEdge, Weighting};
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::AgentId;
use crosstalk_spec::support::{Share, TimeWindow, Timestamp};

use crate::backend::Result;
use crate::backend::fixture::clock::WATERMARK;
use crate::backend::fixture::world::TxRecord;
use crate::contract::graph::{AccessEdge, BipartiteView, Timeline, TimelineBucket, TopologyView};
use crate::url::scope::Scope;
use crosstalk_spec::interfaces::l8_surface::QueryError;

use super::scope::Filter;
use super::summaries::{self, Counts};
use super::{Ctx, route_key};

/// A confirmed transmission in scope, resolved.
pub struct Counted<'a> {
    pub record: &'a TxRecord,
    pub from: AgentId,
    pub to: AgentId,
    pub route: Route,
}

/// The confirmed transmissions a graph counts: in scope, resolved, and not
/// a self-edge.
pub fn counted<'a>(filter: &Filter<'a>) -> Vec<Counted<'a>> {
    let ctx = filter.ctx;
    ctx.world
        .transmissions
        .iter()
        .filter(|r| filter.keeps(r))
        .filter_map(|record| {
            let from = ctx.agent(record.from?);
            let to = ctx.agent(record.transmission.to);
            (from != to).then(|| Counted {
                record,
                from,
                to,
                route: ctx.route(&record.transmission.route),
            })
        })
        .collect()
}

fn store_error(err: impl std::fmt::Display) -> QueryError {
    QueryError::Store {
        reason: err.to_string(),
    }
}

/// Sums transmissions into edges per (from, to, route), with shares under
/// `weighting` summing to 1.
fn edges<'a>(
    items: impl Iterator<Item = &'a Counted<'a>>,
    weighting: Weighting,
) -> Vec<WeightedEdge> {
    // Keyed by (from, to, route order); holds the route and its two sums.
    type Sums = BTreeMap<(AgentId, AgentId, (u8, u128, String)), (Route, u64, u64)>;
    let mut sums = Sums::new();
    for c in items {
        let entry = sums
            .entry((c.from, c.to, route_key(&c.route)))
            .or_insert_with(|| (c.route.clone(), 0, 0));
        entry.1 += 1;
        entry.2 += c.record.matched_bytes;
    }
    let total: u64 = sums
        .values()
        .map(|(_, n, bytes)| match weighting {
            Weighting::Transmissions => *n,
            Weighting::MatchedBytes => *bytes,
        })
        .sum();
    sums.into_iter()
        .filter_map(|((from, to, _), (route, n, bytes))| {
            let value = match weighting {
                Weighting::Transmissions => n,
                Weighting::MatchedBytes => bytes,
            };
            let share = Share::new(if total == 0 {
                0.0
            } else {
                value as f64 / total as f64
            })?;
            Some(WeightedEdge {
                from,
                to,
                route,
                stats: EdgeStats {
                    transmissions: NonZeroU64::new(n)?,
                    matched_bytes: NonZeroU64::new(bytes)?,
                },
                share,
            })
        })
        .collect()
}

fn counts<'a>(items: impl Iterator<Item = &'a Counted<'a>>) -> Counts {
    let mut counts = Counts::new();
    for c in items {
        counts.entry(c.to).or_default().0 += 1;
        counts.entry(c.from).or_default().1 += 1;
    }
    counts
}

pub fn topology(ctx: &Ctx, scope: &Scope, weighting: Weighting) -> Result<TopologyView> {
    let filter = Filter::new(ctx, scope)?;
    let items = counted(&filter);
    let edges = edges(items.iter(), weighting);
    let nodes = summaries::agents_with_parents(
        ctx,
        edges.iter().flat_map(|e| [e.from, e.to]),
        &counts(items.iter()),
    );
    let graph = TopologyGraph {
        window: scope.window,
        weighting,
        topic_version: scope.topic_version,
        nodes: Vec::new(),
        edges,
    };
    TopologyView::new(graph, nodes, WATERMARK).map_err(store_error)
}

pub fn channel_topology(ctx: &Ctx, scope: &Scope, weighting: Weighting) -> Result<BipartiteView> {
    let filter = Filter::new(ctx, scope)?;
    let items = counted(&filter);
    let direct: Vec<&Counted> = items
        .iter()
        .filter(|c| !matches!(c.route, Route::Channel(_)))
        .collect();
    let transmissions = edges(direct.iter().copied(), weighting);

    // Accesses per (channel, agent, op), after resolution.
    let mut sums: BTreeMap<(crosstalk_spec::ids::ChannelId, AgentId, u8), u64> = BTreeMap::new();
    for access in &ctx.world.accesses {
        if !scope.window.contains(access.at) {
            continue;
        }
        let Some(raw) = ctx.world.resource_channel.get(&access.resource) else {
            continue;
        };
        let (agent, channel) = (ctx.agent(access.agent), ctx.channel(*raw));
        if !filter.keeps_access(agent, channel) {
            continue;
        }
        if filter.has_topics() {
            let linked = ctx
                .world
                .access_transmissions
                .get(&access.id)
                .into_iter()
                .flatten()
                .filter_map(|t| ctx.world.tx(*t))
                .any(|t| filter.keeps(t));
            if !linked {
                continue;
            }
        }
        let op = match access.op.kind() {
            AccessKind::Write => 0,
            AccessKind::Read => 1,
        };
        *sums.entry((channel, agent, op)).or_default() += 1;
    }
    let total: u64 = sums.values().sum();
    let accesses: Vec<AccessEdge> = sums
        .into_iter()
        .filter_map(|((channel, agent, op), n)| {
            Some(AccessEdge {
                agent,
                channel,
                op: if op == 0 {
                    AccessKind::Write
                } else {
                    AccessKind::Read
                },
                accesses: NonZeroU64::new(n)?,
                share: Share::new(n as f64 / total as f64)?,
            })
        })
        .collect();

    let mut channel_ids: Vec<_> = accesses.iter().map(|a| a.channel).collect();
    channel_ids.sort();
    channel_ids.dedup();
    let channels = channel_ids
        .iter()
        .filter_map(|id| ctx.state.channels.get(id))
        .map(|r| summaries::node(ctx, r))
        .collect();
    // Channel-routed transmissions are drawn through access edges but still
    // count towards their agents' volume.
    let agent_counts = counts(items.iter());
    let agents = summaries::agents_with_parents(
        ctx,
        accesses
            .iter()
            .map(|a| a.agent)
            .chain(transmissions.iter().flat_map(|e| [e.from, e.to])),
        &agent_counts,
    );
    BipartiteView::new(
        scope.window,
        weighting,
        scope.topic_version,
        agents,
        channels,
        accesses,
        transmissions,
        WATERMARK,
    )
    .map_err(store_error)
}

/// `buckets` equal buckets over `window` (the last absorbs the remainder).
/// Fewer when the window is shorter than `buckets` microseconds.
pub fn buckets(window: TimeWindow, buckets: NonZeroU32) -> Vec<TimeWindow> {
    let start = window.start().as_micros();
    let span = window.end().as_micros() - start;
    let n = u64::from(buckets.get()).min(span).max(1);
    let width = span / n;
    (0..n)
        .filter_map(|i| {
            let from = start + i * width;
            let to = if i + 1 == n {
                window.end().as_micros()
            } else {
                from + width
            };
            TimeWindow::new(Timestamp::from_micros(from), Timestamp::from_micros(to)).ok()
        })
        .collect()
}

/// The bucket index of `at` among `windows`.
pub fn bucket_of(windows: &[TimeWindow], at: Timestamp) -> Option<usize> {
    let i = windows.partition_point(|w| w.end() <= at);
    windows.get(i).filter(|w| w.contains(at)).map(|_| i)
}

pub fn timeline(ctx: &Ctx, scope: &Scope, n: NonZeroU32) -> Result<Timeline> {
    let filter = Filter::new(ctx, scope)?;
    let windows = buckets(scope.window, n);
    let mut sums: HashMap<usize, (u64, u64)> = HashMap::new();
    for c in counted(&filter) {
        if let Some(i) = bucket_of(&windows, c.record.transmission.opened_at) {
            let slot = sums.entry(i).or_default();
            slot.0 += 1;
            slot.1 += c.record.matched_bytes;
        }
    }
    let width = windows
        .first()
        .map_or(0, |w| w.end().as_micros() - w.start().as_micros());
    Ok(Timeline {
        bucket_width: Duration::from_micros(width),
        buckets: windows
            .iter()
            .enumerate()
            .map(|(i, bucket)| {
                let (transmissions, matched_bytes) = sums.get(&i).copied().unwrap_or_default();
                TimelineBucket {
                    bucket: *bucket,
                    transmissions,
                    matched_bytes,
                }
            })
            .collect(),
        watermark: WATERMARK,
    })
}
