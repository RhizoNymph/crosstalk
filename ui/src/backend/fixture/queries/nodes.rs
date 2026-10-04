//! Graph nodes as `crosstalk_spec::aggregates::node` defines them: one per
//! endpoint and canonical ancestor, read from the agent and channel records
//! at query time.

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::aggregates::edge::WeightedEdge;
use crosstalk_spec::aggregates::node::{
    AgentNode, CanonicalOriginKind, CanonicalStateKind, ChannelNode, GraphNode,
};
use crosstalk_spec::derived::flow::channel::{ChannelOrigin, DeclaredHistory};
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::support::NonBlank;

use crate::backend::Result;
use crate::backend::fixture::store::ChannelRecord;
use crate::data::names::{locator_name, pattern_name};
use crate::pending::channel_semantics::Confirmation;

use super::Ctx;
use super::agents::profile::{claims, parent};

fn store(what: &str, detail: impl std::fmt::Debug) -> QueryError {
    QueryError::Store {
        reason: format!("fixture graph node: {what}: {detail:?}"),
    }
}

/// `ids` and every canonical ancestor of each, in id order.
fn with_ancestors(ctx: &Ctx, ids: impl IntoIterator<Item = AgentId>) -> BTreeSet<AgentId> {
    let mut wanted = BTreeSet::new();
    // A parent chain visits each agent at most once, so the number of
    // agents bounds it even if a bug made a cycle.
    let bound = ctx.state.identity.len().max(1);
    for id in ids {
        let mut current = Some(ctx.agent(id));
        for _ in 0..bound {
            let Some(agent) = current else { break };
            if !wanted.insert(agent) {
                break;
            }
            current = parent(ctx, agent);
        }
    }
    wanted
}

/// Transmissions into and out of each agent on `edges`.
fn counts(edges: &[WeightedEdge]) -> BTreeMap<AgentId, (u64, u64)> {
    let mut counts: BTreeMap<AgentId, (u64, u64)> = BTreeMap::new();
    for edge in edges {
        let n = edge.stats.transmissions.get();
        let into = counts.entry(edge.to).or_default();
        into.0 = into.0.saturating_add(n);
        let out = counts.entry(edge.from).or_default();
        out.1 = out.1.saturating_add(n);
    }
    counts
}

fn agent_node(ctx: &Ctx, id: AgentId, (into, out): (u64, u64)) -> Result<AgentNode> {
    let agent = ctx
        .state
        .identity
        .agent(id)
        .ok_or_else(|| store("unknown agent", id))?;
    let state_kind =
        CanonicalStateKind::of(&agent.state).ok_or_else(|| store("merged agent as a node", id))?;
    Ok(AgentNode {
        id,
        label: agent.label.clone(),
        state_kind,
        parent: parent(ctx, id),
        claims: claims(ctx, id),
        transmissions_in: into,
        transmissions_out: out,
    })
}

/// One agent node per id of `endpoints` and per canonical ancestor of one,
/// in id order, counting the transmissions of `edges`.
pub fn agent_nodes(
    ctx: &Ctx,
    endpoints: impl IntoIterator<Item = AgentId>,
    edges: &[WeightedEdge],
) -> Result<Vec<GraphNode>> {
    let counts = counts(edges);
    with_ancestors(ctx, endpoints)
        .into_iter()
        .map(|id| {
            let count = counts.get(&id).copied().unwrap_or_default();
            agent_node(ctx, id, count).map(GraphNode::Agent)
        })
        .collect()
}

/// One channel node per distinct id of `channels`, in id order, and each
/// node's confirmation (the stand-in for `ChannelNode::confirmation`).
pub fn channel_nodes(
    ctx: &Ctx,
    channels: impl IntoIterator<Item = ChannelId>,
) -> Result<(Vec<GraphNode>, BTreeMap<ChannelId, Confirmation>)> {
    let ids: BTreeSet<ChannelId> = channels.into_iter().collect();
    let mut nodes = Vec::with_capacity(ids.len());
    let mut confirmations = BTreeMap::new();
    for id in ids {
        let record = ctx
            .state
            .channels
            .get(&id)
            .ok_or_else(|| store("unknown channel", id))?;
        let (node, confirmation) = channel_node(ctx, record)?;
        confirmations.insert(id, confirmation);
        nodes.push(GraphNode::Channel(node));
    }
    Ok((nodes, confirmations))
}

fn channel_node(ctx: &Ctx, record: &ChannelRecord) -> Result<(ChannelNode, Confirmation)> {
    let channel = record.channel();
    let origin_kind = CanonicalOriginKind::of(&channel.origin)
        .ok_or_else(|| store("superseded channel as a node", channel.id))?;
    let confirmation = ctx
        .confirmation(channel.id)
        .ok_or_else(|| store("a channel not listed as a channel as a node", channel.id))?;
    let node = ChannelNode {
        id: channel.id,
        label: None,
        origin_kind,
        detection_kind: channel.origin.detection_kind(),
        policy_kind: channel.policy.kind(),
        locator_summary: locator_summary(ctx, record)?,
    };
    Ok((node, confirmation))
}

/// What a channel covers, as the spec's `locator_summary` words it: the
/// pattern of a channel declared before traffic, otherwise its seed's
/// locator with the count of further resources (`… (+12)`).
fn locator_summary(ctx: &Ctx, record: &ChannelRecord) -> Result<NonBlank> {
    let channel = record.channel();
    let text = match (&channel.origin, channel.origin.seed()) {
        (
            ChannelOrigin::Declared {
                declaration,
                history: DeclaredHistory::BeforeTraffic(_),
            },
            _,
        ) => pattern_name(&declaration.pattern),
        (_, Some(seed)) => {
            let resource = ctx
                .world
                .resource(seed.resource)
                .ok_or_else(|| store("unknown seed resource", seed.resource))?;
            let further = channel
                .resources
                .iter()
                .filter(|r| **r != seed.resource)
                .count();
            match further {
                0 => locator_name(&resource.locator),
                n => format!("{} (+{n})", locator_name(&resource.locator)),
            }
        }
        (_, None) => return Err(store("channel without pattern or seed", channel.id)),
    };
    NonBlank::new(&text).map_err(|e| store("locator summary", e))
}
