//! Graph nodes as `crosstalk_spec::aggregates::node` defines them: one per
//! endpoint and canonical ancestor, read from the agent and channel records
//! at query time. Also the channel shape the name lookups share.

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::aggregates::edge::WeightedEdge;
use crosstalk_spec::aggregates::node::{
    AgentNode, CanonicalOriginKind, CanonicalStateKind, ChannelNode, GraphNode,
};
use crosstalk_spec::derived::flow::channel::{ChannelOrigin, DeclaredHistory};
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::observed::agent::{AgentLabel, ClaimSet};
use crosstalk_spec::observed::message::ToolName;
use crosstalk_spec::support::NonBlank;

use crate::backend::Result;
use crate::backend::fixture::store::ChannelRecord;
use crate::contract::agents::AgentState;
use crate::contract::graph::ChannelShape;
use crate::data::names::{locator_name, pattern_name};

use super::Ctx;

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
    let bound = ctx.state.agents.len().max(1);
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

/// The canonical form of `id`'s parent, never `id` itself.
fn parent(ctx: &Ctx, id: AgentId) -> Option<AgentId> {
    ctx.state
        .agents
        .get(&id)
        .and_then(|r| r.agent.parent)
        .map(|p| ctx.agent(p))
        .filter(|p| *p != id)
}

fn state_kind(state: &AgentState) -> Option<CanonicalStateKind> {
    match state {
        AgentState::Registered { .. } => Some(CanonicalStateKind::Registered),
        AgentState::Provisional { .. } => Some(CanonicalStateKind::Provisional),
        AgentState::Established { .. } => Some(CanonicalStateKind::Established),
        AgentState::Merged { .. } => None,
    }
}

/// The claims seen on canonical `id` and every alias of it.
fn claims(ctx: &Ctx, id: AgentId) -> ClaimSet {
    let mut set = ClaimSet::default();
    for member in ctx.members(id) {
        for seen in ctx.world.claims.get(member).into_iter().flatten() {
            set.observe(seen.claim.clone(), seen.last_seen);
        }
    }
    set
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
    let record = ctx
        .state
        .agents
        .get(&id)
        .ok_or_else(|| store("unknown agent", id))?;
    let state_kind =
        state_kind(&record.agent.state).ok_or_else(|| store("merged agent as a node", id))?;
    let label = record
        .label
        .as_ref()
        .map(|label| AgentLabel::new(label.as_str()))
        .transpose()
        .map_err(|e| store("label", e))?;
    Ok(AgentNode {
        id,
        label,
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

/// One channel node per distinct id of `channels`, in id order.
pub fn channel_nodes(
    ctx: &Ctx,
    channels: impl IntoIterator<Item = ChannelId>,
) -> Result<Vec<GraphNode>> {
    let ids: BTreeSet<ChannelId> = channels.into_iter().collect();
    ids.into_iter()
        .map(|id| {
            let record = ctx
                .state
                .channels
                .get(&id)
                .ok_or_else(|| store("unknown channel", id))?;
            channel_node(ctx, record).map(GraphNode::Channel)
        })
        .collect()
}

fn channel_node(ctx: &Ctx, record: &ChannelRecord) -> Result<ChannelNode> {
    let channel = &record.channel;
    if record.superseded.is_some() {
        return Err(store("superseded channel as a node", channel.id));
    }
    let origin_kind = CanonicalOriginKind::of(&channel.origin)
        .ok_or_else(|| store("superseded channel as a node", channel.id))?;
    Ok(ChannelNode {
        id: channel.id,
        label: None,
        origin_kind,
        detection_kind: channel.origin.detection_kind(),
        policy_kind: channel.policy.kind(),
        locator_summary: locator_summary(ctx, record)?,
    })
}

/// What a channel covers, as the spec's `locator_summary` words it: the
/// pattern of a channel declared before traffic, otherwise its seed's
/// locator with the count of further resources (`… (+12)`).
fn locator_summary(ctx: &Ctx, record: &ChannelRecord) -> Result<NonBlank> {
    let channel = &record.channel;
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

/// What a channel is named by: a declared channel's pattern, else its seed
/// resource's locator.
pub fn shape(ctx: &Ctx, record: &ChannelRecord) -> ChannelShape {
    let channel = &record.channel;
    match (channel.origin.pattern(), channel.origin.seed()) {
        (Some(pattern), _) => ChannelShape::Pattern(pattern.clone()),
        (None, seed) => {
            let seed = seed.map_or(channel.id.as_ulid(), |seed| seed.resource.as_ulid());
            match ctx
                .world
                .resource(crosstalk_spec::ids::ResourceId::from_ulid(seed))
            {
                Some(resource) => ChannelShape::Seed(resource.locator.clone()),
                None => ChannelShape::Seed(Locator::Opaque {
                    tool: ToolName("unknown".to_owned()),
                    key: format!("{seed:032x}"),
                }),
            }
        }
    }
}
