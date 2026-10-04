//! What L3 knows about one canonical agent, read at query time: its parent
//! in canonical form, its claims and last-seen time over its cluster, and
//! its checked `AgentProfile`. Graph nodes read the same parent and claims
//! ([`super::super::nodes`]), so a node and a row always agree.

use std::collections::HashMap;

use crosstalk_spec::aggregates::agents::{AgentProfile, AgentProfileParts, AgentTraffic};
use crosstalk_spec::aggregates::edge::{TopologyFilter, Weighting};
use crosstalk_spec::aggregates::node::GraphNode;
use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::observed::agent::ClaimSet;
use crosstalk_spec::support::TimeWindow;

use crate::backend::Result;

use super::super::{Ctx, graph};

fn store(what: &str, detail: impl std::fmt::Debug) -> QueryError {
    QueryError::Store {
        reason: format!("fixture agents: {what}: {detail:?}"),
    }
}

/// The canonical form of `id`'s stored parent, never `id` itself.
pub fn parent(ctx: &Ctx, id: AgentId) -> Option<AgentId> {
    ctx.state
        .identity
        .agent(id)
        .and_then(|agent| agent.parent)
        .map(|parent| ctx.agent(parent))
        .filter(|parent| *parent != id)
}

/// `ClaimSet::union` of the claims recorded for canonical `id` and every
/// agent resolving to it.
pub fn claims(ctx: &Ctx, id: AgentId) -> ClaimSet {
    ClaimSet::union(
        ctx.members(id)
            .iter()
            .filter_map(|member| ctx.world.claims.get(member)),
    )
}

/// The profile of canonical agent `id`; `None` for an unknown or merged
/// one, which has no row.
pub fn profile(ctx: &Ctx, id: AgentId) -> Result<Option<AgentProfile>> {
    let Some(agent) = ctx.state.identity.agent(id) else {
        return Ok(None);
    };
    let Ok(state) = agent.state.active() else {
        return Ok(None);
    };
    let members = ctx.members(id);
    let parts = AgentProfileParts {
        id,
        label: agent.label.clone(),
        state,
        parent: parent(ctx, id),
        aliases: members.iter().copied().filter(|m| *m != id).collect(),
        claims: claims(ctx, id),
        last_seen: members
            .iter()
            .filter_map(|m| ctx.world.last_activity.get(m))
            .max()
            .copied(),
    };
    AgentProfile::new(parts)
        .map(Some)
        .map_err(|e| store("profile", (id, e)))
}

/// `EdgeStore::agent_traffic`: each agent's node counts in the topology for
/// `window` under the default filter. Agents without a node are absent
/// (they count zero). Fails like `topology` on an unaligned window.
pub fn traffic(ctx: &Ctx, window: TimeWindow) -> Result<HashMap<AgentId, AgentTraffic>> {
    let graph = graph::graph(
        ctx,
        window,
        Weighting::Transmissions,
        &TopologyFilter::default(),
    )?;
    Ok(graph
        .nodes
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
        .collect())
}
