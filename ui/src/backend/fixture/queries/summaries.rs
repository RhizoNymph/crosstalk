//! Rows shared by lists and graphs: agent summaries, channel summaries and
//! channel nodes, always over canonical agents and channels in force.

use std::collections::{BTreeSet, HashMap, HashSet};

use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::channel::ChannelOrigin;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::backend::fixture::store::ChannelRecord;
use crate::contract::agents::{AgentState, AgentStateKind, AgentSummary, ClaimSeen};
use crate::contract::channels::{ChannelSummary, DetectionKind, OriginKind, policy_kind};
use crate::contract::graph::{ChannelNode, ChannelShape};

use super::Ctx;

/// Per canonical agent: transmissions in and out.
pub type Counts = HashMap<AgentId, (u64, u64)>;

/// Transmission counts over the whole world: in counts every transmission
/// read by the agent, out counts confirmed transmissions it sent. Self-edges
/// (after alias resolution) are not counted.
pub fn global_counts(ctx: &Ctx) -> Counts {
    let mut counts = Counts::new();
    for record in &ctx.world.transmissions {
        let to = ctx.agent(record.transmission.to);
        let from = record.from.map(|f| ctx.agent(f));
        if from == Some(to) {
            continue;
        }
        counts.entry(to).or_default().0 += 1;
        if let Some(from) = from {
            counts.entry(from).or_default().1 += 1;
        }
    }
    counts
}

fn state_kind(state: &AgentState) -> AgentStateKind {
    match state {
        AgentState::Registered { .. } => AgentStateKind::Registered,
        AgentState::Provisional { .. } => AgentStateKind::Provisional,
        // A canonical agent is never merged; report its kind defensively.
        AgentState::Established { .. } | AgentState::Merged { .. } => AgentStateKind::Established,
    }
}

fn state_time(state: &AgentState) -> Timestamp {
    match state {
        AgentState::Registered { at } => *at,
        AgentState::Provisional { first_seen } => *first_seen,
        AgentState::Established { since } => *since,
        AgentState::Merged { at, .. } => *at,
    }
}

/// The summary of canonical agent `id`, with its claims and activity
/// gathered from every alias. `None` for an unknown agent.
pub fn agent(ctx: &Ctx, id: AgentId, counts: (u64, u64)) -> Option<AgentSummary> {
    let record = ctx.state.agents.get(&id)?;
    let members = ctx.members(id);
    let mut claims: Vec<ClaimSeen> = Vec::new();
    for member in members {
        for seen in ctx.world.claims.get(member).into_iter().flatten() {
            match claims.iter_mut().find(|c| c.claim == seen.claim) {
                Some(existing) if existing.last_seen < seen.last_seen => {
                    existing.last_seen = seen.last_seen;
                }
                Some(_) => {}
                None => claims.push(seen.clone()),
            }
        }
    }
    claims.sort_by(|a, b| {
        b.last_seen
            .cmp(&a.last_seen)
            .then_with(|| a.claim.user_agent.cmp(&b.claim.user_agent))
    });
    let last_seen = members
        .iter()
        .filter_map(|m| ctx.world.last_activity.get(m))
        .max()
        .copied()
        .unwrap_or_else(|| state_time(&record.agent.state));
    let parent = record
        .agent
        .parent
        .map(|p| ctx.agent(p))
        .filter(|p| *p != id);
    Some(AgentSummary {
        id,
        label: record.label.clone(),
        state: state_kind(&record.agent.state),
        parent,
        claims,
        transmissions_in: counts.0,
        transmissions_out: counts.1,
        last_seen,
    })
}

/// Summaries for `ids` plus every canonical ancestor, in id order.
pub fn agents_with_parents(
    ctx: &Ctx,
    ids: impl IntoIterator<Item = AgentId>,
    counts: &Counts,
) -> Vec<AgentSummary> {
    let mut wanted: BTreeSet<AgentId> = BTreeSet::new();
    for id in ids {
        let mut current = Some(ctx.agent(id));
        // Parent chains are short; the bound guards against a cycle.
        for _ in 0..16 {
            let Some(agent) = current else { break };
            if !wanted.insert(agent) {
                break;
            }
            current = ctx
                .state
                .agents
                .get(&agent)
                .and_then(|r| r.agent.parent)
                .map(|p| ctx.agent(p))
                .filter(|p| *p != agent);
        }
    }
    wanted
        .into_iter()
        .filter_map(|id| agent(ctx, id, counts.get(&id).copied().unwrap_or_default()))
        .collect()
}

pub fn node(ctx: &Ctx, record: &ChannelRecord) -> ChannelNode {
    let channel = &record.channel;
    let shape = match (channel.origin.pattern(), channel.origin.seed()) {
        (Some(pattern), _) => ChannelShape::Pattern(pattern.clone()),
        (None, seed) => {
            let seed = seed.map_or(channel.id.as_ulid(), |seed| seed.resource.as_ulid());
            match ctx
                .world
                .resource(crosstalk_spec::ids::ResourceId::from_ulid(seed))
            {
                Some(resource) => ChannelShape::Seed(resource.locator.clone()),
                None => {
                    ChannelShape::Seed(crosstalk_spec::derived::flow::resource::Locator::Opaque {
                        tool: crosstalk_spec::observed::message::ToolName("unknown".to_owned()),
                        key: format!("{seed:032x}"),
                    })
                }
            }
        }
    };
    ChannelNode {
        id: channel.id,
        origin: OriginKind::of(&channel.origin),
        detection: DetectionKind::of(&channel.origin),
        policy: policy_kind(&channel.policy),
        shape,
    }
}

/// The list row for a channel, counting the traffic of every channel
/// superseded into it; within `window` when one is given. `last_activity`
/// is always the latest overall.
pub fn channel(ctx: &Ctx, record: &ChannelRecord, window: Option<TimeWindow>) -> ChannelSummary {
    let counted = |at| window.is_none_or(|w: TimeWindow| w.contains(at));
    let members: HashSet<ChannelId> = ctx.channel_members(record.channel.id).into_iter().collect();
    let mut writers = HashSet::new();
    let mut readers = HashSet::new();
    let mut last_activity: Option<Timestamp> = None;
    for access in &ctx.world.accesses {
        let on = ctx
            .world
            .resource_channel
            .get(&access.resource)
            .is_some_and(|c| members.contains(c));
        if !on {
            continue;
        }
        last_activity = last_activity.max(Some(access.at));
        if !counted(access.at) {
            continue;
        }
        let agent = ctx.agent(access.agent);
        match access.op.kind() {
            AccessKind::Write => writers.insert(agent),
            AccessKind::Read => readers.insert(agent),
        };
    }
    let transmissions = ctx
        .world
        .transmissions
        .iter()
        .filter(|t| matches!(t.transmission.route, Route::Channel(c) if members.contains(&c)))
        .filter(|t| counted(t.transmission.opened_at))
        .count();
    let seed = match &record.channel.origin {
        ChannelOrigin::Discovered { seed, .. } | ChannelOrigin::Superseded { seed, .. } => {
            ctx.world.resource(seed.resource).cloned()
        }
        ChannelOrigin::Declared { .. } => None,
    };
    ChannelSummary {
        channel: record.channel.clone(),
        seed,
        superseded: record.superseded,
        writers: u32::try_from(writers.len()).unwrap_or(u32::MAX),
        readers: u32::try_from(readers.len()).unwrap_or(u32::MAX),
        transmissions: transmissions as u64,
        last_activity,
    }
}
