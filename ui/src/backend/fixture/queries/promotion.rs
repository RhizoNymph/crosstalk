//! What promoting a discovered channel with a pattern does, computed once
//! for both `promotion_preview` and `PromoteChannel` (item 26), over every
//! known resource.

use crosstalk_spec::derived::flow::channel::ChannelOrigin;
use crosstalk_spec::derived::flow::channel::detection::TrafficDetection;
use crosstalk_spec::derived::flow::resource::ResourcePattern;
use crosstalk_spec::ids::{ChannelId, ResourceId};

use crate::backend::Result;
use crate::backend::fixture::store::State;
use crate::backend::fixture::world::World;
use crate::contract::channels::PromotionPreview;
use crosstalk_spec::interfaces::l8_surface::{ConflictKind, InputError, QueryError};

/// A promotion worked out but not applied.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    /// Why `PromoteChannel` would be refused, if it would be.
    pub conflict: Option<ConflictKind>,
    /// The live discovered channels whose seed the pattern covers: the
    /// promoted channel and every other one it would supersede.
    pub covered: Vec<ChannelId>,
    /// Resources of the covered channels the pattern matches: what the
    /// declared channel holds. Oldest channel first, each resource once.
    pub resources: Vec<ResourceId>,
    /// Resources of the covered channels the pattern does not match. They
    /// stay with their superseded channel, which resolves to the declared
    /// one.
    pub uncovered: Vec<ResourceId>,
    /// The most advanced detection among the covered channels, which the
    /// declared channel starts `InUse` with; the promoted channel's wins a
    /// tie.
    pub detection: Option<TrafficDetection>,
}

fn rank(detection: &TrafficDetection) -> u8 {
    match detection {
        TrafficDetection::Observed { .. } => 0,
        TrafficDetection::Candidate { .. } => 1,
        TrafficDetection::Dormant { .. } => 2,
        TrafficDetection::Active { .. } => 3,
    }
}

/// Works out the promotion of `channel` under `pattern`. Only an unknown
/// channel is an error; the reasons promotion would be refused are in
/// [`Plan::conflict`].
pub fn plan(
    world: &World,
    state: &State,
    channel: ChannelId,
    pattern: &ResourcePattern,
) -> Result<Plan> {
    let record = state.channels.get(&channel).ok_or(QueryError::NotFound)?;
    let covers = |seed: &ResourceId| {
        world
            .resource(*seed)
            .is_some_and(|r| pattern.matches(&r.locator))
    };
    let superseded = |by: ChannelId| ConflictKind::ChannelSuperseded { channel, by };
    let conflict = match &record.channel.origin {
        ChannelOrigin::Declared { .. } => Some(ConflictKind::ChannelNotDiscovered { channel }),
        ChannelOrigin::Discovered { .. } | ChannelOrigin::Superseded { .. }
            if record.superseded.is_some() =>
        {
            record.superseded.map(|s| superseded(s.into))
        }
        ChannelOrigin::Discovered { seed, .. } if !covers(&seed.resource) => {
            return Err(QueryError::InvalidInput(InputError::PatternMissesSeed));
        }
        ChannelOrigin::Discovered { .. } => None,
        ChannelOrigin::Superseded { supersession, .. } => Some(superseded(supersession.by)),
    };
    let mut covered_records: Vec<_> = state
        .channels
        .values()
        .filter(|r| r.superseded.is_none())
        .filter(
            |r| matches!(&r.channel.origin, ChannelOrigin::Discovered { seed, .. } if covers(&seed.resource)),
        )
        .collect();
    covered_records.sort_by_key(|r| (r.created, r.channel.id));

    let mut resources = Vec::new();
    let mut uncovered = Vec::new();
    let mut detection: Option<TrafficDetection> = None;
    for r in &covered_records {
        let ChannelOrigin::Discovered {
            seed,
            detection: own,
            ..
        } = &r.channel.origin
        else {
            continue;
        };
        let better = detection.as_ref().is_none_or(|best| {
            rank(own) > rank(best) || (rank(own) == rank(best) && r.channel.id == channel)
        });
        if better {
            detection = Some(own.clone());
        }
        let grouped = world
            .resource_channel
            .iter()
            .filter(|(_, c)| **c == r.channel.id)
            .map(|(id, _)| *id);
        let mut own_resources: Vec<ResourceId> = std::iter::once(seed.resource)
            .chain(r.channel.resources.iter().copied())
            .chain(grouped)
            .collect();
        own_resources[1..].sort();
        for id in own_resources {
            if resources.contains(&id) || uncovered.contains(&id) {
                continue;
            }
            match world.resource(id) {
                Some(resource) if pattern.matches(&resource.locator) => resources.push(id),
                Some(_) => uncovered.push(id),
                None => {}
            }
        }
    }
    Ok(Plan {
        conflict,
        covered: covered_records.iter().map(|r| r.channel.id).collect(),
        resources,
        uncovered,
        detection,
    })
}

/// The preview of a plan for `channel`: resources resolved, the promoted
/// channel left out of the superseded ones.
pub fn preview(world: &World, plan: Plan, channel: ChannelId) -> PromotionPreview {
    let resolve = |ids: &[ResourceId]| {
        ids.iter()
            .filter_map(|id| world.resource(*id).cloned())
            .collect()
    };
    PromotionPreview {
        covered_resources: resolve(&plan.resources),
        uncovered_resources: resolve(&plan.uncovered),
        superseded_channels: plan
            .covered
            .iter()
            .copied()
            .filter(|c| *c != channel)
            .collect(),
        conflicts: plan.conflict,
    }
}
