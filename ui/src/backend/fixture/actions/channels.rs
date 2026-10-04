//! Channel policy and promotion (item 16).

use crosstalk_spec::derived::flow::channel::detection::{DeclaredDetection, TrafficDetection};
use crosstalk_spec::derived::flow::channel::policy::{Decision, Policy, PolicyAuthor};
use crosstalk_spec::derived::flow::channel::{Channel, ChannelOrigin};
use crosstalk_spec::derived::flow::resource::ResourcePattern;
use crosstalk_spec::ids::{ChannelId, OperatorId, ResourceId};
use crosstalk_spec::interfaces::l8_surface::PolicyKind;

use crate::backend::Result;
use crate::backend::fixture::clock::NOW;
use crate::backend::fixture::store::{ChannelRecord, State};
use crate::backend::fixture::world::World;
use crate::contract::actions::ActionOutcome;
use crate::contract::channels::Supersession;
use crate::contract::errors::{ConflictKind, QueryError};

use super::effects;

fn policy(kind: PolicyKind, by: OperatorId, note: Option<String>) -> Policy {
    let decision = Decision {
        by: PolicyAuthor::Operator(by),
        at: NOW,
        note,
    };
    match kind {
        PolicyKind::Unreviewed => Policy::Unreviewed(Some(decision)),
        PolicyKind::Sanctioned => Policy::Sanctioned(decision),
        PolicyKind::Unsanctioned => Policy::Unsanctioned(decision),
    }
}

/// Sets a channel's policy. Sanctioning suppresses the channel's active
/// alerts; setting unreviewed records a reset.
pub fn set_policy(
    state: &mut State,
    by: OperatorId,
    channel: ChannelId,
    kind: PolicyKind,
    note: Option<String>,
) -> Result<ActionOutcome> {
    let record = state
        .channels
        .get_mut(&channel)
        .ok_or(QueryError::NotFound)?;
    if record.superseded.is_some() {
        return Err(QueryError::Conflict(ConflictKind::ChannelSuperseded));
    }
    record.channel.policy = policy(kind, by, note);
    if kind == PolicyKind::Sanctioned {
        effects::suppress_channel_alerts(state, channel, NOW);
    }
    Ok(ActionOutcome::Applied)
}

fn rank(detection: &TrafficDetection) -> u8 {
    match detection {
        TrafficDetection::Observed { .. } => 0,
        TrafficDetection::Candidate { .. } => 1,
        TrafficDetection::Dormant { .. } => 2,
        TrafficDetection::Active { .. } => 3,
    }
}

/// Creates a declared channel from a discovered one. It supersedes the
/// channel and every other discovered, not yet superseded channel whose
/// seed the pattern covers, takes over their resources, and starts `InUse`
/// with the most advanced detection among them.
pub fn promote(
    world: &World,
    state: &mut State,
    by: OperatorId,
    channel: ChannelId,
    pattern: &ResourcePattern,
    kind: PolicyKind,
    note: Option<String>,
) -> Result<ActionOutcome> {
    let record = state.channels.get(&channel).ok_or(QueryError::NotFound)?;
    let ChannelOrigin::Discovered { seed, .. } = &record.channel.origin else {
        return Err(QueryError::Conflict(ConflictKind::ChannelNotDiscovered));
    };
    if record.superseded.is_some() {
        return Err(QueryError::Conflict(ConflictKind::ChannelSuperseded));
    }
    let covers = |seed: &ResourceId| {
        world
            .resource(*seed)
            .is_some_and(|r| pattern.matches(&r.locator))
    };
    if !covers(seed) {
        return Err(QueryError::Conflict(ConflictKind::PatternMissesSeed));
    }

    let covered: Vec<&ChannelRecord> = state
        .channels
        .values()
        .filter(|r| r.superseded.is_none())
        .filter(
            |r| matches!(&r.channel.origin, ChannelOrigin::Discovered { seed, .. } if covers(seed)),
        )
        .collect();
    let mut resources = Vec::new();
    let mut best: Option<TrafficDetection> = None;
    for r in &covered {
        if let ChannelOrigin::Discovered {
            seed, detection, ..
        } = &r.channel.origin
        {
            resources.push(*seed);
            let better = best.as_ref().is_none_or(|b| {
                rank(detection) > rank(b) || (rank(detection) == rank(b) && r.channel.id == channel)
            });
            if better {
                best = Some(detection.clone());
            }
        }
        resources.extend(r.channel.resources.iter().copied());
    }
    let covered: Vec<ChannelId> = covered.iter().map(|r| r.channel.id).collect();
    let detection = best.ok_or_else(|| QueryError::Store {
        reason: "promoted channel has no detection".to_owned(),
    })?;

    let id = ChannelId::from_ulid(state.mint.ulid(NOW));
    state.channels.insert(
        id,
        ChannelRecord {
            channel: Channel {
                id,
                origin: ChannelOrigin::Declared {
                    pattern: pattern.clone(),
                    by: PolicyAuthor::Operator(by),
                    at: NOW,
                    detection: DeclaredDetection::InUse(detection),
                },
                resources,
                policy: policy(kind, by, note),
            },
            superseded: None,
            created: NOW,
        },
    );
    for old in covered {
        if let Some(r) = state.channels.get_mut(&old) {
            r.superseded = Some(Supersession {
                into: id,
                by,
                at: NOW,
            });
        }
    }
    if kind == PolicyKind::Sanctioned {
        effects::suppress_channel_alerts(state, id, NOW);
    }
    Ok(ActionOutcome::ChannelPromoted(id))
}
