//! Channel policy and promotion (item 16).

use crosstalk_spec::derived::flow::channel::detection::DeclaredDetection;
use crosstalk_spec::derived::flow::channel::policy::{Decision, Policy, PolicyAuthor};
use crosstalk_spec::derived::flow::channel::{Channel, ChannelOrigin};
use crosstalk_spec::derived::flow::resource::ResourcePattern;
use crosstalk_spec::ids::{ChannelId, OperatorId};
use crosstalk_spec::interfaces::l8_surface::PolicyKind;

use crate::backend::Result;
use crate::backend::fixture::clock::NOW;
use crate::backend::fixture::queries::promotion::plan;
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

/// Creates a declared channel from a discovered one, as worked out by
/// [`plan`]: it supersedes the channel and every other live discovered
/// channel whose seed the pattern covers, holds their resources the pattern
/// matches, and starts `InUse` with the most advanced detection among them.
pub fn promote(
    world: &World,
    state: &mut State,
    by: OperatorId,
    channel: ChannelId,
    pattern: &ResourcePattern,
    kind: PolicyKind,
    note: Option<String>,
) -> Result<ActionOutcome> {
    let plan = plan(world, state, channel, pattern)?;
    if let Some(conflict) = plan.conflict {
        return Err(QueryError::Conflict(conflict));
    }
    let detection = plan.detection.ok_or_else(|| QueryError::Store {
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
                resources: plan.resources,
                policy: policy(kind, by, note),
            },
            superseded: None,
            created: NOW,
        },
    );
    for old in plan.covered {
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
