//! Channel policy and promotion, with the registry's semantics: a decision
//! is recorded in the channel's policy history and the policy becomes the
//! history's current one; a promotion follows `promotion::plan`, keeping
//! the channel's id and superseding the discovered channels its pattern
//! covers.

use crosstalk_spec::derived::flow::channel::policy::{
    Decision, PolicyAuthor, PolicyDecision, Recorded,
};
use crosstalk_spec::derived::flow::channel::promotion::{self, Promotion};
use crosstalk_spec::derived::flow::resource::ResourcePattern;
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l5_flow::PromoteError;
use crosstalk_spec::interfaces::l8_surface::actions::SupersededChannels;
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, ConflictKind, PolicyKind,
};

use crate::queries::channels::registry;
use crate::store::State;
use crate::world::World;

use super::{Acted, Stamp, effects};

/// Records the operator's decision in the channel's policy history, read
/// through the channel directory first: an unknown channel is `NotFound`
/// and a superseded one `Conflict(ChannelSuperseded)` naming the channel to
/// act on instead. Sanctioning suppresses the active alerts about the
/// channel and the channels it superseded; setting unreviewed records a
/// reset. A decision the history already holds (a redelivery) is
/// `Unchanged`.
pub fn set_policy(
    state: &mut State,
    stamp: Stamp,
    channel: ChannelId,
    kind: PolicyKind,
    note: Option<String>,
) -> Acted {
    let record = state
        .channels
        .get_mut(&channel)
        .ok_or(ActionError::NotFound)?;
    if let Some(supersession) = record.channel().origin.supersession() {
        return Err(ActionError::Conflict(ConflictKind::ChannelSuperseded {
            channel,
            by: supersession.by,
        }));
    }
    let recorded = record.record(PolicyDecision {
        kind,
        decision: Decision {
            by: PolicyAuthor::Operator(stamp.by),
            at: stamp.at,
            note,
        },
    });
    if recorded == Recorded::Duplicate {
        return Ok(ActionOutcome::Unchanged);
    }
    if kind == PolicyKind::Sanctioned {
        effects::suppress_channel_alerts(state, channel, stamp.at);
    }
    Ok(ActionOutcome::Applied)
}

/// Promotes the discovered `channel` as `promotion::plan` decides over the
/// stored channels: its origin becomes promoted (same id, resources and
/// detection), the promotion's decision is recorded in its history, and
/// every channel the plan supersedes becomes superseded by it. A refusal
/// changes nothing and maps as `ActionError::from(PromoteError)`. Returns
/// the channel and the channels it superseded.
pub fn promote(
    world: &World,
    state: &mut State,
    stamp: Stamp,
    channel: ChannelId,
    pattern: &ResourcePattern,
    kind: PolicyKind,
    note: Option<String>,
) -> Acted {
    let promotion = Promotion::new(pattern.clone(), kind, stamp.by, stamp.at, note);
    let planned = promotion::plan(channel, promotion.declaration(), &registry(world, state))
        .map_err(|refusal| ActionError::from(PromoteError::Refused(refusal)))?;
    let superseded = SupersededChannels::new(planned.superseded_ids());
    let record = state
        .channels
        .get_mut(&channel)
        .ok_or(ActionError::NotFound)?;
    record.set_origin(planned.origin);
    record.record(promotion.decision().clone());
    for (id, origin) in planned.superseded {
        if let Some(absorbed) = state.channels.get_mut(&id) {
            absorbed.set_origin(origin);
        }
    }
    if kind == PolicyKind::Sanctioned {
        effects::suppress_channel_alerts(state, channel, stamp.at);
    }
    Ok(ActionOutcome::ChannelPromoted {
        channel,
        superseded,
    })
}
