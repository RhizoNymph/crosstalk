//! Channel policy and promotion (item 16), with the registry's semantics:
//! a decision is recorded in the channel's policy history and the policy
//! becomes the history's current one; a promotion follows
//! `promotion::plan`, keeping the channel's id and superseding the
//! discovered channels its pattern covers.

use crosstalk_spec::derived::flow::channel::policy::{Decision, PolicyAuthor, PolicyDecision};
use crosstalk_spec::derived::flow::channel::promotion::{self, Promotion};
use crosstalk_spec::derived::flow::resource::ResourcePattern;
use crosstalk_spec::ids::{ChannelId, OperatorId};
use crosstalk_spec::interfaces::l5_flow::{PromoteError, RegistryError};
use crosstalk_spec::interfaces::l8_surface::{ActionError, PolicyKind, QueryError};

use crate::backend::Result;
use crate::backend::fixture::clock::NOW;
use crate::backend::fixture::queries::channels::registry;
use crate::backend::fixture::store::State;
use crate::backend::fixture::world::World;
use crate::contract::actions::ActionOutcome;

use super::effects;

/// Records the operator's decision in the channel's policy history.
/// Sanctioning suppresses the active alerts about the channel and the
/// channels it superseded; setting unreviewed records a reset. A
/// superseded channel takes no decisions.
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
    if let Some(supersession) = record.channel().origin.supersession() {
        return Err(RegistryError::Superseded {
            channel,
            by: supersession.by,
        }
        .into());
    }
    record.record(PolicyDecision {
        kind,
        decision: Decision {
            by: PolicyAuthor::Operator(by),
            at: NOW,
            note,
        },
    });
    if kind == PolicyKind::Sanctioned {
        effects::suppress_channel_alerts(state, channel, NOW);
    }
    Ok(ActionOutcome::Applied)
}

/// Promotes the discovered `channel` as `promotion::plan` decides over the
/// stored channels: its origin becomes promoted (same id, resources and
/// detection), the promotion's decision is recorded in its history, and
/// every channel the plan supersedes becomes superseded by it. A refusal
/// changes nothing and maps as `ActionError::from(PromoteError)`.
pub fn promote(
    world: &World,
    state: &mut State,
    by: OperatorId,
    channel: ChannelId,
    pattern: &ResourcePattern,
    kind: PolicyKind,
    note: Option<String>,
) -> Result<ActionOutcome> {
    let promotion = Promotion::new(pattern.clone(), kind, by, NOW, note);
    let planned = promotion::plan(channel, promotion.declaration(), &registry(world, state))
        .map_err(|refusal| QueryError::from(ActionError::from(PromoteError::Refused(refusal))))?;
    let record = state
        .channels
        .get_mut(&channel)
        .ok_or(QueryError::NotFound)?;
    record.set_origin(planned.origin);
    record.record(promotion.decision().clone());
    for (id, origin) in planned.superseded {
        if let Some(absorbed) = state.channels.get_mut(&id) {
            absorbed.set_origin(origin);
        }
    }
    if kind == PolicyKind::Sanctioned {
        effects::suppress_channel_alerts(state, channel, NOW);
    }
    Ok(ActionOutcome::ChannelPromoted(channel))
}
