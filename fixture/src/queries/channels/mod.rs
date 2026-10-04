//! Channel reads as `QueryApi` defines them: rows ([`rows`]), a channel's
//! resources and who used them ([`resources`]), names, policy history and
//! the promotion preview, all over the stored channels as the registry
//! would read them ([`registry`]).

pub mod resources;
pub mod rows;

use std::collections::HashMap;

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::channel::Declaration;
use crosstalk_spec::derived::flow::channel::policy::PolicyHistory;
use crosstalk_spec::derived::flow::channel::promotion::{self, PromotionCoverage, Registered};
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l5_flow::PromoteError;
use crosstalk_spec::interfaces::l8_surface::channels::{ChannelName, resolve_names};

use crate::Result;
use crate::store::State;
use crate::world::World;

use super::Ctx;

/// Every stored channel with its seed's locator, as promotion and name
/// resolution read the registry.
pub fn registry<'a>(world: &'a World, state: &'a State) -> Vec<Registered<'a>> {
    state
        .channels
        .values()
        .map(|record| {
            let channel = record.channel();
            Registered {
                channel,
                seed: channel
                    .origin
                    .seed()
                    .and_then(|seed| world.resource(seed.resource))
                    .map(|resource| &resource.locator),
            }
        })
        .collect()
}

/// Every resource stored on `channel`: its seed resource and
/// `Channel::resources`, whenever seen.
fn held(world: &World, state: &State, channel: ChannelId) -> Vec<Resource> {
    let Some(record) = state.channels.get(&channel) else {
        return Vec::new();
    };
    let stored = record.channel();
    stored
        .origin
        .seed()
        .map(|seed| seed.resource)
        .into_iter()
        .chain(stored.resources.iter().copied())
        .filter_map(|id| world.resource(id).cloned())
        .collect()
}

/// `ChannelRegistry::promotion_coverage`: exactly `promotion::coverage`
/// over the stored channels and the resources stored on them.
pub fn coverage(
    world: &World,
    state: &State,
    channel: ChannelId,
    declaration: &Declaration,
) -> std::result::Result<PromotionCoverage, PromoteError> {
    let registry = registry(world, state);
    promotion::coverage(channel, declaration, &registry, |c| held(world, state, c))
        .map_err(PromoteError::Refused)
}

/// `channel_names`: exactly `channels::resolve_names` over the registry.
pub fn names(ctx: &Ctx, ids: &IdBatch<ChannelId>) -> Result<HashMap<ChannelId, ChannelName>> {
    resolve_names(ids, &registry(ctx.world, ctx.state))
}

/// Every decision recorded for the channel, oldest first; `None` for an
/// unknown channel. A superseded channel answers with its own history.
pub fn policy_history(ctx: &Ctx, channel: ChannelId) -> Option<PolicyHistory> {
    ctx.state
        .channels
        .get(&channel)
        .map(|record| record.history().clone())
}
