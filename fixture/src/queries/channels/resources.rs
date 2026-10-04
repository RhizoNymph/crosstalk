//! A channel's resources and who used them in a window
//! (`ChannelRegistry::resource_use`): the resources of the channel's
//! canonical channel (its own and those of every channel it superseded)
//! accessed in the window, newest resource first, with canonical writers
//! and readers and how often each accessed it.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;

use crosstalk_spec::aggregates::access::{AgentAccesses, ResourceUse, ResourceUsePage};
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::ids::{AgentId, ChannelId, ResourceId};
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::paging::{PageRequest, ResourceUseList};
use crosstalk_spec::support::TimeWindow;

use crate::Result;

use super::super::Ctx;
use super::super::graph::{store_error, watermarked};
use super::super::page;

/// Accesses per canonical agent, writers then readers.
type Uses = (HashMap<AgentId, u64>, HashMap<AgentId, u64>);

fn entries(counts: HashMap<AgentId, u64>) -> Result<Vec<AgentAccesses>> {
    counts
        .into_iter()
        .map(|(agent, n)| {
            Ok(AgentAccesses {
                agent,
                accesses: NonZeroU64::new(n)
                    .ok_or_else(|| store_error("an agent without accesses", agent))?,
            })
        })
        .collect()
}

/// Every resource of `canonical` (a channel in force) and of the channels
/// it superseded that was accessed in `window`, newest resource first.
pub fn uses(ctx: &Ctx, canonical: ChannelId, window: TimeWindow) -> Result<Vec<ResourceUse>> {
    let mut used: BTreeMap<ResourceId, Uses> = BTreeMap::new();
    for access in &ctx.world.accesses {
        if !window.contains(access.at) {
            continue;
        }
        let on = ctx
            .world
            .resource_channel
            .get(&access.resource)
            .is_some_and(|stored| ctx.channel(*stored) == canonical);
        if !on {
            continue;
        }
        let (writers, readers) = used.entry(access.resource).or_default();
        let slot = match access.op.kind() {
            AccessKind::Write => writers,
            AccessKind::Read => readers,
        };
        *slot.entry(ctx.agent(access.agent)).or_default() += 1;
    }
    used.into_iter()
        .rev()
        .map(|(id, (writers, readers))| {
            let resource = ctx
                .world
                .resource(id)
                .ok_or_else(|| store_error("unknown resource", id))?
                .clone();
            ResourceUse::new(resource, entries(writers)?, entries(readers)?)
                .map_err(|e| store_error("resource use", e))
        })
        .collect()
}

/// `channel_resources`: one page of [`uses`] of the channel `channel`
/// resolves to, the cursor bound to that channel and the window. Unknown
/// is `NotFound`.
pub fn page(
    ctx: &Ctx,
    channel: ChannelId,
    window: TimeWindow,
    request: &PageRequest<ResourceUseList>,
) -> Result<Watermarked<ResourceUsePage>> {
    if !ctx.state.channels.contains_key(&channel) {
        return Err(QueryError::NotFound);
    }
    let canonical = ctx.channel(channel);
    let items = uses(ctx, canonical, window)?
        .into_iter()
        .map(|resource_use| {
            let id = resource_use.resource().id.as_ulid();
            ((0, u128::MAX - id), resource_use)
        })
        .collect();
    let listed = page::paginate(
        "resources",
        page::digest(&(canonical, window)),
        items,
        request,
    )?;
    Ok(watermarked(ResourceUsePage {
        channel: canonical,
        window,
        page: listed,
    }))
}
