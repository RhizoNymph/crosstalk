//! Batch name lookups (item 24): canonical agents with their labels, and
//! channels in force with their shapes.

use std::collections::HashMap;

use crosstalk_spec::ids::{AgentId, ChannelId};

use crate::contract::agents::AgentName;
use crate::contract::channels::ChannelName;

use super::Ctx;
use super::nodes;

/// Names of the known agents among `ids`, by the id asked for.
pub fn agents(ctx: &Ctx, ids: &[AgentId]) -> HashMap<AgentId, AgentName> {
    ids.iter()
        .filter(|id| ctx.state.agents.contains_key(id))
        .filter_map(|id| {
            let canonical = ctx.agent(*id);
            let record = ctx.state.agents.get(&canonical)?;
            Some((
                *id,
                AgentName {
                    id: canonical,
                    label: record.label.clone(),
                },
            ))
        })
        .collect()
}

/// Names of the known channels among `ids`, by the id asked for.
pub fn channels(ctx: &Ctx, ids: &[ChannelId]) -> HashMap<ChannelId, ChannelName> {
    ids.iter()
        .filter(|id| ctx.state.channels.contains_key(id))
        .filter_map(|id| {
            let in_force = ctx.channel(*id);
            let record = ctx.state.channels.get(&in_force)?;
            Some((
                *id,
                ChannelName {
                    id: in_force,
                    shape: nodes::shape(ctx, record),
                },
            ))
        })
        .collect()
}
