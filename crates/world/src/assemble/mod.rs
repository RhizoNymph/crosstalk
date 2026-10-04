//! Assembly: the generated world as a seed script, every write the
//! pipeline, the surface and config made over the week, each at its time.
//!
//! Each part adds its steps; [`crate::script::Script::into_steps`] orders
//! them by time. Parts are added in dependency order, so among steps at the
//! same instant a fit's activation precedes a job that fails on it, and a
//! channel's discovery precedes the access that discovered it.

mod agents;
mod alerts;
mod channels;
mod config;
mod surface;
mod transmissions;

use std::collections::BTreeMap;

use crosstalk_spec::ids::{ChannelId, ProjectionId};
use crosstalk_spec::support::Timestamp;

use crate::clock::Anchor;
use crate::config::WorldConfig;
use crate::embed::WorldEmbedder;
use crate::error::WorldError;
use crate::generate::Generated;
use crate::scenario::{ChannelKey, JobKey};
use crate::script::{Script, Step};

pub use channels::promotion;

/// The seed script and the ids it mints for roles.
#[derive(Debug)]
pub struct Assembled {
    pub steps: Vec<Step>,
    pub jobs: BTreeMap<JobKey, ProjectionId>,
}

/// Everything assembly reads.
pub struct Inputs<'a> {
    pub generated: &'a Generated,
    pub config: &'a WorldConfig,
    pub declared: &'a BTreeMap<ChannelKey, ChannelId>,
    pub embedder: &'a WorldEmbedder,
    pub anchor: Anchor,
}

pub fn assemble(inputs: Inputs<'_>) -> Result<Assembled, WorldError> {
    let Inputs {
        generated,
        config,
        declared,
        embedder,
        anchor,
    } = inputs;
    let mut script = Script::default();
    config::assemble(generated, config, declared, &mut script)?;
    agents::assemble(generated, &mut script)?;
    channels::assemble(generated, &mut script)?;
    transmissions::assemble(generated, embedder, &mut script)?;
    alerts::assemble(generated, config, &discovered_at(generated), &mut script)?;
    surface::bodies(generated, &mut script);
    let jobs = surface::projections(generated, config, anchor, &mut script)?;
    surface::letters(generated, anchor, &mut script)?;
    Ok(Assembled {
        steps: script.into_steps(),
        jobs,
    })
}

/// When each channel saw its first access: a discovered channel's
/// creation.
pub fn discovered_at(generated: &Generated) -> BTreeMap<ChannelId, Timestamp> {
    let mut out = BTreeMap::new();
    for access in &generated.traffic.accesses {
        if let Some(channel) = generated.traffic.resource_channel.get(&access.resource) {
            out.entry(*channel).or_insert(access.at);
        }
    }
    out
}
