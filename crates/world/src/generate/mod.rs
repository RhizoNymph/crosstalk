//! Generation: the world's data, built from a seed with no store in sight.
//!
//! Every value is built through the spec's checked constructors, so
//! generation exercises their invariants; a refusal is a
//! [`WorldError::Invalid`]. Declared channels are the one input from a
//! store: their ids are the ones the registry assigned when config declared
//! them, before any traffic.

pub mod agents;
pub mod bodies;
pub mod channels;
pub mod drafts;
pub mod evidence;
pub mod retention;
pub mod rules;
pub mod states;
pub mod times;
pub mod topics;
pub mod traffic;

use std::collections::BTreeMap;
use std::sync::Arc;

use crosstalk_spec::ids::{ChannelId, TransmissionId};

use crate::clock::{Anchor, WorldClock};
use crate::config::WorldConfig;
use crate::error::WorldError;
use crate::mint::Mint;
use crate::scenario::{BodySide, ChannelKey};

use agents::Cast;
use channels::ChannelPlan;
use times::Times;
use topics::TopicModel;
use traffic::Traffic;

/// Everything generation produced.
#[derive(Debug, Clone)]
pub struct Generated {
    pub seed: u64,
    pub times: Times,
    pub cast: Cast,
    pub plan: ChannelPlan,
    pub topics: TopicModel,
    pub traffic: Traffic,
    /// Old transmissions whose sender's or reader's bodies are dropped.
    pub dropped: Vec<(TransmissionId, BodySide)>,
}

/// Generates the world of `seed` at `anchor`, its declared channels under
/// `declared`'s ids.
pub fn generate(
    seed: u64,
    anchor: Anchor,
    config: &WorldConfig,
    declared: &BTreeMap<ChannelKey, ChannelId>,
) -> Result<Generated, WorldError> {
    let times = Times::of(anchor);
    let mut mint = Mint::new(seed, "ids", Arc::new(WorldClock::Fixed(anchor)));
    let cast = agents::build(seed, anchor, &mut mint)?;
    let plan = channels::plan(drafts::drafts(&times), declared, &mut mint, &cast)?;
    let topics = topics::build(seed, &times, config.embedding.clone(), &mut mint)?;
    let mut traffic = traffic::generate(traffic::Inputs {
        seed,
        mint: &mut mint,
        times: &times,
        cast: &cast,
        plan: &plan,
        topics: &topics,
    })?;
    let dropped = retention::drop_old_bodies(&times, &traffic.transmissions, &mut traffic.blobs);
    Ok(Generated {
        seed,
        times,
        cast,
        plan,
        topics,
        traffic,
        dropped,
    })
}
