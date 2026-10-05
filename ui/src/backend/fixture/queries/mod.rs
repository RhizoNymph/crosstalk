//! Reads over the world and the current state.
//!
//! Every query builds a [`Ctx`]: a snapshot of alias and supersession
//! resolution, of the verdicts in force and of each channel's cross-agent
//! traffic (`CrossTraffic::tally` with merges resolved, from which its
//! listing and confirmation follow), so each record is resolved the same
//! way within one response.

pub mod agents;
pub mod alerts;
pub mod channels;
pub mod evidence;
pub mod graph;
pub mod linked;
pub mod lists;
pub mod nodes;
pub mod page;
pub mod projection;
pub mod search;
pub mod series;
pub mod topics;
pub mod transmissions;

use std::collections::{BTreeMap, HashMap};

use crosstalk_spec::derived::flow::channel::confirmation::Confirmation;

use crosstalk_spec::derived::flow::channel::confirmation::CrossTraffic;

use crosstalk_spec::aliases::{Aliases, Resolve};
use crosstalk_spec::derived::flow::channel::confirmation::Listing;
use crosstalk_spec::derived::flow::transmission::Crossing;
use crosstalk_spec::derived::flow::transmission::{Route, Transmission};
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::ids::{AgentId, ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};

use std::num::NonZeroU64;

use crosstalk_spec::aggregates::projection::FrameRetention;
use crosstalk_spec::interfaces::l8_surface::export::ExportFormats;
use crosstalk_spec::interfaces::l8_surface::present::Present;
use crosstalk_spec::support::Similarity;

use crate::backend::Result;
use crosstalk_spec::interfaces::l8_surface::QueryError;

use super::store::State;
use super::world::World;

/// Fails with `Forbidden` unless the caller holds `permission`.
pub fn require(caller: &Caller, permission: Permission) -> Result<()> {
    if caller.has(permission) {
        Ok(())
    } else {
        Err(QueryError::Forbidden {
            missing: permission,
        })
    }
}

/// One query's view of the world.
pub struct Ctx<'a> {
    pub world: &'a World,
    pub state: &'a State,
    agents: HashMap<AgentId, AgentId>,
    channels: HashMap<ChannelId, ChannelId>,
    /// Canonical agent to the agents that resolve to it, itself included.
    members: BTreeMap<AgentId, Vec<AgentId>>,
    verdicts: HashMap<TransmissionId, Verdict>,
    /// Each canonical channel's cross-agent traffic at this read.
    traffic: HashMap<ChannelId, CrossTraffic>,
}

impl<'a> Ctx<'a> {
    pub fn new(world: &'a World, state: &'a State) -> Self {
        let agents: HashMap<AgentId, AgentId> = state
            .identity
            .agents()
            .map(|agent| (agent.id, state.identity.canonical(agent.id)))
            .collect();
        let mut members: BTreeMap<AgentId, Vec<AgentId>> = BTreeMap::new();
        for (id, canonical) in &agents {
            members.entry(*canonical).or_default().push(*id);
        }
        for list in members.values_mut() {
            list.sort();
        }
        let channels = state
            .channels
            .keys()
            .map(|id| (*id, state.canonical_channel(*id)))
            .collect();
        let verdicts = state
            .verdicts
            .iter()
            .filter_map(|(id, log)| log.current().map(|v| (*id, v)))
            .collect();
        let mut ctx = Self {
            world,
            state,
            agents,
            channels,
            members,
            verdicts,
            traffic: HashMap::new(),
        };
        ctx.traffic = ctx.tally_traffic();
        ctx
    }

    /// `CrossTraffic::tally` per canonical channel over the transmissions
    /// whose route resolves to it.
    fn tally_traffic(&self) -> HashMap<ChannelId, CrossTraffic> {
        let mut routed: HashMap<ChannelId, Vec<&Transmission>> = HashMap::new();
        for record in &self.world.transmissions {
            if let Route::Channel(stored) = record.transmission.route {
                routed
                    .entry(self.channel(stored))
                    .or_default()
                    .push(&record.transmission);
            }
        }
        routed
            .into_iter()
            .map(|(channel, transmissions)| {
                let traffic = CrossTraffic::tally(transmissions, self.aliases());
                (channel, traffic)
            })
            .collect()
    }

    /// Whether `transmission` crosses agents at this read.
    pub fn crossing(&self, transmission: &Transmission) -> Crossing {
        transmission.crossing(self.aliases())
    }

    /// The cross-agent traffic of the channel `id` resolves to.
    pub fn traffic(&self, id: ChannelId) -> CrossTraffic {
        self.traffic
            .get(&self.channel(id))
            .copied()
            .unwrap_or(CrossTraffic::NONE)
    }

    /// Where the channel stored under `id` is listed (`Listing::of` its
    /// own origin and the traffic of its canonical channel); `None` for a
    /// superseded or unknown channel.
    pub fn listing(&self, id: ChannelId) -> Option<Listing> {
        let record = self.state.channels.get(&id)?;
        Listing::of(&record.channel().origin, self.traffic(id))
    }

    /// The confirmation of the channel `id` resolves to, when it is listed
    /// as a channel; `None` for a declaration without traffic, a hidden
    /// channel or an unknown one.
    pub fn confirmation(&self, id: ChannelId) -> Option<Confirmation> {
        self.listing(self.channel(id))
            .and_then(Listing::confirmation)
    }

    /// Whether the channel `id` resolves to is hidden: discovered, and
    /// every transmission through it now within one agent.
    pub fn hidden(&self, id: ChannelId) -> bool {
        self.listing(self.channel(id)) == Some(Listing::Hidden)
    }

    pub fn agent(&self, id: AgentId) -> AgentId {
        self.agents.get(&id).copied().unwrap_or(id)
    }

    pub fn channel(&self, id: ChannelId) -> ChannelId {
        self.channels.get(&id).copied().unwrap_or(id)
    }

    /// Merges and supersessions as of this read.
    pub fn aliases(&self) -> impl Aliases + Copy + '_ {
        Resolve {
            agents: move |id| self.agent(id),
            channels: move |id| self.channel(id),
        }
    }

    /// The route with its channel resolved through supersession.
    pub fn route(&self, route: &Route) -> Route {
        match route {
            Route::Channel(id) => Route::Channel(self.channel(*id)),
            other => other.clone(),
        }
    }

    /// The agents that resolve to `canonical`, itself included.
    pub fn members(&self, canonical: AgentId) -> &[AgentId] {
        self.members.get(&canonical).map_or(&[], Vec::as_slice)
    }

    /// Canonical agents, in id order.
    pub fn canonical_agents(&self) -> impl Iterator<Item = AgentId> + '_ {
        self.members.keys().copied()
    }

    /// The verdict in force on a transmission (`VerdictLog::current`).
    pub fn verdict(&self, id: TransmissionId) -> Option<Verdict> {
        self.verdicts.get(&id).copied()
    }
}

/// `present`: the clock, the bucket width and export formats the contract
/// gaps report, the active topic version (what `CreateRule` and
/// `UpdateRule` check), the rule form's default remap threshold and the
/// projection frame retention.
pub fn present(ctx: &Ctx) -> Result<Present> {
    let fail = |what: &str| QueryError::Store {
        reason: format!("fixture present: {what}"),
    };
    let export_formats =
        ExportFormats::new(super::export::FORMATS.to_vec()).map_err(|_| fail("export formats"))?;
    let default_remap_threshold =
        Similarity::new(DEFAULT_REMAP_THRESHOLD).map_err(|_| fail("remap threshold"))?;
    let retention =
        NonZeroU64::new(projection::FRAME_RETENTION).ok_or_else(|| fail("retention"))?;
    Ok(Present {
        now: ctx.state.clock.now(),
        bucket_width: super::clock::BUCKET,
        export_formats,
        current_rule_version: ctx.state.active_version(),
        default_remap_threshold,
        frame_retention_micros: FrameRetention::from_micros(retention),
    })
}

/// The remap threshold of a watched-topic rule created without one; the
/// rule form's default (`pages::alerts::rules::form::DEFAULT_REMAP`).
const DEFAULT_REMAP_THRESHOLD: f32 = 0.8;

/// A stable order for routes, which have no `Ord`.
pub fn route_key(route: &Route) -> (u8, u128, String) {
    use crosstalk_spec::derived::flow::transmission::{DelegationDirection, DirectCarrier};
    match route {
        Route::Channel(id) => (0, id.as_ulid(), String::new()),
        Route::Delegation(DelegationDirection::ParentToChild) => (1, 0, String::new()),
        Route::Delegation(DelegationDirection::ChildToParent) => (1, 1, String::new()),
        Route::Direct(DirectCarrier::UserTurn) => (2, 0, String::new()),
        Route::Direct(DirectCarrier::SystemPrompt) => (2, 1, String::new()),
        Route::Direct(DirectCarrier::ToolResult(name)) => (2, 2, name.0.clone()),
        Route::Unobserved => (3, 0, String::new()),
    }
}
