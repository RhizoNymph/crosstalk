//! The registry's traffic writes (`ChannelTraffic`) and the reads that
//! carry a channel's cross-agent traffic (`ChannelReads`), as functions of
//! [`ChannelTable`]. Each write checks before it changes anything.
//!
//! A channel's traffic is read, never stored: [`CrossTraffic::tally`] over
//! the recorded transmissions whose route resolves to it, with agents
//! resolved by the caller's directory at the read.

use std::cmp::Reverse;

use crosstalk_spec::aliases::Resolve;
use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::channel::confirmation::{Confirmation, CrossTraffic};
use crosstalk_spec::derived::flow::channel::detection::TrafficDetection;
use crosstalk_spec::derived::flow::channel::policy::{Policy, PolicyHistory};
use crosstalk_spec::derived::flow::channel::{Channel, ChannelOrigin, Seed};
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::derived::flow::transmission::{
    Crossing, Route, Transmission, TransmissionState,
};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::ids::{AgentId, ChannelId, ResourceId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::channels::{
    ChannelWithTraffic, DetectionUpdate, TrafficError,
};
use crosstalk_spec::interfaces::l5_flow::{ChannelLookup, Discovery, RegistryError};
use crosstalk_spec::interfaces::l8_surface::channel_traffic::ChannelTransmissionFilter;
use crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter;
use crosstalk_spec::support::{Change, Timestamp};

use super::table::{ChannelTable, StoredResource, changed, next_origin};

/// Where `ChannelReads::channels` resumes: after this channel in
/// (`created_at`, id) descending order.
pub(crate) type ChannelKey = (Timestamp, ChannelId);

/// Where `ChannelReads::transmissions` resumes: after this transmission in
/// (`opened_at`, id) descending order.
pub(crate) type TransmissionKey = (Timestamp, TransmissionId);

/// Whether a transmission's evidence is confirmed by content.
fn confirmation_of(state: &TransmissionState) -> Confirmation {
    if state.confirmed().is_some() {
        Confirmation::Confirmed
    } else {
        Confirmation::Unconfirmed
    }
}

/// When a recorded state advances its channel's detection: opened by a
/// co-access (`AwaitingContent`, at `opened_at`) or confirmed by content
/// (at `Confirmed::at`). Suspected, discarded and detected states do not.
fn advances_at(transmission: &Transmission) -> Option<Timestamp> {
    match &transmission.state {
        TransmissionState::AwaitingContent { .. } => Some(transmission.opened_at),
        TransmissionState::Confirmed(confirmed)
        | TransmissionState::Classified { confirmed, .. }
        | TransmissionState::Aggregated { confirmed, .. } => Some(confirmed.at()),
        TransmissionState::Detected
        | TransmissionState::Suspected { .. }
        | TransmissionState::Discarded { .. } => None,
    }
}

impl ChannelTable {
    /// Agents through `agent`, channels through this table's supersessions.
    fn aliases<'a>(
        &'a self,
        agent: &'a impl Fn(AgentId) -> AgentId,
    ) -> Resolve<&'a dyn Fn(AgentId) -> AgentId, impl Fn(ChannelId) -> ChannelId + Copy + 'a> {
        Resolve {
            agents: agent as &dyn Fn(AgentId) -> AgentId,
            channels: move |id| self.canonical(id),
        }
    }

    /// The recorded transmissions whose route resolves to `canonical`.
    fn routed_to(&self, canonical: ChannelId) -> impl Iterator<Item = &Transmission> {
        self.transmissions.values().filter(move |transmission| {
            matches!(transmission.route, Route::Channel(channel) if self.canonical(channel) == canonical)
        })
    }

    /// The cross-agent traffic of `id`'s canonical channel, agents resolved
    /// through `agent`.
    pub(crate) fn cross_traffic(
        &self,
        id: ChannelId,
        agent: &impl Fn(AgentId) -> AgentId,
    ) -> CrossTraffic {
        CrossTraffic::tally(self.routed_to(self.canonical(id)), self.aliases(agent))
    }

    /// `ChannelReads::channel`.
    pub(crate) fn read_channel(
        &self,
        id: ChannelId,
        agent: &impl Fn(AgentId) -> AgentId,
    ) -> Option<ChannelWithTraffic> {
        let channel = self.channels.get(&id)?;
        Some(ChannelWithTraffic::new(
            channel.clone(),
            self.cross_traffic(id, agent),
        ))
    }

    /// `ChannelReads::channels`, before paging: the channels `filter`
    /// keeps with their traffic, newest created first (ties by id), after
    /// `after`.
    pub(crate) fn channels_matching(
        &self,
        filter: &ChannelFilter,
        after: Option<ChannelKey>,
        agent: &impl Fn(AgentId) -> AgentId,
    ) -> Vec<ChannelWithTraffic> {
        let mut rows: Vec<ChannelWithTraffic> = self
            .channels
            .values()
            .filter(|channel| after.is_none_or(|after| key(channel) < after))
            .map(|channel| {
                ChannelWithTraffic::new(channel.clone(), self.cross_traffic(channel.id, agent))
            })
            .filter(|read| filter.keeps(read))
            .collect();
        rows.sort_by_key(|read| Reverse(key(read.channel())));
        rows
    }

    /// `ChannelReads::transmissions`, before paging: the crossing
    /// transmissions routed through `id`'s canonical channel that `filter`
    /// keeps, newest opened first (ties by id), after `after`; with the
    /// canonical channel the cursor binds.
    pub(crate) fn channel_transmissions(
        &self,
        id: ChannelId,
        filter: &ChannelTransmissionFilter,
        after: Option<TransmissionKey>,
        agent: &impl Fn(AgentId) -> AgentId,
    ) -> Result<(ChannelId, Vec<Transmission>), RegistryError> {
        self.channel(id)?;
        let canonical = self.canonical(id);
        let aliases = self.aliases(agent);
        let mut rows: Vec<Transmission> = self
            .routed_to(canonical)
            .filter(|transmission| {
                after.is_none_or(|after| (transmission.opened_at, transmission.id) < after)
            })
            .filter(|transmission| transmission.crossing(aliases) == Crossing::Crosses)
            .filter(|transmission| {
                filter
                    .confirmation
                    .is_none_or(|wanted| confirmation_of(&transmission.state) == wanted)
            })
            .cloned()
            .collect();
        rows.sort_by_key(|transmission| Reverse((transmission.opened_at, transmission.id)));
        Ok((canonical, rows))
    }

    // ---- `ChannelTraffic` ----------------------------------------------------

    /// Store `resource` on `channel`: on no channel for `None`, else on a
    /// declared channel, recorded among its resources.
    fn place(&mut self, resource: Resource, channel: Option<ChannelId>) {
        let id = resource.id;
        self.resources
            .insert(id, StoredResource { resource, channel });
        if let Some(channel) = channel
            && let Some(stored) = self.channels.get_mut(&channel)
            && !stored.resources.contains(&id)
        {
            stored.resources.push(id);
        }
    }

    pub(crate) fn add_resource(
        &mut self,
        resource: Resource,
    ) -> Result<(Option<ChannelId>, Vec<BusEvent>), TrafficError> {
        let lookup = self.lookup(&resource.locator);
        match self.resources.get(&resource.id) {
            // Stored on no channel: it joins a declared channel whose
            // pattern now matches it, and is otherwise already where it
            // belongs.
            Some(StoredResource { channel: None, .. }) => match lookup {
                ChannelLookup::Declared(channel) => {
                    let stored = self
                        .resources
                        .get(&resource.id)
                        .map(|stored| stored.resource.clone());
                    if let Some(stored) = stored {
                        self.place(stored, Some(channel));
                    }
                    Ok((Some(channel), vec![changed(channel)]))
                }
                ChannelLookup::NoChannel | ChannelLookup::Known(_) => {
                    Err(TrafficError::DuplicateResource(resource.id))
                }
            },
            Some(StoredResource {
                channel: Some(_), ..
            }) => Err(TrafficError::DuplicateResource(resource.id)),
            None => {
                if let Some(existing) = self
                    .resources
                    .values()
                    .find(|stored| stored.resource.locator == resource.locator)
                {
                    return Err(TrafficError::DuplicateLocator {
                        existing: existing.resource.id,
                        lookup,
                    });
                }
                match lookup {
                    ChannelLookup::Declared(channel) => {
                        self.place(resource, Some(channel));
                        Ok((Some(channel), vec![changed(channel)]))
                    }
                    ChannelLookup::NoChannel => {
                        self.place(resource, None);
                        Ok((None, Vec::new()))
                    }
                    // A resource on a channel has this locator, and the
                    // locator check above found it.
                    ChannelLookup::Known(_) => Err(TrafficError::Store {
                        reason: "a known locator without a stored resource".to_owned(),
                    }),
                }
            }
        }
    }

    pub(crate) fn record_access(&mut self, access: Access) -> Result<(), TrafficError> {
        if !self.resources.contains_key(&access.resource) {
            return Err(TrafficError::UnknownResource(access.resource));
        }
        if self.accesses.contains_key(&access.id) {
            return Err(TrafficError::DuplicateAccess(access.id));
        }
        self.accesses.insert(access.id, access);
        Ok(())
    }

    pub(crate) fn discover(
        &mut self,
        id: ChannelId,
        resource: ResourceId,
        transmission: TransmissionId,
        at: Timestamp,
    ) -> Result<(Discovery, Vec<BusEvent>), TrafficError> {
        let stored = self
            .resources
            .get(&resource)
            .ok_or(TrafficError::UnknownResource(resource))?;
        if let Some(channel) = stored.channel {
            return Ok((Discovery::Existing(self.canonical(channel)), Vec::new()));
        }
        match self.lookup(&stored.resource.locator) {
            ChannelLookup::Declared(channel) => {
                let stored = stored.resource.clone();
                self.place(stored, Some(channel));
                return Ok((Discovery::Existing(channel), vec![changed(channel)]));
            }
            // A resource on no channel is never `Known`: no other stored
            // resource has its locator.
            ChannelLookup::NoChannel | ChannelLookup::Known(_) => {}
        }
        if self.channels.contains_key(&id) {
            return Err(TrafficError::DuplicateChannel(id));
        }
        let seed = Seed {
            resource,
            first_transmission: transmission,
            opened_at: at,
        };
        let stored = stored.resource.clone();
        self.channels.insert(
            id,
            Channel {
                id,
                origin: ChannelOrigin::Discovered {
                    seed,
                    detection: TrafficDetection::Active {
                        since: at,
                        last_transmission: transmission,
                    },
                },
                resources: Vec::new(),
                policy: Policy::Unreviewed(None),
            },
        );
        self.histories.insert(id, PolicyHistory::empty());
        // The seed is held through the origin, not among `resources`.
        self.resources.insert(
            resource,
            StoredResource {
                resource: stored,
                channel: Some(id),
            },
        );
        let events = vec![
            BusEvent::Detect(DetectEvent::ChannelDiscovered { channel: id, seed }),
            changed(id),
        ];
        Ok((Discovery::Created(id), events))
    }

    pub(crate) fn record_transmission(
        &mut self,
        transmission: &Transmission,
    ) -> Result<(Change, Vec<BusEvent>), TrafficError> {
        let Route::Channel(routed) = transmission.route else {
            return Err(TrafficError::NotChannelRouted(transmission.id));
        };
        if !self.channels.contains_key(&routed) {
            return Err(TrafficError::UnknownChannel(routed));
        }
        let canonical = self.canonical(routed);
        let origin = match advances_at(transmission) {
            None => None,
            Some(at) => {
                let channel = self
                    .channels
                    .get(&canonical)
                    .ok_or(TrafficError::UnknownChannel(canonical))?;
                let since = match channel.origin.traffic() {
                    Some(TrafficDetection::Active { since, .. }) => *since,
                    Some(TrafficDetection::Dormant { .. }) | None => at,
                };
                let detection = TrafficDetection::Active {
                    since,
                    last_transmission: transmission.id,
                };
                let next = next_origin(
                    &channel.origin,
                    canonical,
                    DetectionUpdate::Traffic(detection),
                )?;
                (next != channel.origin).then_some(next)
            }
        };
        let recorded = self.transmissions.get(&transmission.id) != Some(transmission);
        if origin.is_none() && !recorded {
            return Ok((Change::Unchanged, Vec::new()));
        }
        if let Some(origin) = origin
            && let Some(channel) = self.channels.get_mut(&canonical)
        {
            channel.origin = origin;
        }
        self.transmissions
            .insert(transmission.id, transmission.clone());
        Ok((Change::Applied, vec![changed(canonical)]))
    }

    pub(crate) fn set_detection(
        &mut self,
        id: ChannelId,
        update: DetectionUpdate,
    ) -> Result<(Change, Vec<BusEvent>), TrafficError> {
        let channel = self
            .channels
            .get(&id)
            .ok_or(TrafficError::UnknownChannel(id))?;
        let origin = next_origin(&channel.origin, id, update)?;
        if channel.origin == origin {
            return Ok((Change::Unchanged, Vec::new()));
        }
        if let Some(channel) = self.channels.get_mut(&id) {
            channel.origin = origin;
        }
        Ok((Change::Applied, vec![changed(id)]))
    }
}

/// A channel's position in the channel list.
fn key(channel: &Channel) -> ChannelKey {
    (channel.origin.created_at(), channel.id)
}
