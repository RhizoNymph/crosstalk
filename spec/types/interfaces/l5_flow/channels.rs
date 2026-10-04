//! The registry's traffic writes and its reads of stored channels.
//!
//! [`ChannelTraffic`] is what the flow consumer records as accesses arrive
//! and transmissions open and confirm: a resource (on the channel its
//! lookup names, or on none), an access, a channel discovered by a
//! cross-agent transmission, the state of each channel transmission, a
//! detection change. `ChannelRegistry` keeps the declarations, policies and
//! promotions; both write the same registry, so a store implements both.
//! The registry's own rules hold across them: at most one channel per
//! resource (`flow.registry.at-most-one-channel-per-resource`), a channel
//! only once a cross-agent transmission goes through it
//! (`flow.channel.resource-only-until-cross-agent`), no new resource on a
//! superseded channel, a superseded channel's detection frozen, and a
//! channel transmission advancing the canonical channel's detection
//! (`flow.channel.confirmation-advances-canonical-detection`).
//!
//! [`ChannelReads`] is what the surface reads back: one stored channel by
//! id and a filtered page of them, each with its cross-agent traffic at the
//! read ([`ChannelWithTraffic`]), for `QueryApi::channel` and
//! `QueryApi::channels`; the cross-agent transmissions routed through a
//! channel, for `QueryApi::channel_transmissions`.
//!
//! **Traffic is read, not stored.** The registry keeps the state of every
//! transmission routed through a channel as the flow consumer records it
//! ([`ChannelTraffic::record_transmission`]); a channel's
//! [`CrossTraffic`] is [`CrossTraffic::tally`] over those routed through it
//! and every channel it superseded, with agents resolved through
//! `AgentDirectory` at the read, so a merge or an unmerge changes it on the
//! next read and nothing stored is rewritten.

use crate::derived::flow::access::Access;
use crate::derived::flow::channel::Channel;
use crate::derived::flow::channel::confirmation::{CrossTraffic, Listing};
#[cfg(doc)]
use crate::derived::flow::channel::detection::DeclaredDetection;
use crate::derived::flow::channel::detection::TrafficDetection;
use crate::derived::flow::resource::Resource;
use crate::derived::flow::transmission::Transmission;
use crate::ids::{AccessId, ChannelId, ResourceId, TransmissionId};
use crate::interfaces::l8_surface::channel_traffic::ChannelTransmissionFilter;
use crate::interfaces::l8_surface::lists::ChannelFilter;
use crate::paging::{ChannelList, ChannelTransmissionList, Page, PageRequest};
use crate::support::{Change, Timestamp};

use super::{ChannelLookup, Discovery, RegistryError};

/// A detection the flow consumer sets on a channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DetectionUpdate {
    /// The channel's traffic detection becomes this one (a channel turning
    /// dormant). For a channel declared before traffic, its detection
    /// becomes [`DeclaredDetection::InUse`] of it.
    Traffic(TrafficDetection),
    /// A channel declared before traffic saw no cross-agent transmission by
    /// the time its idle window closed: [`DeclaredDetection::AwaitingTraffic`]
    /// to [`DeclaredDetection::Unused`] since `since`.
    Unused { since: Timestamp },
}

/// The flow consumer's writes to the registry. Each checks before it
/// changes anything: a refusal leaves the registry as it was and publishes
/// nothing.
pub trait ChannelTraffic {
    /// Store `resource`, seen for the first time, where its lookup puts it:
    /// on `c` for `Declared(c)` (publishing `Changed::Channel` for `c`),
    /// on no channel for `NoChannel` (publishing nothing). A resource
    /// already stored on no channel whose lookup is now `Declared(c)` (a
    /// declaration added since) joins `c` the same way. Returns the channel
    /// it is on, `None` for none.
    ///
    /// Refuses a resource already on a channel, or on none while its lookup
    /// is still `NoChannel` (`DuplicateResource`), and a resource whose
    /// locator another stored resource has (`DuplicateLocator`).
    fn add_resource(
        &mut self,
        resource: Resource,
    ) -> impl Future<Output = Result<Option<ChannelId>, TrafficError>> + Send;

    /// Record an access of a stored resource, on a channel or not;
    /// `resource_use` counts it once the resource is on one. Announces
    /// nothing: an access changes aggregates, not the channel record.
    /// Refuses an unknown resource and an access id already recorded.
    fn record_access(
        &mut self,
        access: Access,
    ) -> impl Future<Output = Result<(), TrafficError>> + Send;

    /// Discover a channel from the stored `resource` for `transmission`, the
    /// cross-agent transmission a co-access opened on it at `at`
    /// ([`OpensOn::Resource`](super::OpensOn::Resource)), under the id
    /// `channel` the caller minted. In one transaction:
    ///
    /// - the resource on no channel and its lookup `NoChannel`: create the
    ///   discovered channel `channel` with `Seed { resource,
    ///   first_transmission: transmission, opened_at: at }`, detection
    ///   `Active { since: at, last_transmission: transmission }`, policy
    ///   `Unreviewed(None)` and an empty policy history, move the resource
    ///   onto it, publish `ChannelDiscovered` and `Changed::Channel`, and
    ///   return `Created(channel)`;
    /// - the resource on no channel and its lookup `Declared(c)`: the
    ///   resource joins `c` as [`ChannelTraffic::add_resource`] would, and
    ///   the result is `Existing(c)`;
    /// - the resource already on a channel (a concurrent discovery
    ///   committed first): change nothing and return `Existing` with its
    ///   canonical channel.
    ///
    /// So a resource is on at most one channel and one discovery publishes
    /// one `ChannelDiscovered`. The caller then stores the transmission
    /// routed through the returned channel and records it
    /// ([`ChannelTraffic::record_transmission`]).
    ///
    /// Refuses an unknown resource and, when it would create, a taken
    /// channel id (`DuplicateChannel`).
    fn discover(
        &mut self,
        channel: ChannelId,
        resource: ResourceId,
        transmission: TransmissionId,
        at: Timestamp,
    ) -> impl Future<Output = Result<Discovery, TrafficError>> + Send;

    /// Record `transmission`'s current state as traffic of the channel its
    /// stored route names, replacing the state recorded for its id: the
    /// flow consumer records each state the correlator decides for a
    /// channel transmission (open, extend, confirm, suspect, discard), once
    /// it is stored. When the state is opened (`AwaitingContent`, a
    /// co-access between two agents) or confirmed (`Confirmed`, or a later
    /// state carrying it), the
    /// detection of the channel the route resolves to
    /// (`ChannelDirectory::canonical`) becomes or stays `Active` with
    /// `last_transmission` naming it, keeping `since` when it was already
    /// active and otherwise since `opened_at` (opened) or `Confirmed::at`
    /// (confirmed); a superseded channel's own detection stays frozen.
    ///
    /// `Applied`, publishing `Changed::Channel` for the canonical channel
    /// (its traffic, and so its confirmation and listing, may have
    /// changed), when anything changed; `Unchanged`, publishing nothing, for
    /// a state already recorded. Refuses a transmission not routed through
    /// a channel (`NotChannelRouted`) and an unknown channel.
    fn record_transmission(
        &mut self,
        transmission: &Transmission,
    ) -> impl Future<Output = Result<Change, TrafficError>> + Send;

    /// Set `channel`'s detection. `Applied`, publishing `Changed::Channel`,
    /// when it changed; `Unchanged`, publishing nothing, when the channel
    /// already had it.
    ///
    /// Refuses an unknown channel, a superseded one (its detection is
    /// frozen), and `Unused` on a channel that is not a declared channel
    /// awaiting traffic (`NotAwaitingTraffic`).
    fn set_detection(
        &mut self,
        channel: ChannelId,
        update: DetectionUpdate,
    ) -> impl Future<Output = Result<Change, TrafficError>> + Send;
}

/// A stored channel and, when it is in force, its cross-agent traffic at
/// the read. A superseded channel carries none: its traffic is its
/// superseding channel's. Built by [`ChannelWithTraffic::new`], so the two
/// cannot disagree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelWithTraffic {
    channel: Channel,
    traffic: Option<CrossTraffic>,
}

impl ChannelWithTraffic {
    /// `traffic` is the tally over the transmissions routed through
    /// `channel` and every channel it superseded; it is dropped for a
    /// superseded channel.
    pub fn new(channel: Channel, traffic: CrossTraffic) -> Self {
        let traffic = channel.origin.supersession().is_none().then_some(traffic);
        Self { channel, traffic }
    }

    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    /// `None` for a superseded channel.
    pub fn traffic(&self) -> Option<CrossTraffic> {
        self.traffic
    }

    /// [`Listing::of`] the channel's origin and traffic; `None` when
    /// superseded.
    pub fn listing(&self) -> Option<Listing> {
        self.traffic
            .and_then(|traffic| Listing::of(&self.channel.origin, traffic))
    }

    pub fn into_parts(self) -> (Channel, Option<CrossTraffic>) {
        (self.channel, self.traffic)
    }
}

/// Stored channels as the registry holds them: a superseded channel is
/// returned as itself, with its supersession, never as the channel it
/// resolves to.
pub trait ChannelReads {
    /// The channel stored under `id` with its traffic, `None` for an
    /// unknown id. A hidden channel is returned too.
    fn channel(
        &self,
        id: ChannelId,
    ) -> impl Future<Output = Result<Option<ChannelWithTraffic>, RegistryError>> + Send;

    /// The stored channels [`ChannelFilter::keeps`] keeps with their
    /// listing (never a hidden one), newest first: by
    /// [`ChannelOrigin::created_at`] descending, ties by id descending,
    /// read in one snapshot. The filter's window changes no row, but the
    /// cursor binds the whole filter: a cursor presented with another
    /// filter, or one this registry did not issue, is `InvalidCursor`.
    ///
    /// [`ChannelOrigin::created_at`]: crate::derived::flow::channel::ChannelOrigin::created_at
    fn channels(
        &self,
        filter: &ChannelFilter,
        page: &PageRequest<ChannelList>,
    ) -> impl Future<Output = Result<Page<ChannelWithTraffic, ChannelList>, RegistryError>> + Send;

    /// The transmissions routed through `channel`'s canonical channel and
    /// every channel it superseded that cross agents at the read
    /// ([`Transmission::crossing`] is `Crosses`, agents resolved through
    /// `AgentDirectory`) and that `filter` keeps, newest opened first (ties
    /// by id), each in the state last recorded
    /// ([`ChannelTraffic::record_transmission`]). The cursor binds the
    /// canonical channel and the filter. `UnknownChannel` for an unknown
    /// id.
    fn transmissions(
        &self,
        channel: ChannelId,
        filter: &ChannelTransmissionFilter,
        page: &PageRequest<ChannelTransmissionList>,
    ) -> impl Future<Output = Result<Page<Transmission, ChannelTransmissionList>, RegistryError>> + Send;
}

/// Why a traffic write was refused. Nothing changed. Consumer-side only:
/// no surface query or action makes these writes, so none maps to a
/// `QueryError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrafficError {
    Store {
        reason: String,
    },
    DuplicateChannel(ChannelId),
    UnknownChannel(ChannelId),
    DuplicateResource(ResourceId),
    UnknownResource(ResourceId),
    DuplicateAccess(AccessId),
    /// Another stored resource has this locator, on this lookup.
    DuplicateLocator {
        existing: ResourceId,
        lookup: ChannelLookup,
    },
    /// A superseded channel takes no resources and its detection is frozen.
    Superseded {
        channel: ChannelId,
        by: ChannelId,
    },
    /// `Unused` applies only to a declared channel awaiting traffic.
    NotAwaitingTraffic(ChannelId),
    /// Only a transmission whose route is `Route::Channel` is a channel's
    /// traffic.
    NotChannelRouted(TransmissionId),
}
