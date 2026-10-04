//! The registry's traffic writes and its reads of stored channels.
//!
//! [`ChannelTraffic`] is what the flow consumer records as accesses arrive
//! and transmissions confirm: a channel discovered from a `New` lookup, a
//! resource joining a channel, an access, a detection change, a
//! confirmation. `ChannelRegistry` keeps the declarations, policies and
//! promotions; both write the same registry, so a store implements both.
//! The registry's own rules hold across them: one channel per resource
//! (`flow.registry.one-channel-per-resource`), no new resource on a
//! superseded channel, a superseded channel's detection frozen, and a
//! confirmation advancing the canonical channel's detection
//! (`flow.channel.confirmation-advances-canonical-detection`).
//!
//! [`ChannelReads`] is what the surface reads back: one stored channel by
//! id, and a filtered page of them, for `QueryApi::channel` and
//! `QueryApi::channels`.

use crate::derived::flow::access::Access;
use crate::derived::flow::channel::Channel;
#[cfg(doc)]
use crate::derived::flow::channel::detection::DeclaredDetection;
use crate::derived::flow::channel::detection::TrafficDetection;
use crate::derived::flow::resource::Resource;
use crate::ids::{AccessId, ChannelId, ResourceId, TransmissionId};
use crate::interfaces::l8_surface::lists::ChannelFilter;
use crate::paging::{ChannelList, Page, PageRequest};
use crate::support::{Change, Timestamp};

use super::{ChannelLookup, RegistryError};

/// A detection the flow consumer sets on a channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DetectionUpdate {
    /// The channel's traffic detection becomes this one. For a channel
    /// declared before traffic, its detection becomes
    /// [`DeclaredDetection::InUse`] of it.
    Traffic(TrafficDetection),
    /// A channel declared before traffic saw none by the time its idle
    /// window closed: [`DeclaredDetection::AwaitingTraffic`] to
    /// [`DeclaredDetection::Unused`] since `since`.
    Unused { since: Timestamp },
}

/// The flow consumer's writes to the registry. Each checks before it
/// changes anything: a refusal leaves the registry as it was and publishes
/// nothing.
pub trait ChannelTraffic {
    /// Create the discovered channel `channel`, seeded by `resource` and its
    /// `first_access`: detection `Observed { first_access }`, policy
    /// `Unreviewed(None)`, an empty policy history. Publishes
    /// `Changed::Channel` for it.
    ///
    /// Refuses a taken channel id (`DuplicateChannel`), a stored resource
    /// (`DuplicateResource`) and a resource whose lookup is not `New`
    /// (`NotNew`): a channel is discovered only for a locator nothing
    /// claims.
    fn discover(
        &mut self,
        channel: ChannelId,
        resource: Resource,
        first_access: AccessId,
    ) -> impl Future<Output = Result<(), TrafficError>> + Send;

    /// Store `resource` on `channel`. Publishes `Changed::Channel` for it.
    ///
    /// Refuses an unknown channel, a superseded one (`Superseded`), a stored
    /// resource, and a resource whose lookup is neither `New` nor
    /// `Declared(channel)` (`NotNew`).
    fn add_resource(
        &mut self,
        channel: ChannelId,
        resource: Resource,
    ) -> impl Future<Output = Result<(), TrafficError>> + Send;

    /// Record an access of a stored resource; `resource_use` counts it.
    /// Announces nothing: an access changes aggregates, not the channel
    /// record. Refuses an unknown resource and an access id already
    /// recorded.
    fn record_access(
        &mut self,
        access: Access,
    ) -> impl Future<Output = Result<(), TrafficError>> + Send;

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

    /// Apply the confirmation at `at` of `transmission`, whose stored route
    /// names `channel`, to the detection of the channel `channel` resolves
    /// to (`ChannelDirectory::canonical`): it becomes or stays `Active`
    /// with `last_transmission` naming the transmission, keeping `since`
    /// when it was already active. A superseded `channel` keeps its own
    /// detection frozen. Publishes `Changed::Channel` for the canonical
    /// channel and returns it. Refuses an unknown channel.
    fn confirm(
        &mut self,
        channel: ChannelId,
        transmission: TransmissionId,
        at: Timestamp,
    ) -> impl Future<Output = Result<ChannelId, TrafficError>> + Send;
}

/// Stored channels as the registry holds them: a superseded channel is
/// returned as itself, with its supersession, never as the channel it
/// resolves to.
pub trait ChannelReads {
    /// The channel stored under `id`, `None` for an unknown id.
    fn channel(
        &self,
        id: ChannelId,
    ) -> impl Future<Output = Result<Option<Channel>, RegistryError>> + Send;

    /// The stored channels [`ChannelFilter::matches`] keeps, newest id
    /// first, read in one snapshot. The filter's window changes no row, but
    /// the cursor binds the whole filter: a cursor presented with another
    /// filter, or one this registry did not issue, is `InvalidCursor`.
    fn channels(
        &self,
        filter: &ChannelFilter,
        page: &PageRequest<ChannelList>,
    ) -> impl Future<Output = Result<Page<Channel, ChannelList>, RegistryError>> + Send;
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
    /// A channel is discovered only for a locator whose lookup is `New`,
    /// and a resource joins only the channel its lookup names (any channel
    /// when it is `New`).
    NotNew(ChannelLookup),
    /// A superseded channel takes no resources and its detection is frozen.
    Superseded {
        channel: ChannelId,
        by: ChannelId,
    },
    /// `Unused` applies only to a declared channel awaiting traffic.
    NotAwaitingTraffic(ChannelId),
}
