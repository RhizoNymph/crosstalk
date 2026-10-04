//! How channels, resources and accesses get into the registry, and the
//! detection updates the flow consumer applies.
//!
//! `ChannelRegistry` creates channels only by declaration. Discovering a
//! channel from a `New` lookup, adding a resource to a channel, recording
//! an access and moving a channel's detection are the flow consumer's
//! steps (P5) with no trait method of their own. These are the store
//! operations behind them, with the registry's own rules enforced: one
//! channel per resource (`flow.registry.one-channel-per-resource`), no new
//! resource on a superseded channel, a superseded channel's detection
//! frozen, and a confirmation advancing the canonical channel's detection
//! (`flow.channel.confirmation-advances-canonical-detection`). The
//! model-based harness drives the registry under test through the same
//! operations ([`SeedChannels`]), and reads its channels back through
//! [`SeedChannels::channels`], the read the surface's channel pages need
//! and the spec leaves without a trait.

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::channel::Channel;
use crosstalk_spec::derived::flow::channel::detection::TrafficDetection;
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::ids::{AccessId, ChannelId, ResourceId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::ChannelLookup;
use crosstalk_spec::support::Timestamp;

/// A detection the flow consumer sets on a channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DetectionUpdate {
    /// The channel's traffic detection becomes this one. For a channel
    /// declared before traffic, its detection becomes `InUse` of it.
    Traffic(TrafficDetection),
    /// A channel declared before traffic saw none by the time its idle
    /// window closed: `AwaitingTraffic` to `Unused { since }`.
    Unused { since: Timestamp },
}

/// Why a seeding operation was refused. Nothing changed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SeedError {
    #[error("channel {0:?} already exists")]
    DuplicateChannel(ChannelId),
    #[error("channel {0:?} is unknown")]
    UnknownChannel(ChannelId),
    #[error("resource {0:?} is already stored on a channel")]
    DuplicateResource(ResourceId),
    #[error("resource {0:?} is unknown")]
    UnknownResource(ResourceId),
    #[error("access {0:?} is already recorded")]
    DuplicateAccess(AccessId),
    /// A discovered channel is created only for a locator whose lookup is
    /// `New`, and a resource joins only the channel its lookup names (or
    /// any channel when it is `New`).
    #[error("the locator's lookup is {0:?}")]
    NotNew(ChannelLookup),
    /// A superseded channel takes no resources and its detection is frozen.
    #[error("channel {channel:?} is superseded by {by:?}")]
    Superseded { channel: ChannelId, by: ChannelId },
    /// `Unused` applies only to a declared channel awaiting traffic.
    #[error("channel {0:?} cannot become unused from its detection")]
    NotAwaitingTraffic(ChannelId),
}

/// The flow consumer's store operations, implemented by every registry the
/// model-based harness checks.
pub trait SeedChannels {
    /// Create the discovered channel `channel` seeded by `resource` and
    /// `first_access`, detection `Observed { first_access }`, policy
    /// `Unreviewed(None)`. Publishes `Changed::Channel` for it.
    fn discover(
        &mut self,
        channel: ChannelId,
        resource: Resource,
        first_access: AccessId,
    ) -> impl Future<Output = Result<(), SeedError>> + Send;

    /// Store `resource` on `channel`: its lookup must be `New` or
    /// `Declared(channel)`. Publishes `Changed::Channel`.
    fn add_resource(
        &mut self,
        channel: ChannelId,
        resource: Resource,
    ) -> impl Future<Output = Result<(), SeedError>> + Send;

    /// Record an access of a stored resource. Announces nothing: accesses
    /// change aggregates, not the channel record.
    fn record_access(
        &mut self,
        access: Access,
    ) -> impl Future<Output = Result<(), SeedError>> + Send;

    /// Set `channel`'s detection. Publishes `Changed::Channel` when it
    /// changed.
    fn set_detection(
        &mut self,
        channel: ChannelId,
        update: DetectionUpdate,
    ) -> impl Future<Output = Result<(), SeedError>> + Send;

    /// Apply the confirmation at `at` of `transmission`, whose stored route
    /// names `channel`, to the detection of the channel `channel` resolves
    /// to: it becomes or stays `Active` with `last_transmission` naming the
    /// transmission (keeping `since` when already active). Publishes
    /// `Changed::Channel` for that channel. Returns it.
    fn confirm(
        &mut self,
        channel: ChannelId,
        transmission: TransmissionId,
        at: Timestamp,
    ) -> impl Future<Output = Result<ChannelId, SeedError>> + Send;

    /// Every stored channel, ascending by id.
    fn channels(&self) -> impl Future<Output = Vec<Channel>> + Send;
}
