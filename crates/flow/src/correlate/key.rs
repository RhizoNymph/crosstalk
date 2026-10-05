//! The medium an access belongs to: the evidence partition and shard key.

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::ids::{ChannelId, ResourceId};
use crosstalk_spec::interfaces::l5_flow::OpensOn;

/// Where an access's evidence is correlated: the canonical channel its
/// resource is on, or the resource itself while it is on no channel.
/// Writes and reads meet only within one medium, so every medium's
/// evidence lives on one shard (`flow.correlator.shard-affinity`), and a
/// resource's evidence moves to its channel's medium when a channel is
/// discovered from it (`flow.correlator.resource-shard-handoff`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MediumKey {
    Channel(ChannelId),
    Resource(ResourceId),
}

impl MediumKey {
    /// The medium of `access`, whose resource is on the canonical channel
    /// `channel` (`None` for a resource on no channel).
    pub fn of(access: &Access, channel: Option<ChannelId>) -> Self {
        channel.map_or(Self::Resource(access.resource), Self::Channel)
    }

    /// Where a co-access in this medium opens its channel transmission.
    pub fn opens_on(self) -> OpensOn {
        match self {
            Self::Channel(channel) => OpensOn::Channel(channel),
            Self::Resource(resource) => OpensOn::Resource(resource),
        }
    }

    /// The channel, for a channel's medium.
    pub fn channel(self) -> Option<ChannelId> {
        match self {
            Self::Channel(channel) => Some(channel),
            Self::Resource(_) => None,
        }
    }
}
