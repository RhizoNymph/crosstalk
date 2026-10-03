//! Channels: groups of resources that act as one communication medium.
//!
//! A channel has two independent axes:
//! - [`detection`]: what the traffic shows.
//! - [`policy`]: what an operator or config says about it.
//!
//! A channel is either declared in config before any traffic (matched by a
//! pattern) or discovered from traffic (seeded by its first resource). The
//! two origins have different detection states, so "declared but never
//! used" is representable and "discovered but never accessed" is not.

pub mod detection;
pub mod policy;

use crate::derived::flow::resource::ResourcePattern;
use crate::ids::{AccessId, ChannelId, ResourceId};
use crate::support::Timestamp;

use detection::{DeclaredDetection, TrafficDetection};
use policy::{Policy, PolicyAuthor};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Channel {
    pub id: ChannelId,
    pub origin: ChannelOrigin,
    /// Resources seen on this channel so far, beyond a discovered channel's
    /// seed. Empty for a declared channel that has seen no traffic.
    pub resources: Vec<ResourceId>,
    pub policy: Policy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelOrigin {
    Declared {
        pattern: ResourcePattern,
        by: PolicyAuthor,
        at: Timestamp,
        detection: DeclaredDetection,
    },
    Discovered {
        seed: ResourceId,
        first_access: AccessId,
        detection: TrafficDetection,
    },
}
