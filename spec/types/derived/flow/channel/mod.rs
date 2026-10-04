//! Channels: groups of resources that act as one communication medium.
//!
//! A channel has two independent axes:
//! - [`detection`]: what the traffic shows.
//! - [`policy`]: what an operator or config says about it.
//!
//! A channel is declared (matched by a pattern) or discovered from traffic
//! (seeded by its first resource). A declared channel was either declared
//! before any traffic, or discovered and then promoted by an operator, which
//! attached a pattern. Detection follows from that history:
//!
//! | Origin | Detection |
//! | --- | --- |
//! | declared before traffic | [`DeclaredDetection`]: may await traffic or be unused |
//! | promoted | [`TrafficDetection`], carried over unchanged from discovery |
//! | discovered | [`TrafficDetection`] |
//!
//! So "declared but never used" is representable, and "discovered but never
//! accessed" and "promoted but never accessed" are not.
//!
//! **Promotion keeps the channel.** It keeps its id, resources, policy and
//! detection, gains a pattern so future matching resources join it instead
//! of seeding new discovered channels, and records the seed it was
//! discovered from ([`ChannelOrigin::promoted`]).

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
    /// seed. Empty for a channel declared before traffic that has seen none.
    pub resources: Vec<ResourceId>,
    pub policy: Policy,
}

/// The resource and access a discovered channel was created from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Seed {
    pub resource: ResourceId,
    pub first_access: AccessId,
}

/// A pattern and who attached it, when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Declaration {
    pub pattern: ResourcePattern,
    pub by: PolicyAuthor,
    pub at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelOrigin {
    Declared {
        declaration: Declaration,
        history: DeclaredHistory,
    },
    Discovered {
        seed: Seed,
        detection: TrafficDetection,
    },
}

/// How a declared channel came to be declared, with the detection that
/// history allows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeclaredHistory {
    /// Declared in config or by an operator before any traffic.
    BeforeTraffic(DeclaredDetection),
    /// Discovered from traffic, then promoted. Its detection continues from
    /// discovery.
    Promoted {
        from: Seed,
        detection: TrafficDetection,
    },
}

/// Promotion of a channel that is already declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlreadyDeclared;

impl ChannelOrigin {
    /// The origin after promoting this discovered channel with
    /// `declaration`: declared, promoted from its seed, detection unchanged.
    pub fn promoted(&self, declaration: Declaration) -> Result<Self, AlreadyDeclared> {
        match self {
            Self::Discovered { seed, detection } => Ok(Self::Declared {
                declaration,
                history: DeclaredHistory::Promoted {
                    from: *seed,
                    detection: detection.clone(),
                },
            }),
            Self::Declared { .. } => Err(AlreadyDeclared),
        }
    }

    /// The pattern new resources are matched against, if declared.
    pub fn pattern(&self) -> Option<&ResourcePattern> {
        match self {
            Self::Declared { declaration, .. } => Some(&declaration.pattern),
            Self::Discovered { .. } => None,
        }
    }

    /// The traffic detection, once the channel has traffic.
    pub fn traffic(&self) -> Option<&TrafficDetection> {
        match self {
            Self::Declared {
                history: DeclaredHistory::BeforeTraffic(DeclaredDetection::InUse(detection)),
                ..
            }
            | Self::Declared {
                history: DeclaredHistory::Promoted { detection, .. },
                ..
            }
            | Self::Discovered { detection, .. } => Some(detection),
            Self::Declared {
                history:
                    DeclaredHistory::BeforeTraffic(
                        DeclaredDetection::AwaitingTraffic | DeclaredDetection::Unused { .. },
                    ),
                ..
            } => None,
        }
    }
}
