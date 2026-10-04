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
//! **Promotion keeps the channel.** It keeps its id, resources and
//! detection, gains a pattern so future matching resources join it instead
//! of seeding new discovered channels, records the seed it was discovered
//! from ([`ChannelOrigin::promoted`]), and records the operator's policy
//! decision in its policy history ([`promotion::Promotion`]).
//!
//! **Promotion supersedes the channels it absorbs.** Every other discovered
//! channel whose seed the new pattern matches becomes
//! [`ChannelOrigin::Superseded`] by the promoted channel
//! ([`ChannelOrigin::superseded`], [`promotion::plan`]). A superseded
//! channel keeps its id, seed, resources, policy history and the detection
//! it had, accepts no new resources (lookups of its resources return the
//! promoted channel), and cannot be promoted or have its policy set.
//! Records naming it (routes, accesses, edges, alerts) keep its id and
//! resolve to the promoted channel at read time, like a merged agent
//! ([`crate::aliases`], `ChannelDirectory`).
//!
//! ```text
//! Discovered ─promote─▶ Declared(Promoted)
//!     │
//!     └─ another channel's promotion matches its seed ─▶ Superseded { by, at }
//! ```
//!
//! A superseding channel is always a promoted one, which is declared and so
//! never superseded itself: resolving a superseded id takes one step.

pub mod detection;
pub mod policy;
pub mod promotion;

use crate::derived::flow::resource::ResourcePattern;
use crate::ids::{AccessId, ChannelId, ResourceId};
use crate::support::Timestamp;

use detection::{DeclaredDetection, DetectionKind, TrafficDetection};
use policy::{Policy, PolicyAuthor};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Channel {
    pub id: ChannelId,
    pub origin: ChannelOrigin,
    /// Resources recorded on this channel itself, beyond a discovered
    /// channel's seed. Empty for a channel declared before traffic that has
    /// seen none. A promoted channel's resources at read time also include
    /// those of the channels it superseded; they stay stored under those.
    pub resources: Vec<ResourceId>,
    pub policy: Policy,
}

impl Channel {
    /// The channel this one resolves to: the promoted channel that
    /// superseded it, else itself. What `ChannelDirectory::canonical`
    /// returns for this channel's id.
    pub fn canonical(&self) -> ChannelId {
        self.origin
            .supersession()
            .map_or(self.id, |supersession| supersession.by)
    }
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
    /// A discovered channel whose seed another channel's promotion pattern
    /// matched. Its detection is the one it had when superseded; new traffic
    /// on its resources is recorded on the superseding channel.
    Superseded {
        seed: Seed,
        detection: TrafficDetection,
        supersession: Supersession,
    },
}

/// Which promoted channel superseded a discovered one, and when: the
/// promotion's declaration time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Supersession {
    pub by: ChannelId,
    pub at: Timestamp,
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

/// Why an origin cannot be promoted: only a discovered channel can.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotPromotable {
    /// The channel is already declared (before traffic, or promoted).
    AlreadyDeclared,
    /// The channel was superseded; act on the channel that superseded it.
    Superseded(Supersession),
}

/// Promotion of a channel that is already declared.
pub use NotPromotable::AlreadyDeclared;

/// Why an origin cannot be superseded: only a discovered channel can.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotSupersedable {
    /// A declared channel owns its pattern; promotion refuses a pattern
    /// that overlaps it instead.
    Declared,
    AlreadySuperseded(Supersession),
}

impl ChannelOrigin {
    /// The origin after promoting this discovered channel with
    /// `declaration`: declared, promoted from its seed, detection unchanged.
    pub fn promoted(&self, declaration: Declaration) -> Result<Self, NotPromotable> {
        match self {
            Self::Discovered { seed, detection } => Ok(Self::Declared {
                declaration,
                history: DeclaredHistory::Promoted {
                    from: *seed,
                    detection: detection.clone(),
                },
            }),
            Self::Declared { .. } => Err(NotPromotable::AlreadyDeclared),
            Self::Superseded { supersession, .. } => Err(NotPromotable::Superseded(*supersession)),
        }
    }

    /// The origin after another channel's promotion absorbed this
    /// discovered channel: superseded, seed and detection unchanged.
    pub fn superseded(&self, supersession: Supersession) -> Result<Self, NotSupersedable> {
        match self {
            Self::Discovered { seed, detection } => Ok(Self::Superseded {
                seed: *seed,
                detection: detection.clone(),
                supersession,
            }),
            Self::Declared { .. } => Err(NotSupersedable::Declared),
            Self::Superseded { supersession, .. } => {
                Err(NotSupersedable::AlreadySuperseded(*supersession))
            }
        }
    }

    /// Who superseded this channel and when, if anyone did.
    pub fn supersession(&self) -> Option<Supersession> {
        match self {
            Self::Superseded { supersession, .. } => Some(*supersession),
            Self::Declared { .. } | Self::Discovered { .. } => None,
        }
    }

    /// The resource and access the channel was discovered from. `None` only
    /// for a channel declared before traffic.
    pub fn seed(&self) -> Option<Seed> {
        match self {
            Self::Declared {
                history: DeclaredHistory::Promoted { from, .. },
                ..
            } => Some(*from),
            Self::Discovered { seed, .. } | Self::Superseded { seed, .. } => Some(*seed),
            Self::Declared {
                history: DeclaredHistory::BeforeTraffic(_),
                ..
            } => None,
        }
    }

    /// The pattern new resources are matched against, if declared.
    pub fn pattern(&self) -> Option<&ResourcePattern> {
        match self {
            Self::Declared { declaration, .. } => Some(&declaration.pattern),
            Self::Discovered { .. } | Self::Superseded { .. } => None,
        }
    }

    /// The channel's detection state, without its data.
    pub fn detection_kind(&self) -> DetectionKind {
        match self {
            Self::Declared {
                history: DeclaredHistory::BeforeTraffic(detection),
                ..
            } => DetectionKind::of_declared(detection),
            Self::Declared {
                history: DeclaredHistory::Promoted { detection, .. },
                ..
            }
            | Self::Discovered { detection, .. }
            | Self::Superseded { detection, .. } => DetectionKind::of_traffic(detection),
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
            | Self::Discovered { detection, .. }
            | Self::Superseded { detection, .. } => Some(detection),
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
