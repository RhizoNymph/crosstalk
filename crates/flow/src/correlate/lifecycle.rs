//! The transmission lifecycle as the correlator drives it: which update
//! may follow which, so states only move forward.
//!
//! ```text
//! (none) ─OpenChannel─▶ Awaiting ─Suspect─▶ Suspected ─Discard─▶ Discarded (final)
//!                          │                    │
//!                          └──Confirm──▶ Confirmed ◀──Confirm──┘
//! (none) ─OpenConfirmed─▶ Confirmed ─Extend─▶ Confirmed
//! ```
//!
//! [`advance`] is the automaton; the correlator's output for every
//! transmission is a path through it (`flow.transmission.confirm-once`,
//! `flow.transmission.suspect-once`, `flow.transmission.discarded-final`),
//! and the flow consumer refuses to apply an update the stored state does
//! not admit, so a redelivery never moves a stored transmission back.

use crosstalk_spec::derived::flow::transmission::TransmissionState;
use crosstalk_spec::interfaces::l5_flow::TransmissionUpdate;

/// Where a transmission is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stage {
    Awaiting,
    Suspected,
    Confirmed,
    Discarded,
}

impl Stage {
    /// Progress along the lifecycle: never lower after an update.
    pub fn rank(self) -> u8 {
        match self {
            Self::Awaiting => 0,
            Self::Suspected => 1,
            Self::Confirmed | Self::Discarded => 2,
        }
    }

    /// The stage of a stored state; `None` for `Detected`, which the
    /// correlator never stores.
    pub fn of(state: &TransmissionState) -> Option<Self> {
        match state {
            TransmissionState::Detected => None,
            TransmissionState::AwaitingContent { .. } => Some(Self::Awaiting),
            TransmissionState::Suspected { .. } => Some(Self::Suspected),
            TransmissionState::Confirmed(_)
            | TransmissionState::Classified { .. }
            | TransmissionState::Aggregated { .. } => Some(Self::Confirmed),
            TransmissionState::Discarded { .. } => Some(Self::Discarded),
        }
    }
}

/// An update the current stage does not admit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Illegal {
    pub from: Option<Stage>,
    pub update: UpdateKind,
}

/// The kind of a `TransmissionUpdate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UpdateKind {
    OpenChannel,
    OpenConfirmed,
    Extend,
    Confirm,
    Suspect,
    Discard,
}

impl UpdateKind {
    pub fn of(update: &TransmissionUpdate) -> Self {
        match update {
            TransmissionUpdate::OpenChannel { .. } => Self::OpenChannel,
            TransmissionUpdate::OpenConfirmed { .. } => Self::OpenConfirmed,
            TransmissionUpdate::Extend { .. } => Self::Extend,
            TransmissionUpdate::Confirm { .. } => Self::Confirm,
            TransmissionUpdate::Suspect { .. } => Self::Suspect,
            TransmissionUpdate::Discard { .. } => Self::Discard,
        }
    }
}

/// The stage after `update` from `from` (`None`: not opened yet).
pub fn advance(from: Option<Stage>, update: UpdateKind) -> Result<Stage, Illegal> {
    use Stage::{Awaiting, Confirmed, Discarded, Suspected};
    match (from, update) {
        (None, UpdateKind::OpenChannel) => Ok(Awaiting),
        (None, UpdateKind::OpenConfirmed) => Ok(Confirmed),
        (Some(Awaiting), UpdateKind::Suspect) => Ok(Suspected),
        (Some(Awaiting | Suspected), UpdateKind::Confirm) => Ok(Confirmed),
        (Some(Suspected), UpdateKind::Discard) => Ok(Discarded),
        (Some(Confirmed), UpdateKind::Extend) => Ok(Confirmed),
        _ => Err(Illegal { from, update }),
    }
}
