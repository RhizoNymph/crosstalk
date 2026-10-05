//! Confirmation and listing: what a channel's cross-agent traffic shows,
//! decided when the channel is read.
//!
//! Detection is stored and says when traffic flowed. These are computed at
//! read time from the transmissions routed through the channel (and every
//! channel it superseded), with merged agents resolved
//! ([`Transmission::crossing`]), so they cannot contradict the traffic:
//! there is no stored flag to drift from it, and a merge or an unmerge
//! changes them on the next read.
//!
//! | [`CrossTraffic`] | Origin | [`Listing`] |
//! | --- | --- | --- |
//! | a confirmed crossing transmission | any | `Channel(Confirmed)` |
//! | only unconfirmed ones | any | `Channel(Unconfirmed)` |
//! | none | declared (before traffic, or promoted) | `Declaration` |
//! | none | discovered | `Hidden` |
//! | (not read) | superseded | none: its traffic is its superseding channel's |
//!
//! A crossing transmission counts as confirmed in `Confirmed`, `Classified`
//! and `Aggregated`, and as unconfirmed in `AwaitingContent`, `Suspected`
//! and `Discarded`: opened by a co-access between two agents, with no
//! content match (yet, or ever). `Detected` names no sender and counts as
//! neither. Verdicts sit beside detection and change neither: a channel
//! whose transmissions an operator judged false detections is still listed
//! as it was; views that exclude false detections leave out those
//! transmissions, not the channel.
//!
//! A discovered channel exists only because of a cross-agent transmission,
//! so it has crossing traffic until a merge says otherwise. When every
//! transmission through it resolves within one agent it is
//! [`Listing::Hidden`]: in no channel list, graph or count, while its record
//! and policy history stay and an unmerge lists it again. A declaration is
//! operator intent and is never hidden: without crossing traffic it is
//! listed apart, as declared with no traffic yet, and counts as no channel.

use serde::{Deserialize, Serialize};

use crate::aliases::Aliases;
use crate::derived::flow::channel::ChannelOrigin;
use crate::derived::flow::transmission::{Crossing, Transmission};

/// Whether a channel's cross-agent traffic holds a confirmed transmission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Confirmation {
    /// Every crossing transmission through it is unconfirmed: a co-access
    /// between two agents and no content match. Listed, counted and drawn,
    /// marked as unconfirmed, and left out under
    /// `UnconfirmedChannels::Exclude`.
    Unconfirmed,
    /// At least one crossing transmission through it is confirmed by
    /// content evidence.
    Confirmed,
}

/// The crossing transmissions routed through a channel in force and every
/// channel it superseded, over all time, once merged agents resolve. Built
/// by [`CrossTraffic::tally`]; a channel's [`Confirmation`] and
/// [`Listing`] follow from it, so neither can disagree with the counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct CrossTraffic {
    /// Crossing transmissions in `Confirmed`, `Classified` or `Aggregated`.
    pub confirmed: u64,
    /// Crossing transmissions in `AwaitingContent`, `Suspected` or
    /// `Discarded`.
    pub unconfirmed: u64,
}

impl CrossTraffic {
    /// No crossing transmission.
    pub const NONE: Self = Self {
        confirmed: 0,
        unconfirmed: 0,
    };

    /// The reference count: of `transmissions` (every transmission whose
    /// stored route resolves to the channel), those whose
    /// [`Transmission::crossing`] under `aliases` is `Crosses`, split by
    /// whether their state is confirmed.
    pub fn tally<'a>(
        transmissions: impl IntoIterator<Item = &'a Transmission>,
        aliases: impl Aliases + Copy,
    ) -> Self {
        let mut traffic = Self::NONE;
        for transmission in transmissions {
            if transmission.crossing(aliases) != Crossing::Crosses {
                continue;
            }
            let slot = if transmission.state.confirmed().is_some() {
                &mut traffic.confirmed
            } else {
                &mut traffic.unconfirmed
            };
            *slot = slot.saturating_add(1);
        }
        traffic
    }

    /// `None` without a crossing transmission.
    pub fn confirmation(&self) -> Option<Confirmation> {
        if self.confirmed > 0 {
            Some(Confirmation::Confirmed)
        } else if self.unconfirmed > 0 {
            Some(Confirmation::Unconfirmed)
        } else {
            None
        }
    }
}

/// Where a channel in force is listed, read from its origin and its
/// [`CrossTraffic`] ([`Listing::of`]). Derived at the read, never stored
/// or sent: a row carries its traffic and derives its listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Listing {
    /// Crossing traffic: a channel, listed, counted and drawn, marked by
    /// its confirmation.
    Channel(Confirmation),
    /// Declared (before traffic, or promoted) with no crossing
    /// transmission: listed as a declaration with no traffic yet, never
    /// counted as a channel or drawn.
    Declaration,
    /// Discovered, and every transmission through it now resolves within
    /// one agent: in no list, graph or count until an unmerge.
    Hidden,
}

/// Which listings a channel list keeps. `Hidden` is not one: no list shows
/// a hidden channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListingKind {
    Confirmed,
    Unconfirmed,
    Declaration,
}

impl Listing {
    /// The listing of a channel with `origin` and `traffic`. `None` for a
    /// superseded channel, which is listed only by its supersession: its
    /// traffic belongs to the channel that superseded it.
    pub fn of(origin: &ChannelOrigin, traffic: CrossTraffic) -> Option<Self> {
        let without_traffic = match origin {
            ChannelOrigin::Superseded { .. } => return None,
            ChannelOrigin::Declared { .. } => Self::Declaration,
            ChannelOrigin::Discovered { .. } => Self::Hidden,
        };
        Some(
            traffic
                .confirmation()
                .map_or(without_traffic, Self::Channel),
        )
    }

    /// `None` for a hidden channel.
    pub fn kind(self) -> Option<ListingKind> {
        match self {
            Self::Channel(Confirmation::Confirmed) => Some(ListingKind::Confirmed),
            Self::Channel(Confirmation::Unconfirmed) => Some(ListingKind::Unconfirmed),
            Self::Declaration => Some(ListingKind::Declaration),
            Self::Hidden => None,
        }
    }

    /// The confirmation of a listed channel; `None` for a declaration or a
    /// hidden channel, which carry no crossing traffic.
    pub fn confirmation(self) -> Option<Confirmation> {
        match self {
            Self::Channel(confirmation) => Some(confirmation),
            Self::Declaration | Self::Hidden => None,
        }
    }
}
