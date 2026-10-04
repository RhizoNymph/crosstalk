//! Confirmation and listing: what a channel's cross-agent traffic shows,
//! decided when the channel is read. Stand-in for the port's
//! `derived::flow::channel::confirmation` and `Transmission::crossing`.
//!
//! | [`CrossTraffic`] | Origin | [`Listing`] |
//! | --- | --- | --- |
//! | a confirmed crossing transmission | any | `Channel(Confirmed)` |
//! | only unconfirmed ones | any | `Channel(Unconfirmed)` |
//! | none | declared (before traffic, or promoted) | `Declaration` |
//! | none | discovered | `Hidden` |
//! | (not read) | superseded | none: its traffic is its superseding channel's |

use crosstalk_spec::aliases::Aliases;
use crosstalk_spec::derived::flow::channel::ChannelOrigin;
use crosstalk_spec::derived::flow::transmission::{Transmission, TransmissionState};
use crosstalk_spec::ids::{AccessId, AgentId};
use serde::{Deserialize, Serialize};

/// Whether a channel's cross-agent traffic holds a confirmed transmission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Confirmation {
    /// Every crossing transmission through it is unconfirmed: a co-access
    /// between two agents and no content match.
    Unconfirmed,
    /// At least one crossing transmission through it is confirmed by
    /// content evidence.
    Confirmed,
}

/// Whether a transmission is between two different canonical agents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Crossing {
    /// `Detected`: no sender is named yet.
    Unknown,
    Crosses,
    /// Its agents have merged into one since it was recorded.
    WithinOneAgent,
}

/// Whether `transmission` crosses agents under `aliases`: a confirmed one
/// when its sender resolves to another agent than its reader, one backed by
/// co-accesses when any co-access writer (`writer` of the write access)
/// does. Stand-in for the port's `Transmission::crossing`, which reads the
/// writer from `CoAccess::writer`.
pub fn crossing(
    transmission: &Transmission,
    aliases: impl Aliases,
    writer: impl Fn(AccessId) -> Option<AgentId>,
) -> Crossing {
    let reader = aliases.agent(transmission.to);
    let crosses = |from: AgentId| aliases.agent(from) != reader;
    let verdict = |any: bool| {
        if any {
            Crossing::Crosses
        } else {
            Crossing::WithinOneAgent
        }
    };
    match &transmission.state {
        TransmissionState::Detected => Crossing::Unknown,
        TransmissionState::Confirmed(confirmed)
        | TransmissionState::Classified { confirmed, .. }
        | TransmissionState::Aggregated { confirmed, .. } => verdict(crosses(confirmed.from())),
        TransmissionState::AwaitingContent { .. }
        | TransmissionState::Suspected { .. }
        | TransmissionState::Discarded { .. } => verdict(
            transmission
                .state
                .co_accesses()
                .iter()
                .filter_map(|co_access| writer(co_access.write()))
                .any(crosses),
        ),
    }
}

/// The crossing transmissions routed through a channel in force and every
/// channel it superseded, over all time, once merged agents resolve.
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

    /// The reference count: of `transmissions`, those whose [`crossing`]
    /// is `Crosses`, split by whether their state is confirmed.
    pub fn tally<'a>(
        transmissions: impl IntoIterator<Item = &'a Transmission>,
        aliases: impl Aliases + Copy,
        writer: impl Fn(AccessId) -> Option<AgentId>,
    ) -> Self {
        let mut traffic = Self::NONE;
        for transmission in transmissions {
            if crossing(transmission, aliases, &writer) != Crossing::Crosses {
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
/// [`CrossTraffic`] ([`Listing::of`]). Derived at the read, never stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Listing {
    /// Crossing traffic: a channel, listed, counted and drawn, marked by
    /// its confirmation.
    Channel(Confirmation),
    /// Declared with no crossing transmission: listed as a declaration with
    /// no traffic yet, never counted as a channel or drawn.
    Declaration,
    /// Discovered, and every transmission through it now resolves within
    /// one agent: in no list, graph or count until an unmerge.
    Hidden,
}

/// Which listings a channel list keeps. `Hidden` is not one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListingKind {
    Confirmed,
    Unconfirmed,
    Declaration,
}

impl Listing {
    /// The listing of a channel with `origin` and `traffic`. `None` for a
    /// superseded channel.
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
    /// hidden channel.
    pub fn confirmation(self) -> Option<Confirmation> {
        match self {
            Self::Channel(confirmation) => Some(confirmation),
            Self::Declaration | Self::Hidden => None,
        }
    }
}
