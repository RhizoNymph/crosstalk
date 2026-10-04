//! **Temporary.** A stand-in for the spec's channel-semantics change
//! ("channels exist only through cross-agent transmissions"), which the
//! gateway is porting onto its spec separately (invariants INV-850..869).
//! Delete this module when that port lands, and take every type and
//! function below from `crosstalk_spec` instead.
//!
//! The UI was written against that change. The spec it now builds on does
//! not have it yet, so this module holds, inside the UI crate, the parts the
//! pages and the fixture use, in the shape the port gives them:
//!
//! | Here | In the port |
//! | --- | --- |
//! | [`Confirmation`], [`CrossTraffic`], [`Listing`], [`ListingKind`] | `derived::flow::channel::confirmation` |
//! | [`Crossing`], [`crossing`] | `Crossing`, `Transmission::crossing` |
//! | [`UnconfirmedChannels`], [`TopologyFilter`] | `aggregates::filter`: the field `TopologyFilter::unconfirmed_channels`, `admits` refusing a subject within one agent, `AccessSubject::confirmation` |
//! | [`ChannelRow`], [`ChannelStanding`], [`InvalidChannelRow`] | `l8_surface::channels`: `ChannelStanding::InForce { traffic, activity }`, `ChannelRow::{traffic, listing, confirmation}` |
//! | [`ChannelFilter`] | `l8_surface::lists::ChannelFilter` with `listings`, `matches(&ChannelRow)` |
//! | [`QueueCounts`], [`OverviewCounts`] | `l8_surface::overview`: `QueueCounts::unconfirmed_channels`, `tally` over rows and shown alerts |
//! | [`ChannelTransmission`], [`ChannelTransmissionFilter`], [`ChannelTransmissionPage`], [`ChannelTransmissionList`] | `l8_surface::channel_traffic`, `paging::ChannelTransmissionList`, `QueryApi::channel_transmissions` |
//! | [`ChannelGraph`] | `aggregates::node::ChannelNode::confirmation` |
//! | [`alert_shown`] | `AlertSubject::shown` |
//!
//! Where the spec it builds on differs from the port and this module cannot
//! paper over it, the difference is stated where it matters:
//!
//! - `CoAccess` has no writer here, so [`crossing`] and [`CrossTraffic::tally`]
//!   take a `writer` lookup from an access to its agent (the port reads
//!   `CoAccess::writer`).
//! - A discovered channel's `Seed` names its first access here; in the
//!   port it names the first cross-agent transmission and when it opened
//!   (`Seed::{first_transmission, opened_at}`, `ChannelRow::created_at`).
//!   The fixture records the access that opened that transmission.
//! - `TrafficDetection` still has `Observed` and `Candidate` (the port
//!   removes them); the fixture never builds either, and the pages show
//!   them like any other detection.
//! - The port's `ChannelReads::channel` / `channels` return
//!   `ChannelWithTraffic`, and its `ChannelReads::transmissions(channel,
//!   filter, page)` lists a channel's transmissions; the fixture reads its
//!   own world rather than store traits, so nothing here mirrors those.
//!
//! The fixture serves the port-shaped reads as inherent methods of
//! `FixtureBackend` (`backend::fixture::pending`), which take precedence
//! over the `QueryApi` methods of the same names; its `QueryApi`
//! implementation answers the spec's shapes from the same reads. When the
//! port lands, those inherent methods become the `QueryApi`
//! implementation.

mod alerts;
mod confirmation;
mod filter;
mod graph;
mod overview;
mod rows;
mod traffic;

pub use alerts::alert_shown;
pub use confirmation::{Confirmation, CrossTraffic, Crossing, Listing, ListingKind, crossing};
pub use filter::{TopologyFilter, UnconfirmedChannels};
pub use graph::ChannelGraph;
pub use overview::{OverviewCounts, QueueCounts};
pub use rows::{ChannelFilter, ChannelRow, ChannelStanding};
pub use traffic::{
    ChannelTransmission, ChannelTransmissionFilter, ChannelTransmissionList,
    ChannelTransmissionPage,
};
