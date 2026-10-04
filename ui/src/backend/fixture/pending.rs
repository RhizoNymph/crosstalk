//! **Temporary.** The fixture's port-shaped channel reads, part of the
//! channel-semantics stand-in (`crate::pending::channel_semantics`).
//!
//! These are inherent methods named like the `QueryApi` methods they stand
//! in for, so a call such as `backend.channels(..)` resolves to them (an
//! inherent method takes precedence over a trait method) and returns the
//! port's shapes: rows with their cross-agent traffic, the channel-centred
//! graph with each node's confirmation, the overview's unconfirmed-channel
//! queue, and a channel's transmissions. The `QueryApi` implementation
//! (`surface`) answers the spec's shapes from the same reads.
//!
//! When the gateway's port of the channel-semantics spec lands, these
//! become the `QueryApi` implementation and this file is deleted.

use crosstalk_spec::aggregates::edge::Weighting;
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use crosstalk_spec::paging::{ChannelList, Page, PageRequest};
use crosstalk_spec::support::TimeWindow;

use crate::backend::Result;
use crate::pending::channel_semantics::{
    ChannelFilter, ChannelGraph, ChannelRow, ChannelTransmissionFilter, ChannelTransmissionList,
    ChannelTransmissionPage, OverviewCounts, TopologyFilter,
};

use super::FixtureBackend;
use super::queries::{self, require};

impl FixtureBackend {
    /// `QueryApi::channel` with the row's cross-agent traffic.
    pub async fn channel(
        &self,
        caller: &Caller,
        id: ChannelId,
        window: Option<TimeWindow>,
    ) -> Result<Option<Watermarked<ChannelRow>>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::channels::rows::one(ctx, id, window))
            .await
    }

    /// `QueryApi::channels` with listings and the rows' traffic.
    pub async fn channels(
        &self,
        caller: &Caller,
        filter: &ChannelFilter,
        page: &PageRequest<ChannelList>,
    ) -> Result<Watermarked<Page<ChannelRow, ChannelList>>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::channels::rows::list(ctx, filter, page))
            .await
    }

    /// The port's `QueryApi::channel_transmissions`.
    pub async fn channel_transmissions(
        &self,
        caller: &Caller,
        channel: ChannelId,
        filter: &ChannelTransmissionFilter,
        version: TopicVersionSelector,
        page: &PageRequest<ChannelTransmissionList>,
    ) -> Result<ChannelTransmissionPage> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::channels::transmissions::page(ctx, channel, filter, version, page))
            .await
    }

    /// `QueryApi::channel_topology` under the filter's
    /// `unconfirmed_channels`, with each channel node's confirmation.
    pub async fn channel_topology(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<ChannelGraph>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::graph::channel_topology(ctx, window, weighting, filter))
            .await
    }

    /// `QueryApi::overview` with the unconfirmed-channel queue, under the
    /// filter's `unconfirmed_channels`.
    pub async fn overview(
        &self,
        caller: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<OverviewCounts>> {
        require(caller, Permission::View)?;
        self.read(|ctx| queries::graph::overview(ctx, window, filter))
            .await
    }
}
