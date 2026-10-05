//! The watermark, the present, and L7's views: topology, the
//! channel-centred graph, the edge drill-down, series and the overview.

use std::collections::{BTreeMap, HashSet};

use crosstalk_spec::aggregates::access::BipartiteGraph;
use crosstalk_spec::aggregates::alert::{Alert, AlertStateKind};
use crosstalk_spec::aggregates::edge::{
    EdgeSelector, EdgeTransmissionPage, TopologyFilter, TopologyGraph, Weighting,
};
use crosstalk_spec::aggregates::series::{SeriesGrid, SeriesGrouping, TopologySeries};
use crosstalk_spec::aggregates::watermark::{Watermark, Watermarked};
use crosstalk_spec::derived::flow::channel::confirmation::ListingKind;
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::ids::AlertId;
use crosstalk_spec::interfaces::l5_flow::channels::{ChannelReads, ChannelWithTraffic};
use crosstalk_spec::interfaces::l6_analysis::alerts::AlertReads;
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_spec::interfaces::l8_surface::channels::ChannelRow;
use crosstalk_spec::interfaces::l8_surface::lists::{ChannelFilter, OriginFilter};
use crosstalk_spec::interfaces::l8_surface::overview::{OverviewCounts, QueueCounts};
use crosstalk_spec::interfaces::l8_surface::{
    AlertFilter, Caller, Permission, Present, QueryError,
};
use crosstalk_spec::paging::{EdgeTransmissionList, PageRequest};
use crosstalk_spec::support::TimeWindow;

use crate::service::{Surface, largest_page, require};
use crate::stores::SurfaceStores;

impl<S: SurfaceStores> Surface<S> {
    pub(crate) async fn watermark_query(&self, caller: &Caller) -> Result<Watermark, QueryError> {
        require(caller, Permission::View)?;
        Ok(self.stores.edges().watermark().await?)
    }

    /// The wall clock, never earlier than the watermark exposed when it is
    /// read, and the configuration a client builds requests with.
    pub(crate) async fn present_query(&self, caller: &Caller) -> Result<Present, QueryError> {
        require(caller, Permission::View)?;
        let watermark = self.stores.edges().watermark().await?;
        let current_rule_version = self.stores.alerts().rule_version().await?;
        let now = self.now().max(watermark.at());
        Ok(Present {
            now,
            bucket_width: self.bucket_width(),
            export_formats: self.config.export_formats.clone(),
            current_rule_version,
            default_remap_threshold: self.config.default_remap_threshold,
            frame_retention_micros: self.config.frame_retention,
        })
    }

    pub(crate) async fn topology_query(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologyGraph>, QueryError> {
        require(caller, Permission::View)?;
        Ok(self.stores.edges().graph(window, weighting, filter).await?)
    }

    /// The activity `topology` counts (`EdgeStore::totals`, which reads the
    /// watermark before its buckets, before anything else here), then the
    /// queues as of the read: `QueueCounts::tally` over every open alert,
    /// with which of them are shown read now, and the row of every channel
    /// the queues can count (listed in force and unreviewed, or listed as
    /// unconfirmed), each a full traversal of the channel list narrowed to
    /// them. Channels outside both traversals count in no queue, so the
    /// tally over these rows is the tally over every stored channel's row.
    pub(crate) async fn overview_query(
        &self,
        caller: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<OverviewCounts>, QueryError> {
        require(caller, Permission::View)?;
        let activity = self.stores.edges().totals(window, filter).await?;
        let alerts = self.open_alerts().await?;
        let shown: HashSet<AlertId> = self
            .shown_alerts(alerts.clone())
            .await?
            .into_iter()
            .map(|alert| alert.id)
            .collect();
        let rows = self.queue_rows().await?;
        Ok(Watermarked {
            watermark: activity.watermark,
            value: OverviewCounts {
                activity: activity.value,
                queues: QueueCounts::tally(
                    &alerts,
                    |alert| shown.contains(&alert.id),
                    &rows,
                    filter.unconfirmed_channels,
                ),
            },
        })
    }

    async fn open_alerts(&self) -> Result<Vec<Alert>, QueryError> {
        let filter = AlertFilter {
            states: vec![AlertStateKind::Open],
            channel: None,
        };
        let mut request = PageRequest {
            size: largest_page()?,
            after: None,
        };
        let mut alerts = Vec::new();
        loop {
            let page = self.stores.alerts().alerts(&filter, &request).await?;
            let (items, next) = page.into_parts();
            alerts.extend(items);
            match next {
                Some(next) => request.after = Some(next),
                None => return Ok(alerts),
            }
        }
    }

    /// The rows, counted over all time, of the listed channels in force
    /// whose policy is `Unreviewed` and of the channels listed as
    /// unconfirmed: every channel a queue can count.
    async fn queue_rows(&self) -> Result<Vec<ChannelRow>, QueryError> {
        let unreviewed = ChannelFilter {
            origin: OriginFilter::InForce(Vec::new()),
            listings: Vec::new(),
            detections: Vec::new(),
            policies: vec![PolicyKind::Unreviewed],
            window: None,
        };
        let unconfirmed = ChannelFilter {
            listings: vec![ListingKind::Unconfirmed],
            ..ChannelFilter::default()
        };
        let mut reads = BTreeMap::new();
        for filter in [unreviewed, unconfirmed] {
            for read in self.all_channels(&filter).await? {
                reads.insert(read.channel().id, read);
            }
        }
        let all_time = self.all_time()?;
        let routed = if reads.is_empty() {
            Default::default()
        } else {
            self.routed(all_time).await?
        };
        let mut rows = Vec::with_capacity(reads.len());
        for read in reads.into_values() {
            rows.push(self.channel_row(read, all_time, &routed).await?);
        }
        Ok(rows)
    }

    /// Every channel `filter` keeps: a full `ChannelReads::channels`
    /// traversal.
    async fn all_channels(
        &self,
        filter: &ChannelFilter,
    ) -> Result<Vec<ChannelWithTraffic>, QueryError> {
        let mut request = PageRequest {
            size: largest_page()?,
            after: None,
        };
        let mut channels = Vec::new();
        loop {
            let page = self.stores.channels().channels(filter, &request).await?;
            let (items, next) = page.into_parts();
            channels.extend(items);
            match next {
                Some(next) => request.after = Some(next),
                None => return Ok(channels),
            }
        }
    }

    pub(crate) async fn channel_topology_query(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<BipartiteGraph>, QueryError> {
        require(caller, Permission::View)?;
        Ok(self
            .stores
            .edges()
            .channel_topology(window, weighting, filter)
            .await?)
    }

    pub(crate) async fn edge_transmissions_query(
        &self,
        caller: &Caller,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> Result<Watermarked<EdgeTransmissionPage>, QueryError> {
        require(caller, Permission::View)?;
        Ok(self
            .stores
            .edges()
            .transmissions(edge, window, filter, page)
            .await?)
    }

    pub(crate) async fn series_query(
        &self,
        caller: &Caller,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologySeries>, QueryError> {
        require(caller, Permission::View)?;
        Ok(self
            .stores
            .edges()
            .series(grid, weighting, grouping, filter)
            .await?)
    }
}
