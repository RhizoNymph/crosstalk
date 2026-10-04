//! The watermark, the present, and L7's views: topology, the
//! channel-centred graph, the edge drill-down, series and the overview.

use crosstalk_spec::aggregates::access::BipartiteGraph;
use crosstalk_spec::aggregates::alert::{Alert, AlertStateKind};
use crosstalk_spec::aggregates::edge::{
    EdgeSelector, EdgeTransmissionPage, TopologyFilter, TopologyGraph, Weighting,
};
use crosstalk_spec::aggregates::series::{SeriesGrid, SeriesGrouping, TopologySeries};
use crosstalk_spec::aggregates::watermark::{Watermark, Watermarked};
use crosstalk_spec::derived::flow::channel::Channel;
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelReads;
use crosstalk_spec::interfaces::l6_analysis::alerts::AlertReads;
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
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
    /// queues as of the read: `QueueCounts::tally` over every open alert and
    /// every unreviewed channel in force, each a full traversal of the
    /// list narrowed to them.
    pub(crate) async fn overview_query(
        &self,
        caller: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<OverviewCounts>, QueryError> {
        require(caller, Permission::View)?;
        let activity = self.stores.edges().totals(window, filter).await?;
        let alerts = self.open_alerts().await?;
        let channels = self.unreviewed_channels().await?;
        Ok(Watermarked {
            watermark: activity.watermark,
            value: OverviewCounts {
                activity: activity.value,
                queues: QueueCounts::tally(&alerts, &channels),
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

    async fn unreviewed_channels(&self) -> Result<Vec<Channel>, QueryError> {
        let filter = ChannelFilter {
            origin: OriginFilter::InForce(Vec::new()),
            detections: Vec::new(),
            policies: vec![PolicyKind::Unreviewed],
            window: None,
        };
        let mut request = PageRequest {
            size: largest_page()?,
            after: None,
        };
        let mut channels = Vec::new();
        loop {
            let page = self.stores.channels().channels(&filter, &request).await?;
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
