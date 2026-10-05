//! [`EdgeStore`] on [`PgEdgeStore`]: each method is one of the store's
//! writes ([`super::write`]) or reads ([`super::read`], [`super::drill`]).

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::access::{AccessEdge, BipartiteGraph};
use crosstalk_spec::aggregates::agents::AgentTraffic;
use crosstalk_spec::aggregates::edge::{
    EdgeKey, EdgeSelector, EdgeTotals, EdgeTransmissionPage, TopologyFilter, TopologyGraph,
    Weighting,
};
use crosstalk_spec::aggregates::series::{BucketWidth, SeriesGrid, SeriesGrouping, TopologySeries};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::watermark::{PipelineFrontier, Watermark, Watermarked};
use crosstalk_spec::derived::flow::verdict::{Observed, Verdict, VerdictRevision};
use crosstalk_spec::ids::{AgentId, TransmissionId};
use crosstalk_spec::interfaces::l7_topology::{
    AccessContribution, Activation, EdgeContribution, EdgeError, EdgeQueryError, EdgeStore,
};
use crosstalk_spec::paging::{EdgeTransmissionList, PageRequest};
use crosstalk_spec::support::TimeWindow;

use super::PgEdgeStore;
use super::error::DbError;
use super::read::read_watermark;
use crate::env::TopologyEnv;

impl<V: TopologyEnv> EdgeStore for PgEdgeStore<V> {
    async fn apply(&mut self, contribution: &EdgeContribution) -> Result<EdgeKey, EdgeError> {
        self.apply_impl(contribution).await.map_err(Into::into)
    }

    async fn judge(
        &mut self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
    ) -> Result<Observed, EdgeError> {
        self.judge_impl(transmission, verdict, revision)
            .await
            .map_err(Into::into)
    }

    async fn version_ready(
        &mut self,
        version: TopicModelVersion,
        transmissions: u64,
    ) -> Result<(), EdgeError> {
        self.version_ready_impl(version, transmissions)
            .await
            .map_err(Into::into)
    }

    async fn activate(&mut self, version: TopicModelVersion) -> Result<Activation, EdgeError> {
        self.activate_impl(version).await.map_err(Into::into)
    }

    async fn drop_version(&mut self, version: TopicModelVersion) -> Result<(), EdgeError> {
        self.drop_impl(version).await.map_err(Into::into)
    }

    async fn watermark(&self) -> Result<Watermark, EdgeQueryError> {
        let read = async {
            let mut conn = self.pool.acquire().await.map_err(DbError::from)?;
            read_watermark(&mut conn).await
        };
        read.await.map_err(|error| EdgeQueryError::Store {
            reason: error.to_string(),
        })
    }

    async fn advance_watermark(
        &mut self,
        frontier: PipelineFrontier,
    ) -> Result<Option<Watermark>, EdgeError> {
        self.advance_impl(frontier).await.map_err(Into::into)
    }

    async fn apply_access(&mut self, access: &AccessContribution) -> Result<AccessEdge, EdgeError> {
        self.apply_access_impl(access).await.map_err(Into::into)
    }

    async fn graph(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologyGraph>, EdgeQueryError> {
        self.graph_impl(window, weighting, filter)
            .await
            .map_err(Into::into)
    }

    async fn totals(
        &self,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<EdgeTotals>, EdgeQueryError> {
        self.totals_impl(window, filter).await.map_err(Into::into)
    }

    async fn channel_topology(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<BipartiteGraph>, EdgeQueryError> {
        self.channel_topology_impl(window, weighting, filter)
            .await
            .map_err(Into::into)
    }

    async fn transmissions(
        &self,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> Result<Watermarked<EdgeTransmissionPage>, EdgeQueryError> {
        self.transmissions_impl(edge, window, filter, page)
            .await
            .map_err(Into::into)
    }

    async fn agent_traffic(
        &self,
        window: TimeWindow,
        agents: &[AgentId],
    ) -> Result<Watermarked<BTreeMap<AgentId, AgentTraffic>>, EdgeQueryError> {
        self.agent_traffic_impl(window, agents)
            .await
            .map_err(Into::into)
    }

    fn bucket_width(&self) -> BucketWidth {
        self.config.bucket_width
    }

    async fn series(
        &self,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologySeries>, EdgeQueryError> {
        self.series_impl(grid, weighting, grouping, filter)
            .await
            .map_err(Into::into)
    }
}
