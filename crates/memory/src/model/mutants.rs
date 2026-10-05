//! The harnesses are not vacuous: each catches a store with one planted
//! bug. Every mutant wraps a reference store and implements the same spec
//! traits, changing one method.

use crosstalk_spec::aggregates::access::{AccessEdge, BipartiteGraph};
use crosstalk_spec::aggregates::agents::AgentTraffic;
use crosstalk_spec::aggregates::alert::{
    Alert, AlertDraft, AlertRuleDef, RuleName, TriageOutcome, UserRule,
};
use crosstalk_spec::aggregates::edge::{
    EdgeKey, EdgeSelector, EdgeTotals, EdgeTransmissionPage, TopologyFilter, TopologyGraph,
    Weighting,
};
use crosstalk_spec::aggregates::projection::frame::ProjectionFrame;
use crosstalk_spec::aggregates::projection::{FitFailure, Projection, ProjectionInfo};
use crosstalk_spec::aggregates::retention::{Pin, PinChange, RetentionPolicy};
use crosstalk_spec::aggregates::series::{BucketWidth, SeriesGrid, SeriesGrouping, TopologySeries};
use crosstalk_spec::aggregates::topic::{EmbeddingModel, Topic, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crosstalk_spec::aggregates::watermark::{PipelineFrontier, Watermark, Watermarked};
use crosstalk_spec::derived::flow::verdict::{Observed, Verdict, VerdictRevision};
use crosstalk_spec::ids::{
    AgentId, AlertId, AlertRuleId, ChannelId, OperatorId, ProjectionId, SinkId, TopicId,
    TransmissionId,
};
use crosstalk_spec::interfaces::l6_analysis::alerts::{
    AlertActionError, AlertActions, AlertReadError, AlertReads, AlertRuleMaintenance,
};
use crosstalk_spec::interfaces::l6_analysis::lifecycle::{
    CatalogActivation, StoredAssignment, TopicLifecycle, TopicLifecycleError,
};
use crosstalk_spec::interfaces::l6_analysis::{
    AlertRuleStore, AlertTriage, CatalogError, ProjectionJobError, ProjectionStore,
    ProjectionStoreError, RuleError, TopicCatalog, TriageError,
};
use crosstalk_spec::interfaces::l7_topology::{
    AccessContribution, Activation, EdgeContribution, EdgeError, EdgeQueryError, EdgeStore,
};
use crosstalk_spec::interfaces::l8_surface::AlertFilter;
use crosstalk_spec::interfaces::l8_surface::lists::AlertRuleFilter;
use crosstalk_spec::paging::{
    AlertList, AlertRuleList, EdgeTransmissionList, Page, PageRequest, ProjectionList, TopicList,
};
use crosstalk_spec::support::{Change, TimeWindow, Timestamp};
use std::collections::BTreeMap;

use super::HarnessConfig;
use super::analysis::{
    ReferenceAlerts, ReferenceCatalog, check_alert_triage, check_projection_store,
    check_topic_catalog, reference_alerts, reference_catalog,
};
use super::topology::{ReferenceEdges, check_edge_store};
use crate::analysis::projection::InMemoryProjectionStore;
use crate::support::Outbox;

fn harness() -> HarnessConfig {
    HarnessConfig {
        cases: 256,
        max_ops: 60,
    }
}

/// Sizes that forget the outliers.
struct ForgetfulCatalog(ReferenceCatalog);

impl TopicCatalog for ForgetfulCatalog {
    async fn versions(&self) -> Result<TopicVersionHistory, CatalogError> {
        self.0.versions().await
    }

    async fn sizes(
        &self,
        version: TopicModelVersion,
        window: Option<TimeWindow>,
    ) -> Result<TopicSizes, CatalogError> {
        let sizes = self.0.sizes(version, window).await?;
        TopicSizes::new(
            sizes.version(),
            sizes.window(),
            sizes.topics().to_vec(),
            None,
        )
        .map_err(|_| CatalogError::InvalidCursor)
    }

    async fn lineage(&self, from: TopicModelVersion) -> Result<Option<TopicLineage>, CatalogError> {
        self.0.lineage(from).await
    }

    fn retention(&self) -> RetentionPolicy {
        self.0.retention()
    }

    async fn pin(&self, version: TopicModelVersion, pin: Pin) -> Result<PinChange, CatalogError> {
        self.0.pin(version, pin).await
    }

    async fn unpin(
        &self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> Result<PinChange, CatalogError> {
        self.0.unpin(version, at).await
    }

    async fn enforce_retention(
        &self,
        at: Timestamp,
    ) -> Result<Vec<TopicModelVersion>, CatalogError> {
        self.0.enforce_retention(at).await
    }

    async fn topics(
        &self,
        version: TopicModelVersion,
        page: &PageRequest<TopicList>,
    ) -> Result<Page<Topic, TopicList>, CatalogError> {
        self.0.topics(version, page).await
    }
}

impl TopicLifecycle for ForgetfulCatalog {
    async fn begin_fit(&mut self, at: Timestamp) -> Result<TopicModelVersion, TopicLifecycleError> {
        self.0.begin_fit(at).await
    }

    async fn complete_fit(
        &mut self,
        version: TopicModelVersion,
        topics: Vec<Topic>,
        fitted_at: Timestamp,
    ) -> Result<TopicLineage, TopicLifecycleError> {
        self.0.complete_fit(version, topics, fitted_at).await
    }

    async fn fail_fit(&mut self, version: TopicModelVersion) -> Result<(), TopicLifecycleError> {
        self.0.fail_fit(version).await
    }

    async fn mark_ready(
        &mut self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> Result<(), TopicLifecycleError> {
        self.0.mark_ready(version, at).await
    }

    async fn mark_active(
        &mut self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> Result<CatalogActivation, TopicLifecycleError> {
        self.0.mark_active(version, at).await
    }

    async fn assign(
        &mut self,
        transmission: TransmissionId,
        version: TopicModelVersion,
        assignment: StoredAssignment,
    ) -> Result<Change, TopicLifecycleError> {
        self.0.assign(transmission, version, assignment).await
    }
}

#[test]
fn catalog_harness_catches_forgotten_outliers() {
    let result = check_topic_catalog(harness(), |config| async move {
        ForgetfulCatalog(reference_catalog(config).expect("a reference catalog"))
    });
    assert!(result.is_err());
}

/// Leases that never lapse.
struct StickyLeases(InMemoryProjectionStore);

impl ProjectionStore for StickyLeases {
    async fn enqueue(&mut self, job: ProjectionInfo) -> Result<(), ProjectionStoreError> {
        self.0.enqueue(job).await
    }

    async fn claim(&mut self, at: Timestamp) -> Result<Option<ProjectionInfo>, ProjectionJobError> {
        self.0.claim(at).await
    }

    async fn complete(
        &mut self,
        id: ProjectionId,
        frame: ProjectionFrame,
        at: Timestamp,
    ) -> Result<(), ProjectionJobError> {
        self.0.complete(id, frame, at).await
    }

    async fn fail(
        &mut self,
        id: ProjectionId,
        failure: FitFailure,
        at: Timestamp,
    ) -> Result<(), ProjectionJobError> {
        self.0.fail(id, failure, at).await
    }

    async fn requeue_lapsed(&mut self, _now: Timestamp) -> Result<u32, ProjectionJobError> {
        Ok(0)
    }

    async fn expire(&mut self, now: Timestamp) -> Result<u32, ProjectionJobError> {
        self.0.expire(now).await
    }

    async fn info(&self, id: ProjectionId) -> Result<Option<ProjectionInfo>, ProjectionStoreError> {
        self.0.info(id).await
    }

    async fn list(
        &self,
        page: &PageRequest<ProjectionList>,
    ) -> Result<Page<ProjectionInfo, ProjectionList>, ProjectionStoreError> {
        self.0.list(page).await
    }

    async fn projection(&self, id: ProjectionId) -> Result<Projection, ProjectionStoreError> {
        self.0.projection(id).await
    }
}

#[test]
fn projection_harness_catches_sticky_leases() {
    let result = check_projection_store(harness(), |config| async move {
        StickyLeases(InMemoryProjectionStore::new(config, Outbox::none()))
    });
    assert!(result.is_err());
}

/// A verdict copy that ignores every verdict.
struct DeafEdges(ReferenceEdges);

impl EdgeStore for DeafEdges {
    async fn apply(&mut self, contribution: &EdgeContribution) -> Result<EdgeKey, EdgeError> {
        self.0.apply(contribution).await
    }

    async fn judge(
        &mut self,
        _transmission: TransmissionId,
        _verdict: Option<Verdict>,
        _revision: VerdictRevision,
    ) -> Result<Observed, EdgeError> {
        Ok(Observed::Newer)
    }

    async fn version_ready(
        &mut self,
        version: TopicModelVersion,
        transmissions: u64,
    ) -> Result<(), EdgeError> {
        self.0.version_ready(version, transmissions).await
    }

    async fn activate(&mut self, version: TopicModelVersion) -> Result<Activation, EdgeError> {
        self.0.activate(version).await
    }

    async fn drop_version(&mut self, version: TopicModelVersion) -> Result<(), EdgeError> {
        self.0.drop_version(version).await
    }

    async fn watermark(&self) -> Result<Watermark, EdgeQueryError> {
        self.0.watermark().await
    }

    async fn advance_watermark(
        &mut self,
        frontier: PipelineFrontier,
    ) -> Result<Option<Watermark>, EdgeError> {
        self.0.advance_watermark(frontier).await
    }

    async fn apply_access(&mut self, access: &AccessContribution) -> Result<AccessEdge, EdgeError> {
        self.0.apply_access(access).await
    }

    async fn graph(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologyGraph>, EdgeQueryError> {
        self.0.graph(window, weighting, filter).await
    }

    async fn totals(
        &self,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<EdgeTotals>, EdgeQueryError> {
        self.0.totals(window, filter).await
    }

    async fn channel_topology(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<BipartiteGraph>, EdgeQueryError> {
        self.0.channel_topology(window, weighting, filter).await
    }

    async fn transmissions(
        &self,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> Result<Watermarked<EdgeTransmissionPage>, EdgeQueryError> {
        self.0.transmissions(edge, window, filter, page).await
    }

    async fn agent_traffic(
        &self,
        window: TimeWindow,
        agents: &[AgentId],
    ) -> Result<Watermarked<BTreeMap<AgentId, AgentTraffic>>, EdgeQueryError> {
        self.0.agent_traffic(window, agents).await
    }

    fn bucket_width(&self) -> BucketWidth {
        self.0.bucket_width()
    }

    async fn series(
        &self,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologySeries>, EdgeQueryError> {
        self.0.series(grid, weighting, grouping, filter).await
    }
}

impl TopicLifecycle for DeafEdges {
    async fn begin_fit(&mut self, at: Timestamp) -> Result<TopicModelVersion, TopicLifecycleError> {
        self.0.begin_fit(at).await
    }

    async fn complete_fit(
        &mut self,
        version: TopicModelVersion,
        topics: Vec<Topic>,
        fitted_at: Timestamp,
    ) -> Result<TopicLineage, TopicLifecycleError> {
        self.0.complete_fit(version, topics, fitted_at).await
    }

    async fn fail_fit(&mut self, version: TopicModelVersion) -> Result<(), TopicLifecycleError> {
        self.0.fail_fit(version).await
    }

    async fn mark_ready(
        &mut self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> Result<(), TopicLifecycleError> {
        self.0.mark_ready(version, at).await
    }

    async fn mark_active(
        &mut self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> Result<CatalogActivation, TopicLifecycleError> {
        self.0.mark_active(version, at).await
    }

    async fn assign(
        &mut self,
        transmission: TransmissionId,
        version: TopicModelVersion,
        assignment: StoredAssignment,
    ) -> Result<Change, TopicLifecycleError> {
        self.0.assign(transmission, version, assignment).await
    }
}

#[test]
fn edge_harness_catches_ignored_verdicts() {
    let result = check_edge_store(harness(), |config, world| async move {
        DeafEdges(ReferenceEdges::new(config, world).expect("a reference edge world"))
    });
    assert!(result.is_err());
}

/// Sanctions that suppress nothing.
struct LaxTriage(ReferenceAlerts);

impl AlertRuleStore for LaxTriage {
    async fn create(
        &mut self,
        name: RuleName,
        rule: UserRule,
        sinks: Vec<SinkId>,
        by: OperatorId,
        at: Timestamp,
    ) -> Result<AlertRuleId, RuleError> {
        self.0.create(name, rule, sinks, by, at).await
    }

    async fn update(
        &mut self,
        id: AlertRuleId,
        name: RuleName,
        rule: UserRule,
        sinks: Vec<SinkId>,
        by: OperatorId,
    ) -> Result<Change, RuleError> {
        self.0.update(id, name, rule, sinks, by).await
    }

    async fn set_enabled(
        &mut self,
        id: AlertRuleId,
        enabled: bool,
        by: OperatorId,
        at: Timestamp,
    ) -> Result<Change, RuleError> {
        self.0.set_enabled(id, enabled, by, at).await
    }
}

impl AlertTriage for LaxTriage {
    async fn triage(&mut self, draft: AlertDraft) -> Result<TriageOutcome, TriageError> {
        self.0.triage(draft).await
    }

    async fn channel_sanctioned(
        &mut self,
        _channel: ChannelId,
        _at: Timestamp,
    ) -> Result<u32, TriageError> {
        Ok(0)
    }

    async fn rule_disabled(
        &mut self,
        rule: AlertRuleId,
        at: Timestamp,
    ) -> Result<u32, TriageError> {
        self.0.rule_disabled(rule, at).await
    }

    async fn transmission_judged(
        &mut self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
        at: Timestamp,
    ) -> Result<u32, TriageError> {
        self.0
            .transmission_judged(transmission, verdict, revision, at)
            .await
    }
}

impl AlertRuleMaintenance for LaxTriage {
    async fn topic_version_ready(
        &mut self,
        lineage: &TopicLineage,
        topics: &[TopicId],
    ) -> Result<Vec<AlertRuleId>, RuleError> {
        self.0.topic_version_ready(lineage, topics).await
    }

    async fn embedding_model_changed(
        &mut self,
        model: &EmbeddingModel,
    ) -> Result<Vec<AlertRuleId>, RuleError> {
        self.0.embedding_model_changed(model).await
    }
}

impl AlertActions for LaxTriage {
    async fn acknowledge(
        &mut self,
        alert: AlertId,
        by: OperatorId,
        at: Timestamp,
    ) -> Result<Change, AlertActionError> {
        self.0.acknowledge(alert, by, at).await
    }

    async fn resolve(
        &mut self,
        alert: AlertId,
        by: OperatorId,
        at: Timestamp,
        note: Option<String>,
    ) -> Result<Change, AlertActionError> {
        self.0.resolve(alert, by, at, note).await
    }
}

impl AlertReads for LaxTriage {
    async fn rule(&self, id: AlertRuleId) -> Result<Option<AlertRuleDef>, AlertReadError> {
        self.0.rule(id).await
    }

    async fn rules(
        &self,
        filter: &AlertRuleFilter,
        page: &PageRequest<AlertRuleList>,
    ) -> Result<Page<AlertRuleDef, AlertRuleList>, AlertReadError> {
        self.0.rules(filter, page).await
    }

    async fn alert(&self, id: AlertId) -> Result<Option<Alert>, AlertReadError> {
        self.0.alert(id).await
    }

    async fn alerts(
        &self,
        filter: &AlertFilter,
        page: &PageRequest<AlertList>,
    ) -> Result<Page<Alert, AlertList>, AlertReadError> {
        self.0.alerts(filter, page).await
    }

    async fn rule_version(&self) -> Result<TopicModelVersion, AlertReadError> {
        self.0.rule_version().await
    }
}

#[test]
fn triage_harness_catches_lax_sanctions() {
    let result = check_alert_triage(harness(), |world| async move {
        LaxTriage(reference_alerts(world))
    });
    assert!(result.is_err());
}
