use crate::aggregates::access::{AccessEdge, BipartiteGraph, ResourceUsePage};
use crate::aggregates::agents::AgentTraffic;
use crate::aggregates::alert::rules::{AlertRuleKind, RuleName, UserRule};
use crate::aggregates::alert::{AlertDraft, TriageOutcome};
use crate::aggregates::edge::{
    EdgeKey, EdgeSelector, EdgeTotals, EdgeTransmissionPage, TopologyGraph, Weighting,
};
use crate::aggregates::filter::TopologyFilter;
use crate::aggregates::projection::frame::ProjectionFrame;
use crate::aggregates::projection::{
    FitFailure, Projection, ProjectionInfo, ProjectionParams, ProjectionSpec,
};
use crate::aggregates::quality::DetectionQuality;
use crate::aggregates::retention::{Pin, PinChange, RetentionPolicy};
use crate::aggregates::series::{BucketWidth, SeriesGrid, SeriesGrouping, TopologySeries};
use crate::aggregates::topic::{Assignment, Embedding, EmbeddingModel, Topic, TopicModelVersion};
use crate::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crate::aggregates::watermark::{PipelineFrontier, Watermarked};
use crate::derived::flow::channel::Declaration;
use crate::derived::flow::channel::policy::{
    Policy, PolicyAuthor, PolicyDecision, PolicyHistory, Recorded,
};
use crate::derived::flow::channel::promotion::{Promotion, PromotionCoverage};
use crate::derived::flow::resource::{Locator, ResourcePattern};
use crate::derived::flow::verdict::{
    Observed, Verdict, VerdictLog, VerdictRecorded, VerdictRevision,
};
use crate::events::Envelope;
use crate::ids::{
    AgentId, AlertRuleId, ChannelId, OperatorId, ProjectionId, SinkId, TransmissionId,
};
use crate::interfaces::l5_flow::verdicts::{TransmissionVerdicts, VerdictError};
use crate::interfaces::l5_flow::{
    ChannelLookup, ChannelRegistry, PromoteError, Promoted, RegistryError,
};
use crate::interfaces::l6_analysis::{
    AlertRuleEval, AlertRuleStore, AlertTriage, CatalogError, EmbedError, Embedder, FitDocument,
    LayoutError, LayoutFitter, ProjectionJobError, ProjectionSource, ProjectionStore,
    ProjectionStoreError, RuleContext, RuleError, Sample, SampleError, SearchError, SearchIndex,
    SearchQuery, SearchResults, TopicCatalog, TopicError, TopicModel, TriageError,
};
use crate::interfaces::l7_topology::{
    AccessContribution, Activation, EdgeContribution, EdgeError, EdgeQueryError, EdgeStore,
    FrontierSource,
};
use crate::paging::{EdgeTransmissionList, ProjectionList, ResourceUseList, SearchList, TopicList};
use crate::paging::{Page, PageRequest};
use crate::support::{Change, TimeWindow, Timestamp, Watermark};
use std::collections::BTreeMap;

use super::{Dummy, arg, assert_send};

// ── L5 flow ────────────────────────────────────────────────────────────

impl ChannelRegistry for Dummy {
    async fn lookup(&self, _locator: &Locator) -> Result<ChannelLookup, RegistryError> {
        match *self {}
    }
    async fn declare(
        &mut self,
        _pattern: ResourcePattern,
        _policy: Policy,
        _by: PolicyAuthor,
        _at: Timestamp,
    ) -> Result<ChannelId, RegistryError> {
        match *self {}
    }
    async fn set_policy(
        &mut self,
        _channel: ChannelId,
        _decision: PolicyDecision,
    ) -> Result<Recorded, RegistryError> {
        match *self {}
    }
    async fn policy_history(&self, _channel: ChannelId) -> Result<PolicyHistory, RegistryError> {
        match *self {}
    }
    async fn promote(
        &mut self,
        _channel: ChannelId,
        _promotion: Promotion,
    ) -> Result<Promoted, PromoteError> {
        match *self {}
    }
    async fn promotion_coverage(
        &self,
        _channel: ChannelId,
        _declaration: &Declaration,
    ) -> Result<PromotionCoverage, PromoteError> {
        match *self {}
    }
    async fn resource_use(
        &self,
        _channel: ChannelId,
        _window: TimeWindow,
        _page: &PageRequest<ResourceUseList>,
    ) -> Result<ResourceUsePage, RegistryError> {
        match *self {}
    }
}

fn channel_registry<T: ChannelRegistry>(x: &mut T, never: &Dummy) {
    assert_send(x.lookup(arg(never)));
    assert_send(x.declare(arg(never), arg(never), arg(never), arg(never)));
    assert_send(x.set_policy(arg(never), arg(never)));
    assert_send(x.policy_history(arg(never)));
    assert_send(x.promote(arg(never), arg(never)));
    assert_send(x.promotion_coverage(arg(never), arg(never)));
    assert_send(x.resource_use(arg(never), arg(never), arg(never)));
}

impl TransmissionVerdicts for Dummy {
    async fn set(
        &mut self,
        _transmission: TransmissionId,
        _verdict: Option<Verdict>,
        _by: OperatorId,
        _at: Timestamp,
        _note: Option<String>,
    ) -> Result<VerdictRecorded, VerdictError> {
        match *self {}
    }
    async fn log(&self, _transmission: TransmissionId) -> Result<VerdictLog, VerdictError> {
        match *self {}
    }
    async fn quality(&self, _window: TimeWindow) -> Result<DetectionQuality, VerdictError> {
        match *self {}
    }
}

fn transmission_verdicts<T: TransmissionVerdicts>(x: &mut T, never: &Dummy) {
    assert_send(x.set(arg(never), arg(never), arg(never), arg(never), arg(never)));
    assert_send(x.log(arg(never)));
    assert_send(x.quality(arg(never)));
}

// ── L6 analysis ────────────────────────────────────────────────────────

impl Embedder for Dummy {
    fn model(&self) -> EmbeddingModel {
        match *self {}
    }
    async fn embed(&self, _texts: &[&str]) -> Result<Vec<Embedding>, EmbedError> {
        match *self {}
    }
}

impl TopicCatalog for Dummy {
    async fn versions(&self) -> Result<TopicVersionHistory, CatalogError> {
        match *self {}
    }
    async fn sizes(
        &self,
        _version: TopicModelVersion,
        _window: Option<TimeWindow>,
    ) -> Result<TopicSizes, CatalogError> {
        match *self {}
    }
    async fn lineage(
        &self,
        _from: TopicModelVersion,
    ) -> Result<Option<TopicLineage>, CatalogError> {
        match *self {}
    }
    fn retention(&self) -> RetentionPolicy {
        match *self {}
    }
    async fn pin(&self, _version: TopicModelVersion, _pin: Pin) -> Result<PinChange, CatalogError> {
        match *self {}
    }
    async fn unpin(
        &self,
        _version: TopicModelVersion,
        _at: Timestamp,
    ) -> Result<PinChange, CatalogError> {
        match *self {}
    }
    async fn enforce_retention(
        &self,
        _at: Timestamp,
    ) -> Result<Vec<TopicModelVersion>, CatalogError> {
        match *self {}
    }
    async fn topics(
        &self,
        _version: TopicModelVersion,
        _page: &PageRequest<TopicList>,
    ) -> Result<Page<Topic, TopicList>, CatalogError> {
        match *self {}
    }
}

impl SearchIndex for Dummy {
    async fn query(
        &self,
        _query: &SearchQuery,
        _window: Option<TimeWindow>,
        _filter: &TopologyFilter,
        _page: &PageRequest<SearchList>,
    ) -> Result<SearchResults, SearchError> {
        match *self {}
    }
}

impl ProjectionStore for Dummy {
    async fn enqueue(&mut self, _job: ProjectionInfo) -> Result<(), ProjectionStoreError> {
        match *self {}
    }
    async fn claim(
        &mut self,
        _at: Timestamp,
    ) -> Result<Option<ProjectionInfo>, ProjectionJobError> {
        match *self {}
    }
    async fn complete(
        &mut self,
        _id: ProjectionId,
        _frame: ProjectionFrame,
        _at: Timestamp,
    ) -> Result<(), ProjectionJobError> {
        match *self {}
    }
    async fn fail(
        &mut self,
        _id: ProjectionId,
        _failure: FitFailure,
        _at: Timestamp,
    ) -> Result<(), ProjectionJobError> {
        match *self {}
    }
    async fn requeue_lapsed(&mut self, _now: Timestamp) -> Result<u32, ProjectionJobError> {
        match *self {}
    }
    async fn expire(&mut self, _now: Timestamp) -> Result<u32, ProjectionJobError> {
        match *self {}
    }
    async fn info(
        &self,
        _id: ProjectionId,
    ) -> Result<Option<ProjectionInfo>, ProjectionStoreError> {
        match *self {}
    }
    async fn list(
        &self,
        _page: &PageRequest<ProjectionList>,
    ) -> Result<Page<ProjectionInfo, ProjectionList>, ProjectionStoreError> {
        match *self {}
    }
    async fn projection(&self, _id: ProjectionId) -> Result<Projection, ProjectionStoreError> {
        match *self {}
    }
}

impl TopicModel for Dummy {
    fn version(&self) -> TopicModelVersion {
        match *self {}
    }
    async fn fit(
        &self,
        _version: TopicModelVersion,
        _documents: &[FitDocument<'_>],
        _at: Timestamp,
    ) -> Result<Vec<Topic>, TopicError> {
        match *self {}
    }
    fn assign(&self, _embedding: &Embedding) -> Result<Assignment, TopicError> {
        match *self {}
    }
}

impl LayoutFitter for Dummy {
    async fn fit(
        &self,
        _embeddings: &[Embedding],
        _params: ProjectionParams,
    ) -> Result<Vec<[f32; 2]>, LayoutError> {
        match *self {}
    }
}

impl ProjectionSource for Dummy {
    async fn sample(&self, _spec: &ProjectionSpec) -> Result<Sample, SampleError> {
        match *self {}
    }
}

impl RuleContext for Dummy {
    async fn channel_policy(&self, _channel: ChannelId) -> Option<Policy> {
        match *self {}
    }
    async fn transmission_embedding(&self, _transmission: TransmissionId) -> Option<Embedding> {
        match *self {}
    }
}

impl AlertRuleEval for Dummy {
    fn kind(&self) -> AlertRuleKind {
        match *self {}
    }
    async fn evaluate(
        &self,
        _envelope: &Envelope,
        _context: &impl RuleContext,
    ) -> Option<AlertDraft> {
        match *self {}
    }
}

impl AlertTriage for Dummy {
    async fn triage(&mut self, _draft: AlertDraft) -> Result<TriageOutcome, TriageError> {
        match *self {}
    }
    async fn channel_sanctioned(
        &mut self,
        _channel: ChannelId,
        _at: Timestamp,
    ) -> Result<u32, TriageError> {
        match *self {}
    }
    async fn rule_disabled(
        &mut self,
        _rule: AlertRuleId,
        _at: Timestamp,
    ) -> Result<u32, TriageError> {
        match *self {}
    }
    async fn transmission_judged(
        &mut self,
        _transmission: TransmissionId,
        _verdict: Option<Verdict>,
        _revision: VerdictRevision,
        _at: Timestamp,
    ) -> Result<u32, TriageError> {
        match *self {}
    }
}

impl AlertRuleStore for Dummy {
    async fn create(
        &mut self,
        _name: RuleName,
        _rule: UserRule,
        _sinks: Vec<SinkId>,
        _by: OperatorId,
        _at: Timestamp,
    ) -> Result<AlertRuleId, RuleError> {
        match *self {}
    }
    async fn update(
        &mut self,
        _id: AlertRuleId,
        _name: RuleName,
        _rule: UserRule,
        _sinks: Vec<SinkId>,
        _by: OperatorId,
    ) -> Result<Change, RuleError> {
        match *self {}
    }
    async fn set_enabled(
        &mut self,
        _id: AlertRuleId,
        _enabled: bool,
        _by: OperatorId,
        _at: Timestamp,
    ) -> Result<Change, RuleError> {
        match *self {}
    }
}

fn embedder<T: Embedder>(x: &T, never: &Dummy) {
    assert_send(x.embed(arg(never)));
}

fn topic_catalog<T: TopicCatalog>(x: &T, never: &Dummy) {
    assert_send(x.versions());
    assert_send(x.sizes(arg(never), arg(never)));
    assert_send(x.lineage(arg(never)));
    assert_send(x.pin(arg(never), arg(never)));
    assert_send(x.unpin(arg(never), arg(never)));
    assert_send(x.enforce_retention(arg(never)));
    assert_send(x.topics(arg(never), arg(never)));
}

fn search_index<T: SearchIndex>(x: &T, never: &Dummy) {
    assert_send(x.query(arg(never), arg(never), arg(never), arg(never)));
}

fn projection_store<T: ProjectionStore>(x: &mut T, never: &Dummy) {
    assert_send(x.enqueue(arg(never)));
    assert_send(x.claim(arg(never)));
    assert_send(x.complete(arg(never), arg(never), arg(never)));
    assert_send(x.fail(arg(never), arg(never), arg(never)));
    assert_send(x.requeue_lapsed(arg(never)));
    assert_send(x.expire(arg(never)));
    assert_send(x.info(arg(never)));
    assert_send(x.list(arg(never)));
    assert_send(x.projection(arg(never)));
}

fn topic_model<T: TopicModel>(x: &T, never: &Dummy) {
    assert_send(x.fit(arg(never), arg(never), arg(never)));
}

fn layout_fitter<T: LayoutFitter>(x: &T, never: &Dummy) {
    assert_send(x.fit(arg(never), arg(never)));
}

fn projection_source<T: ProjectionSource>(x: &T, never: &Dummy) {
    assert_send(x.sample(arg(never)));
}

fn rule_context<T: RuleContext>(x: &T, never: &Dummy) {
    assert_send(x.channel_policy(arg(never)));
    assert_send(x.transmission_embedding(arg(never)));
}

fn alert_rule_eval<T: AlertRuleEval, C: RuleContext>(x: &T, never: &Dummy) {
    assert_send(x.evaluate(arg(never), arg::<&C>(never)));
}

fn alert_triage<T: AlertTriage>(x: &mut T, never: &Dummy) {
    assert_send(x.triage(arg(never)));
    assert_send(x.channel_sanctioned(arg(never), arg(never)));
    assert_send(x.rule_disabled(arg(never), arg(never)));
    assert_send(x.transmission_judged(arg(never), arg(never), arg(never), arg(never)));
}

fn alert_rule_store<T: AlertRuleStore>(x: &mut T, never: &Dummy) {
    assert_send(x.create(arg(never), arg(never), arg(never), arg(never), arg(never)));
    assert_send(x.update(arg(never), arg(never), arg(never), arg(never), arg(never)));
    assert_send(x.set_enabled(arg(never), arg(never), arg(never), arg(never)));
}

// ── L7 topology ────────────────────────────────────────────────────────

impl EdgeStore for Dummy {
    async fn apply(&mut self, _contribution: &EdgeContribution) -> Result<EdgeKey, EdgeError> {
        match *self {}
    }
    async fn judge(
        &mut self,
        _transmission: TransmissionId,
        _verdict: Option<Verdict>,
        _revision: VerdictRevision,
    ) -> Result<Observed, EdgeError> {
        match *self {}
    }
    async fn version_ready(
        &mut self,
        _version: TopicModelVersion,
        _transmissions: u64,
    ) -> Result<(), EdgeError> {
        match *self {}
    }
    async fn activate(&mut self, _version: TopicModelVersion) -> Result<Activation, EdgeError> {
        match *self {}
    }
    async fn drop_version(&mut self, _version: TopicModelVersion) -> Result<(), EdgeError> {
        match *self {}
    }
    async fn watermark(&self) -> Result<Watermark, EdgeQueryError> {
        match *self {}
    }
    async fn advance_watermark(
        &mut self,
        _frontier: PipelineFrontier,
    ) -> Result<Option<Watermark>, EdgeError> {
        match *self {}
    }
    async fn apply_access(
        &mut self,
        _access: &AccessContribution,
    ) -> Result<AccessEdge, EdgeError> {
        match *self {}
    }
    async fn graph(
        &self,
        _window: TimeWindow,
        _weighting: Weighting,
        _filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologyGraph>, EdgeQueryError> {
        match *self {}
    }
    async fn totals(
        &self,
        _window: TimeWindow,
        _filter: &TopologyFilter,
    ) -> Result<Watermarked<EdgeTotals>, EdgeQueryError> {
        match *self {}
    }
    async fn channel_topology(
        &self,
        _window: TimeWindow,
        _weighting: Weighting,
        _filter: &TopologyFilter,
    ) -> Result<Watermarked<BipartiteGraph>, EdgeQueryError> {
        match *self {}
    }
    async fn transmissions(
        &self,
        _edge: &EdgeSelector,
        _window: TimeWindow,
        _filter: &TopologyFilter,
        _page: &PageRequest<EdgeTransmissionList>,
    ) -> Result<Watermarked<EdgeTransmissionPage>, EdgeQueryError> {
        match *self {}
    }
    async fn agent_traffic(
        &self,
        _window: TimeWindow,
        _agents: &[AgentId],
    ) -> Result<Watermarked<BTreeMap<AgentId, AgentTraffic>>, EdgeQueryError> {
        match *self {}
    }
    fn bucket_width(&self) -> BucketWidth {
        match *self {}
    }
    async fn series(
        &self,
        _grid: SeriesGrid,
        _weighting: Weighting,
        _grouping: SeriesGrouping,
        _filter: &TopologyFilter,
    ) -> Result<Watermarked<TopologySeries>, EdgeQueryError> {
        match *self {}
    }
}

impl FrontierSource for Dummy {
    async fn frontier(&self) -> Result<PipelineFrontier, EdgeError> {
        match *self {}
    }
}

fn edge_store<T: EdgeStore>(x: &mut T, never: &Dummy) {
    assert_send(x.apply(arg(never)));
    assert_send(x.judge(arg(never), arg(never), arg(never)));
    assert_send(x.version_ready(arg(never), arg(never)));
    assert_send(x.activate(arg(never)));
    assert_send(x.drop_version(arg(never)));
    assert_send(x.watermark());
    assert_send(x.advance_watermark(arg(never)));
    assert_send(x.apply_access(arg(never)));
    assert_send(x.graph(arg(never), arg(never), arg(never)));
    assert_send(x.totals(arg(never), arg(never)));
    assert_send(x.channel_topology(arg(never), arg(never), arg(never)));
    assert_send(x.transmissions(arg(never), arg(never), arg(never), arg(never)));
    assert_send(x.agent_traffic(arg(never), arg(never)));
    assert_send(x.series(arg(never), arg(never), arg(never), arg(never)));
}

fn frontier_source<T: FrontierSource>(x: &T) {
    assert_send(x.frontier());
}

#[test]
fn l5_flow_futures_are_send() {
    let _ = channel_registry::<Dummy>;
    let _ = transmission_verdicts::<Dummy>;
}

#[test]
fn l6_analysis_futures_are_send() {
    let _ = embedder::<Dummy>;
    let _ = topic_model::<Dummy>;
    let _ = layout_fitter::<Dummy>;
    let _ = topic_catalog::<Dummy>;
    let _ = search_index::<Dummy>;
    let _ = projection_store::<Dummy>;
    let _ = projection_source::<Dummy>;
    let _ = rule_context::<Dummy>;
    let _ = alert_rule_eval::<Dummy, Dummy>;
    let _ = alert_triage::<Dummy>;
    let _ = alert_rule_store::<Dummy>;
}

#[test]
fn l7_topology_futures_are_send() {
    let _ = edge_store::<Dummy>;
    let _ = frontier_source::<Dummy>;
}
