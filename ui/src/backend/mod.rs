//! The data source behind every page: L8's `QueryApi` and
//! `OperatorActions` plus the contract additions.
//!
//! Implementations enforce permissions themselves and return
//! `QueryError::Forbidden`; pages also check, to render the "content
//! hidden" state instead of an error.

pub mod fixture;

use std::collections::HashMap;
use std::future::Future;

use crosstalk_spec::aggregates::access::BipartiteGraph;
use crosstalk_spec::aggregates::edge::{TopologyFilter, TopologyGraph, Weighting};
use crosstalk_spec::aggregates::series::{SeriesGrid, SeriesGrouping, TopologySeries};
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::aggregates::watermark::{Watermark, Watermarked};
use crosstalk_spec::derived::flow::resource::ResourcePattern;
use crosstalk_spec::ids::{AgentId, AlertId, ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::DeadLetter;
use crosstalk_spec::interfaces::l6_analysis::SearchHit;
use crosstalk_spec::interfaces::l8_surface::overview::OverviewCounts;
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, Caller};
use crosstalk_spec::support::TimeWindow;

use crate::contract::actions::{ActionOutcome, OperatorAction};
use crate::contract::agents::{AgentDetail, AgentListFilter, AgentName, AgentSummary};
use crate::contract::alerts::Alert;
use crate::contract::channels::{
    ChannelListFilter, ChannelName, ChannelSummary, PromotionPreview, ResourceUse,
};
use crate::contract::evidence::TransmissionEvidence;
use crate::contract::graph::{TransmissionSelector, TransmissionSummary};
use crate::contract::research::{
    AuditEntry, AuditFilter, Operator, ProjectionJob, ProjectionParams, ProjectionPoints,
    QualityRow,
};
use crate::contract::rules::{RuleDef, SinkInfo};
use crate::contract::search::SearchRequest;
use crate::contract::topics::{TopicStats, TopicVersionInfo, TopicVersionRemap};
use crate::url::scope::Scope;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::paging::{
    AgentList, AlertList, AuditList, ChannelList, DeadLetterList, Page, PageRequest, SearchList,
    TransmissionList,
};

pub type Result<T> = std::result::Result<T, QueryError>;

/// Every read and action the UI performs. Methods return `Send` futures so
/// pages can call them from Topcoat's multi-threaded runtime.
pub trait Backend: Send + Sync + 'static {
    // The present (item 27). Both need `View`.

    /// The newest fitted topic-model version, which views default to. The
    /// version number is not content, so this needs only `View`.
    fn current_topic_version(
        &self,
        caller: &Caller,
    ) -> impl Future<Output = Result<TopicModelVersion>> + Send;

    // Topology, series and the overview: exactly `QueryApi`'s methods.

    /// View. L7's exposed watermark. Pages show the watermark each
    /// aggregate response carries, so only the tests call this yet.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "QueryApi's method; pages read response watermarks"
        )
    )]
    fn watermark(&self, caller: &Caller) -> impl Future<Output = Result<Watermark>> + Send;

    /// View. The graph over canonical agents for an aligned window, under
    /// the version the filter's selector resolves to, with a node per
    /// endpoint and ancestor.
    fn topology(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> impl Future<Output = Result<Watermarked<TopologyGraph>>> + Send;

    /// View. What `topology` counts for the window and filter, and the
    /// queues (open alerts, unreviewed channels) as of the read.
    fn overview(
        &self,
        caller: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> impl Future<Output = Result<Watermarked<OverviewCounts>>> + Send;

    /// View. Agents and channels as nodes, access edges, and the same
    /// transmission edges as `topology`.
    fn channel_topology(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> impl Future<Output = Result<Watermarked<BipartiteGraph>>> + Send;

    /// View. One series per group, one value per grid point, counted as
    /// `topology` counts the point's window.
    fn series(
        &self,
        caller: &Caller,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> impl Future<Output = Result<Watermarked<TopologySeries>>> + Send;

    // Transmissions (items 1, 17, 22).

    fn transmissions(
        &self,
        caller: &Caller,
        scope: &Scope,
        selector: &TransmissionSelector,
        page: &PageRequest<TransmissionList>,
    ) -> impl Future<Output = Result<Page<TransmissionSummary, TransmissionList>>> + Send;

    /// Needs `Content`.
    fn transmission(
        &self,
        caller: &Caller,
        id: TransmissionId,
    ) -> impl Future<Output = Result<Option<TransmissionEvidence>>> + Send;

    // Content (items 7, 9, 23). All need `Content`.

    fn search(
        &self,
        caller: &Caller,
        request: &SearchRequest,
        scope: &Scope,
        page: &PageRequest<SearchList>,
    ) -> impl Future<Output = Result<Page<SearchHit, SearchList>>> + Send;

    fn topic_versions(
        &self,
        caller: &Caller,
    ) -> impl Future<Output = Result<Vec<TopicVersionInfo>>> + Send;

    fn topics(
        &self,
        caller: &Caller,
        version: TopicModelVersion,
    ) -> impl Future<Output = Result<Vec<Topic>>> + Send;

    fn topic_stats(
        &self,
        caller: &Caller,
        scope: &Scope,
        buckets: std::num::NonZeroU32,
    ) -> impl Future<Output = Result<Vec<TopicStats>>> + Send;

    /// The remap from `from` to the next version, if there is one.
    fn topic_remap(
        &self,
        caller: &Caller,
        from: TopicModelVersion,
    ) -> impl Future<Output = Result<Option<TopicVersionRemap>>> + Send;

    fn fit_projection(
        &self,
        caller: &Caller,
        scope: &Scope,
        params: ProjectionParams,
    ) -> impl Future<Output = Result<ProjectionId>> + Send;

    fn projection_job(
        &self,
        caller: &Caller,
        id: ProjectionId,
    ) -> impl Future<Output = Result<ProjectionJob>> + Send;

    fn projection(
        &self,
        caller: &Caller,
        id: ProjectionId,
    ) -> impl Future<Output = Result<ProjectionPoints>> + Send;

    // Channels and agents (items 1, 5).

    fn channels(
        &self,
        caller: &Caller,
        filter: &ChannelListFilter,
        page: &PageRequest<ChannelList>,
    ) -> impl Future<Output = Result<Page<ChannelSummary, ChannelList>>> + Send;

    fn channel(
        &self,
        caller: &Caller,
        id: ChannelId,
    ) -> impl Future<Output = Result<Option<ChannelSummary>>> + Send;

    fn channel_resources(
        &self,
        caller: &Caller,
        id: ChannelId,
        window: TimeWindow,
    ) -> impl Future<Output = Result<Vec<ResourceUse>>> + Send;

    /// What `PromoteChannel` with `pattern` would do (item 26). Needs
    /// `View`; an unknown channel is `NotFound`, while the reasons it would
    /// be refused are reported in the preview.
    fn promotion_preview(
        &self,
        caller: &Caller,
        id: ChannelId,
        pattern: &ResourcePattern,
    ) -> impl Future<Output = Result<PromotionPreview>> + Send;

    fn agents(
        &self,
        caller: &Caller,
        filter: &AgentListFilter,
        page: &PageRequest<AgentList>,
    ) -> impl Future<Output = Result<Page<AgentSummary, AgentList>>> + Send;

    /// Resolves aliases: asking for a merged agent returns its canonical
    /// agent.
    fn agent(
        &self,
        caller: &Caller,
        id: AgentId,
    ) -> impl Future<Output = Result<Option<AgentDetail>>> + Send;

    // Names (item 24). Both need `View`; unknown ids are left out.

    /// Names for many agents at once, keyed by the id asked for. An alias
    /// is named by its canonical agent.
    fn agent_names(
        &self,
        caller: &Caller,
        ids: &[AgentId],
    ) -> impl Future<Output = Result<HashMap<AgentId, AgentName>>> + Send;

    /// Names for many channels at once, keyed by the id asked for. A
    /// superseded channel is named by the channel in force.
    fn channel_names(
        &self,
        caller: &Caller,
        ids: &[ChannelId],
    ) -> impl Future<Output = Result<HashMap<ChannelId, ChannelName>>> + Send;

    // Alerts and rules (items 1, 18).

    fn alerts(
        &self,
        caller: &Caller,
        filter: &AlertFilter,
        page: &PageRequest<AlertList>,
    ) -> impl Future<Output = Result<Page<Alert, AlertList>>> + Send;

    /// One alert by id (item 25).
    fn alert(
        &self,
        caller: &Caller,
        id: AlertId,
    ) -> impl Future<Output = Result<Option<Alert>>> + Send;

    fn rules(&self, caller: &Caller) -> impl Future<Output = Result<Vec<RuleDef>>> + Send;

    fn sinks(&self, caller: &Caller) -> impl Future<Output = Result<Vec<SinkInfo>>> + Send;

    // Research and pipeline (items 10, 12).

    fn detection_quality(
        &self,
        caller: &Caller,
        window: TimeWindow,
    ) -> impl Future<Output = Result<Vec<QualityRow>>> + Send;

    fn audit(
        &self,
        caller: &Caller,
        filter: &AuditFilter,
        page: &PageRequest<AuditList>,
    ) -> impl Future<Output = Result<Page<AuditEntry, AuditList>>> + Send;

    fn operators(&self, caller: &Caller) -> impl Future<Output = Result<Vec<Operator>>> + Send;

    /// Needs `Operate`.
    fn dead_letters(
        &self,
        caller: &Caller,
        page: &PageRequest<DeadLetterList>,
    ) -> impl Future<Output = Result<Page<DeadLetter, DeadLetterList>>> + Send;

    // Actions (item 13).

    fn act(
        &self,
        caller: &Caller,
        action: OperatorAction,
    ) -> impl Future<Output = Result<ActionOutcome>> + Send;
}
