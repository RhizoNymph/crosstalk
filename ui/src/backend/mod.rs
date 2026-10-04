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
use crosstalk_spec::aggregates::edge::{
    EdgeSelector, EdgeTransmissionPage, TopologyFilter, TopologyGraph, Weighting,
};
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::projection::{Projection, ProjectionInfo, ProjectionParams};
use crosstalk_spec::aggregates::quality::DetectionQuality;
use crosstalk_spec::aggregates::series::{SeriesGrid, SeriesGrouping, TopologySeries};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crosstalk_spec::aggregates::watermark::{Watermark, Watermarked};
use crosstalk_spec::derived::flow::resource::ResourcePattern;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::flow::verdict::VerdictLog;
use crosstalk_spec::ids::{AgentId, AlertId, ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::DeadLetter;
use crosstalk_spec::interfaces::l6_analysis::SearchResults;
use crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence;
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::lists::{SearchRequest, TopicPage};
use crosstalk_spec::interfaces::l8_surface::overview::OverviewCounts;
use crosstalk_spec::interfaces::l8_surface::summary::{TransmissionPage, TransmissionSelection};
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, Caller};
use crosstalk_spec::support::TimeWindow;

use crate::contract::actions::{ActionOutcome, OperatorAction};
use crate::contract::agents::{AgentDetail, AgentListFilter, AgentName, AgentSummary};
use crate::contract::alerts::Alert;
use crate::contract::channels::{
    ChannelListFilter, ChannelName, ChannelSummary, PromotionPreview, ResourceUse,
};
use crate::contract::research::{AuditEntry, AuditFilter, Operator};
use crate::contract::rules::{RuleDef, SinkInfo};
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::paging::{
    AgentList, AlertList, AuditList, ChannelList, DeadLetterList, EdgeTransmissionList, Page,
    PageRequest, ProjectionList, SearchList, TopicList, TransmissionList,
};

pub type Result<T> = std::result::Result<T, QueryError>;

/// Every read and action the UI performs. Methods return `Send` futures so
/// pages can call them from Topcoat's multi-threaded runtime.
pub trait Backend: Send + Sync + 'static {
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

    // Transmissions, evidence, verdicts and search: exactly `QueryApi`'s
    // methods.

    /// View. The transmissions `topology` counts into one of its edges for
    /// the same window and filter, newest confirmation first; the cursor
    /// pins the version the first page resolved.
    fn edge_transmissions(
        &self,
        caller: &Caller,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> impl Future<Output = Result<Watermarked<EdgeTransmissionPage>>> + Send;

    /// View. One `TransmissionSummary::of` row per stored transmission of
    /// the selection, newest id first, topics under `version` (resolved on
    /// the first page, pinned by the cursor).
    fn transmissions_by_id(
        &self,
        caller: &Caller,
        selection: &TransmissionSelection,
        version: TopicVersionSelector,
        page: &PageRequest<TransmissionList>,
    ) -> impl Future<Output = Result<TransmissionPage>> + Send;

    /// Content. The stored record, ids as stored. The evidence page reads
    /// it inside `transmission_evidence`, so only the tests call this yet.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "QueryApi's method; the evidence carries the transmission"
        )
    )]
    fn transmission(
        &self,
        caller: &Caller,
        id: TransmissionId,
    ) -> impl Future<Output = Result<Option<Transmission>>> + Send;

    /// Content. Each content match's excerpts cut with `window` from the
    /// stored bodies (a dropped body is `Excerpted::BodyDropped`), and the
    /// accesses behind the co-access records. `None` for an unknown id.
    fn transmission_evidence(
        &self,
        caller: &Caller,
        id: TransmissionId,
        window: ExcerptWindow,
    ) -> impl Future<Output = Result<Option<TransmissionEvidence>>> + Send;

    /// View. Every verdict record of the transmission, oldest first; an
    /// empty log for one never judged, `None` for an unknown one.
    fn verdicts(
        &self,
        caller: &Caller,
        transmission: TransmissionId,
    ) -> impl Future<Output = Result<Option<VerdictLog>>> + Send;

    /// Content. A page of admitted hits in rank order, for transmissions
    /// confirmed in `window` (when given) that `filter` admits.
    fn search(
        &self,
        caller: &Caller,
        request: &SearchRequest,
        window: Option<TimeWindow>,
        filter: &TopologyFilter,
        page: &PageRequest<SearchList>,
    ) -> impl Future<Output = Result<SearchResults>> + Send;

    // Topics and projections: exactly `QueryApi`'s methods.

    /// View. Every version the topic model has had, oldest first, with its
    /// status and retention. Views default to its active version.
    fn topic_versions(
        &self,
        caller: &Caller,
    ) -> impl Future<Output = Result<TopicVersionHistory>> + Send;

    /// View. Each topic of `version` (`None`: the active one) and its
    /// outliers with the transmissions assigned to them, confirmed in
    /// `window` (all time when `None`). Unknown is `NotFound`, fitting
    /// `Conflict(TopicVersionFitting)`; a dropped version answers only
    /// without a window (its frozen all-time sizes), otherwise
    /// `VersionNotRetained`.
    fn topic_sizes(
        &self,
        caller: &Caller,
        version: Option<TopicModelVersion>,
        window: Option<TimeWindow>,
    ) -> impl Future<Output = Result<Watermarked<TopicSizes>>> + Send;

    /// View. How the topics of `from` carry over to its successor; `None`
    /// while it has none. Unknown is `NotFound`.
    fn topic_lineage(
        &self,
        caller: &Caller,
        from: TopicModelVersion,
    ) -> impl Future<Output = Result<Option<TopicLineage>>> + Send;

    /// Content. A page of a version's topics, newest id first, and that
    /// version. Any version whose fit has returned, dropped ones included.
    fn topics(
        &self,
        caller: &Caller,
        version: TopicVersionSelector,
        page: &PageRequest<TopicList>,
    ) -> impl Future<Output = Result<TopicPage>> + Send;

    /// Content. Records a projection job for the window and filter (its
    /// version resolved and pinned) and returns its id at once.
    fn fit_projection(
        &self,
        caller: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
        params: ProjectionParams,
    ) -> impl Future<Output = Result<ProjectionId>> + Send;

    /// Content. A job's spec, requester and status. Unknown is `NotFound`.
    fn projection_status(
        &self,
        caller: &Caller,
        id: ProjectionId,
    ) -> impl Future<Output = Result<ProjectionInfo>> + Send;

    /// Content. Every job, newest first. No page lists jobs yet, so only
    /// the tests call this.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "QueryApi's method; no page lists jobs yet")
    )]
    fn projections(
        &self,
        caller: &Caller,
        page: &PageRequest<ProjectionList>,
    ) -> impl Future<Output = Result<Page<ProjectionInfo, ProjectionList>>> + Send;

    /// Content. A ready projection: its job record and stored frame.
    /// Queued or fitting is `Conflict(ProjectionNotReady)`, failed
    /// `Conflict(ProjectionFailed)`, expired `ProjectionNotRetained`.
    fn projection(
        &self,
        caller: &Caller,
        id: ProjectionId,
    ) -> impl Future<Output = Result<Projection>> + Send;

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

    // Research and pipeline (item 12).

    /// View. Operator verdicts tallied against the detector's calls for the
    /// judgeable transmissions opened in `window`.
    fn detection_quality(
        &self,
        caller: &Caller,
        window: TimeWindow,
    ) -> impl Future<Output = Result<DetectionQuality>> + Send;

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
