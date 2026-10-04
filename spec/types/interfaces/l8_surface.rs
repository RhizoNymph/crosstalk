//! L8 surface: the query API, the UI's data, operator actions, the live
//! feed, the audit log and alert delivery.
//!
//! Operator actions flow back down the stack: policy changes are published
//! as `PolicyChanged` (applied by L5); agent merges, unmerges and renames go
//! to L3's identity resolver; channel promotion goes to L5
//! (`ChannelRegistry::promote`), and so do verdicts on transmissions
//! (`TransmissionVerdicts::set`); alert rule management goes to L6's
//! `AlertRuleStore`; topic-version pins go to L6's `TopicCatalog` (`pin`,
//! `unpin`). Every action names its
//! permission ([`OperatorAction::required_permission`]), checked before any
//! effect. Wherever an action records an author or time, the surface stamps
//! them from the authenticated caller and the time it accepted the action;
//! callers cannot supply them. Every action call, whatever its outcome,
//! leaves one [`audit::AuditEntry`], and so does every change config makes.
//! Acknowledging or resolving an alert changes the alert store, so the
//! surface publishes `AlertChanged` and `Changed::Alert` for it; every other
//! action's store publishes its own `Changed`.
//!
//! **Callers.** A [`Caller`] is built only by the [`operators::OperatorDirectory`]
//! for one request, from the operator config defines: in trusted mode the
//! one configured operator with every permission, otherwise the operator
//! the request's verified session names, with that operator's permissions.
//!
//! Implementations:
//! - `QueryApi`: the axum HTTP service backing `GraphView` (topology, edge
//!   share, the time brush and trend lines), `TopicHistory` (versions, sizes,
//!   lineage), `ContentExplorer` (search, topics, UMAP), the channel policy
//!   history, the operator directory and the audit log.
//! - `LiveFeed` ([`live`]): the SSE endpoint that tells the UI, by id, what
//!   to re-query.
//! - `AuditLog` ([`audit`]): `PgAuditLog`, append-only.
//! - `AlertSink`: `WebhookSink`, `SlackSink`, `LogSink`. Each configured
//!   sink has a [`SinkId`](crate::ids::SinkId) ([`sinks`]); an alert is delivered to the sinks its rule
//!   lists, and `QueryApi::sinks` reports each sink's last delivery.
//!
//! **Channels.** `channels` lists [`ChannelRow`]s (the stored channel, its
//! seed resource, and either its activity or its supersession), and
//! `channel` returns one as the head of a channel page; `channel_names`
//! names channel ids in batches; `promotion_preview` shows what
//! `PromoteChannel` would do, computed by the same `promotion::plan`
//! ([`channels`]).
//!
//! **Lists.** Channels, agents, alert rules, alerts, dead letters, the audit
//! log, the transmissions behind an edge, transmission rows by id, search
//! hits, the topics of a version and stored projections are read a page at
//! a time with the
//! cursors of [`crate::paging`], so a traversal is stable under concurrent
//! inserts. Their filters and request types are in [`lists`]. Whole values
//! with their own invariants (a policy history, the topic version history,
//! topic sizes, a lineage, a graph, a series grid, an agent's detail) are
//! returned whole.
//!
//! **Agents.** `agents` lists canonical agents only, as
//! [`AgentRow`]s: label, state, canonical parent, aliases, harness claims
//! with their last-seen times, when the agent was last seen, and its
//! transmissions in and out over the query's window, counted as its node in
//! `topology` for that window. `agent` returns one [`AgentDetail`],
//! following a merged id to its canonical agent and saying so;
//! `agent_names` names a batch of ids in one call. See
//! [`crate::aggregates::agents`].
//!
//! **Linked views.** `topology`, `channel_topology`, `series`, `search`,
//! `edge_transmissions` and `fit_projection` take the same
//! [`TopologyFilter`] and apply it as [`TopologyFilter::admits`] defines
//! (and, for the channel-centred view's accesses,
//! [`TopologyFilter::admits_access`]), so a selection in one view narrows
//! the others to the same transmissions. The filter's
//! [`TopicVersionSelector`] is resolved as [`crate::aggregates::filter`]
//! defines; each response reports the version it resolved to, and a client
//! links views by pinning that version in the others. A pinned version that
//! is unknown is `NotFound`, still fitting or never activated is a
//! `Conflict`, and no longer retained is `VersionNotRetained`. A filter
//! naming topics outside the resolved version is
//! `Conflict(TopicsNotInVersion)`.
//!
//! **Aliases.** Merged agents and superseded channels are resolved at read
//! time ([`crate::aliases`]): every id a response names is canonical, and
//! every id a request names is resolved before matching. Actions that change
//! a channel (policy, promotion) refuse a superseded one with
//! `Conflict(ChannelSuperseded)`, naming the channel to act on instead.
//!
//! **Watermarks.** `topology`, `channel_topology`, `series`, `overview`,
//! `edge_transmissions`, `channel_resources`, `channel`, `channels` and
//! `topic_sizes` return their
//! result [`Watermarked`]: with L7's watermark (`EdgeStore::watermark`),
//! read before the data. `agents` and `agent` carry the watermark read
//! before their traffic counts (`EdgeStore::agent_traffic`); their
//! identity and activity come from L3 and are not settled by it. They all count by event time (`Confirmed::at`,
//! `Access::at`), so everything in the result before the watermark is final
//! ([`crate::aggregates::watermark`]). `watermark` returns the current one,
//! and the feed reports each advance. A stored projection is not wrapped:
//! its frame is fixed when fitted and carries the watermark its sample was
//! read under (`Projection::watermark`, from `Fitted::watermark`).
//!
//! **Retention.** A topic-model version that retention has dropped
//! ([`crate::aggregates::retention`]) is `VersionNotRetained` wherever its
//! buckets or assignments would be read; its history entry, topics, lineage
//! and all-time sizes stay readable.
//!
//! **Transmissions.** `transmissions_by_id` lists a selection's rows
//! ([`summary::TransmissionSummary`]: canonical parties, resolved route,
//! state with what it knows, topic under one resolved version, current
//! verdict; no content) for View. `transmission_evidence` returns the text
//! behind one transmission ([`evidence::TransmissionEvidence`]: excerpts
//! of both sides of each content match, cut from the stored bodies as
//! [`excerpt`] defines, and the accesses behind its co-access records) for
//! Content. Neither is `Watermarked`: both read a transmission's current
//! state, not buckets. Verdicts stay in `verdicts`.
//!
//! **Projections.** `fit_projection` resolves and pins the filter's version,
//! records a queued job and returns its id at once; the fit runs in the
//! background ([`crate::aggregates::projection`]). `projection_status` and
//! `projections` report jobs; `projection` returns a ready projection's
//! stored frame, identical on every read until its frame expires.
//!
//! **Export.** `export` streams one dataset (transmissions, edge or access
//! buckets, topics, a stored projection, verdicts) between a header and a
//! trailer, reading only data settled before the watermark read at its
//! start ([`export`]). It is not paged and every export is audited.
//!
//! **The present.** `present` returns what a client needs before it can
//! build a valid request: the gateway's clock, the bucket width windows
//! align to, the export formats it writes, the topic version rules are
//! written against, the default remap threshold and the frame retention
//! ([`present`]).
//!
//! **Errors.** Every method fails with a [`QueryError`]. How each store's
//! error becomes one is defined once, by the `From` impls in
//! [`query_errors`].

pub mod actions;
pub mod audit;
pub mod channels;
pub mod errors;
pub mod evidence;
pub mod excerpt;
pub mod export;
pub mod http;
pub mod lists;
pub mod live;
pub mod operators;
pub mod overview;
pub mod permissions;
pub mod present;
pub mod query_errors;
pub mod sinks;
pub mod summary;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::aggregates::access::{BipartiteGraph, ResourceUsePage};
use crate::aggregates::agents::{AgentDetail, AgentName, AgentRow};
use crate::aggregates::alert::{Alert, AlertRuleDef};
use crate::aggregates::edge::{
    EdgeSelector, EdgeTransmissionPage, TopologyFilter, TopologyGraph, Weighting,
};
use crate::aggregates::filter::TopicVersionSelector;
use crate::aggregates::projection::{Projection, ProjectionInfo, ProjectionParams};
use crate::aggregates::quality::DetectionQuality;
use crate::aggregates::series::{SeriesGrid, SeriesGrouping, TopologySeries};
use crate::aggregates::topic::TopicModelVersion;
use crate::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crate::aggregates::watermark::{Watermark, Watermarked};
use crate::batch::IdBatch;
use crate::derived::flow::channel::policy::PolicyHistory;
use crate::derived::flow::resource::ResourcePattern;
use crate::derived::flow::transmission::Transmission;
use crate::derived::flow::verdict::VerdictLog;
use crate::ids::{AgentId, AlertId, AlertRuleId, ChannelId, ProjectionId, TransmissionId};
use crate::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crate::interfaces::l6_analysis::SearchResults;
use crate::paging::{
    AgentList, AlertList, AlertRuleList, AuditList, ChannelList, DeadLetterList,
    EdgeTransmissionList, Page, PageRequest, ProjectionList, ResourceUseList, SearchList,
    TopicList, TransmissionList,
};
use crate::support::TimeWindow;
use crate::wire::WireRequest;

use audit::{AuditEntry, AuditFilter};
use channels::{ChannelName, ChannelRow, PromotionPreview};
use evidence::TransmissionEvidence;
use excerpt::ExcerptWindow;
use export::{Export, ExportRequest, ExportStream};
use lists::{AgentFilter, AlertRuleFilter, ChannelFilter, SearchRequest, TopicPage};
use operators::Operator;
use overview::OverviewCounts;
use summary::{TransmissionPage, TransmissionSelection};

pub use actions::{ActionKind, ActionOutcome, ActionRequest, OperatorAction};
pub use errors::{ActionError, ConflictKind, InputError, QueryError};
pub use permissions::{Caller, CallerSnapshot, Permission, PermissionSet};
pub use present::Present;
pub use sinks::{AlertSink, SinkError, SinkInfo, SinkKind};

/// The policy an operator asks for. The surface stamps the author and time
/// from the authenticated caller; callers cannot supply them.
pub use crate::derived::flow::channel::policy::PolicyKind;

/// Empty `states` means every state. `channel` keeps alerts whose subject is
/// that channel or a transmission routed through it, with the listed
/// channel, the subject's channel and the transmission's route all resolved
/// through supersession: filtering on a promoted channel shows the alerts
/// still stored under the channels it superseded. A request:
/// `{"states": ["open"], "channel": null}`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct AlertFilter {
    pub states: Vec<AlertStateKind>,
    pub channel: Option<ChannelId>,
}

impl WireRequest for AlertFilter {}

/// An alert state without its data, which `AlertFilter::states` matches
/// ([`AlertState::kind`](crate::aggregates::alert::AlertState::kind)).
pub use crate::aggregates::alert::AlertStateKind;

/// Every method checks the caller's permission first and returns
/// `Forbidden` without reading anything when it is missing. List methods
/// return `InvalidCursor` for a cursor the surface did not issue or issued
/// for a different request.
pub trait QueryApi {
    /// The stream `export` returns.
    type ExportRows: ExportStream + Send + 'static;

    /// View. The channel stored under `id` as a [`ChannelRow`], the head of
    /// the channel page: a superseded id answers with its own record and its
    /// supersession (the UI's banner to the channel in force), not with the
    /// channel it resolves to. Counts are over `window` (all time when
    /// `None`), as for a `channels` row, and an unaligned window is refused
    /// as there. `None` for an unknown channel. The watermark is read from
    /// L7 before the registry and the buckets.
    fn channel(
        &self,
        caller: &Caller,
        id: ChannelId,
        window: Option<TimeWindow>,
    ) -> impl Future<Output = Result<Option<Watermarked<ChannelRow>>, QueryError>> + Send;

    /// View. Every policy decision recorded for the channel, config and
    /// operator alike, oldest first; its last entry is the channel's current
    /// policy. `None` for an unknown channel.
    fn policy_history(
        &self,
        caller: &Caller,
        channel: ChannelId,
    ) -> impl Future<Output = Result<Option<PolicyHistory>, QueryError>> + Send;

    /// View. A page of the channels `filter` matches
    /// ([`ChannelFilter::matches`]; superseded channels only when its origin
    /// filter asks for them), newest channel first, each as a
    /// [`ChannelRow`]. A row in force counts its writers and readers in
    /// `filter.window` (all time when `None`) over itself and every channel
    /// it superseded, exactly as
    /// [`ChannelCounts::tally`](channels::ChannelCounts::tally) of a full
    /// `channel_resources` traversal of the same channel and window, and
    /// its transmissions as the topology graph counts them on it for that
    /// window under `TopologyFilter::default()`
    /// ([`ChannelCounts::routed`](channels::ChannelCounts::routed), as
    /// `overview` counts active channels); a superseded row carries its
    /// supersession and no counts. A window not on bucket boundaries is
    /// `InvalidInput(UnalignedWindow)`, as for `topology`. The window never
    /// changes which channels are listed, and the cursor binds it with the
    /// rest of the filter. The watermark is read from L7 before the registry
    /// and the buckets, as for `channel_resources`.
    fn channels(
        &self,
        caller: &Caller,
        filter: &ChannelFilter,
        page: &PageRequest<ChannelList>,
    ) -> impl Future<Output = Result<Watermarked<Page<ChannelRow, ChannelList>>, QueryError>> + Send;

    /// View. For each id of `ids` that the registry knows, keyed by that
    /// id, the name of the channel it resolves to through
    /// `ChannelDirectory` (a superseded id is named by its channel in
    /// force): the channel's id and its pattern or seed locator. Unknown ids
    /// are left out. Exactly [`channels::resolve_names`] over the registered
    /// channels, ordered by id as `agent_names` is. The batch is bounded as for `agent_names`: a request with
    /// more than [`IdBatch::MAX`] distinct ids is refused before the call
    /// as `InvalidInput(TooManyIds)` (`QueryError::from(TooManyIds)`).
    fn channel_names(
        &self,
        caller: &Caller,
        ids: &IdBatch<ChannelId>,
    ) -> impl Future<Output = Result<BTreeMap<ChannelId, ChannelName>, QueryError>> + Send;

    /// View. What `PromoteChannel { channel, pattern, .. }` would do if the
    /// caller sent it now: the surface builds the declaration the action
    /// would record (the caller's operator, the time it accepted this
    /// request, `pattern`), reads `ChannelRegistry::promotion_coverage`,
    /// which runs the same `promotion::plan` over the same stored channels
    /// that `promote` would, and returns
    /// [`PromotionPreview::from_registry`] of it. The coverage is over every
    /// resource the channel and the channels it would supersede have held,
    /// not a window. A refusal maps as the action's does: superseded, not
    /// discovered and overlapping patterns are a preview with that
    /// `conflict()`; an unknown channel is `NotFound` and a pattern that
    /// misses the seed `InvalidInput(PatternMissesSeed)`.
    ///
    /// View, not Govern: the preview changes nothing and shows only
    /// structure View already shows (channel ids, resources and their
    /// locators, declared patterns through `channels`), so a reviewer
    /// without Govern can prepare a promotion for someone who has it. The
    /// action itself needs Govern.
    fn promotion_preview(
        &self,
        caller: &Caller,
        channel: ChannelId,
        pattern: &ResourcePattern,
    ) -> impl Future<Output = Result<PromotionPreview, QueryError>> + Send;

    /// View. One row per canonical agent `filter` admits
    /// ([`AgentFilter::matches`]), newest agent first ([`AgentReads::list`]);
    /// a merged agent is never a row. Each row's traffic is
    /// [`EdgeStore::agent_traffic`] over `window` for the page's agents, and
    /// the page carries the watermark that call read before its buckets.
    /// `window` restricts the counts, never the rows. An unaligned window is
    /// `InvalidInput(UnalignedWindow)`.
    ///
    /// [`AgentReads::list`]: crate::interfaces::l3_reconstruction::agents::AgentReads::list
    /// [`EdgeStore::agent_traffic`]: crate::interfaces::l7_topology::EdgeStore::agent_traffic
    fn agents(
        &self,
        caller: &Caller,
        filter: &AgentFilter,
        window: TimeWindow,
        page: &PageRequest<AgentList>,
    ) -> impl Future<Output = Result<Watermarked<Page<AgentRow, AgentList>>, QueryError>> + Send;

    /// View. The detail of the canonical agent `id` resolves to
    /// ([`AgentReads::cluster`]): the agent, its aliases, children, merge
    /// records (reverted ones with their reversal) and vetoes, with its
    /// traffic in `window` as for `agents`. A merged `id` answers for its
    /// canonical agent with `AgentLookup::Redirected { from: id }`. `None`
    /// for an unknown id.
    ///
    /// [`AgentReads::cluster`]: crate::interfaces::l3_reconstruction::agents::AgentReads::cluster
    fn agent(
        &self,
        caller: &Caller,
        id: AgentId,
        window: TimeWindow,
    ) -> impl Future<Output = Result<Option<Watermarked<AgentDetail>>, QueryError>> + Send;

    /// View. The name of each id of `ids` that names a stored agent: its
    /// canonical agent and that agent's current label, keyed by the id asked
    /// for, so an alias is named by the agent it was merged into
    /// ([`AgentReads::names`]). Unknown ids are absent from the map, not
    /// errors. The map is ordered by id, so its JSON object's keys are in
    /// ascending ULID text and one answer has one encoding. A batch is at
    /// most [`IdBatch::MAX`] distinct ids; a request
    /// with more is refused before the call as
    /// `InvalidInput(TooManyIds)` (`QueryError::from(TooManyIds)`). Labels
    /// change only with `Changed::Agent`, so names are not watermarked.
    ///
    /// [`AgentReads::names`]: crate::interfaces::l3_reconstruction::agents::AgentReads::names
    fn agent_names(
        &self,
        caller: &Caller,
        ids: &IdBatch<AgentId>,
    ) -> impl Future<Output = Result<BTreeMap<AgentId, AgentName>, QueryError>> + Send;

    /// View. Built-in rules first, in [`BuiltinRule::ALL`] order, then user
    /// rules newest first. Every rule is listed: none is ever deleted. One
    /// rule by id is `alert_rule`.
    ///
    /// [`BuiltinRule::ALL`]: crate::aggregates::alert::BuiltinRule::ALL
    fn alert_rules(
        &self,
        caller: &Caller,
        filter: &AlertRuleFilter,
        page: &PageRequest<AlertRuleList>,
    ) -> impl Future<Output = Result<Page<AlertRuleDef, AlertRuleList>, QueryError>> + Send;

    /// View. One rule, for rule pages and alert and audit links: the same
    /// value `alert_rules` lists under `id`, built-in or user, stale or
    /// not. Rule ids are never aliased and no rule is ever deleted, so
    /// `None` only for an id no rule ever had.
    fn alert_rule(
        &self,
        caller: &Caller,
        id: AlertRuleId,
    ) -> impl Future<Output = Result<Option<AlertRuleDef>, QueryError>> + Send;

    /// Govern. Every configured alert sink and how its last delivery went,
    /// for choosing a rule's sinks. Govern rather than View because a
    /// delivery error can name the sink's endpoint.
    fn sinks(
        &self,
        caller: &Caller,
    ) -> impl Future<Output = Result<Vec<SinkInfo>, QueryError>> + Send;

    /// Operate. Dead letters of one consumer group, or of every group,
    /// newest envelope first.
    fn dead_letters(
        &self,
        caller: &Caller,
        group: Option<&ConsumerGroup>,
        page: &PageRequest<DeadLetterList>,
    ) -> impl Future<Output = Result<Page<DeadLetter, DeadLetterList>, QueryError>> + Send;

    /// View. Newest alert first.
    fn alerts(
        &self,
        caller: &Caller,
        filter: &AlertFilter,
        page: &PageRequest<AlertList>,
    ) -> impl Future<Output = Result<Page<Alert, AlertList>, QueryError>> + Send;

    /// View. One alert, for alert pages and audit links: the same value
    /// `alerts` lists under `id` (its subject as raised; see
    /// [`AlertSubject`]). Alert ids are never aliased. `None` for an
    /// unknown id.
    ///
    /// [`AlertSubject`]: crate::aggregates::alert::AlertSubject
    fn alert(
        &self,
        caller: &Caller,
        id: AlertId,
    ) -> impl Future<Output = Result<Option<Alert>, QueryError>> + Send;

    /// View. L7's exposed watermark (`EdgeStore::watermark`).
    fn watermark(
        &self,
        caller: &Caller,
    ) -> impl Future<Output = Result<Watermark, QueryError>> + Send;

    /// View. Where the gateway is now and what it is configured with
    /// ([`present`]): its wall clock, L7's bucket width (equal to
    /// `EdgeStore::bucket_width`), the export formats it writes in offer
    /// order, the topic-model version watched-topic rules are written
    /// against (the one `CreateRule` and `UpdateRule` check), the default
    /// remap threshold and the projection frame retention. Reads no
    /// buckets, so it is not `Watermarked`.
    fn present(&self, caller: &Caller) -> impl Future<Output = Result<Present, QueryError>> + Send;

    /// View. Exactly [`EdgeStore::graph`], under the version the filter's
    /// selector resolves to.
    ///
    /// [`EdgeStore::graph`]: crate::interfaces::l7_topology::EdgeStore::graph
    fn topology(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> impl Future<Output = Result<Watermarked<TopologyGraph>, QueryError>> + Send;

    /// View. The overview's counts ([`overview`]) without paging any list:
    /// the activity `topology` counts for the same window and filter
    /// ([`EdgeStore::totals`]: transmissions, matched bytes, active
    /// channels, and the resolved topic version), and the queues as of the
    /// read (open alerts, unreviewed channels), which no window or filter
    /// narrows. The watermark is read before anything else and governs the
    /// activity; the queues have no settling point. Fails as `topology`
    /// does (`InvalidInput(UnalignedWindow)`, the topic version's errors,
    /// `Conflict(TopicsNotInVersion)`).
    ///
    /// [`EdgeStore::totals`]: crate::interfaces::l7_topology::EdgeStore::totals
    fn overview(
        &self,
        caller: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> impl Future<Output = Result<Watermarked<OverviewCounts>, QueryError>> + Send;

    /// View. Exactly [`EdgeStore::channel_topology`]: agents and channels as
    /// nodes, access edges (writes nobody read included) and the same
    /// transmission edges as `topology`, with the watermark read before the
    /// buckets. An unaligned window is `InvalidInput(UnalignedWindow)`.
    ///
    /// [`EdgeStore::channel_topology`]: crate::interfaces::l7_topology::EdgeStore::channel_topology
    fn channel_topology(
        &self,
        caller: &Caller,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> impl Future<Output = Result<Watermarked<BipartiteGraph>, QueryError>> + Send;

    /// View. Exactly [`ChannelRegistry::resource_use`]: the resources of
    /// `channel`'s canonical channel accessed in `window`, newest first, with
    /// canonical writers and readers. A superseded `channel` answers for the
    /// channel that superseded it, named in the page. Unknown is `NotFound`.
    /// The surface reads L7's watermark (`EdgeStore::watermark`) before the
    /// registry: accesses are stored by event time, so every access before it
    /// is already counted.
    ///
    /// [`ChannelRegistry::resource_use`]: crate::interfaces::l5_flow::ChannelRegistry::resource_use
    fn channel_resources(
        &self,
        caller: &Caller,
        channel: ChannelId,
        window: TimeWindow,
        page: &PageRequest<ResourceUseList>,
    ) -> impl Future<Output = Result<Watermarked<ResourceUsePage>, QueryError>> + Send;

    /// View. The transmissions `topology` counts into one of its edges for
    /// the same window and filter (`EdgeStore::transmissions`): ids, times,
    /// byte counts and topic ids, no content. Content is behind
    /// `transmission`, `transmission_evidence` and `search`.
    fn edge_transmissions(
        &self,
        caller: &Caller,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> impl Future<Output = Result<Watermarked<EdgeTransmissionPage>, QueryError>> + Send;

    /// View. One [`TransmissionSummary::of`] row per transmission of
    /// `selection` (a lasso or a search's hits), newest id first; ids of no
    /// stored transmission are left out. Topics are read under `version`,
    /// resolved on the first page as a linked view resolves it (errors as
    /// for any linked view; the catalog's retention decides what is
    /// retained) and pinned by the cursor. No window and no filter: the
    /// selection came from a view that applied them. Not `Watermarked`:
    /// rows are each transmission's current state. A request the surface
    /// cannot build a selection from is refused before the call
    /// (`QueryError::from(InvalidSelection)`): no ids is
    /// `InvalidInput(EmptySelection)`, more than
    /// [`TransmissionSelection::MAX`] distinct ids
    /// `InvalidInput(TooManyIds)`.
    ///
    /// [`TransmissionSummary::of`]: summary::TransmissionSummary::of
    fn transmissions_by_id(
        &self,
        caller: &Caller,
        selection: &TransmissionSelection,
        version: TopicVersionSelector,
        page: &PageRequest<TransmissionList>,
    ) -> impl Future<Output = Result<TransmissionPage, QueryError>> + Send;

    /// View. Exactly [`EdgeStore::series`], under the version the filter's
    /// selector resolves to; a grid for another bucket width is
    /// `InvalidInput(BucketWidthMismatch)`, like an unaligned graph window.
    ///
    /// [`EdgeStore::series`]: crate::interfaces::l7_topology::EdgeStore::series
    fn series(
        &self,
        caller: &Caller,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> impl Future<Output = Result<Watermarked<TopologySeries>, QueryError>> + Send;

    /// View.
    fn topic_versions(
        &self,
        caller: &Caller,
    ) -> impl Future<Output = Result<TopicVersionHistory, QueryError>> + Send;

    /// View. `None` is the active version. An unknown version is
    /// `NotFound`; a fitting one is `Conflict(TopicVersionFitting)`. A
    /// dropped version answers without a window with its frozen all-time
    /// sizes, and with a window `VersionNotRetained`. The watermark is read
    /// from L7 before the catalog.
    fn topic_sizes(
        &self,
        caller: &Caller,
        version: Option<TopicModelVersion>,
        window: Option<TimeWindow>,
    ) -> impl Future<Output = Result<Watermarked<TopicSizes>, QueryError>> + Send;

    /// View. The lineage from `from` to its successor; `None` while it has
    /// none. An unknown version is `NotFound`.
    fn topic_lineage(
        &self,
        caller: &Caller,
        from: TopicModelVersion,
    ) -> impl Future<Output = Result<Option<TopicLineage>, QueryError>> + Send;

    /// Content. Embeds `request`'s text with the current embedding model
    /// for the semantic and hybrid modes, then runs [`SearchIndex::query`]: a
    /// page of admitted hits in rank order. Text too long to embed is
    /// `InvalidInput(QueryTooLong)`; a page after an embedding-model change
    /// is `Conflict(EmbeddingModelChanged)`.
    ///
    /// [`SearchIndex::query`]: crate::interfaces::l6_analysis::SearchIndex::query
    fn search(
        &self,
        caller: &Caller,
        request: &SearchRequest,
        window: Option<TimeWindow>,
        filter: &TopologyFilter,
        page: &PageRequest<SearchList>,
    ) -> impl Future<Output = Result<SearchResults, QueryError>> + Send;

    /// Content. The stored record, ids as stored.
    fn transmission(
        &self,
        caller: &Caller,
        id: TransmissionId,
    ) -> impl Future<Output = Result<Option<Transmission>, QueryError>> + Send;

    /// Content. The text behind a transmission
    /// ([`TransmissionEvidence::assemble`]): for each content match the
    /// sender's and the reader's excerpt, cut with `window` from the stored
    /// bodies ([`Excerpted::of`]), and the access and resource behind each
    /// access its co-access records name, with the access's canonical
    /// agent. A body content retention dropped is
    /// [`Excerpted::BodyDropped`], and the rest is still returned. `None`
    /// for an unknown id. A record the transmission names that cannot be
    /// read is `Store` ([`evidence::EvidenceError`]). A window over
    /// [`ExcerptWindow::MAX_CONTEXT`] is refused before the call as
    /// `InvalidInput(ExcerptContextTooLong)` (`QueryError::from(InvalidWindow)`).
    ///
    /// [`Excerpted::of`]: excerpt::Excerpted::of
    /// [`Excerpted::BodyDropped`]: excerpt::Excerpted::BodyDropped
    fn transmission_evidence(
        &self,
        caller: &Caller,
        id: TransmissionId,
        window: ExcerptWindow,
    ) -> impl Future<Output = Result<Option<TransmissionEvidence>, QueryError>> + Send;

    /// Content. A version's topics, newest id first, and the version they
    /// belong to. `Current` is the catalog's active version; a pinned one may
    /// be any version whose fit has returned (unlike a linked view, it need
    /// not have been activated): unknown is `NotFound`, still fitting is
    /// `Conflict(TopicVersionFitting)`.
    fn topics(
        &self,
        caller: &Caller,
        version: TopicVersionSelector,
        page: &PageRequest<TopicList>,
    ) -> impl Future<Output = Result<TopicPage, QueryError>> + Send;

    /// Content. Validate and record a projection job, and return its id
    /// without waiting for the fit. Resolves the filter's version (errors as
    /// for any linked view) and pins it, and records the current embedding
    /// model, `params` (seed included), the caller and the time. Fails with
    /// `Conflict(ProjectionQueueFull)` when
    /// [`ProjectionStore::MAX_PENDING`] jobs are pending. Each call records
    /// a new job.
    ///
    /// [`ProjectionStore::MAX_PENDING`]: crate::interfaces::l6_analysis::ProjectionStore::MAX_PENDING
    fn fit_projection(
        &self,
        caller: &Caller,
        window: TimeWindow,
        filter: &TopologyFilter,
        params: ProjectionParams,
    ) -> impl Future<Output = Result<ProjectionId, QueryError>> + Send;

    /// Content. A job's spec, requester and status. Unknown is `NotFound`.
    fn projection_status(
        &self,
        caller: &Caller,
        id: ProjectionId,
    ) -> impl Future<Output = Result<ProjectionInfo, QueryError>> + Send;

    /// Content. Every job, newest first.
    fn projections(
        &self,
        caller: &Caller,
        page: &PageRequest<ProjectionList>,
    ) -> impl Future<Output = Result<Page<ProjectionInfo, ProjectionList>, QueryError>> + Send;

    /// Content. A ready projection: its job record and stored frame, the
    /// same on every call. Unknown is `NotFound`; queued or fitting is
    /// `Conflict(ProjectionNotReady)`; failed is `Conflict(ProjectionFailed)`;
    /// expired is `ProjectionNotRetained`.
    fn projection(
        &self,
        caller: &Caller,
        id: ProjectionId,
    ) -> impl Future<Output = Result<Projection, QueryError>> + Send;

    /// View. Every verdict record of the transmission, oldest first
    /// (`TransmissionVerdicts::log`); its last record is the current
    /// verdict. An empty log for a transmission never judged, `None` for an
    /// unknown one. Records hold ids, verdicts, times and operator notes, no
    /// message content.
    fn verdicts(
        &self,
        caller: &Caller,
        transmission: TransmissionId,
    ) -> impl Future<Output = Result<Option<VerdictLog>, QueryError>> + Send;

    /// View. Operator verdicts tallied against the detector's calls for the
    /// judgeable transmissions opened in `window`
    /// (`TransmissionVerdicts::quality`; see [`crate::aggregates::quality`]).
    /// Rows hold route kinds, match classes and counts only.
    fn detection_quality(
        &self,
        caller: &Caller,
        window: TimeWindow,
    ) -> impl Future<Output = Result<DetectionQuality, QueryError>> + Send;

    /// Audit. The audit entries `filter` matches, operator and config
    /// alike, newest first by time and id (`AuditLog::query`).
    fn audit(
        &self,
        caller: &Caller,
        filter: &AuditFilter,
        page: &PageRequest<AuditList>,
    ) -> impl Future<Output = Result<Page<AuditEntry, AuditList>, QueryError>> + Send;

    /// View. Every operator the directory holds, by id: the ones config
    /// defines now, and every one it defined before, listed with no
    /// permissions, so past decisions and audit entries can still show a
    /// name (`OperatorDirectory::operators`).
    fn operators(
        &self,
        caller: &Caller,
    ) -> impl Future<Output = Result<Vec<Operator>, QueryError>> + Send;

    /// View, or Content when `request` includes content or names a
    /// projection ([`ExportRequest::required_permission`]); without it,
    /// `Forbidden { missing }` before anything is read. A format outside
    /// `present`'s `export_formats` is then `InvalidInput(UnsupportedFormat)`
    /// ([`ExportFormats::check`](export::ExportFormats::check)), also before
    /// anything is read. Reads L7's
    /// watermark first, then plans the export (`ExportSource::plan`): the
    /// filter's version resolved and pinned as for any linked view (errors
    /// as for one), the window cut at the watermark, agent and channel
    /// resolution and current verdicts captured for the whole export, the
    /// rows counted. More rows than `ExportLimits::max_rows` is
    /// `Conflict(ExportTooLarge)`; an unaligned window for edges or
    /// accesses is `InvalidInput(UnalignedWindow)`; a projection that is
    /// unknown, not ready, failed or expired fails as `projection` does.
    /// Returns the header and the stream of rows, which always ends with a
    /// trailer, `Complete` or recording why it failed. Every call is
    /// audited ([`export::record`]): `Started` is appended before the
    /// header is returned, and a failed append fails the call with `Store`.
    fn export(
        &self,
        caller: &Caller,
        request: &ExportRequest,
    ) -> impl Future<Output = Result<Export<Self::ExportRows>, QueryError>> + Send;
}

pub trait OperatorActions {
    /// Check the permission, apply the action and record it. A client
    /// sends an [`ActionRequest`], never an action: the HTTP layer decodes
    /// it (`decode_request`) and stamps it with the caller
    /// ([`ActionRequest::into_action`]) before calling `act`. `SetPolicy` on
    /// a superseded channel is refused with `Conflict(ChannelSuperseded)`
    /// (read through `ChannelDirectory`) before `PolicyChanged` is
    /// published. Every refusal of the layer that owns an action's effect
    /// maps through one `ActionError::from` ([`query_errors`]):
    /// `RegistryError` for `SetPolicy`, `PromoteError` for
    /// `PromoteChannel`, `ResolveError` for merges, unmerges and renames,
    /// `VerdictError` for `SetVerdict`, `RuleError` for rule management,
    /// `CatalogError` and `PinError` for pins. A call that returns `Ok` or an `ActionError` other than `Store` leaves exactly one
    /// operator audit entry, whose outcome is what it returns
    /// (`AuditOutcome::of`): a `Succeeded` entry is written in the same
    /// transaction as the action's effect, and a `Forbidden` or `Rejected`
    /// one with no effect. A `Store` error had no effect and leaves at most
    /// one entry, written when the audit log is still reachable.
    fn act(
        &self,
        caller: &Caller,
        action: OperatorAction,
    ) -> impl Future<Output = Result<ActionOutcome, ActionError>> + Send;
}
