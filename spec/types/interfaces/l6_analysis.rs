//! L6 analysis: embeddings, topics, search and alert rules. Consumer groups
//! `analyze` and `alerts`.
//!
//! `analyze` is triggered by `TransmissionConfirmed`: embed the matched
//! content, index it ([`corpus::SearchCorpus::index`]), assign a topic
//! ([`lifecycle::TopicLifecycle::assign`]), publish
//! `TransmissionClassified`. The clock triggers periodic re-fits, one at a
//! time, through [`lifecycle::TopicLifecycle`]: a re-fit records its
//! version as `Fitting` in the [`TopicCatalog`] (`begin_fit`); when
//! `TopicModel::fit` returns, the catalog stores the topics and the
//! [`TopicLineage`] from the predecessor (the version before it in the
//! history) to the new version, computed from centroid similarity
//! (`complete_fit`). `analyze` then re-classifies every transmission,
//! publishes `TopicVersionReady`, and marks the version `Ready`
//! (`mark_ready`). `TopicVersionActivated` from L7 makes it `Active` and
//! supersedes the older versions (`mark_active`).
//!
//! **Retention.** The catalog applies a [`RetentionPolicy`] (configuration)
//! when it processes `TopicVersionActivated` (`mark_active`) or an unpin,
//! and `analyze` calls [`TopicCatalog::enforce_retention`] when it starts.
//! Enforcement marks every version [`RetentionPolicy::to_drop`] returns
//! dropped, freezing its all-time sizes, and only then deletes the
//! version's topic assignments, in one transaction; the catalog publishes
//! `TopicVersionDropped` (and `Changed::TopicVersion`) for each from that
//! transaction. The catalog owns the event: it is the only publisher of
//! `TopicVersionDropped`, since the drop is its decision. L7 deletes its
//! buckets on the event. Topics and lineage are kept. Pins and drops are
//! serialized in the catalog's store.
//!
//! `alerts` evaluates rules against detect and insight events and triages
//! the drafts; it suppresses alerts on `PolicyChanged` and `ChannelPromoted`
//! (sanctioned), stamping each suppression with the event's time, and on
//! `VerdictSet` keeps its copy of the transmission's current verdict
//! ([`CurrentVerdict`]) and suppresses the transmission's active alerts when
//! it is `FalseDetection` ([`AlertTriage::transmission_judged`]). On
//! `TopicVersionReady` it carries every current watched-topic rule on the
//! predecessor over with [`AlertRuleDef::remap`] (that is,
//! [`TopicLineage::remap`] over the stored lineage), which yields the rule's
//! new [`TopicWatch`]: it becomes [`TopicWatch::Stale`] exactly when the
//! lineage shows a watched topic without a successor at or above the rule's
//! threshold ([`alerts::AlertRuleMaintenance::topic_version_ready`]). When
//! it starts with an [`Embedder`] whose model differs from a current
//! semantic rule's, it marks the rule stale
//! ([`AlertRuleDef::embedding_model_changed`],
//! [`alerts::AlertRuleMaintenance::embedding_model_changed`]) before
//! evaluating any event.
//! A triage outcome that changes a stored alert (a deduplicated occurrence,
//! a suppression) publishes `AlertChanged` with the alert's next
//! `AlertRevision`; a rule going stale publishes `AlertRuleChanged` with its
//! next `RuleRevision`. The surface manages rules directly through
//! `AlertRuleStore`, acknowledges and resolves alerts through
//! [`alerts::AlertActions`], and reads rules and alerts through
//! [`alerts::AlertReads`].
//!
//! For the live feed, L6 publishes `Changed` after every committed change:
//! `Alert` for an opened, deduplicated, suppressed, acknowledged or
//! resolved alert; `Rule` for a
//! created (by an operator or config), updated, enabled, disabled or newly
//! stale rule; `TopicVersion` for each status change in the catalog and for
//! each pin, unpin and drop; and `Projection` when a projection job becomes
//! ready or fails, or its frame expires.
//!
//! Implementations:
//! - `Embedder`: `OpenAiEmbedder` (an OpenAI-compatible endpoint, decision
//!   D1).
//! - `TopicModel`: `SidecarTopicModel` (BERTopic-style: UMAP, HDBSCAN and
//!   c-TF-IDF in the Python sidecar, decision D1).
//! - `TopicCatalog`, `TopicLifecycle`: `PgTopicCatalog`.
//! - `SearchIndex`, `SearchCorpus`: `PgHybridSearch` (full-text plus
//!   pgvector).
//! - `ProjectionStore`: `PgProjectionStore` (jobs, and frames as `bytea` in
//!   the binary layout of [`crate::aggregates::projection::frame`]).
//! - `ProjectionSource`: `PgProjectionSource` (reads a fit's sample beside
//!   the embeddings).
//! - `LayoutFitter`: `SidecarLayoutFitter` (UMAP in the Python sidecar;
//!   seeded, single-threaded, so deterministic).
//! - `AlertRuleEval`: one per [`AlertRuleKind`].
//! - `AlertTriage`, `AlertRuleStore`, `AlertRuleMaintenance`,
//!   `AlertActions`, `AlertReads`: `PgAlertStore`, one transaction scope
//!   over rules, alerts and triage's verdict copy.
//!
//! Search and projection take the same [`TopologyFilter`] as the topology
//! graph and apply it as [`TopologyFilter::admits`] defines, resolving agents
//! (the transmission's and the filter's) through `AgentDirectory` and
//! channels through `ChannelDirectory` (`Route::resolved`, see
//! [`crate::aliases`]), so the views link. Both resolve the filter's topic-model version with
//! [`TopicVersionSelector::resolve`] against the catalog's history (see
//! [`crate::aggregates::filter`]). `analyze` also keeps a [`CurrentVerdict`]
//! per transmission from `VerdictSet`, from which each hit's and point's
//! `FilterSubject::false_detection` is read at query time.
//!
//! **Projection jobs.** The surface records a queued [`ProjectionInfo`]
//! (`ProjectionStore::enqueue`). A fitter loop claims the oldest queued job
//! (`claim`, which makes it `Fitting` under a lease), reads its sample
//! (`ProjectionSource::sample`), lays it out (`LayoutFitter::fit`), builds
//! the frame with [`ProjectionFrame::from_points`] and stores it
//! (`complete`, frame and `Ready` status in one transaction). A
//! [`FitFailure`] from the sample or the layout ([`LayoutError::Failed`]) is
//! recorded with `fail`; any other error ([`LayoutError::Backend`] included)
//! leaves the job to be requeued when its lease lapses. One fit
//! runs at a time per fitter.

pub mod alerts;
pub mod corpus;
pub mod lifecycle;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::aggregates::alert::{
    AlertDraft, AlertRuleKind, NotEditable, RuleName, StaleRule, TriageOutcome, UserRule,
};
#[cfg(doc)]
use crate::aggregates::alert::{
    AlertRuleConfig, AlertRuleDef, AlertRuleSet, RuleRevision, TopicWatch,
};
#[cfg(doc)]
use crate::aggregates::filter::TopicVersionSelector;
use crate::aggregates::filter::{TopologyFilter, VersionUnavailable};
use crate::aggregates::projection::frame::ProjectionFrame;
use crate::aggregates::projection::{
    FitFailure, InvalidTransition, PointRoute, Projection, ProjectionInfo, ProjectionMismatch,
    ProjectionParams, ProjectionSpec, ProjectionStatusKind,
};
use crate::aggregates::retention::{Pin, PinChange, RetentionPolicy};
use crate::aggregates::topic::{Assignment, Embedding, EmbeddingModel, Topic, TopicModelVersion};
use crate::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crate::batch::IdBatch;
use crate::derived::flow::channel::policy::Policy;
#[cfg(doc)]
use crate::derived::flow::verdict::CurrentVerdict;
use crate::derived::flow::verdict::{Verdict, VerdictRevision};
use crate::events::Envelope;
use crate::ids::{
    AgentId, AlertRuleId, ChannelId, OperatorId, ProjectionId, SinkId, TopicId, TransmissionId,
};
use crate::paging::{Page, PageRequest, ProjectionList, SearchList, TopicList};
use crate::support::{Change, NonBlank, NonEmpty, Similarity, TimeWindow, Timestamp, Watermark};

pub trait Embedder {
    fn model(&self) -> EmbeddingModel;

    fn embed(
        &self,
        texts: &[&str],
    ) -> impl Future<Output = Result<Vec<Embedding>, EmbedError>> + Send;
}

/// One document of a topic fit: a confirmed transmission's matched content
/// and its embedding. The text feeds the topics' c-TF-IDF terms.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FitDocument<'a> {
    pub text: &'a str,
    pub embedding: &'a Embedding,
}

pub trait TopicModel {
    /// The version `assign` classifies under: 0 until the first fit
    /// returns, then the version of the latest successful fit.
    fn version(&self) -> TopicModelVersion;

    /// Fit `version` from scratch over `documents` at `at`, and make it the
    /// current version. `version` is the one the catalog began
    /// (`TopicLifecycle::begin_fit`); one not above [`TopicModel::version`]
    /// is `VersionNotNewer`, changing nothing. Every returned topic has
    /// `version` and `fitted_at = at`. The caller re-assigns every
    /// transmission afterwards. A `Backend` failure changes nothing.
    fn fit(
        &self,
        version: TopicModelVersion,
        documents: &[FitDocument<'_>],
        at: Timestamp,
    ) -> impl Future<Output = Result<Vec<Topic>, TopicError>> + Send;

    fn assign(&self, embedding: &Embedding) -> Result<Assignment, TopicError>;
}

/// The record of topic-model versions, topic sizes and lineage.
pub trait TopicCatalog {
    fn versions(&self) -> impl Future<Output = Result<TopicVersionHistory, CatalogError>> + Send;

    /// Each of `version`'s topics, and its outliers, with the transmissions
    /// assigned to them under `version`; with a window, only transmissions
    /// confirmed in it; in either case only transmissions whose sender and
    /// reader resolve to different agents at the read (through
    /// `AgentDirectory`: one whose agents have since merged into one counts
    /// nowhere, `analysis.sizes.match-cross-agent-assignments`). Every topic
    /// of the version is listed once. Fails with `StillFitting` for a
    /// version that is not ready yet. For a dropped version, returns without
    /// a window the all-time sizes frozen when it was dropped (merges up to
    /// then applied), and with a window `VersionNotRetained`.
    fn sizes(
        &self,
        version: TopicModelVersion,
        window: Option<TimeWindow>,
    ) -> impl Future<Output = Result<TopicSizes, CatalogError>> + Send;

    /// The lineage from `from` to its successor: one entry per topic of
    /// `from`, whose best link is the successor's topic with the most similar
    /// centroid (ties to the lower id). `None` while `from` has no successor
    /// whose fit has returned.
    fn lineage(
        &self,
        from: TopicModelVersion,
    ) -> impl Future<Output = Result<Option<TopicLineage>, CatalogError>> + Send;

    /// The policy retention applies.
    fn retention(&self) -> RetentionPolicy;

    /// Pin `version` ([`TopicVersionHistory::pin`]): `UnknownVersion`,
    /// `StillFitting` or `VersionNotRetained` when it is unknown, fitting or
    /// dropped, changing nothing; the surface maps each with
    /// `ActionError::from` (`NotFound`, `Conflict(TopicVersionFitting)`,
    /// `Conflict(TopicVersionDropped)`). Serialized with `enforce_retention`.
    fn pin(
        &self,
        version: TopicModelVersion,
        pin: Pin,
    ) -> impl Future<Output = Result<PinChange, CatalogError>> + Send;

    /// Unpin `version` ([`TopicVersionHistory::unpin`]) as of `at`, then,
    /// when the pin changed, enforce retention at `at` as
    /// [`TopicCatalog::enforce_retention`] does, in the same transaction, so
    /// an unpinned version outside the policy is dropped.
    fn unpin(
        &self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> impl Future<Output = Result<PinChange, CatalogError>> + Send;

    /// Mark every version `RetentionPolicy::to_drop` returns dropped at `at`,
    /// freezing its all-time sizes, and delete their topic assignments, in
    /// one transaction; publish one `TopicVersionDropped` and one
    /// `Changed::TopicVersion` per version from it. Returns those versions,
    /// oldest first. A version superseded after `at` cannot be marked yet
    /// and is left for a later enforcement.
    fn enforce_retention(
        &self,
        at: Timestamp,
    ) -> impl Future<Output = Result<Vec<TopicModelVersion>, CatalogError>> + Send;

    /// `version`'s topics, newest id first. Any version whose fit has
    /// returned (ready, active or superseded) can be read: the catalog keeps
    /// every version's topics. Fails with `StillFitting` for a fitting one.
    fn topics(
        &self,
        version: TopicModelVersion,
        page: &PageRequest<TopicList>,
    ) -> impl Future<Output = Result<Page<Topic, TopicList>, CatalogError>> + Send;

    /// The stored assignments under `version` of the transmissions in
    /// `ids`, read in one snapshot: each assigned transmission's topic,
    /// `None` for an outlier. A transmission `version` holds no assignment
    /// for (classified under other versions only, or never), and every id
    /// under a version that is unknown, still fitting or dropped (its
    /// assignments deleted), is absent: the map's keys are a subset of
    /// `ids` (`analysis.catalog.assignments-as-stored`). What a row's
    /// `TopicUnder` under a version is read from, so rows agree with the
    /// graph's topic slots, search and projection samples under that
    /// version.
    fn assignments(
        &self,
        version: TopicModelVersion,
        ids: &IdBatch<TransmissionId>,
    ) -> impl Future<Output = Result<BTreeMap<TransmissionId, Option<TopicId>>, CatalogError>> + Send;
}

/// A query as the index runs it. The surface builds it from the operator's
/// text, embedding that text with the current model for the semantic and
/// hybrid modes.
#[derive(Debug, Clone, PartialEq)]
pub enum SearchQuery {
    Text(NonBlank),
    Semantic(Embedding),
    Hybrid {
        text: NonBlank,
        embedding: Embedding,
    },
}

/// A response (inside [`SearchResults`]); `score` is a [`Similarity`], a
/// finite JSON number in `0.0..=1.0`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SearchHit {
    pub transmission: TransmissionId,
    pub score: Similarity,
    pub snippet: String,
}

/// One page of hits, in descending (score, `TransmissionId`), and the
/// topic-model version the filter's topics were evaluated under: the one
/// the first page resolved, pinned by the cursor for every later page. The
/// response of `QueryApi::search`.
///
/// The only wire types of this layer are these two; the traits, the
/// in-process [`SearchQuery`], samples and every store error stay off the
/// wire (store errors reach a client as the `QueryError` they map to).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SearchResults {
    pub topic_version: TopicModelVersion,
    pub page: Page<SearchHit, SearchList>,
}

pub trait SearchIndex {
    /// Hits on confirmed transmissions whose `Confirmed::at` lies in `window`
    /// (when given) and that `filter` admits, a page at a time. The filter is
    /// applied before ranking, so every page holds admitted hits only and a
    /// full traversal lists every admitted hit once, in rank order. A hit's
    /// score depends only on the query, the embedding model and the
    /// transmission (the hybrid score is the mean of the normalized text
    /// rank and the cosine similarity), so keyset paging on (score, id) is
    /// stable under concurrent indexing. The first page resolves the
    /// filter's topic version and rejects topics outside it; the cursor pins
    /// that version and the query's embedding model.
    fn query(
        &self,
        query: &SearchQuery,
        window: Option<TimeWindow>,
        filter: &TopologyFilter,
        page: &PageRequest<SearchList>,
    ) -> impl Future<Output = Result<SearchResults, SearchError>> + Send;
}

/// Projection jobs and their stored frames. Reads and `enqueue` fail with
/// [`ProjectionStoreError`]; the fitter's calls with [`ProjectionJobError`]. See
/// [`crate::aggregates::projection`] for the lifecycle, sampling and
/// retention.
pub trait ProjectionStore {
    /// At most this many jobs are queued or fitting at once; `enqueue`
    /// beyond it is `QueueFull`.
    const MAX_PENDING: u32 = 16;

    /// Record a job made with [`ProjectionInfo::queued`]. Idempotent on its
    /// id: enqueuing the same info again changes nothing.
    fn enqueue(
        &mut self,
        job: ProjectionInfo,
    ) -> impl Future<Output = Result<(), ProjectionStoreError>> + Send;

    /// Make the oldest queued job `Fitting` as of `at` under a lease, and
    /// return it. `None` when nothing is queued.
    fn claim(
        &mut self,
        at: Timestamp,
    ) -> impl Future<Output = Result<Option<ProjectionInfo>, ProjectionJobError>> + Send;

    /// Store `frame` and make the job `Ready` in one transaction. The fit's
    /// watermark, matching and point counts are the frame header's. A job
    /// that is not fitting is `Transition`, and a frame that does not belong
    /// to the job `FrameMismatch`, changing nothing.
    fn complete(
        &mut self,
        id: ProjectionId,
        frame: ProjectionFrame,
        at: Timestamp,
    ) -> impl Future<Output = Result<(), ProjectionJobError>> + Send;

    fn fail(
        &mut self,
        id: ProjectionId,
        failure: FitFailure,
        at: Timestamp,
    ) -> impl Future<Output = Result<(), ProjectionJobError>> + Send;

    /// Return to `Queued` every fitting job whose lease lapsed before `now`.
    fn requeue_lapsed(
        &mut self,
        now: Timestamp,
    ) -> impl Future<Output = Result<u32, ProjectionJobError>> + Send;

    /// Drop the frame of every ready projection fitted more than the frame
    /// retention before `now`, making it `Expired`.
    fn expire(
        &mut self,
        now: Timestamp,
    ) -> impl Future<Output = Result<u32, ProjectionJobError>> + Send;

    fn info(
        &self,
        id: ProjectionId,
    ) -> impl Future<Output = Result<Option<ProjectionInfo>, ProjectionStoreError>> + Send;

    /// Every job, newest id first.
    fn list(
        &self,
        page: &PageRequest<ProjectionList>,
    ) -> impl Future<Output = Result<Page<ProjectionInfo, ProjectionList>, ProjectionStoreError>> + Send;

    /// A ready projection's job record and stored frame, identical on every
    /// read until it expires.
    fn projection(
        &self,
        id: ProjectionId,
    ) -> impl Future<Output = Result<Projection, ProjectionStoreError>> + Send;
}

/// One sampled transmission, as a fit reads it.
#[derive(Debug, Clone, PartialEq)]
pub struct SampleRow {
    pub transmission: TransmissionId,
    /// Canonical when the sample was read.
    pub from: AgentId,
    pub to: AgentId,
    /// The route kind and, for a channel route, the channel resolved
    /// through supersession when the sample was read.
    pub route: PointRoute,
    /// Under the spec's topic version; `None` for an outlier.
    pub topic: Option<TopicId>,
    pub confirmed_at: Timestamp,
    /// From the spec's embedding model.
    pub embedding: Embedding,
}

/// What a fit lays out.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    /// When the sample was read.
    pub watermark: Watermark,
    /// Transmissions admitted before sampling.
    pub matching: u64,
    /// At most the spec's sample size, in ascending sample-key order.
    pub rows: Vec<SampleRow>,
}

pub trait ProjectionSource {
    /// The sample of `spec` as of now: every transmission confirmed in its
    /// window that its pinned filter admits and that has an embedding from
    /// its model, reduced to the sample size by smallest sample key.
    fn sample(
        &self,
        spec: &ProjectionSpec,
    ) -> impl Future<Output = Result<Sample, SampleError>> + Send;
}

/// UMAP to two dimensions, cosine metric.
pub trait LayoutFitter {
    /// One coordinate pair per embedding, in the same order. Deterministic:
    /// the same embeddings in the same order with the same params (seed
    /// included) give the same coordinates, bit for bit. A deterministic
    /// refusal is `LayoutError::Failed`, recorded as the job's failure; a
    /// `Backend` failure leaves the job to be requeued.
    fn fit(
        &self,
        embeddings: &[Embedding],
        params: ProjectionParams,
    ) -> impl Future<Output = Result<Vec<[f32; 2]>, LayoutError>> + Send;
}

/// What a rule may look up while evaluating, beyond the event itself.
///
/// `Sync` because [`AlertRuleEval::evaluate`] borrows the context into its
/// `Send` future: a shared reference is `Send` only when its target is
/// `Sync`.
pub trait RuleContext: Sync {
    fn channel_policy(&self, channel: ChannelId) -> impl Future<Output = Option<Policy>> + Send;

    fn transmission_embedding(
        &self,
        transmission: TransmissionId,
    ) -> impl Future<Output = Option<Embedding>> + Send;
}

pub trait AlertRuleEval {
    fn kind(&self) -> AlertRuleKind;

    /// A draft's `raised_at` is the envelope's time, so evaluation is
    /// deterministic for the same envelope and context.
    fn evaluate(
        &self,
        envelope: &Envelope,
        context: &impl RuleContext,
    ) -> impl Future<Output = Option<AlertDraft>> + Send;
}

pub trait AlertTriage {
    /// Opens, deduplicates, or returns `RuleInactive` when the draft's rule
    /// no longer evaluates ([`AlertRuleDef::evaluates`], read in the same
    /// transaction), or `OperatorRejected` when the draft's subject is a
    /// transmission whose current verdict in triage's copy is
    /// `FalseDetection` (read in the same transaction).
    fn triage(
        &mut self,
        draft: AlertDraft,
    ) -> impl Future<Output = Result<TriageOutcome, TriageError>> + Send;

    /// Suppress at `at` the active alerts whose subject is `channel` or a
    /// channel it superseded (`AlertSubject::resolved`). Triggered by
    /// `PolicyChanged` and `ChannelPromoted` carrying `Sanctioned`; `at` is
    /// the event's time. Returns how many alerts it suppressed.
    fn channel_sanctioned(
        &mut self,
        channel: ChannelId,
        at: Timestamp,
    ) -> impl Future<Output = Result<u32, TriageError>> + Send;

    /// Suppress at `at` the active alerts raised by `rule`. Returns how many
    /// alerts it suppressed.
    fn rule_disabled(
        &mut self,
        rule: AlertRuleId,
        at: Timestamp,
    ) -> impl Future<Output = Result<u32, TriageError>> + Send;

    /// Record `transmission`'s verdict at `revision` in triage's copy
    /// ([`CurrentVerdict::observe`]). When the revision is newer and the
    /// verdict is `FalseDetection`, suppress every active alert whose
    /// subject is `transmission`, whatever its rule, with reason
    /// `OperatorRejected` at `at`, in the same transaction. A stale
    /// revision, a `Genuine` verdict and a withdrawal suppress nothing and
    /// reopen nothing. Triggered by `VerdictSet`, whose time is `at`;
    /// returns how many alerts it suppressed.
    fn transmission_judged(
        &mut self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
        at: Timestamp,
    ) -> impl Future<Output = Result<u32, TriageError>> + Send;
}

/// Operator management of alert rules, called by the surface. The store
/// holds an [`AlertRuleSet`]: each built-in rule exactly once, and the user
/// rules. Rules are read by the `alerts` consumer group; triage re-checks a
/// rule's status, so a change takes effect for every draft triaged after it
/// commits. Every stored change publishes one `AlertRuleChanged` with the
/// rule's next [`RuleRevision`]; an `Unchanged` result publishes nothing.
/// Rules are never deleted.
///
/// A [`UserRule`] is resolved before it is stored: a watched-topic rule
/// must name the topic-model version the alerts consumer last made current
/// (`TopicVersionNotCurrent` otherwise) and topics that exist in it
/// (`UnknownTopics`), and `None` takes the configured
/// [`AlertRuleConfig::default_remap_threshold`]; a semantic query is
/// embedded with the current model (`Embed` when that fails). Every sink
/// must be configured (`UnknownSink`).
pub trait AlertRuleStore {
    /// Create an enabled, current user rule created by `by` at `at`, under
    /// a fresh id the store assigns. Returns the id.
    fn create(
        &mut self,
        name: RuleName,
        rule: UserRule,
        sinks: Vec<SinkId>,
        by: OperatorId,
        at: Timestamp,
    ) -> impl Future<Output = Result<AlertRuleId, RuleError>> + Send;

    /// Replace a user rule's name, definition and sinks
    /// ([`AlertRuleDef::update`]): same kind only, creator kept. A stale rule
    /// is retargeted to the current version or model and enabled. Its alerts
    /// are left as they are. `NotEditable` for a built-in rule or another
    /// kind.
    fn update(
        &mut self,
        id: AlertRuleId,
        name: RuleName,
        rule: UserRule,
        sinks: Vec<SinkId>,
        by: OperatorId,
    ) -> impl Future<Output = Result<Change, RuleError>> + Send;

    /// Enable or disable any rule, built in or not
    /// ([`AlertRuleDef::set_enabled`]), as `by` at `at`. Disabling
    /// suppresses its active alerts at `at` (`AlertTriage::rule_disabled`)
    /// in the same transaction, and is allowed whether or not the rule is
    /// stale. Enabling a stale rule is refused with `Stale`, changing
    /// nothing and publishing nothing: only `update` retargets a stale rule,
    /// and it enables it too.
    fn set_enabled(
        &mut self,
        id: AlertRuleId,
        enabled: bool,
        by: OperatorId,
        at: Timestamp,
    ) -> impl Future<Output = Result<Change, RuleError>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbedError {
    Model {
        reason: String,
    },
    /// An input exceeded the model's context and could not be chunked.
    TooLong {
        index: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicError {
    NotFitted,
    WrongModel {
        expected: EmbeddingModel,
        got: EmbeddingModel,
    },
    TooFewSamples {
        needed: u32,
        got: u32,
    },
    /// `fit` of a version that is not above the current one.
    VersionNotNewer {
        current: TopicModelVersion,
        requested: TopicModelVersion,
    },
    /// The fitting service failed or could not be reached (a timeout, a
    /// transport error, a reply that breaks its contract). Not a property
    /// of the input: a later fit may succeed.
    Backend {
        reason: String,
    },
}

/// Why [`LayoutFitter::fit`] returned no layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LayoutError {
    /// The fitting service failed or could not be reached. Never recorded
    /// as a job's failure: the job is requeued when its lease lapses.
    Backend { reason: String },
    /// Fitting these embeddings with these params cannot succeed; recorded
    /// with `ProjectionStore::fail`.
    Failed(FitFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    Store {
        reason: String,
    },
    UnknownVersion(TopicModelVersion),
    StillFitting(TopicModelVersion),
    /// Retention dropped the version's assignments.
    VersionNotRetained(TopicModelVersion),
    /// A cursor the catalog did not issue, or issued for another version.
    InvalidCursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchError {
    Store {
        reason: String,
    },
    /// The query's embedding is not from the index's model: the model
    /// changed after the surface embedded the text, or during a traversal.
    WrongModel {
        index: EmbeddingModel,
        query: EmbeddingModel,
    },
    Version(VersionUnavailable),
    /// The filter lists topics that are not in the resolved version.
    TopicsNotInVersion {
        version: TopicModelVersion,
        topics: Vec<TopicId>,
    },
    InvalidCursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionStoreError {
    Store {
        reason: String,
    },
    Unknown(ProjectionId),
    /// Queued or fitting.
    NotReady {
        projection: ProjectionId,
        status: ProjectionStatusKind,
    },
    Failed {
        projection: ProjectionId,
        failure: FitFailure,
    },
    /// Its frame was dropped after the retention period.
    NotRetained(ProjectionId),
    /// [`ProjectionStore::MAX_PENDING`] jobs are already queued or fitting.
    QueueFull,
    InvalidCursor,
}

/// Why a fitter's call on a job failed. Each refusal changes nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionJobError {
    Store {
        reason: String,
    },
    Unknown(ProjectionId),
    /// `complete` or `fail` on a job not in a state that allows it.
    Transition(InvalidTransition),
    /// `complete` with a frame that does not belong to the job: its header
    /// names another projection or topic version, or its watermark, sample
    /// limit or counts disagree with the fit ([`Projection::new`]'s check).
    FrameMismatch {
        projection: ProjectionId,
        mismatch: ProjectionMismatch,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SampleError {
    Store {
        reason: String,
    },
    /// Fitting this spec cannot succeed; recorded as the job's failure.
    Failed(FitFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriageError {
    Store { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleError {
    Store {
        reason: String,
    },
    UnknownRule(AlertRuleId),
    /// An update of a built-in rule, or one changing a rule's kind.
    NotEditable(NotEditable),
    /// Enabling a stale rule; it must be updated instead.
    Stale(StaleRule),
    /// A watched-topic rule for a version that is not current.
    TopicVersionNotCurrent {
        requested: TopicModelVersion,
        current: TopicModelVersion,
    },
    /// Watched topics that do not exist in the requested version.
    UnknownTopics(NonEmpty<TopicId>),
    UnknownSink(SinkId),
    /// The semantic query text could not be embedded.
    Embed(EmbedError),
}
