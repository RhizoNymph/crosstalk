//! L6 analysis: embeddings, topics, search and alert rules. Consumer groups
//! `analyze` and `alerts`.
//!
//! `analyze` is triggered by `TransmissionConfirmed`: embed the matched
//! content, assign a topic, publish `TransmissionClassified`. The clock
//! triggers periodic re-fits, one at a time. A re-fit records its version as
//! `Fitting` in the [`TopicCatalog`]; when `TopicModel::fit` returns, the
//! catalog stores the [`TopicLineage`] from the predecessor (the newest
//! version that is no longer fitting) to the new version, computed from
//! centroid similarity. `analyze` then re-classifies every transmission and
//! publishes `TopicVersionReady`, and the version becomes `Ready`.
//! `TopicVersionActivated` from L7 makes it `Active` and supersedes the older
//! versions.
//!
//! **Retention.** The catalog applies a [`RetentionPolicy`] (configuration):
//! after it processes `TopicVersionActivated` or an unpin, and when it
//! starts, it calls [`TopicCatalog::enforce_retention`], which marks every
//! version [`RetentionPolicy::to_drop`] returns dropped, freezing its
//! all-time sizes, and then publishes `TopicVersionDropped` for each. Only
//! after the mark does `analyze` delete the version's topic assignments; L7
//! deletes its buckets on the event. Topics and lineage are kept. Pins and
//! drops are serialized in the catalog's store.
//!
//! `alerts` evaluates rules against detect and insight events and triages
//! the drafts; it suppresses alerts on `PolicyChanged` and `ChannelPromoted`
//! (sanctioned), and on
//! `VerdictSet` keeps its copy of the transmission's current verdict
//! ([`CurrentVerdict`]) and suppresses the transmission's active alerts when
//! it is `FalseDetection` ([`AlertTriage::transmission_judged`]). On
//! `TopicVersionReady` it carries every current watched-topic rule on the
//! predecessor over with [`AlertRuleDef::remap`] (that is,
//! [`TopicLineage::remap`] over the stored lineage), which yields the rule's
//! new [`TopicWatch`]: it becomes [`TopicWatch::Stale`] exactly when the
//! lineage shows a watched topic without a successor at or above the rule's
//! threshold. When it starts with an [`Embedder`] whose model differs from a
//! current semantic rule's, it marks the rule stale
//! ([`AlertRuleDef::embedding_model_changed`]) before evaluating any event.
//! A triage outcome that changes a stored alert (a deduplicated occurrence,
//! a suppression) publishes `AlertChanged` with the alert's next
//! `AlertRevision`; a rule going stale publishes `AlertRuleChanged` with its
//! next `RuleRevision`. The surface manages rules directly through
//! `AlertRuleStore`.
//!
//! For the live feed, L6 publishes `Changed` after every committed change:
//! `Alert` for an opened, deduplicated or suppressed alert; `Rule` for a
//! created (by an operator or config), updated, enabled, disabled or newly
//! stale rule; `TopicVersion` for each status change in the catalog and for
//! each pin, unpin and drop; and `Projection` when a projection job becomes
//! ready or fails, or its frame expires.
//!
//! Implementations:
//! - `Embedder`: `LocalOnnxEmbedder`, `ApiEmbedder`.
//! - `TopicModel`: `UmapHdbscanTopics` (BERTopic-style).
//! - `TopicCatalog`: `PgTopicCatalog`.
//! - `SearchIndex`: `PgHybridSearch` (full-text plus pgvector).
//! - `ProjectionStore`: `PgProjectionStore` (jobs, and frames as `bytea` in
//!   the binary layout of [`crate::aggregates::projection::frame`]).
//! - `ProjectionSource`: `PgProjectionSource` (reads a fit's sample beside
//!   the embeddings).
//! - `LayoutFitter`: `UmapLayout` (seeded, single-threaded, so deterministic).
//! - `AlertRuleEval`: one per [`AlertRuleKind`].
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
//! [`FitFailure`] from the sample or the layout is recorded with `fail`; any
//! other error leaves the job to be requeued when its lease lapses. One fit
//! runs at a time per fitter.

use crate::aggregates::alert::{
    AlertDraft, AlertRuleKind, NotEditable, RuleName, TriageOutcome, UserRule,
};
#[cfg(doc)]
use crate::aggregates::alert::{
    AlertRuleConfig, AlertRuleDef, AlertRuleSet, RuleRevision, TopicWatch,
};
use crate::aggregates::edge::RouteKind;
#[cfg(doc)]
use crate::aggregates::filter::TopicVersionSelector;
use crate::aggregates::filter::{TopologyFilter, VersionUnavailable};
use crate::aggregates::projection::frame::ProjectionFrame;
use crate::aggregates::projection::{
    FitFailure, InvalidTransition, Projection, ProjectionInfo, ProjectionParams, ProjectionSpec,
    ProjectionStatusKind,
};
use crate::aggregates::retention::{Pin, PinChange, RetentionPolicy};
use crate::aggregates::topic::{Assignment, Embedding, EmbeddingModel, Topic, TopicModelVersion};
use crate::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
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

    async fn embed(&self, texts: &[&str]) -> Result<Vec<Embedding>, EmbedError>;
}

pub trait TopicModel {
    fn version(&self) -> TopicModelVersion;

    /// Fit a new version from scratch. The caller re-assigns every
    /// transmission afterwards.
    fn fit(&self, embeddings: &[Embedding]) -> Result<(TopicModelVersion, Vec<Topic>), TopicError>;

    fn assign(&self, embedding: &Embedding) -> Result<Assignment, TopicError>;
}

/// The record of topic-model versions, topic sizes and lineage.
pub trait TopicCatalog {
    async fn versions(&self) -> Result<TopicVersionHistory, CatalogError>;

    /// Each of `version`'s topics, and its outliers, with the transmissions
    /// assigned to them under `version`; with a window, only transmissions
    /// confirmed in it. Every topic of the version is listed once. Fails with
    /// `StillFitting` for a version that is not ready yet. For a dropped
    /// version, returns without a window the all-time sizes frozen when it
    /// was dropped, and with a window `VersionNotRetained`.
    async fn sizes(
        &self,
        version: TopicModelVersion,
        window: Option<TimeWindow>,
    ) -> Result<TopicSizes, CatalogError>;

    /// The lineage from `from` to its successor: one entry per topic of
    /// `from`, whose best link is the successor's topic with the most similar
    /// centroid (ties to the lower id). `None` while `from` has no successor
    /// whose fit has returned.
    async fn lineage(&self, from: TopicModelVersion) -> Result<Option<TopicLineage>, CatalogError>;

    /// The policy retention applies.
    fn retention(&self) -> RetentionPolicy;

    /// Pin `version` ([`TopicVersionHistory::pin`]): `UnknownVersion`,
    /// `StillFitting` or `VersionNotRetained` when it is unknown, fitting or
    /// dropped, changing nothing. Serialized with `enforce_retention`.
    async fn pin(&self, version: TopicModelVersion, pin: Pin) -> Result<PinChange, CatalogError>;

    /// Unpin `version` ([`TopicVersionHistory::unpin`]), then enforce
    /// retention, so an unpinned version outside the policy is dropped.
    async fn unpin(&self, version: TopicModelVersion) -> Result<PinChange, CatalogError>;

    /// Mark every version `RetentionPolicy::to_drop` returns dropped at `at`,
    /// freezing its all-time sizes, in one transaction. Returns those
    /// versions, oldest first; the caller then publishes one
    /// `TopicVersionDropped` per version and deletes their assignments.
    async fn enforce_retention(
        &self,
        at: Timestamp,
    ) -> Result<Vec<TopicModelVersion>, CatalogError>;

    /// `version`'s topics, newest id first. Any version whose fit has
    /// returned (ready, active or superseded) can be read: the catalog keeps
    /// every version's topics. Fails with `StillFitting` for a fitting one.
    async fn topics(
        &self,
        version: TopicModelVersion,
        page: &PageRequest<TopicList>,
    ) -> Result<Page<Topic, TopicList>, CatalogError>;
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

#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub transmission: TransmissionId,
    pub score: Similarity,
    pub snippet: String,
}

/// One page of hits, in descending (score, `TransmissionId`), and the
/// topic-model version the filter's topics were evaluated under: the one
/// the first page resolved, pinned by the cursor for every later page.
#[derive(Debug, Clone, PartialEq)]
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
    async fn query(
        &self,
        query: &SearchQuery,
        window: Option<TimeWindow>,
        filter: &TopologyFilter,
        page: &PageRequest<SearchList>,
    ) -> Result<SearchResults, SearchError>;
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
    async fn enqueue(&mut self, job: ProjectionInfo) -> Result<(), ProjectionStoreError>;

    /// Make the oldest queued job `Fitting` as of `at` under a lease, and
    /// return it. `None` when nothing is queued.
    async fn claim(&mut self, at: Timestamp) -> Result<Option<ProjectionInfo>, ProjectionJobError>;

    /// Store `frame` and make the job `Ready` in one transaction. The fit's
    /// watermark, matching and point counts are the frame header's.
    async fn complete(
        &mut self,
        id: ProjectionId,
        frame: ProjectionFrame,
        at: Timestamp,
    ) -> Result<(), ProjectionJobError>;

    async fn fail(
        &mut self,
        id: ProjectionId,
        failure: FitFailure,
        at: Timestamp,
    ) -> Result<(), ProjectionJobError>;

    /// Return to `Queued` every fitting job whose lease lapsed before `now`.
    async fn requeue_lapsed(&mut self, now: Timestamp) -> Result<u32, ProjectionJobError>;

    /// Drop the frame of every ready projection fitted more than the frame
    /// retention before `now`, making it `Expired`.
    async fn expire(&mut self, now: Timestamp) -> Result<u32, ProjectionJobError>;

    async fn info(&self, id: ProjectionId) -> Result<Option<ProjectionInfo>, ProjectionStoreError>;

    /// Every job, newest id first.
    async fn list(
        &self,
        page: &PageRequest<ProjectionList>,
    ) -> Result<Page<ProjectionInfo, ProjectionList>, ProjectionStoreError>;

    /// A ready projection's job record and stored frame, identical on every
    /// read until it expires.
    async fn projection(&self, id: ProjectionId) -> Result<Projection, ProjectionStoreError>;
}

/// One sampled transmission, as a fit reads it.
#[derive(Debug, Clone, PartialEq)]
pub struct SampleRow {
    pub transmission: TransmissionId,
    /// Canonical when the sample was read.
    pub from: AgentId,
    pub to: AgentId,
    pub route: RouteKind,
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
    async fn sample(&self, spec: &ProjectionSpec) -> Result<Sample, SampleError>;
}

/// UMAP to two dimensions, cosine metric.
pub trait LayoutFitter {
    /// One coordinate pair per embedding, in the same order. Deterministic:
    /// the same embeddings in the same order with the same params (seed
    /// included) give the same coordinates, bit for bit.
    fn fit(
        &self,
        embeddings: &[Embedding],
        params: ProjectionParams,
    ) -> Result<Vec<[f32; 2]>, FitFailure>;
}

/// What a rule may look up while evaluating, beyond the event itself.
pub trait RuleContext {
    async fn channel_policy(&self, channel: ChannelId) -> Option<Policy>;

    async fn transmission_embedding(&self, transmission: TransmissionId) -> Option<Embedding>;
}

pub trait AlertRuleEval {
    fn kind(&self) -> AlertRuleKind;

    /// A draft's `raised_at` is the envelope's time, so evaluation is
    /// deterministic for the same envelope and context.
    async fn evaluate(&self, envelope: &Envelope, context: &impl RuleContext)
    -> Option<AlertDraft>;
}

pub trait AlertTriage {
    /// Opens, deduplicates, or returns `RuleInactive` when the draft's rule
    /// no longer evaluates ([`AlertRuleDef::evaluates`], read in the same
    /// transaction), or `OperatorRejected` when the draft's subject is a
    /// transmission whose current verdict in triage's copy is
    /// `FalseDetection` (read in the same transaction).
    async fn triage(&mut self, draft: AlertDraft) -> Result<TriageOutcome, TriageError>;

    /// Suppress the active alerts whose subject is `channel` or a channel
    /// it superseded (`AlertSubject::resolved`). Triggered by
    /// `PolicyChanged` and `ChannelPromoted` carrying `Sanctioned`.
    async fn channel_sanctioned(&mut self, channel: ChannelId) -> Result<u32, TriageError>;

    /// Suppress the active alerts raised by `rule`.
    async fn rule_disabled(&mut self, rule: AlertRuleId) -> Result<u32, TriageError>;

    /// Record `transmission`'s verdict at `revision` in triage's copy
    /// ([`CurrentVerdict::observe`]). When the revision is newer and the
    /// verdict is `FalseDetection`, suppress every active alert whose
    /// subject is `transmission`, whatever its rule, with reason
    /// `OperatorRejected`, in the same transaction. A stale revision, a
    /// `Genuine` verdict and a withdrawal suppress nothing and reopen
    /// nothing. Triggered by `VerdictSet`; returns how many alerts it
    /// suppressed.
    async fn transmission_judged(
        &mut self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
    ) -> Result<u32, TriageError>;
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
    async fn create(
        &mut self,
        name: RuleName,
        rule: UserRule,
        sinks: Vec<SinkId>,
        by: OperatorId,
        at: Timestamp,
    ) -> Result<AlertRuleId, RuleError>;

    /// Replace a user rule's name, definition and sinks
    /// ([`AlertRuleDef::update`]): same kind only, creator kept. A stale rule
    /// is retargeted to the current version or model and enabled. Its alerts
    /// are left as they are. `NotEditable` for a built-in rule or another
    /// kind.
    async fn update(
        &mut self,
        id: AlertRuleId,
        name: RuleName,
        rule: UserRule,
        sinks: Vec<SinkId>,
        by: OperatorId,
    ) -> Result<Change, RuleError>;

    /// Enable or disable any rule, built in or not
    /// ([`AlertRuleDef::set_enabled`]). Disabling suppresses its active
    /// alerts (`AlertTriage::rule_disabled`) in the same transaction.
    /// Enabling a stale rule leaves it stale.
    async fn set_enabled(
        &mut self,
        id: AlertRuleId,
        enabled: bool,
        by: OperatorId,
    ) -> Result<Change, RuleError>;
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

/// Why a fitter's call on a job failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionJobError {
    Store {
        reason: String,
    },
    Unknown(ProjectionId),
    /// `complete` or `fail` on a job not in a state that allows it, or a
    /// frame that does not belong to the job.
    Transition(InvalidTransition),
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
