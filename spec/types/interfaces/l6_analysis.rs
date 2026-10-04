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
//! `alerts` evaluates rules against detect and insight events and triages
//! the drafts; it suppresses alerts on `PolicyChanged` (sanctioned) and
//! `TransmissionDismissed`, and on `VerdictSet` keeps its copy of the
//! transmission's current verdict ([`CurrentVerdict`]) and suppresses the
//! transmission's active alerts when it is `FalseDetection`
//! ([`AlertTriage::transmission_judged`]). On `TopicVersionReady` it carries every current
//! watched-topic rule on the predecessor over with [`TopicLineage::remap`]
//! over the stored lineage, which yields the rule's new [`TopicWatch`]: it
//! becomes [`TopicWatch::Stale`] exactly when the lineage shows a watched
//! topic without a successor at or above the rule's threshold. A triage
//! outcome that changes a stored alert (a deduplicated occurrence, a
//! suppression) publishes `AlertChanged` with the alert's next
//! `AlertRevision`. The surface manages rules directly through
//! `AlertRuleStore`.
//!
//! Implementations:
//! - `Embedder`: `LocalOnnxEmbedder`, `ApiEmbedder`.
//! - `TopicModel`: `UmapHdbscanTopics` (BERTopic-style).
//! - `TopicCatalog`: `PgTopicCatalog`.
//! - `SearchIndex`: `PgHybridSearch` (full-text plus pgvector).
//! - `ProjectionIndex`: `PgProjection` (layout coordinates stored beside the
//!   embeddings).
//! - `AlertRuleEval`: one per [`AlertRuleKind`].
//!
//! Search and projection take the same [`TopologyFilter`] as the topology
//! graph and apply it as [`TopologyFilter::admits`] defines, resolving agents
//! (the transmission's and the filter's) through `AgentDirectory` at query
//! time, so the views link. `analyze` also keeps a [`CurrentVerdict`] per
//! transmission from `VerdictSet`, from which each hit's and point's
//! `FilterSubject::false_detection` is read at query time.

use crate::aggregates::alert::{
    AlertDraft, AlertRuleKind, KindChanged, RuleStatus, TriageOutcome, WatchedTopics,
};
#[cfg(doc)]
use crate::aggregates::alert::{AlertRuleDef, ContentRule, TopicWatch};
use crate::aggregates::filter::TopologyFilter;
use crate::aggregates::projection::{Projection, ProjectionLimit};
use crate::aggregates::topic::{Assignment, Embedding, EmbeddingModel, Topic, TopicModelVersion};
use crate::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crate::derived::flow::channel::policy::Policy;
#[cfg(doc)]
use crate::derived::flow::verdict::CurrentVerdict;
use crate::derived::flow::verdict::{Verdict, VerdictRevision};
use crate::events::Envelope;
use crate::ids::{AlertRuleId, ChannelId, OperatorId, TopicId, TransmissionId};
use crate::support::{NonBlank, Similarity, TimeWindow};

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
    /// `StillFitting` for a version that is not ready yet.
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum SearchQuery {
    Text(String),
    Semantic(Embedding),
    Hybrid { text: String, embedding: Embedding },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub transmission: TransmissionId,
    pub score: Similarity,
    pub snippet: String,
}

/// Hits in descending score, and the topic-model version the filter's
/// topics were evaluated under (the active one at query time).
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResults {
    pub topic_version: TopicModelVersion,
    pub hits: Vec<SearchHit>,
}

pub trait SearchIndex {
    /// Hits on confirmed transmissions whose `Confirmed::at` lies in `window`
    /// (when given) and that `filter` admits, at most `limit` of them. The
    /// filter is applied before ranking and truncation, so a filtered query
    /// returns the best `limit` admitted hits, not the admitted part of the
    /// best `limit` hits.
    async fn query(
        &self,
        query: &SearchQuery,
        window: Option<TimeWindow>,
        filter: &TopologyFilter,
        limit: u32,
    ) -> Result<SearchResults, SearchError>;
}

/// The 2-D layout of transmission embeddings. See
/// [`crate::aggregates::projection`] for layouts, tokens and sampling.
pub trait ProjectionIndex {
    /// The current layout's points for transmissions confirmed in `window`
    /// that `filter` admits, sampled down to `limit`. Points carry canonical
    /// agents resolved at query time. Before the first topic-model fit there
    /// is no layout and the projection is empty.
    async fn project(
        &self,
        window: TimeWindow,
        filter: &TopologyFilter,
        limit: ProjectionLimit,
    ) -> Result<Projection, ProjectionError>;
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

    /// Suppress the active alerts whose subject is `channel`.
    async fn channel_sanctioned(&mut self, channel: ChannelId) -> Result<u32, TriageError>;

    /// Suppress the active alerts raised by `rule`.
    async fn rule_disabled(&mut self, rule: AlertRuleId) -> Result<u32, TriageError>;

    /// Suppress the active alerts of `SuspectedTransmission` rules whose
    /// subject is `transmission`, with reason `TransmissionDismissed`.
    /// Triggered by `TransmissionDismissed`.
    async fn transmission_dismissed(
        &mut self,
        transmission: TransmissionId,
    ) -> Result<u32, TriageError>;

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

/// A content rule as an operator asks for it. The store turns it into a
/// [`ContentRule`]: a semantic query's text is embedded with the current
/// embedding model.
#[derive(Debug, Clone, PartialEq)]
pub enum RuleRequest {
    /// `topics.version` must be the topic-model version the alerts consumer
    /// last made current, and every topic must exist in it.
    WatchedTopic {
        topics: WatchedTopics,
        remap_threshold: Similarity,
    },
    SemanticQuery {
        text: NonBlank,
        threshold: Similarity,
    },
}

/// Operator management of alert rules, called by the surface. Rules are
/// read by the `alerts` consumer group; triage re-checks a rule's status, so
/// a change takes effect for every draft triaged after it commits.
pub trait AlertRuleStore {
    /// Create a content rule with a caller-chosen id. A retry with the same
    /// id and request changes nothing; the same id with another request is
    /// `DuplicateId`.
    async fn create(
        &mut self,
        id: AlertRuleId,
        request: RuleRequest,
        status: RuleStatus,
        by: OperatorId,
    ) -> Result<(), RuleError>;

    /// Replace a rule's definition ([`AlertRuleDef::update`]): same kind
    /// only, status kept, a stale watched-topic rule made current. Its
    /// alerts are left as they are.
    async fn update(
        &mut self,
        id: AlertRuleId,
        request: RuleRequest,
        by: OperatorId,
    ) -> Result<(), RuleError>;

    /// Enable or disable any rule. Disabling suppresses its active alerts
    /// (`AlertTriage::rule_disabled`) in the same transaction. Enabling a
    /// stale rule leaves it stale. Setting the status it already has changes
    /// nothing.
    async fn set_status(
        &mut self,
        id: AlertRuleId,
        status: RuleStatus,
        by: OperatorId,
    ) -> Result<(), RuleError>;
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
    Store { reason: String },
    UnknownVersion(TopicModelVersion),
    StillFitting(TopicModelVersion),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchError {
    Store { reason: String },
    BadQuery { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionError {
    Store { reason: String },
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
    /// A create reusing an id with a different request.
    DuplicateId(AlertRuleId),
    /// An update to another kind, including any update of a rule that takes
    /// no parameters.
    KindChanged(KindChanged),
    /// A watched-topic request for a version that is not current.
    NotCurrentVersion {
        requested: TopicModelVersion,
        current: TopicModelVersion,
    },
    /// A watched topic that does not exist in the requested version.
    UnknownTopic(TopicId),
    /// The semantic query text could not be embedded.
    Embed(EmbedError),
}
