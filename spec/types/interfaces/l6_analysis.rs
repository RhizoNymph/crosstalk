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
//! versions. `alerts` evaluates rules against detect and insight events and
//! triages the drafts; on `TopicVersionReady` it remaps every watched-topic
//! rule on the predecessor with [`TopicLineage::remap`] over the stored
//! lineage, so a rule becomes `Stale` exactly when the lineage shows a
//! watched topic without a successor above the rule's threshold.
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
//! time, so the views link.

use crate::aggregates::alert::{AlertDraft, AlertRuleKind, TriageOutcome};
use crate::aggregates::filter::TopologyFilter;
use crate::aggregates::projection::{Projection, ProjectionLimit};
use crate::aggregates::topic::{Assignment, Embedding, EmbeddingModel, Topic, TopicModelVersion};
use crate::aggregates::topic_history::{TopicLineage, TopicSizes, TopicVersionHistory};
use crate::derived::flow::channel::policy::Policy;
use crate::events::Envelope;
use crate::ids::{AlertRuleId, ChannelId, TransmissionId};
use crate::support::{Similarity, TimeWindow};

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
    async fn triage(&mut self, draft: AlertDraft) -> Result<TriageOutcome, TriageError>;

    /// Suppress the active alerts whose subject is `channel`.
    async fn channel_sanctioned(&mut self, channel: ChannelId) -> Result<u32, TriageError>;

    /// Suppress the active alerts raised by `rule`.
    async fn rule_disabled(&mut self, rule: AlertRuleId) -> Result<u32, TriageError>;
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
