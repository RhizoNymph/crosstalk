//! L6 analysis: embeddings, topics, search and alert rules. Consumer groups
//! `analyze` and `alerts`.
//!
//! `analyze` is triggered by `TransmissionConfirmed`: embed the matched
//! content, assign a topic, publish `TransmissionClassified`. The clock
//! triggers periodic re-fits. `alerts` evaluates rules against detect and
//! insight events and triages the drafts; it suppresses alerts on
//! `PolicyChanged` (sanctioned) and `TransmissionDismissed`. The surface
//! manages rules directly through `AlertRuleStore`.
//!
//! Implementations:
//! - `Embedder`: `LocalOnnxEmbedder`, `ApiEmbedder`.
//! - `TopicModel`: `UmapHdbscanTopics` (BERTopic-style).
//! - `SearchIndex`: `PgHybridSearch` (full-text plus pgvector).
//! - `AlertRuleEval`: one per [`AlertRuleKind`].

use crate::aggregates::alert::{
    AlertDraft, AlertRuleKind, KindChanged, RuleStatus, TriageOutcome, WatchedTopics,
};
#[cfg(doc)]
use crate::aggregates::alert::{AlertRuleDef, ContentRule};
use crate::aggregates::topic::{Assignment, Embedding, EmbeddingModel, Topic, TopicModelVersion};
use crate::derived::flow::channel::policy::Policy;
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

pub trait SearchIndex {
    async fn query(
        &self,
        query: &SearchQuery,
        window: Option<TimeWindow>,
        limit: u32,
    ) -> Result<Vec<SearchHit>, SearchError>;
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
    /// transaction).
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
pub enum SearchError {
    Store { reason: String },
    BadQuery { reason: String },
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
