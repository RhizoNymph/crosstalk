//! L6 analysis: embeddings, topics, search and alert rules. Consumer groups
//! `analyze` and `alerts`.
//!
//! `analyze` is triggered by `TransmissionConfirmed`: embed the matched
//! content, assign a topic, publish `TransmissionClassified`. The clock
//! triggers periodic re-fits. `alerts` evaluates rules against detect and
//! insight events and triages the drafts.
//!
//! Implementations:
//! - `Embedder`: `LocalOnnxEmbedder`, `ApiEmbedder`.
//! - `TopicModel`: `UmapHdbscanTopics` (BERTopic-style).
//! - `SearchIndex`: `PgHybridSearch` (full-text plus pgvector).
//! - `AlertRuleEval`: one per [`AlertRuleKind`].

use crate::aggregates::alert::{AlertDraft, AlertRuleKind, TriageOutcome};
use crate::aggregates::topic::{Assignment, Embedding, EmbeddingModel, Topic, TopicModelVersion};
use crate::derived::flow::channel::policy::Policy;
use crate::events::BusEvent;
use crate::ids::{ChannelId, TransmissionId};
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
}

pub trait AlertRuleEval {
    fn kind(&self) -> AlertRuleKind;

    async fn evaluate(&self, event: &BusEvent, context: &impl RuleContext) -> Option<AlertDraft>;
}

pub trait AlertTriage {
    async fn triage(&mut self, draft: AlertDraft) -> Result<TriageOutcome, TriageError>;
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
