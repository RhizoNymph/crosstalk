//! Embeddings and topics over transmission content.

use crate::ids::{TopicId, TransmissionId};
use crate::support::{Similarity, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EmbeddingModel(pub String);

/// A vector from one embedding model. Vectors from different models are
/// never compared.
///
/// Invariant: `values.len()` is the model's dimension, and the vector is
/// L2-normalized.
#[derive(Debug, Clone, PartialEq)]
pub struct Embedding {
    pub model: EmbeddingModel,
    pub values: Vec<f32>,
}

/// One fit of the topic model. Topic ids are only meaningful within the fit
/// that produced them; a re-cluster produces a new version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicModelVersion(pub u32);

#[derive(Debug, Clone, PartialEq)]
pub struct Topic {
    pub id: TopicId,
    pub version: TopicModelVersion,
    pub label: String,
    /// Top c-TF-IDF terms, highest weight first.
    pub terms: Vec<(String, f32)>,
    pub fitted_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicAssignment {
    pub transmission: TransmissionId,
    pub version: TopicModelVersion,
    pub assignment: Assignment,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Assignment {
    Topic {
        topic: TopicId,
        confidence: Similarity,
    },
    Outlier,
}
