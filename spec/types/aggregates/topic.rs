//! Embeddings and topics over transmission content.

use std::num::NonZeroU16;

use crate::ids::{TopicId, TransmissionId};
use crate::support::{Similarity, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EmbeddingModel {
    pub name: String,
    pub dimension: NonZeroU16,
}

/// A vector from one embedding model. Vectors from different models are
/// never compared.
///
/// Built only through [`Embedding::new`]: `values` has the model's dimension
/// and an L2 norm within [`Embedding::NORM_TOLERANCE`] of 1.
#[derive(Debug, Clone, PartialEq)]
pub struct Embedding {
    model: EmbeddingModel,
    values: Vec<f32>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InvalidEmbedding {
    WrongDimension { expected: u16, got: usize },
    NotNormalized { norm: f32 },
}

impl Embedding {
    pub const NORM_TOLERANCE: f32 = 1e-3;

    pub fn new(model: EmbeddingModel, values: Vec<f32>) -> Result<Self, InvalidEmbedding> {
        if values.len() != usize::from(model.dimension.get()) {
            return Err(InvalidEmbedding::WrongDimension {
                expected: model.dimension.get(),
                got: values.len(),
            });
        }
        let norm = values.iter().map(|v| v * v).sum::<f32>().sqrt();
        if norm.is_nan() || (1.0 - norm).abs() > Self::NORM_TOLERANCE {
            return Err(InvalidEmbedding::NotNormalized { norm });
        }
        Ok(Self { model, values })
    }

    pub fn model(&self) -> &EmbeddingModel {
        &self.model
    }

    pub fn values(&self) -> &[f32] {
        &self.values
    }
}

/// One fit of the topic model. Topic ids are only meaningful within the fit
/// that produced them; a re-cluster produces a new version. Version 0 is the
/// unfitted model: active from the start, it classifies every transmission
/// as an outlier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicModelVersion(pub u32);

#[derive(Debug, Clone, PartialEq)]
pub struct Topic {
    pub id: TopicId,
    pub version: TopicModelVersion,
    pub label: String,
    /// Top c-TF-IDF terms, highest weight first.
    pub terms: Vec<(String, f32)>,
    /// The mean of its members' embeddings, normalized. Compared across
    /// versions to build the topic lineage, from which watched topics are
    /// remapped.
    pub centroid: Embedding,
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
