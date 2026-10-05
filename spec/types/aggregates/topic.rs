//! Embeddings and topics over transmission content.

use std::num::NonZeroU16;

use serde::{Deserialize, Serialize};

use crate::ids::{TopicId, TransmissionId};
use crate::support::{Finite, Similarity, Timestamp};
use crate::wire::{Rejected, WireRequest};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct EmbeddingModel {
    pub name: String,
    pub dimension: NonZeroU16,
}

/// A vector from one embedding model. Vectors from different models are
/// never compared.
///
/// Built only through [`Embedding::new`]: `values` has the model's dimension
/// and an L2 norm within [`Embedding::NORM_TOLERANCE`] of 1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawEmbedding")]
pub struct Embedding {
    model: EmbeddingModel,
    values: Vec<f32>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InvalidEmbedding {
    WrongDimension { expected: u16, got: usize },
    NotNormalized { norm: f32 },
}

/// [`Embedding`]'s fields, decoded without the checks. Decoding goes
/// through [`Embedding::new`], which also refuses NaN and infinite values
/// (their norm is not within the tolerance of 1), including a number too
/// large for an `f32`, which decodes to infinity.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawEmbedding {
    model: EmbeddingModel,
    values: Vec<f32>,
}

impl TryFrom<RawEmbedding> for Embedding {
    type Error = Rejected<InvalidEmbedding>;

    fn try_from(raw: RawEmbedding) -> Result<Self, Self::Error> {
        Self::new(raw.model, raw.values).map_err(|error| Rejected::new("embedding", error))
    }
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
/// as an outlier. On the wire, the number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TopicModelVersion(pub u32);

/// A client names a version: `topic_sizes` and `topic_lineage` take one, and
/// so do the pin and unpin actions. Any number decodes; an unknown version
/// is the query's `NotFound`, not a decode error.
impl WireRequest for TopicModelVersion {}

/// A response: `QueryApi::topics` pages them. On the wire, `terms` is an
/// array of `[term, weight]` pairs, each weight a finite JSON number.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Topic {
    pub id: TopicId,
    pub version: TopicModelVersion,
    pub label: String,
    /// Top c-TF-IDF terms, highest weight first. A weight is never NaN or
    /// infinite, so it always encodes as a JSON number.
    pub terms: Vec<(String, Finite)>,
    /// The mean of its members' embeddings, normalized. Compared across
    /// versions to build the topic lineage, from which watched topics are
    /// remapped.
    pub centroid: Embedding,
    pub fitted_at: Timestamp,
}

/// Not wire data: what the topic model returns in process. The bus carries
/// a transmission's topic as `TransmissionClassified`'s `Classification`.
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
