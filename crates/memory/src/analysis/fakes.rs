//! Deterministic test doubles of L6's computational traits. They are not
//! reference models: an embedder, a topic model and a layout are
//! computations, not stores, and their real implementations are judged by
//! their own invariants. These give the stores and the simulation inputs
//! that are stable from run to run.
//!
//! - [`FakeEmbedder`]: a bag of hashed terms, normalized, so texts sharing
//!   terms are similar.
//! - [`FakeTopicModel`]: nearest-anchor clustering, the first `k` distinct
//!   embeddings as anchors.
//! - [`FakeLayoutFitter`]: two coordinates read off each embedding and
//!   offset by the seed.
//! - [`FakeRuleContext`]: channel policies and transmission embeddings set
//!   by the test.

use std::collections::BTreeMap;
use std::num::NonZeroU16;
use std::sync::{Arc, Mutex};

use crosstalk_spec::aggregates::projection::{FitFailure, ProjectionParams};
use crosstalk_spec::aggregates::topic::{
    Assignment, Embedding, EmbeddingModel, Topic, TopicModelVersion,
};
use crosstalk_spec::derived::flow::channel::policy::Policy;
use crosstalk_spec::ids::{ChannelId, TopicId, TransmissionId};
use crosstalk_spec::interfaces::l6_analysis::{
    EmbedError, Embedder, LayoutFitter, RuleContext, TopicError, TopicModel,
};
use crosstalk_spec::support::{Clock, Similarity};

use super::search::terms;
use super::support::similarity;
use crate::support::{IdSequence, lock};

/// The model a [`FakeEmbedder`] reports by default.
pub fn fake_model(name: &str, dimension: NonZeroU16) -> EmbeddingModel {
    EmbeddingModel {
        name: name.to_owned(),
        dimension,
    }
}

/// Embeds a text as its hashed terms: each term adds 1 to the coordinate
/// its BLAKE3 hash picks, and the vector is normalized. A text without
/// terms embeds as the first unit vector. Texts longer than `max_chars`
/// are refused as `TooLong`.
#[derive(Debug, Clone)]
pub struct FakeEmbedder {
    model: EmbeddingModel,
    max_chars: usize,
}

impl FakeEmbedder {
    pub fn new(model: EmbeddingModel, max_chars: usize) -> Self {
        Self { model, max_chars }
    }

    /// The embedding of `text`, if it fits.
    pub fn embed_one(&self, text: &str) -> Result<Embedding, EmbedError> {
        let dimension = usize::from(self.model.dimension.get());
        let mut values = vec![0.0f32; dimension];
        for term in terms(text) {
            let hash = blake3::hash(term.as_bytes());
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&hash.as_bytes()[..8]);
            let slot = u64::from_le_bytes(bytes) % u64::from(self.model.dimension.get());
            let slot = usize::try_from(slot).unwrap_or(0);
            if let Some(value) = values.get_mut(slot) {
                *value += 1.0;
            }
        }
        let norm = values.iter().map(|v| v * v).sum::<f32>().sqrt();
        if norm > 0.0 {
            for value in &mut values {
                *value /= norm;
            }
        } else if let Some(first) = values.first_mut() {
            *first = 1.0;
        }
        Embedding::new(self.model.clone(), values).map_err(|error| EmbedError::Model {
            reason: format!("{error:?}"),
        })
    }
}

impl Embedder for FakeEmbedder {
    fn model(&self) -> EmbeddingModel {
        self.model.clone()
    }

    async fn embed(&self, texts: &[&str]) -> Result<Vec<Embedding>, EmbedError> {
        if let Some(index) = texts
            .iter()
            .position(|text| text.chars().count() > self.max_chars)
        {
            return Err(EmbedError::TooLong { index });
        }
        texts.iter().map(|text| self.embed_one(text)).collect()
    }
}

/// Clusters around the first `clusters` distinct embeddings of a fit; an
/// embedding whose best similarity is below `outlier_below` is an outlier.
/// Version 0, before the first fit, classifies everything as an outlier.
#[derive(Clone)]
pub struct FakeTopicModel {
    model: EmbeddingModel,
    clusters: usize,
    min_samples: u32,
    outlier_below: Similarity,
    clock: Arc<dyn Clock>,
    state: Arc<Mutex<FitState>>,
}

#[derive(Debug, Clone)]
struct FitState {
    version: TopicModelVersion,
    topics: Vec<Topic>,
    ids: IdSequence,
}

impl FakeTopicModel {
    pub fn new(
        model: EmbeddingModel,
        clusters: usize,
        min_samples: u32,
        outlier_below: Similarity,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            model,
            clusters,
            min_samples,
            outlier_below,
            clock,
            state: Arc::new(Mutex::new(FitState {
                version: TopicModelVersion(0),
                topics: Vec::new(),
                ids: IdSequence::default(),
            })),
        }
    }

    fn check_model(&self, embedding: &Embedding) -> Result<(), TopicError> {
        if *embedding.model() == self.model {
            Ok(())
        } else {
            Err(TopicError::WrongModel {
                expected: self.model.clone(),
                got: embedding.model().clone(),
            })
        }
    }
}

/// The normalized mean of `members`, or `None` when it is the zero vector.
fn centroid(model: &EmbeddingModel, members: &[&Embedding]) -> Option<Embedding> {
    let dimension = usize::from(model.dimension.get());
    let mut sum = vec![0.0f32; dimension];
    for member in members {
        for (total, value) in sum.iter_mut().zip(member.values()) {
            *total += value;
        }
    }
    let norm = sum.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm <= 0.0 {
        return None;
    }
    Embedding::new(model.clone(), sum.into_iter().map(|v| v / norm).collect()).ok()
}

/// The index of the anchor most similar to `embedding`, ties to the lower
/// index, with its similarity.
fn nearest(anchors: &[&Embedding], embedding: &Embedding) -> Option<(usize, Similarity)> {
    anchors
        .iter()
        .enumerate()
        .filter_map(|(index, anchor)| similarity(anchor, embedding).map(|s| (index, s)))
        .fold(
            None,
            |best: Option<(usize, Similarity)>, (index, s)| match best {
                Some((_, kept)) if kept.get() >= s.get() => best,
                _ => Some((index, s)),
            },
        )
}

impl TopicModel for FakeTopicModel {
    fn version(&self) -> TopicModelVersion {
        lock(&self.state).version
    }

    fn fit(&self, embeddings: &[Embedding]) -> Result<(TopicModelVersion, Vec<Topic>), TopicError> {
        for embedding in embeddings {
            self.check_model(embedding)?;
        }
        let got = u32::try_from(embeddings.len()).unwrap_or(u32::MAX);
        if got < self.min_samples {
            return Err(TopicError::TooFewSamples {
                needed: self.min_samples,
                got,
            });
        }
        let mut anchors: Vec<&Embedding> = Vec::new();
        for embedding in embeddings {
            if anchors.len() < self.clusters && !anchors.contains(&embedding) {
                anchors.push(embedding);
            }
        }
        let mut members: Vec<Vec<&Embedding>> = vec![Vec::new(); anchors.len()];
        for embedding in embeddings {
            if let Some((index, _)) = nearest(&anchors, embedding)
                && let Some(cluster) = members.get_mut(index)
            {
                cluster.push(embedding);
            }
        }
        let fitted_at = self.clock.now();
        let mut state = lock(&self.state);
        let version = TopicModelVersion(state.version.0.saturating_add(1));
        let mut topics = Vec::new();
        for (index, cluster) in members.iter().enumerate() {
            let Some(centroid) = centroid(&self.model, cluster) else {
                continue;
            };
            topics.push(Topic {
                id: TopicId::from_ulid(state.ids.next_ulid()),
                version,
                label: format!("topic {index}"),
                terms: Vec::new(),
                centroid,
                fitted_at,
            });
        }
        state.version = version;
        state.topics.clone_from(&topics);
        Ok((version, topics))
    }

    fn assign(&self, embedding: &Embedding) -> Result<Assignment, TopicError> {
        self.check_model(embedding)?;
        let state = lock(&self.state);
        let centroids: Vec<&Embedding> = state.topics.iter().map(|topic| &topic.centroid).collect();
        match nearest(&centroids, embedding) {
            Some((index, confidence)) if confidence >= self.outlier_below => {
                let topic = state
                    .topics
                    .get(index)
                    .map(|topic| topic.id)
                    .ok_or(TopicError::NotFitted)?;
                Ok(Assignment::Topic { topic, confidence })
            }
            Some(_) | None => Ok(Assignment::Outlier),
        }
    }
}

/// Lays each embedding at its first two coordinates, shifted by an offset
/// derived from the seed. Deterministic bit for bit; refuses fewer points
/// than `neighbors + 1`.
#[derive(Debug, Clone, Copy, Default)]
pub struct FakeLayoutFitter;

impl LayoutFitter for FakeLayoutFitter {
    fn fit(
        &self,
        embeddings: &[Embedding],
        params: ProjectionParams,
    ) -> Result<Vec<[f32; 2]>, FitFailure> {
        let needed = u32::from(params.neighbors()) + 1;
        let got = u64::try_from(embeddings.len()).unwrap_or(u64::MAX);
        if got < u64::from(needed) {
            return Err(FitFailure::TooFewPoints { needed, got });
        }
        // The low 16 bits of the seed, as a small exact offset.
        let offset = f32::from(u16::try_from(params.seed() & 0xffff).unwrap_or(0)) / 65_536.0;
        embeddings
            .iter()
            .map(|embedding| {
                let values = embedding.values();
                let x = values.first().copied().unwrap_or(0.0) + offset;
                let y = values.get(1).copied().unwrap_or(0.0) - offset;
                if x.is_finite() && y.is_finite() {
                    Ok([x, y])
                } else {
                    Err(FitFailure::NonFiniteLayout)
                }
            })
            .collect()
    }
}

/// What a rule may look up, set by the test.
#[derive(Debug, Clone, Default)]
pub struct FakeRuleContext {
    pub policies: BTreeMap<ChannelId, Policy>,
    pub embeddings: BTreeMap<TransmissionId, Embedding>,
}

impl RuleContext for FakeRuleContext {
    async fn channel_policy(&self, channel: ChannelId) -> Option<Policy> {
        self.policies.get(&channel).cloned()
    }

    async fn transmission_embedding(&self, transmission: TransmissionId) -> Option<Embedding> {
        self.embeddings.get(&transmission).cloned()
    }
}
