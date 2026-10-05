//! [`SidecarTopicModel`]: the spec's `TopicModel` over the sidecar's
//! `/v1/topics/fit`, with local, centroid-based assignment.
//!
//! **Fit.** `fit(version, documents, at)` refuses a version not above the
//! current one, checks every embedding is from the configured model and
//! that there are enough documents, sends the embeddings and texts, checks
//! the reply against the contract, and builds one [`Topic`] per cluster:
//! the sidecar's label and terms, the centroid computed here as the
//! normalized mean of the cluster's members (`analysis.topic.centroid-mean`
//! by construction), `version`, `fitted_at = at`, and an id derived from
//! (`at`, `version`, cluster index) by [`topic_id`]. The new topics become
//! current only if no newer fit got there first.
//!
//! **Assign.** The current topic whose centroid is most similar (cosine,
//! clamped into `0..=1`, ties to the lower index), if that similarity is at
//! least the configured `outlier_below`; `Outlier` otherwise, and always
//! under version 0, which has no topics.
//!
//! The current fit lives in a `tokio::sync::watch` channel: `assign` and
//! `version` read the latest value, `fit` replaces it with a
//! compare-and-set (`send_if_modified`).

use crosstalk_spec::aggregates::topic::{
    Assignment, Embedding, EmbeddingModel, Topic, TopicModelVersion,
};
use crosstalk_spec::ids::TopicId;
use crosstalk_spec::interfaces::l6_analysis::{FitDocument, TopicError, TopicModel};
use crosstalk_spec::support::{Finite, Similarity, Timestamp};
use tokio::sync::watch;

use super::params::TopicFitParams;
use super::wire::{ErrorBody, TOPICS_FIT, TopicsFitReply, TopicsFitRequest};
use super::{ContractViolation, Outcome, SidecarClient, SidecarError};
use crate::remote::matrix::Matrix;

/// What the topic model is configured with.
#[derive(Debug, Clone, PartialEq)]
pub struct TopicModelConfig {
    /// The model every embedding must come from.
    pub model: EmbeddingModel,
    pub fit: TopicFitParams,
    /// `assign` reports `Outlier` below this similarity to the nearest
    /// centroid.
    pub outlier_below: Similarity,
}

/// The fit `assign` classifies under.
#[derive(Debug, Clone, PartialEq)]
struct Current {
    version: TopicModelVersion,
    topics: Vec<Topic>,
}

/// The spec's `TopicModel` over the topics sidecar.
#[derive(Debug)]
pub struct SidecarTopicModel {
    client: SidecarClient,
    config: TopicModelConfig,
    current: watch::Sender<Current>,
}

/// Why [`SidecarTopicModel::restore`] refused the catalog's topics.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RestoreError {
    #[error("topic {topic:?} belongs to version {got:?}, not {expected:?}")]
    ForeignTopic {
        topic: TopicId,
        expected: TopicModelVersion,
        got: TopicModelVersion,
    },
    #[error("topic {topic:?}'s centroid is from model {got:?}, not {expected:?}")]
    WrongModel {
        topic: TopicId,
        expected: EmbeddingModel,
        got: EmbeddingModel,
    },
    #[error("version 0 has no topics")]
    TopicsUnderVersionZero,
}

impl SidecarTopicModel {
    /// The unfitted model: version 0, every embedding an outlier.
    pub fn new(client: SidecarClient, config: TopicModelConfig) -> Self {
        Self {
            client,
            config,
            current: watch::Sender::new(Current {
                version: TopicModelVersion(0),
                topics: Vec::new(),
            }),
        }
    }

    /// The model as the catalog last recorded it: `version` current, with
    /// its `topics` (after a restart, before the next fit).
    pub fn restore(
        client: SidecarClient,
        config: TopicModelConfig,
        version: TopicModelVersion,
        topics: Vec<Topic>,
    ) -> Result<Self, RestoreError> {
        if version == TopicModelVersion(0) && !topics.is_empty() {
            return Err(RestoreError::TopicsUnderVersionZero);
        }
        for topic in &topics {
            if topic.version != version {
                return Err(RestoreError::ForeignTopic {
                    topic: topic.id,
                    expected: version,
                    got: topic.version,
                });
            }
            if *topic.centroid.model() != config.model {
                return Err(RestoreError::WrongModel {
                    topic: topic.id,
                    expected: config.model.clone(),
                    got: topic.centroid.model().clone(),
                });
            }
        }
        Ok(Self {
            client,
            config,
            current: watch::Sender::new(Current { version, topics }),
        })
    }

    pub fn config(&self) -> &TopicModelConfig {
        &self.config
    }

    /// The current version's topics.
    pub fn topics(&self) -> Vec<Topic> {
        self.current.borrow().topics.clone()
    }

    fn check_model(&self, embedding: &Embedding) -> Result<(), TopicError> {
        if *embedding.model() == self.config.model {
            Ok(())
        } else {
            Err(TopicError::WrongModel {
                expected: self.config.model.clone(),
                got: embedding.model().clone(),
            })
        }
    }

    /// Ask the sidecar and build the topics, without making them current.
    async fn fit_topics(
        &self,
        version: TopicModelVersion,
        documents: &[FitDocument<'_>],
        at: Timestamp,
    ) -> Result<Vec<Topic>, TopicError> {
        let request = TopicsFitRequest {
            embeddings: Matrix::encode(
                self.config.model.dimension,
                documents.iter().map(|doc| doc.embedding.values()),
            )
            .map_err(|error| {
                backend(&SidecarError::Encode {
                    route: TOPICS_FIT,
                    reason: error.to_string(),
                })
            })?,
            texts: documents.iter().map(|doc| doc.text.to_owned()).collect(),
            params: self.config.fit,
        };
        let rows = u64::try_from(documents.len()).unwrap_or(u64::MAX);
        let outcome = self
            .client
            .post::<_, TopicsFitReply>(TOPICS_FIT, &request, rows)
            .await
            .map_err(|error| backend(&error))?;
        match outcome {
            Outcome::Reply(reply) => {
                let embeddings: Vec<&Embedding> =
                    documents.iter().map(|doc| doc.embedding).collect();
                build_topics(&self.config.model, &embeddings, reply, version, at).map_err(
                    |violation| {
                        backend(&SidecarError::Contract {
                            route: TOPICS_FIT,
                            violation,
                        })
                    },
                )
            }
            Outcome::Refused(ErrorBody::TooFewSamples { needed, got }) => {
                Err(TopicError::TooFewSamples { needed, got })
            }
            Outcome::Refused(other) => Err(backend(&SidecarError::refused(TOPICS_FIT, other))),
        }
    }
}

impl TopicModel for SidecarTopicModel {
    fn version(&self) -> TopicModelVersion {
        self.current.borrow().version
    }

    async fn fit(
        &self,
        version: TopicModelVersion,
        documents: &[FitDocument<'_>],
        at: Timestamp,
    ) -> Result<Vec<Topic>, TopicError> {
        let current = self.version();
        if version <= current {
            return Err(TopicError::VersionNotNewer {
                current,
                requested: version,
            });
        }
        for doc in documents {
            self.check_model(doc.embedding)?;
        }
        let needed = self.config.fit.needed();
        let got = u32::try_from(documents.len()).unwrap_or(u32::MAX);
        if got < needed {
            return Err(TopicError::TooFewSamples { needed, got });
        }
        let topics = self.fit_topics(version, documents, at).await?;
        let mut refused = None;
        self.current.send_if_modified(|current| {
            if version > current.version {
                *current = Current {
                    version,
                    topics: topics.clone(),
                };
                true
            } else {
                refused = Some(current.version);
                false
            }
        });
        match refused {
            Some(current) => Err(TopicError::VersionNotNewer {
                current,
                requested: version,
            }),
            None => {
                tracing::info!(
                    version = version.0,
                    documents = documents.len(),
                    topics = topics.len(),
                    "topic model fitted"
                );
                Ok(topics)
            }
        }
    }

    fn assign(&self, embedding: &Embedding) -> Result<Assignment, TopicError> {
        self.check_model(embedding)?;
        let current = self.current.borrow();
        let best = current
            .topics
            .iter()
            .filter_map(|topic| cosine(&topic.centroid, embedding).map(|s| (topic.id, s)))
            .fold(
                None,
                |best: Option<(TopicId, Similarity)>, (id, s)| match best {
                    Some((_, kept)) if kept.get() >= s.get() => best,
                    _ => Some((id, s)),
                },
            );
        Ok(match best {
            Some((topic, confidence)) if confidence >= self.config.outlier_below => {
                Assignment::Topic { topic, confidence }
            }
            Some(_) | None => Assignment::Outlier,
        })
    }
}

fn backend(error: &SidecarError) -> TopicError {
    TopicError::Backend {
        reason: error.to_string(),
    }
}

/// The cosine similarity of two unit embeddings of one model, clamped into
/// `0..=1` (anything not above zero, `-0.0` included, is `+0.0`); `None`
/// across models.
pub fn cosine(a: &Embedding, b: &Embedding) -> Option<Similarity> {
    if a.model() != b.model() {
        return None;
    }
    let dot: f32 = a.values().iter().zip(b.values()).map(|(x, y)| x * y).sum();
    let clamped = if dot > 0.0 { dot.min(1.0) } else { 0.0 };
    Similarity::new(clamped).ok()
}

/// The id of cluster `index` of the fit of `version` at `at`: a ULID whose
/// time is `at` in milliseconds and whose 80 low bits are the first ten
/// bytes of `BLAKE3::derive_key("crosstalk topic id v1", version as u32 LE
/// ‖ index as u32 LE)`. The same fit always names the same topics, and two
/// fits name different ones unless 80 bits of BLAKE3 collide.
pub fn topic_id(at: Timestamp, version: TopicModelVersion, index: u32) -> TopicId {
    let mut material = [0u8; 8];
    material[..4].copy_from_slice(&version.0.to_le_bytes());
    material[4..].copy_from_slice(&index.to_le_bytes());
    let key = blake3::derive_key("crosstalk topic id v1", &material);
    let mut random = [0u8; 16];
    random[6..].copy_from_slice(&key[..10]);
    let millis = u128::from(at.as_micros() / 1_000) & ((1u128 << 48) - 1);
    TopicId::from_ulid((millis << 80) | u128::from_be_bytes(random))
}

/// The normalized mean of `members`, summed in f64 in input order; `None`
/// when it is (numerically) the zero vector.
pub fn centroid(model: &EmbeddingModel, members: &[&Embedding]) -> Option<Embedding> {
    let mut sum = vec![0.0f64; usize::from(model.dimension.get())];
    for member in members {
        for (total, value) in sum.iter_mut().zip(member.values()) {
            *total += f64::from(*value);
        }
    }
    let norm = sum.iter().map(|v| v * v).sum::<f64>().sqrt();
    if !norm.is_normal() {
        return None;
    }
    // Rounding each normalized component to the nearest f32 is the intent.
    #[allow(clippy::cast_possible_truncation)]
    let values = sum.iter().map(|v| (v / norm) as f32).collect();
    Embedding::new(model.clone(), values).ok()
}

/// Check the reply against the contract and build the topics.
pub(crate) fn build_topics(
    model: &EmbeddingModel,
    embeddings: &[&Embedding],
    reply: TopicsFitReply,
    version: TopicModelVersion,
    at: Timestamp,
) -> Result<Vec<Topic>, ContractViolation> {
    if reply.labels.len() != embeddings.len() {
        return Err(ContractViolation::LabelCount {
            expected: embeddings.len(),
            got: reply.labels.len(),
        });
    }
    let mut members: Vec<Vec<&Embedding>> = vec![Vec::new(); reply.topics.len()];
    for (index, (&label, &embedding)) in reply.labels.iter().zip(embeddings).enumerate() {
        if label == -1 {
            continue;
        }
        let cluster = usize::try_from(label)
            .ok()
            .and_then(|cluster| members.get_mut(cluster))
            .ok_or(ContractViolation::LabelOutOfRange { index, label })?;
        cluster.push(embedding);
    }
    reply
        .topics
        .into_iter()
        .zip(members)
        .enumerate()
        .map(|(index, (topic, members))| {
            if members.is_empty() {
                return Err(ContractViolation::EmptyTopic { topic: index });
            }
            let centroid = centroid(model, &members)
                .ok_or(ContractViolation::DegenerateCentroid { topic: index })?;
            let terms = terms(index, topic.terms)?;
            let cluster = u32::try_from(index).unwrap_or(u32::MAX);
            Ok(Topic {
                id: topic_id(at, version, cluster),
                version,
                label: topic.label,
                terms,
                centroid,
                fitted_at: at,
            })
        })
        .collect()
}

/// The terms, each weight a finite positive number that stays finite as an
/// `f32`.
fn terms(
    topic: usize,
    terms: Vec<(String, f64)>,
) -> Result<Vec<(String, Finite)>, ContractViolation> {
    terms
        .into_iter()
        .map(|(term, weight)| {
            // Weights are c-TF-IDF scores well inside f32's range; rounding
            // to the nearest f32 is the intent, and the check below refuses
            // anything that overflowed.
            #[allow(clippy::cast_possible_truncation)]
            let narrow = weight as f32;
            match Finite::new(narrow) {
                Ok(finite) if weight > 0.0 && narrow > 0.0 => Ok((term, finite)),
                _ => Err(ContractViolation::TermWeight {
                    topic,
                    term,
                    weight,
                }),
            }
        })
        .collect()
}
