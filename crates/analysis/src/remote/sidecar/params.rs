//! The topic fit's parameters, checked as the sidecar checks them, so a
//! request the adapter builds is never `invalid_request` for its params.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

/// How the sidecar fits topics: UMAP reduction, HDBSCAN clustering and
/// c-TF-IDF terms. On the wire, the request's `params` object.
///
/// Built only through [`TopicFitParams::new`]; decoding goes through it too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawTopicFitParams")]
pub struct TopicFitParams {
    seed: u64,
    min_cluster_size: u32,
    min_samples: Option<NonZeroU32>,
    umap_neighbors: u16,
    umap_components: u16,
    top_terms: u16,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawTopicFitParams {
    seed: u64,
    min_cluster_size: u32,
    min_samples: Option<NonZeroU32>,
    umap_neighbors: u16,
    umap_components: u16,
    top_terms: u16,
}

impl TryFrom<RawTopicFitParams> for TopicFitParams {
    type Error = InvalidTopicFitParams;

    fn try_from(raw: RawTopicFitParams) -> Result<Self, Self::Error> {
        Self::new(
            raw.seed,
            raw.min_cluster_size,
            raw.min_samples,
            raw.umap_neighbors,
            raw.umap_components,
            raw.top_terms,
        )
    }
}

/// A parameter outside the range the contract allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidTopicFitParams {
    #[error("min_cluster_size {got} is below {min}")]
    MinClusterSize { min: u32, got: u32 },
    #[error("umap_neighbors {got} is outside {min}..={max}")]
    Neighbors { min: u16, max: u16, got: u16 },
    #[error("umap_components {got} is outside {min}..={max}")]
    Components { min: u16, max: u16, got: u16 },
    #[error("top_terms {got} is outside {min}..={max}")]
    TopTerms { min: u16, max: u16, got: u16 },
}

impl TopicFitParams {
    pub const MIN_CLUSTER_SIZE: u32 = 2;
    pub const MIN_NEIGHBORS: u16 = 2;
    pub const MAX_NEIGHBORS: u16 = 200;
    pub const MIN_COMPONENTS: u16 = 1;
    pub const MAX_COMPONENTS: u16 = 100;
    pub const MIN_TOP_TERMS: u16 = 1;
    pub const MAX_TOP_TERMS: u16 = 50;

    /// `min_samples` `None` is HDBSCAN's default (`min_cluster_size`).
    pub fn new(
        seed: u64,
        min_cluster_size: u32,
        min_samples: Option<NonZeroU32>,
        umap_neighbors: u16,
        umap_components: u16,
        top_terms: u16,
    ) -> Result<Self, InvalidTopicFitParams> {
        if min_cluster_size < Self::MIN_CLUSTER_SIZE {
            return Err(InvalidTopicFitParams::MinClusterSize {
                min: Self::MIN_CLUSTER_SIZE,
                got: min_cluster_size,
            });
        }
        if !(Self::MIN_NEIGHBORS..=Self::MAX_NEIGHBORS).contains(&umap_neighbors) {
            return Err(InvalidTopicFitParams::Neighbors {
                min: Self::MIN_NEIGHBORS,
                max: Self::MAX_NEIGHBORS,
                got: umap_neighbors,
            });
        }
        if !(Self::MIN_COMPONENTS..=Self::MAX_COMPONENTS).contains(&umap_components) {
            return Err(InvalidTopicFitParams::Components {
                min: Self::MIN_COMPONENTS,
                max: Self::MAX_COMPONENTS,
                got: umap_components,
            });
        }
        if !(Self::MIN_TOP_TERMS..=Self::MAX_TOP_TERMS).contains(&top_terms) {
            return Err(InvalidTopicFitParams::TopTerms {
                min: Self::MIN_TOP_TERMS,
                max: Self::MAX_TOP_TERMS,
                got: top_terms,
            });
        }
        Ok(Self {
            seed,
            min_cluster_size,
            min_samples,
            umap_neighbors,
            umap_components,
            top_terms,
        })
    }

    pub fn seed(self) -> u64 {
        self.seed
    }

    pub fn min_cluster_size(self) -> u32 {
        self.min_cluster_size
    }

    pub fn min_samples(self) -> Option<NonZeroU32> {
        self.min_samples
    }

    pub fn umap_neighbors(self) -> u16 {
        self.umap_neighbors
    }

    pub fn umap_components(self) -> u16 {
        self.umap_components
    }

    pub fn top_terms(self) -> u16 {
        self.top_terms
    }

    /// The fewest documents a fit takes: `max(min_cluster_size,
    /// umap_neighbors + 1, umap_components + 2)`.
    pub fn needed(self) -> u32 {
        self.min_cluster_size
            .max(u32::from(self.umap_neighbors) + 1)
            .max(u32::from(self.umap_components) + 2)
    }
}

impl Default for TopicFitParams {
    /// Seed 0, clusters of at least 10, UMAP to 5 dimensions over 15
    /// neighbours, 10 terms per topic.
    fn default() -> Self {
        Self {
            seed: 0,
            min_cluster_size: 10,
            min_samples: None,
            umap_neighbors: 15,
            umap_components: 5,
            top_terms: 10,
        }
    }
}
