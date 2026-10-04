//! Topic stats, versions and remaps (item 7).

use crosstalk_spec::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crosstalk_spec::ids::TopicId;
use crosstalk_spec::support::{Similarity, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicVersionInfo {
    pub version: TopicModelVersion,
    pub fitted_at: Timestamp,
    pub embedding_model: EmbeddingModel,
    pub topics: u32,
    /// Pinned versions are retained regardless of the retention policy.
    pub pinned: bool,
}

/// A topic's volume in a scope. `trend` holds one count per bucket of the
/// scope's timeline, oldest first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicStats {
    /// `None` for outliers.
    pub topic: Option<TopicId>,
    pub transmissions: u64,
    pub trend: Vec<u64>,
}

/// Where a topic of one version went in the next.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TopicRemap {
    pub from: TopicId,
    /// `None` when no topic of the next version reached the threshold.
    pub to: Option<(TopicId, Similarity)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicVersionRemap {
    pub from: TopicModelVersion,
    pub to: TopicModelVersion,
    pub remaps: Vec<TopicRemap>,
}
