//! The topic catalog as the spec states it: the version history with each
//! version's status and retention, and the lineage from each version to the
//! next.
//!
//! - v0, the unfitted model, was active from the start until v1 was
//!   activated. Retention keeps the last [`KEEP_LAST`] versions that have
//!   been active, so v2's activation dropped it: its topics (none) and
//!   lineage stay readable, its assignments and buckets are gone.
//! - v1 was fitted six days ago and superseded by v2; an operator pinned it
//!   a day after it was activated.
//! - v2, fitted two days ago, is active.
//!
//! Each fit started twenty minutes before its topics were returned and was
//! ready and activated ten minutes after, so the version a transmission was
//! classified under ([`super::topics::version_at`]) is the active one.

use crosstalk_spec::aggregates::retention::{Pin, RetentionPolicy};
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::{
    CompletedFit, FitRecord, LineageEntry, LineageLink, TopicLineage, TopicVersionHistory,
    TopicVersionInfo, TopicVersionStatus,
};
use crosstalk_spec::support::{Similarity, Timestamp};

use crate::clock::{DAY, MINUTE, minus, plus};

use super::GenError;
use super::history::{CONFIG_AT, OPERATOR_RESEARCHER};
use super::topics::{V1_AT, V2_AT, similarity};

/// How many versions that have been active retention keeps: the spec's
/// minimum.
pub const KEEP_LAST: u32 = 2;
/// Every link of a lineage entry besides its best is at or above this.
pub const LINEAGE_FLOOR: f32 = 0.6;
/// When v1 was pinned.
pub const V1_PINNED_AT: Timestamp = plus(V1_AT, DAY);

/// The fit that was ready, and activated, at `activated`.
fn fit(activated: Timestamp, topics: usize) -> Result<CompletedFit, GenError> {
    Ok(CompletedFit {
        started_at: minus(activated, 30 * MINUTE),
        fitted_at: fitted_at(activated),
        ready_at: activated,
        topics: u32::try_from(topics).map_err(|e| GenError::invalid("topic count", e))?,
    })
}

/// When the fit activated at `activated` returned: its topics' `fitted_at`.
pub const fn fitted_at(activated: Timestamp) -> Timestamp {
    minus(activated, 10 * MINUTE)
}

fn info(version: u32, status: TopicVersionStatus) -> Result<TopicVersionInfo, GenError> {
    TopicVersionInfo::new(TopicModelVersion(version), status)
        .map_err(|e| GenError::invalid("TopicVersionInfo", e))
}

/// The history of v0, v1 and v2 with v1 pinned and v0 dropped.
pub fn history(topics: &[Topic]) -> Result<TopicVersionHistory, GenError> {
    let count = |v: u32| topics.iter().filter(|t| t.version.0 == v).count();
    let (v1, v2) = (TopicModelVersion(1), TopicModelVersion(2));
    let mut history = TopicVersionHistory::new(vec![
        info(
            0,
            TopicVersionStatus::Superseded {
                fit: FitRecord::Unfitted,
                activated_at: Some(CONFIG_AT),
                by: v1,
                superseded_at: V1_AT,
            },
        )?,
        info(
            1,
            TopicVersionStatus::Superseded {
                fit: FitRecord::Fitted(fit(V1_AT, count(1))?),
                activated_at: Some(V1_AT),
                by: v2,
                superseded_at: V2_AT,
            },
        )?,
        info(
            2,
            TopicVersionStatus::Active {
                fit: FitRecord::Fitted(fit(V2_AT, count(2))?),
                activated_at: V2_AT,
            },
        )?,
    ])
    .map_err(|e| GenError::invalid("TopicVersionHistory", e))?;
    history
        .pin(
            v1,
            Pin {
                by: OPERATOR_RESEARCHER,
                at: V1_PINNED_AT,
            },
        )
        .map_err(|e| GenError::invalid("pin", e))?;
    let policy = RetentionPolicy::new(KEEP_LAST).map_err(|e| GenError::invalid("policy", e))?;
    for version in policy.to_drop(&history) {
        history
            .mark_dropped(version, V2_AT, policy)
            .map_err(|e| GenError::invalid("mark_dropped", e))?;
    }
    Ok(history)
}

/// The lineage from `from` to `to`: for each topic of `from`, the topic of
/// `to` with the most similar centroid (ties to the lower id), then every
/// other one at or above [`LINEAGE_FLOOR`], in lineage order.
pub fn lineage(
    topics: &[Topic],
    from: TopicModelVersion,
    to: TopicModelVersion,
) -> Result<TopicLineage, GenError> {
    let similar =
        |value: f32| Similarity::new(value).map_err(|e| GenError::invalid("Similarity", e));
    let floor = similar(LINEAGE_FLOOR)?;
    let mut entries = Vec::new();
    for old in topics.iter().filter(|t| t.version == from) {
        let mut links = topics
            .iter()
            .filter(|t| t.version == to)
            .map(|t| {
                Ok(LineageLink {
                    topic: t.id,
                    similarity: similar(similarity(&old.centroid, &t.centroid))?,
                })
            })
            .collect::<Result<Vec<_>, GenError>>()?;
        links.sort_by(|a, b| {
            b.similarity
                .get()
                .total_cmp(&a.similarity.get())
                .then_with(|| a.topic.cmp(&b.topic))
        });
        let mut links = links.into_iter();
        let best = links.next();
        let others = links.filter(|link| link.similarity >= floor).collect();
        entries.push(
            LineageEntry::new(old.id, best, others)
                .map_err(|e| GenError::invalid("LineageEntry", e))?,
        );
    }
    TopicLineage::new(from, to, floor, entries).map_err(|e| GenError::invalid("TopicLineage", e))
}
