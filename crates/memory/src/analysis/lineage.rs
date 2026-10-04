//! The lineage the catalog stores when a fit returns: for each topic of the
//! predecessor, every successor topic ranked by centroid similarity.

use std::cmp::Ordering;

use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::{
    InvalidLineage, InvalidLineageEntry, LineageEntry, LineageLink, TopicLineage,
};
use crosstalk_spec::support::Similarity;

use super::support::similarity;

/// The spec refused the lineage built. Never happens for the lineage built
/// here: links are sorted into lineage order and filtered at the floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineageError {
    Entry(InvalidLineageEntry),
    Lineage(InvalidLineage),
}

/// The lineage from `from` (with topics `older`) to `to` (with `newer`).
///
/// One entry per topic of `older`, ascending by id. Its best link is the
/// newer topic whose centroid is most similar ([`similarity`]), ties to the
/// lower id; the other links are every other newer topic at or above
/// `floor`, in lineage order. A newer topic whose centroid is from another
/// embedding model is never compared, so it is never linked.
pub fn lineage_between(
    from: TopicModelVersion,
    older: &[&Topic],
    to: TopicModelVersion,
    newer: &[&Topic],
    floor: Similarity,
) -> Result<TopicLineage, LineageError> {
    let mut older: Vec<&Topic> = older.to_vec();
    older.sort_by_key(|topic| topic.id);
    let mut entries = Vec::with_capacity(older.len());
    for topic in older {
        let mut links: Vec<LineageLink> = newer
            .iter()
            .filter_map(|candidate| {
                similarity(&topic.centroid, &candidate.centroid).map(|similarity| LineageLink {
                    topic: candidate.id,
                    similarity,
                })
            })
            .collect();
        links.sort_by(lineage_order);
        let mut links = links.into_iter();
        let best = links.next();
        let others: Vec<LineageLink> = links.filter(|link| link.similarity >= floor).collect();
        entries.push(LineageEntry::new(topic.id, best, others).map_err(LineageError::Entry)?);
    }
    TopicLineage::new(from, to, floor, entries).map_err(LineageError::Lineage)
}

/// Higher similarity first, ties to the lower topic id.
fn lineage_order(a: &LineageLink, b: &LineageLink) -> Ordering {
    b.similarity
        .get()
        .total_cmp(&a.similarity.get())
        .then_with(|| a.topic.cmp(&b.topic))
}
