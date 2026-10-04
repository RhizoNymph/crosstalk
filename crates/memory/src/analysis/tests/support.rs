//! Test helpers for the L6 stores.

use crosstalk_spec::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crosstalk_spec::support::Timestamp;

use crate::analysis::catalog::InMemoryTopicCatalog;
use crate::model::build::{test_model, topic, topic_id, ts, unit};

pub fn model() -> EmbeddingModel {
    test_model("test")
}

/// Fit a version whose topics are `topics` (id number and centroid
/// direction), started at `at`, returned at `at + 1`, ready at `at + 2`.
pub fn fit_ready(
    catalog: &InMemoryTopicCatalog,
    at: u64,
    topics: &[(u64, [f32; 3])],
) -> TopicModelVersion {
    let version = catalog.begin_fit(ts(at)).unwrap();
    let fitted: Vec<_> = topics
        .iter()
        .map(|(id, [x, y, z])| {
            topic(
                topic_id(*id),
                version,
                unit(&model(), *x, *y, *z).unwrap(),
                ts(at + 1),
            )
        })
        .collect();
    catalog.fit_returned(version, fitted, ts(at + 1)).unwrap();
    catalog.ready(version, ts(at + 2)).unwrap();
    version
}

/// Fit, ready and activate a version at `at + 3`.
pub fn fit_active(
    catalog: &InMemoryTopicCatalog,
    at: u64,
    topics: &[(u64, [f32; 3])],
) -> TopicModelVersion {
    let version = fit_ready(catalog, at, topics);
    catalog.activated(version, ts(at + 3)).unwrap();
    version
}

pub fn at(micros: u64) -> Timestamp {
    ts(micros)
}
