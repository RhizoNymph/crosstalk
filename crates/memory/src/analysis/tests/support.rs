//! Test helpers for the L6 stores.

use crosstalk_spec::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crosstalk_spec::interfaces::l6_analysis::lifecycle::TopicLifecycle;
use crosstalk_spec::support::Timestamp;

use crate::model::build::{test_model, topic, topic_id, ts, unit};

pub fn model() -> EmbeddingModel {
    test_model("test")
}

/// Fit a version whose topics are `topics` (id number and centroid
/// direction), started at `at`, returned at `at + 1`, ready at `at + 2`.
pub async fn fit_ready(
    catalog: &mut impl TopicLifecycle,
    at: u64,
    topics: &[(u64, [f32; 3])],
) -> TopicModelVersion {
    let version = catalog.begin_fit(ts(at)).await.unwrap();
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
    catalog
        .complete_fit(version, fitted, ts(at + 1))
        .await
        .unwrap();
    catalog.mark_ready(version, ts(at + 2)).await.unwrap();
    version
}

/// Fit, ready and activate a version at `at + 3`.
pub async fn fit_active(
    catalog: &mut impl TopicLifecycle,
    at: u64,
    topics: &[(u64, [f32; 3])],
) -> TopicModelVersion {
    let version = fit_ready(catalog, at, topics).await;
    catalog.mark_active(version, ts(at + 3)).await.unwrap();
    version
}

pub fn at(micros: u64) -> Timestamp {
    ts(micros)
}
