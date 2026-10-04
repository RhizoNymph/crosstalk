//! `SidecarTopicModel` against a fake sidecar.

use std::time::Duration;

use crosstalk_spec::aggregates::topic::{Assignment, Embedding, TopicModelVersion};
use crosstalk_spec::interfaces::l6_analysis::{FitDocument, TopicError, TopicModel};
use crosstalk_spec::support::Similarity;
use crosstalk_testkit::upstream::{Fault, Reply};
use hyper::StatusCode;
use proptest::prelude::*;
use serde_json::json;

use super::{
    client_for, client_with_timeout, fake, model, other_model, raw, request_json, ts, unit, unit_of,
};
use crate::remote::sidecar::params::TopicFitParams;
use crate::remote::sidecar::topics::{SidecarTopicModel, TopicModelConfig, centroid, topic_id};

/// Clusters of 2, UMAP over 2 neighbours to 1 dimension: 3 documents.
fn params() -> TopicFitParams {
    TopicFitParams::new(7, 2, None, 2, 1, 5).unwrap()
}

fn config(outlier_below: f32) -> TopicModelConfig {
    TopicModelConfig {
        model: model(),
        fit: params(),
        outlier_below: Similarity::new(outlier_below).unwrap(),
    }
}

fn documents<'a>(embeddings: &'a [Embedding], texts: &'a [&'a str]) -> Vec<FitDocument<'a>> {
    embeddings
        .iter()
        .zip(texts)
        .map(|(embedding, text)| FitDocument { text, embedding })
        .collect()
}

/// Four documents: two near x, one near y, one outlier.
fn corpus() -> (Vec<Embedding>, Vec<&'static str>) {
    (
        vec![
            unit(1.0, 0.1, 0.0),
            unit(0.0, 1.0, 0.0),
            unit(1.0, -0.1, 0.0),
            unit(0.0, 0.0, 1.0),
        ],
        vec!["wiki page", "deploy job", "wiki edit", "lunch"],
    )
}

fn two_topic_reply() -> serde_json::Value {
    json!({
        "labels": [0, 1, 0, -1],
        "topics": [
            {"label": "wiki, page, edit", "terms": [["wiki", 0.5], ["page", 0.25], ["edit", 0.25]]},
            {"label": "deploy, job", "terms": [["deploy", 0.5], ["job", 0.5]]}
        ]
    })
}

async fn fitted(
    reply: serde_json::Value,
) -> (crosstalk_testkit::upstream::FakeUpstream, SidecarTopicModel) {
    let upstream = fake().await;
    let topics = SidecarTopicModel::new(client_for(&upstream), config(0.5));
    upstream
        .reply_next(Reply::json(StatusCode::OK, &reply))
        .await
        .unwrap();
    let (embeddings, texts) = corpus();
    topics
        .fit(TopicModelVersion(1), &documents(&embeddings, &texts), ts(9))
        .await
        .unwrap();
    (upstream, topics)
}

#[tokio::test]
async fn unfitted_model_has_version_zero() {
    // analysis.topic.version-zero-outlier
    let upstream = fake().await;
    let topics = SidecarTopicModel::new(client_for(&upstream), config(0.0));
    assert_eq!(topics.version(), TopicModelVersion(0));
    assert!(topics.topics().is_empty());
}

#[tokio::test]
async fn version_zero_assigns_outlier() {
    // analysis.topic.version-zero-outlier: even at threshold 0, version 0
    // has no topic to assign.
    let upstream = fake().await;
    let topics = SidecarTopicModel::new(client_for(&upstream), config(0.0));
    for embedding in [
        unit(1.0, 0.0, 0.0),
        unit(0.0, 1.0, 0.0),
        unit(0.0, 0.0, 1.0),
    ] {
        assert_eq!(topics.assign(&embedding), Ok(Assignment::Outlier));
    }
    assert!(upstream.received().await.unwrap().is_empty());
}

#[tokio::test]
async fn first_fit_returns_version_above_unfitted() {
    // analysis.topic.refit-new-version
    let (_upstream, topics) = fitted(two_topic_reply()).await;
    assert_eq!(topics.version(), TopicModelVersion(1));
    assert!(
        topics
            .topics()
            .iter()
            .all(|topic| topic.version == TopicModelVersion(1))
    );
}

#[tokio::test]
async fn fit_returns_version_above_current() {
    // analysis.topic.refit-new-version: a version not above the current one
    // is refused without a request; a later one becomes current.
    let (upstream, topics) = fitted(two_topic_reply()).await;
    let (embeddings, texts) = corpus();
    for stale in [TopicModelVersion(0), TopicModelVersion(1)] {
        assert_eq!(
            topics
                .fit(stale, &documents(&embeddings, &texts), ts(10))
                .await,
            Err(TopicError::VersionNotNewer {
                current: TopicModelVersion(1),
                requested: stale
            })
        );
    }
    assert_eq!(upstream.received().await.unwrap().len(), 1);
    assert_eq!(topics.version(), TopicModelVersion(1));
    // A failed fit's number is skipped by the catalog: 3 follows 1.
    upstream
        .reply_next(Reply::json(StatusCode::OK, &two_topic_reply()))
        .await
        .unwrap();
    let refit = topics
        .fit(
            TopicModelVersion(3),
            &documents(&embeddings, &texts),
            ts(10),
        )
        .await
        .unwrap();
    assert_eq!(topics.version(), TopicModelVersion(3));
    assert!(
        refit
            .iter()
            .all(|topic| topic.version == TopicModelVersion(3))
    );
    assert_eq!(topics.topics(), refit);
}

#[tokio::test]
async fn fit_sends_the_contract_request() {
    let (upstream, _topics) = fitted(two_topic_reply()).await;
    let received = upstream.received().await.unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].method, hyper::Method::POST);
    assert_eq!(received[0].target, "/v1/topics/fit");
    assert_eq!(
        received[0].headers.get_str("content-type"),
        Some("application/json")
    );
    let body = request_json(&upstream, 0).await;
    assert_eq!(
        body["params"],
        json!({"seed": 7, "min_cluster_size": 2, "min_samples": null, "umap_neighbors": 2, "umap_components": 1, "top_terms": 5})
    );
    assert_eq!(
        body["texts"],
        json!(["wiki page", "deploy job", "wiki edit", "lunch"])
    );
    assert_eq!(body["embeddings"]["rows"], json!(4));
    assert_eq!(body["embeddings"]["columns"], json!(3));
    let (embeddings, _) = corpus();
    let expected: String = embeddings
        .iter()
        .flat_map(|embedding| embedding.values().iter().flat_map(|v| v.to_le_bytes()))
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(body["embeddings"]["data"], json!(expected));
}

#[tokio::test]
async fn fit_builds_topics_from_the_reply() {
    let (_upstream, topics) = fitted(two_topic_reply()).await;
    let fitted = topics.topics();
    assert_eq!(fitted.len(), 2);
    assert_eq!(fitted[0].label, "wiki, page, edit");
    assert_eq!(
        fitted[0]
            .terms
            .iter()
            .map(|(term, weight)| (term.as_str(), weight.get()))
            .collect::<Vec<_>>(),
        vec![("wiki", 0.5), ("page", 0.25), ("edit", 0.25)]
    );
    assert_eq!(fitted[1].label, "deploy, job");
    for (index, topic) in fitted.iter().enumerate() {
        assert_eq!(topic.fitted_at, ts(9));
        assert_eq!(
            topic.id,
            topic_id(ts(9), TopicModelVersion(1), u32::try_from(index).unwrap())
        );
    }
}

#[tokio::test]
async fn fit_centroid_is_normalized_member_mean() {
    // analysis.topic.centroid-mean
    let (_upstream, topics) = fitted(two_topic_reply()).await;
    let fitted = topics.topics();
    let (embeddings, _) = corpus();
    // Topic 0 holds documents 0 and 2: the mean of (1, ±0.1, 0) normalized
    // is the x axis.
    let mean: Vec<f32> = embeddings[0]
        .values()
        .iter()
        .zip(embeddings[2].values())
        .map(|(a, b)| a + b)
        .collect();
    let norm = mean.iter().map(|v| v * v).sum::<f32>().sqrt();
    for (got, want) in fitted[0]
        .centroid
        .values()
        .iter()
        .zip(mean.iter().map(|v| v / norm))
    {
        assert!((got - want).abs() < 1e-6, "{got} vs {want}");
    }
    assert_eq!(fitted[0].centroid.values(), &[1.0, 0.0, 0.0]);
    // Topic 1's only member is its own centroid.
    assert_eq!(fitted[1].centroid, embeddings[1]);
}

/// An embedding from three arbitrary components, or `None` when they are
/// all (nearly) zero.
fn arbitrary_unit(values: [f32; 3]) -> Option<Embedding> {
    let norm = values.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm < 1e-3 {
        return None;
    }
    Embedding::new(model(), values.iter().map(|v| v / norm).collect()).ok()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn fit_centroids_match_member_means(
        points in proptest::collection::vec(
            (proptest::array::uniform3(-1.0f32..1.0), -1i64..3),
            3..24,
        ),
    ) {
        // analysis.topic.centroid-mean, over generated clusterings: every
        // topic's centroid is its members' normalized mean.
        let (embeddings, labels): (Vec<Embedding>, Vec<i64>) = points
            .into_iter()
            .filter_map(|(values, label)| arbitrary_unit(values).map(|e| (e, label)))
            .unzip();
        prop_assume!(embeddings.len() >= 3);
        // Renumber so every topic named has a member, as the contract says.
        let mut seen: Vec<i64> = Vec::new();
        for &label in &labels {
            if label >= 0 && !seen.contains(&label) {
                seen.push(label);
            }
        }
        let labels: Vec<i64> = labels
            .iter()
            .map(|label| {
                seen.iter()
                    .position(|seen| seen == label)
                    .map_or(-1, |index| i64::try_from(index).unwrap())
            })
            .collect();
        // A cluster whose mean is the zero vector is a contract violation,
        // tested apart.
        let means_ok = (0..seen.len()).all(|cluster| {
            let members: Vec<&Embedding> = embeddings
                .iter()
                .zip(&labels)
                .filter(|(_, label)| **label == i64::try_from(cluster).unwrap())
                .map(|(embedding, _)| embedding)
                .collect();
            centroid(&model(), &members).is_some()
        });
        prop_assume!(means_ok);
        let reply = json!({
            "labels": labels,
            "topics": (0..seen.len())
                .map(|k| json!({"label": format!("topic {k}"), "terms": []}))
                .collect::<Vec<_>>(),
        });
        let texts: Vec<&str> = embeddings.iter().map(|_| "text").collect();
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let topics = runtime.block_on(async {
            let upstream = fake().await;
            let config = TopicModelConfig {
                model: model(),
                fit: TopicFitParams::new(0, 2, None, 2, 1, 5).unwrap(),
                outlier_below: Similarity::new(0.5).unwrap(),
            };
            let topics = SidecarTopicModel::new(client_for(&upstream), config);
            upstream.reply_next(Reply::json(StatusCode::OK, &reply)).await.unwrap();
            topics
                .fit(TopicModelVersion(1), &documents(&embeddings, &texts), ts(1))
                .await
                .unwrap()
        });
        prop_assert_eq!(topics.len(), seen.len());
        for (cluster, topic) in topics.iter().enumerate() {
            let mut mean = [0.0f64; 3];
            for (embedding, label) in embeddings.iter().zip(&labels) {
                if *label == i64::try_from(cluster).unwrap() {
                    for (total, value) in mean.iter_mut().zip(embedding.values()) {
                        *total += f64::from(*value);
                    }
                }
            }
            let norm = mean.iter().map(|v| v * v).sum::<f64>().sqrt();
            let norm_of_centroid = topic.centroid.values().iter().map(|v| v * v).sum::<f32>().sqrt();
            prop_assert!((norm_of_centroid - 1.0).abs() <= Embedding::NORM_TOLERANCE);
            for (got, total) in topic.centroid.values().iter().zip(mean) {
                let want = total / norm;
                prop_assert!((f64::from(*got) - want).abs() <= f64::from(Embedding::NORM_TOLERANCE));
            }
        }
    }
}

#[tokio::test]
async fn assign_rejects_embedding_of_other_model() {
    // analysis.embedding.same-model-only, for TopicModel::assign; fit
    // refuses it too, without a request.
    let (upstream, topics) = fitted(two_topic_reply()).await;
    let foreign = unit_of(&other_model(), 1.0, 0.0, 0.0);
    assert_eq!(
        topics.assign(&foreign),
        Err(TopicError::WrongModel {
            expected: model(),
            got: other_model()
        })
    );
    let embeddings = vec![unit(1.0, 0.0, 0.0), foreign.clone(), unit(0.0, 1.0, 0.0)];
    let texts = ["a", "b", "c"];
    assert_eq!(
        topics
            .fit(
                TopicModelVersion(2),
                &documents(&embeddings, &texts),
                ts(10)
            )
            .await,
        Err(TopicError::WrongModel {
            expected: model(),
            got: other_model()
        })
    );
    assert_eq!(upstream.received().await.unwrap().len(), 1);
}

#[tokio::test]
async fn assign_takes_the_most_similar_centroid_above_the_threshold() {
    let (_upstream, topics) = fitted(two_topic_reply()).await;
    let fitted = topics.topics();
    match topics.assign(&unit(1.0, 0.05, 0.0)).unwrap() {
        Assignment::Topic { topic, confidence } => {
            assert_eq!(topic, fitted[0].id);
            assert!(confidence.get() > 0.99);
        }
        Assignment::Outlier => panic!("near the x axis is topic 0"),
    }
    match topics.assign(&unit(0.1, 1.0, 0.0)).unwrap() {
        Assignment::Topic { topic, .. } => assert_eq!(topic, fitted[1].id),
        Assignment::Outlier => panic!("near the y axis is topic 1"),
    }
    // Equidistant from both: the lower index wins.
    match topics.assign(&unit(1.0, 1.0, 0.0)).unwrap() {
        Assignment::Topic { topic, .. } => assert_eq!(topic, fitted[0].id),
        Assignment::Outlier => panic!("0.707 is above the threshold"),
    }
    // Below the threshold (0.5) to every centroid.
    assert_eq!(topics.assign(&unit(0.0, 0.0, 1.0)), Ok(Assignment::Outlier));
    assert_eq!(
        topics.assign(&unit(-1.0, 0.0, 0.2)),
        Ok(Assignment::Outlier)
    );
}

#[tokio::test]
async fn too_few_documents_are_refused_without_a_request() {
    let upstream = fake().await;
    let topics = SidecarTopicModel::new(client_for(&upstream), config(0.5));
    let embeddings = vec![unit(1.0, 0.0, 0.0), unit(0.0, 1.0, 0.0)];
    let texts = ["a", "b"];
    assert_eq!(
        topics
            .fit(TopicModelVersion(1), &documents(&embeddings, &texts), ts(1))
            .await,
        Err(TopicError::TooFewSamples { needed: 3, got: 2 })
    );
    assert!(upstream.received().await.unwrap().is_empty());
    assert_eq!(topics.version(), TopicModelVersion(0));
}

#[tokio::test]
async fn sidecar_too_few_samples_is_the_spec_error() {
    let upstream = fake().await;
    let topics = SidecarTopicModel::new(client_for(&upstream), config(0.5));
    upstream
        .reply_next(Reply::json(
            StatusCode::UNPROCESSABLE_ENTITY,
            &json!({"type": "too_few_samples", "data": {"needed": 10, "got": 4}}),
        ))
        .await
        .unwrap();
    let (embeddings, texts) = corpus();
    assert_eq!(
        topics
            .fit(TopicModelVersion(1), &documents(&embeddings, &texts), ts(1))
            .await,
        Err(TopicError::TooFewSamples { needed: 10, got: 4 })
    );
    assert_eq!(topics.version(), TopicModelVersion(0));
}

/// Every reply that is not a usable fit is `Backend`, and leaves the model
/// as it was.
#[tokio::test]
async fn sidecar_failures_are_backend_errors_and_change_nothing() {
    let broken = [
        raw(StatusCode::OK, "not json"),
        Reply::json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({"type": "internal", "data": {"reason": "boom"}}),
        ),
        Reply::json(
            StatusCode::BAD_REQUEST,
            &json!({"type": "invalid_request", "data": {"reason": "texts"}}),
        ),
        Reply::json(
            StatusCode::UNPROCESSABLE_ENTITY,
            &json!({"type": "non_finite_layout"}),
        ),
        raw(StatusCode::BAD_GATEWAY, "<html>bad gateway</html>"),
        // Three labels for four documents.
        Reply::json(
            StatusCode::OK,
            &json!({"labels": [0, 0, -1], "topics": [{"label": "a", "terms": []}]}),
        ),
        // A label naming no topic.
        Reply::json(
            StatusCode::OK,
            &json!({"labels": [0, 1, 0, -1], "topics": [{"label": "a", "terms": []}]}),
        ),
        // A topic without a member.
        Reply::json(
            StatusCode::OK,
            &json!({"labels": [0, 0, 0, -1], "topics": [{"label": "a", "terms": []}, {"label": "b", "terms": []}]}),
        ),
        // A non-positive weight.
        Reply::json(
            StatusCode::OK,
            &json!({"labels": [0, 0, 0, -1], "topics": [{"label": "a", "terms": [["wiki", 0.0]]}]}),
        ),
        // A weight beyond f32.
        Reply::json(
            StatusCode::OK,
            &json!({"labels": [0, 0, 0, -1], "topics": [{"label": "a", "terms": [["wiki", 1e300]]}]}),
        ),
        // An unknown field.
        Reply::json(
            StatusCode::OK,
            &json!({"labels": [-1, -1, -1, -1], "topics": [], "model": "x"}),
        ),
    ];
    let (embeddings, texts) = corpus();
    for reply in broken {
        let upstream = fake().await;
        let topics = SidecarTopicModel::new(client_for(&upstream), config(0.5));
        upstream.reply_next(reply.clone()).await.unwrap();
        let result = topics
            .fit(TopicModelVersion(1), &documents(&embeddings, &texts), ts(1))
            .await;
        assert!(
            matches!(result, Err(TopicError::Backend { .. })),
            "{reply:?} gave {result:?}"
        );
        assert_eq!(topics.version(), TopicModelVersion(0));
    }
}

#[tokio::test]
async fn degenerate_centroid_is_a_backend_error() {
    let upstream = fake().await;
    let topics = SidecarTopicModel::new(client_for(&upstream), config(0.5));
    let embeddings = vec![
        unit(1.0, 0.0, 0.0),
        unit(-1.0, 0.0, 0.0),
        unit(0.0, 1.0, 0.0),
    ];
    let texts = ["a", "b", "c"];
    upstream
        .reply_next(Reply::json(
            StatusCode::OK,
            &json!({"labels": [0, 0, -1], "topics": [{"label": "a", "terms": []}]}),
        ))
        .await
        .unwrap();
    let result = topics
        .fit(TopicModelVersion(1), &documents(&embeddings, &texts), ts(1))
        .await;
    assert!(
        matches!(result, Err(TopicError::Backend { reason }) if reason.contains("zero vector"))
    );
}

#[tokio::test]
async fn timeout_and_unreachable_sidecar_are_backend_errors() {
    let (embeddings, texts) = corpus();
    let upstream = fake().await;
    let topics = SidecarTopicModel::new(
        client_with_timeout(&upstream, Duration::from_millis(200)),
        config(0.5),
    );
    upstream
        .reply_next(Reply::json(StatusCode::OK, &two_topic_reply()).with_fault(Fault::NoResponse))
        .await
        .unwrap();
    let result = topics
        .fit(TopicModelVersion(1), &documents(&embeddings, &texts), ts(1))
        .await;
    assert!(
        matches!(&result, Err(TopicError::Backend { reason }) if reason.contains("within 200 ms")),
        "{result:?}"
    );

    let client = client_for(&upstream);
    drop(upstream);
    let topics = SidecarTopicModel::new(client, config(0.5));
    let result = topics
        .fit(TopicModelVersion(1), &documents(&embeddings, &texts), ts(1))
        .await;
    assert!(
        matches!(result, Err(TopicError::Backend { .. })),
        "{result:?}"
    );
    assert_eq!(topics.version(), TopicModelVersion(0));
}

#[tokio::test]
async fn same_reply_gives_the_same_topics() {
    // The adapter adds no nondeterminism: ids are derived, centroids summed
    // in order.
    let (_a, first) = fitted(two_topic_reply()).await;
    let (_b, second) = fitted(two_topic_reply()).await;
    assert_eq!(first.topics(), second.topics());
}

#[test]
fn topic_ids_carry_the_fit_time_and_differ_by_version_and_index() {
    let id = topic_id(ts(9), TopicModelVersion(1), 0);
    let millis = u128::from(ts(9).as_micros() / 1_000);
    assert_eq!(id.as_ulid() >> 80, millis);
    assert_eq!(id, topic_id(ts(9), TopicModelVersion(1), 0));
    assert_ne!(id, topic_id(ts(9), TopicModelVersion(1), 1));
    assert_ne!(id, topic_id(ts(9), TopicModelVersion(2), 0));
    // The time is only the prefix: the same fit at another time keeps the
    // random part.
    let later = topic_id(ts(10), TopicModelVersion(1), 0);
    assert_eq!(
        later.as_ulid() & ((1 << 80) - 1),
        id.as_ulid() & ((1 << 80) - 1)
    );
}

#[tokio::test]
async fn restore_rebuilds_the_current_fit() {
    let (_upstream, fitted) = fitted(two_topic_reply()).await;
    let topics = fitted.topics();
    let upstream = fake().await;
    let restored = SidecarTopicModel::restore(
        client_for(&upstream),
        config(0.5),
        TopicModelVersion(1),
        topics.clone(),
    )
    .unwrap();
    assert_eq!(restored.version(), TopicModelVersion(1));
    assert_eq!(
        restored.assign(&unit(1.0, 0.0, 0.0)),
        fitted.assign(&unit(1.0, 0.0, 0.0))
    );
    assert!(
        SidecarTopicModel::restore(
            client_for(&upstream),
            config(0.5),
            TopicModelVersion(2),
            topics.clone()
        )
        .is_err()
    );
    assert!(
        SidecarTopicModel::restore(
            client_for(&upstream),
            config(0.5),
            TopicModelVersion(0),
            topics
        )
        .is_err()
    );
}

#[test]
fn fit_params_are_checked() {
    assert!(TopicFitParams::new(0, 1, None, 15, 5, 10).is_err());
    assert!(TopicFitParams::new(0, 2, None, 1, 5, 10).is_err());
    assert!(TopicFitParams::new(0, 2, None, 201, 5, 10).is_err());
    assert!(TopicFitParams::new(0, 2, None, 15, 0, 10).is_err());
    assert!(TopicFitParams::new(0, 2, None, 15, 101, 10).is_err());
    assert!(TopicFitParams::new(0, 2, None, 15, 5, 0).is_err());
    assert!(TopicFitParams::new(0, 2, None, 15, 5, 51).is_err());
    let params = TopicFitParams::new(0, 2, None, 15, 5, 10).unwrap();
    assert_eq!(params.needed(), 16);
    assert_eq!(TopicFitParams::default().needed(), 16);
    assert!(
        serde_json::from_value::<TopicFitParams>(
            json!({"seed": 0, "min_cluster_size": 1, "min_samples": null, "umap_neighbors": 15, "umap_components": 5, "top_terms": 10})
        )
        .is_err()
    );
}
