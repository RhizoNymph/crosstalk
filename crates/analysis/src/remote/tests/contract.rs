//! The fixture files the sidecar's golden tests use
//! (`sidecar/topics/tests/fixtures/`), read from Rust: each request file is
//! exactly the bytes the adapter sends for its inputs, and each golden reply
//! decodes into topics or a layout. So the Python and Rust halves of the
//! contract are pinned by the same files.

use std::num::NonZeroU16;
use std::path::PathBuf;

use crosstalk_spec::aggregates::projection::ProjectionParams;
use crosstalk_spec::aggregates::topic::{Embedding, EmbeddingModel, TopicModelVersion};
use crosstalk_spec::interfaces::l6_analysis::{FitDocument, LayoutFitter, TopicModel};
use crosstalk_spec::support::Similarity;
use crosstalk_testkit::upstream::FakeUpstream;
use hyper::StatusCode;

use super::{client_for, fake, raw, ts};
use crate::remote::matrix::Matrix;
use crate::remote::sidecar::layout::SidecarLayoutFitter;
use crate::remote::sidecar::topics::{SidecarTopicModel, TopicModelConfig};
use crate::remote::sidecar::wire::{
    LayoutFitRequest, LayoutReply, LayoutTransformRequest, TopicsFitReply, TopicsFitRequest,
};

pub(crate) fn fixture(name: &str) -> String {
    let path: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "..",
        "..",
        "sidecar",
        "topics",
        "tests",
        "fixtures",
        name,
    ]
    .iter()
    .collect();
    std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

pub(crate) fn fixture_model(columns: NonZeroU16) -> EmbeddingModel {
    EmbeddingModel {
        name: "fixture".to_owned(),
        dimension: columns,
    }
}

/// The matrix's rows as embeddings of `model`.
pub(crate) fn embeddings(matrix: &Matrix, model: &EmbeddingModel) -> Vec<Embedding> {
    let width = usize::from(matrix.columns().get());
    matrix
        .decode()
        .unwrap()
        .chunks(width)
        .map(|row| Embedding::new(model.clone(), row.to_vec()).unwrap())
        .collect()
}

async fn sent_body(upstream: &FakeUpstream) -> String {
    let received = upstream.received().await.unwrap();
    assert_eq!(received.len(), 1);
    String::from_utf8(received[0].body.to_vec()).unwrap()
}

#[tokio::test]
async fn topics_fit_fixture_is_what_the_adapter_sends_and_its_golden_decodes() {
    let request_text = fixture("topics_fit.request.json");
    let response_text = fixture("topics_fit.response.json");
    let request: TopicsFitRequest = serde_json::from_str(&request_text).unwrap();
    assert_eq!(serde_json::to_string(&request).unwrap(), request_text);
    let model = fixture_model(request.embeddings.columns());
    let embeddings = embeddings(&request.embeddings, &model);
    let documents: Vec<FitDocument<'_>> = embeddings
        .iter()
        .zip(&request.texts)
        .map(|(embedding, text)| FitDocument { text, embedding })
        .collect();

    let upstream = fake().await;
    let topics = SidecarTopicModel::new(
        client_for(&upstream),
        TopicModelConfig {
            model,
            fit: request.params,
            outlier_below: Similarity::new(0.0).unwrap(),
        },
    );
    upstream
        .reply_next(raw(StatusCode::OK, &response_text))
        .await
        .unwrap();
    let fitted = topics
        .fit(TopicModelVersion(1), &documents, ts(1))
        .await
        .unwrap();
    assert_eq!(sent_body(&upstream).await, request_text);

    let golden: TopicsFitReply = serde_json::from_str(&response_text).unwrap();
    assert_eq!(fitted.len(), golden.topics.len());
    assert!(!fitted.is_empty());
    for (topic, reply) in fitted.iter().zip(&golden.topics) {
        assert_eq!(topic.label, reply.label);
        assert_eq!(topic.terms.len(), reply.terms.len());
    }
    // Each document's nearest centroid is the cluster the sidecar put it
    // in, for this well-separated fixture.
    for (embedding, label) in embeddings.iter().zip(&golden.labels) {
        if let Ok(cluster) = usize::try_from(*label) {
            match topics.assign(embedding).unwrap() {
                crosstalk_spec::aggregates::topic::Assignment::Topic { topic, .. } => {
                    assert_eq!(topic, fitted[cluster].id);
                }
                crosstalk_spec::aggregates::topic::Assignment::Outlier => {
                    panic!("threshold 0 assigns every embedding")
                }
            }
        }
    }
}

#[tokio::test]
async fn layout_fit_fixture_is_what_the_adapter_sends_and_its_golden_decodes() {
    let request_text = fixture("layout_fit.request.json");
    let response_text = fixture("layout_fit.response.json");
    let request: LayoutFitRequest = serde_json::from_str(&request_text).unwrap();
    assert_eq!(serde_json::to_string(&request).unwrap(), request_text);
    let model = fixture_model(request.embeddings.columns());
    let embeddings = embeddings(&request.embeddings, &model);

    let upstream = fake().await;
    let fitter = SidecarLayoutFitter::new(client_for(&upstream));
    upstream
        .reply_next(raw(StatusCode::OK, &response_text))
        .await
        .unwrap();
    let layout = fitter.fit(&embeddings, request.params).await.unwrap();
    assert_eq!(sent_body(&upstream).await, request_text);

    let golden: LayoutReply = serde_json::from_str(&response_text).unwrap();
    assert_eq!(serde_json::to_string(&golden).unwrap(), response_text);
    let values = golden.coordinates.decode().unwrap();
    assert_eq!(layout.len(), embeddings.len());
    let bits: Vec<u32> = layout
        .iter()
        .flat_map(|[x, y]| [x.to_bits(), y.to_bits()])
        .collect();
    assert_eq!(bits, values.iter().map(|v| v.to_bits()).collect::<Vec<_>>());
}

#[tokio::test]
async fn layout_transform_fixture_is_what_the_adapter_sends_and_its_golden_decodes() {
    let request_text = fixture("layout_transform.request.json");
    let response_text = fixture("layout_transform.response.json");
    let request: LayoutTransformRequest = serde_json::from_str(&request_text).unwrap();
    assert_eq!(serde_json::to_string(&request).unwrap(), request_text);
    let model = fixture_model(request.base.columns());
    let base = embeddings(&request.base, &model);
    let points = embeddings(&request.points, &model);
    let params: ProjectionParams = request.params;

    let upstream = fake().await;
    let fitter = SidecarLayoutFitter::new(client_for(&upstream));
    upstream
        .reply_next(raw(StatusCode::OK, &response_text))
        .await
        .unwrap();
    let placed = fitter.transform(&base, params, &points).await.unwrap();
    assert_eq!(sent_body(&upstream).await, request_text);
    assert_eq!(placed.len(), points.len());
    let golden: LayoutReply = serde_json::from_str(&response_text).unwrap();
    assert_eq!(serde_json::to_string(&golden).unwrap(), response_text);
}
