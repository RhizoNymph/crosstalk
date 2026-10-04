//! Against a running sidecar. Ignored by default; to run:
//!
//! ```sh
//! (cd sidecar/topics && uv run crosstalk-topics) &
//! CROSSTALK_TOPICS_URL=http://127.0.0.1:8090 cargo test -p crosstalk-analysis -- --ignored
//! ```
//!
//! `CROSSTALK_TOPICS_URL` defaults to `http://127.0.0.1:8090`. The goldens
//! were blessed on x86-64; the determinism half holds on any host.

use std::num::NonZeroU64;

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::interfaces::l6_analysis::{FitDocument, LayoutFitter, TopicModel};
use crosstalk_spec::support::Similarity;

use super::contract::{embeddings, fixture, fixture_model};
use super::ts;
use crate::remote::http::HttpClient;
use crate::remote::sidecar::layout::SidecarLayoutFitter;
use crate::remote::sidecar::topics::{SidecarTopicModel, TopicModelConfig};
use crate::remote::sidecar::wire::{
    CONTRACT, LayoutFitRequest, LayoutReply, LayoutTransformRequest, TopicsFitReply,
    TopicsFitRequest,
};
use crate::remote::sidecar::{SidecarClient, SidecarConfig};

fn live_client() -> SidecarClient {
    let url = std::env::var("CROSSTALK_TOPICS_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8090".to_owned());
    let config = SidecarConfig::new(&url, NonZeroU64::new(300_000).unwrap()).unwrap();
    SidecarClient::new(config, HttpClient::new())
}

fn bits(layout: &[[f32; 2]]) -> Vec<u32> {
    layout
        .iter()
        .flat_map(|[x, y]| [x.to_bits(), y.to_bits()])
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a running sidecar: see the module doc"]
async fn live_sidecar_speaks_the_contract() {
    let health = live_client().health().await.unwrap();
    assert_eq!(health.status, "ok");
    assert_eq!(health.contract, CONTRACT);
    assert!(health.versions.contains_key("umap_learn"));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a running sidecar: see the module doc"]
async fn live_topic_fit_matches_the_golden_and_repeats() {
    let request: TopicsFitRequest =
        serde_json::from_str(&fixture("topics_fit.request.json")).unwrap();
    let golden: TopicsFitReply =
        serde_json::from_str(&fixture("topics_fit.response.json")).unwrap();
    let model = fixture_model(request.embeddings.columns());
    let embeddings = embeddings(&request.embeddings, &model);
    let documents: Vec<FitDocument<'_>> = embeddings
        .iter()
        .zip(&request.texts)
        .map(|(embedding, text)| FitDocument { text, embedding })
        .collect();
    let config = TopicModelConfig {
        model,
        fit: request.params,
        outlier_below: Similarity::new(0.5).unwrap(),
    };
    let first = SidecarTopicModel::new(live_client(), config.clone());
    let second = SidecarTopicModel::new(live_client(), config);
    let a = first
        .fit(TopicModelVersion(1), &documents, ts(1))
        .await
        .unwrap();
    let b = second
        .fit(TopicModelVersion(1), &documents, ts(1))
        .await
        .unwrap();
    assert_eq!(a, b);
    assert_eq!(
        a.iter()
            .map(|topic| topic.label.as_str())
            .collect::<Vec<_>>(),
        golden
            .topics
            .iter()
            .map(|topic| topic.label.as_str())
            .collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a running sidecar: see the module doc"]
async fn live_layout_is_bit_identical_to_the_golden_on_every_fit() {
    let request: LayoutFitRequest =
        serde_json::from_str(&fixture("layout_fit.request.json")).unwrap();
    let golden: LayoutReply = serde_json::from_str(&fixture("layout_fit.response.json")).unwrap();
    let model = fixture_model(request.embeddings.columns());
    let embeddings = embeddings(&request.embeddings, &model);
    let fitter = SidecarLayoutFitter::new(live_client());
    let first = fitter.fit(&embeddings, request.params).await.unwrap();
    let second = fitter.fit(&embeddings, request.params).await.unwrap();
    assert_eq!(bits(&first), bits(&second));
    let want: Vec<u32> = golden
        .coordinates
        .decode()
        .unwrap()
        .iter()
        .map(|v| v.to_bits())
        .collect();
    assert_eq!(bits(&first), want);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a running sidecar: see the module doc"]
async fn live_transform_matches_the_golden() {
    let request: LayoutTransformRequest =
        serde_json::from_str(&fixture("layout_transform.request.json")).unwrap();
    let golden: LayoutReply =
        serde_json::from_str(&fixture("layout_transform.response.json")).unwrap();
    let model = fixture_model(request.base.columns());
    let base = embeddings(&request.base, &model);
    let points = embeddings(&request.points, &model);
    let fitter = SidecarLayoutFitter::new(live_client());
    let placed = fitter
        .transform(&base, request.params, &points)
        .await
        .unwrap();
    let want: Vec<u32> = golden
        .coordinates
        .decode()
        .unwrap()
        .iter()
        .map(|v| v.to_bits())
        .collect();
    assert_eq!(bits(&placed), want);
}
