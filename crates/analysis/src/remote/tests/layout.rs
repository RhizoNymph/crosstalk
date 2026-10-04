//! `SidecarLayoutFitter` against a fake sidecar.

use std::num::NonZeroU16;
use std::time::Duration;

use crosstalk_spec::aggregates::projection::{FitFailure, ProjectionLimit, ProjectionParams};
use crosstalk_spec::aggregates::topic::Embedding;
use crosstalk_spec::interfaces::l6_analysis::{LayoutError, LayoutFitter};
use crosstalk_testkit::upstream::{Fault, Reply};
use hyper::StatusCode;
use proptest::prelude::*;
use serde_json::json;

use super::{
    client_for, client_with_timeout, fake, model, other_model, raw, request_json, unit, unit_of,
};
use crate::remote::matrix::Matrix;
use crate::remote::sidecar::layout::{SidecarLayoutFitter, TransformError};

fn params(neighbors: u16, seed: u64) -> ProjectionParams {
    ProjectionParams::new(ProjectionLimit::new(100).unwrap(), neighbors, 100, seed).unwrap()
}

fn points(n: usize) -> Vec<Embedding> {
    (0..n)
        .map(|i| {
            let i = f32::from(u16::try_from(i).unwrap());
            unit(1.0 + i, 0.5 * i, 1.0)
        })
        .collect()
}

fn coordinates(pairs: &[[f32; 2]]) -> serde_json::Value {
    let matrix = Matrix::encode(
        NonZeroU16::new(2).unwrap(),
        pairs.iter().map(|pair| pair.as_slice()),
    )
    .unwrap();
    json!({"coordinates": matrix})
}

fn bits(layout: &[[f32; 2]]) -> Vec<u32> {
    layout
        .iter()
        .flat_map(|[x, y]| [x.to_bits(), y.to_bits()])
        .collect()
}

#[tokio::test]
async fn fit_sends_params_and_returns_the_coordinates_exactly() {
    let upstream = fake().await;
    let fitter = SidecarLayoutFitter::new(client_for(&upstream));
    let layout = [[0.1f32, -2.5], [3.25, 1e-7], [-0.0, 7.0]];
    upstream
        .reply_next(Reply::json(StatusCode::OK, &coordinates(&layout)))
        .await
        .unwrap();
    let got = fitter.fit(&points(3), params(2, u64::MAX)).await.unwrap();
    assert_eq!(bits(&got), bits(&layout));
    let received = upstream.received().await.unwrap();
    assert_eq!(received[0].target, "/v1/layout/fit");
    let body = request_json(&upstream, 0).await;
    assert_eq!(
        body["params"],
        json!({"limit": 100, "neighbors": 2, "min_dist_milli": 100, "seed": u64::MAX})
    );
    assert_eq!(body["embeddings"]["rows"], json!(3));
    assert_eq!(body["embeddings"]["columns"], json!(3));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    #[test]
    fn prop_layout_is_deterministic(
        raw_bits in proptest::collection::vec((any::<u32>(), any::<u32>()), 3..12),
        seed in any::<u64>(),
    ) {
        // analysis.projection.deterministic-layout, the adapter's half:
        // whatever bits the sidecar computes reach the caller unchanged, on
        // every call. The sidecar's half is its own determinism suite.
        let layout: Vec<[f32; 2]> = raw_bits
            .iter()
            .map(|(x, y)| [f32::from_bits(*x), f32::from_bits(*y)])
            .filter(|[x, y]| x.is_finite() && y.is_finite())
            .collect();
        prop_assume!(layout.len() >= 3);
        let embeddings = points(layout.len());
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let (first, second) = runtime.block_on(async {
            let upstream = fake().await;
            let fitter = SidecarLayoutFitter::new(client_for(&upstream));
            let reply = Reply::json(StatusCode::OK, &coordinates(&layout));
            upstream.reply_next(reply.clone()).await.unwrap();
            upstream.reply_next(reply).await.unwrap();
            let first = fitter.fit(&embeddings, params(2, seed)).await.unwrap();
            let second = fitter.fit(&embeddings, params(2, seed)).await.unwrap();
            let received = upstream.received().await.unwrap();
            assert_eq!(received[0].body, received[1].body);
            (first, second)
        });
        prop_assert_eq!(bits(&first), bits(&layout));
        prop_assert_eq!(bits(&first), bits(&second));
    }
}

#[tokio::test]
async fn too_few_points_fail_without_a_request() {
    let upstream = fake().await;
    let fitter = SidecarLayoutFitter::new(client_for(&upstream));
    assert_eq!(
        fitter.fit(&points(15), params(15, 1)).await,
        Err(LayoutError::Failed(FitFailure::TooFewPoints {
            needed: 16,
            got: 15
        }))
    );
    assert_eq!(
        fitter.fit(&[], params(2, 1)).await,
        Err(LayoutError::Failed(FitFailure::TooFewPoints {
            needed: 3,
            got: 0
        }))
    );
    assert!(upstream.received().await.unwrap().is_empty());
}

#[tokio::test]
async fn sidecar_refusals_are_fit_failures() {
    let refusals = [
        (
            Reply::json(
                StatusCode::UNPROCESSABLE_ENTITY,
                &json!({"type": "too_few_points", "data": {"needed": 16, "got": 9}}),
            ),
            FitFailure::TooFewPoints { needed: 16, got: 9 },
        ),
        (
            Reply::json(
                StatusCode::UNPROCESSABLE_ENTITY,
                &json!({"type": "non_finite_layout"}),
            ),
            FitFailure::NonFiniteLayout,
        ),
        (
            // A NaN in the reply is UMAP's output, not a fault.
            Reply::json(
                StatusCode::OK,
                &json!({"coordinates": {"rows": 3, "columns": 2, "data": format!("0000803f0000c07f{}", "0".repeat(32))}}),
            ),
            FitFailure::NonFiniteLayout,
        ),
    ];
    for (reply, failure) in refusals {
        let upstream = fake().await;
        let fitter = SidecarLayoutFitter::new(client_for(&upstream));
        upstream.reply_next(reply).await.unwrap();
        assert_eq!(
            fitter.fit(&points(3), params(2, 1)).await,
            Err(LayoutError::Failed(failure))
        );
    }
}

#[tokio::test]
async fn transport_failures_are_backend_errors() {
    // analysis.layout.backend-failure-not-recorded: nothing but the
    // sidecar's deterministic refusals becomes a FitFailure.
    let broken = [
        raw(StatusCode::OK, "{"),
        Reply::json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({"type": "internal", "data": {"reason": "boom"}}),
        ),
        Reply::json(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "starting"}),
        ),
        Reply::json(
            StatusCode::BAD_REQUEST,
            &json!({"type": "invalid_request", "data": {"reason": "rows > limit"}}),
        ),
        Reply::json(
            StatusCode::PAYLOAD_TOO_LARGE,
            &json!({"type": "payload_too_large", "data": {"limit_bytes": 10}}),
        ),
        Reply::json(
            StatusCode::UNPROCESSABLE_ENTITY,
            &json!({"type": "too_few_samples", "data": {"needed": 3, "got": 1}}),
        ),
        Reply::json(
            StatusCode::UNPROCESSABLE_ENTITY,
            &json!({"unexpected": true}),
        ),
        // Two rows for three points.
        Reply::json(StatusCode::OK, &coordinates(&[[0.0, 0.0], [1.0, 1.0]])),
        // Three columns.
        Reply::json(
            StatusCode::OK,
            &json!({"coordinates": {"rows": 3, "columns": 3, "data": "00".repeat(36)}}),
        ),
        // Upper-case hex.
        Reply::json(
            StatusCode::OK,
            &json!({"coordinates": {"rows": 3, "columns": 2, "data": "0000803F".repeat(6)}}),
        ),
        // A body cut short.
        Reply::json(
            StatusCode::OK,
            &coordinates(&[[0.0, 0.0], [1.0, 1.0], [2.0, 2.0]]),
        )
        .with_fault(Fault::Disconnect { after_chunks: 0 }),
    ];
    for reply in broken {
        let upstream = fake().await;
        let fitter = SidecarLayoutFitter::new(client_for(&upstream));
        upstream.reply_next(reply.clone()).await.unwrap();
        let result = fitter.fit(&points(3), params(2, 1)).await;
        assert!(
            matches!(result, Err(LayoutError::Backend { .. })),
            "{reply:?} gave {result:?}"
        );
    }

    let upstream = fake().await;
    let fitter =
        SidecarLayoutFitter::new(client_with_timeout(&upstream, Duration::from_millis(200)));
    upstream
        .reply_next(
            Reply::json(StatusCode::OK, &coordinates(&[[0.0, 0.0]; 3]))
                .with_fault(Fault::Stall { after_chunks: 0 }),
        )
        .await
        .unwrap();
    let result = fitter.fit(&points(3), params(2, 1)).await;
    assert!(
        matches!(&result, Err(LayoutError::Backend { reason }) if reason.contains("within 200 ms")),
        "{result:?}"
    );

    let client = client_for(&upstream);
    drop(upstream);
    let result = SidecarLayoutFitter::new(client)
        .fit(&points(3), params(2, 1))
        .await;
    assert!(
        matches!(result, Err(LayoutError::Backend { .. })),
        "{result:?}"
    );
}

#[tokio::test]
async fn mixed_models_are_refused_without_a_request() {
    let upstream = fake().await;
    let fitter = SidecarLayoutFitter::new(client_for(&upstream));
    let mut embeddings = points(3);
    embeddings.push(unit_of(&other_model(), 1.0, 0.0, 0.0));
    assert!(matches!(
        fitter.fit(&embeddings, params(2, 1)).await,
        Err(LayoutError::Backend { .. })
    ));
    assert!(upstream.received().await.unwrap().is_empty());
}

#[tokio::test]
async fn transform_sends_base_params_and_points() {
    let upstream = fake().await;
    let fitter = SidecarLayoutFitter::new(client_for(&upstream));
    let placed = [[0.5f32, 0.25], [-1.0, 2.0]];
    upstream
        .reply_next(Reply::json(StatusCode::OK, &coordinates(&placed)))
        .await
        .unwrap();
    let got = fitter
        .transform(&points(3), params(2, 9), &points(2))
        .await
        .unwrap();
    assert_eq!(bits(&got), bits(&placed));
    let received = upstream.received().await.unwrap();
    assert_eq!(received[0].target, "/v1/layout/transform");
    let body = request_json(&upstream, 0).await;
    assert_eq!(body["base"]["rows"], json!(3));
    assert_eq!(body["points"]["rows"], json!(2));
    assert_eq!(body["params"]["seed"], json!(9));
    let keys: Vec<&String> = body.as_object().unwrap().keys().collect();
    assert_eq!(keys.len(), 3);
}

#[tokio::test]
async fn transform_refuses_points_of_another_model_and_short_bases() {
    let upstream = fake().await;
    let fitter = SidecarLayoutFitter::new(client_for(&upstream));
    let foreign = [unit(1.0, 0.0, 0.0), unit_of(&other_model(), 1.0, 0.0, 0.0)];
    assert_eq!(
        fitter.transform(&points(3), params(2, 1), &foreign).await,
        Err(TransformError::WrongModel {
            index: 4,
            expected: model(),
            got: other_model()
        })
    );
    assert_eq!(
        fitter.transform(&points(2), params(2, 1), &points(1)).await,
        Err(TransformError::Layout(LayoutError::Failed(
            FitFailure::TooFewPoints { needed: 3, got: 2 }
        )))
    );
    assert!(upstream.received().await.unwrap().is_empty());
    // A reply with the wrong number of points is a backend failure.
    upstream
        .reply_next(Reply::json(StatusCode::OK, &coordinates(&[[0.0, 0.0]])))
        .await
        .unwrap();
    assert!(matches!(
        fitter.transform(&points(3), params(2, 1), &points(2)).await,
        Err(TransformError::Layout(LayoutError::Backend { .. }))
    ));
}
