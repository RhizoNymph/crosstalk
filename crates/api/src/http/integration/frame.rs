//! `GET /projections/{id}/frame`: the frame's bytes with a strong ETag of
//! their digest, cached privately until retention drops the frame, a 304
//! for a matching `If-None-Match`, and every refusal `no-store`.

use std::sync::Arc;

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, ETAG, IF_NONE_MATCH};
use axum::http::{Request, StatusCode};
use crosstalk_spec::aggregates::projection::frame::{FrameHeader, ProjectionFrame};
use crosstalk_spec::aggregates::projection::{
    ProjectedPoint, Projection, ProjectionInfo, ProjectionStatus,
};
use crosstalk_spec::interfaces::l8_surface::{Permission, QueryError};
use crosstalk_spec::support::Blake3;
use serde_json::{Value, json};

use super::cases::PROJECTION;
use super::fake::{Call, Fake};
use super::{FULL, error_json, golden, operator, send, server, token, without};

/// The ready job of the golden, with the golden points as its frame.
fn projection() -> Projection {
    let points: Vec<ProjectedPoint> = golden("projections/projected_points");
    let mut info: Value = golden("projections/projection_ready");
    // As many points as the job matched, all of them in the frame.
    info["status"]["data"]["points"] = json!(points.len());
    info["status"]["data"]["matching"] = json!(points.len());
    let info: ProjectionInfo = serde_json::from_value(info).expect("a ready job");
    let ProjectionStatus::Ready(fitted) = info.status().clone() else {
        panic!("the golden job is ready");
    };
    let header = FrameHeader {
        projection: info.id(),
        topic_version: info.spec().topic_version(),
        watermark: fitted.watermark,
        limit: info.spec().params().limit(),
        matching: fitted.matching,
    };
    let frame = ProjectionFrame::from_points(header, &points).expect("a valid frame");
    Projection::new(info, frame).expect("the frame matches the job")
}

fn get_frame(as_operator: u128, if_none_match: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .uri(format!("/projections/{PROJECTION}/frame"))
        .header(AUTHORIZATION, format!("Bearer {}", token(as_operator)));
    if let Some(tag) = if_none_match {
        builder = builder.header(IF_NONE_MATCH, tag);
    }
    builder.body(Body::empty()).expect("a request")
}

#[tokio::test]
async fn frame_is_cached_and_revalidated() {
    let projection = projection();
    let bytes = projection.frame().encode();
    let etag = format!("\"{}\"", Blake3::of(&bytes).to_hex());
    // Fitted at 11:08:30, read at 12:00:00, kept 180 days.
    let cache_control = format!(
        "private, max-age={}, immutable",
        180 * 86_400 - (51 * 60 + 30)
    );
    let fake = Arc::new(Fake::default());
    fake.project(projection);
    let router = server(&fake);

    let reply = send(&router, get_frame(FULL, None)).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(
        reply.header(CONTENT_TYPE.as_str()),
        Some("application/octet-stream")
    );
    assert_eq!(
        reply.body.to_vec(),
        bytes,
        "ProjectionFrame::encode's bytes"
    );
    assert_eq!(reply.header(ETAG.as_str()), Some(etag.as_str()));
    assert_eq!(
        reply.header(CACHE_CONTROL.as_str()),
        Some(cache_control.as_str())
    );
    assert_eq!(
        fake.calls(),
        vec![Call {
            method: "projection",
            operator: operator(FULL),
            args: json!({ "id": PROJECTION }),
        }]
    );

    for tag in [
        etag.clone(),
        format!("W/{etag}"),
        "*".to_owned(),
        format!("\"other\", {etag}"),
    ] {
        let reply = send(&router, get_frame(FULL, Some(&tag))).await;
        assert_eq!(reply.status, StatusCode::NOT_MODIFIED, "{tag}");
        assert!(reply.body.is_empty(), "{tag}: no body");
        assert_eq!(reply.header(ETAG.as_str()), Some(etag.as_str()));
        assert_eq!(
            reply.header(CACHE_CONTROL.as_str()),
            Some(cache_control.as_str())
        );
    }
    let reply = send(&router, get_frame(FULL, Some("\"other\""))).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body.to_vec(), bytes);

    // Without Content: a 403 even for a matching tag, nothing read.
    let fake_403 = Arc::new(Fake::default());
    fake_403.project(self::projection());
    let reply = send(
        &server(&fake_403),
        get_frame(without(Permission::Content), Some(&etag)),
    )
    .await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);
    assert_eq!(reply.header(CACHE_CONTROL.as_str()), Some("no-store"));
    assert_eq!(reply.header(ETAG.as_str()), None);
    assert!(fake_403.untouched());
}

/// A job not ready, failed or expired, and an unknown one, answer with
/// their error and `no-store`, even for a request with a matching tag.
#[tokio::test]
async fn a_frame_not_ready_is_its_error_and_never_cached() {
    let rows: Vec<Value> = golden("http/query_error_statuses");
    let wanted = [
        "projection_not_ready",
        "projection_failed",
        "projection_not_retained",
    ];
    let mut seen = 0;
    for row in rows {
        let error = &row["error"];
        let kind = error["data"]["type"]
            .as_str()
            .or_else(|| error["type"].as_str())
            .unwrap_or_default();
        if !wanted.contains(&kind) {
            continue;
        }
        seen += 1;
        let error: QueryError = serde_json::from_value(error.clone()).expect("an error");
        let fake = Arc::new(Fake::default());
        fake.project(projection());
        fake.fail_with(error.clone());
        let reply = send(&server(&fake), get_frame(FULL, Some("*"))).await;
        assert_eq!(json!(reply.status.as_u16()), row["status"], "{kind}");
        assert_eq!(reply.json(), error_json(&error));
        assert_eq!(reply.header(CACHE_CONTROL.as_str()), Some("no-store"));
        assert_eq!(reply.header(ETAG.as_str()), None);
    }
    assert_eq!(seen, wanted.len());
    let fake = Arc::new(Fake::default());
    let reply = send(&server(&fake), get_frame(FULL, None)).await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);
    assert_eq!(reply.json(), error_json(&QueryError::NotFound));
}
