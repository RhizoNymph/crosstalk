//! `projection` over HTTP: the frame then the job record, the frame checked
//! against its ETag, kept and revalidated with `If-None-Match`, and every
//! refusal of the frame route answered before the record is read.

use crosstalk_spec::aggregates::projection::frame::{FrameHeader, ProjectionFrame};
use crosstalk_spec::aggregates::projection::{
    ProjectionInfo, ProjectionLimit, ProjectionStatusKind,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l8_surface::http::frame::OCTET_STREAM;
use crosstalk_spec::interfaces::l8_surface::{ConflictKind, QueryApi, QueryError};
use crosstalk_spec::support::Blake3;

use super::stub::{Body, Reply, Stub, fast_config};
use super::{ULID_A, caller, from_json, golden_json, id};
use crate::HttpClient;

fn projection_id() -> ProjectionId {
    id(ULID_A)
}

/// The ready job of the golden, with nothing matched: an empty frame.
fn ready_info() -> ProjectionInfo {
    let mut json = golden_json("projections/projection_ready.json");
    json["status"]["data"]["matching"] = 0.into();
    json["status"]["data"]["points"] = 0.into();
    serde_json::from_value(json).unwrap_or_else(|error| panic!("{error}"))
}

fn frame() -> ProjectionFrame {
    let header = FrameHeader {
        projection: projection_id(),
        topic_version: TopicModelVersion(2),
        watermark: from_json("\"2026-10-04T11:00:00.000000Z\""),
        limit: ProjectionLimit::new(5000).unwrap_or_else(|error| panic!("{error:?}")),
        matching: 0,
    };
    ProjectionFrame::from_points(header, &[]).unwrap_or_else(|error| panic!("{error:?}"))
}

fn etag(bytes: &[u8]) -> String {
    format!("\"{}\"", Blake3::of(bytes).to_hex())
}

fn frame_reply(bytes: &[u8], etag: &str) -> Reply {
    Reply {
        status: 200,
        headers: vec![
            ("content-type", OCTET_STREAM.to_owned()),
            ("etag", etag.to_owned()),
            (
                "cache-control",
                "private, max-age=600, immutable".to_owned(),
            ),
        ],
        body: Body::Whole(bytes.to_vec()),
    }
}

/// A stub serving the frame (`304` when `If-None-Match` names it) and
/// `record` as the job.
async fn surface(record: ProjectionInfo) -> Stub {
    let bytes = frame().encode();
    let tag = etag(&bytes);
    Stub::start(move |request, _| {
        if request.path.ends_with("/frame") {
            if request.header("if-none-match") == Some(tag.as_str()) {
                return Reply {
                    status: 304,
                    headers: vec![("etag", tag.clone())],
                    body: Body::Whole(Vec::new()),
                };
            }
            frame_reply(&bytes, &tag)
        } else {
            Reply::value(200, &record)
        }
    })
    .await
}

/// The frame is read first, then the record, and they make the projection.
#[tokio::test]
async fn a_projection_is_its_frame_and_record() {
    let mut stub = surface(ready_info()).await;
    let projection = stub
        .client()
        .projection(&caller(), projection_id())
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(projection.info(), &ready_info());
    assert_eq!(projection.frame(), &frame());
    let requests = stub.requests();
    let paths: Vec<&str> = requests.iter().map(|r| r.path.as_str()).collect();
    assert_eq!(
        paths,
        vec![
            format!("/projections/{ULID_A}/frame"),
            format!("/projections/{ULID_A}"),
        ]
    );
    assert_eq!(requests[0].header("accept"), Some(OCTET_STREAM));
    assert_eq!(requests[0].header("if-none-match"), None);
}

/// A second read revalidates the kept frame with its ETag and takes the
/// `304`; with no cache, every read fetches the bytes.
#[tokio::test]
async fn a_kept_frame_is_revalidated() {
    let mut stub = surface(ready_info()).await;
    let client = stub.client();
    let first = client.projection(&caller(), projection_id()).await;
    let second = client.projection(&caller(), projection_id()).await;
    assert_eq!(first, second);
    assert!(second.is_ok());
    let tags: Vec<Option<String>> = stub
        .requests()
        .iter()
        .filter(|request| request.path.ends_with("/frame"))
        .map(|request| request.header("if-none-match").map(str::to_owned))
        .collect();
    assert_eq!(tags, vec![None, Some(etag(&frame().encode()))]);

    let mut stub = surface(ready_info()).await;
    let client = HttpClient::new(stub.base(), fast_config().with_frame_cache(0));
    let _ = client.projection(&caller(), projection_id()).await;
    let _ = client.projection(&caller(), projection_id()).await;
    assert!(
        stub.requests()
            .iter()
            .all(|request| request.header("if-none-match").is_none())
    );
}

/// Bytes that are not what their ETag names are not trusted.
#[tokio::test]
async fn a_frame_must_match_its_etag() {
    let bytes = frame().encode();
    let stub = Stub::always(frame_reply(&bytes, &format!("\"{}\"", "0".repeat(64)))).await;
    let result = stub.client().projection(&caller(), projection_id()).await;
    assert!(
        matches!(&result, Err(QueryError::Store { reason }) if reason.contains("ETag")),
        "{result:?}"
    );
}

/// A frame the surface refuses (not ready, failed, expired, unknown,
/// forbidden) is that refusal, and the record is not read.
#[tokio::test]
async fn a_refused_frame_is_its_error() {
    let not_ready = QueryError::Conflict(ConflictKind::ProjectionNotReady {
        projection: projection_id(),
        status: ProjectionStatusKind::Fitting,
    });
    let gone = QueryError::ProjectionNotRetained {
        projection: projection_id(),
    };
    for (status, error) in [(409, not_ready), (410, gone), (404, QueryError::NotFound)] {
        let mut stub = Stub::always(Reply::value(status, &error)).await;
        let result = stub.client().projection(&caller(), projection_id()).await;
        assert_eq!(result, Err(error));
        assert_eq!(stub.requests().len(), 1, "the record is not read");
    }
}

/// A frame that expired between the two reads is `ProjectionNotRetained`,
/// as the in-process method answers, and is no longer kept.
#[tokio::test]
async fn a_frame_expired_in_between_is_not_retained() {
    let expired: ProjectionInfo = super::golden_value("projections/projection_expired.json");
    let mut stub = surface(expired).await;
    let client = stub.client();
    for _ in 0..2 {
        let result = client.projection(&caller(), projection_id()).await;
        assert_eq!(
            result,
            Err(QueryError::ProjectionNotRetained {
                projection: projection_id()
            })
        );
    }
    let tags: Vec<Option<String>> = stub
        .requests()
        .iter()
        .filter(|request| request.path.ends_with("/frame"))
        .map(|request| request.header("if-none-match").map(str::to_owned))
        .collect();
    assert_eq!(tags, vec![None, None], "an expired frame is dropped");
}
