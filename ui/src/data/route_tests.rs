//! The data routes through the router: statuses, content types and the
//! strict query, against the fixture backend.

use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use topcoat::router::header::CONTENT_TYPE;
use topcoat::router::request::Request;
use topcoat::router::{Body, Router, RouterBuilderDiscoverExt, StatusCode, to_bytes};

use super::require;
use crate::backend::fixture::FixtureBackend;
use crate::config::TrustedOperator;

const VIEW: &str = "from=2026-10-02T00:00:00Z&to=2026-10-03T00:00:00Z&v=0&w=tx";

struct Reply {
    status: StatusCode,
    content_type: String,
    body: Vec<u8>,
}

async fn get(uri: &str) -> Reply {
    let router = Router::builder()
        .discover()
        .app_context(TrustedOperator {
            id: OperatorId::from_ulid(1),
            name: "test".to_owned(),
        })
        .app_context(FixtureBackend::new(7))
        .build();
    let request = Request::<()>::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("request");
    let response = router.handle(request).await;
    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let body = to_bytes(response.into_body(), 1 << 24)
        .await
        .expect("body")
        .to_vec();
    Reply {
        status,
        content_type,
        body,
    }
}

fn json(reply: &Reply) -> serde_json::Value {
    serde_json::from_slice(&reply.body).expect("json body")
}

#[tokio::test]
async fn topology_answers_both_modes() {
    for mode in ["agents", "channels"] {
        let reply = get(&format!("/data/topology?{VIEW}&g={mode}")).await;
        assert_eq!(reply.status, StatusCode::OK, "{mode}");
        assert_eq!(reply.content_type, "application/json");
        let body = json(&reply);
        assert_eq!(body["mode"], mode);
        assert!(body["nodes"].is_array() && body["edges"].is_array());
    }
}

#[tokio::test]
async fn incomplete_view_state_is_a_400_not_a_redirect() {
    let reply = get("/data/topology?g=agents&w=tx").await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    let text = String::from_utf8(reply.body).expect("utf8");
    assert!(text.contains("missing from, to, v"), "{text}");

    let reply = get("/data/timeline").await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn invalid_view_state_is_a_400() {
    let reply = get(&format!("/data/topology?{VIEW}&g=both")).await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn timeline_bounds_buckets() {
    let reply = get(&format!("/data/timeline?{VIEW}&g=agents&buckets=24")).await;
    assert_eq!(reply.status, StatusCode::OK);
    let body = json(&reply);
    assert!(body["buckets"].is_array());
    assert_eq!(body["window"]["from"], "2026-10-02T00:00:00Z");

    for bad in ["0", "5000", "x"] {
        let reply = get(&format!("/data/timeline?{VIEW}&g=agents&buckets={bad}")).await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "buckets={bad}");
    }
}

#[tokio::test]
async fn projection_validates_its_id() {
    let reply = get("/data/projection/not-a-ulid").await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);

    let reply = get("/data/projection/01J9ZQ3W8D7ZZZZZZZZZZZZZZZ").await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);
}

#[test]
fn require_checks_the_permission() {
    let caller = Caller {
        operator: OperatorId::from_ulid(2),
        permissions: vec![Permission::View],
    };
    assert!(require(&caller, Permission::View).is_ok());
    assert!(require(&caller, Permission::Content).is_err());
}
