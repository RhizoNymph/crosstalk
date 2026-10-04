//! The data routes through the router: statuses, content types and the
//! strict query, against the fixture backend.

use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::Permission;
use topcoat::router::header::CONTENT_TYPE;
use topcoat::router::request::Request;
use topcoat::router::{Body, Router, RouterBuilderDiscoverExt, StatusCode, to_bytes};

use super::require;
use crate::backend::fixture::FixtureBackend;

const VIEW: &str = "from=2026-10-02T00:00:00Z&to=2026-10-03T00:00:00Z&v=2&w=tx";

struct Reply {
    status: StatusCode,
    content_type: String,
    body: Vec<u8>,
}

async fn get(uri: &str) -> Reply {
    get_from(FixtureBackend::try_new(7).expect("fixture generates"), uri).await
}

/// `uri` through a router serving `backend`.
async fn get_from(backend: FixtureBackend, uri: &str) -> Reply {
    let router = Router::builder()
        .discover()
        .app_context(crate::testing::operator())
        .app_context(backend)
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
async fn channels_mode_marks_unconfirmed_channels_and_confirmed_only_drops_them() {
    let channel_nodes = |body: &serde_json::Value| -> Vec<serde_json::Value> {
        body["nodes"]
            .as_array()
            .expect("nodes")
            .iter()
            .filter(|n| n["kind"] == "channel")
            .cloned()
            .collect()
    };
    let week = "from=2026-09-26T00:00:00Z&to=2026-10-03T00:00:00Z&v=2&w=tx&g=channels";
    let all = json(&get(&format!("/data/topology?{week}")).await);
    let nodes = channel_nodes(&all);
    assert!(nodes.iter().any(|n| n["confirmation"] == "unconfirmed"));
    assert!(
        nodes.iter().all(|n| !n["name"]
            .as_str()
            .unwrap_or_default()
            .contains("scratch/notes")),
        "a resource one agent uses is no channel"
    );
    let confirmed = json(&get(&format!("/data/topology?{week}&u=confirmed")).await);
    let kept = channel_nodes(&confirmed);
    assert!(kept.iter().all(|n| n["confirmation"] == "confirmed"));
    assert_eq!(
        kept.len() + 1,
        nodes.len(),
        "only the unconfirmed channel goes"
    );
}

#[tokio::test]
async fn a_dropped_topic_version_is_a_400_naming_it() {
    let dropped = VIEW.replace("v=2", "v=0");
    let reply = get(&format!("/data/topology?{dropped}&g=agents")).await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    let text = String::from_utf8(reply.body).expect("utf8");
    assert!(text.contains("version 0 is no longer retained"), "{text}");
}

#[tokio::test]
async fn projections_answer_with_their_payload_or_why_not() {
    use crosstalk_spec::aggregates::projection::ProjectionStatusKind;
    use crosstalk_spec::aggregates::projection::{ProjectionLimit, ProjectionParams};

    use crate::pages::common::paging::first;
    use crate::url::ulid::UlidId;
    use crosstalk_spec::interfaces::l8_surface::QueryApi;

    let backend = FixtureBackend::try_new(7).expect("fixture generates");
    let caller = crate::testing::operator().caller();
    let jobs = backend
        .projections(&caller, &first(50u16))
        .await
        .expect("jobs");
    let queued = jobs
        .items()
        .iter()
        .find(|info| info.status().kind() == ProjectionStatusKind::Queued)
        .expect("a queued job")
        .id();
    let expired = jobs
        .items()
        .iter()
        .find(|info| info.status().kind() == ProjectionStatusKind::Expired)
        .expect("an expired job")
        .id();
    let reply = get(&format!("/data/projection/{}", queued.to_ulid())).await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    assert!(String::from_utf8_lossy(&reply.body).contains("is queued"));
    let reply = get(&format!("/data/projection/{}", expired.to_ulid())).await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);

    // A fit answers with its binary payload: every channel-routed point
    // names its channel, read from the transmissions' rows.
    let params = ProjectionParams::new(ProjectionLimit::new(400).expect("limit"), 15, 100, 3)
        .expect("params");
    let window = crosstalk_spec::support::TimeWindow::new(
        crosstalk_spec::support::Timestamp::from_micros(1_790_899_200_000_000),
        crosstalk_spec::support::Timestamp::from_micros(1_790_985_600_000_000),
    )
    .expect("window");
    let id = backend
        .fit_projection(&caller, window, &Default::default(), params)
        .await
        .expect("fit");
    let reply = get_from(backend, &format!("/data/projection/{}", id.to_ulid())).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.content_type, "application/octet-stream");
    let decoded = super::projection::decode::decode(&reply.body).expect("decodes");
    assert_eq!(decoded.header.count, 400);
    assert!(!decoded.header.channels.is_empty());
    for (route, channel) in decoded.routes.iter().zip(&decoded.channels) {
        assert_eq!(*route == 0, channel.is_some());
    }
    assert!(decoded.header.topics.iter().all(|t| t.label.is_some()));
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
    let caller = crate::testing::caller_of(OperatorId::from_ulid(2), &[Permission::View]);
    assert!(require(&caller, Permission::View).is_ok());
    assert!(require(&caller, Permission::Content).is_err());
}
