//! The UI over the http backend against a real HTTP server in this
//! process (`crate::testing::http`): the world's surface served by
//! `crosstalk_api::http`, read by `crosstalk_client`.

use std::num::NonZeroU32;
use std::pin::Pin;
use std::time::Duration;

use crosstalk_client::BearerToken;
use crosstalk_spec::aggregates::alert::AlertState;
use crosstalk_spec::ids::AlertId;
use crosstalk_spec::interfaces::l8_surface::audit::AuditFilter;
use crosstalk_spec::interfaces::l8_surface::{
    AlertFilter, AlertStateKind, OperatorAction, OperatorActions, Permission, PermissionSet,
    QueryApi, QueryError,
};
use futures_core::Stream;
use topcoat::router::request::Request;
use topcoat::router::{Body, BodyDataStream, Router, StatusCode};

use super::identity::IdentityError;
use crate::config::{HttpConfig, OperatorPick};
use crate::pages::common::paging::first;
use crate::testing::http::{
    HttpWorld, ONCALL_TOKEN, RESEARCHER_TOKEN, UNKNOWN_TOKEN, oncall, researcher, stranger,
};
use crate::testing::{Reply, get_from, send_to};
use crate::url::ulid::UlidId;
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorConfig, OperatorName,
};
use crosstalk_world::config::{OPERATOR_ONCALL, OPERATOR_RESEARCHER};

/// The outer bound on waiting for a condition (a refresh), however busy
/// the machine.
const WAIT: Duration = Duration::from_secs(60);

/// How long the live test waits for its own event, however busy the
/// machine: an outer bound, not the expected time.
const LIVE_DEADLINE: Duration = Duration::from_secs(120);

/// Every section the navigation links to, and the root.
const NAV: [&str; 10] = [
    "/",
    "/topology",
    "/explore",
    "/topics",
    "/channels",
    "/agents",
    "/alerts",
    "/export",
    "/audit",
    "/pipeline",
];

/// `uri` through `router`, following redirects as `curl -L` does: the
/// last reply and the path it came from.
async fn follow(router: &Router, uri: &str) -> (String, Reply) {
    let mut at = uri.to_owned();
    for _ in 0..5 {
        let reply = get_from(router, &at).await;
        if !reply.status.is_redirection() {
            return (at, reply);
        }
        at = reply.location.clone().expect("a redirect has a location");
    }
    panic!("{uri}: too many redirects");
}

/// The researcher's router over HTTP.
async fn researcher_router(world: &HttpWorld) -> Router {
    let access = world
        .access(RESEARCHER_TOKEN, researcher())
        .await
        .expect("the researcher's access");
    world.router(RESEARCHER_TOKEN, access)
}

/// The oldest open alert, read on the surface itself.
async fn open_alert(world: &HttpWorld) -> AlertId {
    let filter = AlertFilter {
        states: vec![AlertStateKind::Open],
        channel: None,
    };
    world
        .surface
        .alerts(&world.researcher().await, &filter, &first(NonZeroU32::MIN))
        .await
        .expect("alerts")
        .items()
        .first()
        .expect("the world has an open alert")
        .id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_learns_the_tokens_operator_and_permissions_from_the_server() {
    let world = HttpWorld::start().await;
    let started = super::start(&HttpConfig {
        url: world.base.clone(),
        token: BearerToken::new(RESEARCHER_TOKEN).expect("token"),
        operator: researcher(),
    })
    .await
    .expect("the http backend starts");
    let access = started.identity.current();
    assert_eq!(access.name(), "researcher");
    assert_eq!(access.caller().permissions(), PermissionSet::ALL);
    started.refresh.abort();

    let oncall_access = world
        .access(ONCALL_TOKEN, oncall())
        .await
        .expect("on-call access");
    assert_eq!(oncall_access.name(), "oncall");
    assert_eq!(
        oncall_access.caller().permissions(),
        PermissionSet::of([Permission::View, Permission::Content, Permission::Triage])
    );

    // The world has two operators, so the only-one pick needs an id.
    assert!(matches!(
        world
            .access(RESEARCHER_TOKEN, OperatorPick::TheOnlyOne)
            .await,
        Err(IdentityError::Several { ids }) if ids.len() == 2
    ));
    assert!(matches!(
        world.access(RESEARCHER_TOKEN, stranger()).await,
        Err(IdentityError::NotListed(_))
    ));
    // A token the server does not know: the 401 is a store failure.
    assert!(matches!(
        world.access(UNKNOWN_TOKEN, researcher()).await,
        Err(IdentityError::Read(QueryError::Store { .. }))
    ));
    world.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_nav_page_answers_200_over_http() {
    let world = HttpWorld::start().await;
    let router = researcher_router(&world).await;
    for path in NAV {
        let (at, reply) = follow(&router, path).await;
        assert_eq!(
            reply.status,
            StatusCode::OK,
            "{path} ({at}): {}",
            reply.body
        );
        assert!(
            reply.body.contains("signed in as researcher"),
            "{path}: the server's name for the token"
        );
        assert!(
            !reply.body.contains("the gateway&#39;s store failed")
                && !reply.body.contains("the gateway's store failed"),
            "{path}: {}",
            reply.body
        );
    }
    drop(router);
    world.stop().await;
}

/// The next SSE frame; the caller bounds the wait.
async fn next_frame(body: &mut BodyDataStream) -> String {
    let next = std::future::poll_fn(|cx| Pin::new(&mut *body).poll_next(cx)).await;
    let bytes = next.expect("the stream is open").expect("a frame");
    String::from_utf8(bytes.to_vec()).expect("utf8")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_event_reaches_data_live_over_http() {
    let world = HttpWorld::start().await;
    let router = researcher_router(&world).await;
    let response = router
        .handle(
            Request::builder()
                .uri("/data/live")
                .body(Body::empty())
                .expect("request"),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();

    let alert = open_alert(&world).await;
    world
        .surface
        .act(
            &world.researcher().await,
            OperatorAction::Acknowledge { alert },
        )
        .await
        .expect("acknowledge");
    let wanted = format!(
        "event: alert\ndata: {{\"id\":\"{}\"}}\nid: ",
        alert.to_ulid()
    );
    // The world is settled before it is served (`seed_world`), but another
    // write may still land first: skip any frame that is not ours, under
    // one generous deadline rather than a frame count.
    let mut skipped = 0_usize;
    let found = tokio::time::timeout(LIVE_DEADLINE, async {
        loop {
            let frame = next_frame(&mut body).await;
            if frame.starts_with(&wanted) {
                return;
            }
            assert!(
                !frame.starts_with("event: end"),
                "the stream ended after {skipped} other frames: {frame}"
            );
            skipped += 1;
        }
    })
    .await;
    assert!(
        found.is_ok(),
        "no alert event within {LIVE_DEADLINE:?} ({skipped} other frames)"
    );
    drop(body);
    drop(router);
    world.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_operator_action_round_trips_over_http() {
    let world = HttpWorld::start().await;
    let router = researcher_router(&world).await;
    let alert = open_alert(&world).await;
    let (url, reply) = follow(&router, &format!("/alerts/{}", alert.to_ulid())).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(
        reply.body.contains(">Acknowledge</button>"),
        "{}",
        reply.body
    );

    let posted = router
        .handle(
            Request::builder()
                .method("POST")
                .uri(&url)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("action=acknowledge"))
                .expect("request"),
        )
        .await;
    assert_eq!(posted.status(), StatusCode::SEE_OTHER);

    // The surface behind the server holds the acknowledgement, by the
    // token's operator.
    let stored = world
        .surface
        .alert(&world.researcher().await, alert)
        .await
        .expect("alert")
        .expect("the alert exists");
    assert!(
        matches!(stored.state, AlertState::Acknowledged { by, .. } if by == OPERATOR_RESEARCHER),
        "{:?}",
        stored.state
    );
    let (_, reply) = follow(&router, &url).await;
    assert!(
        reply.body.contains(">acknowledged</span>"),
        "{}",
        reply.body
    );
    drop(router);
    world.stop().await;
}

/// Asserts `reply` is the full-page gateway state: `status`, `title`, the
/// gateway's URL, the layout around it, and no token.
fn assert_gateway_page(reply: &Reply, status: StatusCode, title: &str, url: &str, context: &str) {
    assert_eq!(reply.status, status, "{context}: {}", reply.body);
    assert!(reply.body.contains(title), "{context}: {}", reply.body);
    assert!(reply.body.contains(url), "{context}: the gateway url {url}");
    assert!(
        reply.body.starts_with("<!DOCTYPE html>") && reply.body.contains("signed in as"),
        "{context}: a full page in the layout"
    );
    for token in [RESEARCHER_TOKEN, ONCALL_TOKEN, UNKNOWN_TOKEN] {
        assert!(
            !reply.body.contains(token),
            "{context}: a token on the page"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_token_renders_the_token_refused_page() {
    let world = HttpWorld::start().await;
    let access = world
        .access(RESEARCHER_TOKEN, researcher())
        .await
        .expect("access");
    // The token is rotated on the server after the UI learned who it is.
    let router = world.router(UNKNOWN_TOKEN, access);
    let url = world.base.to_string();
    for path in NAV {
        let (at, reply) = follow(&router, path).await;
        assert_gateway_page(
            &reply,
            StatusCode::BAD_GATEWAY,
            "The gateway refused the token",
            &url,
            &format!("{path} ({at})"),
        );
    }
    // An action posted meanwhile gets the same page, not a 500.
    let posted = send_to(
        &router,
        Request::builder()
            .method("POST")
            .uri("/pipeline")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("action=replay"))
            .expect("request"),
    )
    .await;
    assert_gateway_page(
        &posted,
        StatusCode::BAD_GATEWAY,
        "The gateway refused the token",
        &url,
        "POST /pipeline",
    );
    // Data routes keep their status: the elements show their own error.
    let live = router
        .handle(
            Request::builder()
                .uri("/data/live")
                .body(Body::empty())
                .expect("request"),
        )
        .await;
    assert_eq!(live.status(), StatusCode::INTERNAL_SERVER_ERROR);
    drop(router);
    world.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_gateway_renders_the_unreachable_page() {
    let world = HttpWorld::start().await;
    let access = world
        .access(RESEARCHER_TOKEN, researcher())
        .await
        .expect("access");
    let router = world.router(RESEARCHER_TOKEN, access);
    let url = world.base.to_string();
    world.stop().await;
    for path in NAV {
        let (at, reply) = follow(&router, path).await;
        assert_gateway_page(
            &reply,
            StatusCode::SERVICE_UNAVAILABLE,
            "The gateway is unreachable",
            &url,
            &format!("{path} ({at})"),
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_gateway_page_is_not_a_route_of_its_own() {
    let world = HttpWorld::start().await;
    let router = researcher_router(&world).await;
    let reply = get_from(&router, "/_gateway").await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND, "{}", reply.body);
    drop(router);
    world.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_ui_gates_on_the_servers_permissions_for_the_token() {
    let world = HttpWorld::start().await;
    let access = world.access(ONCALL_TOKEN, oncall()).await.expect("access");
    let caller = access.caller();
    let router = world.router(ONCALL_TOKEN, access);
    let (_, reply) = follow(&router, "/alerts").await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("signed in as oncall"));
    // On-call may not audit: the UI says so, and the server agrees.
    let (_, audit) = follow(&router, "/audit").await;
    assert_eq!(audit.status, StatusCode::FORBIDDEN);
    assert!(
        audit.body.contains("this needs the Audit permission"),
        "{}: {}",
        audit.status,
        audit.body
    );
    let served = world
        .client(ONCALL_TOKEN)
        .audit(&caller, &AuditFilter::default(), &first(NonZeroU32::MIN))
        .await;
    assert_eq!(
        served.err(),
        Some(QueryError::Forbidden {
            missing: Permission::Audit
        })
    );
    drop(router);
    world.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_identity_follows_a_permission_change_on_the_server() {
    let world = HttpWorld::start().await;
    let access = world
        .access(RESEARCHER_TOKEN, researcher())
        .await
        .expect("access");
    let (sender, mut receiver) = tokio::sync::watch::channel(access);
    let refresh = super::identity::spawn_refresh(
        world.client(RESEARCHER_TOKEN),
        OPERATOR_RESEARCHER,
        sender,
        Duration::from_millis(50),
    );
    let fewer = PermissionSet::of([Permission::View, Permission::Content]);
    world
        .load_access(&AccessConfig::Authenticated(vec![
            OperatorConfig {
                id: OPERATOR_RESEARCHER,
                name: OperatorName::new("researcher").expect("name"),
                permissions: fewer,
            },
            OperatorConfig {
                id: OPERATOR_ONCALL,
                name: OperatorName::new("oncall").expect("name"),
                permissions: PermissionSet::ALL,
            },
        ]))
        .await;
    tokio::time::timeout(WAIT, receiver.changed())
        .await
        .expect("a refresh in time")
        .expect("the refresher runs");
    assert_eq!(receiver.borrow().caller().permissions(), fewer);
    refresh.abort();
    world.stop().await;
}
