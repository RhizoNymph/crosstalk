//! Follow mode through the router: `/` and `/topology` follow the present
//! by default and resolve `follow=<span>` on every render; every other
//! page and every data route takes a pinned window.

use topcoat::router::{Router, StatusCode};

use crate::backend::fixture::FixtureBackend;
use crate::pages::topology::tests::fixture_state;
use crate::testing::fixture_api::FixtureApi;
use crate::testing::http::RESEARCHER_TOKEN;
use crate::testing::{Reply, SEED, get, get_from, router_over};

/// The fixture's last day under its active version, followed.
const FOLLOWED: &str = "follow=1d&v=2&w=tx&g=agents";
/// [`FOLLOWED`] resolved on the fixed clock (`NOW` is 2026-10-03T00:00Z).
const RESOLVED: &str = "from=2026-10-02T00:00:00Z&to=2026-10-03T00:00:00Z&v=2&w=tx&g=agents";

/// The `data-src` of the page's `<ct-topology>`.
fn topology_src(body: &str) -> String {
    let at = body.find("<ct-topology").expect("the graph");
    let rest = &body[at..];
    let start = rest.find("data-src=\"").expect("its data-src") + "data-src=\"".len();
    let end = rest[start..].find('"').expect("closed") + start;
    rest[start..end].replace("&amp;", "&")
}

/// The href of the follow bar's Pin link.
fn pin_href(body: &str) -> String {
    let at = body.find(">Pin</a>").expect("a Pin link");
    let before = &body[..at];
    let start = before.rfind("href=\"").expect("its href") + "href=\"".len();
    let end = before[start..].find('"').expect("closed") + start;
    before[start..end].replace("&amp;", "&")
}

async fn assert_redirect(router: &Router, uri: &str, to: &str) {
    let reply = get_from(router, uri).await;
    assert_eq!(
        reply.status,
        StatusCode::TEMPORARY_REDIRECT,
        "{uri}: {}",
        reply.body
    );
    assert_eq!(reply.location.as_deref(), Some(to), "{uri}");
}

/// The checks every backend passes: the default redirect, and a followed
/// `/` and `/topology` with the bar, Pin and pinned data sources.
async fn follows_on(router: &Router, version: u32) {
    let followed = format!("follow=1d&v={version}&w=tx&g=agents");
    for path in ["/", "/topology"] {
        assert_redirect(router, path, &format!("{path}?{followed}")).await;
        let reply = get_from(router, &format!("{path}?{followed}")).await;
        assert_eq!(reply.status, StatusCode::OK, "{path}: {}", reply.body);
        assert!(reply.body.contains("Following the last 1 d"), "{path}");
        assert!(reply.body.contains("data-live-follow=\"1d\""), "{path}");
        let pin = pin_href(&reply.body);
        assert!(pin.starts_with(&format!("{path}?from=")), "{path}: {pin}");
        assert!(!pin.contains("follow="), "{path}: {pin}");
        let pinned = get_from(router, &pin).await;
        assert_eq!(pinned.status, StatusCode::OK, "{pin}: {}", pinned.body);
        assert!(!pinned.body.contains("Following the last"), "{pin}");
        assert!(pinned.body.contains(">Follow</a>"), "{pin}");
    }
    let reply = get_from(router, &format!("/topology?{followed}")).await;
    let src = topology_src(&reply.body);
    assert!(src.starts_with("/data/topology?from="), "{src}");
    let data = get_from(router, &src).await;
    assert_eq!(data.status, StatusCode::OK, "{src}: {}", data.body);
}

#[tokio::test]
async fn the_overview_and_topology_follow_the_last_day_by_default() {
    for path in ["/", "/topology"] {
        let reply = get(path).await;
        assert_eq!(reply.status, StatusCode::TEMPORARY_REDIRECT, "{path}");
        assert_eq!(
            reply.location.as_deref(),
            Some(format!("{path}?{FOLLOWED}").as_str())
        );
    }
}

#[tokio::test]
async fn a_url_with_every_key_but_a_window_still_redirects_to_follow() {
    for path in ["/", "/topology"] {
        let reply = get(&format!("{path}?v=2&w=tx&g=agents")).await;
        assert_eq!(reply.status, StatusCode::TEMPORARY_REDIRECT, "{path}");
        assert_eq!(
            reply.location.as_deref(),
            Some(format!("{path}?{FOLLOWED}").as_str())
        );
    }
}

#[tokio::test]
async fn an_incomplete_followed_url_is_completed_and_keeps_following() {
    let reply = get("/topology?follow=6h").await;
    assert_eq!(reply.status, StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        reply.location.as_deref(),
        Some("/topology?follow=6h&v=2&w=tx&g=agents")
    );
}

#[tokio::test]
async fn a_followed_topology_shows_the_bar_and_names_the_pinned_window_below_it() {
    let reply = get(&format!("/topology?{FOLLOWED}")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let body = &reply.body;
    assert!(body.contains("Following the last 1 d"));
    assert_eq!(pin_href(body), format!("/topology?{RESOLVED}"));
    assert_eq!(topology_src(body), format!("/data/topology?{RESOLVED}"));
    assert!(body.contains("/data/timeline?from="), "the brush is pinned");
    assert!(
        !body.contains("/data/timeline?follow"),
        "the brush is pinned"
    );
    assert!(
        body.contains("2026-10-02 00:00:00 UTC → 2026-10-03 00:00:00 UTC"),
        "the header shows the resolved window"
    );
    assert!(
        body.contains("id=\"topology-graph\" data-live-keep=\"\""),
        "the graph survives a refresh"
    );
    assert!(body.contains("id=\"topology-brush\" data-live-keep=\"\""));
    assert!(body.contains("data-live-watch=\"watermark\""));
    // The page's own links keep following.
    assert!(body.contains("href=\"/topology?follow=1d&amp;v=2&amp;w=tx&amp;g=channels"));
    assert!(body.contains("type=\"hidden\" name=\"follow\" value=\"1d\""));
}

#[tokio::test]
async fn a_followed_page_carries_follow_in_its_navigation() {
    let reply = get(&format!("/?{FOLLOWED}")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(
        reply
            .body
            .contains("href=\"/channels?follow=1d&amp;v=2&amp;w=tx&amp;g=agents\""),
        "{}",
        reply.body
    );
}

#[tokio::test]
async fn other_pages_resolve_follow_to_the_pinned_window_keeping_their_keys() {
    let reply = get(&format!("/channels?{FOLLOWED}&tab=review")).await;
    assert_eq!(
        reply.status,
        StatusCode::TEMPORARY_REDIRECT,
        "{}",
        reply.body
    );
    assert_eq!(
        reply.location.as_deref(),
        Some(format!("/channels?{RESOLVED}&tab=review").as_str())
    );
    for path in ["/explore", "/agents", "/alerts", "/topics"] {
        let reply = get(&format!("{path}?{FOLLOWED}")).await;
        assert_eq!(reply.status, StatusCode::TEMPORARY_REDIRECT, "{path}");
        assert_eq!(
            reply.location.as_deref(),
            Some(format!("{path}?{RESOLVED}").as_str())
        );
    }
}

#[tokio::test]
async fn pinned_urls_are_unchanged() {
    let query = fixture_state().to_query();
    assert_eq!(query, RESOLVED);
    for path in ["/", "/topology", "/channels", "/agents"] {
        let reply = get(&format!("{path}?{query}")).await;
        assert_eq!(reply.status, StatusCode::OK, "{path}: {}", reply.body);
        assert!(!reply.body.contains("Following the last"), "{path}");
        assert!(!reply.body.contains("data-live-follow"), "{path}");
    }
    let reply = get("/channels").await;
    assert_eq!(reply.status, StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        reply.location.as_deref(),
        Some(format!("/channels?{RESOLVED}").as_str()),
        "other pages still default to a pinned day"
    );
}

#[tokio::test]
async fn pinned_overview_and_topology_offer_follow() {
    let reply = get(&format!("/topology?{RESOLVED}&sel=")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(
        reply
            .body
            .contains("href=\"/topology?follow=1d&amp;v=2&amp;w=tx&amp;g=agents\"")
    );
    let bytes = RESOLVED.replace("w=tx", "w=bytes");
    let reply = get(&format!("/?{bytes}")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(
        reply
            .body
            .contains("href=\"/?follow=1d&amp;v=2&amp;w=bytes&amp;g=agents\""),
        "Follow keeps the rest of the view"
    );
}

#[tokio::test]
async fn follow_with_a_window_or_an_unknown_span_is_a_bad_request() {
    for uri in [
        format!("/topology?{FOLLOWED}&from=2026-10-02T00:00:00Z&to=2026-10-03T00:00:00Z"),
        format!("/?{FOLLOWED}&to=2026-10-03T00:00:00Z"),
        "/topology?follow=2d&v=2&w=tx&g=agents".to_owned(),
        "/?follow=24h".to_owned(),
        format!("/channels?{FOLLOWED}&from=2026-10-02T00:00:00Z"),
    ] {
        let reply = get(&uri).await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "{uri}");
        assert!(reply.body.contains("follow"), "{uri}: {}", reply.body);
    }
}

#[tokio::test]
async fn data_routes_refuse_follow() {
    for uri in [
        format!("/data/topology?{FOLLOWED}"),
        format!("/data/topology?{RESOLVED}&follow=1d"),
        format!("/data/timeline?{FOLLOWED}&buckets=24"),
    ] {
        let reply = get(&uri).await;
        assert_eq!(
            reply.status,
            StatusCode::BAD_REQUEST,
            "{uri}: {}",
            reply.body
        );
        assert!(reply.body.contains("follow"), "{uri}: {}", reply.body);
    }
}

#[tokio::test]
async fn the_header_says_provisional_only_when_the_window_passes_the_watermark() {
    // The fixed clock's watermark is 2026-10-02T23:50Z.
    for path in ["/", "/topology"] {
        let reply = get(&format!("{path}?{FOLLOWED}")).await;
        assert!(
            reply
                .body
                .contains("provisional after 2026-10-02 23:50:00 UTC"),
            "{path}: {}",
            reply.body
        );
        assert!(!reply.body.contains("final up to"), "{path}");
        let settled = "from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z&v=2&w=tx&g=agents";
        let reply = get(&format!("{path}?{settled}")).await;
        assert_eq!(reply.status, StatusCode::OK, "{path}: {}", reply.body);
        assert!(
            reply.body.contains("final up to 2026-10-02 23:50:00 UTC"),
            "{path}"
        );
        assert!(!reply.body.contains("provisional after"), "{path}");
    }
}

/// A router over the fixture replaying at `at` (micros).
fn replay_at(at: u64) -> Router {
    let at = crosstalk_spec::support::Timestamp::from_micros(at);
    router_over(FixtureBackend::try_replay_at(SEED, at).expect("fixture generates"))
}

/// The window a followed `/topology` resolves, as its Pin link names it.
async fn resolved_at(router: &Router) -> String {
    let reply: Reply = get_from(router, &format!("/topology?{FOLLOWED}")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    pin_href(&reply.body)
}

#[tokio::test]
async fn a_followed_window_slides_with_the_replay_clock() {
    const NOW: u64 = 1_790_985_600_000_000;
    const MINUTE: u64 = 60_000_000;
    let two_hours_ago = NOW - 120 * MINUTE;
    let first = resolved_at(&replay_at(two_hours_ago - 3 * MINUTE)).await;
    assert_eq!(
        first, "/topology?from=2026-10-01T22:00:00Z&to=2026-10-02T22:00:00Z&v=2&w=tx&g=agents",
        "[align_up(now) − 1 d, align_up(now))"
    );
    // Within the same bucket the window stands still.
    assert_eq!(resolved_at(&replay_at(two_hours_ago - MINUTE)).await, first);
    // The next bucket slides it by one bucket; the URL stays `follow=1d`.
    let later = replay_at(two_hours_ago + MINUTE);
    assert_eq!(
        resolved_at(&later).await,
        "/topology?from=2026-10-01T22:05:00Z&to=2026-10-02T22:05:00Z&v=2&w=tx&g=agents"
    );
    // A pinned default elsewhere still ends a bucket past the data's end.
    let reply = get_from(&later, "/channels").await;
    assert_eq!(
        reply.location.as_deref(),
        Some("/channels?from=2026-10-02T00:05:00Z&to=2026-10-03T00:05:00Z&v=2&w=tx&g=agents")
    );
}

#[tokio::test]
async fn the_fixture_backend_follows() {
    let router = router_over(FixtureBackend::try_new(SEED).expect("fixture generates"));
    follows_on(&router, 2).await;
}

#[tokio::test]
async fn the_world_backend_follows() {
    let (world, in_process) = crate::backend::world::WorldBackend::start(SEED)
        .await
        .expect("world starts");
    let router = crate::testing::router_over_app(crate::backend::AppBackend::World(world));
    let version = active_version(&router).await;
    follows_on(&router, version).await;
    drop(router);
    in_process.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_http_backend_follows() {
    let api = FixtureApi::start().await;
    let access = api.access(RESEARCHER_TOKEN).await.expect("access");
    let router = api.router(RESEARCHER_TOKEN, access);
    follows_on(&router, 2).await;
    drop(router);
    api.stop().await;
}

/// The topic version a backend's default view names.
async fn active_version(router: &Router) -> u32 {
    let reply = get_from(router, "/").await;
    let location = reply.location.expect("a redirect");
    let v = location
        .split(['?', '&'])
        .find_map(|pair| pair.strip_prefix("v="))
        .expect("a version");
    v.parse().expect("a number")
}
