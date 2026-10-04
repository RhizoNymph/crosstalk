//! Router tests of the topology page and its drawer against the fixture
//! world, plus the drawer's argument validation.

use std::num::NonZeroU32;

use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use topcoat::router::StatusCode;

use super::drawer::model::{Drawer, load};
use super::selection::Selection;
use super::*;
use crate::backend::fixture::FixtureBackend;
use crate::testing::{cx, get};
use crate::url::ulid::UlidId;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::paging::PageRequest;

/// The default window of the fixture world (the last day) under v2.
pub fn fixture_state() -> ViewState {
    let mut state = crate::components::href::tests::state();
    state.scope.topic_version = TopicModelVersion(2);
    state
}

fn everyone() -> Caller {
    crate::testing::operator().caller()
}

fn first<L>(limit: u32) -> PageRequest<L> {
    crate::pages::common::paging::first(NonZeroU32::new(limit).expect("limit"))
}

/// The heaviest edge of the fixture's default view, as a selection.
async fn heaviest_edge() -> Selection {
    let backend = FixtureBackend::try_new(7).expect("fixture generates");
    let scope = fixture_state().scope;
    let graph = backend
        .topology(
            &everyone(),
            scope.window,
            Weighting::Transmissions,
            &scope.topology_filter(),
        )
        .await
        .expect("topology");
    let edge = graph
        .value
        .edges
        .iter()
        .max_by(|a, b| a.share.get().total_cmp(&b.share.get()))
        .expect("an edge");
    Selection::edge(edge.from, edge.to, &edge.route)
}

pub async fn agent_labelled(label: &str) -> crosstalk_spec::ids::AgentId {
    let backend = FixtureBackend::try_new(7).expect("fixture generates");
    let page = backend
        .agents(
            &everyone(),
            &crate::contract::agents::AgentListFilter::default(),
            &first(500),
        )
        .await
        .expect("agents");
    page.items()
        .iter()
        .find(|a| a.label.as_ref().is_some_and(|l| l.as_str() == label))
        .map(|a| a.id)
        .expect("labelled agent")
}

pub async fn wiki_channel() -> crosstalk_spec::ids::ChannelId {
    crate::testing::channel_id(crate::backend::fixture::ChannelKey::HijackedWiki)
}

/// The fixture's bucket width: five minutes.
fn bucket() -> BucketWidth {
    BucketWidth::from_micros(std::num::NonZeroU64::new(300_000_000).expect("five minutes"))
}

fn url(extra: &str) -> String {
    format!("/topology?{}{extra}", fixture_state().to_query())
}

#[test]
fn brush_shows_a_week_in_hours() {
    let state = fixture_state();
    let now = state.scope.window.end();
    let (window, buckets) = brush_window(state.scope.window, now, bucket());
    assert_eq!(buckets, 168);
    assert_eq!(window.end(), now);
    let src = timeline_src(&state, now, bucket());
    assert!(src.starts_with("/data/timeline?from=2026-09-26T00:00:00Z&to=2026-10-03T00:00:00Z"));
    assert!(src.ends_with("&buckets=168"));
}

#[test]
fn the_brush_window_is_aligned_whatever_the_present() {
    let state = fixture_state();
    // A wall clock between bucket boundaries.
    let now = Timestamp::from_micros(state.scope.window.end().as_micros() + 61_000_000);
    let (window, _) = brush_window(state.scope.window, now, bucket());
    assert!(
        crate::url::scope::is_aligned(window, bucket()),
        "{window:?}"
    );
    assert!(window.end() >= now && window.start() <= state.scope.window.start());
}

/// What the brush's `change` handler navigates to: the page URL with
/// `from` and `to` replaced by two of the payload's bucket edges.
fn brushed(from: &str, to: &str) -> String {
    let kept: Vec<String> = fixture_state()
        .to_query()
        .split('&')
        .filter(|pair| !pair.starts_with("from=") && !pair.starts_with("to="))
        .map(str::to_owned)
        .collect();
    format!("/topology?from={from}&to={to}&{}", kept.join("&"))
}

#[tokio::test]
async fn a_brushed_window_is_canonical() {
    let state = fixture_state();
    let now = state.scope.window.end();
    let reply = get(&timeline_src(&state, now, bucket())).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let payload: serde_json::Value = serde_json::from_str(&reply.body).expect("json");
    let buckets = payload["buckets"].as_array().expect("buckets");
    assert_eq!(buckets.len(), 168);
    let from = buckets[100]["from"].as_str().expect("from");
    let to = buckets[130]["to"].as_str().expect("to");
    let reply = get(&brushed(from, to)).await;
    assert_eq!(
        reply.status,
        StatusCode::OK,
        "a brushed URL is served without a redirect: {:?}",
        reply.location
    );
}

#[tokio::test]
async fn incomplete_urls_redirect_to_canonical() {
    let reply = get("/topology").await;
    assert_eq!(reply.status, StatusCode::TEMPORARY_REDIRECT);
}

#[tokio::test]
async fn renders_graph_brush_filter_and_heaviest_edges() {
    let reply = get(&url("")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let body = &reply.body;
    assert!(body.contains("<ct-topology"));
    assert!(body.contains("data-src=\"/data/topology?from=2026-10-02T00:00:00Z"));
    assert!(body.contains("<ct-timebrush"));
    assert!(body.contains("buckets=168"));
    assert!(
        body.contains("final up to 2026-10-02 23:50:00 UTC"),
        "watermark"
    );
    assert!(body.contains("Heaviest edges"));
    assert!(body.contains("data-select=\"edge:"), "selectable edges");
    assert!(body.contains("name=\"fa\""), "agent choices");
    assert!(
        body.contains(">pi-scraper</span>"),
        "labelled agents are offered"
    );
    assert!(body.contains("wiki.example.org/wiki/Agent_Coordination"));
    assert!(body.contains("name=\"ft\""), "topics offered with Content");
}

#[tokio::test]
async fn a_selected_edge_lists_its_transmissions() {
    let sel = heaviest_edge().await.encode();
    let reply = get(&url(&format!("&sel={sel}"))).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("Transmissions on this edge"));
    assert!(reply.body.contains("href=\"/transmissions/"));
    assert!(reply.body.contains(&format!("data-drawer=\"{sel}\"")));
    assert!(reply.body.contains(" of transmissions"));
}

#[tokio::test]
async fn agent_and_channel_selections_show_cards() {
    let agent = agent_labelled("pi-scraper").await;
    let reply = get(&url(&format!("&sel=agent:{}", agent.to_ulid()))).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("Open agent page"));
    assert!(
        reply.body.contains("claims"),
        "harness claims shown as claims"
    );

    let wiki = wiki_channel().await;
    let reply = get(&url(&format!("&sel=channel:{}", wiki.to_ulid()))).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("Open channel page"));
    assert!(reply.body.contains("Edges through it"));
    assert!(reply.body.contains("unreviewed"));
}

#[tokio::test]
async fn bad_page_keys_are_422() {
    let reply = get(&url("&sel=node:1")).await;
    assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(reply.body.contains("sel: unknown selection kind"));
    let reply = get(&url("&collapse=yes")).await;
    assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn filter_form_redirects_to_the_canonical_filter() {
    let reply = get(&url(
        "&apply=1&fr=channel&fr=direct&fx=exclude-false&collapse=1",
    ))
    .await;
    assert_eq!(reply.status, StatusCode::SEE_OTHER, "{}", reply.body);
    let location = reply.location.expect("location");
    assert!(
        location.contains("&g=agents&r=channel,direct&x=exclude-false&collapse=1"),
        "{location}"
    );
    let reply = get(&url("&apply=1&fr=teleport")).await;
    assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn active_filters_show_as_chips_and_toggles_carry_state() {
    let reply = get(&url("&r=channel&collapse=1")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("Clear all"));
    assert!(reply.body.contains("data-collapse=\"true\""));
    assert!(
        reply
            .body
            .contains("g=channels&amp;r=channel&amp;collapse=1"),
        "mode toggle keeps filter and keys"
    );
}

#[tokio::test]
async fn drawer_arguments_are_validated() {
    let cx = cx();
    let caller = everyone();
    let state = fixture_state().to_query();
    let bad_state = load(&cx, &caller, "from=yesterday", "", "").await;
    assert!(matches!(
        bad_state,
        Err(UiError::Field { field: "state", .. })
    ));
    let bad_sel = load(&cx, &caller, &state, "edge:x", "").await;
    assert!(matches!(bad_sel, Err(UiError::Field { field: "sel", .. })));
    let bad_cursor = load(&cx, &caller, &state, "", "a b").await;
    assert!(bad_cursor.is_err());
    let viewer = crate::testing::caller_of(OperatorId::from_ulid(1), &[Permission::Content]);
    assert_eq!(
        load(&cx, &viewer, &state, "", "").await.map(|_| ()),
        Err(UiError::Query(QueryError::Forbidden {
            missing: Permission::View
        }))
    );
    let unknown = format!("agent:{}", "01J9ZQ3W8D0000000000000999");
    let (_, drawer) = load(&cx, &caller, &state, &unknown, "")
        .await
        .expect("load");
    assert_eq!(drawer, Drawer::Missing("No agent has this id."));
}

#[tokio::test]
async fn edge_pages_follow_the_cursor() {
    let cx = cx();
    let caller = everyone();
    let state = fixture_state().to_query();
    let sel = heaviest_edge().await.encode();
    let (_, first) = load(&cx, &caller, &state, &sel, "").await.expect("load");
    let Drawer::Edge(first) = first else {
        panic!("edge drawer")
    };
    assert!(!first.paged);
    let next = first
        .next
        .clone()
        .expect("the heaviest edge has a second page");
    let (_, second) = load(&cx, &caller, &state, &sel, &next).await.expect("load");
    let Drawer::Edge(second) = second else {
        panic!("edge drawer")
    };
    assert!(second.paged);
    assert_ne!(
        first.rows.first().map(|r| r.id),
        second.rows.first().map(|r| r.id)
    );
}
