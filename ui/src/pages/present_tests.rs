//! Pages read the backend's present through `QueryApi::present`, once per
//! request: the view defaults, the layout and every component share the
//! first read (`crate::app::present`).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use topcoat::router::{Router, StatusCode};

use crate::backend::fixture::FixtureBackend;
use crate::pages::topology::tests::fixture_state;
use crate::testing::{Reply, SEED, get_from, router_over};

/// A router over a fresh fixture, and its count of present reads.
fn counted() -> (Router, Arc<AtomicUsize>) {
    let backend = FixtureBackend::try_new(SEED).expect("fixture generates");
    let reads = backend.present_reads();
    (router_over(backend), reads)
}

/// `uri`'s reply and how many times serving it read the present.
async fn reads_for(uri: &str) -> (Reply, usize) {
    let (router, reads) = counted();
    let reply = get_from(&router, uri).await;
    (reply, reads.load(Ordering::SeqCst))
}

#[tokio::test]
async fn the_topology_page_reads_the_present_once() {
    let state = fixture_state();
    let (reply, reads) = reads_for(&format!("/topology?{}", state.to_query())).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert_eq!(reads, 1);
    // The time brush is built from the present: its grid and its window
    // reach the fixture's `now`.
    assert!(reply.body.contains("/data/timeline?"), "time brush");
}

#[tokio::test]
async fn the_export_page_reads_the_present_once_and_offers_its_formats() {
    let state = fixture_state();
    let (reply, reads) = reads_for(&format!("/export?{}", state.to_query())).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert_eq!(reads, 1);
    assert!(reply.body.contains("JSON Lines"), "{}", reply.body);
    assert!(
        reply
            .body
            .contains("Parquet (not available on this backend)"),
        "the fixture writes JSONL only"
    );
}

#[tokio::test]
async fn a_redirect_to_the_canonical_view_reads_the_present_once() {
    let (reply, reads) = reads_for("/topology").await;
    assert!(reply.status.is_redirection(), "{}", reply.status);
    assert_eq!(reads, 1);
}

#[tokio::test]
async fn pages_that_read_trends_or_rule_versions_read_the_present_once() {
    let query = fixture_state().to_query();
    for path in ["/topics", "/explore", "/alerts/rules/new?kind=watched", "/"] {
        let sep = if path.contains('?') { '&' } else { '?' };
        let (reply, reads) = reads_for(&format!("{path}{sep}{query}")).await;
        assert_eq!(reply.status, StatusCode::OK, "{path}: {}", reply.body);
        assert_eq!(reads, 1, "{path}");
    }
}

#[tokio::test]
async fn the_timeline_route_reads_the_present_once() {
    let query = fixture_state().to_query();
    let (reply, reads) = reads_for(&format!("/data/timeline?{query}&buckets=24")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert_eq!(reads, 1);
}

/// The new watched-topic rule form's threshold input, as rendered.
async fn remap_default(router: &Router) -> String {
    let query = fixture_state().to_query();
    let reply = get_from(router, &format!("/alerts/rules/new?{query}&kind=watched")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let at = reply
        .body
        .find("name=\"remap_threshold\" value=\"")
        .expect("threshold input");
    let rest = &reply.body[at + "name=\"remap_threshold\" value=\"".len()..];
    rest[..rest.find('"').expect("value end")].to_owned()
}

fn similarity(value: f32) -> crosstalk_spec::support::Similarity {
    crosstalk_spec::support::Similarity::new(value).expect("similarity")
}

#[tokio::test]
async fn the_rule_form_defaults_to_the_fixtures_remap_threshold() {
    let (router, _) = counted();
    assert_eq!(remap_default(&router).await, "0.80");
}

#[tokio::test]
async fn the_rule_form_default_is_the_presents_not_the_uis() {
    let backend = FixtureBackend::try_new(SEED)
        .expect("fixture generates")
        .with_default_remap(similarity(0.55));
    assert_eq!(remap_default(&router_over(backend)).await, "0.55");
}

#[tokio::test]
async fn the_world_backend_offers_its_own_remap_threshold() {
    let (world, in_process) = crate::backend::world::WorldBackend::start(SEED)
        .await
        .expect("world starts");
    let router = crate::testing::router_over_app(crate::backend::AppBackend::World(world));
    assert_eq!(remap_default(&router).await, "0.65");
    drop(router);
    in_process.shutdown().await;
}

/// How many of v1's topics the topics page's remap table leaves unmapped.
async fn unmapped_in_v1(router: &Router) -> usize {
    let query = fixture_state().to_query();
    let reply = get_from(router, &format!("/topics?{query}&ver=1")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("Remap v1 → v2"), "the remap table");
    reply.body.matches("unmapped").count()
}

#[tokio::test]
async fn the_topics_remap_table_maps_at_the_presents_threshold() {
    let (router, _) = counted();
    let at_default = unmapped_in_v1(&router).await;
    assert!(at_default > 0, "at 0.80 a v1 topic is unmapped");
    let backend = FixtureBackend::try_new(SEED)
        .expect("fixture generates")
        .with_default_remap(similarity(0.0));
    let at_zero = unmapped_in_v1(&router_over(backend)).await;
    assert!(at_zero < at_default, "{at_zero} < {at_default}");
}
