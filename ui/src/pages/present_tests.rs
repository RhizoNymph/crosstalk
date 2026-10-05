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
