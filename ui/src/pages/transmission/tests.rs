//! The evidence page against the fixture world: matched text, weaker
//! states, verdict posts and the view without `Content`.

use std::num::NonZeroU32;

use crate::error::UiError;
use crosstalk_spec::derived::provenance::matching::MatchKind;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, QueryError};
use topcoat::router::StatusCode;

use super::*;
use crate::backend::fixture::FixtureBackend;
use crate::contract::graph::TransmissionSummary;
use crate::testing::{cx, get, post};

fn everyone() -> Caller {
    crate::testing::operator().caller()
}

async fn week_scope(backend: &FixtureBackend) -> Scope {
    Scope {
        window: all_time(backend.now(&everyone()).await.expect("now")).expect("window"),
        topic_version: backend
            .current_topic_version(&everyone())
            .await
            .expect("version"),
        filter: TopologyFilter::default(),
    }
}

/// The newest transmissions of the fixture world.
async fn transmissions(limit: u32) -> Vec<TransmissionSummary> {
    let backend = FixtureBackend::new(7);
    backend
        .transmissions(
            &everyone(),
            &week_scope(&backend).await,
            &TransmissionSelector::All,
            &PageRequest::first(NonZeroU32::new(limit).expect("limit")),
        )
        .await
        .expect("transmissions")
        .items
}

async fn first_in(kind: TransmissionStateKind) -> TransmissionId {
    transmissions(5000)
        .await
        .into_iter()
        .find(|t| t.state == kind)
        .map(|t| t.id)
        .expect("a transmission in that state")
}

/// A confirmed transmission with a decoded match of two codecs.
async fn decoded_twice() -> TransmissionId {
    let backend = FixtureBackend::new(7);
    for summary in transmissions(5000).await {
        if summary.state != TransmissionStateKind::Aggregated {
            continue;
        }
        let evidence = backend
            .transmission(&everyone(), summary.id)
            .await
            .expect("evidence")
            .expect("exists");
        if evidence.matches.iter().any(|m| {
            matches!(m.content_match.kind(), MatchKind::Decoded(chain) if chain.iter().count() == 2)
        }) {
            return summary.id;
        }
    }
    panic!("the world has a base64 → url match")
}

fn url(id: TransmissionId) -> String {
    format!(
        "/transmissions/{}?{}",
        id.to_ulid(),
        crate::pages::topology::tests::fixture_state().to_query()
    )
}

#[tokio::test]
async fn unknown_and_malformed_ids_are_not_found() {
    let reply = get(&url(TransmissionId::from_ulid(1))).await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);
    assert!(reply.body.contains("Transmission not found"));
    let state = crate::pages::topology::tests::fixture_state().to_query();
    let reply = get(&format!("/transmissions/nope?{state}")).await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn confirmed_evidence_shows_both_sides_and_the_decode_chain() {
    let id = decoded_twice().await;
    let reply = get(&url(id)).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let body = &reply.body;
    assert!(body.contains("decoded base64 → url"), "{body}");
    assert!(body.contains("Sender originated"));
    assert!(body.contains("Reader read"));
    assert!(body.contains("<mark"));
    assert!(body.contains("content evidence"));
    assert!(body.contains("Co-access timeline"));
    assert!(body.contains("Record verdict"));
    assert!(body.contains("show edge"));
}

#[tokio::test]
async fn suspected_and_discarded_read_as_weaker() {
    let reply = get(&url(first_in(TransmissionStateKind::Suspected).await)).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("access pattern only"));
    assert!(reply.body.contains("unknown sender"));
    assert!(reply.body.contains("weaker evidence"));
    assert!(reply.body.contains("rests on the access pattern alone"));

    let reply = get(&url(first_in(TransmissionStateKind::Discarded).await)).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert!(reply.body.contains("Discarded at"));
    assert!(reply.body.contains("not counted anywhere"));
}

#[tokio::test]
async fn pending_transmissions_offer_no_verdict() {
    let id = first_in(TransmissionStateKind::AwaitingContent).await;
    let reply = get(&url(id)).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("Nothing to judge yet"));
    let reply = post(&url(id), "action=set-verdict&verdict=genuine").await;
    assert_eq!(reply.status, StatusCode::CONFLICT, "{}", reply.body);
    assert!(reply.body.contains("nothing to judge yet"));
}

#[tokio::test]
async fn verdict_posts_validate_then_redirect() {
    let id = first_in(TransmissionStateKind::Suspected).await;
    let reply = post(
        &url(id),
        "action=set-verdict&verdict=false-detection&note=echo",
    )
    .await;
    assert_eq!(reply.status, StatusCode::SEE_OTHER, "{}", reply.body);
    let location = reply.location.expect("location");
    assert!(location.starts_with(&format!("/transmissions/{}?", id.to_ulid())));
    assert!(location.ends_with("&flash=verdict-recorded"));

    let reply = post(&url(id), "action=set-verdict&verdict=maybe").await;
    assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(reply.body.contains("verdict: unknown verdict"));
    let reply = post(&url(id), "action=launch").await;
    assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(reply.body.contains("action: unknown action"));
}

#[tokio::test]
async fn without_content_only_structure_shows() {
    let id = decoded_twice().await;
    let viewer = crate::testing::caller_of(
        OperatorId::from_ulid(1),
        &[Permission::View, Permission::Triage],
    );
    let cx = cx();
    let state = crate::pages::topology::tests::fixture_state();
    let loaded = load(&cx, &viewer, id, &state)
        .await
        .expect("load")
        .expect("found");
    assert!(loaded.matches.is_none());
    assert!(loaded.co_access.is_none());
    assert!(loaded.verdicts.is_none());
    assert!(matches!(loaded.form, FormState::Closed(_)));
    assert_eq!(loaded.header.topic, TopicCell::Hidden);
    assert!(loaded.header.from.is_some(), "structure stays");
    assert_eq!(
        loaded.header.state_text,
        kind_text(TransmissionStateKind::Aggregated)
    );

    let nobody = crate::testing::caller_of(OperatorId::from_ulid(1), &[Permission::Audit]);
    assert!(matches!(
        load(&cx, &nobody, id, &state).await,
        Err(UiError::Query(QueryError::Forbidden { .. }))
    ));
}
