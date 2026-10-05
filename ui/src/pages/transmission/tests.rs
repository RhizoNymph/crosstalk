//! The evidence page against the fixture world: matched text, weaker
//! states, verdict posts and the view without `Content`.

use crate::error::UiError;
use crosstalk_spec::derived::provenance::matching::MatchKind;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::summary::{TransmissionSelection, TransmissionSummary};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, QueryError};
use topcoat::router::StatusCode;

use super::*;
use crate::backend::fixture::FixtureBackend;
use crate::testing::{cx, get, post};

fn everyone() -> Caller {
    crate::testing::operator().caller()
}

/// Every transmission of the fixture world, newest id first, paged through
/// `transmissions_by_id` under the fixture view's version.
async fn transmissions(limit: u32) -> Vec<TransmissionSummary> {
    let backend = FixtureBackend::try_new(7).expect("fixture generates");
    let ids = TransmissionSelection::new(backend.transmission_ids()).expect("selection");
    let version = TopicVersionSelector::Pinned(
        crate::pages::topology::tests::fixture_state()
            .scope
            .topic_version,
    );
    let mut out = Vec::new();
    let mut after = None;
    while out.len() < limit as usize {
        let page = crosstalk_spec::paging::PageRequest {
            size: crate::pages::common::paging::size(500),
            after,
        };
        let (items, next) = backend
            .transmissions_by_id(&everyone(), &ids, version, &page)
            .await
            .expect("transmissions")
            .page
            .into_parts();
        out.extend(items);
        match next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    out.truncate(limit as usize);
    out
}

async fn first_in(kind: TransmissionStateKind) -> TransmissionId {
    transmissions(5000)
        .await
        .into_iter()
        .find(|t| t.state.kind() == kind)
        .map(|t| t.id)
        .expect("a transmission in that state")
}

/// A confirmed transmission with a decoded match of two codecs.
async fn decoded_twice() -> TransmissionId {
    let backend = FixtureBackend::try_new(7).expect("fixture generates");
    for summary in transmissions(5000).await {
        if summary.state.kind() != TransmissionStateKind::Aggregated {
            continue;
        }
        let evidence = backend
            .transmission_evidence(&everyone(), summary.id, ExcerptWindow::DEFAULT)
            .await
            .expect("evidence")
            .expect("exists");
        if evidence.matches().iter().any(|m| {
            matches!(m.content_match().kind(), MatchKind::Decoded(chain) if chain.iter().count() == 2)
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
    // `SetVerdict` needs Triage alone: the form is offered, saying the
    // text is hidden.
    assert!(matches!(
        loaded.form,
        FormState::Open { content: false, .. }
    ));
    assert_eq!(loaded.header.topic, TopicCell::Hidden);
    assert!(loaded.header.from.is_some(), "structure stays");
    assert!(loaded.header.confirmed.is_some() && loaded.header.matched.is_some());
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

#[tokio::test]
async fn the_verdict_log_needs_only_view() {
    let judged = transmissions(5000)
        .await
        .into_iter()
        .find(|t| t.state.verdict().is_some())
        .expect("a judged transmission");
    let viewer = crate::testing::caller_with(&[Permission::View]);
    let loaded = load(
        &cx(),
        &viewer,
        judged.id,
        &crate::pages::topology::tests::fixture_state(),
    )
    .await
    .expect("load")
    .expect("found");
    assert!(loaded.matches.is_none());
    assert!(!loaded.verdicts.is_empty());
    assert_eq!(
        loaded.verdicts.iter().filter(|row| row.current).count(),
        1,
        "the one in force is marked"
    );
    assert!(loaded.verdicts[0].current, "newest first");
    assert_eq!(loaded.header.verdict, judged.state.verdict());
}

#[tokio::test]
async fn dropped_bodies_show_a_notice_instead_of_text() {
    let dropped = crate::testing::world().scenario().dropped.clone();
    assert!(!dropped.is_empty());
    for (id, _) in dropped {
        let reply = get(&url(id)).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(reply.body.contains("Body dropped"), "{}", reply.body);
        assert!(reply.body.contains("<mark"), "the other side is shown");
    }
}
