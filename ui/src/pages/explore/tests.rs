//! The explore page against the fixture world: search, the fit flow, a
//! stored projection and the lasso resolved server-side.

use std::num::NonZeroU32;

use crate::error::UiError;
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::projection::{
    ProjectionLimit, ProjectionParams, ProjectionStatusKind,
};
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, QueryError};
use topcoat::router::StatusCode;

use super::results::{Results, load};
use super::*;
use crate::pages::common::topics::all_topics;
use crate::pages::topology::tests::fixture_state;
use crate::testing::{cx, get, post, render};

fn everyone() -> Caller {
    crate::testing::operator().caller()
}

fn url(extra: &str) -> String {
    format!("/explore?{}{extra}", fixture_state().to_query())
}

fn params() -> ProjectionParams {
    ProjectionParams::new(ProjectionLimit::new(5000).expect("limit"), 15, 100, 42).expect("params")
}

/// A context whose backend holds a projection of the fixture view.
async fn fitted() -> (Cx, ProjectionId) {
    let cx = cx();
    let scope = fixture_state().scope;
    let id = backend(&cx)
        .fit_projection(
            &everyone(),
            scope.window,
            &scope.topology_filter(),
            params(),
        )
        .await
        .expect("fit");
    (cx, id)
}

/// The id of the fixture's seeded job in `status`.
async fn seeded(status: ProjectionStatusKind) -> ProjectionId {
    let cx = cx();
    backend(&cx)
        .projections(&everyone(), &crate::pages::common::paging::first(50u16))
        .await
        .expect("jobs")
        .items()
        .iter()
        .find(|info| info.status().kind() == status)
        .map(|info| info.id())
        .expect("a seeded job")
}

#[tokio::test]
async fn without_a_projection_the_page_offers_to_fit_one() {
    let reply = get("/explore").await;
    assert_eq!(reply.status, StatusCode::TEMPORARY_REDIRECT);
    let reply = get(&url("")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("No projection yet"));
    assert!(reply.body.contains("Fit projection"));
    assert!(reply.body.contains("name=\"seed\" min=\"0\" value=\"42\""));
    assert!(!reply.body.contains("<ct-projection"));
    assert!(reply.body.contains("Topics · v2"));
    assert!(reply.body.contains(">Watch</a>"));
    assert!(reply.body.contains("kind=watched&amp;topic="));
}

#[tokio::test]
async fn search_lists_hits_with_links_to_evidence() {
    let reply = get(&url("&q=wiki&m=text")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("data-hits=\""), "{}", reply.body);
    assert!(reply.body.contains("href=\"/transmissions/"));
    assert!(
        reply.body.contains("value=\"wiki\""),
        "the query stays in the box"
    );
    let reply = get(&url("&q=zzzqqqxxx&m=text")).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert!(
        reply
            .body
            .contains("No transmission in the view matches this search.")
    );
}

#[tokio::test]
async fn bad_keys_are_422() {
    let reply = get(&url("&m=fuzzy")).await;
    assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(reply.body.contains("m: unknown search mode"));
    let reply = get(&url("&ps=lasso:0,0")).await;
    assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn unknown_projections_offer_a_new_fit() {
    let reply = get(&url("&p=01J9ZQ3W8D0000000000000001")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("No stored projection has this id"));
}

#[tokio::test]
async fn fitting_redirects_to_the_projection() {
    let reply = post(
        &url("&q=wiki&cb=route"),
        "action=fit&neighbors=15&min_dist=0.1&seed=42&sample_limit=5000",
    )
    .await;
    assert_eq!(reply.status, StatusCode::SEE_OTHER, "{}", reply.body);
    let location = reply.location.expect("location");
    assert!(location.starts_with("/explore?from="), "{location}");
    assert!(location.contains("&q=wiki&p="), "{location}");
    assert!(
        location.ends_with("&cb=route&flash=projection-fitted"),
        "{location}"
    );

    let reply = post(&url(""), "action=fit&neighbors=0").await;
    assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(reply.body.contains("neighbors: expected 2 to 200"));
    let reply = post(&url(""), "action=refit").await;
    assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn a_stored_projection_renders_with_highlighted_hits() {
    let (cx, id) = fitted().await;
    let raw = RawExploreQuery {
        q: Some("wiki".into()),
        m: Some("text".into()),
        p: Some(id.to_ulid()),
        cb: Some("sender".into()),
        ps: None,
    };
    let query = ExploreQuery::parse(&raw).expect("query");
    let cx = &cx;
    let html = render(
        view! { cx => explore_body(state: fixture_state(), query: query, page: crate::pages::common::paging::first(NonZeroU32::new(50).expect("n")), fit_error: None, fit_fields: None) },
        cx,
    )
    .await;
    assert!(html.contains("<ct-projection"), "{html}");
    assert!(html.contains(&format!("data-src=\"/data/projection/{}\"", id.to_ulid())));
    assert!(html.contains("data-color-by=\"sender\""));
    let highlight = html
        .split("data-highlight=\"")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .expect("highlight");
    assert!(highlight.len() >= 26, "hits are highlighted: {highlight:?}");
    assert!(html.contains("seed 42"));
    assert!(html.contains("Click a point, or shift-drag a lasso"));
}

#[tokio::test]
async fn lassos_and_points_resolve_against_the_stored_projection() {
    let (cx, id) = fitted().await;
    let caller = everyone();
    let state = fixture_state().to_query();
    let projection = id.to_ulid();
    let everything = "lasso:-50,-50;50,-50;50,50;-50,50";
    let Results::Lasso {
        selected,
        of,
        rows,
        next,
        paged,
    } = load(&cx, &caller, &state, &projection, everything, "")
        .await
        .expect("lasso")
    else {
        panic!("lasso results")
    };
    assert_eq!(
        selected, of,
        "a lasso around everything selects every point"
    );
    assert!(of > 0);
    assert_eq!(rows.len(), results::RESULTS_PAGE.get() as usize);
    assert!(!paged);
    let next = next.expect("more than one page");
    let Results::Lasso { paged, .. } = load(&cx, &caller, &state, &projection, everything, &next)
        .await
        .expect("page 2")
    else {
        panic!("lasso results")
    };
    assert!(paged);

    let nowhere = "lasso:100,100;101,100;101,101";
    assert!(matches!(
        load(&cx, &caller, &state, &projection, nowhere, "").await,
        Ok(Results::Lasso { selected: 0, .. })
    ));

    let point = format!("point:{}", rows[0].id.to_ulid());
    assert!(matches!(
        load(&cx, &caller, &state, &projection, &point, "").await,
        Ok(Results::Point(Some(_)))
    ));
    assert_eq!(
        load(&cx, &caller, &state, &projection, "", "").await,
        Ok(Results::Idle)
    );
}

#[tokio::test]
async fn results_validate_their_arguments_and_permissions() {
    let (cx, id) = fitted().await;
    let caller = everyone();
    let state = fixture_state().to_query();
    let projection = id.to_ulid();
    assert!(
        load(&cx, &caller, &state, &projection, "lasso:1,2", "")
            .await
            .is_err()
    );
    assert!(
        load(&cx, &caller, "w=tx", &projection, "", "")
            .await
            .is_err()
    );
    assert!(
        load(
            &cx,
            &caller,
            &state,
            "nope",
            "point:01J9ZQ3W8D0000000000000001",
            ""
        )
        .await
        .is_err()
    );
    let viewer = crate::testing::caller_of(OperatorId::from_ulid(1), &[Permission::View]);
    assert_eq!(
        load(&cx, &viewer, &state, &projection, "", "").await,
        Err(UiError::Query(QueryError::Forbidden {
            missing: Permission::Content
        }))
    );
}

#[tokio::test]
async fn watch_links_preselect_the_topic_in_the_rule_form() {
    let cx = cx();
    let version = TopicVersionSelector::Pinned(fixture_state().scope.topic_version);
    let topic = all_topics(backend(&cx), &everyone(), version)
        .await
        .expect("topics")
        .1
        .into_iter()
        .next()
        .expect("a topic")
        .id;
    let link = topics::watch_url(topic, &fixture_state());
    let reply = get(&link).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(
        reply
            .body
            .contains(&format!("value=\"{}\" checked", topic.to_ulid())),
        "{}",
        reply.body
    );
    let unknown = link.replace(&topic.to_ulid(), "01J9ZQ3W8D0000000000000001");
    let reply = get(&unknown).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert!(
        !reply.body.contains("\" checked"),
        "unknown topics are ignored"
    );
}

#[tokio::test]
async fn the_panel_follows_the_job_status() {
    let id = |status| async move { seeded(status).await.to_ulid() };
    let reply = get(&url(&format!(
        "&p={}",
        id(ProjectionStatusKind::Queued).await
    )))
    .await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("Queued for fitting since"));
    assert!(!reply.body.contains("<ct-projection"));
    let reply = get(&url(&format!(
        "&p={}",
        id(ProjectionStatusKind::Fitting).await
    )))
    .await;
    assert!(reply.body.contains("Fitting since"));
    let reply = get(&url(&format!(
        "&p={}",
        id(ProjectionStatusKind::Failed).await
    )))
    .await;
    assert!(
        reply
            .body
            .contains("Fitting failed: topic model version 0 was dropped"),
        "{}",
        reply.body
    );
    let reply = get(&url(&format!(
        "&p={}",
        id(ProjectionStatusKind::Expired).await
    )))
    .await;
    assert!(reply.body.contains("fitted too long ago"));
    assert!(
        reply.body.contains("name=\"seed\" min=\"0\" value=\"42\""),
        "the refit form keeps the seed"
    );
    assert!(
        reply
            .body
            .contains("min_dist\" min=\"0\" max=\"1\" step=\"0.001\" value=\"0.100\"")
    );
}

#[tokio::test]
async fn a_projection_of_another_view_is_flagged_stale() {
    let (cx, id) = fitted().await;
    let mut other = fixture_state();
    other.scope.filter.route_kinds = vec![crosstalk_spec::aggregates::edge::RouteKind::Channel];
    let job = backend(&cx)
        .projection_status(&everyone(), id)
        .await
        .expect("status");
    assert!(matches!(
        panel(Some(Ok(job.clone())), &fixture_state()),
        ProjectionPanel::Ready { stale: false, .. }
    ));
    assert!(matches!(
        panel(Some(Ok(job)), &other),
        ProjectionPanel::Ready { stale: true, .. }
    ));
    assert_eq!(panel(None, &other), ProjectionPanel::NotFitted);
}

#[test]
fn form_fields_round_trip_through_the_parser() {
    let fields = form_fields(params());
    assert_eq!(fit::parse(&fields), Ok(params()));
}
