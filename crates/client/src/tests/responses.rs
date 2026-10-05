//! Success bodies: a route answering with a spec golden decodes to exactly
//! the golden's value, `null` is `None`, `202` is the success of
//! `fit_projection` alone, and a JSON route answering `200` with another
//! content type is not trusted.

use std::fmt::Debug;

use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::ids::{AlertId, ProjectionId};
use crosstalk_spec::interfaces::l8_surface::audit::AuditFilter;
use crosstalk_spec::interfaces::l8_surface::channel_traffic::ChannelTransmissionFilter;
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::http::{Route, Target, resolve};
use crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter;
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionSelection;
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, QueryApi, QueryError};
use serde::de::DeserializeOwned;

use super::stub::{Reply, Stub};
use super::{
    ULID_A, ULID_C, agent, caller, channel, filter, golden, golden_value, id, page, transmission,
    window,
};
use crate::{ClientError, HttpClient};

/// The stub answers `route`'s success status with the golden at `path`;
/// `call` returns exactly the golden's value and sent `route`.
async fn answers<T: DeserializeOwned + PartialEq + Debug>(
    path: &str,
    route: Route,
    call: impl AsyncFnOnce(&HttpClient) -> Result<T, QueryError>,
) {
    let status = route.spec().success.status.code();
    let mut stub = Stub::always(Reply::json(status, golden(path))).await;
    let got = call(&stub.client()).await;
    assert_eq!(got, Ok(golden_value::<T>(path)), "{path}");
    let request = stub.only_request();
    let method = route.spec().method;
    assert_eq!(
        resolve(method, &request.path).map(|(target, _)| target),
        Some(Target::Route(route)),
        "{path}"
    );
}

#[tokio::test]
async fn json_routes_decode_their_goldens() {
    let c = &caller();
    answers(
        "surface_reads/channels/channel_found.json",
        Route::Channel,
        async |client| client.channel(c, channel(), None).await,
    )
    .await;
    answers(
        "surface_reads/channels/channel_unknown.json",
        Route::Channel,
        async |client| client.channel(c, channel(), Some(window())).await,
    )
    .await;
    answers(
        "surface_reads/channels/channels_page.json",
        Route::Channels,
        async |client| client.channels(c, &ChannelFilter::default(), &page()).await,
    )
    .await;
    answers(
        "surface_reads/channel_traffic/channel_transmission_page.json",
        Route::ChannelTransmissions,
        async |client| {
            client
                .channel_transmissions(
                    c,
                    channel(),
                    &ChannelTransmissionFilter::default(),
                    TopicVersionSelector::Current,
                    &page(),
                )
                .await
        },
    )
    .await;
    answers(
        "agents/agent_names_several.json",
        Route::AgentNames,
        async |client| {
            let ids = IdBatch::new([agent(ULID_A)]).unwrap_or_else(|e| panic!("{e:?}"));
            client.agent_names(c, &ids).await
        },
    )
    .await;
    answers("alerts/alerts_page.json", Route::Alerts, async |client| {
        client.alerts(c, &AlertFilter::default(), &page()).await
    })
    .await;
    answers("alerts/alert_found.json", Route::Alert, async |client| {
        client.alert(c, id::<AlertId>(ULID_A)).await
    })
    .await;
    answers("alerts/alert_unknown.json", Route::Alert, async |client| {
        client.alert(c, id::<AlertId>(ULID_A)).await
    })
    .await;
    answers(
        "topology/topology_graph.json",
        Route::Topology,
        async |client| {
            client
                .topology(
                    c,
                    window(),
                    golden_value("topology/weighting_transmissions.json"),
                    &filter(),
                )
                .await
        },
    )
    .await;
    answers(
        "surface_reads/transmissions/transmission_page.json",
        Route::TransmissionsById,
        async |client| {
            let selection = TransmissionSelection::new(vec![transmission()])
                .unwrap_or_else(|e| panic!("{e:?}"));
            client
                .transmissions_by_id(c, &selection, TopicVersionSelector::Current, &page())
                .await
        },
    )
    .await;
    answers(
        "surface_reads/evidence/transmission_evidence_shown.json",
        Route::TransmissionEvidence,
        async |client| {
            client
                .transmission_evidence(c, transmission(), ExcerptWindow::DEFAULT)
                .await
        },
    )
    .await;
    answers(
        "projections/projection_ready.json",
        Route::ProjectionStatus,
        async |client| {
            client
                .projection_status(c, id::<ProjectionId>(ULID_A))
                .await
        },
    )
    .await;
    answers(
        "projections/projections_page.json",
        Route::Projections,
        async |client| client.projections(c, &page()).await,
    )
    .await;
    answers(
        "surface_actions/audit/audit_page.json",
        Route::Audit,
        async |client| client.audit(c, &AuditFilter::default(), &page()).await,
    )
    .await;
    answers(
        "surface_actions/operators/operators.json",
        Route::Operators,
        async |client| client.operators(c).await,
    )
    .await;
    answers(
        "surface_reads/present/present_every_format.json",
        Route::Present,
        async |client| client.present(c).await,
    )
    .await;
}

/// `fit_projection` succeeds with `202` and the id; a `200` is not its
/// success.
#[tokio::test]
async fn fit_projection_is_accepted_with_202() {
    let id = id::<ProjectionId>(ULID_C);
    let stub = Stub::always(Reply::value(202, &id)).await;
    let fitted = stub
        .client()
        .fit_projection(&caller(), window(), &filter(), super::params())
        .await;
    assert_eq!(fitted, Ok(id));

    let stub = Stub::always(Reply::value(200, &id)).await;
    let error = stub
        .client()
        .call::<ProjectionId, QueryError>(Route::FitProjection, |b| {
            b.body(
                &crosstalk_spec::interfaces::l8_surface::http::bodies::FitProjectionBody {
                    window: window(),
                    filter: filter(),
                    params: super::params(),
                },
            )
        })
        .await;
    assert!(
        matches!(
            error,
            Err(ClientError::UnexpectedResponse { status: 200, .. })
        ),
        "{error:?}"
    );
}

/// A success of another content type, or a body that is not the route's
/// result, is not trusted.
#[tokio::test]
async fn a_success_must_be_the_routes_json() {
    let html = Reply {
        status: 200,
        headers: vec![("content-type", "text/html".to_owned())],
        body: super::stub::Body::Whole(b"<html></html>".to_vec()),
    };
    let stub = Stub::always(html).await;
    let error = stub
        .client()
        .call::<serde_json::Value, QueryError>(Route::Watermark, |b| b)
        .await;
    assert!(
        matches!(&error, Err(ClientError::UnexpectedResponse { reason, .. }) if reason.contains("text/html")),
        "{error:?}"
    );

    let stub = Stub::always(Reply::json(200, r#"{"not":"a watermark"}"#)).await;
    let result = stub.client().watermark(&caller()).await;
    assert!(
        matches!(&result, Err(QueryError::Store { reason }) if reason.contains("not the route's result")),
        "{result:?}"
    );

    // Parameters of the content type are ignored, as the binding reads it.
    let json = Reply {
        status: 200,
        headers: vec![("content-type", "Application/JSON; charset=utf-8".to_owned())],
        body: super::stub::Body::Whole(b"null".to_vec()),
    };
    let stub = Stub::always(json).await;
    let result = stub.client().alert(&caller(), id::<AlertId>(ULID_A)).await;
    assert_eq!(result, Ok(None));
}
