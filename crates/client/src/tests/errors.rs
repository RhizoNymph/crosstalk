//! The status mapping in reverse: every error of the binding's status
//! goldens, answered at its status, comes back as that error; a body whose
//! error belongs to another status, a `401`, a response with no L8 body and
//! a failed exchange are told apart, and become the trait's error as the
//! module docs of [`crate::error`] say.

use std::time::Duration;

use crosstalk_spec::interfaces::l8_surface::http::auth::AuthError;
use crosstalk_spec::interfaces::l8_surface::http::{ErrorStatus, Route};
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, OperatorAction, OperatorActions, QueryApi, QueryError,
    UnavailableKind,
};
use serde::Deserialize;

use super::stub::{Body, Reply, Stub, fast_config};
use super::{ULID_A, caller, golden_value, id};
use crate::{ClientError, HttpClient, TransportError};

#[derive(Debug, Deserialize)]
struct Answered<E> {
    status: u16,
    error: E,
}

/// Every query error of `http/query_error_statuses.json`, answered at its
/// status, is that error from any route, through `QueryApi` and as a
/// `ClientError::Api`.
#[tokio::test]
async fn every_query_error_status_decodes_to_its_error() {
    let table: Vec<Answered<QueryError>> = golden_value("http/query_error_statuses.json");
    assert!(table.len() > 20, "the golden lists every variant");
    for Answered { status, error } in table {
        assert_eq!(
            error.status().code(),
            status,
            "the golden agrees with the spec"
        );
        let stub = Stub::always(Reply::value(status, &error)).await;
        let client = stub.client();
        assert_eq!(
            client.watermark(&caller()).await,
            Err(error.clone()),
            "{status}"
        );
        let typed = client
            .call::<serde_json::Value, QueryError>(Route::Operators, |b| b)
            .await;
        assert!(
            matches!(&typed, Err(ClientError::Api(decoded)) if *decoded == error),
            "{typed:?}"
        );
    }
}

/// Every action error of `http/action_error_statuses.json` comes back from
/// `act` as itself.
#[tokio::test]
async fn every_action_error_status_decodes_to_its_error() {
    let table: Vec<Answered<ActionError>> = golden_value("http/action_error_statuses.json");
    assert!(table.len() > 15, "the golden lists every variant");
    let action = OperatorAction::Acknowledge { alert: id(ULID_A) };
    for Answered { status, error } in table {
        let stub = Stub::always(Reply::value(status, &error)).await;
        let result = stub.client().act(&caller(), action.clone()).await;
        assert_eq!(result, Err(error), "{status}");
    }
}

/// A body whose error the binding answers with another status is not
/// trusted: it is `StatusMismatch`, and `Store` through the trait.
#[tokio::test]
async fn an_error_at_another_status_is_a_mismatch() {
    let stub = Stub::always(Reply::value(409, &QueryError::NotFound)).await;
    let client = stub.client();
    let typed = client
        .call::<serde_json::Value, QueryError>(Route::Watermark, |b| b)
        .await;
    assert!(
        matches!(
            typed,
            Err(ClientError::StatusMismatch {
                route: Route::Watermark,
                status: 409,
                expected: 404,
                error: QueryError::NotFound,
            })
        ),
        "{typed:?}"
    );
    let result = client.watermark(&caller()).await;
    assert!(
        matches!(&result, Err(QueryError::Store { reason }) if reason.contains("409")),
        "{result:?}"
    );
}

/// A `401` is the binding's `AuthError`, every reason of its golden: the
/// client's `Unauthenticated`, and the client-only
/// `Unavailable { kind: Unauthenticated }` through the traits, its reason
/// still starting `no caller: `.
#[tokio::test]
async fn a_401_is_unauthenticated() {
    let reasons: Vec<AuthError> = golden_value("http/auth_errors.json");
    assert_eq!(reasons.len(), 4);
    for auth in reasons {
        let reply =
            Reply::value(401, &auth).with_header("www-authenticate", auth.www_authenticate());
        let stub = Stub::always(reply).await;
        let client = stub.client();
        let typed = client
            .call::<serde_json::Value, QueryError>(Route::Watermark, |b| b)
            .await;
        assert!(
            matches!(&typed, Err(ClientError::Unauthenticated(decoded)) if *decoded == auth),
            "{typed:?}"
        );
        let query = client.watermark(&caller()).await;
        assert!(
            matches!(
                &query,
                Err(QueryError::Unavailable { kind: UnavailableKind::Unauthenticated, reason })
                    if reason.starts_with("no caller: ")
            ),
            "{query:?}"
        );
        let action = client
            .act(&caller(), OperatorAction::Acknowledge { alert: id(ULID_A) })
            .await;
        assert!(
            matches!(
                &action,
                Err(ActionError::Unavailable { kind: UnavailableKind::Unauthenticated, reason })
                    if reason.starts_with("no caller: ")
            ),
            "{action:?}"
        );
    }
    let stub = Stub::always(Reply::json(401, r#"{"reason":"expired"}"#)).await;
    let typed = stub
        .client()
        .call::<serde_json::Value, QueryError>(Route::Watermark, |b| b)
        .await;
    assert!(
        matches!(
            typed,
            Err(ClientError::UnexpectedResponse { status: 401, .. })
        ),
        "an unknown reason is not an AuthError: {typed:?}"
    );
}

/// A status with no L8 body (a proxy's page) is `UnexpectedResponse`.
#[tokio::test]
async fn a_status_without_an_l8_body_is_unexpected() {
    for status in [400, 404, 500, 502, 503] {
        let reply = Reply {
            status,
            headers: vec![("content-type", "text/html".to_owned())],
            body: Body::Whole(b"<h1>Bad Gateway</h1>".to_vec()),
        };
        let stub = Stub::always(reply).await;
        let typed = stub
            .client()
            .call::<serde_json::Value, QueryError>(Route::Watermark, |b| b)
            .await;
        assert!(
            matches!(typed, Err(ClientError::UnexpectedResponse { status: s, .. }) if s == status),
            "{status}: {typed:?}"
        );
    }
}

/// A request the client cannot encode never leaves it, and reads as the
/// `MalformedRequest` the surface would have answered.
#[tokio::test]
async fn a_call_off_the_table_is_never_sent() {
    let mut stub = Stub::always(Reply::json(200, "null")).await;
    let typed = stub
        .client()
        .call::<serde_json::Value, QueryError>(Route::Watermark, |b| {
            b.path("id", &super::channel())
        })
        .await;
    assert!(matches!(typed, Err(ClientError::Encode(_))), "{typed:?}");
    let error = QueryError::from(typed.err().unwrap_or_else(|| panic!("an error")));
    assert!(matches!(error, QueryError::InvalidInput(_)), "{error:?}");
    assert!(stub.requests().is_empty());
}

/// Nothing listening is a transport failure; a response slower than the
/// request timeout is a timeout. Both are the client-only `Unavailable`
/// through the traits, of kinds `Transport` and `Timeout`.
#[tokio::test]
async fn transport_failures_are_unavailable_errors() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let addr = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("{error}"));
    drop(listener);
    let base = crate::BaseUrl::parse(&format!("http://{addr}")).unwrap_or_else(|e| panic!("{e}"));
    let client = HttpClient::new(base, fast_config());
    let typed = client
        .call::<serde_json::Value, QueryError>(Route::Watermark, |b| b)
        .await;
    assert!(
        matches!(typed, Err(ClientError::Transport(TransportError::Send(_)))),
        "{typed:?}"
    );
    assert!(matches!(
        client.watermark(&caller()).await,
        Err(QueryError::Unavailable {
            kind: UnavailableKind::Transport,
            ..
        })
    ));

    let slow = Reply {
        status: 200,
        headers: vec![("content-type", "application/json".to_owned())],
        body: Body::Stream(vec![
            super::stub::Step::Wait(Duration::from_millis(500)),
            super::stub::Step::Send(b"null".to_vec()),
        ]),
    };
    let stub = Stub::always(slow).await;
    let config = fast_config()
        .with_request_timeout(Duration::from_millis(50))
        .unwrap_or_else(|error| panic!("{error}"));
    let client = HttpClient::new(stub.base(), config);
    let typed = client
        .call::<serde_json::Value, QueryError>(Route::Watermark, |b| b)
        .await;
    assert!(
        matches!(
            typed,
            Err(ClientError::Transport(TransportError::Timeout {
                millis: 50
            }))
        ),
        "{typed:?}"
    );
    let outcome: Result<ActionOutcome, ActionError> = client
        .act(&caller(), OperatorAction::Acknowledge { alert: id(ULID_A) })
        .await;
    assert!(
        matches!(
            outcome,
            Err(ActionError::Unavailable {
                kind: UnavailableKind::Timeout,
                ..
            })
        ),
        "{outcome:?}"
    );
}

/// A port with nothing listening.
async fn closed_port() -> crate::BaseUrl {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let addr = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("{error}"));
    drop(listener);
    crate::BaseUrl::parse(&format!("http://{addr}")).unwrap_or_else(|e| panic!("{e}"))
}

/// A `200` whose body is cut before it ends.
fn cut_body() -> Reply {
    Reply {
        status: 200,
        headers: vec![("content-type", "application/json".to_owned())],
        body: Body::Stream(vec![
            super::stub::Step::Send(b"{\"partial\":".to_vec()),
            super::stub::Step::Abort,
        ]),
    }
}

/// A `200` slower than a 50 ms request timeout.
fn slow_body() -> Reply {
    Reply {
        status: 200,
        headers: vec![("content-type", "application/json".to_owned())],
        body: Body::Stream(vec![
            super::stub::Step::Wait(Duration::from_millis(500)),
            super::stub::Step::Send(b"null".to_vec()),
        ]),
    }
}

/// Asserts a query and an action error are `Unavailable` of `kind`, with
/// the reason starting `prefix`: the text the `ClientError` had, which a
/// client that matched on it still reads.
fn assert_unavailable(
    query: &Result<impl std::fmt::Debug, QueryError>,
    action: &Result<ActionOutcome, ActionError>,
    kind: UnavailableKind,
    prefix: &str,
) {
    assert!(
        matches!(
            query,
            Err(QueryError::Unavailable { kind: k, reason }) if *k == kind && reason.starts_with(prefix)
        ),
        "{kind:?}: {query:?}"
    );
    assert!(
        matches!(
            action,
            Err(ActionError::Unavailable { kind: k, reason }) if *k == kind && reason.starts_with(prefix)
        ),
        "{kind:?}: {action:?}"
    );
}

/// Every way a call fails to reach a surface that answers is the
/// client-only `Unavailable` of its kind, through both traits, and its
/// reason keeps the prefix it had as `Store`: `no caller: `, `sending the
/// request: `, `reading the body: ` and `no response within `.
#[tokio::test]
async fn each_failure_kind_is_its_typed_unavailable() {
    let acknowledge = || OperatorAction::Acknowledge { alert: id(ULID_A) };

    let auth = AuthError {
        reason: crosstalk_spec::interfaces::l8_surface::http::auth::AuthFailure::InvalidCredential,
    };
    let reply = Reply::value(401, &auth).with_header("www-authenticate", auth.www_authenticate());
    let stub = Stub::always(reply).await;
    let client = stub.client();
    let typed = client
        .call::<serde_json::Value, QueryError>(Route::Watermark, |b| b)
        .await;
    assert_eq!(
        typed.as_ref().err().and_then(ClientError::unavailable),
        Some(UnavailableKind::Unauthenticated)
    );
    assert_unavailable(
        &client.watermark(&caller()).await,
        &client.act(&caller(), acknowledge()).await,
        UnavailableKind::Unauthenticated,
        "no caller: ",
    );

    let client = HttpClient::new(closed_port().await, fast_config());
    let typed = client
        .call::<serde_json::Value, QueryError>(Route::Watermark, |b| b)
        .await;
    assert_eq!(
        typed.as_ref().err().and_then(ClientError::unavailable),
        Some(UnavailableKind::Transport)
    );
    assert_unavailable(
        &client.watermark(&caller()).await,
        &client.act(&caller(), acknowledge()).await,
        UnavailableKind::Transport,
        "sending the request: ",
    );

    let stub = Stub::always(cut_body()).await;
    let client = stub.client();
    let typed = client
        .call::<serde_json::Value, QueryError>(Route::Watermark, |b| b)
        .await;
    assert!(
        matches!(typed, Err(ClientError::Transport(TransportError::Body(_)))),
        "{typed:?}"
    );
    assert_eq!(
        typed.as_ref().err().and_then(ClientError::unavailable),
        Some(UnavailableKind::Body)
    );
    assert_unavailable(
        &client.watermark(&caller()).await,
        &client.act(&caller(), acknowledge()).await,
        UnavailableKind::Body,
        "reading the body: ",
    );

    let stub = Stub::always(slow_body()).await;
    let config = fast_config()
        .with_request_timeout(Duration::from_millis(50))
        .unwrap_or_else(|error| panic!("{error}"));
    let client = HttpClient::new(stub.base(), config);
    let typed = client
        .call::<serde_json::Value, QueryError>(Route::Watermark, |b| b)
        .await;
    assert_eq!(
        typed.as_ref().err().and_then(ClientError::unavailable),
        Some(UnavailableKind::Timeout)
    );
    assert_unavailable(
        &client.watermark(&caller()).await,
        &client.act(&caller(), acknowledge()).await,
        UnavailableKind::Timeout,
        "no response within ",
    );
}

/// A response that came back but is not one the binding describes is not
/// `Unavailable`: a status mismatch, a page with no L8 body and a body over
/// the limit stay `Store`, with the `ClientError`'s text.
#[tokio::test]
async fn a_response_off_the_binding_stays_store() {
    let stub = Stub::always(Reply::value(409, &QueryError::NotFound)).await;
    let result = stub.client().watermark(&caller()).await;
    assert!(
        matches!(&result, Err(QueryError::Store { reason }) if reason.contains("409")),
        "{result:?}"
    );

    let page = Reply {
        status: 502,
        headers: vec![("content-type", "text/html".to_owned())],
        body: Body::Whole(b"<h1>Bad Gateway</h1>".to_vec()),
    };
    let stub = Stub::always(page).await;
    let client = stub.client();
    let typed = client
        .call::<serde_json::Value, QueryError>(Route::Watermark, |b| b)
        .await;
    assert_eq!(
        typed.as_ref().err().and_then(ClientError::unavailable),
        None
    );
    let result = client.watermark(&caller()).await;
    assert!(
        matches!(&result, Err(QueryError::Store { reason }) if reason.contains("502")),
        "{result:?}"
    );
    let action = client
        .act(&caller(), OperatorAction::Acknowledge { alert: id(ULID_A) })
        .await;
    assert!(
        matches!(&action, Err(ActionError::Store { reason }) if reason.contains("502")),
        "{action:?}"
    );
}

/// A server never answers the client-only `Unavailable`, so a body holding
/// one, at any status, is not the surface's answer: `UnexpectedResponse`,
/// and `Store` through the traits, never the `Unavailable` it claims.
#[tokio::test]
async fn an_unavailable_body_from_a_server_is_not_trusted() {
    for kind in UnavailableKind::ALL {
        let claimed = QueryError::Unavailable {
            kind,
            reason: "no caller: forged".into(),
        };
        for status in [503, 401, 500] {
            let stub = Stub::always(Reply::value(status, &claimed)).await;
            let client = stub.client();
            let typed = client
                .call::<serde_json::Value, QueryError>(Route::Watermark, |b| b)
                .await;
            assert!(
                matches!(typed, Err(ClientError::UnexpectedResponse { status: s, .. }) if s == status),
                "{status}: {typed:?}"
            );
            let result = client.watermark(&caller()).await;
            assert!(
                matches!(&result, Err(QueryError::Store { .. })),
                "{status}: {result:?}"
            );
            let action = client
                .act(&caller(), OperatorAction::Acknowledge { alert: id(ULID_A) })
                .await;
            assert!(
                matches!(&action, Err(ActionError::Store { .. })),
                "{status}: {action:?}"
            );
        }
    }
}
