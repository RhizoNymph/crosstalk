//! Every route served from the table, its permission refused, its request
//! read strictly, and its response never stored by a shared cache.

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, LOCATION};
use axum::http::{Request, StatusCode};
use crosstalk_spec::interfaces::l8_surface::http::request::MAX_BODY_BYTES;
use crosstalk_spec::interfaces::l8_surface::http::{Method, Route, RoutePermission, Status};
use crosstalk_spec::interfaces::l8_surface::{InputError, QueryError};
use serde_json::{Value, json};

use super::cases::{CHANNEL, Case, TRANSMISSION, cases};
use super::fake::{Call, Fake};
use super::{FULL, compact, error_json, operator, request, send, server, token, without};

/// A fake answering every case's method with its response.
fn answering() -> Arc<Fake> {
    let fake = Arc::new(Fake::default());
    for case in cases() {
        fake.respond(case.method(), case.response.clone());
    }
    fake
}

fn success(route: Route) -> u16 {
    route.spec().success.status.code()
}

/// Every query route answers its success with its method's value, which
/// is the golden the surface returned byte for byte, after calling the
/// method once with exactly the arguments the client sent; `HEAD` is
/// answered for every `GET`. Together with the frame, export, live and
/// action tests, every route of the table is served.
#[tokio::test]
async fn every_route_is_served() {
    let mut served = BTreeSet::new();
    for case in cases() {
        let fake = answering();
        let router = server(&fake);
        let reply = send(&router, request(&case.request(), FULL)).await;
        let route = case.route;
        assert_eq!(
            reply.status.as_u16(),
            success(route),
            "{route:?}: {reply:?}"
        );
        assert_eq!(
            reply.header(CONTENT_TYPE.as_str()),
            Some("application/json")
        );
        assert_eq!(reply.header(CACHE_CONTROL.as_str()), Some("no-store"));
        assert_eq!(
            String::from_utf8_lossy(&reply.body),
            compact(&case.response),
            "{route:?}: the response is the surface's value, unchanged"
        );
        assert_eq!(
            fake.calls(),
            vec![Call {
                method: case.method(),
                operator: operator(FULL),
                args: case.args(),
            }],
            "{route:?}: the method is called once with the request's arguments"
        );
        if route == Route::FitProjection {
            let id = case.response.trim_matches('"');
            assert_eq!(
                reply.header(LOCATION.as_str()),
                Some(&*format!("/projections/{id}"))
            );
        }
        if route.method() == Method::Get {
            let mut head = request(&case.request(), FULL);
            *head.method_mut() = axum::http::Method::HEAD;
            let reply = send(&router, head).await;
            assert_eq!(reply.status.as_u16(), success(route), "HEAD {route:?}");
            assert!(reply.body.is_empty(), "HEAD {route:?} has no body");
        }
        served.insert(route.index());
    }
    let elsewhere = [Route::ProjectionFrame, Route::Export, Route::Live];
    for route in Route::all() {
        let tested = served.contains(&route.index())
            || elsewhere.contains(&route)
            || matches!(route, Route::Action(_));
        assert!(tested, "{route:?} has no serving test");
    }
}

/// A caller without the route's permission is a 403 naming it, and the
/// method reads nothing.
#[tokio::test]
async fn every_route_refuses_a_caller_without_its_permission() {
    for case in cases() {
        let route = case.route;
        let RoutePermission::Fixed(needed) = route.permission() else {
            panic!("{route:?}: a query route has a fixed permission");
        };
        let fake = answering();
        let reply = send(&server(&fake), request(&case.request(), without(needed))).await;
        assert_eq!(reply.status, StatusCode::FORBIDDEN, "{route:?}");
        assert_eq!(
            reply.json(),
            error_json(&QueryError::Forbidden { missing: needed })
        );
        assert_eq!(reply.header(CACHE_CONTROL.as_str()), Some("no-store"));
        assert!(fake.untouched(), "{route:?}: nothing is read");
    }
}

fn malformed(reply: &super::Reply) -> bool {
    reply.status == StatusCode::BAD_REQUEST
        && reply.json()["type"] == "invalid_input"
        && reply.json()["data"]["type"] == "malformed_request"
}

fn raw(method: &str, uri: &str, content_type: Option<&str>, body: Vec<u8>) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(AUTHORIZATION, format!("Bearer {}", token(FULL)));
    if let Some(content_type) = content_type {
        builder = builder.header(CONTENT_TYPE, content_type);
    }
    builder.body(Body::from(body)).expect("a valid request")
}

/// A request that is not its route's types is a 400 `MalformedRequest`
/// that reaches no method; the action route answers the same with an
/// `ActionError`.
#[tokio::test]
async fn undecodable_requests_are_400_and_unaudited() {
    let window = super::cases::window();
    let window = serde_json::to_string(&window).expect("JSON");
    let encode =
        |text: &str| -> String { form_urlencoded::byte_serialize(text.as_bytes()).collect() };
    let w = encode(&window);
    let json = Some("application/json");
    let over = vec![b' '; MAX_BODY_BYTES + 1];
    let requests = vec![
        // A path id that is not ULID text.
        raw("GET", "/channels/not-a-ulid", None, vec![]),
        raw("GET", &format!("/channels/{}", CHANNEL.to_lowercase()), None, vec![]),
        raw("GET", "/topic-versions/02/lineage", None, vec![]),
        // Unknown, repeated, missing and misshapen query parameters.
        raw("GET", &format!("/channels/{CHANNEL}?windw={w}"), None, vec![]),
        raw("GET", &format!("/channels/{CHANNEL}?window={w}&window={w}"), None, vec![]),
        raw("GET", "/detection-quality", None, vec![]),
        raw("GET", "/detection-quality?window=%7B%7D", None, vec![]),
        raw("GET", "/detection-quality?window=yesterday", None, vec![]),
        raw("GET", &format!("/detection-quality?window={w}&caller=1"), None, vec![]),
        // A body on a route that takes none.
        raw("GET", "/watermark", json, b"{}".to_vec()),
        // Bodies that are not JSON, not the type, too large or not
        // application/json.
        raw("POST", "/query/agent-names", json, b"[\"01J9".to_vec()),
        raw("POST", "/query/agent-names", json, b"{\"ids\": []}".to_vec()),
        raw("POST", "/query/agent-names", Some("text/plain"), b"[]".to_vec()),
        raw("POST", "/query/agent-names", Some("application/x-www-form-urlencoded"), b"[]".to_vec()),
        raw("POST", "/query/agent-names", None, b"[]".to_vec()),
        raw("POST", "/query/agent-names", json, over.clone()),
        raw("POST", "/query/overview", json, b"{\"window\": null}".to_vec()),
        // Actions: not JSON, an unknown type, a stamped field, a form.
        raw("POST", "/actions", json, b"{".to_vec()),
        raw("POST", "/actions", json, b"{\"type\": \"delete_everything\"}".to_vec()),
        raw(
            "POST",
            "/actions",
            json,
            format!(
                "{{\"type\": \"acknowledge\", \"data\": {{\"alert\": \"{TRANSMISSION}\", \"by\": \"{TRANSMISSION}\"}}}}"
            )
            .into_bytes(),
        ),
        raw("POST", "/actions", Some("text/plain"), b"{}".to_vec()),
        raw("POST", "/actions?type=acknowledge", json, b"{}".to_vec()),
        raw("POST", "/actions", json, over),
    ];
    for request in requests {
        let line = format!("{} {}", request.method(), request.uri());
        let fake = answering();
        let reply = send(&server(&fake), request).await;
        assert!(malformed(&reply), "{line}: {reply:?}");
        assert_eq!(
            reply.header(CACHE_CONTROL.as_str()),
            Some("no-store"),
            "{line}"
        );
        assert!(fake.untouched(), "{line}: nothing is called or audited");
    }
}

/// A request value its checked constructor refuses is the `InvalidInput`
/// the spec names (422), before anything is read: too many ids in a batch
/// or a selection, an empty selection, an excerpt context over the limit,
/// a self-merge.
#[tokio::test]
async fn checked_constructors_refuse_with_their_input_error() {
    let ids: Vec<String> = (1..=1001_u128)
        .map(|n| {
            format!(
                "\"{}\"",
                crosstalk_spec::ids::AgentId::from_ulid(n).ulid_text()
            )
        })
        .collect();
    let batch = format!("[{}]", ids.join(","));
    let selection = |ids: &str| {
        format!(
            "{{\"selection\": {ids}, \"version\": {{\"type\": \"current\"}}, \"page\": {{\"size\": 50, \"after\": null}}}}"
        )
    };
    let many: Vec<String> = (1..=100_001_u128)
        .map(|n| {
            format!(
                "\"{}\"",
                crosstalk_spec::ids::TransmissionId::from_ulid(n).ulid_text()
            )
        })
        .collect();
    let json = Some("application/json");
    let context = form_urlencoded::byte_serialize(b"{\"context\": 2049}").collect::<String>();
    let cases: Vec<(Request<Body>, InputError)> = vec![
        (
            raw("POST", "/query/agent-names", json, batch.clone().into_bytes()),
            InputError::TooManyIds { max: 1000, got: 1001 },
        ),
        (
            raw("POST", "/query/channel-names", json, batch.into_bytes()),
            InputError::TooManyIds { max: 1000, got: 1001 },
        ),
        (
            raw("POST", "/query/transmissions", json, selection("[]").into_bytes()),
            InputError::EmptySelection,
        ),
        (
            raw(
                "POST",
                "/query/transmissions",
                json,
                selection(&format!("[{}]", many.join(","))).into_bytes(),
            ),
            InputError::TooManyIds {
                max: 100_000,
                got: 100_001,
            },
        ),
        (
            raw(
                "GET",
                &format!("/transmissions/{TRANSMISSION}/evidence?window={context}"),
                None,
                vec![],
            ),
            InputError::ExcerptContextTooLong { max: 2048, got: 2049 },
        ),
        (
            raw(
                "POST",
                "/actions",
                json,
                format!(
                    "{{\"type\": \"merge_agents\", \"data\": {{\"from\": \"{CHANNEL}\", \"into\": \"{CHANNEL}\"}}}}"
                )
                .into_bytes(),
            ),
            InputError::SelfMerge,
        ),
    ];
    for (request, expected) in cases {
        let line = format!("{} {}", request.method(), request.uri().path());
        let fake = answering();
        let reply = send(&server(&fake), request).await;
        assert_eq!(
            reply.status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{line}: {reply:?}"
        );
        assert_eq!(
            reply.json(),
            error_json(&QueryError::InvalidInput(expected)),
            "{line}"
        );
        assert!(fake.untouched(), "{line}: refused before the call");
    }
}

/// A path no route serves, and a method the path does not answer, are
/// `404 NotFound` (after authentication), as `resolve` answers them.
#[tokio::test]
async fn unserved_paths_and_methods_are_not_found() {
    let requests = [
        raw("GET", "/nothing-here", None, vec![]),
        raw("GET", "/channels/", None, vec![]),
        raw("GET", &format!("/channels//{CHANNEL}"), None, vec![]),
        raw("GET", "/v1/channels", None, vec![]),
        raw(
            "POST",
            "/watermark",
            Some("application/json"),
            b"{}".to_vec(),
        ),
        raw("PUT", "/actions", Some("application/json"), b"{}".to_vec()),
        raw("DELETE", &format!("/channels/{CHANNEL}"), None, vec![]),
        raw("GET", "/actions", None, vec![]),
        raw("HEAD", "/query/topology", None, vec![]),
    ];
    for request in requests {
        let line = format!("{} {}", request.method(), request.uri());
        let fake = answering();
        let reply = send(&server(&fake), request).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND, "{line}");
        if reply.body.is_empty() {
            // HEAD: the same status, no body.
            continue;
        }
        assert_eq!(reply.json(), error_json(&QueryError::NotFound), "{line}");
        assert_eq!(
            reply.header(CACHE_CONTROL.as_str()),
            Some("no-store"),
            "{line}"
        );
        assert!(fake.untouched(), "{line}");
    }
}

/// Every response but a ready frame is `Cache-Control: no-store`:
/// successes, every error, a 401 and a 404. (The live feed, the export
/// and the frame check theirs in their own tests.)
#[tokio::test]
async fn responses_are_not_stored_by_shared_caches() {
    let fake = answering();
    let router = server(&fake);
    for case in cases() {
        let reply = send(&router, request(&case.request(), FULL)).await;
        assert_eq!(
            reply.header(CACHE_CONTROL.as_str()),
            Some("no-store"),
            "{:?}",
            case.route
        );
    }
    let errors: Vec<Value> = super::decode(&super::golden_text("http/query_error_statuses"));
    let case: Case = cases()
        .into_iter()
        .find(|case| case.route == Route::Watermark)
        .expect("the watermark case");
    for entry in errors {
        let error: QueryError = serde_json::from_value(entry["error"].clone()).expect("an error");
        fake.fail_with(error);
        let reply = send(&router, request(&case.request(), FULL)).await;
        assert_eq!(json!(reply.status.as_u16()), entry["status"]);
        assert_eq!(reply.header(CACHE_CONTROL.as_str()), Some("no-store"));
    }
    let anonymous = Request::builder()
        .uri("/watermark")
        .body(Body::empty())
        .expect("a request");
    let reply = send(&router, anonymous).await;
    assert_eq!(reply.status.as_u16(), Status::Unauthorized.code());
    assert_eq!(reply.header(CACHE_CONTROL.as_str()), Some("no-store"));
    let reply = send(&router, raw("GET", "/nothing", None, vec![])).await;
    assert_eq!(reply.header(CACHE_CONTROL.as_str()), Some("no-store"));
}
