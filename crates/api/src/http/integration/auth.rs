//! Who is calling: only the credential headers name the caller; without a
//! caller every request is a 401 before its route is resolved.

use std::sync::Arc;

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, COOKIE, WWW_AUTHENTICATE};
use axum::http::{Request, StatusCode};
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::http::auth::{
    AuthError, AuthFailure, Credential, SESSION_COOKIE,
};
use crosstalk_spec::interfaces::l8_surface::http::{RequestBuilder, Route};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorDirectory, OperatorName, RequestIdentity, TrustedOperator,
};
use crosstalk_spec::interfaces::l8_surface::{ActionRequest, Permission};
use serde_json::json;
use tokio::sync::watch;

use super::fake::Fake;
use super::{
    FULL, authenticated, directory, error_json, golden, operator, send, server, server_with, token,
    tokens, without,
};
use crate::http::{Auth, CredentialVerifier, StaticTokens};

/// `GET /watermark` with `headers`.
fn get(path: &str, headers: &[(&str, &str)]) -> Request<Body> {
    let mut builder = Request::builder().uri(path);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    builder.body(Body::empty()).expect("a request")
}

fn answering() -> Arc<Fake> {
    let fake = Arc::new(Fake::default());
    fake.respond("watermark", "\"2026-10-04T12:00:00.000000Z\"".to_owned());
    fake
}

fn assert_401(reply: &super::Reply, reason: AuthFailure, line: &str) {
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{line}");
    assert_eq!(
        reply.json(),
        error_json(&AuthError { reason }),
        "{line}: the AuthError names why"
    );
    let challenge = AuthError { reason }.www_authenticate();
    assert_eq!(
        reply.header(WWW_AUTHENTICATE.as_str()),
        Some(challenge.as_str()),
        "{line}"
    );
    assert_eq!(reply.header("cache-control"), Some("no-store"), "{line}");
}

/// No credential, a malformed or unknown one, an unknown operator's and a
/// former operator's are each a 401 naming why, with the bearer challenge,
/// whatever the path (a route, no route, an action), and reach nothing.
#[tokio::test]
async fn requests_without_a_caller_are_401_and_unaudited() {
    let bearer = |n: u128| format!("Bearer {}", token(n));
    let cases: Vec<(Vec<(&str, String)>, AuthFailure)> = vec![
        (vec![], AuthFailure::NoCredential),
        (
            vec![("cookie", "theme=dark; other=1".to_owned())],
            AuthFailure::NoCredential,
        ),
        (
            vec![(
                "authorization",
                "Bearer not-a-known-token-at-all".to_owned(),
            )],
            AuthFailure::InvalidCredential,
        ),
        (
            vec![("authorization", format!("Basic {}", token(FULL)))],
            AuthFailure::InvalidCredential,
        ),
        (
            vec![("authorization", "Bearer".to_owned())],
            AuthFailure::InvalidCredential,
        ),
        (
            vec![("authorization", "Bearer a b c".to_owned())],
            AuthFailure::InvalidCredential,
        ),
        (
            vec![
                ("authorization", bearer(FULL)),
                ("authorization", bearer(FULL)),
            ],
            AuthFailure::InvalidCredential,
        ),
        (
            vec![("cookie", format!("{SESSION_COOKIE}=some-session-id"))],
            AuthFailure::InvalidCredential,
        ),
        (
            vec![("cookie", format!("{SESSION_COOKIE}=a; {SESSION_COOKIE}=b"))],
            AuthFailure::InvalidCredential,
        ),
        (
            vec![("authorization", bearer(99))],
            AuthFailure::UnknownOperator,
        ),
    ];
    for (headers, reason) in cases {
        for path in ["/watermark", "/nothing-here", "/actions", "/live"] {
            let line = format!("{path} {headers:?}");
            let fake = answering();
            let mut builder = Request::builder().uri(path);
            if path == "/actions" {
                builder = builder
                    .method("POST")
                    .header(CONTENT_TYPE, "application/json");
            }
            for (name, value) in &headers {
                builder = builder.header(*name, value);
            }
            let body = if path == "/actions" {
                Body::from(
                    r#"{"type": "acknowledge", "data": {"alert": "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA"}}"#,
                )
            } else {
                Body::empty()
            };
            let reply = send(&server(&fake), builder.body(body).expect("a request")).await;
            assert_401(&reply, reason, &line);
            assert!(fake.untouched(), "{line}: nothing is called or audited");
        }
    }
}

/// An operator config no longer defines is a former operator: its token
/// still verifies, and it gets a 401, from the next request on.
#[tokio::test]
async fn a_former_operator_is_401_after_the_reload() {
    let config = authenticated();
    let first = directory(&config);
    let (sender, receiver) = watch::channel(first.clone());
    let fake = answering();
    let router = server_with(&fake, Auth::new(receiver, tokens()));
    let who = without(Permission::Audit);
    let bearer = format!("Bearer {}", token(who));
    let reply = send(&router, get("/watermark", &[("authorization", &bearer)])).await;
    assert_eq!(reply.status, StatusCode::OK);
    let AccessConfig::Authenticated(operators) = config else {
        panic!("authenticated");
    };
    let fewer = AccessConfig::Authenticated(
        operators
            .into_iter()
            .filter(|configured| configured.id != operator(who))
            .collect(),
    );
    let (second, _) = OperatorDirectory::load(Some(&first), &fewer).expect("a valid config");
    sender.send(second).expect("the server holds the receiver");
    let calls = fake.calls().len();
    let reply = send(&router, get("/watermark", &[("authorization", &bearer)])).await;
    assert_401(&reply, AuthFailure::FormerOperator, "after the reload");
    assert_eq!(fake.calls().len(), calls, "nothing more is called");
}

/// Knows one session, as the session store would.
struct OneSession {
    session: &'static str,
    operator: OperatorId,
}

impl CredentialVerifier for OneSession {
    fn verify(&self, credential: &Credential) -> Option<OperatorId> {
        match credential {
            Credential::Session(secret) if secret.expose() == self.session => Some(self.operator),
            Credential::Session(_) => None,
            Credential::Bearer(_) => tokens().verify(credential),
        }
    }
}

/// The caller is the credential's operator only: operator ids in the
/// path, the query, the body or other headers name nobody, the bearer
/// token wins over the session cookie, and a merge is authored by the
/// caller.
#[tokio::test]
async fn forged_operator_field_has_no_effect() {
    let who = without(Permission::Audit);
    let full = operator(FULL).ulid_text();
    let session = "session-of-the-full-operator";
    let auth = Auth::fixed(
        directory(&authenticated()),
        OneSession {
            session,
            operator: operator(FULL),
        },
    );
    let fake = answering();
    let router = server_with(&fake, auth);
    let bearer = format!("Bearer {}", token(who));
    let cookie = format!("{SESSION_COOKIE}={session}");
    // The session cookie alone is the full operator.
    let reply = send(&router, get("/watermark", &[("cookie", &cookie)])).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(
        fake.calls().last().map(|call| call.operator),
        Some(operator(FULL))
    );
    // With a bearer token too, the token is the credential.
    let forged = [
        ("authorization", bearer.as_str()),
        ("cookie", cookie.as_str()),
        ("x-crosstalk-operator", full.as_str()),
        ("x-forwarded-user", full.as_str()),
        ("from", full.as_str()),
    ];
    let reply = send(&router, get("/watermark", &forged)).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(
        fake.calls().last().map(|call| call.operator),
        Some(operator(who))
    );
    // Query parameters and body fields that would name an operator are
    // not the route's: refused, nothing called.
    let calls = fake.calls().len();
    let path = format!("/watermark?operator={full}");
    let reply = send(&router, get(&path, &[("authorization", &bearer)])).await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    let mut merge = json!({"type": "merge_agents", "data": {
        "from": "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA", "into": "01J9Z3M2C5D6E7F8G9H0J1K2M3"
    }});
    merge["data"]["author"] = json!({"type": "operator", "data": full});
    let forged_body = Request::builder()
        .method("POST")
        .uri("/actions")
        .header(AUTHORIZATION, &bearer)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(merge.to_string()))
        .expect("a request");
    let reply = send(&router, forged_body).await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    assert_eq!(fake.calls().len(), calls);
    assert!(fake.actions().is_empty());
    // A merge is authored by the caller, whatever else the request says.
    let request: ActionRequest = golden("surface_actions/actions/request_merge_agents");
    let encoded = RequestBuilder::new(Route::Action(request.kind()))
        .body(&request)
        .build()
        .expect("a request");
    let merge = super::http_request(&encoded)
        .header(AUTHORIZATION, &bearer)
        .header(COOKIE, &cookie)
        .header("x-crosstalk-operator", &full)
        .body(Body::from(encoded.body.clone().unwrap_or_default()))
        .expect("a request");
    let reply = send(&router, merge).await;
    assert_eq!(reply.status, StatusCode::OK, "{reply:?}");
    let caller = directory(&authenticated())
        .caller(RequestIdentity::Verified(operator(who)))
        .expect("a configured operator");
    let expected = request.into_action(&caller).expect("two agents");
    assert_eq!(fake.actions(), vec![(operator(who), expected)]);
}

/// In trusted mode every request is the trusted operator's, whatever
/// credential it carries or lacks.
#[tokio::test]
async fn trusted_mode_ignores_the_credential() {
    let trusted = AccessConfig::Trusted(TrustedOperator {
        id: operator(7),
        name: OperatorName::new("the operator").expect("a valid name"),
    });
    let auth = Auth::fixed(directory(&trusted), StaticTokens::default());
    let fake = answering();
    let router = server_with(&fake, auth);
    for headers in [
        vec![],
        vec![("authorization", "Bearer whatever-this-may-be")],
        vec![("authorization", "Basic xyz")],
    ] {
        let reply = send(&router, get("/watermark", &headers)).await;
        assert_eq!(reply.status, StatusCode::OK, "{headers:?}");
        assert_eq!(
            fake.calls().last().map(|call| call.operator),
            Some(operator(7))
        );
    }
}
