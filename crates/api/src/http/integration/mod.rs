//! The server over a fake surface ([`fake::Fake`]), driven through the
//! router as a client would: every route, every error status,
//! authentication, the live feed, frames and exports. Request values and
//! responses come from the spec's wire goldens, so each test also checks
//! that the goldens round-trip through the handlers unchanged.
//!
//! Evidence for the `surface.http.*` invariants (and
//! `surface.api.caller-from-session`, `surface.live.sse-frame-matches-item`).

mod actions;
mod auth;
mod cases;
mod errors;
mod export;
mod fake;
mod frame;
mod live;
mod routes;
mod serve;

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, Request, StatusCode};
use crosstalk_spec::aggregates::projection::FrameRetention;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::http::EncodedRequest;
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorConfig, OperatorDirectory, OperatorName,
};
use crosstalk_spec::interfaces::l8_surface::{Permission, PermissionSet};
use crosstalk_spec::support::{Clock, Timestamp};
use http_body_util::BodyExt;
use serde::de::DeserializeOwned;
use tower::ServiceExt;

use super::{Auth, BearerToken, HttpApi, HttpConfig, StaticTokens};
use fake::Fake;

/// The operator with every permission.
pub(super) const FULL: u128 = 1;

/// The operator with every permission but `permission`.
pub(super) fn without(permission: Permission) -> u128 {
    10 + permission as u128
}

pub(super) fn operator(n: u128) -> OperatorId {
    OperatorId::from_ulid(n)
}

/// Operator `n`'s API token.
pub(super) fn token(n: u128) -> String {
    format!("test-token-for-operator-{n:04}")
}

fn configured(n: u128, permissions: PermissionSet) -> OperatorConfig {
    OperatorConfig {
        id: operator(n),
        name: OperatorName::new(&format!("operator {n}")).expect("a valid name"),
        permissions,
    }
}

/// `FULL`, and one operator per permission that lacks only it.
pub(super) fn authenticated() -> AccessConfig {
    let mut operators = vec![configured(FULL, PermissionSet::ALL)];
    for missing in Permission::ALL {
        let rest = Permission::ALL.into_iter().filter(|p| *p != missing);
        operators.push(configured(without(missing), PermissionSet::of(rest)));
    }
    AccessConfig::Authenticated(operators)
}

pub(super) fn directory(config: &AccessConfig) -> OperatorDirectory {
    OperatorDirectory::load(None, config)
        .expect("a valid access config")
        .0
}

/// Every configured operator's token, and one for an operator config
/// never defined (99).
pub(super) fn tokens() -> StaticTokens {
    let mut known: Vec<u128> = vec![FULL, 99];
    known.extend(Permission::ALL.map(without));
    StaticTokens::new(known.into_iter().map(|n| {
        (
            BearerToken::new(&token(n)).expect("a valid token"),
            operator(n),
        )
    }))
}

/// The clock the frame tests read: 2026-10-04T12:00:00Z.
pub(super) struct FixedClock(pub Timestamp);

impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        self.0
    }
}

pub(super) const NOW_MICROS: u64 = 1_791_115_200_000_000;

pub(super) fn config() -> HttpConfig {
    HttpConfig {
        frame_retention: FrameRetention::from_days(
            std::num::NonZeroU16::new(FrameRetention::DEFAULT_DAYS).expect("non-zero"),
        ),
        clock: Arc::new(FixedClock(Timestamp::from_micros(NOW_MICROS))),
    }
}

/// A server over `fake`, in authenticated mode with [`authenticated`]'s
/// operators.
pub(super) fn server(fake: &Arc<Fake>) -> Router {
    server_with(fake, Auth::fixed(directory(&authenticated()), tokens()))
}

pub(super) fn server_with(fake: &Arc<Fake>, auth: Auth) -> Router {
    HttpApi::new(Arc::clone(fake), auth, config()).router()
}

/// A response, read whole.
#[derive(Debug)]
pub(super) struct Reply {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl Reply {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(name)
            .map(|value| value.to_str().expect("a text header"))
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|error| {
            panic!(
                "not JSON ({error}): {}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }
}

/// Sends `request` through `router` and reads the whole response.
pub(super) async fn send(router: &Router, request: Request<Body>) -> Reply {
    let response = router
        .clone()
        .oneshot(request)
        .await
        .unwrap_or_else(|never| match never {});
    let (parts, body) = response.into_parts();
    let body = body.collect().await.expect("a whole body").to_bytes();
    Reply {
        status: parts.status,
        headers: parts.headers,
        body,
    }
}

/// `encoded` as an HTTP request from operator `as_operator` (bearer token).
pub(super) fn request(encoded: &EncodedRequest, as_operator: u128) -> Request<Body> {
    let mut builder = http_request(encoded);
    builder = builder.header(AUTHORIZATION, format!("Bearer {}", token(as_operator)));
    builder
        .body(Body::from(encoded.body.clone().unwrap_or_default()))
        .expect("a valid request")
}

/// `encoded` as an HTTP request builder, without a credential.
pub(super) fn http_request(encoded: &EncodedRequest) -> axum::http::request::Builder {
    let mut uri = encoded.path.clone();
    if !encoded.query.is_empty() {
        let query = form_urlencoded::Serializer::new(String::new())
            .extend_pairs(encoded.query.iter().map(|(name, value)| (*name, value)))
            .finish();
        uri.push('?');
        uri.push_str(&query);
    }
    let mut builder = Request::builder().method(encoded.method.as_str()).uri(uri);
    if encoded.body.is_some() {
        builder = builder.header(CONTENT_TYPE, "application/json");
    }
    for (name, value) in &encoded.headers {
        builder = builder.header(*name, value);
    }
    builder
}

fn golden_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../spec/types/tests/golden")
}

/// The golden file `area/name.json`'s text.
pub(super) fn golden_text(name: &str) -> String {
    let path = golden_root().join(format!("{name}.json"));
    std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {path:?}: {error}"))
}

/// The golden file's bytes, whatever its extension.
pub(super) fn golden_bytes(file: &str) -> Vec<u8> {
    let path = golden_root().join(file);
    std::fs::read(&path).unwrap_or_else(|error| panic!("read {path:?}: {error}"))
}

/// The golden file decoded as `T`.
pub(super) fn golden<T: DeserializeOwned>(name: &str) -> T {
    decode(&golden_text(name))
}

/// JSON text decoded as `T`.
pub(super) fn decode<T: DeserializeOwned>(json: &str) -> T {
    serde_json::from_str(json)
        .unwrap_or_else(|error| panic!("{}: {error} in {json}", std::any::type_name::<T>()))
}

/// JSON text without the whitespace between tokens: a pretty golden as
/// the compact bytes `serde_json::to_vec` writes for the same value.
pub(super) fn compact(json: &str) -> String {
    let mut out = String::with_capacity(json.len());
    let mut in_string = false;
    let mut escaped = false;
    for ch in json.chars() {
        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
        } else if ch == '"' {
            in_string = true;
            out.push(ch);
        } else if !ch.is_whitespace() {
            out.push(ch);
        }
    }
    out
}

/// The JSON `value` as a query error's JSON.
pub(super) fn error_json<E: serde::Serialize>(error: &E) -> serde_json::Value {
    serde_json::to_value(error).expect("an error has JSON")
}
