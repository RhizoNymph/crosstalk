//! Reading one request against its route: the path parameters, the query
//! string and the body, each checked by the spec's readers before any value
//! is decoded (`surface.http.undecodable-request-bad-request`).
//!
//! ```text
//! path   ─ match_template(route.path(), raw path) ─▶ PathParams     (not percent-decoded)
//! query  ─ form_urlencoded pairs ─ QueryParams::new(route, ..) ─▶ QueryParams
//!                                                 unknown or repeated ─▶ DecodeError
//! body   ─ read at most MAX_BODY_BYTES ─ check_body(route, Content-Type, ..) ─▶ Option<body>
//!                     over, none expected, not application/json ─▶ DecodeError
//! ```
//!
//! Each argument is then decoded by name through `decode_request`
//! ([`Input::path`], [`Input::query`], [`Input::body`]); a refusal is a
//! `DecodeError`, which the route answers as `MalformedRequest` (400).

use axum::body::{Body, Bytes};
use axum::http::HeaderMap;
use axum::http::header::CONTENT_TYPE;
use axum::http::request::Parts;
use crosstalk_spec::interfaces::l8_surface::http::path::match_template;
use crosstalk_spec::interfaces::l8_surface::http::request::{MAX_BODY_BYTES, check_body};
use crosstalk_spec::interfaces::l8_surface::http::{PathArg, PathParams, QueryParams, Route};
use crosstalk_spec::wire::{DecodeError, DecodeErrorKind, WireRequest, decode_request};

/// Why a request could not be read as its route's.
#[derive(Debug)]
pub(super) enum Unread {
    /// The path is not the route's (the router matched what the table
    /// does not): no route.
    NoRoute,
    /// The query string or the body is not the route's.
    Malformed(DecodeError),
}

impl From<DecodeError> for Unread {
    fn from(error: DecodeError) -> Self {
        Self::Malformed(error)
    }
}

/// One request, read against its route.
#[derive(Debug)]
pub(super) struct Input {
    params: PathParams,
    query: QueryParams,
    body: Option<Bytes>,
    headers: HeaderMap,
}

impl Input {
    /// Reads `parts` and `body` as `route`'s request.
    pub(super) async fn read(route: Route, parts: Parts, body: Body) -> Result<Self, Unread> {
        let params = match_template(route.path(), parts.uri.path()).ok_or(Unread::NoRoute)?;
        let pairs = form_urlencoded::parse(parts.uri.query().unwrap_or_default().as_bytes())
            .map(|(name, value)| (name.into_owned(), value.into_owned()));
        let query = QueryParams::new(route, pairs)?;
        // One byte over the limit is enough for `check_body` to refuse it.
        let bytes = axum::body::to_bytes(body, MAX_BODY_BYTES + 1)
            .await
            .map_err(|error| DecodeError {
                kind: DecodeErrorKind::Data,
                reason: format!("request body over {MAX_BODY_BYTES} bytes or unreadable: {error}"),
            })?;
        let content_type = parts
            .headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok());
        let body = check_body(route, content_type, &bytes)?
            .is_some()
            .then_some(bytes);
        Ok(Self {
            params,
            query,
            body,
            headers: parts.headers,
        })
    }

    /// The path parameter `name`.
    pub(super) fn path<T: PathArg>(&self, name: &str) -> Result<T, DecodeError> {
        self.params.decode(name)
    }

    /// The query parameter `name`'s JSON, `null` when absent.
    pub(super) fn query<T: WireRequest>(&self, name: &str) -> Result<T, DecodeError> {
        self.query.decode(name)
    }

    /// The text query parameter `name` (the live feed's `cursor`).
    pub(super) fn query_text(&self, name: &str) -> Option<&str> {
        self.query.text(name)
    }

    /// The whole body as `T`. A route that takes a body always has one
    /// (`check_body`); an empty one does not decode.
    pub(super) fn body<T: WireRequest>(&self) -> Result<T, DecodeError> {
        decode_request(self.body.as_deref().unwrap_or_default())
    }

    /// The request header `name` as text: its fields joined with `, `
    /// (RFC 9110's list syntax), `None` when absent. Bytes outside UTF-8
    /// are replaced, so such a value never matches what the server wrote.
    pub(super) fn header(&self, name: &str) -> Option<String> {
        let fields: Vec<String> = self
            .headers
            .get_all(name)
            .iter()
            .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
            .collect();
        (!fields.is_empty()).then(|| fields.join(", "))
    }
}
