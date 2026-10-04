//! Encoding a call as an HTTP request ([`RequestBuilder`], the client's
//! half) and reading the query string back ([`QueryParams`], the server's
//! half). Both follow the route's [`RouteSpec`]: an
//! argument goes only where the table puts it, under its name.
//!
//! The query string is `application/x-www-form-urlencoded` (the WHATWG URL
//! standard's form encoding), so [`EncodedRequest::query`] and
//! [`QueryParams::new`] hold the pairs before encoding and after decoding.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::wire::{DecodeError, DecodeErrorKind, WireRequest, decode_request};

use super::path::{PathArg, Segment, segments};
use super::routes::Route;
use super::{Method, Place, RouteSpec};

/// The largest request body the surface reads: room for a full
/// `TransmissionSelection` (100,000 ids) and then some. A longer body is
/// not read; it is refused as `MalformedRequest`.
pub const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

/// One call as HTTP: what a client sends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedRequest {
    pub method: Method,
    /// The template with every parameter replaced by its segment.
    pub path: String,
    /// Query parameters, name and value before form encoding, in the
    /// table's order.
    pub query: Vec<(&'static str, String)>,
    /// Request headers that carry an argument (the live feed's
    /// `Last-Event-ID`); the credential is the client's own concern.
    pub headers: Vec<(&'static str, String)>,
    /// The JSON body, sent with `Content-Type: application/json`.
    pub body: Option<Vec<u8>>,
}

/// Why a call could not be encoded as its route's request. Every variant
/// but `Json` is a client that does not follow the table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodeError {
    /// An argument the route does not have in that place.
    Undeclared { name: String, place: Place },
    /// An argument given twice.
    Repeated { name: &'static str },
    /// A required argument not given (or given as `null`).
    Missing { name: &'static str },
    /// The value has no JSON (a timestamp after year 9999).
    Json { name: &'static str, reason: String },
}

/// Builds the request for one call of `route`, an argument at a time, and
/// checks it against the table when built.
#[derive(Debug, Clone)]
pub struct RequestBuilder {
    spec: RouteSpec,
    path: Vec<(&'static str, String)>,
    query: Vec<(&'static str, String)>,
    headers: Vec<(&'static str, String)>,
    body: Option<Vec<u8>>,
    given: Vec<&'static str>,
    error: Option<EncodeError>,
}

impl RequestBuilder {
    pub fn new(route: Route) -> Self {
        Self {
            spec: route.spec(),
            path: Vec::new(),
            query: Vec::new(),
            headers: Vec::new(),
            body: None,
            given: Vec::new(),
            error: None,
        }
    }

    /// The path parameter `name`.
    pub fn path<T: PathArg>(mut self, name: &str, value: &T) -> Self {
        if let Some(name) = self.declare(name, Place::Path) {
            self.path.push((name, value.segment()));
        }
        self
    }

    /// The query parameter `name`, as `value`'s JSON. A value that encodes
    /// as `null` (an `Option`'s `None`) is left out.
    pub fn query<T: WireRequest>(mut self, name: &str, value: &T) -> Self {
        if let Some(name) = self.declare(name, Place::Query)
            && let Some(json) = self.json(name, value)
            && json != "null"
        {
            self.query.push((name, json));
        }
        self
    }

    /// The text query parameter `name` (the live feed's `cursor`); `None`
    /// leaves it out.
    pub fn query_text(mut self, name: &str, value: Option<&str>) -> Self {
        if let Some(name) = self.declare(name, Place::QueryText)
            && let Some(value) = value
        {
            self.query.push((name, value.to_owned()));
        }
        self
    }

    /// The header `name` (the live feed's `Last-Event-ID`); `None` leaves
    /// it out.
    pub fn header(mut self, name: &str, value: Option<&str>) -> Self {
        if let Some(name) = self.declare(name, Place::Header)
            && let Some(value) = value
        {
            self.headers.push((name, value.to_owned()));
        }
        self
    }

    /// The whole body, for a route whose one body argument is the body
    /// ([`Place::Body`]) or whose body is an object of its arguments
    /// ([`Place::BodyField`], built with a type from
    /// [`bodies`](super::bodies)). For an object, every field the route
    /// declares counts as given.
    pub fn body<T: WireRequest>(mut self, value: &T) -> Self {
        let place = if self.spec.args_in(Place::Body).next().is_some() {
            Place::Body
        } else {
            Place::BodyField
        };
        let names: Vec<&'static str> = self.spec.args_in(place).map(|arg| arg.name).collect();
        if names.is_empty() {
            self.fail(EncodeError::Undeclared {
                name: "body".to_owned(),
                place: Place::Body,
            });
            return self;
        }
        for name in &names {
            if self.given.contains(name) {
                self.fail(EncodeError::Repeated { name });
                return self;
            }
            self.given.push(name);
        }
        if let Some(json) = self.json("body", value) {
            self.body = Some(json.into_bytes());
        }
        self
    }

    /// The request, or the first way the call did not follow the table.
    pub fn build(self) -> Result<EncodedRequest, EncodeError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        if let Some(missing) = self
            .spec
            .args
            .iter()
            .find(|arg| !arg.optional && !self.present(arg.name, arg.place))
        {
            return Err(EncodeError::Missing { name: missing.name });
        }
        let template = segments(self.spec.path).unwrap_or_default();
        let mut path = String::new();
        for segment in template {
            path.push('/');
            match segment {
                Segment::Literal(literal) => path.push_str(literal),
                Segment::Param(name) => {
                    let value = self
                        .path
                        .iter()
                        .find(|(param, _)| *param == name)
                        .map(|(_, value)| value.as_str())
                        .ok_or(EncodeError::Missing { name })?;
                    path.push_str(value);
                }
            }
        }
        if path.is_empty() {
            path.push('/');
        }
        let order = |name: &&'static str| {
            self.spec
                .args
                .iter()
                .position(|arg| arg.name == *name)
                .unwrap_or(usize::MAX)
        };
        let mut query = self.query;
        query.sort_by_key(|(name, _)| order(name));
        Ok(EncodedRequest {
            method: self.spec.method,
            path,
            query,
            headers: self.headers,
            body: self.body,
        })
    }

    fn present(&self, name: &'static str, place: Place) -> bool {
        match place {
            Place::Path => self.path.iter().any(|(param, _)| *param == name),
            Place::Query | Place::QueryText => self.query.iter().any(|(param, _)| *param == name),
            Place::Header => self.headers.iter().any(|(param, _)| *param == name),
            Place::Body | Place::BodyField => self.body.is_some() && self.given.contains(&name),
        }
    }

    /// The declared name, recorded as given; `None` after recording why
    /// not.
    fn declare(&mut self, name: &str, place: Place) -> Option<&'static str> {
        let Some(arg) = self
            .spec
            .args
            .iter()
            .find(|arg| arg.name == name && arg.place == place)
        else {
            self.fail(EncodeError::Undeclared {
                name: name.to_owned(),
                place,
            });
            return None;
        };
        if self.given.contains(&arg.name) {
            self.fail(EncodeError::Repeated { name: arg.name });
            return None;
        }
        self.given.push(arg.name);
        Some(arg.name)
    }

    fn json<T: Serialize>(&mut self, name: &'static str, value: &T) -> Option<String> {
        match serde_json::to_string(value) {
            Ok(json) => Some(json),
            Err(error) => {
                self.fail(EncodeError::Json {
                    name,
                    reason: error.to_string(),
                });
                None
            }
        }
    }

    fn fail(&mut self, error: EncodeError) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }
}

/// A request's query string, checked against its route: every name is one
/// of the route's query parameters, and none appears twice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryParams(BTreeMap<String, String>);

impl QueryParams {
    /// The decoded pairs of the query string. An unknown or repeated name
    /// is a `DecodeError`, which the surface answers as `MalformedRequest`,
    /// as a JSON body's unknown or repeated field is.
    pub fn new(
        route: Route,
        pairs: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, DecodeError> {
        let spec = route.spec();
        let mut params = BTreeMap::new();
        for (name, value) in pairs {
            let declared = spec.args.iter().any(|arg| {
                arg.name == name && matches!(arg.place, Place::Query | Place::QueryText)
            });
            if !declared {
                return Err(data_error(format!("unknown query parameter `{name}`")));
            }
            if params.contains_key(&name) {
                return Err(data_error(format!("query parameter `{name}` given twice")));
            }
            params.insert(name, value);
        }
        Ok(Self(params))
    }

    /// The parameter `name` decoded as `T` through `decode_request`. An
    /// absent parameter reads as `null`: `None` for an `Option`, a
    /// `DecodeError` for anything else.
    pub fn decode<T: WireRequest>(&self, name: &str) -> Result<T, DecodeError> {
        let json = self.0.get(name).map_or("null", String::as_str);
        decode_request(json.as_bytes()).map_err(|error| DecodeError {
            kind: error.kind,
            reason: format!("query parameter `{name}`: {}", error.reason),
        })
    }

    /// The text parameter `name`, as sent.
    pub fn text(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }
}

/// Reads a request body: `None` when the route takes none. A body on a
/// route that takes none, a body over [`MAX_BODY_BYTES`], or a body whose
/// `Content-Type` is not `application/json` (parameters aside) is a
/// `DecodeError`, so a form or a text body never reaches the decoder: the
/// HTML forms a cross-site page can post send neither.
pub fn check_body<'a>(
    route: Route,
    content_type: Option<&str>,
    body: &'a [u8],
) -> Result<Option<&'a [u8]>, DecodeError> {
    let takes_body = route.spec().has_body();
    if !takes_body {
        return if body.is_empty() {
            Ok(None)
        } else {
            Err(data_error("this route takes no body".to_owned()))
        };
    }
    if body.len() > MAX_BODY_BYTES {
        return Err(data_error(format!(
            "request body over {MAX_BODY_BYTES} bytes"
        )));
    }
    let essence = content_type
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    if !essence.is_some_and(|essence| essence.eq_ignore_ascii_case(super::JSON)) {
        return Err(DecodeError {
            kind: DecodeErrorKind::Syntax,
            reason: format!("content-type must be {}", super::JSON),
        });
    }
    Ok(Some(body))
}

fn data_error(reason: String) -> DecodeError {
    DecodeError {
        kind: DecodeErrorKind::Data,
        reason,
    }
}
