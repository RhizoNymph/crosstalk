//! What the fake upstream answers: replies, faults, pacing and routes.

use std::collections::VecDeque;
use std::time::Duration;

use bytes::Bytes;
use hyper::header::{HeaderName, HeaderValue};
use hyper::{Method, StatusCode};

use crate::corpus::http::{CorpusResponse, Headers, ResponseBody};
use crate::corpus::{Case, CorpusRequest};

/// When chunks go out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Pacing {
    /// Before the first body chunk (the head goes out at once).
    pub first_chunk: Duration,
    /// Between consecutive chunks.
    pub between_chunks: Duration,
}

impl Pacing {
    /// Everything at once.
    pub const IMMEDIATE: Self = Self {
        first_chunk: Duration::ZERO,
        between_chunks: Duration::ZERO,
    };

    /// `delay` before the first chunk and between every two.
    pub fn every(delay: Duration) -> Self {
        Self {
            first_chunk: delay,
            between_chunks: delay,
        }
    }
}

/// Something going wrong with a reply, on command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Send the head and the first `after_chunks` chunks, then nothing more
    /// while the connection stays open: the body never ends. Released when
    /// the client goes away or the upstream is dropped.
    Stall { after_chunks: usize },
    /// Send the head and the first `after_chunks` chunks, then drop the
    /// connection without ending the body (no terminating chunk, or fewer
    /// bytes than `content-length`).
    Disconnect { after_chunks: usize },
    /// Read the request and never answer it, not even the head.
    NoResponse,
}

/// How a reply's body is framed on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Framing {
    /// A known length: `content-length`.
    Whole,
    /// Chunked transfer encoding, one wire chunk per body chunk.
    Streamed,
}

/// One answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub status: StatusCode,
    /// End-to-end headers, in order. Framing headers are set by the server.
    pub headers: Headers,
    /// The body, in the chunks it is sent in.
    pub chunks: Vec<Bytes>,
    pub framing: Framing,
    pub pacing: Pacing,
    pub fault: Option<Fault>,
}

impl Reply {
    /// The recorded response: its status, headers and body bytes. An event
    /// stream goes out one event per chunk, chunked; anything else whole.
    pub fn from_response(response: &CorpusResponse) -> Self {
        let framing = match response.body {
            ResponseBody::Whole(_) => Framing::Whole,
            ResponseBody::EventStream(_) => Framing::Streamed,
        };
        Self {
            status: response.status,
            headers: response.headers.clone(),
            chunks: response.body.chunks(),
            framing,
            pacing: Pacing::IMMEDIATE,
            fault: None,
        }
    }

    /// `case`'s recorded response.
    pub fn from_case(case: &Case) -> Self {
        Self::from_response(&case.response)
    }

    /// A whole JSON body.
    pub fn json(status: StatusCode, body: &serde_json::Value) -> Self {
        let mut headers = Headers::new();
        headers.push(
            hyper::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        Self {
            status,
            headers,
            chunks: vec![Bytes::from(body.to_string())],
            framing: Framing::Whole,
            pacing: Pacing::IMMEDIATE,
            fault: None,
        }
    }

    /// An Anthropic error response for `status`, with the error type the API
    /// uses for it (`rate_limit_error` for 429, `overloaded_error` for 529,
    /// `api_error` for an unlisted status).
    pub fn error(status: StatusCode, message: &str) -> Self {
        Self::json(
            status,
            &serde_json::json!({
                "type": "error",
                "error": {"type": error_type(status), "message": message},
            }),
        )
    }

    /// Add a header.
    pub fn header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.push(name, value);
        self
    }

    pub fn paced(mut self, pacing: Pacing) -> Self {
        self.pacing = pacing;
        self
    }

    pub fn with_fault(mut self, fault: Fault) -> Self {
        self.fault = Some(fault);
        self
    }

    /// The body's bytes, concatenated.
    pub fn body(&self) -> Bytes {
        self.chunks.concat().into()
    }
}

/// The Anthropic error type for an HTTP status.
pub fn error_type(status: StatusCode) -> &'static str {
    match status.as_u16() {
        400 => "invalid_request_error",
        401 => "authentication_error",
        402 => "billing_error",
        403 => "permission_error",
        404 => "not_found_error",
        409 => "conflict_error",
        413 => "request_too_large",
        429 => "rate_limit_error",
        504 => "timeout_error",
        529 => "overloaded_error",
        _ => "api_error",
    }
}

/// Which requests a route answers: a method and a path, query ignored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub method: Method,
    pub path: String,
}

impl Route {
    pub fn new(method: Method, path: &str) -> Self {
        Self {
            method,
            path: path.to_owned(),
        }
    }

    /// The route of a recorded request.
    pub fn of(request: &CorpusRequest) -> Self {
        Self::new(request.method.clone(), request.path())
    }

    pub fn matches(&self, method: &Method, target: &str) -> bool {
        let path = target.split_once('?').map_or(target, |(path, _)| path);
        self.method == *method && self.path == path
    }
}

/// The replies the fake upstream serves, by route.
///
/// Each route answers with its replies in order; its last reply is repeated
/// for every later request. A request no route matches gets the fallback, a
/// 404 `not_found_error` unless set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Script {
    routes: Vec<(Route, VecDeque<Reply>)>,
    fallback: Reply,
}

impl Default for Script {
    fn default() -> Self {
        Self::new()
    }
}

impl Script {
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
            fallback: Reply::error(
                StatusCode::NOT_FOUND,
                "The requested resource could not be found.",
            ),
        }
    }

    /// Answer `route` with `reply`, after any replies it already has.
    pub fn route(mut self, route: Route, reply: Reply) -> Self {
        self.mount(route, reply);
        self
    }

    /// Replay `case`: its request's method and path answered with its
    /// recorded response.
    pub fn case(self, case: &Case) -> Self {
        self.route(Route::of(&case.request), Reply::from_case(case))
    }

    /// Replay every case, in order. Cases on one route answer in turn.
    pub fn cases<'a>(self, cases: impl IntoIterator<Item = &'a Case>) -> Self {
        cases.into_iter().fold(self, Self::case)
    }

    pub fn fallback(mut self, reply: Reply) -> Self {
        self.fallback = reply;
        self
    }

    pub(crate) fn mount(&mut self, route: Route, reply: Reply) {
        match self
            .routes
            .iter_mut()
            .find(|(existing, _)| *existing == route)
        {
            Some((_, replies)) => replies.push_back(reply),
            None => self.routes.push((route, VecDeque::from([reply]))),
        }
    }

    /// The reply for a request: the matching route's next reply, or the
    /// fallback.
    pub(crate) fn answer(&mut self, method: &Method, target: &str) -> Reply {
        let replies = self
            .routes
            .iter_mut()
            .find(|(route, _)| route.matches(method, target))
            .map(|(_, replies)| replies);
        match replies {
            Some(replies) if replies.len() > 1 => {
                replies.pop_front().unwrap_or_else(|| self.fallback.clone())
            }
            Some(replies) => replies
                .front()
                .cloned()
                .unwrap_or_else(|| self.fallback.clone()),
            None => self.fallback.clone(),
        }
    }
}
