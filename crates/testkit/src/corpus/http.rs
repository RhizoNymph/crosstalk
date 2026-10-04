//! Recorded HTTP messages: the `request.http` and `response.http` formats,
//! and comparison of what went over the wire with a recording.
//!
//! A file is a start line, header lines, one empty line, then the body
//! verbatim to the end of the file (no trailing newline is added or
//! removed). Lines of the head end with LF; a CR before it is dropped.
//! Header names are recorded in lower case, in the order sent.
//!
//! Framing and hop-by-hop headers ([`NOT_RECORDED`]) are never recorded:
//! the sender sets them for its own connection, and a proxy may change
//! them. Comparisons ignore them on both sides.

use std::path::{Path, PathBuf};

use bytes::Bytes;
use crosstalk_spec::interfaces::l0_ingress::{RequestHead, ResponseFraming, ResponseHead};
use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use hyper::http::uri::PathAndQuery;
use hyper::{Method, StatusCode};

use crate::corpus::sse::{EventStream, SseError};

/// Headers a recording leaves out and comparisons ignore: framing,
/// hop-by-hop and per-connection headers, and `date`.
pub const NOT_RECORDED: &[&str] = &[
    "connection",
    "content-length",
    "date",
    "host",
    "keep-alive",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// Whether `name` is in [`NOT_RECORDED`].
pub fn is_not_recorded(name: &HeaderName) -> bool {
    NOT_RECORDED.contains(&name.as_str())
}

/// Headers in the order they were sent, names lower case.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Headers(Vec<(HeaderName, HeaderValue)>);

impl Headers {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, name: HeaderName, value: HeaderValue) {
        self.0.push((name, value));
    }

    /// Every header in `map`, in its iteration order (grouped by name, each
    /// name's values in the order received).
    pub fn from_map(map: &HeaderMap) -> Self {
        Self(
            map.iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        )
    }

    pub fn iter(&self) -> impl Iterator<Item = &(HeaderName, HeaderValue)> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The first value of `name` (any case).
    pub fn get(&self, name: &str) -> Option<&HeaderValue> {
        self.0
            .iter()
            .find(|(header, _)| header.as_str().eq_ignore_ascii_case(name))
            .map(|(_, value)| value)
    }

    /// The first value of `name` as text, if it is visible ASCII.
    pub fn get_str(&self, name: &str) -> Option<&str> {
        self.get(name).and_then(|value| value.to_str().ok())
    }

    /// The headers a recording keeps: everything but [`NOT_RECORDED`].
    pub fn end_to_end(&self) -> Self {
        Self(
            self.0
                .iter()
                .filter(|(name, _)| !is_not_recorded(name))
                .cloned()
                .collect(),
        )
    }

    /// As `(name, value)` text pairs, for the spec's request and response
    /// heads. A value that is not UTF-8 is converted lossily.
    pub fn to_pairs(&self) -> Vec<(String, String)> {
        self.0
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect()
    }

    /// End-to-end headers by name, stably: order between names is ignored,
    /// order among one name's values is kept.
    fn comparable(&self) -> Vec<(String, Vec<u8>)> {
        let mut pairs: Vec<(String, Vec<u8>)> = self
            .end_to_end()
            .0
            .into_iter()
            .map(|(name, value)| (name.as_str().to_owned(), value.as_bytes().to_vec()))
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs
    }
}

/// A recorded request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorpusRequest {
    pub method: Method,
    /// The request target in origin form: path and query.
    pub target: PathAndQuery,
    pub headers: Headers,
    pub body: Bytes,
}

impl CorpusRequest {
    pub fn path(&self) -> &str {
        self.target.path()
    }

    pub fn query(&self) -> Option<&str> {
        self.target.query()
    }

    /// The body as JSON.
    pub fn json(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::from_slice(&self.body)
    }

    /// The spec's request head: what routing and identification read.
    pub fn head(&self) -> RequestHead {
        RequestHead {
            method: self.method.as_str().to_owned(),
            path: self.path().to_owned(),
            query: self.query().map(str::to_owned),
            headers: self.headers.to_pairs(),
        }
    }

    /// How a request that went over the wire differs from this one: method,
    /// target, end-to-end headers and body bytes. Empty when it was
    /// forwarded unchanged.
    pub fn differences(
        &self,
        method: &Method,
        target: &str,
        headers: &Headers,
        body: &[u8],
    ) -> Vec<Difference> {
        let mut differences = Vec::new();
        if method != self.method {
            differences.push(Difference::Method {
                expected: self.method.to_string(),
                got: method.to_string(),
            });
        }
        if target != self.target.as_str() {
            differences.push(Difference::Target {
                expected: self.target.as_str().to_owned(),
                got: target.to_owned(),
            });
        }
        differences.extend(header_differences(&self.headers, headers));
        differences.extend(body_difference(&self.body, body));
        differences
    }
}

/// A recorded response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorpusResponse {
    pub status: StatusCode,
    pub headers: Headers,
    pub body: ResponseBody,
}

/// A response body, framed as its `content-type` says
/// ([`ResponseHead::framing`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseBody {
    Whole(Bytes),
    EventStream(EventStream),
}

impl ResponseBody {
    /// The body's bytes, exactly as recorded.
    pub fn bytes(&self) -> &Bytes {
        match self {
            Self::Whole(bytes) => bytes,
            Self::EventStream(stream) => stream.raw(),
        }
    }

    /// The chunks a server sends: one per event for a stream, the whole
    /// body (or nothing, if empty) otherwise.
    pub fn chunks(&self) -> Vec<Bytes> {
        match self {
            Self::Whole(bytes) if bytes.is_empty() => Vec::new(),
            Self::Whole(bytes) => vec![bytes.clone()],
            Self::EventStream(stream) => stream.chunks(),
        }
    }

    pub fn events(&self) -> Option<&EventStream> {
        match self {
            Self::Whole(_) => None,
            Self::EventStream(stream) => Some(stream),
        }
    }
}

impl CorpusResponse {
    /// The spec's response head: what chooses the framer.
    pub fn head(&self) -> ResponseHead {
        ResponseHead {
            status: self.status.as_u16(),
            headers: self.headers.to_pairs(),
        }
    }

    pub fn framing(&self) -> ResponseFraming {
        self.head().framing()
    }

    /// The body as JSON, for a whole body.
    pub fn json(&self) -> Option<Result<serde_json::Value, serde_json::Error>> {
        match &self.body {
            ResponseBody::Whole(bytes) => Some(serde_json::from_slice(bytes)),
            ResponseBody::EventStream(_) => None,
        }
    }

    /// How a response that went over the wire differs from this one:
    /// status, end-to-end headers and body bytes.
    pub fn differences(
        &self,
        status: StatusCode,
        headers: &Headers,
        body: &[u8],
    ) -> Vec<Difference> {
        let mut differences = Vec::new();
        if status != self.status {
            differences.push(Difference::Status {
                expected: self.status.as_u16(),
                got: status.as_u16(),
            });
        }
        differences.extend(header_differences(&self.headers, headers));
        differences.extend(body_difference(self.body.bytes(), body));
        differences
    }
}

/// One way a message on the wire differs from its recording.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Difference {
    Method {
        expected: String,
        got: String,
    },
    Target {
        expected: String,
        got: String,
    },
    Status {
        expected: u16,
        got: u16,
    },
    /// The end-to-end headers differ; both sides sorted by name.
    Headers {
        expected: Vec<(String, String)>,
        got: Vec<(String, String)>,
    },
    /// The bodies differ, first at byte `at`.
    Body {
        expected_len: usize,
        got_len: usize,
        at: usize,
    },
}

fn header_differences(expected: &Headers, got: &Headers) -> Option<Difference> {
    let (expected, got) = (expected.comparable(), got.comparable());
    if expected == got {
        return None;
    }
    let text = |pairs: Vec<(String, Vec<u8>)>| {
        pairs
            .into_iter()
            .map(|(name, value)| (name, String::from_utf8_lossy(&value).into_owned()))
            .collect()
    };
    Some(Difference::Headers {
        expected: text(expected),
        got: text(got),
    })
}

fn body_difference(expected: &[u8], got: &[u8]) -> Option<Difference> {
    if expected == got {
        return None;
    }
    let at = expected
        .iter()
        .zip(got)
        .position(|(a, b)| a != b)
        .unwrap_or(expected.len().min(got.len()));
    Some(Difference::Body {
        expected_len: expected.len(),
        got_len: got.len(),
        at,
    })
}

/// Why a recorded message file could not be read.
#[derive(Debug, thiserror::Error)]
pub enum HttpFileError {
    #[error("reading {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("{path}: {problem}")]
    Malformed { path: PathBuf, problem: Malformed },
}

/// What is wrong with a recorded message's head.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Malformed {
    #[error("no empty line ends the head")]
    NoBlankLine,
    #[error("the head is not UTF-8")]
    HeadNotUtf8,
    #[error("bad start line {line:?}")]
    StartLine { line: String },
    #[error("bad method {method:?}")]
    Method { method: String },
    #[error("bad request target {target:?}")]
    Target { target: String },
    #[error("bad status {status:?}")]
    Status { status: String },
    #[error("bad header line {line:?}")]
    HeaderLine { line: String },
    #[error("header name {name:?} is not a lower-case token")]
    HeaderName { name: String },
    #[error("bad value for header {name:?}")]
    HeaderValue { name: String },
    #[error("header {name:?} is never recorded (framing or hop-by-hop)")]
    NotRecorded { name: String },
    #[error("bad event stream: {0}")]
    EventStream(SseError),
}

/// A file split into start line, headers and body.
struct Parsed {
    start: String,
    headers: Headers,
    body: Bytes,
}

fn read(path: &Path) -> Result<Bytes, HttpFileError> {
    std::fs::read(path)
        .map(Bytes::from)
        .map_err(|source| HttpFileError::Io {
            path: path.to_owned(),
            source,
        })
}

fn parse(bytes: &Bytes) -> Result<Parsed, Malformed> {
    let split = bytes
        .windows(2)
        .position(|pair| pair == b"\n\n")
        .map(|at| (at + 1, at + 2))
        .or_else(|| {
            bytes
                .windows(4)
                .position(|quad| quad == b"\r\n\r\n")
                .map(|at| (at + 2, at + 4))
        })
        .ok_or(Malformed::NoBlankLine)?;
    let head = std::str::from_utf8(&bytes[..split.0]).map_err(|_| Malformed::HeadNotUtf8)?;
    let mut lines = head.lines();
    let start = lines.next().unwrap_or_default().to_owned();
    let mut headers = Headers::new();
    for line in lines {
        let (name, value) = line.split_once(':').ok_or_else(|| Malformed::HeaderLine {
            line: line.to_owned(),
        })?;
        if name.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return Err(Malformed::HeaderName {
                name: name.to_owned(),
            });
        }
        let header =
            HeaderName::from_bytes(name.as_bytes()).map_err(|_| Malformed::HeaderName {
                name: name.to_owned(),
            })?;
        if is_not_recorded(&header) {
            return Err(Malformed::NotRecorded {
                name: name.to_owned(),
            });
        }
        let value =
            HeaderValue::from_str(value.strip_prefix(' ').unwrap_or(value)).map_err(|_| {
                Malformed::HeaderValue {
                    name: name.to_owned(),
                }
            })?;
        headers.push(header, value);
    }
    Ok(Parsed {
        start,
        headers,
        body: bytes.slice(split.1..),
    })
}

/// Read a `request.http` file.
pub fn read_request(path: &Path) -> Result<CorpusRequest, HttpFileError> {
    let bytes = read(path)?;
    parse_request(&bytes).map_err(|problem| HttpFileError::Malformed {
        path: path.to_owned(),
        problem,
    })
}

/// Parse a `request.http` file's contents.
pub fn parse_request(bytes: &Bytes) -> Result<CorpusRequest, Malformed> {
    let parsed = parse(bytes)?;
    let mut parts = parsed.start.split(' ');
    let (Some(method), Some(target), Some("HTTP/1.1"), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(Malformed::StartLine { line: parsed.start });
    };
    let method = Method::from_bytes(method.as_bytes()).map_err(|_| Malformed::Method {
        method: method.to_owned(),
    })?;
    let target = target
        .parse::<PathAndQuery>()
        .ok()
        .filter(|target| target.as_str().starts_with('/'))
        .ok_or_else(|| Malformed::Target {
            target: target.to_owned(),
        })?;
    Ok(CorpusRequest {
        method,
        target,
        headers: parsed.headers,
        body: parsed.body,
    })
}

/// Read a `response.http` file.
pub fn read_response(path: &Path) -> Result<CorpusResponse, HttpFileError> {
    let bytes = read(path)?;
    parse_response(&bytes).map_err(|problem| HttpFileError::Malformed {
        path: path.to_owned(),
        problem,
    })
}

/// Parse a `response.http` file's contents. The body is an event stream
/// when `content-type` says `text/event-stream`.
pub fn parse_response(bytes: &Bytes) -> Result<CorpusResponse, Malformed> {
    let parsed = parse(bytes)?;
    let mut parts = parsed.start.splitn(3, ' ');
    let (Some("HTTP/1.1"), Some(status)) = (parts.next(), parts.next()) else {
        return Err(Malformed::StartLine { line: parsed.start });
    };
    let status = status
        .parse::<u16>()
        .ok()
        .and_then(|code| StatusCode::from_u16(code).ok())
        .ok_or_else(|| Malformed::Status {
            status: status.to_owned(),
        })?;
    let head = ResponseHead {
        status: status.as_u16(),
        headers: parsed.headers.to_pairs(),
    };
    let body = match head.framing() {
        ResponseFraming::EventStream => ResponseBody::EventStream(
            EventStream::parse(parsed.body).map_err(Malformed::EventStream)?,
        ),
        ResponseFraming::Whole => ResponseBody::Whole(parsed.body),
    };
    Ok(CorpusResponse {
        status,
        headers: parsed.headers,
        body,
    })
}
